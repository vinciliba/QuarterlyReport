# reporting/quarterly_report/modules/granting.py
from __future__ import annotations

# ═══════════════════════════════════════════════════════════════════
# STANDARD LIBRARY IMPORTS
# ═══════════════════════════════════════════════════════════════════
import calendar
import locale
import logging
import re
import sqlite3
import warnings
from datetime import date, datetime
from pathlib import Path
from typing import List, Tuple, Union

# ═══════════════════════════════════════════════════════════════════
# THIRD-PARTY IMPORTS
# ═══════════════════════════════════════════════════════════════════
import altair as alt
import numpy as np
import pandas as pd
from altair_saver import save
from great_tables import GT, md, loc, style
from IPython.display import display, clear_output
import selenium.webdriver

# ═══════════════════════════════════════════════════════════════════
# PROJECT IMPORTS
# ═══════════════════════════════════════════════════════════════════

# Database utilities
from ingestion.db_utils import (
    init_db,
    fetch_latest_table_data,
    fetch_vars_for_report,
    get_alias_last_load,
    get_variable_status,
    load_report_params,
    insert_variable
)

# Reporting utilities
from reporting.quarterly_report.utils import Database, RenderContext, BaseModule
from reporting.quarterly_report.report_utils.granting_utils import (
    enrich_grants,
    _ensure_timedelta_cols,
    _coerce_date_columns
)

# ═══════════════════════════════════════════════════════════════════
# CONFIGURE WARNINGS AND SETTINGS
# ═══════════════════════════════════════════════════════════════════
warnings.filterwarnings("ignore", message=".*convert_dtype.*", category=FutureWarning)
warnings.filterwarnings('ignore')


# Configure logging
logging.basicConfig(
    level=logging.DEBUG,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s',
    handlers=[
        logging.FileHandler('amendments_report.log'),
        logging.StreamHandler()
    ]
)
logger = logging.getLogger("Payments")

CALLS_TYPES_LIST = ['STG', 'ADG', 'POC', 'COG', 'SYG', 'StG', 'CoG', 'AdG', 'SyG', 'PoC', 'CSA']

# ──────────────────────────────────────────────────────────────
# HELPERS
# ──────────────────────────────────────────────────────────────

def determine_epoch_year(cutoff_date: pd.Timestamp) -> int:
    """
    Returns the correct reporting year.
    If the cutoff is in January, then we are reporting for the *previous* year.
    """
    return cutoff_date.year - 1 if cutoff_date.month == 1 else cutoff_date.year



def get_scope_start_end(cutoff: pd.Timestamp) -> Tuple[pd.Timestamp, pd.Timestamp]:
    """
    Unified scope logic with year transition:
    • If cutoff is in January → report full previous year
    • Otherwise → return start of year to quarter-end
    """
    if cutoff.month == 1:
        year = cutoff.year - 1
        return pd.Timestamp(year=year, month=1, day=1), pd.Timestamp(year=year, month=12, day=31)

    def quarter_end(cutoff: pd.Timestamp) -> pd.Timestamp:
        first_day = cutoff.replace(day=1)
        last_month = first_day - pd.offsets.MonthBegin()
        m = last_month.month

        if m <= 3:
            return pd.Timestamp(year=cutoff.year, month=3, day=31)
        elif m <= 6:
            return pd.Timestamp(year=cutoff.year, month=6, day=30)
        elif m <= 9:
            return pd.Timestamp(year=cutoff.year, month=9, day=30)
        else:
            return pd.Timestamp(year=cutoff.year, month=12, day=31)

    return pd.Timestamp(year=cutoff.year, month=1, day=1), quarter_end(cutoff)



def months_in_scope(cutoff: pd.Timestamp) -> list[str]:
    """
    Returns list of month names from January to last *full* month before cutoff.
    Handles year rollover if cutoff is in January.
    """
    if cutoff.month == 1:
        year = cutoff.year - 1
        end_month = 12
    else:
        year = cutoff.year
        end_month = cutoff.month - 1

    months = pd.date_range(
        start=pd.Timestamp(year=year, month=1, day=1),
        end=pd.Timestamp(year=year, month=end_month, day=1),
        freq="MS"
    ).strftime("%B").tolist()

    return months

def determine_po_category(row):

    instrument = str(row.get('Instrument', '')).strip()
    topic = str(row.get('Topic', '')).strip()

    try:
        if topic and any(call_type in topic for call_type in CALLS_TYPES_LIST):
            category = next(call_type for call_type in CALLS_TYPES_LIST if call_type in topic).upper()
            return category
        elif instrument and any(call_type in instrument for call_type in CALLS_TYPES_LIST):
            category = next(call_type for call_type in CALLS_TYPES_LIST if call_type in instrument).upper()
            return category
        return ''
    except Exception as e:
        raise

def determine_po_category_po_list(row):

    summa = str(row.get('PO Purchase Order Item Desc', '')).strip()
    abac = str(row.get('PO ABAC SAP Reference', '')).strip()

    try:
        if summa and any(call_type in summa for call_type in CALLS_TYPES_LIST):
            category = next(call_type for call_type in CALLS_TYPES_LIST if call_type in summa).upper()
            return category
        elif abac and any(call_type in abac for call_type in CALLS_TYPES_LIST):
            category = next(call_type for call_type in CALLS_TYPES_LIST if call_type in abac).upper()
            return category
        return ''
    except Exception as e:
        raise

def extract_project_number(row):
    """
    Extract project number from 'Inv Text' if 'v_check_payment_type' contains RP patterns,
    otherwise return original 'v_check_payment_type' value
    """
    payment_type = row['v_check_payment_type']
    inv_text = row['Inv Text']
    
    # Handle NaN values
    if pd.isna(payment_type):
        return payment_type
    
    # Convert to string to handle any data type
    payment_type_str = str(payment_type)
    
    # Check if the payment_type contains RP patterns:
    # - Original pattern: RP + number + = + FP/IP (e.g., RP4=FP, RP2=IP)
    # - New pattern: RP + number + - + FP/IP (e.g., RP4-FP, RP2-IP)
    rp_patterns = [
        r'RP\d+=(?:FP|IP)',  # Original pattern: RP4=FP, RP2=IP, etc.
        r'RP\d+-(?:FP|IP)'   # New pattern: RP4-FP, RP2-IP, etc.
    ]
    
    # Check if any of the RP patterns match
    has_rp_pattern = any(re.search(pattern, payment_type_str) for pattern in rp_patterns)
    
    if has_rp_pattern:
        # Extract the numerical part from Inv Text column
        if pd.notna(inv_text):
            inv_text_str = str(inv_text).strip()
            # Extract leading digits from Inv Text
            number_match = re.match(r'^(\d+)', inv_text_str)
            if number_match:
                return number_match.group(1)
        
        # If no number found in Inv Text, return original payment_type
        return payment_type
    
    # Return original v_check_payment_type if no RP pattern found
    return payment_type


def map_project_to_call_type(project_num, mapping_dict):
    # If it's a numeric string, try to convert and lookup
    try:
        # Try to convert to int for lookup
        numeric_key = int(project_num)
        if numeric_key in mapping_dict:
            return mapping_dict[numeric_key]
    except (ValueError, TypeError):
        # If conversion fails, it's a non-numeric string like 'EXPERTS'
        pass
    
    # Return original value if no match found
    return project_num

def map_call_type_with_experts(row, grant_map):
    """
    Map call_type based on project_number and Inv Parking Person Id
    """
    project_num = row['project_number']
    contract_type = row['v_payment_type']
    
    # First, try to map using grant_map (convert project_num to int if possible)
    try:
        numeric_key = int(project_num)
        if numeric_key in grant_map:
            return grant_map[numeric_key]
    except (ValueError, TypeError):
        pass
    
    # If project_number is 'EXPERTS', keep it as 'EXPERTS'
    if str(project_num).upper() == 'EXPERTS' or str(contract_type).upper() == 'EXPERTS':
        return 'EXPERTS'
    
    # Return original project_number if no conditions are met
    return project_num

def map_payment_type(row):
    if row['v_payment_type'] == 'Other' and row['Pay Workflow Last AOS Person Id'] == 'WALASOU':
        return 'EXPERTS'
    return row['v_payment_type']

# Instead, handle conversion in the mapping function
def safe_map_project_to_call_type(project_num, mapping_dict):
    """
    Maps project number to call type, handles all data type issues internally
    """
    try:
        # Handle NaN values
        if pd.isna(project_num):
            return None
            
        # Convert whatever format to integer for lookup
        if isinstance(project_num, str):
            # Handle strings like '4500053782.0'
            if project_num.endswith('.0'):
                numeric_key = int(project_num[:-2])
            else:
                numeric_key = int(float(project_num))
        else:
            # Handle numeric values (float/int)
            numeric_key = int(float(project_num))
            
        # Lookup in mapping dictionary
        if numeric_key in mapping_dict:
            result = mapping_dict[numeric_key]
            if pd.notna(result) and result != '':
                return result
                
    except (ValueError, TypeError, OverflowError):
        # Any conversion error, return None
        pass
    
    return None

# Apply mapping without converting the whole column
def apply_conditional_mapping(row):
    current_call_type = row['call_type']
    po_key = row['PO Purchase Order Key']  # Use as-is, no conversion
    
    should_map = (
        pd.isna(current_call_type) or 
        current_call_type == '' or 
        current_call_type not in CALLS_TYPES_LIST or 
        current_call_type in ['EXPERTS', 'CSA']
    )
    
    if should_map:
        mapped_value = safe_map_project_to_call_type(po_key, po_map)
        return mapped_value if mapped_value is not None else current_call_type
    else:
        return current_call_type
    

def calculate_current_ttp_metrics(df_paym, cutoff):
    """
    Calculate current TTP metrics from df_paym data, filtering out negative v_TTP_NET
    """

    try:
            # Filter data up to cutoff and deduplicate by Pay Payment Key
            quarter_dates = get_scope_start_end(cutoff=cutoff)
            last_valid_date = quarter_dates[1]

            df_filtered = df_paym[
                df_paym['Pay Document Date (dd/mm/yyyy)'] <= last_valid_date
            ].copy()
            df_unique = df_filtered.drop_duplicates(subset=['Pay Payment Key']).copy()
            
            # Convert to numeric and filter negative v_TTP_NET
            df_unique['v_TTP_NET'] = pd.to_numeric(df_unique['v_TTP_NET'], errors='coerce')
            df_unique['v_TTP_GROSS'] = pd.to_numeric(df_unique['v_TTP_GROSS'], errors='coerce')
            df_unique['v_payment_in_time'] = pd.to_numeric(df_unique['v_payment_in_time'], errors='coerce')
            df_unique = df_unique[df_unique['v_TTP_NET'] >= 0]
            
            results = {}
            
            # Calculate by Programme and Payment Type
            for programme in ['H2020', 'HEU']:
                prog_data = df_unique[df_unique['Programme'] == programme]
                if len(prog_data) == 0:
                    continue
                    
                results[programme] = {}
                
                # Overall programme metrics
                prog_valid = prog_data[prog_data['v_payment_in_time'].notna()]
                results[programme]['overall'] = {
                    'avg_ttp_net': prog_data['v_TTP_NET'].mean(),
                    'avg_ttp_gross': prog_data['v_TTP_GROSS'].mean(),
                    'on_time_pct': prog_data['v_payment_in_time'].sum() / len(prog_valid) if len(prog_valid) > 0 else 0
                }
                
                # By payment type - using correct short form values from v_payment_type
                payment_types = ['IP', 'FP', 'EXPERTS', 'PF']  # Short form values
                for payment_type in payment_types:
                    pt_data = prog_data[prog_data['v_payment_type'] == payment_type]
                    if len(pt_data) > 0:
                        pt_valid = pt_data[pt_data['v_payment_in_time'].notna()]
                        results[programme][payment_type] = {
                            'avg_ttp_net': pt_data['v_TTP_NET'].mean(),
                            'avg_ttp_gross': pt_data['v_TTP_GROSS'].mean(),
                            'on_time_pct': pt_data['v_payment_in_time'].sum() / len(pt_valid) if len(pt_valid) > 0 else 0
                        }
            
            # Overall total
            total_valid = df_unique[df_unique['v_payment_in_time'].notna()]
            results['TOTAL'] = {
                'avg_ttp_net': df_unique['v_TTP_NET'].mean(),
                'avg_ttp_gross': df_unique['v_TTP_GROSS'].mean(),
                'on_time_pct': df_unique['v_payment_in_time'].sum() / len(total_valid) if len(total_valid) > 0 else 0
            }
            return results
    
    except Exception as e:
        raise Exception(f"Error in calculate_current_ttp_metrics: {str(e)}")
    
# ──────────────────────────────────────────────────────────────
# TIME MANIPULATION HELPERS
# ──────────────────────────────────────────────────────────────

# Existing periodicity functions
def determine_epoch_year(cutoff_date: pd.Timestamp) -> int:
    """
    Returns the correct reporting year.
    If the cutoff is in January, then we are reporting for the *previous* year.
    """
    return cutoff_date.year - 1 if cutoff_date.month == 1 else cutoff_date.year

def get_scope_start_end(cutoff: pd.Timestamp) -> Tuple[pd.Timestamp, pd.Timestamp]:
    """
    Unified scope logic with year transition:
    • If cutoff is in January → report full previous year
    • Otherwise → return start of year to quarter-end
    """
    if cutoff.month == 1:
        year = cutoff.year - 1
        return pd.Timestamp(year=year, month=1, day=1), pd.Timestamp(year=year, month=12, day=31)

    def quarter_end(cutoff: pd.Timestamp) -> pd.Timestamp:
        first_day = cutoff.replace(day=1)
        last_month = first_day - pd.offsets.MonthBegin()
        m = last_month.month

        if m <= 3:
            return pd.Timestamp(year=cutoff.year, month=3, day=31)
        elif m <= 6:
            return pd.Timestamp(year=cutoff.year, month=6, day=30)
        elif m <= 9:
            return pd.Timestamp(year=cutoff.year, month=9, day=30)
        else:
            return pd.Timestamp(year=cutoff.year, month=12, day=31)

    return pd.Timestamp(year=cutoff.year, month=1, day=1), quarter_end(cutoff)

def months_in_scope(cutoff: pd.Timestamp) -> list[str]:
    """
    Returns list of month names from January to last *full* month before cutoff.
    Handles year rollover if cutoff is in January.
    """
    if cutoff.month == 1:
        year = cutoff.year - 1
        end_month = 12
    else:
        year = cutoff.year
        end_month = cutoff.month - 1

    months = pd.date_range(
        start=pd.Timestamp(year=year, month=1, day=1),
        end=pd.Timestamp(year=year, month=end_month, day=1),
        freq="MS"
    ).strftime("%B").tolist()

    return months


# ──────────────────────────────────────────────────────────────
# PAYMENTS TABLES : 1. quarterly_tables_generation_main
# ──────────────────────────────────────────────────────────────
def quarterly_tables_generation_main(df_paym, cutoff, db_path, report, table_colors):
        
    """
    Main function to generate quarterly tables with comprehensive error handling
    
    Returns:
        tuple: (success: bool, message: str, results: dict or None)
    """
    
    try:
        # Validate input parameters
        if df_paym is None or df_paym.empty:
            return False, "Input DataFrame is empty or None", None
            
        if cutoff is None:
            return False, "Cutoff date is required", None
            
        if db_path is None:
            return False, "Database path is required", None
            
        if report is None:
            return False, "Report parameter is required", None

        
        def create_quarterly_payment_tables(df_paym, cutoff_date=None):
            """
            Create quarterly payment tables matching the format from the Excel file
            - Amount summing: All v_amount_to_sum per payment key, regrouped by fund source
            - Number of payments: Count unique Pay Payment Key occurrences (deduplicated)
            - Assumes df_paym is already filtered for the correct time scope
            """
            
            print("=== QUARTERLY PAYMENT TABLES GENERATION ===")
            
            # Step 1: Set cutoff date for metadata
            if cutoff_date is None:
                cutoff_date = pd.Timestamp.now()
            elif isinstance(cutoff_date, str):
                cutoff_date = pd.Timestamp(cutoff_date)
            
            print(f"Cutoff date: {cutoff_date}")
            
            # Step 2: Get reporting metadata (for reference)
            reporting_year = determine_epoch_year(cutoff_date)
            scope_start, scope_end = get_scope_start_end(cutoff_date)
            months_in_report = months_in_scope(cutoff_date)
            
            print(f"Reporting year: {reporting_year}")
            print(f"Expected scope: {scope_start} to {scope_end}")
            print(f"Note: Assuming df_paym is already filtered for this scope")
            
            # Step 3: Validate required columns
            required_columns = [
                'Pay Payment Key', 
                'v_amount_to_sum', 
                'Fund Source',
                'v_payment_type', 
                'Pay Document Date (dd/mm/yyyy)',
                'Programme'
            ]
            
            # Check for optional call_type column
            optional_columns = ['call_type', 'Call Type', 'v_call_type']
            call_type_col = None
            for col in optional_columns:
                if col in df_paym.columns:
                    call_type_col = col
                    print(f"Found call type column: {col}")
                    break
            
            if call_type_col:
                required_columns.append(call_type_col)
            else:
                print("No call_type column found - will use Fund Source only")
            
            missing_columns = [col for col in required_columns if col not in df_paym.columns]
            if missing_columns:
                print(f"ERROR: Missing required columns: {missing_columns}")
                return None
            
            print("✓ All required columns present")
            
            # Step 4: Create working dataframe (skip date filtering since already done)
            df_work = df_paym[required_columns].copy()
            
            # Check for any remaining invalid dates
            invalid_dates = df_work['Pay Document Date (dd/mm/yyyy)'].isna().sum()
            if invalid_dates > 0:
                print(f"WARNING: {invalid_dates} rows with invalid dates found, removing them")
                df_work = df_work.dropna(subset=['Pay Document Date (dd/mm/yyyy)'])
            
            print(f"Working dataset: {len(df_work)} rows")
            
            if len(df_work) == 0:
                print("ERROR: No data available after validation")
                return None
            
            # Create quarter and year columns
            df_work['Quarter'] = df_work['Pay Document Date (dd/mm/yyyy)'].dt.to_period('Q')
            df_work['Year'] = df_work['Pay Document Date (dd/mm/yyyy)'].dt.year
            df_work['Quarter_Label'] = df_work['Quarter'].astype(str)
            
            print(f"Actual date range: {df_work['Pay Document Date (dd/mm/yyyy)'].min()} to {df_work['Pay Document Date (dd/mm/yyyy)'].max()}")
            print(f"Quarters found: {sorted(df_work['Quarter_Label'].unique())}")
            
            # Step 5: Map payment types and fund sources
            payment_type_mapping = {
                'IP': 'Interim Payments',
                'FP': 'Final Payments', 
                'PF': 'Pre-financing',
                'EXPERTS': 'Experts and Support'
            }
            
            # Keep original fund sources for now (don't map to C1/E0 yet)
            df_work['Payment_Type_Desc'] = df_work['v_payment_type'].map(payment_type_mapping)
            
            # Use call_type if available, otherwise use Fund Source
            if call_type_col:
                df_work['Call_Type_Display'] = df_work[call_type_col]
                print(f"Call types found: {sorted(df_work['Call_Type_Display'].unique())}")
            else:
                df_work['Call_Type_Display'] = df_work['Fund Source']
                print(f"Using Fund Source as call type: {sorted(df_work['Call_Type_Display'].unique())}")
            
            # Handle unmapped payment types
            unmapped_payments = df_work[df_work['Payment_Type_Desc'].isna()]['v_payment_type'].unique()
            if len(unmapped_payments) > 0:
                print(f"WARNING: Unmapped payment types found: {unmapped_payments}")
                # Keep unmapped ones with their original value
                df_work['Payment_Type_Desc'] = df_work['Payment_Type_Desc'].fillna(df_work['v_payment_type'])
            
            # Step 6: Split by Programme (H2020 and HEU)
            programmes = df_work['Programme'].unique()
            print(f"Programmes found: {programmes}")
            
            results = {
                'metadata': {
                    'cutoff_date': cutoff_date,
                    'reporting_year': reporting_year,
                    'scope_start': scope_start,
                    'scope_end': scope_end,
                    'months_in_scope': months_in_report,
                    'actual_date_range': {
                        'start': df_work['Pay Document Date (dd/mm/yyyy)'].min(),
                        'end': df_work['Pay Document Date (dd/mm/yyyy)'].max()
                    },
                    'call_type_column': call_type_col,
                    'has_call_types': call_type_col is not None
                },
                'tables': {}
            }

            for programme in programmes:
                if programme not in ['H2020', 'HEU']:
                    print(f"Skipping programme: {programme}")
                    continue
                    
                print(f"\n=== Processing {programme} ===")
                df_prog = df_work[df_work['Programme'] == programme].copy()
                
                if len(df_prog) == 0:
                    print(f"No data for {programme}")
                    continue
                
                # Create aggregation tables
                tables = create_programme_tables(df_prog, programme, reporting_year)
                results['tables'][programme] = tables
            
            return results

        def create_programme_tables(df_prog, programme_name, reporting_year):
            """
            Create all payment type tables for a specific programme
            """
            
            tables = {}
            
            # Get unique payment types in this programme
            payment_types = df_prog['Payment_Type_Desc'].dropna().unique()
            
            for payment_type in payment_types:
                print(f"  Creating table for: {payment_type}")
                
                df_type = df_prog[df_prog['Payment_Type_Desc'] == payment_type].copy()
                
                if len(df_type) == 0:
                    continue
                    
                # Create quarterly aggregation
                quarterly_table = create_quarterly_aggregation(df_type, payment_type, reporting_year)
                tables[payment_type] = quarterly_table
            
            # Create overall summary table
            print(f"  Creating overall summary table")
            overall_table = create_quarterly_aggregation(df_prog, "All Payments", reporting_year)
            tables['All_Payments'] = overall_table
            
            return tables

        def create_quarterly_aggregation(df_type, payment_type_name, reporting_year):
            """
            Create quarterly aggregation table for a specific payment type
            - Amounts: Sum all v_amount_to_sum (including by call type/fund source)
            - Transactions: Count unique Pay Payment Key
            - VOBU/EFTA: Sum only EFTA and VOBU fund sources
            """
            
            # Create base aggregation structure
            agg_data = []
            
            # Get all quarters in the data
            quarters = sorted(df_type['Quarter'].unique())
            
            for quarter in quarters:
                df_q = df_type[df_type['Quarter'] == quarter].copy()
                
                # Get call types for this quarter (using Call_Type_Display)
                call_types = df_q['Call_Type_Display'].unique()
                
                quarter_row = {
                    'Quarter': str(quarter),
                    'Quarter_Short': f"{quarter.quarter}Q{quarter.year}",
                    'Year': quarter.year,
                    'Payment_Type': payment_type_name,
                    'Reporting_Year': reporting_year
                }
                
                # AMOUNTS: Sum all v_amount_to_sum by call type
                total_amount_all_types = 0
                vobu_efta_amount_all_types = 0
                
                for call_type in call_types:
                    df_call_type = df_q[df_q['Call_Type_Display'] == call_type]
                    
                    # Total amount for this call type
                    total_amount = df_call_type['v_amount_to_sum'].sum()
                    quarter_row[f'Total_Amount_{call_type}'] = total_amount
                    total_amount_all_types += total_amount
                    
                    # VOBU/EFTA amount: Only sum EFTA and VOBU fund sources
                    df_vobu_efta = df_call_type[df_call_type['Fund Source'].isin(['VOBU', 'EFTA'])]
                    vobu_efta_amount = df_vobu_efta['v_amount_to_sum'].sum()
                    quarter_row[f'VOBU_EFTA_Amount_{call_type}'] = vobu_efta_amount
                    vobu_efta_amount_all_types += vobu_efta_amount
                    
                    # TRANSACTIONS: Count unique Pay Payment Key for this call type
                    unique_transactions_call_type = df_call_type['Pay Payment Key'].nunique()
                    quarter_row[f'No_of_Transactions_{call_type}'] = unique_transactions_call_type
                
                # TRANSACTIONS: Count unique Pay Payment Key (deduplicated across all call types)
                unique_transactions = df_q['Pay Payment Key'].nunique()
                quarter_row['No_of_Transactions'] = unique_transactions
                
                # OVERALL TOTALS
                quarter_row['Total_Amount'] = total_amount_all_types
                quarter_row['VOBU_EFTA_Amount'] = vobu_efta_amount_all_types
                
                agg_data.append(quarter_row)
            
            # Convert to DataFrame
            df_result = pd.DataFrame(agg_data)
            
            # Add total row
            if len(df_result) > 0:
                total_row = create_total_row(df_type, df_result, payment_type_name, reporting_year)
                df_result = pd.concat([df_result, total_row], ignore_index=True)
            
            return df_result

        def create_total_row(df_type, df_result, payment_type_name, reporting_year):
            """
            Create total row for the aggregation table with VOBU/EFTA logic and transaction counts by call type
            """
            
            total_row = {
                'Quarter': 'Total',
                'Quarter_Short': 'Total',
                'Year': reporting_year,
                'Payment_Type': payment_type_name,
                'Reporting_Year': reporting_year
            }
            
            # Sum all amount columns (exclude Total row if it exists)
            df_data_only = df_result[df_result['Quarter'] != 'Total']
            
            # Sum individual call type amounts
            amount_cols = [col for col in df_result.columns if 'Amount' in col and col not in ['Total_Amount', 'VOBU_EFTA_Amount']]
            for col in amount_cols:
                total_row[col] = df_data_only[col].sum()
            
            # Calculate VOBU/EFTA total from original data (not summing quarterly totals to avoid double counting)
            df_vobu_efta = df_type[df_type['Fund Source'].isin(['VOBU', 'EFTA'])]
            total_row['VOBU_EFTA_Amount'] = df_vobu_efta['v_amount_to_sum'].sum()
            
            # Calculate transaction counts by call type from original data
            call_types = df_type['Call_Type_Display'].unique()
            for call_type in call_types:
                df_call_type = df_type[df_type['Call_Type_Display'] == call_type]
                total_row[f'No_of_Transactions_{call_type}'] = df_call_type['Pay Payment Key'].nunique()
            
            # Overall total amount
            total_row['Total_Amount'] = df_type['v_amount_to_sum'].sum()
            
            # Sum unique transactions across all quarters (deduplicated at total level)
            total_row['No_of_Transactions'] = df_type['Pay Payment Key'].nunique()
            
            return pd.DataFrame([total_row])

        def format_table_for_great_tables(df_table, payment_type, programme, repeat_quarter=True):
            """
            Format table for great_tables library - creates clean pandas DataFrame
            Structure exactly like Excel: Quarter | Metric | ADG | COG | POC | STG | SYG | Total
            
            Args:
                repeat_quarter (bool): If True, repeat quarter value in each row. If False, show only once per group.
                                    True is recommended for great_tables compatibility.
            """
            
            if len(df_table) == 0:
                return pd.DataFrame()
            
            # Separate data rows from total row
            df_data = df_table[df_table['Quarter'] != 'Total'].copy()
            df_total = df_table[df_table['Quarter'] == 'Total'].copy()
            
            if len(df_data) == 0:
                return pd.DataFrame()
            
            # Get unique quarters and call types from the data
            quarters = sorted(df_data['Quarter_Short'].unique())
            
            # Extract call type columns from the dataframe
            call_type_cols = [col for col in df_data.columns if col.startswith('Total_Amount_') and not col.endswith('Amount')]
            call_types = sorted([col.replace('Total_Amount_', '') for col in call_type_cols if col != 'Total_Amount'])
            
            print(f"  Formatting for great_tables - Call types: {call_types}, Quarters: {quarters}")
            print(f"  Quarter repeat mode: {repeat_quarter}")
            
            # Create the structure for great_tables - Quarter and Metric as separate columns
            table_data = []
            
            # === PROCESS EACH QUARTER ===
            for quarter in quarters:
                quarter_data = df_data[df_data['Quarter_Short'] == quarter]
                
                if len(quarter_data) == 0:
                    continue
                    
                # ROW 1: Total Amount for this quarter
                total_amount_row = {
                    'Quarter': quarter, 
                    'Metric': 'Total Amount'
                }
                
                for call_type in call_types:
                    amount_col = f'Total_Amount_{call_type}'
                    total_amount_row[call_type] = quarter_data[amount_col].iloc[0] if amount_col in quarter_data.columns else 0
                
                total_amount_row['Total'] = quarter_data['Total_Amount'].iloc[0]
                table_data.append(total_amount_row)
                
                # ROW 2: Out of Which VOBU/EFTA for this quarter
                vobu_efta_row = {
                    'Quarter': quarter if repeat_quarter else '', 
                    'Metric': 'Out of Which VOBU/EFTA'
                }
                
                for call_type in call_types:
                    vobu_efta_col = f'VOBU_EFTA_Amount_{call_type}'
                    vobu_efta_row[call_type] = quarter_data[vobu_efta_col].iloc[0] if vobu_efta_col in quarter_data.columns else 0
                
                vobu_efta_row['Total'] = quarter_data['VOBU_EFTA_Amount'].iloc[0]
                table_data.append(vobu_efta_row)
                
                # ROW 3: No of Transactions for this quarter
                transactions_row = {
                    'Quarter': quarter if repeat_quarter else '', 
                    'Metric': 'No of Transactions'
                }
                
                for call_type in call_types:
                    transactions_col = f'No_of_Transactions_{call_type}'
                    transactions_row[call_type] = quarter_data[transactions_col].iloc[0] if transactions_col in quarter_data.columns else 0
                
                transactions_row['Total'] = quarter_data['No_of_Transactions'].iloc[0]
                table_data.append(transactions_row)
            
            # === TOTAL ROWS (from df_total) ===
            if len(df_total) > 0:
                
                # TOTAL ROW 1: Total Amount
                total_amount_row = {
                    'Quarter': 'Total', 
                    'Metric': 'Total Amount'
                }
                
                for call_type in call_types:
                    amount_col = f'Total_Amount_{call_type}'
                    total_amount_row[call_type] = df_total[amount_col].iloc[0] if amount_col in df_total.columns else 0
                
                total_amount_row['Total'] = df_total['Total_Amount'].iloc[0]
                table_data.append(total_amount_row)
                
                # TOTAL ROW 2: Out of Which VOBU/EFTA
                total_vobu_efta_row = {
                    'Quarter': 'Total' if repeat_quarter else '', 
                    'Metric': 'Out of Which VOBU/EFTA'
                }
                
                for call_type in call_types:
                    vobu_efta_col = f'VOBU_EFTA_Amount_{call_type}'
                    total_vobu_efta_row[call_type] = df_total[vobu_efta_col].iloc[0] if vobu_efta_col in df_total.columns else 0
                
                total_vobu_efta_row['Total'] = df_total['VOBU_EFTA_Amount'].iloc[0]
                table_data.append(total_vobu_efta_row)
                
                # TOTAL ROW 3: No of Transactions
                total_transactions_row = {
                    'Quarter': 'Total' if repeat_quarter else '', 
                    'Metric': 'No of Transactions'
                }
                
                for call_type in call_types:
                    transactions_col = f'No_of_Transactions_{call_type}'
                    total_transactions_row[call_type] = df_total[transactions_col].iloc[0] if transactions_col in df_total.columns else 0
                
                total_transactions_row['Total'] = df_total['No_of_Transactions'].iloc[0]
                table_data.append(total_transactions_row)
            
            # Convert to DataFrame
            great_tables_df = pd.DataFrame(table_data)
            
            # Reorder columns: Quarter, Metric, then call types in alphabetical order, then Total
            column_order = ['Quarter', 'Metric'] + call_types + ['Total']
            great_tables_df = great_tables_df[column_order]
            
            return great_tables_df


        def format_quarterly_tables_for_great_tables(results):
            """
            Format the results for great_tables library - clean pandas DataFrames
            """
            
            if 'tables' not in results:
                return results
                
            formatted_results = {
                'metadata': results['metadata'],
                'great_tables': {}
            }
            
            for programme, tables in results['tables'].items():
                formatted_results['great_tables'][programme] = {}
                
                for payment_type, df_table in tables.items():
                    # Create great_tables format
                    gt_table = format_table_for_great_tables(df_table, payment_type, programme)
                    formatted_results['great_tables'][programme][payment_type] = gt_table
            
            return formatted_results

        # Main execution function with great_tables output
        def generate_all_quarterly_tables(df_paym, cutoff_date=None):
            """
            Main function to generate all quarterly payment tables for great_tables
            """
            
            print("Starting quarterly table generation for great_tables...")
            
            if cutoff_date is not None:
                print(f"Using provided cutoff date: {cutoff_date}")
            else:
                cutoff_date = pd.Timestamp.now()
                print(f"Using current date as cutoff: {cutoff_date}")
            
            # Generate tables with scope filtering
            results = create_quarterly_payment_tables(df_paym, cutoff_date)
            
            if results is None:
                return None
            
            # Format for great_tables
            formatted_results = format_quarterly_tables_for_great_tables(results)
            
            # Display summary
            print("\n=== GENERATION COMPLETE ===")
            print(f"Reporting for: {results['metadata']['reporting_year']}")
            print(f"Scope: {results['metadata']['scope_start']} to {results['metadata']['scope_end']}")
            print(f"VOBU/EFTA aggregation: Only EFTA and VOBU fund sources included")
            
            if 'tables' in results:
                for programme, tables in results['tables'].items():
                    print(f"\n{programme} Programme:")
                    for payment_type, table in tables.items():
                        data_rows = len(table[table['Quarter'] != 'Total']) if len(table) > 0 else 0
                        print(f"  - {payment_type}: {data_rows} quarters")
            
            return formatted_results

        # Updated utility functions for great_tables
        def get_great_table(formatted_results, programme, payment_type):
            """
            Get a specific table formatted for great_tables
            """
            try:
                return formatted_results['great_tables'][programme][payment_type]
            except KeyError:
                print(f"Table not found: {programme} - {payment_type}")
                available_programmes = list(formatted_results.get('great_tables', {}).keys())
                print(f"Available programmes: {available_programmes}")
                if programme in formatted_results.get('great_tables', {}):
                    available_payment_types = list(formatted_results['great_tables'][programme].keys())
                    print(f"Available payment types for {programme}: {available_payment_types}")
                return pd.DataFrame()

        def get_summary_table(formatted_results, programme):
            """
            Get the summary table that includes all payment types (including experts)
            """
            return get_great_table(formatted_results, programme, 'All_Payments')

        def create_comprehensive_summary_table(results, programme):
            """
            Create a comprehensive summary table showing all payment types in one view
            """
            
            if 'tables' not in results or programme not in results['tables']:
                print(f"No data found for programme: {programme}")
                return pd.DataFrame()
            
            programme_tables = results['tables'][programme]
            
            # Initialize summary data
            summary_data = []
            
            # Get all call types from any table
            all_call_types = set()
            for payment_type, table in programme_tables.items():
                if payment_type != 'All_Payments' and len(table) > 0:
                    total_row = table[table['Quarter'] == 'Total']
                    if len(total_row) > 0:
                        call_type_cols = [col for col in total_row.columns if col.startswith('Total_Amount_')]
                        call_types = [col.replace('Total_Amount_', '') for col in call_type_cols]
                        all_call_types.update(call_types)
            
            all_call_types = sorted(list(all_call_types))
            
            # Create rows for each payment type
            for payment_type, table in programme_tables.items():
                if payment_type == 'All_Payments':
                    continue  # Skip the existing all payments, we'll create our own
                    
                if len(table) == 0:
                    continue
                    
                total_row = table[table['Quarter'] == 'Total']
                if len(total_row) == 0:
                    continue
                    
                # === TOTAL AMOUNT ROW ===
                amount_row = {'Payment_Type': payment_type, 'Metric': 'Total Amount'}
                
                for call_type in all_call_types:
                    amount_col = f'Total_Amount_{call_type}'
                    amount_row[call_type] = total_row[amount_col].iloc[0] if amount_col in total_row.columns else 0
                
                amount_row['Total'] = total_row['Total_Amount'].iloc[0]
                summary_data.append(amount_row)
                
                # === VOBU/EFTA ROW ===
                vobu_efta_row = {'Payment_Type': payment_type, 'Metric': 'Out of Which VOBU/EFTA'}
                
                for call_type in all_call_types:
                    vobu_efta_col = f'VOBU_EFTA_Amount_{call_type}'
                    vobu_efta_row[call_type] = total_row[vobu_efta_col].iloc[0] if vobu_efta_col in total_row.columns else 0
                
                vobu_efta_row['Total'] = total_row['VOBU_EFTA_Amount'].iloc[0]
                summary_data.append(vobu_efta_row)
                
                # === TRANSACTIONS ROW ===
                transactions_row = {'Payment_Type': payment_type, 'Metric': 'No of Transactions'}
                
                for call_type in all_call_types:
                    transactions_col = f'No_of_Transactions_{call_type}'
                    transactions_row[call_type] = total_row[transactions_col].iloc[0] if transactions_col in total_row.columns else 0
                
                transactions_row['Total'] = total_row['No_of_Transactions'].iloc[0]
                summary_data.append(transactions_row)
            
            # === CREATE OVERALL TOTALS ===
            if summary_data:
                # Get the All_Payments table data
                all_payments_table = programme_tables.get('All_Payments', pd.DataFrame())
                
                if len(all_payments_table) > 0:
                    total_row = all_payments_table[all_payments_table['Quarter'] == 'Total']
                    
                    if len(total_row) > 0:
                        # TOTAL AMOUNTS ACROSS ALL PAYMENT TYPES
                        total_amount_row = {'Payment_Type': 'TOTAL ALL TYPES', 'Metric': 'Total Amount'}
                        for call_type in all_call_types:
                            amount_col = f'Total_Amount_{call_type}'
                            total_amount_row[call_type] = total_row[amount_col].iloc[0] if amount_col in total_row.columns else 0
                        total_amount_row['Total'] = total_row['Total_Amount'].iloc[0]
                        summary_data.append(total_amount_row)
                        
                        # TOTAL VOBU/EFTA ACROSS ALL PAYMENT TYPES
                        total_vobu_efta_row = {'Payment_Type': 'TOTAL ALL TYPES', 'Metric': 'Out of Which VOBU/EFTA'}
                        for call_type in all_call_types:
                            vobu_efta_col = f'VOBU_EFTA_Amount_{call_type}'
                            total_vobu_efta_row[call_type] = total_row[vobu_efta_col].iloc[0] if vobu_efta_col in total_row.columns else 0
                        total_vobu_efta_row['Total'] = total_row['VOBU_EFTA_Amount'].iloc[0]
                        summary_data.append(total_vobu_efta_row)
                        
                        # TOTAL TRANSACTIONS ACROSS ALL PAYMENT TYPES (deduplicated)
                        total_transactions_row = {'Payment_Type': 'TOTAL ALL TYPES', 'Metric': 'No of Transactions'}
                        for call_type in all_call_types:
                            transactions_col = f'No_of_Transactions_{call_type}'
                            total_transactions_row[call_type] = total_row[transactions_col].iloc[0] if transactions_col in total_row.columns else 0
                        total_transactions_row['Total'] = total_row['No_of_Transactions'].iloc[0]
                        summary_data.append(total_transactions_row)
            
            # Convert to DataFrame
            if summary_data:
                summary_df = pd.DataFrame(summary_data)
                
                # Reorder columns
                column_order = ['Payment_Type', 'Metric'] + all_call_types + ['Total']
                summary_df = summary_df[column_order]
                
                return summary_df
            else:
                return pd.DataFrame()

        def create_payment_type_comparison_table(results, programme):
            """
            Create a table showing just the totals for each payment type for easy comparison
            """
            
            if 'tables' not in results or programme not in results['tables']:
                return pd.DataFrame()
            
            programme_tables = results['tables'][programme]
            comparison_data = []
            
            for payment_type, table in programme_tables.items():
                if payment_type == 'All_Payments':
                    continue
                    
                if len(table) == 0:
                    continue
                    
                total_row = table[table['Quarter'] == 'Total']
                if len(total_row) == 0:
                    continue
                
                comparison_row = {
                    'Payment_Type': payment_type,
                    'Total_Amount': total_row['Total_Amount'].iloc[0],
                    'VOBU_EFTA_Amount': total_row['VOBU_EFTA_Amount'].iloc[0],
                    'No_of_Transactions': total_row['No_of_Transactions'].iloc[0]
                }
                
                comparison_data.append(comparison_row)
            
            # Add overall total
            if comparison_data:
                all_payments_table = programme_tables.get('All_Payments', pd.DataFrame())
                if len(all_payments_table) > 0:
                    total_row = all_payments_table[all_payments_table['Quarter'] == 'Total']
                    if len(total_row) > 0:
                        overall_row = {
                            'Payment_Type': 'TOTAL ALL TYPES',
                            'Total_Amount': total_row['Total_Amount'].iloc[0],
                            'VOBU_EFTA_Amount': total_row['VOBU_EFTA_Amount'].iloc[0],
                            'No_of_Transactions': total_row['No_of_Transactions'].iloc[0]
                        }
                        comparison_data.append(overall_row)
            
            return pd.DataFrame(comparison_data)

        def list_available_tables(formatted_results):
            """
            List all available tables including summary options
            """
            print("=== AVAILABLE TABLES FOR GREAT_TABLES ===")
            
            if 'tables' not in formatted_results:
                print("No tables found")
                return
            
            for programme, tables in formatted_results['tables'].items():
                print(f"\n{programme} Programme:")
                for payment_type, df_table in tables.items():
                    rows, cols = df_table.shape
                    if payment_type == 'All_Payments':
                        print(f"  - {payment_type}: {rows} rows x {cols} columns ⭐ SUMMARY TABLE")
                    else:
                        print(f"  - {payment_type}: {rows} rows x {cols} columns")
                
                print(f"\n  📊 Access functions available:")
                print(f"    # Individual payment types:")
                print(f"    get_great_table(results, '{programme}', 'Pre-financing', repeat_quarter=True)  # Recommended")
                print(f"    get_great_table_repeated(results, '{programme}', 'Pre-financing')  # Same as above")
                print(f"    get_great_table_grouped(results, '{programme}', 'Pre-financing')   # Excel visual style")
                print(f"    ")
                print(f"    # Summary tables:")
                print(f"    get_summary_table(results, '{programme}', repeat_quarter=True)  # All payment types")
                print(f"    create_comprehensive_summary_table(results, '{programme}')       # Alternative")
                print(f"    create_payment_type_comparison_table(results, '{programme}')     # Quick comparison")

        def get_all_programme_tables(formatted_results, programme):
            """
            Get all tables for a specific programme as a dictionary
            """
            try:
                return formatted_results['tables'][programme]
            except KeyError:
                print(f"Programme not found: {programme}")
                available = list(formatted_results.get('tables', {}).keys())
                print(f"Available programmes: {available}")
                return {}

        def combine_payment_types_table(formatted_results, programme):
            """
            Combine all payment types for a programme into one large table
            """
            programme_tables = get_all_programme_tables(formatted_results, programme)
            
            if not programme_tables:
                return pd.DataFrame()
            
            combined_tables = []
            
            for payment_type, df_table in programme_tables.items():
                if len(df_table) > 0:
                    # Add a separator row if not the first table
                    if len(combined_tables) > 0:
                        separator_row = pd.DataFrame([{
                            'Metric': f'--- {payment_type} ---',
                            **{col: '' for col in df_table.columns if col != 'Metric'}
                        }])
                        combined_tables.append(separator_row)
                    
                    combined_tables.append(df_table)
            
            if combined_tables:
                return pd.concat(combined_tables, ignore_index=True)
            else:
                return pd.DataFrame()
            

        # MAIN EXECUTION WITH ERROR HANDLING
        print("🚀 Starting quarterly tables generation...")
        try:
            quarterly_tables = generate_all_quarterly_tables(df_paym, cutoff)
            if quarterly_tables is None:
                return False, "Failed to generate quarterly tables - no data returned", None
                
            print("✅ Quarterly tables generated successfully")
            
        except Exception as e:
            return False, f"Error generating quarterly tables: {str(e)}", None
        
        # Step 2: Check if we have any data to work with
        try:
            if 'great_tables' not in quarterly_tables:
                return False, "No great_tables found in quarterly tables results", None
                
            programs_found = list(quarterly_tables.get('great_tables', {}).keys())
            if not programs_found:
                return False, "No programs found in quarterly tables", None
                
            print(f"✅ Found data for programs: {programs_found}")
            
        except Exception as e:
            return False, f"Error validating quarterly tables structure: {str(e)}", None

        #### TABLE FORMATTING ####
        def format_table_clean(df,
                                title="HEU grants - all payments, number of transactions, and corresponding amounts",
                                rowname_col="Metric",
                                groupname_col="Quarter",
                                programme_name="HEU",
                                metric_col="Metric",
                                table_colors=None
                                ):
            """
                Clean, single-pass HEU table formatter that applies all styling consistently

                Args:
                    df: DataFrame with data to format
                    title: Table title
                    rowname_col: Column name for row names (e.g. "Quarter")
                    groupname_col: Column name for groups (e.g. "Year")
                    programme_name: Program name for spanner label (e.g. "HEU")
                    metric_col: Metric column name (e.g. "Metric")
                    table_colors: Dictionary with custom colors
            """

            # Default colors if not provided
            if table_colors is None:
                table_colors = {}
            
            BLUE = table_colors.get("BLUE", "#004A99")
            LIGHT_BLUE = table_colors.get("LIGHT_BLUE", "#d6e6f4")
            DARK_BLUE = table_colors.get("DARK_BLUE", "#01244B") 
            SUB_TOTAL_BACKGROUND = table_colors.get("subtotal_background_color", "#E6E6FA")
            
            # Identify columns and rows for formatting
            exclude_cols = [rowname_col, groupname_col, metric_col]
            spanner_cols = [col for col in df.columns if col not in exclude_cols]
            num_data_columns = len(spanner_cols)

            # Smart width calculation based on column count
            if num_data_columns == 1:
                # Single column: compact table
                stub_width = 180
                column_width = 140
                base_width = stub_width + column_width + 100  # Extra padding for single column
                
            elif num_data_columns == 2:
                # Two columns: still compact but balanced
                stub_width = 160
                column_width = 130
                base_width = stub_width + (num_data_columns * column_width) + 80
                
            elif num_data_columns <= 4:
                # Few columns: moderate width
                stub_width = 140
                column_width = 120
                base_width = stub_width + (num_data_columns * column_width) + 60
                
            elif num_data_columns <= 7:
                # Standard columns: your current formula works well
                stub_width = 140
                column_width = 110
                base_width = stub_width + (num_data_columns * column_width) + 50
                
            else:
                # Many columns: slightly narrower columns to fit
                stub_width = 130
                column_width = 100
                base_width = stub_width + (num_data_columns * column_width) + 40
            
            # Apply reasonable min/max bounds
            min_width = 400 if num_data_columns == 1 else 500
            max_width = 1600
            
            optimal_width = max(min_width, min(base_width, max_width))

            
            # Identify special rows
            total_rows_indices = df.index[df[rowname_col].astype(str).str.contains('VOBU/EFTA', case=False, na=False)].tolist()
            amount_mask = df[metric_col].str.contains('Total Amount|Out of Which', case=False, na=False)
            amount_rows = df[amount_mask].index.tolist()
            transaction_mask = df[metric_col].str.contains('No of Transactions', case=False, na=False)
            transaction_rows = df[transaction_mask].index.tolist()

            
            # Create and format table in ONE clean chain
            tbl = (
                GT(df, 
                rowname_col=rowname_col, 
                groupname_col=groupname_col)
                
                # 1. Basic setup
                .tab_header(title=md(title))
                .tab_spanner(label=programme_name, columns=spanner_cols)
                .tab_stubhead(label="Quarter")
        
                
                # 2. Table options
                .tab_options(
                    table_font_size="12px",
                    table_width="100%",
                    table_font_color=DARK_BLUE,
                )

                #3.Apply Theme 
                .opt_stylize(style=3, color='blue')
                
                #4. Customize Theme 
                .tab_style(
                    style=[
                        style.text(color=DARK_BLUE, weight="bold", font='Arial'),
                    ],
                    locations=[
                        loc.body(rows=total_rows_indices),
                        loc.stub(rows=total_rows_indices)
                    ]
                )
                .opt_table_font(font="Arial")
                .tab_options(
                        table_background_color="white",
                        heading_background_color="white",
                        #  stub_background_color="#0076BA",
                        table_font_size='small',
                        row_group_font_size='small',
                        row_group_font_weight='bold',
                        table_width='800px',
                )
                
                # 5. Currency formatting
                .fmt_currency(
                    columns=spanner_cols,
                    rows=amount_rows,
                    currency="EUR",
                    decimals=2,
                    use_seps=True
                )
                
                # 6. Integer formatting
                .fmt_integer(
                    columns=spanner_cols,
                    rows=transaction_rows,
                    use_seps=True
                )
        
            )
        
            return tbl
        
        # IMPROVED VERSION OF YOUR CODE WITH SUGGESTIONS

        def generate_all_program_tables(quarterly_tables, db_path, report, table_colors, logger):
            """
            Generate and save all program tables with improved error handling and cleaner logic
            """
            
            # Configuration - easier to maintain
            PROGRAMS = ['HEU', 'H2020']
            
            # Payment types with corresponding titles - eliminates if/elif chain
            PAYMENT_TYPES = {
                'All_Payments': 'All payments: number of transactions, and corresponding amounts',
                'Pre-financing': 'Pre-financing: number of transactions, and corresponding amounts', 
                'Interim Payments': 'Interim Payments: number of transactions, and corresponding amounts',
                'Final Payments': 'Final Payments: number of transactions, and corresponding amounts',
                'Experts and Support': 'Experts and Support: number of transactions, and corresponding amounts'
            }
            
            # Track success/failure statistics
            results = {
                'successful': [],
                'failed': [],
                'skipped': []
            }
            
            logger.info(f"🚀 Starting table generation for {len(PROGRAMS)} programs × {len(PAYMENT_TYPES)} payment types")
            
            for program in PROGRAMS:
                logger.info(f"📊 Processing program: {program}")
                
                for pay_type, title in PAYMENT_TYPES.items():
                    var_name = f'{program}_{pay_type}'

                    if pay_type == 'Interim Payments':
                        anchor_name = f'{program}_Interim_Payments'
                    elif pay_type == 'Final Payments':
                        anchor_name = f'{program}_Final_Payments'
                    elif pay_type == 'Pre-financing':
                        anchor_name = f'{program}_Pre_Financing'
                    else:
                        anchor_name = f'{program}_{pay_type}'            
                    
                    try:
                        # 1. Get table data
                        logger.debug(f"Fetching data for {var_name}")
                        table_data = get_great_table(quarterly_tables, program, pay_type)
                        
                        # Fixed DataFrame validation - avoid ambiguous truth value
                        if table_data is None or (isinstance(table_data, pd.DataFrame) and table_data.empty):
                            logger.warning(f"⚠️  No data found for {var_name} - skipping")
                            results['skipped'].append(var_name)
                            continue
                        
                        # Additional validation - check if DataFrame has the expected structure
                        if isinstance(table_data, pd.DataFrame):
                            required_columns = ["Metric", "Quarter"]  # Add other required columns as needed
                            missing_columns = [col for col in required_columns if col not in table_data.columns]
                            if missing_columns:
                                logger.error(f"❌ Missing required columns in {var_name}: {missing_columns}")
                                results['failed'].append(f"{var_name} (missing columns)")
                                continue
                        
                        # 2. Format table
                        logger.debug(f"Formatting table for {var_name}")
                        formatted_table = format_table_clean(  # Using your improved function
                            df=table_data,
                            title=f"{program} grants - {title}",  # More descriptive title
                            rowname_col="Metric",
                            groupname_col="Quarter",
                            programme_name=program,
                            metric_col="Metric",
                            table_colors=table_colors
                        )
                        
                        # 3. Save to database
                        logger.debug(f"Saving {var_name} to database")
                        insert_variable(
                            report=report,
                            module="PaymentsModule",
                            var=var_name,
                            value=table_data.to_dict() if isinstance(table_data, pd.DataFrame) else table_data,
                            db_path=db_path,
                            anchor=anchor_name,
                            gt_table=formatted_table
                        )
                        
                        logger.info(f"✅ Successfully processed {var_name}")
                        results['successful'].append(var_name)
                        
                    except KeyError as e:
                        logger.error(f"❌ Data source not found for {var_name}: {str(e)}")
                        results['failed'].append(f"{var_name} (data not found)")
                        
                    except ValueError as e:
                        logger.error(f"❌ Data validation error for {var_name}: {str(e)}")
                        results['failed'].append(f"{var_name} (validation error)")
                        
                    except Exception as e:
                        logger.error(f"❌ Unexpected error processing {var_name}: {str(e)}")
                        results['failed'].append(f"{var_name} (unexpected error)")
            
            # Summary report
            logger.info("\n" + "="*50)
            logger.info("📈 TABLE GENERATION SUMMARY")
            logger.info("="*50)
            logger.info(f"✅ Successful: {len(results['successful'])}")
            logger.info(f"❌ Failed: {len(results['failed'])}")
            logger.info(f"⚠️  Skipped: {len(results['skipped'])}")
            
            if results['successful']:
                logger.info(f"\n✅ Successful tables: {', '.join(results['successful'])}")
            
            if results['failed']:
                logger.warning(f"\n❌ Failed tables: {', '.join(results['failed'])}")
            
            if results['skipped']:
                logger.warning(f"\n⚠️  Skipped tables: {', '.join(results['skipped'])}")
            
            return results


        def check_available_data(quarterly_tables, logger):
            """
            Check what data is actually available for each program and payment type
            """
            PROGRAMS = ['HEU', 'H2020']
            PAYMENT_TYPES = ['All_Payments', 'Pre-financing', 'Interim Payments', 'Final Payments']
            
            logger.info("🔍 CHECKING AVAILABLE DATA:")
            logger.info("="*40)
            
            availability_map = {}
            
            for program in PROGRAMS:
                logger.info(f"\n📊 Program: {program}")
                available_types = []
                
                for pay_type in PAYMENT_TYPES:
                    try:
                        table_data = get_great_table(quarterly_tables, program, pay_type)
                        if table_data is not None and isinstance(table_data, pd.DataFrame) and not table_data.empty:
                            logger.info(f"  ✅ {pay_type}: Available ({len(table_data)} rows)")
                            available_types.append(pay_type)
                        else:
                            logger.warning(f"  ❌ {pay_type}: Not available or empty")
                    except Exception as e:
                        logger.error(f"  ❌ {pay_type}: Error - {str(e)}")
                
                availability_map[program] = available_types
                logger.info(f"  📋 Available for {program}: {available_types}")
            
            return availability_map


        # ALTERNATIVE: More modular approach
        def process_single_table(quarterly_tables, program, pay_type, title, 
                                db_path, report, table_colors, logger):
            """
            Process a single program/payment type combination
            Returns: (success: bool, message: str)
            """
            var_name = f'{program}_{pay_type}'

            if pay_type == 'Interim Payments':
                anchor_name = f'{program}_Interim_Payments'
            elif pay_type == 'Final Payments':
                anchor_name = f'{program}_Final_Payments'
            elif pay_type == 'Pre-financing':
                anchor_name = f'{program}_Pre_Financing'
            else:
                anchor_name = f'{program}_{pay_type}'

            
            try:
                # Get data
                table_data = get_great_table(quarterly_tables, program, pay_type)
                
                if table_data is None or (isinstance(table_data, pd.DataFrame) and table_data.empty):
                    return False, f"No data available for {var_name}"
                
                # Format table
                formatted_table = format_table_clean(
                    df=table_data,
                    title=f"{program} grants - {title}",
                    rowname_col="Metric",
                    groupname_col="Quarter",
                    programme_name=program,
                    metric_col="Metric",
                    table_colors=table_colors
                )
                
                # Save to database
                insert_variable(
                    report=report,
                    module="PaymentsModule",
                    var=var_name,
                    value=table_data.to_dict() if isinstance(table_data, pd.DataFrame) else table_data,
                    db_path=db_path,
                    anchor=anchor_name,
                    gt_table=formatted_table
                )
                
                return True, f"Successfully processed {var_name}"
                
            except Exception as e:
                return False, f"Error processing {var_name}: {str(e)}"

        #generate_all_program_tables(quarterly_tables, db_path, report, table_colors, logger)
        # Step 3: Generate and save formatted tables
        try:
            # Create a simple logger if not provided
            class SimpleLogger:
                def info(self, msg): print(f"INFO: {msg}")
                def warning(self, msg): print(f"WARNING: {msg}")
                def error(self, msg): print(f"ERROR: {msg}")
                def debug(self, msg): print(f"DEBUG: {msg}")
            
            logger = SimpleLogger()
            
            # Generate all program tables
            generation_results = generate_all_program_tables(
                quarterly_tables, db_path, report, table_colors, logger
            )
            
            # Check results
            if generation_results is None:
                return False, "Table generation returned no results", None
                
            successful_count = len(generation_results.get('successful', []))
            failed_count = len(generation_results.get('failed', []))
            skipped_count = len(generation_results.get('skipped', []))
            
            print(f"📊 Generation Summary:")
            print(f"   ✅ Successful: {successful_count}")
            print(f"   ❌ Failed: {failed_count}")
            print(f"   ⚠️  Skipped: {skipped_count}")
            
            # Determine overall success
            if successful_count > 0 and failed_count == 0:
                success_message = f"All quarterly tables generated successfully! " \
                                f"Processed {successful_count} tables"
                if skipped_count > 0:
                    success_message += f" (skipped {skipped_count} empty tables)"
                    
                return True, success_message, {
                    'quarterly_tables': quarterly_tables,
                    'generation_results': generation_results,
                    'summary': {
                        'successful': successful_count,
                        'failed': failed_count,
                        'skipped': skipped_count
                    }
                }
                
            elif successful_count > 0 and failed_count > 0:
                warning_message = f"Quarterly tables partially completed. " \
                                f"Successful: {successful_count}, Failed: {failed_count}"
                if skipped_count > 0:
                    warning_message += f", Skipped: {skipped_count}"
                    
                return True, warning_message, {
                    'quarterly_tables': quarterly_tables,
                    'generation_results': generation_results,
                    'summary': {
                        'successful': successful_count,
                        'failed': failed_count,
                        'skipped': skipped_count
                    }
                }
                
            else:
                error_message = f"Quarterly table generation failed. " \
                              f"No tables were successfully created. " \
                              f"Failed: {failed_count}, Skipped: {skipped_count}"
                return False, error_message, generation_results
                
        except Exception as e:
            return False, f"Error during table generation and formatting: {str(e)}", None

    except Exception as e:
        # Catch any unexpected errors
        error_message = f"Unexpected error in quarterly_tables_generation_main: {str(e)}"
        print(f"❌ {error_message}")
        return False, error_message, None
        
    finally:
        print("🏁 Quarterly tables generation process completed")


# ──────────────────────────────────────────────────────────────
# PAYMENTS TABLES : 2. TTP SUMMARY And OverView TTP
# ──────────────────────────────────────────────────────────────
def generate_ttp_summary_overview (df_paym, cutoff, db_path, report, table_colors, report_params):
        
    try:
        # ═══════════════════════════════════════════════════════════════════
        # INPUT VALIDATION
        # ═══════════════════════════════════════════════════════════════════
        
        if df_paym is None or df_paym.empty:
            return False, "Input DataFrame (df_paym) is empty or None", None
            
        if cutoff is None:
            return False, "Cutoff date is required", None
            
        if db_path is None:
            return False, "Database path is required", None
            
        if report is None:
            return False, "Report parameter is required", None
            
        if report_params is None:
            return False, "Report parameters are required", None

        print("🚀 Starting TTP tables generation...")

        # ═══════════════════════════════════════════════════════════════════
        # DATA PREPARATION AND VALIDATION
        # ═══════════════════════════════════════════════════════════════════

        try:
            vars_all = fetch_vars_for_report(report, db_path)
            if not vars_all:
                return False, "Failed to fetch variables from database", None
            
            heu_vars_data = vars_all.get('table_2a_HE')
            h2020_vars_data = vars_all.get('table_2a_H2020')

            # Validate required data exists
            if not heu_vars_data and not h2020_vars_data:
                return False, "No HEU or H2020 data found in database variables", None

            heu_total_appropriations = next(
                (item['Available_Payment_Appropriations'] for item in heu_vars_data 
                if item['Budget Address Type'] == 'Total'), 
                None
            )if heu_vars_data else None

            heu_total_appropriations_expt = next(
                (item['Available_Payment_Appropriations'] for item in heu_vars_data 
                if item['Budget Address Type'] == 'Experts'), 
                None
            )if heu_vars_data else None

            h2020_total_appropriations = next(
                (item['Available_Payment_Appropriations'] for item in h2020_vars_data 
                if item['Budget Address Type'] == 'Total'), 
                None
            )if h2020_vars_data else None

            TTP_gross = report_params.get("TTP_GROSS_HISTORY")
            if not TTP_gross:
                return False, "TTP_GROSS_HISTORY not found in report parameters", None
            TTP_gross_H2020 = TTP_gross.get('H2020')

            print("✅ Data preparation completed successfully")

        except Exception as e:
            return False, f"Error during data preparation: {str(e)}", None

    
        # =============================================================================
        # CLEAN TTP CALCULATION FUNCTIONS
        # =============================================================================

        def calculate_current_ttp_metrics(df_paym, cutoff):
            """
            Calculate current TTP metrics from df_paym data
            """
            # Filter data up to cutoff and deduplicate by Pay Payment Key
            quarter_dates = get_scope_start_end(cutoff=cutoff)
            last_valid_date = quarter_dates[1]

            df_filtered = df_paym[
                df_paym['Pay Document Date (dd/mm/yyyy)'] <= last_valid_date
            ].copy()
            df_unique = df_filtered.drop_duplicates(subset=['Pay Payment Key']).copy()
            
            # Convert to numeric
            df_unique['v_TTP_NET'] = pd.to_numeric(df_unique['v_TTP_NET'], errors='coerce')
            df_unique['v_TTP_GROSS'] = pd.to_numeric(df_unique['v_TTP_GROSS'], errors='coerce')
            df_unique['v_payment_in_time'] = pd.to_numeric(df_unique['v_payment_in_time'], errors='coerce')

            # Filter out negative TTP_NET values
            df_unique = df_unique[df_unique['v_TTP_NET'] >= 0]
            
            results = {}
            
            # Calculate by Programme and Payment Type
            for programme in ['H2020', 'HEU']:
                prog_data = df_unique[df_unique['Programme'] == programme]
                if len(prog_data) == 0:
                    continue
                    
                results[programme] = {}
                
                # Overall programme metrics
                prog_valid = prog_data[prog_data['v_payment_in_time'].notna()]
                results[programme]['overall'] = {
                    'avg_ttp_net': prog_data['v_TTP_NET'].mean(),
                    'avg_ttp_gross': prog_data['v_TTP_GROSS'].mean(),
                    'on_time_pct': prog_data['v_payment_in_time'].sum() / len(prog_valid) if len(prog_valid) > 0 else 0
                }
                
                # By payment type - using correct short form values from v_payment_type
                payment_types = ['IP', 'FP', 'EXPERTS', 'PF']  # Short form values
                
                for payment_type in payment_types:
                    pt_data = prog_data[prog_data['v_payment_type'] == payment_type]
                    if len(pt_data) > 0:
                        pt_valid = pt_data[pt_data['v_payment_in_time'].notna()]
                        results[programme][payment_type] = {
                            'avg_ttp_net': pt_data['v_TTP_NET'].mean(),
                            'avg_ttp_gross': pt_data['v_TTP_GROSS'].mean(),
                            'on_time_pct': pt_data['v_payment_in_time'].sum() / len(pt_valid) if len(pt_valid) > 0 else 0
                        }
            
            # Overall total
            total_valid = df_unique[df_unique['v_payment_in_time'].notna()]
            results['TOTAL'] = {
                'avg_ttp_net': df_unique['v_TTP_NET'].mean(),
                'avg_ttp_gross': df_unique['v_TTP_GROSS'].mean(),
                'on_time_pct': df_unique['v_payment_in_time'].sum() / len(total_valid) if len(total_valid) > 0 else 0
            }
            
            return results

        def load_historical_ttp_data(report_name='Quarterly_Report', db_path="database/reporting.db"):
            """
            Load historical TTP data from database
            """
            DB_PATH = Path(db_path)
            report_params = load_report_params(report_name=report_name, db_path=DB_PATH)
            
            return {
                "TTP_NET_HISTORY": report_params.get("TTP_NET_HISTORY"),
                "TTP_GROSS_HISTORY": report_params.get("TTP_GROSS_HISTORY"),
                "PAYMENTS_ON_TIME_HISTORY": report_params.get("PAYMENTS_ON_TIME_HISTORY")
            }

        def create_ttp_comparison_table(df_paym, cutoff, historical_data):
            """
            Create TTP comparison table matching the image structure
            """
            # Calculate current metrics
            current_metrics = calculate_current_ttp_metrics(df_paym, cutoff)
            
            # Determine labels based on cutoff
            cutoff_date = pd.to_datetime(cutoff)
            current_year = cutoff_date.year
            current_label = f"{current_year}-YTD"
            historical_label = f"Dec {current_year - 1}"
            
            # Build comparison data
            comparison_data = []
            
            # H2020 section
            h2020_current = current_metrics.get('H2020', {})
            
            # H2020 - Interim Payments (IP)
            current_ip = h2020_current.get('IP', {'avg_ttp_net': 0, 'avg_ttp_gross': 0, 'on_time_pct': 0})
            comparison_data.append({
                'Type of Payments': 'Interim Payments',
                f'Average Net Time to Pay (in days) {current_label}': round(current_ip['avg_ttp_net'], 1),
                f'Average Net Time to Pay (in days) {historical_label}': historical_data["TTP_NET_HISTORY"]["H2020"].get("IP", "n.a"),
                f'Average Gross Time to Pay (in days) {current_label}': round(current_ip['avg_ttp_gross'], 1),
                f'Average Gross Time to Pay (in days) {historical_label}': historical_data["TTP_GROSS_HISTORY"]["H2020"].get("IP", "n.a"),
                f'Target Paid on Time - Contractually {current_label}': f"{current_ip['on_time_pct']*100:.2f}%",
                f'Target Paid on Time - Contractually {historical_label}': f"{historical_data['PAYMENTS_ON_TIME_HISTORY']['H2020'].get('IP', 0)*100:.2f}%"
            })
            
            # H2020 - Final Payments (FP)
            current_fp = h2020_current.get('FP', {'avg_ttp_net': 0, 'avg_ttp_gross': 0, 'on_time_pct': 0})
            comparison_data.append({
                'Type of Payments': 'Final Payments',
                f'Average Net Time to Pay (in days) {current_label}': round(current_fp['avg_ttp_net'], 1),
                f'Average Net Time to Pay (in days) {historical_label}': historical_data["TTP_NET_HISTORY"]["H2020"].get("FP", "n.a"),
                f'Average Gross Time to Pay (in days) {current_label}': round(current_fp['avg_ttp_gross'], 1),
                f'Average Gross Time to Pay (in days) {historical_label}': historical_data["TTP_GROSS_HISTORY"]["H2020"].get("FP", "n.a"),
                f'Target Paid on Time - Contractually {current_label}': f"{current_fp['on_time_pct']*100:.2f}%",
                f'Target Paid on Time - Contractually {historical_label}': f"{historical_data['PAYMENTS_ON_TIME_HISTORY']['H2020'].get('FP', 0)*100:.2f}%"
            })
            
            # H2020 - Experts Payments (EXPERTS)
            current_exp = h2020_current.get('EXPERTS', {'avg_ttp_net': 0, 'avg_ttp_gross': 0, 'on_time_pct': 0})
            comparison_data.append({
                'Type of Payments': 'Experts Payments',
                f'Average Net Time to Pay (in days) {current_label}': round(current_exp['avg_ttp_net'], 1),
                f'Average Net Time to Pay (in days) {historical_label}': historical_data["TTP_NET_HISTORY"]["H2020"].get("Experts", "n.a"),
                f'Average Gross Time to Pay (in days) {current_label}': round(current_exp['avg_ttp_gross'], 1),
                f'Average Gross Time to Pay (in days) {historical_label}': historical_data["TTP_GROSS_HISTORY"]["H2020"].get("Experts", "n.a"),
                f'Target Paid on Time - Contractually {current_label}': f"{current_exp['on_time_pct']*100:.2f}%",
                f'Target Paid on Time - Contractually {historical_label}': f"{historical_data['PAYMENTS_ON_TIME_HISTORY']['H2020'].get('Experts', 0)*100:.2f}%"
            })
            
            # H2020 - Overall
            current_h2020 = h2020_current.get('overall', {'avg_ttp_net': 0, 'avg_ttp_gross': 0, 'on_time_pct': 0})
            comparison_data.append({
                'Type of Payments': 'H2020',
                f'Average Net Time to Pay (in days) {current_label}': round(current_h2020['avg_ttp_net'], 1),
                f'Average Net Time to Pay (in days) {historical_label}': historical_data["TTP_NET_HISTORY"]["H2020"].get("H2020", "n.a"),
                f'Average Gross Time to Pay (in days) {current_label}': round(current_h2020['avg_ttp_gross'], 1),
                f'Average Gross Time to Pay (in days) {historical_label}': historical_data["TTP_GROSS_HISTORY"]["H2020"].get("H2020", "n.a"),
                f'Target Paid on Time - Contractually {current_label}': f"{current_h2020['on_time_pct']*100:.2f}%",
                f'Target Paid on Time - Contractually {historical_label}': f"{historical_data['PAYMENTS_ON_TIME_HISTORY']['H2020'].get('H2020', 0)*100:.2f}%"
            })
            
            # HEU section
            heu_current = current_metrics.get('HEU', {})
            
            # HEU - Prefinancing Payments (PF)
            current_pf = heu_current.get('PF', {'avg_ttp_net': 0, 'avg_ttp_gross': 0, 'on_time_pct': 0})
            comparison_data.append({
                'Type of Payments': 'Prefinancing Payments',
                f'Average Net Time to Pay (in days) {current_label}': round(current_pf['avg_ttp_net'], 1),
                f'Average Net Time to Pay (in days) {historical_label}': historical_data["TTP_NET_HISTORY"]["HEU"].get("PF", "n.a"),
                f'Average Gross Time to Pay (in days) {current_label}': round(current_pf['avg_ttp_gross'], 1),
                f'Average Gross Time to Pay (in days) {historical_label}': historical_data["TTP_GROSS_HISTORY"]["HEU"].get("PF", "n.a"),
                f'Target Paid on Time - Contractually {current_label}': f"{current_pf['on_time_pct']*100:.2f}%",
                f'Target Paid on Time - Contractually {historical_label}': f"{historical_data['PAYMENTS_ON_TIME_HISTORY']['HEU'].get('PF', 0)*100:.2f}%"
            })
            
            # HEU - Interim Payments (IP)
            current_ip_heu = heu_current.get('IP', {'avg_ttp_net': 0, 'avg_ttp_gross': 0, 'on_time_pct': 0})
            comparison_data.append({
                'Type of Payments': 'Interim Payments',
                f'Average Net Time to Pay (in days) {current_label}': round(current_ip_heu['avg_ttp_net'], 1),
                f'Average Net Time to Pay (in days) {historical_label}': historical_data["TTP_NET_HISTORY"]["HEU"].get("IP", "n.a"),
                f'Average Gross Time to Pay (in days) {current_label}': round(current_ip_heu['avg_ttp_gross'], 1),
                f'Average Gross Time to Pay (in days) {historical_label}': historical_data["TTP_GROSS_HISTORY"]["HEU"].get("IP", "n.a"),
                f'Target Paid on Time - Contractually {current_label}': f"{current_ip_heu['on_time_pct']*100:.2f}%",
                f'Target Paid on Time - Contractually {historical_label}': f"{historical_data['PAYMENTS_ON_TIME_HISTORY']['HEU'].get('IP', 0)*100:.2f}%"
            })
            
            # HEU - Final Payments (FP)
            current_fp_heu = heu_current.get('FP', {'avg_ttp_net': 0, 'avg_ttp_gross': 0, 'on_time_pct': 0})
            comparison_data.append({
                'Type of Payments': 'Final Payments',
                f'Average Net Time to Pay (in days) {current_label}': round(current_fp_heu['avg_ttp_net'], 1),
                f'Average Net Time to Pay (in days) {historical_label}': historical_data["TTP_NET_HISTORY"]["HEU"].get("FP", "n.a"),
                f'Average Gross Time to Pay (in days) {current_label}': round(current_fp_heu['avg_ttp_gross'], 1),
                f'Average Gross Time to Pay (in days) {historical_label}': historical_data["TTP_GROSS_HISTORY"]["HEU"].get("FP", "n.a"),
                f'Target Paid on Time - Contractually {current_label}': f"{current_fp_heu['on_time_pct']*100:.2f}%",
                f'Target Paid on Time - Contractually {historical_label}': f"{historical_data['PAYMENTS_ON_TIME_HISTORY']['HEU'].get('FP', 0)*100:.2f}%"
            })
            
            # HEU - Experts Payment (EXPERTS)
            current_exp_heu = heu_current.get('EXPERTS', {'avg_ttp_net': 0, 'avg_ttp_gross': 0, 'on_time_pct': 0})
            comparison_data.append({
                'Type of Payments': 'Experts Payment',
                f'Average Net Time to Pay (in days) {current_label}': round(current_exp_heu['avg_ttp_net'], 1),
                f'Average Net Time to Pay (in days) {historical_label}': historical_data["TTP_NET_HISTORY"]["HEU"].get("Experts", "n.a"),
                f'Average Gross Time to Pay (in days) {current_label}': round(current_exp_heu['avg_ttp_gross'], 1),
                f'Average Gross Time to Pay (in days) {historical_label}': historical_data["TTP_GROSS_HISTORY"]["HEU"].get("Experts", "n.a"),
                f'Target Paid on Time - Contractually {current_label}': f"{current_exp_heu['on_time_pct']*100:.2f}%",
                f'Target Paid on Time - Contractually {historical_label}': f"{historical_data['PAYMENTS_ON_TIME_HISTORY']['HEU'].get('Experts', 0)*100:.2f}%"
            })
            
            # HEU - Overall
            current_heu = heu_current.get('overall', {'avg_ttp_net': 0, 'avg_ttp_gross': 0, 'on_time_pct': 0})
            comparison_data.append({
                'Type of Payments': 'HEU',
                f'Average Net Time to Pay (in days) {current_label}': round(current_heu['avg_ttp_net'], 1),
                f'Average Net Time to Pay (in days) {historical_label}': historical_data["TTP_NET_HISTORY"]["HEU"].get("HEU", "n.a"),
                f'Average Gross Time to Pay (in days) {current_label}': round(current_heu['avg_ttp_gross'], 1),
                f'Average Gross Time to Pay (in days) {historical_label}': historical_data["TTP_GROSS_HISTORY"]["HEU"].get("HEU", "n.a"),
                f'Target Paid on Time - Contractually {current_label}': f"{current_heu['on_time_pct']*100:.2f}%",
                f'Target Paid on Time - Contractually {historical_label}': f"{historical_data['PAYMENTS_ON_TIME_HISTORY']['HEU'].get('HEU', 0)*100:.2f}%"
            })
            
            # TOTAL row
            current_total = current_metrics.get('TOTAL', {'avg_ttp_net': 0, 'avg_ttp_gross': 0, 'on_time_pct': 0})
            comparison_data.append({
                'Type of Payments': 'TOTAL',
                f'Average Net Time to Pay (in days) {current_label}': round(current_total['avg_ttp_net'], 1),
                f'Average Net Time to Pay (in days) {historical_label}': historical_data["TTP_NET_HISTORY"]["ALL"].get("TOTAL", "n.a"),
                f'Average Gross Time to Pay (in days) {current_label}': round(current_total['avg_ttp_gross'], 1),
                f'Average Gross Time to Pay (in days) {historical_label}': historical_data["TTP_GROSS_HISTORY"]["ALL"].get("TOTAL", "n.a"),
                f'Target Paid on Time - Contractually {current_label}': f"{current_total['on_time_pct']*100:.2f}%",
                f'Target Paid on Time - Contractually {historical_label}': f"{historical_data['PAYMENTS_ON_TIME_HISTORY']['ALL'].get('TOTAL', 0)*100:.2f}%"
            })
            
            return pd.DataFrame(comparison_data)

        def create_ttp_effectiveness_table(df_paym, cutoff, historical_data, report_name='Quarterly_Report', db_path="database/reporting.db"):
            """
            Create TTP effectiveness and efficiency indicators table
            """
            # Calculate current metrics
            current_metrics = calculate_current_ttp_metrics(df_paym, cutoff)
            
            # Determine labels based on cutoff using get_scope_start_end
            quarter_dates = get_scope_start_end(cutoff=cutoff)
            last_valid_date = quarter_dates[1]
            current_month = pd.to_datetime(last_valid_date).strftime('%b-%y')
            
            cutoff_date = pd.to_datetime(cutoff)
            current_year = cutoff_date.year
            historical_label = f"Dec-{str(current_year-1)[-2:]}"
            
            # Load database parameters for admin and expert meetings
            from pathlib import Path
            DB_PATH = Path(db_path)
            report_params = load_report_params(report_name=report_name, db_path=DB_PATH)
            
            admin_eff = report_params.get("Administrative_expenditure_Effectiveness", {})
            admin_ttp = report_params.get("Administrative_expenditure_ttp", {})
            expt_meet_eff = report_params.get("Expert_meetings_Effectiveness", {})
            expt_meet_ttp = report_params.get("Expert_meetings_ttp", {})
            
            # Build effectiveness data
            effectiveness_data = []
            
            # Get current metrics for calculations
            h2020_current = current_metrics.get('H2020', {})
            heu_current = current_metrics.get('HEU', {})
            
            # Research grants - Interim Payments - H2020
            h2020_ip = h2020_current.get('IP', {}).get('on_time_pct', 0)
            effectiveness_data.append({
                'Time-to–Pay: % of payments made on time (H2020 - HEU)': 'Research grants - Interim Payments - H2020',
                current_month: f"{h2020_ip*100:.2f}%",
                historical_label: f"{historical_data['PAYMENTS_ON_TIME_HISTORY']['H2020'].get('IP', 0)*100:.2f}%",
                'Target': '95% in 90 days'
            })
            
            # Research grants - Final Payments - H2020
            h2020_fp = h2020_current.get('FP', {}).get('on_time_pct', 0)
            effectiveness_data.append({
                'Time-to–Pay: % of payments made on time (H2020 - HEU)': 'Research grants - Final Payments - H2020',
                current_month: f"{h2020_fp*100:.2f}%",
                historical_label: f"{historical_data['PAYMENTS_ON_TIME_HISTORY']['H2020'].get('FP', 0)*100:.2f}%",
                'Target': '95% in 90 days'
            })
            
            # Experts with Appointment Letters H2020
            h2020_exp = h2020_current.get('EXPERTS', {}).get('on_time_pct', 0)
            effectiveness_data.append({
                'Time-to–Pay: % of payments made on time (H2020 - HEU)': 'Experts with Appointment Letters H2020',
                current_month: f"{h2020_exp*100:.2f}%",
                historical_label: f"{historical_data['PAYMENTS_ON_TIME_HISTORY']['H2020'].get('Experts', 0)*100:.2f}%",
                'Target': '95% in 30 days'
            })
            
            # Research grants - Pre-financings HEU
            heu_pf = heu_current.get('PF', {}).get('on_time_pct', 0)
            effectiveness_data.append({
                'Time-to–Pay: % of payments made on time (H2020 - HEU)': 'Research grants - Pre-financings HEU',
                current_month: f"{heu_pf*100:.2f}%",
                historical_label: f"{historical_data['PAYMENTS_ON_TIME_HISTORY']['HEU'].get('PF', 0)*100:.2f}%",
                'Target': '95% in 30 days'
            })
            
            # Research grants - Interim Payments HEU
            heu_ip = heu_current.get('IP', {}).get('on_time_pct', 0)
            effectiveness_data.append({
                'Time-to–Pay: % of payments made on time (H2020 - HEU)': 'Research grants - Interim Payments HEU',
                current_month: f"{heu_ip*100:.2f}%",
                historical_label: f"{historical_data['PAYMENTS_ON_TIME_HISTORY']['HEU'].get('IP', 0)*100:.2f}%",
                'Target': '95% in 90 days'
            })
            
            # Research grants - Final Payments HEU
            heu_fp = heu_current.get('FP', {}).get('on_time_pct', 0)
            effectiveness_data.append({
                'Time-to–Pay: % of payments made on time (H2020 - HEU)': 'Research grants - Final Payments HEU',
                current_month: f"{heu_fp*100:.2f}%",
                historical_label: f"{historical_data['PAYMENTS_ON_TIME_HISTORY']['HEU'].get('FP', 0)*100:.2f}%",
                'Target': '95% in 90 days'
            })
            
            # Administrative expenditure (from database)
            admin_current = admin_eff.get("Current", 0)
            admin_old = admin_eff.get("Old", 0)
            admin_target = admin_eff.get("Target", "99% in 30 days")
            
            # Format admin values - handle both percentage (0.985) and already formatted (98.5%) values
            if isinstance(admin_current, (int, float)) and admin_current <= 1:
                admin_current_str = f"{admin_current*100:.2f}%"
            else:
                admin_current_str = str(admin_current) if admin_current != "na" else "n/a"
                
            if isinstance(admin_old, (int, float)) and admin_old <= 1:
                admin_old_str = f"{admin_old*100:.2f}%"
            else:
                admin_old_str = str(admin_old) if admin_old != "na" else "n/a"
            
            effectiveness_data.append({
                'Time-to–Pay: % of payments made on time (H2020 - HEU)': 'Administrative expenditure',
                current_month: admin_current_str,
                historical_label: admin_old_str,
                'Target': admin_target
            })
            
            # Experts with Appointment Letters HEU
            heu_exp = heu_current.get('EXPERTS', {}).get('on_time_pct', 0)
            effectiveness_data.append({
                'Time-to–Pay: % of payments made on time (H2020 - HEU)': 'Experts with Appointment Letters HEU',
                current_month: f"{heu_exp*100:.2f}%",
                historical_label: f"{historical_data['PAYMENTS_ON_TIME_HISTORY']['HEU'].get('Experts', 0)*100:.2f}%",
                'Target': '95% in 30 days'
            })
            
            # Expert meetings PMO (from database)
            expt_current = expt_meet_eff.get("Current", "na")
            expt_old = expt_meet_eff.get("Old", "na")
            expt_target = expt_meet_eff.get("Target", "n/a")
            
            # Format expert values - handle both percentage and string values
            if isinstance(expt_current, (int, float)) and expt_current <= 1:
                expt_current_str = f"{expt_current*100:.2f}%"
            else:
                expt_current_str = str(expt_current) if expt_current != "na" else "n/a"
                
            if isinstance(expt_old, (int, float)) and expt_old <= 1:
                expt_old_str = f"{expt_old*100:.2f}%"
            else:
                expt_old_str = str(expt_old) if expt_old != "na" else "n/a"
            
            effectiveness_data.append({
                'Time-to–Pay: % of payments made on time (H2020 - HEU)': 'Expert meetings PMO',
                current_month: expt_current_str,
                historical_label: expt_old_str,
                'Target': expt_target
            })
            
            return pd.DataFrame(effectiveness_data)



        def create_ttp_days_table(df_paym, cutoff, historical_data, report_name='Quarterly_Report', db_path="database/reporting.db"):
            """
            Create Time to Pay: Average number of days (H2020 - HEU) table
            """
            # Calculate current metrics
            current_metrics = calculate_current_ttp_metrics(df_paym, cutoff)
            
            # Load database parameters for admin and expert meetings
            from pathlib import Path
            DB_PATH = Path(db_path)
            report_params = load_report_params(report_name=report_name, db_path=DB_PATH)
            
            admin_eff = report_params.get("Administrative_expenditure_Effectiveness", {})
            admin_ttp = report_params.get("Administrative_expenditure_ttp", {})
            expt_meet_eff = report_params.get("Expert_meetings_Effectiveness", {})
            expt_meet_ttp = report_params.get("Expert_meetings_ttp", {})
            
            # Build days data
            days_data = []
            
            # Get current metrics for calculations
            h2020_current = current_metrics.get('H2020', {})
            heu_current = current_metrics.get('HEU', {})
            
            # Research grants (days) - Interim Payments- H2020
            h2020_ip = h2020_current.get('IP', {})
            days_data.append({
                'Time to Pay: Average number of days (H2020 - HEU)': 'Research grants (days) - Interim Payments- H2020',
                'NET': round(h2020_ip.get('avg_ttp_net', 0), 1),
                'GROSS': round(h2020_ip.get('avg_ttp_gross', 0), 1),
                'Target': '90 days'
            })
            
            # Research grants (days) - Final Payments- H2020
            h2020_fp = h2020_current.get('FP', {})
            days_data.append({
                'Time to Pay: Average number of days (H2020 - HEU)': 'Research grants (days) - Final Payments- H2020',
                'NET': round(h2020_fp.get('avg_ttp_net', 0), 1),
                'GROSS': round(h2020_fp.get('avg_ttp_gross', 0), 1),
                'Target': '90 days'
            })
            
            # Experts with Appointment Letters (days) H2020
            h2020_exp = h2020_current.get('EXPERTS', {})
            days_data.append({
                'Time to Pay: Average number of days (H2020 - HEU)': 'Experts with Appointment Letters (days) H2020',
                'NET': round(h2020_exp.get('avg_ttp_net', 0), 1),
                'GROSS': round(h2020_exp.get('avg_ttp_gross', 0), 1),
                'Target': '30 days'
            })
            
            # Research grants (days) - Pre-financings - HEU
            heu_pf = heu_current.get('PF', {})
            days_data.append({
                'Time to Pay: Average number of days (H2020 - HEU)': 'Research grants (days) - Pre-financings - HEU',
                'NET': round(heu_pf.get('avg_ttp_net', 0), 1),
                'GROSS': round(heu_pf.get('avg_ttp_gross', 0), 1),
                'Target': '30 days'
            })
            
            # Research grants (days) - Interim Payments- HEU
            heu_ip = heu_current.get('IP', {})
            days_data.append({
                'Time to Pay: Average number of days (H2020 - HEU)': 'Research grants (days) - Interim Payments- HEU',
                'NET': round(heu_ip.get('avg_ttp_net', 0), 1),
                'GROSS': round(heu_ip.get('avg_ttp_gross', 0), 1),
                'Target': '90 days'
            })
            
            # Research grants (days) - Final Payments- HEU
            heu_fp = heu_current.get('FP', {})
            days_data.append({
                'Time to Pay: Average number of days (H2020 - HEU)': 'Research grants (days) - Final Payments- HEU',
                'NET': round(heu_fp.get('avg_ttp_net', 0), 1),
                'GROSS': round(heu_fp.get('avg_ttp_gross', 0), 1),
                'Target': '90 days'
            })
            
            # Expert with Appointment Letter (days) HEU
            heu_exp = heu_current.get('EXPERTS', {})
            days_data.append({
                'Time to Pay: Average number of days (H2020 - HEU)': 'Expert with Appointment Letter (days) HEU',
                'NET': round(heu_exp.get('avg_ttp_net', 0), 1),
                'GROSS': round(heu_exp.get('avg_ttp_gross', 0), 1),
                'Target': '30 days'
            })
            
            # Expert meetings (PMO) (days) - from database
            expt_net_current = expt_meet_ttp.get("Current", "na")
            expt_gross_current = expt_meet_ttp.get("Current", "na")  # Assuming same for both NET and GROSS
            
            # Format expert meeting values
            if isinstance(expt_net_current, (int, float)):
                expt_net_str = str(round(expt_net_current, 1))
                expt_gross_str = str(round(expt_gross_current, 1))
            else:
                expt_net_str = "n/a"
                expt_gross_str = "n/a"
            
            days_data.append({
                'Time to Pay: Average number of days (H2020 - HEU)': 'Expert meetings (PMO) (days)',
                'NET': expt_net_str,
                'GROSS': expt_gross_str,
                'Target': '30 days'
            })
            
            # Administrative expenditure (days) - from database
            admin_net_current = admin_ttp.get("Current", 0)
            admin_gross_current = admin_ttp.get("Current", 0)  # Assuming same for both NET and GROSS
            
            # Format admin values
            if isinstance(admin_net_current, (int, float)):
                admin_net_str = str(round(admin_net_current, 1))
                admin_gross_str = str(round(admin_gross_current, 1))
            else:
                admin_net_str = "n/a"
                admin_gross_str = "n/a"
            
            days_data.append({
                'Time to Pay: Average number of days (H2020 - HEU)': 'Administrative expenditure (days)',
                'NET': admin_net_str,
                'GROSS': admin_gross_str,
                'Target': '30 days'
            })
            
            return pd.DataFrame(days_data)

        def generate_all_ttp_tables(df_paym, cutoff, report_name='Quarterly_Report', db_path="database/reporting.db"):
            """
            Generate all three TTP tables - comparison table, effectiveness table, and days table
            
            Usage in Jupyter:
            comparison_table, effectiveness_table, days_table = generate_all_ttp_tables(df_paym, cutoff)
            """
            # Load historical data
            historical_data = load_historical_ttp_data(report_name=report_name, db_path=db_path)
            
            # Create all three tables
            comparison_table = create_ttp_comparison_table(df_paym, cutoff, historical_data)
            effectiveness_table = create_ttp_effectiveness_table(df_paym, cutoff, historical_data, report_name, db_path)
            days_table = create_ttp_days_table(df_paym, cutoff, historical_data, report_name, db_path)
            
            return comparison_table, effectiveness_table, days_table
        
        def format_table_ttp_summary(df,
                            title="Time-to-Pay Performance Summary",
                            rowname_col="Type of Payments",
                            table_colors=None):
            """
            Format TTP table with 3 spanners: Average Net Time to Pay, Average Gross Time to Pay, Target Paid on Time
            
            Args:
                df: DataFrame with TTP data
                title: Table title
                rowname_col: Column name for row names (default: "Type of Payments")
                table_colors: Dictionary with custom colors
            """
        
            # Default colors if not provided
            if table_colors is None:
                table_colors = {}
            
            BLUE = table_colors.get("BLUE", "#004A99")
            LIGHT_BLUE = table_colors.get("LIGHT_BLUE", "#d6e6f4")
            DARK_BLUE = table_colors.get("DARK_BLUE", "#01244B") 
            SUB_TOTAL_BACKGROUND = table_colors.get("subtotal_background_color", "#E6E6FA")
            
            # Identify columns for each spanner
            all_columns = [col for col in df.columns if col != rowname_col]
            
            # Identify spanner columns based on column names
            net_time_cols = [col for col in all_columns if 'Average Net Time to Pay' in col]
            gross_time_cols = [col for col in all_columns if 'Average Gross Time to Pay' in col]
            target_cols = [col for col in all_columns if 'Target Paid on Time' in col]
            
            print(f"📊 Identified columns:")
            print(f"   Net Time columns: {net_time_cols}")
            print(f"   Gross Time columns: {gross_time_cols}")
            print(f"   Target columns: {target_cols}")
            
            # Identify second columns for LIGHT_BLUE highlighting
            second_columns = []
            if len(net_time_cols) > 1:
                second_columns.append(net_time_cols[1])
            if len(gross_time_cols) > 1:
                second_columns.append(gross_time_cols[1])
            if len(target_cols) > 1:
                second_columns.append(target_cols[1])
            
            print(f"🎨 Second columns (LIGHT_BLUE background): {second_columns}")
            
            # Fixed width for TTP table (6 data columns + stub)
            table_width = "1200px"
            
            # Identify special rows for formatting
            h2020_rows = df.index[df[rowname_col].astype(str).str.contains('H2020', case=False, na=False)].tolist()
            heu_rows = df.index[df[rowname_col].astype(str).str.contains('HEU', case=False, na=False)].tolist()
            total_rows = df.index[df[rowname_col].astype(str).str.contains('TOTAL', case=False, na=False)].tolist()
            
            # Create and format table with 3 spanners
            tbl = (
                GT(df, rowname_col=rowname_col)
                
                # 1. Basic setup
                .tab_header(title=md(title))
                
                # 2. Create the 3 spanners
                .tab_spanner(label="Average Net Time to Pay (in days)", columns=net_time_cols)
                .tab_spanner(label="Average Gross Time to Pay (in days)", columns=gross_time_cols)
                .tab_spanner(label="Target Paid on Time - Contractually", columns=target_cols)
                
                # 3. Set stub header
                .tab_stubhead(label="Type of Payments")
                
                # 4. Apply theme and basic styling
                .opt_stylize(style=5, color='blue')
                .opt_table_font(font="Arial")
                
                # 5. Table options
                .tab_options(
                    table_background_color="white",
                    heading_background_color="white",
                    table_font_size='small',
                    table_font_color=DARK_BLUE,
                    table_width=table_width,
                    heading_title_font_size="16px",
                    heading_title_font_weight="bold",
                    stub_background_color=LIGHT_BLUE,
                    row_striping_include_table_body=False,
                    row_striping_include_stub=False,
                    column_labels_background_color="#004d80",
                )
                
                # 6. Format time columns (days) as numbers with 1 decimal
                .fmt_number(
                    columns=net_time_cols + gross_time_cols,
                    decimals=1
                )
                
        
                # 8. Style second column of each spanner with LIGHT_BLUE background
                .tab_style(
                    style=[style.fill(color="#E3EDF6"),
                    style.text(color=DARK_BLUE, weight="bold")],
                    locations=loc.body(columns=[col for col in [
                        net_time_cols[1] if len(net_time_cols) > 1 else None,
                        gross_time_cols[1] if len(gross_time_cols) > 1 else None,
                        target_cols[1] if len(target_cols) > 1 else None
                    ] if col is not None])
                )
                
                # 9. Style special rows
                .tab_style(
                    style=[
                        style.text(color='white', weight="bold"),
                        style.fill(color="#004d80")
                    ],
                    locations=[
                        loc.body(rows=h2020_rows + heu_rows + total_rows),
                        loc.stub(rows=h2020_rows + heu_rows + total_rows)
                    ]
                )

                # 10. Style column spanners
                .tab_style(
                    style=[
                        style.text(color="white", weight="bold"),
                        style.fill(color="#004d80")
                    ],
                    locations=loc.column_labels()
                )
                
                # 11. Center align data columns
                .cols_align(
                    align="center",
                    columns=all_columns
                )
                
                # 12. Make stub (row names) left-aligned and bold
                .tab_style(
                    style=style.text(weight="bold"),
                    locations=loc.stub()
                )
                .tab_options( heading_subtitle_font_size="medium", heading_title_font_size="large", table_font_size='medium',  column_labels_font_size='medium',row_group_font_size='medium', stub_font_size='medium')
            )
        
            return tbl
        

        def format_tables_ttp_overview(df,table_colors=None):
            """
            Format TTP table with 3 spanners: Average Net Time to Pay, Average Gross Time to Pay, Target Paid on Time
            
            Args:
                df: DataFrame with TTP data
                title: Table title
                rowname_col: Column name for row names (default: "Type of Payments")
                table_colors: Dictionary with custom colors
            """
            
            # Default colors if not provided
            if table_colors is None:
                table_colors = {}
            
            BLUE = table_colors.get("BLUE", "#004A99")
            LIGHT_BLUE = table_colors.get("LIGHT_BLUE", "#d6e6f4")
            DARK_BLUE = table_colors.get("DARK_BLUE", "#01244B") 
            SUB_TOTAL_BACKGROUND = table_colors.get("subtotal_background_color", "#E6E6FA")
            
            columns_cetered = df.columns[1:].tolist()
            # Fixed width for TTP table (6 data columns + stub)
            table_width = "1200px"
            
            # Create and format table with 3 spanners
            tbl = (
                GT(df)
                
                # 4. Apply theme and basic styling
                .opt_stylize(style=5, color='blue')
                .opt_table_font(font="Arial")
            
                # 5. Table options
                .tab_options(
                    table_background_color="white",
                    heading_background_color="white",
                    table_font_size='small',
                    table_font_color=DARK_BLUE,
                    table_width=table_width,
                    heading_title_font_size="16px",
                    heading_title_font_weight="bold",
                    row_striping_include_table_body=False,
                    row_striping_include_stub=False,
                    column_labels_background_color=LIGHT_BLUE,
                    column_labels_font_weight='bold',
                )
            

                # 11. Center align data columns
                .cols_align(
                    align="center",
                    columns=columns_cetered 
                )
                
            )
    
            return tbl

        # =============================================================================
        # MAIN FUNCTION 
        # =============================================================================

        def generate_ttp_tables(df_paym, cutoff, report_name='Quarterly_Report', db_path="database/reporting.db"):
            """
            Main function to generate all TTP tables
            
            Usage in Jupyter:
            comparison_table, effectiveness_table, days_table = generate_ttp_tables(df_paym, cutoff)
            
            # Display all three tables
            print("TTP Comparison Table:")
            display(comparison_table)
            
            print("\nEffectiveness and Efficiency Indicators:")
            display(effectiveness_table)
            
            print("\nTime to Pay: Average number of days:")
            display(days_table)
            """
            return generate_all_ttp_tables(df_paym, cutoff, report_name, db_path)
        

        def save_ttp_tables_main_overview(table_data, var_name, module):
  
            if table_data is None or (isinstance(table_data, pd.DataFrame) and table_data.empty):
                return False, f"No data available for {var_name}"
            
            if module == 'OverviewModule':
                formatted_table =  format_tables_ttp_overview(table_data, table_colors)
            elif module == 'PaymentsModule':
                formatted_table= format_table_ttp_summary(table_data,
                            title="Time-to-Pay Performance Summary",
                            rowname_col="Type of Payments",
                            table_colors=table_colors)

            try:
                # Save to database
                insert_variable(
                    report=report,
                    module= module,
                    var=var_name,
                    value=table_data.to_dict() if isinstance(table_data, pd.DataFrame) else table_data,
                    db_path=db_path,
                    anchor=var_name,
                    gt_table=formatted_table
                )
                
                return True, f"Successfully processed {var_name}"
                
            except Exception as e:
                return False, f"Error processing {var_name}: {str(e)}"


        # ═══════════════════════════════════════════════════════════════════
        # MAIN EXECUTION WITH ERROR HANDLING
        # ═══════════════════════════════════════════════════════════════════

        # comparison_table, effectiveness_table, ttp_days_table = generate_ttp_tables(df_paym, cutoff)

        try:
            print("📊 Generating TTP tables...")
            
            # Generate the three main TTP tables
            comparison_table, effectiveness_table, ttp_days_table = generate_ttp_tables(df_paym, cutoff)
            
            # Validate that tables were generated
            if comparison_table is None or effectiveness_table is None or ttp_days_table is None:
                return False, "One or more TTP tables failed to generate", None
                
            if (isinstance(comparison_table, pd.DataFrame) and comparison_table.empty) or \
                (isinstance(effectiveness_table, pd.DataFrame) and effectiveness_table.empty) or \
                (isinstance(ttp_days_table, pd.DataFrame) and ttp_days_table.empty):
                return False, "One or more TTP tables are empty", None
                
            print("✅ TTP tables generated successfully")
        
        except Exception as e:
            return False, f"Error generating TTP tables: {str(e)}", None
        
        # ═══════════════════════════════════════════════════════════════════
        # SAVE TABLES WITH ERROR TRACKING
        # ═══════════════════════════════════════════════════════════════════
    
        try:
            print("💾 Saving TTP tables to database...")
            
            # Define tables to save
            tables_ttp = [
                (ttp_days_table, 'Table_overview_ttp', "OverviewModule"), 
                (effectiveness_table, 'Table_overview_ttp_effectiveness', "OverviewModule"),
                (comparison_table, 'TTP_performance_summary_table', 'PaymentsModule')
            ]
            
            # Track results
            successful_saves = []
            failed_saves = []
            
            # Save each table
            for table_data, var_name, module in tables_ttp:
                try:
                    success, message = save_ttp_tables_main_overview(table_data, var_name, module)
                    
                    if success:
                        successful_saves.append(var_name)
                        print(f"   ✅ {var_name}")
                    else:
                        failed_saves.append(f"{var_name}: {message}")
                        print(f"   ❌ {var_name}: {message}")
                        
                except Exception as e:
                    error_msg = f"{var_name}: {str(e)}"
                    failed_saves.append(error_msg)
                    print(f"   ❌ {error_msg}")
            # ═══════════════════════════════════════════════════════════════════
            # DETERMINE OVERALL SUCCESS
            # ═══════════════════════════════════════════════════════════════════
            
            total_tables = len(tables_ttp)
            successful_count = len(successful_saves)
            failed_count = len(failed_saves)
            
            print(f"\n📈 TTP Tables Summary:")
            print(f"   ✅ Successful: {successful_count}")
            print(f"   ❌ Failed: {failed_count}")
            
            # Return results based on success/failure
            if successful_count == total_tables and failed_count == 0:
                success_message = f"All TTP tables generated and saved successfully! " \
                                f"Processed {successful_count} tables"
                                
                return True, success_message, {
                    'comparison_table': comparison_table,
                    'effectiveness_table': effectiveness_table,
                    'ttp_days_table': ttp_days_table,
                    'summary': {
                        'successful': successful_count,
                        'failed': failed_count,
                        'successful_tables': successful_saves,
                        'failed_tables': failed_saves
                    }
                }
                
            elif successful_count > 0 and failed_count > 0:
                warning_message = f"TTP tables partially completed. " \
                                f"Successful: {successful_count}, Failed: {failed_count}"
                                
                return True, warning_message, {
                    'comparison_table': comparison_table,
                    'effectiveness_table': effectiveness_table,
                    'ttp_days_table': ttp_days_table,
                    'summary': {
                        'successful': successful_count,
                        'failed': failed_count,
                        'successful_tables': successful_saves,
                        'failed_tables': failed_saves
                    }
                }
                
            else:
                error_message = f"TTP table generation failed. " \
                              f"No tables were successfully saved. " \
                              f"Failed: {failed_count}"
                              
                return False, error_message, {
                    'summary': {
                        'successful': successful_count,
                        'failed': failed_count,
                        'successful_tables': successful_saves,
                        'failed_tables': failed_saves
                    }
                }
                
        except Exception as e:
            return False, f"Error during table saving process: {str(e)}", None
        
    except Exception as e:
        # Catch any unexpected errors
        error_message = f"Unexpected error in generate_ttp_items_main: {str(e)}"
        print(f"❌ {error_message}")
        return False, error_message, None
    
    finally:
        print("🏁 TTP tables generation process completed")
        

# ──────────────────────────────────────────────────────────────
# PAYMENTS TABLES : 3. TTP TABLES
# ──────────────────────────────────────────────────────────────
def generate_ttp_tables (df_paym, cutoff, db_path, report, table_colors, report_params):
        
    try:
        # ═══════════════════════════════════════════════════════════════════
        # INPUT VALIDATION
        # ═══════════════════════════════════════════════════════════════════
        
        if df_paym is None or df_paym.empty:
            return False, "Input DataFrame (df_paym) is empty or None", None
            
        if cutoff is None:
            return False, "Cutoff date is required", None
            
        if db_path is None:
            return False, "Database path is required", None
            
        if report is None:
            return False, "Report parameter is required", None

        print("🚀 Starting TTP tables and charts generation...")

        
        # ═══════════════════════════════════════════════════════════════════
        # VALIDATE REQUIRED COLUMNS
        # ═══════════════════════════════════════════════════════════════════
        
        required_columns = [
            'Pay Document Date (dd/mm/yyyy)', 
            'Pay Payment Key', 
            'v_TTP_NET', 
            'v_TTP_GROSS', 
            'v_payment_in_time',
            'Programme',
            'v_payment_type'
        ]
        
        missing_columns = [col for col in required_columns if col not in df_paym.columns]
        if missing_columns:
            return False, f"Missing required columns: {missing_columns}", None

        print("✅ Input validation completed successfully")

        # ═══════════════════════════════════════════════════════════════════
        # NESTED FUNCTION DEFINITIONS 
        # ═══════════════════════════════════════════════════════════════════

            
        def create_quarterly_ttp_table(df_paym, cutoff, programme, payment_type):
            """
            Create a quarterly TTP table for a specific programme and payment type
            Returns table, programme, payment_type, and a flag indicating if the table is empty
            """
            try: 
                # Use same logic as calculate_current_ttp_metrics
                quarter_dates = get_scope_start_end(cutoff=cutoff)
                last_valid_date = quarter_dates[1]

                # Filter data up to cutoff and deduplicate by Pay Payment Key (same as comparison table)
                df_filtered = df_paym[
                    df_paym['Pay Document Date (dd/mm/yyyy)'] <= last_valid_date
                ].copy()
                df_unique = df_filtered.drop_duplicates(subset=['Pay Payment Key']).copy()
                
                # Convert to numeric and filter negative v_TTP_NET (same as comparison table)
                df_unique['v_TTP_NET'] = pd.to_numeric(df_unique['v_TTP_NET'], errors='coerce')
                df_unique['v_TTP_GROSS'] = pd.to_numeric(df_unique['v_TTP_GROSS'], errors='coerce')
                df_unique['v_payment_in_time'] = pd.to_numeric(df_unique['v_payment_in_time'], errors='coerce')
                df_unique = df_unique[df_unique['v_TTP_NET'] >= 0]
                
                # Now filter by programme and payment type (same as comparison table order)
                df_filtered = df_unique[
                    (df_unique['Programme'] == programme) &
                    (df_unique['v_payment_type'] == payment_type)
                ].copy()
                
                # If no data after filtering, return an empty table with a flag
                if df_filtered.empty:
                    empty_table = pd.DataFrame(columns=['Quarter', 'ADG', 'COG', 'POC', 'STG', 'SYG', 'Total'])
                    return empty_table, programme, payment_type, True
                
                # Extract quarter from date
                df_filtered['Quarter'] = pd.to_datetime(df_filtered['Pay Document Date (dd/mm/yyyy)']).dt.to_period('Q').astype(str)
                
                # Use call_type as CallType (based on logs)
                if 'call_type' not in df_filtered.columns:
                    df_filtered['call_type'] = 'Default'  # Placeholder if call_type is missing
                df_filtered['CallType'] = df_filtered['call_type']
                
                # Aggregate by Quarter and CallType (using v_TTP_NET mean as metric)
                quarterly_data = df_filtered.groupby(['Quarter', 'CallType']).agg({
                    'v_TTP_NET': 'mean'
                }).round(1).unstack(fill_value=0)
                
                # Rename columns to match call types from logs (ADG, COG, etc.)
                quarterly_data.columns = [f'{col[1]}' for col in quarterly_data.columns]
                
                # Add Total column (average across call types for each quarter)
                quarterly_data['Total'] = quarterly_data.mean(axis=1).round(1)
                
                # Calculate total row from original data (to match comparison table calculation)
                total_by_calltype = df_filtered.groupby('CallType')['v_TTP_NET'].mean().round(1)
                overall_total = df_filtered['v_TTP_NET'].mean().round(1)
                
                # Create total row with proper structure matching quarterly_data columns
                total_row = pd.Series(index=quarterly_data.columns, dtype=float)
                for col in quarterly_data.columns:
                    if col == 'Total':
                        total_row[col] = overall_total
                    elif col in total_by_calltype.index:
                        total_row[col] = total_by_calltype[col]
                    else:
                        total_row[col] = 0.0
                total_row.name = 'Total'
                
                quarterly_table = pd.concat([quarterly_data, total_row.to_frame().T])
                
                # Simplify index to only include Quarter
                quarterly_table.index = quarterly_table.index.droplevel([1, 2]) if isinstance(quarterly_table.index, pd.MultiIndex) else quarterly_table.index
                
                return quarterly_table, programme, payment_type, False
            
            except Exception as e:
                raise Exception(f"Error creating quarterly table for {programme} {payment_type}: {str(e)}")

        def generate_quarterly_tables(df_paym, cutoff):
            """
            Generate quarterly TTP tables for each programme and payment type
            Includes a flag to indicate empty tables
            """
            try:
                tables = {}
                payment_types = ['IP', 'FP', 'EXPERTS', 'PF']  # Short form values
                programs = ['H2020', 'HEU']

                for prog in programs:
                    for pt in payment_types:
                        table, program, payment_type, is_empty = create_quarterly_ttp_table(df_paym, cutoff, prog, pt)
                        tables[f'{prog}_{pt}_table'] = {
                            'table': table,
                            'programme': program,
                            'payment_type': payment_type,
                            'is_empty': is_empty
                        }

                return tables
            
            except Exception as e:
                raise Exception(f"Error generating quarterly tables: {str(e)}")


        def format_tables_ttp_main(df,table_colors=None,table_title='nothing', table_subtitle='nothing'):
            """
            Format TTP table with 3 spanners: Average Net Time to Pay, Average Gross Time to Pay, Target Paid on Time
            
            Args:
                df: DataFrame with TTP data
                title: Table title
                rowname_col: Column name for row names (default: "Type of Payments")
                table_colors: Dictionary with custom colors
            """

            try:
            
                # Default colors if not provided
                if table_colors is None:
                    table_colors = {}
                
                BLUE = table_colors.get("BLUE", "#004A99")
                LIGHT_BLUE = table_colors.get("LIGHT_BLUE", "#d6e6f4")
                DARK_BLUE = table_colors.get("DARK_BLUE", "#01244B") 
                SUB_TOTAL_BACKGROUND = table_colors.get("subtotal_background_color", "#E6E6FA")
                
                # Dynamic width calculation
                ttp_columns = df.columns[1:].to_list()
                num_data_columns = len(ttp_columns)
                base_width_per_column = 40  # pixels per column
                stub_width = 200  # width for the first column (Quarter)
                
                table_width = f"{stub_width + (num_data_columns * base_width_per_column)}px"
                
                total_rows = df.index[df['Quarter'].astype(str).str.contains('Total', case=False, na=False)].tolist()
                ttp_columns = df.columns[1:].to_list()
                
                # Create and format table with 3 spanners
                tbl = (
                    GT(df)
                    
                    # 4. Apply theme and basic styling
                    .opt_stylize(style=5, color='blue')
                    .opt_table_font(font="Arial")

                    .tab_header(
                            title =  table_title,
                            subtitle = table_subtitle
                        )
                    
                    
                    # 5. Table options
                    .tab_options(
                        table_background_color="white",
                        heading_background_color="white",
                        table_font_size='small',
                        table_font_color=DARK_BLUE,
                        table_width=table_width,
                        heading_title_font_size="12px",
                        heading_subtitle_font_size="10px",
                        heading_title_font_weight="bold",
                        row_striping_include_table_body=False,
                        row_striping_include_stub=False
                    )
                    .tab_options( heading_subtitle_font_size="medium", heading_title_font_size="large", table_font_size='medium',  column_labels_font_size='medium',row_group_font_size='medium', stub_font_size='medium')
                    #Format target columns (percentages) 
                    .fmt_number(
                        columns=ttp_columns,
                        decimals=1
                    )
                    
                    .tab_style(
                        style=[
                            style.text(color='white', weight="bold"),
                            style.fill(color="#004d80")
                        ],
                        locations=[
                            loc.body(rows=total_rows),
                            loc.stub(rows=total_rows)
                        ]
                    )
                    
                )
            
                return tbl
            
            except Exception as e:
                    raise Exception(f"Error formatting table: {str(e)}")


        # =============================================================================
        # MAIN USAGE FOR TABLES
        # =============================================================================


        def main_tables(df_paym, cutoff, report=None, db_path=None, table_colors=None):
            """
            Main function with GT table saving options to prevent infinite loops
            """
            try:
            
                # Generate tables
                quarterly_tables = generate_quarterly_tables(df_paym, cutoff)
                
                # Track results
                results = []
                successful_tables = {}
                failed_tables = []
                
                
                # Process each table
                for key, data in quarterly_tables.items():
                    table_data = data['table']
                    program = data['programme']
                    payment_type = data['payment_type']
                    is_empty = data['is_empty']
                    
                    var_name = f'Table_ttp_{program}_{payment_type}'

                    if payment_type == 'Interim Payments':
                        anchor_name = f'{program}_Interim_Payments'
                    elif payment_type == 'Final Payments':
                        anchor_name = f'{program}_Final_Payments'
                    elif payment_type == 'Pre-financing':
                        anchor_name = f'{program}_Pre_Financing'
                    else:
                        anchor_name = f'{program}_{payment_type}'

                    
                    # Skip empty tables
                    if table_data is None or (isinstance(table_data, pd.DataFrame) and table_data.empty) or is_empty:
                        results.append((False, f"No data available for {var_name}"))
                        print(f"Skipping {var_name}: No data available")
                        continue
                    
                    try:
                        # Reset index to create Quarter column
                        table_data_copy = table_data.copy()
                        table_data_copy.reset_index(inplace=True, names='Quarter')

                        if payment_type == 'IP':
                            table_title = 'Interim Payments – Time to Pay'
                        elif payment_type =='FP':
                            table_title = 'Final Payments – Time to Pay'
                        elif payment_type =='PF':
                            table_title = 'Pre-Finncing Payments – Time to Pay'
                        elif payment_type == 'EXPERTS' :
                            table_title = 'Experts Payments and Support Activities – Time to Pay'
                        
                        # Format table
                        formatted_table = format_tables_ttp_main(table_data_copy, 
                                                                 table_colors, 
                                                                 table_title=program, 
                                                                 table_subtitle=table_title)
                        
                        # Store successful table
                        successful_tables[key] = {
                            'table': table_data_copy,
                            'formatted_table': formatted_table,
                            'programme': program,
                            'payment_type': payment_type
                        }
                        
                        # Save to database with simple GT save for TTP tables
                        if report and db_path:
                            print(f"Saving {var_name} with simple GT save...")
                            
                            # Use simple GT save to avoid infinite loops
                            insert_variable(
                                report=report,
                                module="PaymentsModule",
                                var=var_name,
                                value=table_data_copy.to_dict() if isinstance(table_data_copy, pd.DataFrame) else table_data_copy,
                                db_path=db_path,
                                anchor=anchor_name,
                                gt_table=formatted_table,
                                simple_gt_save=True  # ← KEY: Use simple save instead of complex retry logic
                            )
                            results.append((True, f"Successfully processed and saved {var_name}"))
                            print(f"✓ Successfully processed and saved {var_name}")
                        else:
                            results.append((True, f"Successfully processed {var_name} (not saved - missing db params)"))
                            print(f"✓ Successfully processed {var_name} (not saved to DB)")
                        
                    except Exception as e:
                        error_msg = f"Error processing {var_name}: {str(e)}"
                        results.append((False, error_msg))
                        failed_tables.append(error_msg)
                        print(f"   ❌ {error_msg}")
                
                # Print summary
                successful_count = sum(1 for success, _ in results if success)
                total_count = len(results)
                failed_count = len(failed_tables)
                total_count = len(results)
                
                print(f"\n📊 TTP Tables Summary:")
                print(f"   ✅ Successful: {successful_count}")
                print(f"   ❌ Failed: {failed_count}")
                print(f"   📋 Total: {total_count}")
                
                return quarterly_tables, results, successful_tables, failed_tables
                
            except Exception as e:
                raise Exception(f"Error in main_tables processing: {str(e)}")
        

        # ═══════════════════════════════════════════════════════════════════
        # MAIN EXECUTION WITH ERROR HANDLING
        # ═══════════════════════════════════════════════════════════════════

        try:
            print("📊 Generating quarterly TTP tables...")
            
            # Execute main table processing
            quarterly_tables, results, successful_tables, failed_tables = main_tables(
                df_paym, 
                cutoff, 
                report='Quarterly_Report',  # Fixed the typo
                db_path=db_path,
                table_colors=table_colors
            )
            
            # Validate that quarterly_tables were generated
            if not quarterly_tables:
                return False, "Failed to generate any quarterly tables", None
                
            print("✅ Quarterly tables generation completed")
            
        except Exception as e:
            return False, f"Error generating quarterly tables: {str(e)}", None

        # ═══════════════════════════════════════════════════════════════════
        # DETERMINE OVERALL SUCCESS AND PREPARE RESULTS
        # ═══════════════════════════════════════════════════════════════════
        
        successful_count = sum(1 for success, _ in results if success)
        failed_count = len(failed_tables)
        total_count = len(results)
        
        print(f"\n📈 Final TTP Tables & Charts Summary:")
        print(f"   ✅ Successful: {successful_count}")
        print(f"   ❌ Failed: {failed_count}")
        print(f"   📋 Total: {total_count}")
        
        # Prepare return results
        return_results = {
            'quarterly_tables': quarterly_tables,
            'successful_tables': successful_tables,
            'summary': {
                'total_tables': total_count,
                'successful': successful_count,
                'failed': failed_count,
                'successful_list': [success_msg for success, success_msg in results if success],
                'failed_list': failed_tables
            }
        }
        
        # Determine success level
        if successful_count == total_count and failed_count == 0:
            success_message = f"All TTP tables and charts generated successfully! " \
                            f"Processed {successful_count} tables"
            return True, success_message, return_results
            
        elif successful_count > 0 and failed_count > 0:
            warning_message = f"TTP tables and charts partially completed. " \
                            f"Successful: {successful_count}, Failed: {failed_count}"
            return True, warning_message, return_results
            
        else:
            error_message = f"TTP tables and charts generation failed. " \
                          f"No tables were successfully processed. " \
                          f"Failed: {failed_count}"
            return False, error_message, return_results

       
    except Exception as e:
        # Catch any unexpected errors
        error_message = f"Unexpected error in generate_ttp_tables_charts: {str(e)}"
        print(f"❌ {error_message}")
        return False, error_message, None
        
    finally:
        print("🏁 TTP tables and charts generation process completed")



# ──────────────────────────────────────────────────────────────
# PAYMENTS TABLES : 4. TTP CHARTS
# ──────────────────────────────────────────────────────────────
def generate_ttp_charts (df_paym, cutoff, db_path, report, table_colors, report_params, quarterly_tables):
     
    """
    Generate TTP charts with comprehensive error handling
    
    Returns:
        tuple: (success: bool, message: str, results: dict or None)
    """
        
    try:
        # ═══════════════════════════════════════════════════════════════════
        # INPUT VALIDATION
        # ═══════════════════════════════════════════════════════════════════
        
        if df_paym is None or df_paym.empty:
            return False, "Input DataFrame (df_paym) is empty or None", None
            
        if cutoff is None:
            return False, "Cutoff date is required", None
            
        if db_path is None:
            return False, "Database path is required", None
            
        if report is None:
            return False, "Report parameter is required", None
            
        if quarterly_tables is None:
            return False, "Quarterly tables parameter is required", None

        print("🚀 Starting TTP charts generation...")

        # ═══════════════════════════════════════════════════════════════════
        # VALIDATE REQUIRED COLUMNS
        # ═══════════════════════════════════════════════════════════════════
        
        required_columns = [
            'Pay Document Date (dd/mm/yyyy)', 
            'Pay Payment Key', 
            'v_TTP_NET', 
            'v_TTP_GROSS', 
            'v_payment_in_time',
            'Programme',
            'v_payment_type'
        ]
        
        missing_columns = [col for col in required_columns if col not in df_paym.columns]
        if missing_columns:
            return False, f"Missing required columns: {missing_columns}", None

        print("✅ Input validation completed successfully")

        # ═══════════════════════════════════════════════════════════════════
        # NESTED FUNCTION DEFINITIONS WITH ERROR HANDLING
        # ═══════════════════════════════════════════════════════════════════


        def rolling_ttp(df,programme,typeofpayment):

            """
            Calculate rolling average TTP with error handling
            """
            
            try:
        
                start, end  = get_scope_start_end(cutoff)
                last_month = int(end.month)
                i = 1
                moving_avg_ttp = []
                months = []

                quarter_dates = get_scope_start_end(cutoff=cutoff)
                last_valid_date = quarter_dates[1]

                df_filtered = df[
                        df['Pay Document Date (dd/mm/yyyy)'] <= last_valid_date
                ].copy()
                df_unique = df_filtered.drop_duplicates(subset=['Pay Payment Key']).copy()
                
                # Convert to numeric and filter negative v_TTP_NET
                df_unique['v_TTP_NET'] = pd.to_numeric(df_unique['v_TTP_NET'], errors='coerce')
                df_unique['v_TTP_GROSS'] = pd.to_numeric(df_unique['v_TTP_GROSS'], errors='coerce')
                df_unique['v_payment_in_time'] = pd.to_numeric(df_unique['v_payment_in_time'], errors='coerce')
                df_unique = df_unique[df_unique['v_TTP_NET'] >= 0]

                while i <= last_month : 
                        df_test =  df_unique.loc[( df_unique['Programme'] == programme) & ( df_unique['v_payment_type'] == typeofpayment) & ( df_unique['Month']<= i )]    
                        mean = round(df_test['TTP_NET'].mean(),1)
                        moving_avg_ttp.insert(i,mean)
                        months.insert(i,i)
                        i+=1

                d = {'Month': months, 'TTP': moving_avg_ttp}
                df_mov_ttp = pd.DataFrame(data=d)
                return df_mov_ttp 
            except Exception as e:
                raise Exception(f"Error in rolling_ttp for {programme} {typeofpayment}: {str(e)}")


        def avg_ttp(df,programme,typeofpayment):
                """
                Calculate average TTP by month with error handling
                """

                try:
            
                    start, end  = get_scope_start_end(cutoff)
                    last_month = int(end.month)
                    i = 1
                    moving_avg_ttp = []
                    months = []

                    quarter_dates = get_scope_start_end(cutoff=cutoff)
                    last_valid_date = quarter_dates[1]

                    df_filtered = df[
                            df['Pay Document Date (dd/mm/yyyy)'] <= last_valid_date
                    ].copy()
                    df_unique = df_filtered.drop_duplicates(subset=['Pay Payment Key']).copy()
                    
                    # Convert to numeric and filter negative v_TTP_NET
                    df_unique['v_TTP_NET'] = pd.to_numeric(df_unique['v_TTP_NET'], errors='coerce')
                    df_unique['v_TTP_GROSS'] = pd.to_numeric(df_unique['v_TTP_GROSS'], errors='coerce')
                    df_unique['v_payment_in_time'] = pd.to_numeric(df_unique['v_payment_in_time'], errors='coerce')
                    df_unique = df_unique[df_unique['v_TTP_NET'] >= 0]

                    pivot_ttp_month =  df_unique.pivot_table( index= df_unique[["Month"]], values= df_unique[['TTP_NET']],fill_value=0,aggfunc='mean')
                    pivot_ttp_month['TTP_NET'] = pivot_ttp_month['TTP_NET'].round(1)
                    #pivot_ttp_month.columns =  pivot_ttp_month.columns.droplevel()
                    pivot_ttp_month.reset_index(inplace = True)

                    return pivot_ttp_month
                
                except Exception as e:
                    raise Exception(f"Error in avg_ttp for {programme} {typeofpayment}: {str(e)}")

    
        # =============================================================================
        # CHART GENERATION FUNCTIONS
        # =============================================================================

        def chart_machine_ttp(df, paymentType, prog, avg, rollingAvg):
            """
            Generate TTP chart with error handling
            """
            try:
            
                # Time limits
                time_limits = {
                    'PF': 30, 'EXPERTS': 30, 'IP': 90, 'FP': 90
                }
                time_limit = time_limits.get(paymentType, 90)
                
                # Clean data
                df_clean = df.copy()
                df_clean['Month'] = pd.to_numeric(df_clean['Month'], errors='coerce')
                df_clean['TTP_NET'] = pd.to_numeric(df_clean['TTP_NET'], errors='coerce')
                df_clean = df_clean.dropna(subset=['Month', 'TTP_NET'])
                
                rollingAvg_clean = rollingAvg.copy()
                rollingAvg_clean['Month'] = pd.to_numeric(rollingAvg_clean['Month'], errors='coerce')
                rollingAvg_clean['TTP'] = pd.to_numeric(rollingAvg_clean['TTP'], errors='coerce')
                rollingAvg_clean = rollingAvg_clean.dropna()
                
                if df_clean.empty:
                    return alt.Chart().mark_text(text="No data available")
                
                # Calculate Y-axis range - extend if values are close to limits
                max_value = max(
                    df_clean['TTP_NET'].max() if not df_clean.empty else 0,
                    rollingAvg_clean['TTP'].max() if not rollingAvg_clean.empty else 0,
                    time_limit
                )
                
                # Extend Y-axis if monthly average is close to time limit (within 10 days)
                if abs(avg - time_limit) <= 10:
                    y_max = max_value * 1.2  # Extend by 20%
                else:
                    y_max = max_value * 1.1  # Normal 10% extension
                
                # Get last month with data for triangle positioning
                last_month_with_data = df_clean['Month'].max() if not df_clean.empty else 1
                start, end = get_scope_start_end(cutoff)
                year = int(end.year)
                
                # Get rolling average value for triangle positioning - position exactly on the line
                last_rolling_value = rollingAvg_clean[
                    rollingAvg_clean['Month'] == last_month_with_data
                ]['TTP'].values
                triangle_y_pos = last_rolling_value[0] if len(last_rolling_value) > 0 else avg
                
                # Create triangle annotation data positioned on the rolling average line
                triangle_data = pd.DataFrame({
                    'Month': [last_month_with_data],
                    'TTP': [triangle_y_pos],  # Exact position on rolling average line
                    'Triangle': ['⯆'],  # Down-pointing arrow
                    'Comment': [f'{prog} Average {year} = {avg}']
                })
                
                # Main bars with darker color for better label visibility
                bars = alt.Chart(df_clean).mark_bar(
                    opacity=0.8,
                    color='#4682B4'  # Darker steel blue for better contrast
                ).encode(
                    x=alt.X('Month:O', title='Month'),
                    y=alt.Y('TTP_NET:Q', title='Days', scale=alt.Scale(domain=[0, y_max]))
                )
                
                # Bar labels with darker color for visibility
                bar_labels = bars.mark_text(
                    dy=-8,
                    fontSize=11,
                    fontWeight='bold',
                    color="#0A6BBA"  # Dark blue for better visibility
                ).encode(
                    text=alt.Text('TTP_NET:Q', format='.1f')
                )
                
                # Rolling average line - keeping red as requested
                avg_line = alt.Chart(rollingAvg_clean).mark_line(
                    color='#DC143C',  # Crimson red
                    strokeWidth=3,
                    strokeDash=[5, 5]
                ).encode(
                    x=alt.X('Month:O'),
                    y=alt.Y('TTP:Q', scale=alt.Scale(domain=[0, y_max]))
                )
                
                # Time limit line - keeping orange
                limit_line = alt.Chart(pd.DataFrame({
                    'Month': list(range(1, 13)),
                    'limit': [time_limit] * 12
                })).mark_line(
                    color='#FF8C00',  # Dark orange
                    strokeWidth=2
                ).encode(
                    x=alt.X('Month:O'),
                    y=alt.Y('limit:Q', scale=alt.Scale(domain=[0, y_max]))
                )
                
                # Triangle positioned next to the rolling average line
                triangle = alt.Chart(triangle_data).mark_text(
                    dx=15,   # Small offset to the right of the data point
                    dy=-5,   # Slightly above the rolling average line
                    fontSize=25,  # Bigger arrow
                    color='orange',
                    fontWeight='bold'
                ).encode(
                    x=alt.X('Month:O'),
                    y=alt.Y('TTP:Q', scale=alt.Scale(domain=[0, y_max])),
                    text='Triangle:N'
                )
                
                # Annotation text positioned above the triangle
                annotation = alt.Chart(triangle_data).mark_text(
                    dx=15,   # Aligned with triangle
                    dy=-25,  # Above the triangle
                    fontSize=12,
                    fontWeight='bold',
                    color='#1B5390',
                    align='left'
                ).encode(
                    x=alt.X('Month:O'),
                    y=alt.Y('TTP:Q', scale=alt.Scale(domain=[0, y_max])),
                    text='Comment:N'
                )
                
                # Create unified legend data
                legend_data = pd.DataFrame({
                    'Legend': ['Monthly Values', 'Rolling Average', 'Contractual Limit'],
                    'Month': [1, 1, 1],  # Dummy values for positioning
                    'Value': [0, 0, 0],   # Dummy values
                    'Color': ['#4682B4', '#DC143C', '#FF8C00']
                })
                
                # Create legend as separate marks
                legend_bars = alt.Chart(legend_data).mark_rect(
                    width=15,
                    height=15
                ).encode(
                    x=alt.X('Legend:N', title=None, axis=alt.Axis(labelAngle=0)),
                    color=alt.Color('Color:N', scale=None, legend=None)
                ).properties(
                    width=400,
                    height=30
                ).resolve_scale(color='independent')
                
                # Main chart
                main_chart = (bars + bar_labels + avg_line + limit_line + triangle + annotation).properties(
                    width=600,
                    height=300,
                    title=alt.TitleParams(
                        text=f'{prog} {paymentType} - Time to Pay Analysis',
                        fontSize=16,
                        fontWeight='bold',
                        color='#1B5390',
                        align='center',
                        anchor='middle'
                    )
                ).resolve_scale(
                    color='independent'
                )
                
                # Combine main chart with legend
                final_chart = alt.vconcat(
                    main_chart,
                    legend_bars
                ).resolve_scale(
                    color='independent'
                )
                
                return final_chart
            
            except Exception as e:
                raise Exception(f"Error creating chart for {prog} {paymentType}: {str(e)}")


        # =============================================================================
        # FINAL GENERATION FUNCTIONS
        # =============================================================================

        def generate_charts(df_paym, cutoff, quarterly_tables):
            """
            Generate TTP charts for each programme and payment type, skipping empty tables
            """
            try:
                charts = {}
                chart_results = []
                successful_charts = {}
                failed_charts = []
                
                payment_types = ['IP', 'FP', 'EXPERTS', 'PF']
                programs = ['H2020', 'HEU']

                for prog in programs:
                    for pt in payment_types:
                        table_key = f'{prog}_{pt}_table'
                        var_name = f'{prog}_{pt}_ttp_chart'
                        
                        try:
                            # Skip chart generation if the corresponding table is empty
                            if table_key in quarterly_tables and quarterly_tables[table_key].get('is_empty', False):
                                chart_results.append((False, f"Skipped {var_name}: corresponding table is empty"))
                                print(f"⚠️ Skipping chart for {table_key} (empty table)")
                                continue

                            # Prepare data for chart
                            df_chart = df_paym[
                                (df_paym['Programme'] == prog) &
                                (df_paym['v_payment_type'] == pt)
                            ].copy()
                            
                            if df_chart.empty:
                                chart_results.append((False, f"No data available for {var_name}"))
                                failed_charts.append(f"{var_name}: No data available")
                                print(f"⚠️ No data for chart {var_name}")
                                continue
                            
                            df_chart['Month'] = pd.to_datetime(df_chart['Pay Document Date (dd/mm/yyyy)']).dt.month
                            df_chart['TTP_NET'] = pd.to_numeric(df_chart['v_TTP_NET'], errors='coerce')
                            df_chart = df_chart[df_chart['TTP_NET'] >= 0]

                            # Calculate metrics for this combination
                            current_metrics = calculate_current_ttp_metrics(df_chart, cutoff)
                            
                            # Safely extract average
                            avg_ttp_net = 0
                            if prog in current_metrics and pt in current_metrics[prog]:
                                avg_ttp_net = round(float(current_metrics[prog][pt]['avg_ttp_net']), 1)
                            
                            rolling_avg = rolling_ttp(df_chart, prog, pt)
                            df_ttp = avg_ttp(df_chart, prog, pt)

                            # Generate chart
                            chart = chart_machine_ttp(df_ttp, pt, prog, avg_ttp_net, rolling_avg)
                            
                            # Store successful chart
                            charts[f'{prog}_{pt}_chart'] = chart
                            successful_charts[var_name] = {
                                'chart': chart,
                                'programme': prog,
                                'payment_type': pt,
                                'data': df_ttp
                            }
                            
                            # Save to database
                            if report and db_path:
                                print(f"💾 Saving {var_name}...")
                                
                                insert_variable(
                                    report=report, 
                                    module="PaymentsModule", 
                                    var=var_name,
                                    value=df_ttp.to_dict() if isinstance(df_ttp, pd.DataFrame) else df_ttp,
                                    db_path=db_path, 
                                    anchor=var_name, 
                                    altair_chart=chart
                                )
                                chart_results.append((True, f"Successfully generated and saved {var_name}"))
                                print(f"   ✅ {var_name}")
                            else:
                                chart_results.append((True, f"Successfully generated {var_name} (not saved - missing db params)"))
                                print(f"   ✅ {var_name} (not saved to DB)")
                                
                        except Exception as e:
                            error_msg = f"Error generating {var_name}: {str(e)}"
                            chart_results.append((False, error_msg))
                            failed_charts.append(error_msg)
                            print(f"   ❌ {error_msg}")

                return charts, chart_results, successful_charts, failed_charts
                
            except Exception as e:
                raise Exception(f"Error in generate_charts: {str(e)}")


        # =============================================================================
        # MAIN USAGE FOR CHARTS
        # =============================================================================

        def main_charts(df_paym, cutoff, quarterly_tables):
            """
            Main function to generate TTP charts with comprehensive error handling
            """
            try:
                print("📊 Generating TTP charts...")
                
                # Generate charts
                ttp_charts, chart_results, successful_charts, failed_charts = generate_charts(df_paym, cutoff, quarterly_tables)
                
                # Calculate summary
                successful_count = sum(1 for success, _ in chart_results if success)
                failed_count = len(failed_charts)
                total_count = len(chart_results)
                
                print(f"\n📊 Chart Generation Summary:")
                print(f"   ✅ Successful: {successful_count}")
                print(f"   ❌ Failed: {failed_count}")
                print(f"   📋 Total: {total_count}")
                
                return ttp_charts, chart_results, successful_charts, failed_charts
                
            except Exception as e:
                raise Exception(f"Error in main_charts: {str(e)}")

        # ═══════════════════════════════════════════════════════════════════
        # MAIN EXECUTION WITH ERROR HANDLING
        # ═══════════════════════════════════════════════════════════════════

        try:
            print("📊 Starting chart generation process...")
            
            # Execute main chart processing
            ttp_charts, chart_results, successful_charts, failed_charts = main_charts(df_paym, cutoff, quarterly_tables)
            
            print("✅ Chart generation process completed")
            
        except Exception as e:
            return False, f"Error during chart generation: {str(e)}", None

        # ═══════════════════════════════════════════════════════════════════
        # DETERMINE OVERALL SUCCESS AND PREPARE RESULTS
        # ═══════════════════════════════════════════════════════════════════
        
        successful_count = sum(1 for success, _ in chart_results if success)
        failed_count = len(failed_charts)
        total_count = len(chart_results)
        
        print(f"\n📈 Final TTP Charts Summary:")
        print(f"   ✅ Successful: {successful_count}")
        print(f"   ❌ Failed: {failed_count}")
        print(f"   📋 Total: {total_count}")
        
        # Prepare return results
        return_results = {
            'ttp_charts': ttp_charts,
            'successful_charts': successful_charts,
            'summary': {
                'total_charts': total_count,
                'successful': successful_count,
                'failed': failed_count,
                'successful_list': [success_msg for success, success_msg in chart_results if success],
                'failed_list': failed_charts
            }
        }
        
        # Determine success level
        if successful_count == total_count and failed_count == 0:
            success_message = f"All TTP charts generated successfully! " \
                            f"Created {successful_count} charts"
            return True, success_message, return_results
            
        elif successful_count > 0 and failed_count > 0:
            warning_message = f"TTP charts partially completed. " \
                            f"Successful: {successful_count}, Failed: {failed_count}"
            return True, warning_message, return_results
            
        else:
            error_message = f"TTP charts generation failed. " \
                          f"No charts were successfully generated. " \
                          f"Failed: {failed_count}"
            return False, error_message, return_results
            

    except Exception as e:
            # Catch any unexpected errors
            error_message = f"Unexpected error in generate_ttp_charts: {str(e)}"
            print(f"❌ {error_message}")
            return False, error_message, None
            
    finally:
        print("🏁 TTP charts generation process completed")


# ──────────────────────────────────────────────────────────────
# PAYMENTS TABLES : 5. Annex Tables TTP and Effectiveness 
# ──────────────────────────────────────────────────────────────    

def annex_tables_ttp_eff (df_paym, cutoff, db_path, report, table_colors, report_params):

    """
    Generate Annex tables for TTP and Effectiveness with comprehensive error handling
    
    Returns:
        tuple: (success: bool, message: str, results: dict or None)
    """
    
    try:
        # ═══════════════════════════════════════════════════════════════════
        # INPUT VALIDATION
        # ═══════════════════════════════════════════════════════════════════
        
        if df_paym is None or df_paym.empty:
            return False, "Input DataFrame (df_paym) is empty or None", None
            
        if cutoff is None:
            return False, "Cutoff date is required", None
            
        if db_path is None:
            return False, "Database path is required", None
            
        if report is None:
            return False, "Report parameter is required", None
            
        if report_params is None:
            return False, "Report parameters are required", None

        # Validate required columns
        required_columns = [
            'Pay Document Date (dd/mm/yyyy)', 
            'Pay Payment Key', 
            'v_TTP_NET', 
            'v_TTP_GROSS', 
            'v_payment_in_time',
            'Programme',
            'v_payment_type',
            'call_type'
        ]
        
        missing_columns = [col for col in required_columns if col not in df_paym.columns]
        if missing_columns:
            return False, f"Missing required columns: {missing_columns}", None

        print("✅ Input validation completed successfully")

        # ═══════════════════════════════════════════════════════════════════
        # NESTED FUNCTION DEFINITIONS WITH ERROR HANDLING
        # ═══════════════════════════════════════════════════════════════════

        def create_effectiveness_breakdown(df_paym, cutoff):
            """
            Create effectiveness breakdown tables by directorate and payment type
            """
            try:
                # Filter data up to cutoff and deduplicate by Pay Payment Key
                quarter_dates = get_scope_start_end(cutoff=cutoff)
                last_valid_date = quarter_dates[1]

                df_filtered = df_paym[
                    df_paym['Pay Document Date (dd/mm/yyyy)'] <= last_valid_date
                ].copy()
                df_unique = df_filtered.drop_duplicates(subset=['Pay Payment Key']).copy()
                
                # Convert to numeric
                df_unique['v_TTP_NET'] = pd.to_numeric(df_unique['v_TTP_NET'], errors='coerce')
                df_unique['v_TTP_GROSS'] = pd.to_numeric(df_unique['v_TTP_GROSS'], errors='coerce')
                df_unique['v_payment_in_time'] = pd.to_numeric(df_unique['v_payment_in_time'], errors='coerce')
                
                # Filter out negative TTP_NET values
                df_unique = df_unique[df_unique['v_TTP_NET'] >= 0]
                
                # Determine year label from cutoff
                cutoff_date = pd.to_datetime(cutoff)
                current_year = cutoff_date.year
                year_label = f"{current_year} - YTD"
                
                return df_unique, year_label
            
            except Exception as e:
                raise Exception(f"Error in create_effectiveness_breakdown: {str(e)}")


        def create_quarterly_ttp_breakdown(df_paym, cutoff, metric_type='NET'):
            """
            Create quarterly TTP breakdown tables by directorate and payment type
            
            Parameters:
            - df_paym: Payment data DataFrame
            - cutoff: Cutoff date for analysis
            - metric_type: 'NET' or 'GROSS' for which TTP metric to use
            """

            try:

                # Filter data up to cutoff and deduplicate by Pay Payment Key
                quarter_dates = get_scope_start_end(cutoff=cutoff)
                last_valid_date = quarter_dates[1]

                df_filtered = df_paym[
                    df_paym['Pay Document Date (dd/mm/yyyy)'] <= last_valid_date
                ].copy()
                df_unique = df_filtered.drop_duplicates(subset=['Pay Payment Key']).copy()
                
                # Convert to numeric
                df_unique['v_TTP_NET'] = pd.to_numeric(df_unique['v_TTP_NET'], errors='coerce')
                df_unique['v_TTP_GROSS'] = pd.to_numeric(df_unique['v_TTP_GROSS'], errors='coerce')
                df_unique['v_payment_in_time'] = pd.to_numeric(df_unique['v_payment_in_time'], errors='coerce')
                
                # Filter out negative TTP_NET values
                df_unique = df_unique[df_unique['v_TTP_NET'] >= 0]
                
                # Extract quarter from date
                df_unique['Quarter'] = pd.to_datetime(df_unique['Pay Document Date (dd/mm/yyyy)']).dt.to_period('Q')
                
                # Create quarter labels (e.g., 2024Q1, 2024Q2, etc.)
                df_unique['Quarter_Label'] = df_unique['Quarter'].astype(str)
                
                # Get unique quarters and sort them
                quarters = sorted(df_unique['Quarter_Label'].unique())
                
                return quarters, df_unique
            
            except Exception as e:
                raise Exception(f"Error in create_quarterly_ttp_breakdown: {str(e)}")

        def create_h2020_quarterly_table(df_paym, cutoff, metric_type='NET'):
            """
            Create H2020 quarterly breakdown table with MultiIndex columns
            """
            try:
                quarters, df_unique = create_quarterly_ttp_breakdown(df_paym, cutoff, metric_type)
                
                # Filter for H2020
                h2020_data = df_unique[df_unique['Programme'] == 'H2020'].copy()
                
                # Get actual directorates from the data
                available_directorates = sorted([d for d in h2020_data['call_type'].unique() if pd.notna(d)])
                
                # Define payment types based on the image structure
                payment_types_h2020 = ['FP', 'IP']  # Final Payments and Interim Payments
                
                # Create MultiIndex columns structure
                columns = []
                
                # Add directorate columns
                for dir_name in available_directorates:
                    for pt in payment_types_h2020:
                        columns.append((dir_name, pt))
                
                # Add Experts column (single column, no sub-payment types)
                columns.append(('Experts', 'Experts'))
                
                # Add Total column  
                columns.append(('Total', 'Total'))
                
                multi_columns = pd.MultiIndex.from_tuples(columns, names=['Directorate', 'Payment_Type'])
                
                # Create DataFrame with quarters as index
                quarters_with_summary = quarters + ['Dep C.']
                quarterly_data = pd.DataFrame(index=quarters_with_summary, columns=multi_columns, dtype=float)
                
                ttp_column = f'v_TTP_{metric_type}'
                
                # Fill quarterly data
                for quarter in quarters:
                    quarter_data = h2020_data[h2020_data['Quarter_Label'] == quarter]
                    
                    # Fill directorate data
                    for dir_name in available_directorates:
                        dir_data = quarter_data[quarter_data['call_type'] == dir_name]
                        
                        for pt in payment_types_h2020:
                            pt_data = dir_data[dir_data['v_payment_type'] == pt]
                            avg_ttp = pt_data[ttp_column].mean() if len(pt_data) > 0 else 0
                            quarterly_data.loc[quarter, (dir_name, pt)] = round(avg_ttp, 1) if not pd.isna(avg_ttp) else 0.0
                    
                    # Experts
                    experts_data = quarter_data[quarter_data['v_payment_type'] == 'EXPERTS']
                    experts_avg = experts_data[ttp_column].mean() if len(experts_data) > 0 else 0
                    quarterly_data.loc[quarter, ('Experts', 'Experts')] = round(experts_avg, 1) if not pd.isna(experts_avg) else 0.0
                    
                    # Total
                    total_avg = quarter_data[ttp_column].mean() if len(quarter_data) > 0 else 0
                    quarterly_data.loc[quarter, ('Total', 'Total')] = round(total_avg, 1) if not pd.isna(total_avg) else 0.0
                
                # Fill Dep C. summary row
                for dir_name in available_directorates:
                    dir_data = h2020_data[h2020_data['call_type'] == dir_name]
                    
                    for pt in payment_types_h2020:
                        pt_data = dir_data[dir_data['v_payment_type'] == pt]
                        avg_ttp = pt_data[ttp_column].mean() if len(pt_data) > 0 else 0
                        quarterly_data.loc['Dep C.', (dir_name, pt)] = round(avg_ttp, 1) if not pd.isna(avg_ttp) else 0.0
                
                # Overall experts and total for Dep C.
                experts_overall = h2020_data[h2020_data['v_payment_type'] == 'EXPERTS'][ttp_column].mean()
                quarterly_data.loc['Dep C.', ('Experts', 'Experts')] = round(experts_overall, 1) if not pd.isna(experts_overall) else 0.0
                
                total_overall = h2020_data[ttp_column].mean()
                quarterly_data.loc['Dep C.', ('Total', 'Total')] = round(total_overall, 1) if not pd.isna(total_overall) else 0.0
                
                # Convert to numeric and handle any remaining NaN values
                quarterly_data = quarterly_data.fillna(0.0).infer_objects(copy=False).astype(float)
                
                return quarterly_data
            
            except Exception as e:
                raise Exception(f"Error creating H2020 quarterly table: {str(e)}")

        def create_heu_quarterly_table(df_paym, cutoff, metric_type='NET'):
            """
            Create HEU quarterly breakdown table with MultiIndex columns
            """
            try:
                quarters, df_unique = create_quarterly_ttp_breakdown(df_paym, cutoff, metric_type)
                
                # Filter for HEU
                heu_data = df_unique[df_unique['Programme'] == 'HEU'].copy()
                
                # Get actual directorates from the data
                available_directorates = sorted([d for d in heu_data['call_type'].unique() if pd.notna(d)])
                
                # Create MultiIndex columns structure based on image
                columns = []
                
                # Based on image: ADG(PF,IP), COG(PF,FP), POC(PF,IP), STG(PF,IP), SYG(PF)
                directorate_payment_mapping = {
                    'ADG': ['PF', 'IP'],
                    'COG': ['PF', 'FP'], 
                    'POC': ['PF', 'IP'],
                    'STG': ['PF', 'IP'],
                    'SYG': ['PF']
                }
                
                # Add columns for available directorates
                for dir_name in available_directorates:
                    if dir_name in directorate_payment_mapping:
                        for pt in directorate_payment_mapping[dir_name]:
                            columns.append((dir_name, pt))
                
                # Add Experts column
                columns.append(('Experts', 'Experts'))
                
                # Add Total column  
                columns.append(('Total', 'Total'))
                
                multi_columns = pd.MultiIndex.from_tuples(columns, names=['Directorate', 'Payment_Type'])
                
                # Create DataFrame with quarters as index
                quarters_with_summary = quarters + ['Dep C.']
                quarterly_data = pd.DataFrame(index=quarters_with_summary, columns=multi_columns, dtype=float)

                
                ttp_column = f'v_TTP_{metric_type}'
                
                # Fill quarterly data
                for quarter in quarters:
                    quarter_data = heu_data[heu_data['Quarter_Label'] == quarter]
                    
                    # Fill directorate data
                    for dir_name in available_directorates:
                        if dir_name in directorate_payment_mapping:
                            dir_data = quarter_data[quarter_data['call_type'] == dir_name]
                            
                            for pt in directorate_payment_mapping[dir_name]:
                                pt_data = dir_data[dir_data['v_payment_type'] == pt]
                                avg_ttp = pt_data[ttp_column].mean() if len(pt_data) > 0 else 0
                                quarterly_data.loc[quarter, (dir_name, pt)] = round(avg_ttp, 1) if not pd.isna(avg_ttp) else 0.0
                    
                    # Experts
                    experts_data = quarter_data[quarter_data['v_payment_type'] == 'EXPERTS']
                    experts_avg = experts_data[ttp_column].mean() if len(experts_data) > 0 else 0
                    quarterly_data.loc[quarter, ('Experts', 'Experts')] = round(experts_avg, 1) if not pd.isna(experts_avg) else 0.0
                    
                    # Total
                    total_avg = quarter_data[ttp_column].mean() if len(quarter_data) > 0 else 0
                    quarterly_data.loc[quarter, ('Total', 'Total')] = round(total_avg, 1) if not pd.isna(total_avg) else 0.0
                
                # Fill Dep C. summary row
                for dir_name in available_directorates:
                    if dir_name in directorate_payment_mapping:
                        dir_data = heu_data[heu_data['call_type'] == dir_name]
                        
                        for pt in directorate_payment_mapping[dir_name]:
                            pt_data = dir_data[dir_data['v_payment_type'] == pt]
                            avg_ttp = pt_data[ttp_column].mean() if len(pt_data) > 0 else 0
                            quarterly_data.loc['Dep C.', (dir_name, pt)] = round(avg_ttp, 1) if not pd.isna(avg_ttp) else 0.0
                
                # Overall experts and total for Dep C.
                experts_overall = heu_data[heu_data['v_payment_type'] == 'EXPERTS'][ttp_column].mean()
                quarterly_data.loc['Dep C.', ('Experts', 'Experts')] = round(experts_overall, 1) if not pd.isna(experts_overall) else 0.0
                
                total_overall = heu_data[ttp_column].mean()
                quarterly_data.loc['Dep C.', ('Total', 'Total')] = round(total_overall, 1) if not pd.isna(total_overall) else 0.0
                
                # Convert to numeric and handle any remaining NaN values
                quarterly_data = quarterly_data.fillna(0.0).infer_objects(copy=False).astype(float)
                
                return quarterly_data
            except Exception as e:
                raise Exception(f"Error creating HEU quarterly table: {str(e)}")

        def generate_quarterly_breakdown_tables(df_paym, cutoff, metric_type='NET'):
            """
            Generate both H2020 and HEU quarterly breakdown tables with MultiIndex columns
            
            Usage in Jupyter:
            h2020_table, heu_table = generate_quarterly_breakdown_tables(df_paym, cutoff, 'NET')
            
            # Display tables
            print("H2020 Quarterly Breakdown:")
            display(h2020_table)
            
            print("\nHEU Quarterly Breakdown:")
            display(heu_table)
            
            # Access MultiIndex structure:
            print("HEU columns:", heu_table.columns.tolist())
            print("Data for ADG-PF:", heu_table[('ADG', 'PF')])
            """
            try:
                h2020_table = create_h2020_quarterly_table(df_paym, cutoff, metric_type)
                heu_table = create_heu_quarterly_table(df_paym, cutoff, metric_type)
                
                return h2020_table, heu_table
            except Exception as e:
                raise Exception(f"Error generating quarterly breakdown tables: {str(e)}")


        def create_h2020_effectiveness_table(df_paym, cutoff):
            """
            Create H2020 effectiveness breakdown table by directorate
            """

            try:
                df_unique, year_label = create_effectiveness_breakdown(df_paym, cutoff)
                
                # Filter for H2020
                h2020_data = df_unique[df_unique['Programme'] == 'H2020'].copy()
                
                # Get actual directorates from the data
                available_directorates = sorted([d for d in h2020_data['call_type'].unique() if pd.notna(d)])
                
                # Build table data
                table_data = []
                
                # Calculate effectiveness for each directorate
                for dir_name in available_directorates:
                    dir_data = h2020_data[h2020_data['call_type'] == dir_name]
                    row = {'call_type': dir_name}
                    
                    # Final Payments (30 days target)
                    fp_data = dir_data[dir_data['v_payment_type'] == 'FP']
                    if len(fp_data) > 0:
                        fp_valid = fp_data[fp_data['v_payment_in_time'].notna()]
                        fp_effectiveness = (fp_data['v_payment_in_time'].sum() / len(fp_valid) * 100) if len(fp_valid) > 0 else 0
                        row[f'Final Payments - The contractual time limit is 30 days {year_label}'] = f"{fp_effectiveness:.2f}%"
                    else:
                        row[f'Final Payments - The contractual time limit is 30 days {year_label}'] = "-"
                    
                    # Interim Payments (60 days target)
                    ip_data = dir_data[dir_data['v_payment_type'] == 'IP']
                    if len(ip_data) > 0:
                        ip_valid = ip_data[ip_data['v_payment_in_time'].notna()]
                        ip_effectiveness = (ip_data['v_payment_in_time'].sum() / len(ip_valid) * 100) if len(ip_valid) > 0 else 0
                        row[f'Interim Payments - The contractual time limit is 60 days {year_label}'] = f"{ip_effectiveness:.2f}%"
                    else:
                        row[f'Interim Payments - The contractual time limit is 60 days {year_label}'] = "-"
                    
                    # Experts Payment (30 days target)
                    exp_data = dir_data[dir_data['v_payment_type'] == 'EXPERTS']
                    if len(exp_data) > 0:
                        exp_valid = exp_data[exp_data['v_payment_in_time'].notna()]
                        exp_effectiveness = (exp_data['v_payment_in_time'].sum() / len(exp_valid) * 100) if len(exp_valid) > 0 else 0
                        row[f'Experts Payment - The contractual time limit is 30 days {year_label}'] = f"{exp_effectiveness:.2f}%"
                    else:
                        row[f'Experts Payment - The contractual time limit is 30 days {year_label}'] = "-"
                    
                    # All Payments (60 days target)
                    if len(dir_data) > 0:
                        all_valid = dir_data[dir_data['v_payment_in_time'].notna()]
                        all_effectiveness = (dir_data['v_payment_in_time'].sum() / len(all_valid) * 100) if len(all_valid) > 0 else 0
                        row[f'All Payments - The contractual time limit is 60 days {year_label}'] = f"{all_effectiveness:.2f}%"
                    else:
                        row[f'All Payments - The contractual time limit is 60 days {year_label}'] = "-"
                    
                    table_data.append(row)
                
                # Add "All Payments" summary row
                all_row = {'call_type': 'All Payments'}
                
                # Calculate overall effectiveness for all directorates
                # Final Payments
                all_fp_data = h2020_data[h2020_data['v_payment_type'] == 'FP']
                if len(all_fp_data) > 0:
                    all_fp_valid = all_fp_data[all_fp_data['v_payment_in_time'].notna()]
                    all_fp_effectiveness = (all_fp_data['v_payment_in_time'].sum() / len(all_fp_valid) * 100) if len(all_fp_valid) > 0 else 0
                    all_row[f'Final Payments - The contractual time limit is 30 days {year_label}'] = f"{all_fp_effectiveness:.2f}%"
                else:
                    all_row[f'Final Payments - The contractual time limit is 30 days {year_label}'] = "-"
                
                # Interim Payments
                all_ip_data = h2020_data[h2020_data['v_payment_type'] == 'IP']
                if len(all_ip_data) > 0:
                    all_ip_valid = all_ip_data[all_ip_data['v_payment_in_time'].notna()]
                    all_ip_effectiveness = (all_ip_data['v_payment_in_time'].sum() / len(all_ip_valid) * 100) if len(all_ip_valid) > 0 else 0
                    all_row[f'Interim Payments - The contractual time limit is 60 days {year_label}'] = f"{all_ip_effectiveness:.2f}%"
                else:
                    all_row[f'Interim Payments - The contractual time limit is 60 days {year_label}'] = "-"
                
                # Experts Payment
                all_exp_data = h2020_data[h2020_data['v_payment_type'] == 'EXPERTS']
                if len(all_exp_data) > 0:
                    all_exp_valid = all_exp_data[all_exp_data['v_payment_in_time'].notna()]
                    all_exp_effectiveness = (all_exp_data['v_payment_in_time'].sum() / len(all_exp_valid) * 100) if len(all_exp_valid) > 0 else 0
                    all_row[f'Experts Payment - The contractual time limit is 30 days {year_label}'] = f"{all_exp_effectiveness:.2f}%"
                else:
                    all_row[f'Experts Payment - The contractual time limit is 30 days {year_label}'] = "-"
                
                # All Payments
                if len(h2020_data) > 0:
                    all_h2020_valid = h2020_data[h2020_data['v_payment_in_time'].notna()]
                    all_h2020_effectiveness = (h2020_data['v_payment_in_time'].sum() / len(all_h2020_valid) * 100) if len(all_h2020_valid) > 0 else 0
                    all_row[f'All Payments - The contractual time limit is 60 days {year_label}'] = f"{all_h2020_effectiveness:.2f}%"
                else:
                    all_row[f'All Payments - The contractual time limit is 60 days {year_label}'] = "-"
                
                table_data.append(all_row)
                
                return pd.DataFrame(table_data)
            
            except Exception as e:
                raise Exception(f"Error creating H2020 effectiveness table: {str(e)}")

        def create_heu_effectiveness_table(df_paym, cutoff):
            """
            Create HEU effectiveness breakdown table by directorate
            """
            try:
                df_unique, year_label = create_effectiveness_breakdown(df_paym, cutoff)
                
                # Filter for HEU
                heu_data = df_unique[df_unique['Programme'] == 'HEU'].copy()
                
                # Get actual directorates from the data
                available_directorates = sorted([d for d in heu_data['call_type'].unique() if pd.notna(d)])
                
                # Build table data
                table_data = []
                
                # Calculate effectiveness for each directorate
                for dir_name in available_directorates:
                    dir_data = heu_data[heu_data['call_type'] == dir_name]
                    row = {'call_type': dir_name}
                    
                    # Experts Payment (30 days target)
                    exp_data = dir_data[dir_data['v_payment_type'] == 'EXPERTS']
                    if len(exp_data) > 0:
                        exp_valid = exp_data[exp_data['v_payment_in_time'].notna()]
                        exp_effectiveness = (exp_data['v_payment_in_time'].sum() / len(exp_valid) * 100) if len(exp_valid) > 0 else 0
                        row[f'Experts Payment - The contractual time limit is 30 days {year_label}'] = f"{exp_effectiveness:.2f}%"
                    else:
                        row[f'Experts Payment - The contractual time limit is 30 days {year_label}'] = "-"
                    
                    # Final Payments (90 days target)
                    fp_data = dir_data[dir_data['v_payment_type'] == 'FP']
                    if len(fp_data) > 0:
                        fp_valid = fp_data[fp_data['v_payment_in_time'].notna()]
                        fp_effectiveness = (fp_data['v_payment_in_time'].sum() / len(fp_valid) * 100) if len(fp_valid) > 0 else 0
                        row[f'Final Payments - The contractual time limit is 90 days {year_label}'] = f"{fp_effectiveness:.2f}%"
                    else:
                        row[f'Final Payments - The contractual time limit is 90 days {year_label}'] = "-"
                    
                    # Interim Payments (90 days target)
                    ip_data = dir_data[dir_data['v_payment_type'] == 'IP']
                    if len(ip_data) > 0:
                        ip_valid = ip_data[ip_data['v_payment_in_time'].notna()]
                        ip_effectiveness = (ip_data['v_payment_in_time'].sum() / len(ip_valid) * 100) if len(ip_valid) > 0 else 0
                        row[f'Interim Payments - The contractual time limit is 90 days {year_label}'] = f"{ip_effectiveness:.2f}%"
                    else:
                        row[f'Interim Payments - The contractual time limit is 90 days {year_label}'] = "-"
                    
                    # Prefinancing Payments (days target - need to determine from data)
                    pf_data = dir_data[dir_data['v_payment_type'] == 'PF']
                    if len(pf_data) > 0:
                        pf_valid = pf_data[pf_data['v_payment_in_time'].notna()]
                        pf_effectiveness = (pf_data['v_payment_in_time'].sum() / len(pf_valid) * 100) if len(pf_valid) > 0 else 0
                        row[f'Prefinancing Payments - The contractual time limit is days {year_label}'] = f"{pf_effectiveness:.2f}%"
                    else:
                        row[f'Prefinancing Payments - The contractual time limit is days {year_label}'] = "-"
                    
                    # All Payments (90 days target)
                    if len(dir_data) > 0:
                        all_valid = dir_data[dir_data['v_payment_in_time'].notna()]
                        all_effectiveness = (dir_data['v_payment_in_time'].sum() / len(all_valid) * 100) if len(all_valid) > 0 else 0
                        row[f'All Payments - The contractual time limit is 90 days {year_label}'] = f"{all_effectiveness:.2f}%"
                    else:
                        row[f'All Payments - The contractual time limit is 90 days {year_label}'] = "-"
                    
                    table_data.append(row)
                
                # Add "All Payments" summary row
                all_row = {'call_type': 'All Payments'}
                
                # Calculate overall effectiveness for all directorates
                # Experts Payment
                all_exp_data = heu_data[heu_data['v_payment_type'] == 'EXPERTS']
                if len(all_exp_data) > 0:
                    all_exp_valid = all_exp_data[all_exp_data['v_payment_in_time'].notna()]
                    all_exp_effectiveness = (all_exp_data['v_payment_in_time'].sum() / len(all_exp_valid) * 100) if len(all_exp_valid) > 0 else 0
                    all_row[f'Experts Payment - The contractual time limit is 30 days {year_label}'] = f"{all_exp_effectiveness:.2f}%"
                else:
                    all_row[f'Experts Payment - The contractual time limit is 30 days {year_label}'] = "-"
                
                # Final Payments
                all_fp_data = heu_data[heu_data['v_payment_type'] == 'FP']
                if len(all_fp_data) > 0:
                    all_fp_valid = all_fp_data[all_fp_data['v_payment_in_time'].notna()]
                    all_fp_effectiveness = (all_fp_data['v_payment_in_time'].sum() / len(all_fp_valid) * 100) if len(all_fp_valid) > 0 else 0
                    all_row[f'Final Payments - The contractual time limit is 90 days {year_label}'] = f"{all_fp_effectiveness:.2f}%"
                else:
                    all_row[f'Final Payments - The contractual time limit is 90 days {year_label}'] = "-"
                
                # Interim Payments
                all_ip_data = heu_data[heu_data['v_payment_type'] == 'IP']
                if len(all_ip_data) > 0:
                    all_ip_valid = all_ip_data[all_ip_data['v_payment_in_time'].notna()]
                    all_ip_effectiveness = (all_ip_data['v_payment_in_time'].sum() / len(all_ip_valid) * 100) if len(all_ip_valid) > 0 else 0
                    all_row[f'Interim Payments - The contractual time limit is 90 days {year_label}'] = f"{all_ip_effectiveness:.2f}%"
                else:
                    all_row[f'Interim Payments - The contractual time limit is 90 days {year_label}'] = "-"
                
                # Prefinancing Payments
                all_pf_data = heu_data[heu_data['v_payment_type'] == 'PF']
                if len(all_pf_data) > 0:
                    all_pf_valid = all_pf_data[all_pf_data['v_payment_in_time'].notna()]
                    all_pf_effectiveness = (all_pf_data['v_payment_in_time'].sum() / len(all_pf_valid) * 100) if len(all_pf_valid) > 0 else 0
                    all_row[f'Prefinancing Payments - The contractual time limit is days {year_label}'] = f"{all_pf_effectiveness:.2f}%"
                else:
                    all_row[f'Prefinancing Payments - The contractual time limit is days {year_label}'] = "-"
                
                # All Payments
                if len(heu_data) > 0:
                    all_heu_valid = heu_data[heu_data['v_payment_in_time'].notna()]
                    all_heu_effectiveness = (heu_data['v_payment_in_time'].sum() / len(all_heu_valid) * 100) if len(all_heu_valid) > 0 else 0
                    all_row[f'All Payments - The contractual time limit is 90 days {year_label}'] = f"{all_heu_effectiveness:.2f}%"
                else:
                    all_row[f'All Payments - The contractual time limit is 90 days {year_label}'] = "-"
                
                table_data.append(all_row)
                
                return pd.DataFrame(table_data)
            
            except Exception as e:
                raise Exception(f"Error creating HEU effectiveness table: {str(e)}")


        def generate_effectiveness_breakdown_tables(df_paym, cutoff):
            """
            Generate both H2020 and HEU effectiveness breakdown tables (ORIGINAL STRUCTURE)
            
            Usage in Jupyter:
            h2020_eff_table, heu_eff_table = generate_effectiveness_breakdown_tables(df_paym, cutoff)
            
            # Display tables
            print("H2020 Effectiveness Breakdown:")
            display(h2020_eff_table)
            
            print("\nHEU Effectiveness Breakdown:")
            display(heu_eff_table)
            """
            try:
                h2020_eff_table = create_h2020_effectiveness_table(df_paym, cutoff)
                heu_eff_table = create_heu_effectiveness_table(df_paym, cutoff)
                
                return h2020_eff_table, heu_eff_table
            
            except Exception as e:
                raise Exception(f"Error generating effectiveness breakdown tables: {str(e)}")

        # =============================================================================
        # CLEAN TTP CALCULATION FUNCTIONS (from original file)
        # =============================================================================

        def calculate_current_ttp_metrics(df_paym, cutoff):
            """
            Calculate current TTP metrics from df_paym data
            """

            try:
                # Filter data up to cutoff and deduplicate by Pay Payment Key
                quarter_dates = get_scope_start_end(cutoff=cutoff)
                last_valid_date = quarter_dates[1]

                df_filtered = df_paym[
                    df_paym['Pay Document Date (dd/mm/yyyy)'] <= last_valid_date
                ].copy()
                df_unique = df_filtered.drop_duplicates(subset=['Pay Payment Key']).copy()
                
                # Convert to numeric
                df_unique['v_TTP_NET'] = pd.to_numeric(df_unique['v_TTP_NET'], errors='coerce')
                df_unique['v_TTP_GROSS'] = pd.to_numeric(df_unique['v_TTP_GROSS'], errors='coerce')
                df_unique['v_payment_in_time'] = pd.to_numeric(df_unique['v_payment_in_time'], errors='coerce')

                # Filter out negative TTP_NET values
                df_unique = df_unique[df_unique['v_TTP_NET'] >= 0]
                
                results = {}
                
                # Calculate by Programme and Payment Type
                for programme in ['H2020', 'HEU']:
                    prog_data = df_unique[df_unique['Programme'] == programme]
                    if len(prog_data) == 0:
                        continue
                        
                    results[programme] = {}
                    
                    # Overall programme metrics
                    prog_valid = prog_data[prog_data['v_payment_in_time'].notna()]
                    results[programme]['overall'] = {
                        'avg_ttp_net': prog_data['v_TTP_NET'].mean(),
                        'avg_ttp_gross': prog_data['v_TTP_GROSS'].mean(),
                        'on_time_pct': prog_data['v_payment_in_time'].sum() / len(prog_valid) if len(prog_valid) > 0 else 0
                    }
                    
                    # By payment type - using correct short form values from v_payment_type
                    payment_types = ['IP', 'FP', 'EXPERTS', 'PF']  # Short form values
                    
                    for payment_type in payment_types:
                        pt_data = prog_data[prog_data['v_payment_type'] == payment_type]
                        if len(pt_data) > 0:
                            pt_valid = pt_data[pt_data['v_payment_in_time'].notna()]
                            results[programme][payment_type] = {
                                'avg_ttp_net': pt_data['v_TTP_NET'].mean(),
                                'avg_ttp_gross': pt_data['v_TTP_GROSS'].mean(),
                                'on_time_pct': pt_data['v_payment_in_time'].sum() / len(pt_valid) if len(pt_valid) > 0 else 0
                            }
                
                # Overall total
                total_valid = df_unique[df_unique['v_payment_in_time'].notna()]
                results['TOTAL'] = {
                    'avg_ttp_net': df_unique['v_TTP_NET'].mean(),
                    'avg_ttp_gross': df_unique['v_TTP_GROSS'].mean(),
                    'on_time_pct': df_unique['v_payment_in_time'].sum() / len(total_valid) if len(total_valid) > 0 else 0
                }
                
                return results
            
            except Exception as e:
                raise Exception(f"Error calculating TTP metrics: {str(e)}")
            

        def load_historical_ttp_data(report_name='Quarterly_Report', db_path="database/reporting.db"):
            """
            Load historical TTP data from database
            """
            try:
                DB_PATH = Path(db_path)
                report_params = load_report_params(report_name=report_name, db_path=DB_PATH)
                
                return {
                    "TTP_NET_HISTORY": report_params.get("TTP_NET_HISTORY"),
                    "TTP_GROSS_HISTORY": report_params.get("TTP_GROSS_HISTORY"),
                    "PAYMENTS_ON_TIME_HISTORY": report_params.get("PAYMENTS_ON_TIME_HISTORY")
                }
            except Exception as e:
                raise Exception(f"Error in loading historical ttp data: {str(e)}")

        def create_ttp_comparison_table(df_paym, cutoff, historical_data):
            """
            Create TTP comparison table matching the image structure
            """

            try:
                # Calculate current metrics
                current_metrics = calculate_current_ttp_metrics(df_paym, cutoff)
                
                # Determine labels based on cutoff
                cutoff_date = pd.to_datetime(cutoff)
                current_year = cutoff_date.year
                current_label = f"{current_year}-YTD"
                historical_label = f"Dec {current_year - 1}"
                
                # Build comparison data
                comparison_data = []
                
                # H2020 section
                h2020_current = current_metrics.get('H2020', {})
                
                # H2020 - Interim Payments (IP)
                current_ip = h2020_current.get('IP', {'avg_ttp_net': 0, 'avg_ttp_gross': 0, 'on_time_pct': 0})
                comparison_data.append({
                    'Type of Payments': 'Interim Payments',
                    f'Average Net Time to Pay (in days) {current_label}': round(current_ip['avg_ttp_net'], 1),
                    f'Average Net Time to Pay (in days) {historical_label}': historical_data["TTP_NET_HISTORY"]["H2020"].get("IP", "n.a"),
                    f'Average Gross Time to Pay (in days) {current_label}': round(current_ip['avg_ttp_gross'], 1),
                    f'Average Gross Time to Pay (in days) {historical_label}': historical_data["TTP_GROSS_HISTORY"]["H2020"].get("IP", "n.a"),
                    f'Target Paid on Time - Contractually {current_label}': f"{current_ip['on_time_pct']*100:.2f}%",
                    f'Target Paid on Time - Contractually {historical_label}': f"{historical_data['PAYMENTS_ON_TIME_HISTORY']['H2020'].get('IP', 0)*100:.2f}%"
                })
                
                # H2020 - Final Payments (FP)
                current_fp = h2020_current.get('FP', {'avg_ttp_net': 0, 'avg_ttp_gross': 0, 'on_time_pct': 0})
                comparison_data.append({
                    'Type of Payments': 'Final Payments',
                    f'Average Net Time to Pay (in days) {current_label}': round(current_fp['avg_ttp_net'], 1),
                    f'Average Net Time to Pay (in days) {historical_label}': historical_data["TTP_NET_HISTORY"]["H2020"].get("FP", "n.a"),
                    f'Average Gross Time to Pay (in days) {current_label}': round(current_fp['avg_ttp_gross'], 1),
                    f'Average Gross Time to Pay (in days) {historical_label}': historical_data["TTP_GROSS_HISTORY"]["H2020"].get("FP", "n.a"),
                    f'Target Paid on Time - Contractually {current_label}': f"{current_fp['on_time_pct']*100:.2f}%",
                    f'Target Paid on Time - Contractually {historical_label}': f"{historical_data['PAYMENTS_ON_TIME_HISTORY']['H2020'].get('FP', 0)*100:.2f}%"
                })
                
                # H2020 - Experts Payments (EXPERTS)
                current_exp = h2020_current.get('EXPERTS', {'avg_ttp_net': 0, 'avg_ttp_gross': 0, 'on_time_pct': 0})
                comparison_data.append({
                    'Type of Payments': 'Experts Payments',
                    f'Average Net Time to Pay (in days) {current_label}': round(current_exp['avg_ttp_net'], 1),
                    f'Average Net Time to Pay (in days) {historical_label}': historical_data["TTP_NET_HISTORY"]["H2020"].get("Experts", "n.a"),
                    f'Average Gross Time to Pay (in days) {current_label}': round(current_exp['avg_ttp_gross'], 1),
                    f'Average Gross Time to Pay (in days) {historical_label}': historical_data["TTP_GROSS_HISTORY"]["H2020"].get("Experts", "n.a"),
                    f'Target Paid on Time - Contractually {current_label}': f"{current_exp['on_time_pct']*100:.2f}%",
                    f'Target Paid on Time - Contractually {historical_label}': f"{historical_data['PAYMENTS_ON_TIME_HISTORY']['H2020'].get('Experts', 0)*100:.2f}%"
                })
                
                # H2020 - Overall
                current_h2020 = h2020_current.get('overall', {'avg_ttp_net': 0, 'avg_ttp_gross': 0, 'on_time_pct': 0})
                comparison_data.append({
                    'Type of Payments': 'H2020',
                    f'Average Net Time to Pay (in days) {current_label}': round(current_h2020['avg_ttp_net'], 1),
                    f'Average Net Time to Pay (in days) {historical_label}': historical_data["TTP_NET_HISTORY"]["H2020"].get("H2020", "n.a"),
                    f'Average Gross Time to Pay (in days) {current_label}': round(current_h2020['avg_ttp_gross'], 1),
                    f'Average Gross Time to Pay (in days) {historical_label}': historical_data["TTP_GROSS_HISTORY"]["H2020"].get("H2020", "n.a"),
                    f'Target Paid on Time - Contractually {current_label}': f"{current_h2020['on_time_pct']*100:.2f}%",
                    f'Target Paid on Time - Contractually {historical_label}': f"{historical_data['PAYMENTS_ON_TIME_HISTORY']['H2020'].get('H2020', 0)*100:.2f}%"
                })
                
                # HEU section
                heu_current = current_metrics.get('HEU', {})
                
                # HEU - Prefinancing Payments (PF)
                current_pf = heu_current.get('PF', {'avg_ttp_net': 0, 'avg_ttp_gross': 0, 'on_time_pct': 0})
                comparison_data.append({
                    'Type of Payments': 'Prefinancing Payments',
                    f'Average Net Time to Pay (in days) {current_label}': round(current_pf['avg_ttp_net'], 1),
                    f'Average Net Time to Pay (in days) {historical_label}': historical_data["TTP_NET_HISTORY"]["HEU"].get("PF", "n.a"),
                    f'Average Gross Time to Pay (in days) {current_label}': round(current_pf['avg_ttp_gross'], 1),
                    f'Average Gross Time to Pay (in days) {historical_label}': historical_data["TTP_GROSS_HISTORY"]["HEU"].get("PF", "n.a"),
                    f'Target Paid on Time - Contractually {current_label}': f"{current_pf['on_time_pct']*100:.2f}%",
                    f'Target Paid on Time - Contractually {historical_label}': f"{historical_data['PAYMENTS_ON_TIME_HISTORY']['HEU'].get('PF', 0)*100:.2f}%"
                })
                
                # HEU - Interim Payments (IP)
                current_ip_heu = heu_current.get('IP', {'avg_ttp_net': 0, 'avg_ttp_gross': 0, 'on_time_pct': 0})
                comparison_data.append({
                    'Type of Payments': 'Interim Payments',
                    f'Average Net Time to Pay (in days) {current_label}': round(current_ip_heu['avg_ttp_net'], 1),
                    f'Average Net Time to Pay (in days) {historical_label}': historical_data["TTP_NET_HISTORY"]["HEU"].get("IP", "n.a"),
                    f'Average Gross Time to Pay (in days) {current_label}': round(current_ip_heu['avg_ttp_gross'], 1),
                    f'Average Gross Time to Pay (in days) {historical_label}': historical_data["TTP_GROSS_HISTORY"]["HEU"].get("IP", "n.a"),
                    f'Target Paid on Time - Contractually {current_label}': f"{current_ip_heu['on_time_pct']*100:.2f}%",
                    f'Target Paid on Time - Contractually {historical_label}': f"{historical_data['PAYMENTS_ON_TIME_HISTORY']['HEU'].get('IP', 0)*100:.2f}%"
                })
                
                # HEU - Final Payments (FP)
                current_fp_heu = heu_current.get('FP', {'avg_ttp_net': 0, 'avg_ttp_gross': 0, 'on_time_pct': 0})
                comparison_data.append({
                    'Type of Payments': 'Final Payments',
                    f'Average Net Time to Pay (in days) {current_label}': round(current_fp_heu['avg_ttp_net'], 1),
                    f'Average Net Time to Pay (in days) {historical_label}': historical_data["TTP_NET_HISTORY"]["HEU"].get("FP", "n.a"),
                    f'Average Gross Time to Pay (in days) {current_label}': round(current_fp_heu['avg_ttp_gross'], 1),
                    f'Average Gross Time to Pay (in days) {historical_label}': historical_data["TTP_GROSS_HISTORY"]["HEU"].get("FP", "n.a"),
                    f'Target Paid on Time - Contractually {current_label}': f"{current_fp_heu['on_time_pct']*100:.2f}%",
                    f'Target Paid on Time - Contractually {historical_label}': f"{historical_data['PAYMENTS_ON_TIME_HISTORY']['HEU'].get('FP', 0)*100:.2f}%"
                })
                
                # HEU - Experts Payment (EXPERTS)
                current_exp_heu = heu_current.get('EXPERTS', {'avg_ttp_net': 0, 'avg_ttp_gross': 0, 'on_time_pct': 0})
                comparison_data.append({
                    'Type of Payments': 'Experts Payment',
                    f'Average Net Time to Pay (in days) {current_label}': round(current_exp_heu['avg_ttp_net'], 1),
                    f'Average Net Time to Pay (in days) {historical_label}': historical_data["TTP_NET_HISTORY"]["HEU"].get("Experts", "n.a"),
                    f'Average Gross Time to Pay (in days) {current_label}': round(current_exp_heu['avg_ttp_gross'], 1),
                    f'Average Gross Time to Pay (in days) {historical_label}': historical_data["TTP_GROSS_HISTORY"]["HEU"].get("Experts", "n.a"),
                    f'Target Paid on Time - Contractually {current_label}': f"{current_exp_heu['on_time_pct']*100:.2f}%",
                    f'Target Paid on Time - Contractually {historical_label}': f"{historical_data['PAYMENTS_ON_TIME_HISTORY']['HEU'].get('Experts', 0)*100:.2f}%"
                })
                
                # HEU - Overall
                current_heu = heu_current.get('overall', {'avg_ttp_net': 0, 'avg_ttp_gross': 0, 'on_time_pct': 0})
                comparison_data.append({
                    'Type of Payments': 'HEU',
                    f'Average Net Time to Pay (in days) {current_label}': round(current_heu['avg_ttp_net'], 1),
                    f'Average Net Time to Pay (in days) {historical_label}': historical_data["TTP_NET_HISTORY"]["HEU"].get("HEU", "n.a"),
                    f'Average Gross Time to Pay (in days) {current_label}': round(current_heu['avg_ttp_gross'], 1),
                    f'Average Gross Time to Pay (in days) {historical_label}': historical_data["TTP_GROSS_HISTORY"]["HEU"].get("HEU", "n.a"),
                    f'Target Paid on Time - Contractually {current_label}': f"{current_heu['on_time_pct']*100:.2f}%",
                    f'Target Paid on Time - Contractually {historical_label}': f"{historical_data['PAYMENTS_ON_TIME_HISTORY']['HEU'].get('HEU', 0)*100:.2f}%"
                })
                
                # TOTAL row
                current_total = current_metrics.get('TOTAL', {'avg_ttp_net': 0, 'avg_ttp_gross': 0, 'on_time_pct': 0})
                comparison_data.append({
                    'Type of Payments': 'TOTAL',
                    f'Average Net Time to Pay (in days) {current_label}': round(current_total['avg_ttp_net'], 1),
                    f'Average Net Time to Pay (in days) {historical_label}': historical_data["TTP_NET_HISTORY"]["ALL"].get("TOTAL", "n.a"),
                    f'Average Gross Time to Pay (in days) {current_label}': round(current_total['avg_ttp_gross'], 1),
                    f'Average Gross Time to Pay (in days) {historical_label}': historical_data["TTP_GROSS_HISTORY"]["ALL"].get("TOTAL", "n.a"),
                    f'Target Paid on Time - Contractually {current_label}': f"{current_total['on_time_pct']*100:.2f}%",
                    f'Target Paid on Time - Contractually {historical_label}': f"{historical_data['PAYMENTS_ON_TIME_HISTORY']['ALL'].get('TOTAL', 0)*100:.2f}%"
                })
                
                return pd.DataFrame(comparison_data)
            
            except Exception as e:
                raise Exception(f"Error creating TTP comparison table : {str(e)}")


        def create_ttp_effectiveness_table(df_paym, cutoff, historical_data, report_name='Quarterly_Report', db_path="database/reporting.db"):
            """
            Create TTP effectiveness and efficiency indicators table
            """
            try:
                # Calculate current metrics
                current_metrics = calculate_current_ttp_metrics(df_paym, cutoff)
                
                # Determine labels based on cutoff using get_scope_start_end
                quarter_dates = get_scope_start_end(cutoff=cutoff)
                last_valid_date = quarter_dates[1]
                current_month = pd.to_datetime(last_valid_date).strftime('%b-%y')
                
                cutoff_date = pd.to_datetime(cutoff)
                current_year = cutoff_date.year
                historical_label = f"Dec-{str(current_year-1)[-2:]}"
                
                # Load database parameters for admin and expert meetings
                from pathlib import Path
                DB_PATH = Path(db_path)
                report_params = load_report_params(report_name=report_name, db_path=DB_PATH)
                
                admin_eff = report_params.get("Administrative_expenditure_Effectiveness", {})
                admin_ttp = report_params.get("Administrative_expenditure_ttp", {})
                expt_meet_eff = report_params.get("Expert_meetings_Effectiveness", {})
                expt_meet_ttp = report_params.get("Expert_meetings_ttp", {})
                
                # Build effectiveness data
                effectiveness_data = []
                
                # Get current metrics for calculations
                h2020_current = current_metrics.get('H2020', {})
                heu_current = current_metrics.get('HEU', {})
                
                # Research grants - Interim Payments - H2020
                h2020_ip = h2020_current.get('IP', {}).get('on_time_pct', 0)
                effectiveness_data.append({
                    'Time-to–Pay: % of payments made on time (H2020 - HEU)': 'Research grants - Interim Payments - H2020',
                    current_month: f"{h2020_ip*100:.2f}%",
                    historical_label: f"{historical_data['PAYMENTS_ON_TIME_HISTORY']['H2020'].get('IP', 0)*100:.2f}%",
                    'Target': '95% in 90 days'
                })
                
                # Research grants - Final Payments - H2020
                h2020_fp = h2020_current.get('FP', {}).get('on_time_pct', 0)
                effectiveness_data.append({
                    'Time-to–Pay: % of payments made on time (H2020 - HEU)': 'Research grants - Final Payments - H2020',
                    current_month: f"{h2020_fp*100:.2f}%",
                    historical_label: f"{historical_data['PAYMENTS_ON_TIME_HISTORY']['H2020'].get('FP', 0)*100:.2f}%",
                    'Target': '95% in 90 days'
                })
                
                # Experts with Appointment Letters H2020
                h2020_exp = h2020_current.get('EXPERTS', {}).get('on_time_pct', 0)
                effectiveness_data.append({
                    'Time-to–Pay: % of payments made on time (H2020 - HEU)': 'Experts with Appointment Letters H2020',
                    current_month: f"{h2020_exp*100:.2f}%",
                    historical_label: f"{historical_data['PAYMENTS_ON_TIME_HISTORY']['H2020'].get('Experts', 0)*100:.2f}%",
                    'Target': '95% in 30 days'
                })
                
                # Research grants - Pre-financings HEU
                heu_pf = heu_current.get('PF', {}).get('on_time_pct', 0)
                effectiveness_data.append({
                    'Time-to–Pay: % of payments made on time (H2020 - HEU)': 'Research grants - Pre-financings HEU',
                    current_month: f"{heu_pf*100:.2f}%",
                    historical_label: f"{historical_data['PAYMENTS_ON_TIME_HISTORY']['HEU'].get('PF', 0)*100:.2f}%",
                    'Target': '95% in 30 days'
                })
                
                # Research grants - Interim Payments HEU
                heu_ip = heu_current.get('IP', {}).get('on_time_pct', 0)
                effectiveness_data.append({
                    'Time-to–Pay: % of payments made on time (H2020 - HEU)': 'Research grants - Interim Payments HEU',
                    current_month: f"{heu_ip*100:.2f}%",
                    historical_label: f"{historical_data['PAYMENTS_ON_TIME_HISTORY']['HEU'].get('IP', 0)*100:.2f}%",
                    'Target': '95% in 90 days'
                })
                
                # Research grants - Final Payments HEU
                heu_fp = heu_current.get('FP', {}).get('on_time_pct', 0)
                effectiveness_data.append({
                    'Time-to–Pay: % of payments made on time (H2020 - HEU)': 'Research grants - Final Payments HEU',
                    current_month: f"{heu_fp*100:.2f}%",
                    historical_label: f"{historical_data['PAYMENTS_ON_TIME_HISTORY']['HEU'].get('FP', 0)*100:.2f}%",
                    'Target': '95% in 90 days'
                })
                
                # Administrative expenditure (from database)
                admin_current = admin_eff.get("Current", 0)
                admin_old = admin_eff.get("Old", 0)
                admin_target = admin_eff.get("Target", "99% in 30 days")
                
                # Format admin values - handle both percentage (0.985) and already formatted (98.5%) values
                if isinstance(admin_current, (int, float)) and admin_current <= 1:
                    admin_current_str = f"{admin_current*100:.2f}%"
                else:
                    admin_current_str = str(admin_current) if admin_current != "na" else "n/a"
                    
                if isinstance(admin_old, (int, float)) and admin_old <= 1:
                    admin_old_str = f"{admin_old*100:.2f}%"
                else:
                    admin_old_str = str(admin_old) if admin_old != "na" else "n/a"
                
                effectiveness_data.append({
                    'Time-to–Pay: % of payments made on time (H2020 - HEU)': 'Administrative expenditure',
                    current_month: admin_current_str,
                    historical_label: admin_old_str,
                    'Target': admin_target
                })
                
                # Experts with Appointment Letters HEU
                heu_exp = heu_current.get('EXPERTS', {}).get('on_time_pct', 0)
                effectiveness_data.append({
                    'Time-to–Pay: % of payments made on time (H2020 - HEU)': 'Experts with Appointment Letters HEU',
                    current_month: f"{heu_exp*100:.2f}%",
                    historical_label: f"{historical_data['PAYMENTS_ON_TIME_HISTORY']['HEU'].get('Experts', 0)*100:.2f}%",
                    'Target': '95% in 30 days'
                })
                
                # Expert meetings PMO (from database)
                expt_current = expt_meet_eff.get("Current", "na")
                expt_old = expt_meet_eff.get("Old", "na")
                expt_target = expt_meet_eff.get("Target", "n/a")
                
                # Format expert values - handle both percentage and string values
                if isinstance(expt_current, (int, float)) and expt_current <= 1:
                    expt_current_str = f"{expt_current*100:.2f}%"
                else:
                    expt_current_str = str(expt_current) if expt_current != "na" else "n/a"
                    
                if isinstance(expt_old, (int, float)) and expt_old <= 1:
                    expt_old_str = f"{expt_old*100:.2f}%"
                else:
                    expt_old_str = str(expt_old) if expt_old != "na" else "n/a"
                
                effectiveness_data.append({
                    'Time-to–Pay: % of payments made on time (H2020 - HEU)': 'Expert meetings PMO',
                    current_month: expt_current_str,
                    historical_label: expt_old_str,
                    'Target': expt_target
                })
                
                return pd.DataFrame(effectiveness_data)
            except Exception as e:
                raise Exception(f"Error creating TTP effectiveness and efficiency indicators table: {str(e)}")

        def create_ttp_days_table(df_paym, cutoff, historical_data, report_name='Quarterly_Report', db_path="database/reporting.db"):
            """
            Create Time to Pay: Average number of days (H2020 - HEU) table
            """
            try:
                # Calculate current metrics
                current_metrics = calculate_current_ttp_metrics(df_paym, cutoff)
                
                # Load database parameters for admin and expert meetings
                from pathlib import Path
                DB_PATH = Path(db_path)
                report_params = load_report_params(report_name=report_name, db_path=DB_PATH)
                
                admin_eff = report_params.get("Administrative_expenditure_Effectiveness", {})
                admin_ttp = report_params.get("Administrative_expenditure_ttp", {})
                expt_meet_eff = report_params.get("Expert_meetings_Effectiveness", {})
                expt_meet_ttp = report_params.get("Expert_meetings_ttp", {})
                
                # Build days data
                days_data = []
                
                # Get current metrics for calculations
                h2020_current = current_metrics.get('H2020', {})
                heu_current = current_metrics.get('HEU', {})
                
                # Research grants (days) - Interim Payments- H2020
                h2020_ip = h2020_current.get('IP', {})
                days_data.append({
                    'Time to Pay: Average number of days (H2020 - HEU)': 'Research grants (days) - Interim Payments- H2020',
                    'NET': round(h2020_ip.get('avg_ttp_net', 0), 1),
                    'GROSS': round(h2020_ip.get('avg_ttp_gross', 0), 1),
                    'Target': '90 days'
                })
                
                # Research grants (days) - Final Payments- H2020
                h2020_fp = h2020_current.get('FP', {})
                days_data.append({
                    'Time to Pay: Average number of days (H2020 - HEU)': 'Research grants (days) - Final Payments- H2020',
                    'NET': round(h2020_fp.get('avg_ttp_net', 0), 1),
                    'GROSS': round(h2020_fp.get('avg_ttp_gross', 0), 1),
                    'Target': '90 days'
                })
                
                # Experts with Appointment Letters (days) H2020
                h2020_exp = h2020_current.get('EXPERTS', {})
                days_data.append({
                    'Time to Pay: Average number of days (H2020 - HEU)': 'Experts with Appointment Letters (days) H2020',
                    'NET': round(h2020_exp.get('avg_ttp_net', 0), 1),
                    'GROSS': round(h2020_exp.get('avg_ttp_gross', 0), 1),
                    'Target': '30 days'
                })
                
                # Research grants (days) - Pre-financings - HEU
                heu_pf = heu_current.get('PF', {})
                days_data.append({
                    'Time to Pay: Average number of days (H2020 - HEU)': 'Research grants (days) - Pre-financings - HEU',
                    'NET': round(heu_pf.get('avg_ttp_net', 0), 1),
                    'GROSS': round(heu_pf.get('avg_ttp_gross', 0), 1),
                    'Target': '30 days'
                })
                
                # Research grants (days) - Interim Payments- HEU
                heu_ip = heu_current.get('IP', {})
                days_data.append({
                    'Time to Pay: Average number of days (H2020 - HEU)': 'Research grants (days) - Interim Payments- HEU',
                    'NET': round(heu_ip.get('avg_ttp_net', 0), 1),
                    'GROSS': round(heu_ip.get('avg_ttp_gross', 0), 1),
                    'Target': '90 days'
                })
                
                # Research grants (days) - Final Payments- HEU
                heu_fp = heu_current.get('FP', {})
                days_data.append({
                    'Time to Pay: Average number of days (H2020 - HEU)': 'Research grants (days) - Final Payments- HEU',
                    'NET': round(heu_fp.get('avg_ttp_net', 0), 1),
                    'GROSS': round(heu_fp.get('avg_ttp_gross', 0), 1),
                    'Target': '90 days'
                })
                
                # Expert with Appointment Letter (days) HEU
                heu_exp = heu_current.get('EXPERTS', {})
                days_data.append({
                    'Time to Pay: Average number of days (H2020 - HEU)': 'Expert with Appointment Letter (days) HEU',
                    'NET': round(heu_exp.get('avg_ttp_net', 0), 1),
                    'GROSS': round(heu_exp.get('avg_ttp_gross', 0), 1),
                    'Target': '30 days'
                })
                
                # Expert meetings (PMO) (days) - from database
                expt_net_current = expt_meet_ttp.get("Current", "na")
                expt_gross_current = expt_meet_ttp.get("Current", "na")  # Assuming same for both NET and GROSS
                
                # Format expert meeting values
                if isinstance(expt_net_current, (int, float)):
                    expt_net_str = str(round(expt_net_current, 1))
                    expt_gross_str = str(round(expt_gross_current, 1))
                else:
                    expt_net_str = "n/a"
                    expt_gross_str = "n/a"
                
                days_data.append({
                    'Time to Pay: Average number of days (H2020 - HEU)': 'Expert meetings (PMO) (days)',
                    'NET': expt_net_str,
                    'GROSS': expt_gross_str,
                    'Target': '30 days'
                })
                
                # Administrative expenditure (days) - from database
                admin_net_current = admin_ttp.get("Current", 0)
                admin_gross_current = admin_ttp.get("Current", 0)  # Assuming same for both NET and GROSS
                
                # Format admin values
                if isinstance(admin_net_current, (int, float)):
                    admin_net_str = str(round(admin_net_current, 1))
                    admin_gross_str = str(round(admin_gross_current, 1))
                else:
                    admin_net_str = "n/a"
                    admin_gross_str = "n/a"
                
                days_data.append({
                    'Time to Pay: Average number of days (H2020 - HEU)': 'Administrative expenditure (days)',
                    'NET': admin_net_str,
                    'GROSS': admin_gross_str,
                    'Target': '30 days'
                })
                
                return pd.DataFrame(days_data)
            
            except Exception as e:
                    raise Exception(f"Error creating Time to Pay: {str(e)}")

        def generate_all_ttp_tables(df_paym, cutoff, report_name='Quarterly_Report', db_path="database/reporting.db"):
                """
                Generate all three TTP tables - comparison table, effectiveness table, and days table
                
                Usage in Jupyter:
                comparison_table, effectiveness_table, days_table = generate_all_ttp_tables(df_paym, cutoff)
                """
                try:
                    # Load historical data
                    historical_data = load_historical_ttp_data(report_name=report_name, db_path=db_path)
                    
                    # Create all three tables
                    comparison_table = create_ttp_comparison_table(df_paym, cutoff, historical_data)
                    effectiveness_table = create_ttp_effectiveness_table(df_paym, cutoff, historical_data, report_name, db_path)
                    days_table = create_ttp_days_table(df_paym, cutoff, historical_data, report_name, db_path)
                    
                    return comparison_table, effectiveness_table, days_table
            
                except Exception as e:
                    raise Exception(f"Error creating Time to Pay: {str(e)}")

        def generate_complete_ttp_suite(df_paym, cutoff, report_name='Quarterly_Report', db_path="database/reporting.db"):
            """
            Generate the complete TTP report suite with all table variations
            
            Usage in Jupyter:
            tables = generate_complete_ttp_suite(df_paym, cutoff)
            
            # Access any table:
            comparison_table = tables['comparison']
            effectiveness_table = tables['effectiveness'] 
            days_table = tables['days']
            h2020_net_quarterly = tables['h2020_net_quarterly']  # MultiIndex DataFrame
            heu_net_quarterly = tables['heu_net_quarterly']      # MultiIndex DataFrame  
            h2020_gross_quarterly = tables['h2020_gross_quarterly']
            heu_gross_quarterly = tables['heu_gross_quarterly']
            h2020_effectiveness = tables['h2020_effectiveness']  # Clean table like Picture 1
            heu_effectiveness = tables['heu_effectiveness']      # Clean table like Picture 1
            
            # Example: Access specific data from MultiIndex quarterly table
            print("ADG PF data:", tables['heu_net_quarterly'][('ADG', 'PF')])
            """
            try:
            
                # Generate summary tables
                comparison_table, effectiveness_table, days_table = generate_all_ttp_tables(df_paym, cutoff, report_name, db_path)
                
                # Generate quarterly breakdown tables (days) with MultiIndex - FOR GREAT_TABLES
                h2020_net_quarterly, heu_net_quarterly = generate_quarterly_breakdown_tables(df_paym, cutoff, 'NET')
                h2020_gross_quarterly, heu_gross_quarterly = generate_quarterly_breakdown_tables(df_paym, cutoff, 'GROSS')
                
                # Generate effectiveness breakdown tables (percentages) - CLEAN STRUCTURE like Picture 1
                h2020_effectiveness, heu_effectiveness = generate_effectiveness_breakdown_tables(df_paym, cutoff)
                
                return {
                    'comparison': comparison_table,
                    'effectiveness': effectiveness_table,
                    'days': days_table,
                    'h2020_net_quarterly': h2020_net_quarterly,
                    'heu_net_quarterly': heu_net_quarterly,
                    'h2020_gross_quarterly': h2020_gross_quarterly,
                    'heu_gross_quarterly': heu_gross_quarterly,
                    'h2020_effectiveness': h2020_effectiveness,
                    'heu_effectiveness': heu_effectiveness
                }
            except Exception as e:
                    raise Exception(f"Error generating complete ttp suite: {str(e)}")
        # =============================================================================
        # UPDATED MAIN FUNCTIONS - USE THESE IN JUPYTER
        # =============================================================================

        def generate_ttp_tables(df_paym, cutoff, report_name='Quarterly_Report', db_path="database/reporting.db"):
            """
            Main function to generate all TTP tables
            
            Usage in Jupyter:
            comparison_table, effectiveness_table, days_table = generate_ttp_tables(df_paym, cutoff)
            
            # Display all three summary tables
            print("TTP Comparison Table:")
            display(comparison_table)
            
            print("\nEffectiveness and Efficiency Indicators:")
            display(effectiveness_table)
            
            print("\nTime to Pay: Average number of days:")
            display(days_table)
            """
            return generate_all_ttp_tables(df_paym, cutoff, report_name, db_path)

        def generate_quarterly_tables_for_great_tables(df_paym, cutoff):
            """
            Generate quarterly breakdown tables specifically formatted for great_tables (MultiIndex)
            
            Usage in Jupyter:
            h2020_net, heu_net, h2020_gross, heu_gross = generate_quarterly_tables_for_great_tables(df_paym, cutoff)
            
            # These DataFrames have MultiIndex columns perfect for great_tables:
            # - Level 0: Directorate (ADG, COG, POC, STG, SYG, Experts, Total)  
            # - Level 1: Payment Type (PF, IP, FP, EXPERTS)
            # - Index: Quarters (2024Q1, 2024Q2, etc.) + "Dep C." summary row
            
            # Example usage:
            print("HEU NET quarterly table structure:")
            print("Columns:", heu_net.columns.tolist())
            print("Index:", heu_net.index.tolist())
            print("ADG-PF column:", heu_net[('ADG', 'PF')])
            """
            try:
                h2020_net, heu_net = generate_quarterly_breakdown_tables(df_paym, cutoff, 'NET')
                h2020_gross, heu_gross = generate_quarterly_breakdown_tables(df_paym, cutoff, 'GROSS')
                
                return h2020_net, heu_net, h2020_gross, heu_gross
            
            except Exception as e:
                    raise Exception(f"Error generating quarterly tables for great tables: {str(e)}")

        def generate_effectiveness_tables_for_display(df_paym, cutoff):
            """
            Generate effectiveness breakdown tables for regular display (Clean structure like Picture 1)
            
            Usage in Jupyter:
            h2020_eff, heu_eff = generate_effectiveness_tables_for_display(df_paym, cutoff)
            
            # These DataFrames have clean structure like Picture 1:
            # - Rows: Directorates (ADG, COG, POC, STG, SYG, All Payments)
            # - Columns: Payment types with descriptive headers
            # - Values: Formatted percentages like "100.00%"
            
            # Example usage:
            print("H2020 Effectiveness Table:")
            display(h2020_eff)
            """
            return generate_effectiveness_breakdown_tables(df_paym, cutoff)
        

        def format_tables_ttp_annex(df, table_colors=None, table_title='nothing'):
            """
            Format TTP table with MultiIndex columns converted to GT spanners
            
            Args:
                df: DataFrame with MultiIndex columns
                table_colors: Dictionary with custom colors
                table_title: Table title
            """
            try:
                # Default colors if not provided
                if table_colors is None:
                    table_colors = {}
                
                BLUE = table_colors.get("BLUE", "#004A99")
                LIGHT_BLUE = table_colors.get("LIGHT_BLUE", "#d6e6f4")
                DARK_BLUE = table_colors.get("DARK_BLUE", "#01244B") 
                SUB_TOTAL_BACKGROUND = table_colors.get("subtotal_background_color", "#E6E6FA")
                
                # Step 1: Flatten MultiIndex columns
                df_flat = df.copy()
                
                if isinstance(df.columns, pd.MultiIndex):
                    # Combine MultiIndex levels into single column names
                    # Format: "Level0_Level1" or just "Level1" if Level0 is empty/duplicate
                    new_columns = []
                    level_0_groups = {}  # Track which columns belong to which top-level group
                    
                    for i, (level0, level1) in enumerate(df.columns):
                        if pd.isna(level0) or level0 == '' or 'Unnamed' in str(level0):
                            # If top level is empty, just use level1
                            col_name = str(level1)
                        else:
                            # Combine both levels
                            col_name = f"{level0}_{level1}"
                            # Track group membership for spanners
                            if level0 not in level_0_groups:
                                level_0_groups[level0] = []
                            level_0_groups[level0].append(col_name)
                        
                        new_columns.append(col_name)
                    
                    # Apply flattened column names
                    df_flat.columns = new_columns
                    
                else:
                    # Already flat columns
                    level_0_groups = {}
                    new_columns = df.columns.tolist()
                
                # Reset index to make the first column a regular column
                df_flat = df_flat.reset_index()
                
                # Apply column renames AFTER reset_index (which creates 'index' column)
                column_renames = {
                    'index': 'Quarter',
                    'Experts_Experts': 'Experts',
                    'Total_Total': 'Total'
                }
                df_flat = df_flat.rename(columns=column_renames)
                
                # Update level_0_groups to reflect renamed columns
                if level_0_groups:
                    updated_groups = {}
                    for group_name, columns in level_0_groups.items():
                        updated_columns = [column_renames.get(col, col) for col in columns]
                        updated_groups[group_name] = updated_columns
                    level_0_groups = updated_groups
                
                # Step 2: Create GT table
                tbl = GT(df_flat)
                
                # Step 3: Add spanners for MultiIndex structure
                if level_0_groups:
                    for group_name, columns in level_0_groups.items():
                        # Only add spanner if there are multiple columns in the group
                        if len(columns) > 1:
                            tbl = tbl.tab_spanner(
                                label=group_name,
                                columns=columns
                            )
                
                # Step 4: Get data columns for formatting (exclude index column)
                data_columns = [col for col in df_flat.columns if col != df_flat.columns[0]]
                
                total_rows =  df_flat.index[  df_flat['Quarter'].astype(str).str.contains('Dep C.', case=False, na=False)].tolist()
                
                # Dynamic width calculation
                num_data_columns = len(data_columns)
                base_width_per_column = 80  # Increased for better readability
                stub_width = 200  # width for the first column
                table_width = f"{stub_width + (num_data_columns * base_width_per_column)}px"
                
                # Step 5: Apply styling
                tbl = (
                    tbl
                    .opt_stylize(style=5, color='blue')
                    .opt_table_font(font="Arial")
                    
                    .tab_header(title=md(f'**{table_title}**'))
                    
                    .tab_options(
                        table_background_color="white",
                        heading_background_color="white",
                        table_font_size='small',
                        table_font_color=DARK_BLUE,
                        table_width=table_width,
                        heading_title_font_size="16px",
                        heading_subtitle_font_size="10px",
                        heading_title_font_weight="bold",
                        row_striping_include_table_body=False,
                        row_striping_include_stub=False
                    )
                    
                    # Format numeric columns
                    .fmt_number(
                        columns=data_columns,
                        decimals=1
                    )

                    .tab_style(
                    style=[
                        style.text(color='white', weight="bold"),
                        style.fill(color="#004d80")
                    ],
                    locations=[
                        loc.body(rows=total_rows),
                        loc.stub(rows=total_rows)
                    ]
                )
                )
                
                return tbl
            
            except Exception as e:
                    raise Exception(f"Error formatring tables ttp annex: {str(e)}")


        def format_tables_effect_annex(df,table_colors=None, title = 'none'):
            """
            Format TTP table with 3 spanners: Average Net Time to Pay, Average Gross Time to Pay, Target Paid on Time
            
            Args:
                df: DataFrame with TTP data
                title: Table title
                rowname_col: Column name for row names (default: "Type of Payments")
                table_colors: Dictionary with custom colors
            """
            
            try:
                # Default colors if not provided
                if table_colors is None:
                    table_colors = {}
                
                BLUE = table_colors.get("BLUE", "#004A99")
                LIGHT_BLUE = table_colors.get("LIGHT_BLUE", "#d6e6f4")
                DARK_BLUE = table_colors.get("DARK_BLUE", "#01244B") 
                SUB_TOTAL_BACKGROUND = table_colors.get("subtotal_background_color", "#E6E6FA")
                
                columns_cetered = df.columns[1:].tolist()

                # Dynamic width calculation
                ttp_columns = df.columns[1:].to_list()
                num_data_columns = len(ttp_columns)
                base_width_per_column = 120  # pixels per column
                stub_width = 200  # width for the first column (Quarter)
                
                table_width = f"{stub_width + (num_data_columns * base_width_per_column)}px"

                total_rows = df.index[df['call_type'].astype(str).str.contains('All Payments', case=False, na=False)].tolist()
                
                df = df.rename(columns={'call_type': 'Call Type'})
                # Create and format table with 3 spanners
                tbl = (
                    GT(df)

                    .tab_header(title=md(f'**{title}**'))
                    
                    # 4. Apply theme and basic styling
                    .opt_stylize(style=5, color='blue')
                    .opt_table_font(font="Arial")
                
                    # 5. Table options
                    .tab_options(
                        table_background_color="white",
                        heading_background_color="white",
                        table_font_size='small',
                        table_font_color=DARK_BLUE,
                        table_width=table_width,
                        heading_title_font_size="16px",
                        heading_title_font_weight="bold",
                        row_striping_include_table_body=False,
                        row_striping_include_stub=False,
                        # column_labels_background_color="#004d80",
                        column_labels_font_weight='bold',
                    )
                

                    # 11. Center align data columns
                    .cols_align(
                        align="center",
                        columns=columns_cetered 
                    )

                    .tab_style(
                                style=[
                                    style.text(color='white', weight="bold"),
                                    style.fill(color="#004d80")
                                ],
                                locations=[
                                    loc.body(rows=total_rows),
                                    loc.stub(rows=total_rows)
                                ]
                            )
                            
                )
            
                return tbl
            except Exception as e:
                    raise Exception(f"Error formatring tables effect annex: {str(e)}")
        
        def create_annex_tables(formatted_table, table_data, var_name, module):                 
            
            try:         
                # Convert DataFrame to JSON-serializable format
                if isinstance(table_data, pd.DataFrame):
                    # Reset index to avoid tuple keys and convert to records
                    df_copy = table_data.reset_index()
                    # Convert column names to strings if they're tuples
                    df_copy.columns = [str(col) if isinstance(col, tuple) else col for col in df_copy.columns]
                    table_value = df_copy.to_dict('records')
                else:
                    table_value = table_data
                    
                # Save to database         
                insert_variable(             
                    report=report,             
                    module=module,             
                    var=var_name,             
                    value=table_value,             
                    db_path=db_path,             
                    anchor=var_name,             
                    gt_table=formatted_table         
                )                                
                
                return True, f"Successfully processed {var_name}"                        
                
            except Exception as e:         
                return False, f"Error processing {var_name}: {str(e)}"
            
        # ═══════════════════════════════════════════════════════════════════
        # TABLE GENERATION WITH ERROR HANDLING
        # ═══════════════════════════════════════════════════════════════════

        table_results = []
        successful_tables = {}
        failed_tables = []
        generated_tables = {}

        # ──────────────────────────────────────────────────────────────
        # 1. GENERATE TTP NET/GROSS TABLES FOR ANNEX
        # ──────────────────────────────────────────────────────────────
        
        print("📊 Generating TTP NET/GROSS tables...")
        
        h2020_net = None
        heu_net = None
        h2020_gross = None
        heu_gross = None

        #generate TTP NET Tables for Annex
        # h2020_net, heu_net, h2020_gross, heu_gross = generate_quarterly_tables_for_great_tables(df_paym, cutoff)

        try:
                print("   🔄 Generating quarterly TTP tables...")
                h2020_net, heu_net, h2020_gross, heu_gross = generate_quarterly_tables_for_great_tables(df_paym, cutoff)
                print("   ✅ Quarterly TTP tables generated successfully")
    
        except Exception as e:
                error_msg = f"Error generating quarterly TTP tables: {str(e)}"
                print(f"   ❌ {error_msg}")
                failed_tables.append(error_msg)
                table_results.append((False, error_msg))

        
       
        # ──────────────────────────────────────────────────────────────
        # 2. FORMAT TTP TABLES
        # ──────────────────────────────────────────────────────────────
        
        # h2020_net_formated = format_tables_ttp_annex(h2020_net, table_colors=table_colors, table_title='H2020')
        # heu_net_formated = format_tables_ttp_annex(heu_net, table_colors=table_colors, table_title='HEU')
        
        h2020_net_formatted = None
        heu_net_formatted = None
        
        # Format H2020 NET table
        if h2020_net is not None:
            try:
                print("   🔄 Formatting H2020 NET table...")
                h2020_net_formatted = format_tables_ttp_annex(h2020_net, table_colors=table_colors, table_title='H2020')
                print("   ✅ H2020 NET table formatted successfully")
                
            except Exception as e:
                error_msg = f"Error formatting H2020 NET table: {str(e)}"
                print(f"   ❌ {error_msg}")
                failed_tables.append(error_msg)
                table_results.append((False, error_msg))
        else:
            error_msg = "H2020 NET table not available for formatting"
            print(f"   ⚠️ {error_msg}")
            failed_tables.append(error_msg)
        
        # Format HEU NET table
        if heu_net is not None:
            try:
                print("   🔄 Formatting HEU NET table...")
                heu_net_formatted = format_tables_ttp_annex(heu_net, table_colors=table_colors, table_title='HEU')
                print("   ✅ HEU NET table formatted successfully")
                
            except Exception as e:
                error_msg = f"Error formatting HEU NET table: {str(e)}"
                print(f"   ❌ {error_msg}")
                failed_tables.append(error_msg)
                table_results.append((False, error_msg))
        else:
            error_msg = "HEU NET table not available for formatting"
            print(f"   ⚠️ {error_msg}")
            failed_tables.append(error_msg)

        # ──────────────────────────────────────────────────────────────
        # 3. GENERATE EFFECTIVENESS TABLES FOR ANNEX
        # ──────────────────────────────────────────────────────────────

        print("📊 Generating effectiveness tables...")
        
        h2020_effect = None
        heu_effect = None

        # tables = generate_complete_ttp_suite(df_paym, cutoff)
        # h2020_effect = tables['h2020_effectiveness']
        # heu_effect = tables['heu_effectiveness']

        try:
            print("   🔄 Generating complete TTP suite...")
            tables = generate_complete_ttp_suite(df_paym, cutoff)
            h2020_effect = tables['h2020_effectiveness']
            heu_effect = tables['heu_effectiveness']
            print("   ✅ Complete TTP suite generated successfully")
            
        except Exception as e:
            error_msg = f"Error generating complete TTP suite: {str(e)}"
            print(f"   ❌ {error_msg}")
            failed_tables.append(error_msg)
            table_results.append((False, error_msg))

        
        # ──────────────────────────────────────────────────────────────
        # 4. FORMAT EFFECTIVENESS TABLES
        # ──────────────────────────────────────────────────────────────
        
        h2020_effect_formatted = None
        heu_effect_formatted = None
    

        # h2020_effect_formatted = format_tables_effect_annex(h2020_effect,table_colors=table_colors, title = 'H2020')
        # heu_effect_formatted = format_tables_effect_annex(heu_effect,table_colors=table_colors, title = 'HEU')

          # Format H2020 effectiveness table
        if h2020_effect is not None:
            try:
                print("   🔄 Formatting H2020 effectiveness table...")
                h2020_effect_formatted = format_tables_effect_annex(h2020_effect, table_colors=table_colors, title='H2020')
                print("   ✅ H2020 effectiveness table formatted successfully")
                
            except Exception as e:
                error_msg = f"Error formatting H2020 effectiveness table: {str(e)}"
                print(f"   ❌ {error_msg}")
                failed_tables.append(error_msg)
                table_results.append((False, error_msg))
        else:
            error_msg = "H2020 effectiveness table not available for formatting"
            print(f"   ⚠️ {error_msg}")
            failed_tables.append(error_msg)
        
        # Format HEU effectiveness table
        if heu_effect is not None:
            try:
                print("   🔄 Formatting HEU effectiveness table...")
                heu_effect_formatted = format_tables_effect_annex(heu_effect, table_colors=table_colors, title='HEU')
                print("   ✅ HEU effectiveness table formatted successfully")
                
            except Exception as e:
                error_msg = f"Error formatting HEU effectiveness table: {str(e)}"
                print(f"   ❌ {error_msg}")
                failed_tables.append(error_msg)
                table_results.append((False, error_msg))
        else:
            error_msg = "HEU effectiveness table not available for formatting"
            print(f"   ⚠️ {error_msg}")
            failed_tables.append(error_msg)
        
        # ──────────────────────────────────────────────────────────────
        # 5. CREATE ANNEX TABLES LIST (your existing workflow)
        # ──────────────────────────────────────────────────────────────


        # annex_tables = [(h2020_net_formated , h2020_net, 'Overview_h2020_tp_net'), (heu_net_formated , heu_net, 'Overview_heu_tp_net'),
        #                 (h2020_effect_formatted , h2020_effect, 'Overview_h2020_eff'), (heu_effect_formatted , heu_effect, 'Overview_heu_eff')
        #                     ]


        # for formatted_table, data_table, var_name in annex_tables:
        #     create_annex_tables(formatted_table, data_table, var_name, module = 'AuriModule')

        print("📋 Preparing annex tables for processing...")
        
        annex_tables = []
        
        # Only add tables that were successfully generated and formatted
        if h2020_net_formatted is not None and h2020_net is not None:
            annex_tables.append((h2020_net_formatted, h2020_net, 'Overview_h2020_tp_net'))
            
        if heu_net_formatted is not None and heu_net is not None:
            annex_tables.append((heu_net_formatted, heu_net, 'Overview_heu_tp_net'))
            
        if h2020_effect_formatted is not None and h2020_effect is not None:
            annex_tables.append((h2020_effect_formatted, h2020_effect, 'Overview_h2020_eff'))
            
        if heu_effect_formatted is not None and heu_effect is not None:
            annex_tables.append((heu_effect_formatted, heu_effect, 'Overview_heu_eff'))
        
        print(f"   📊 Prepared {len(annex_tables)} tables for processing")
    
        # ──────────────────────────────────────────────────────────────
        # 6. PROCESS AND SAVE ANNEX TABLES (your existing workflow)
        # ──────────────────────────────────────────────────────────────

        print("💾 Processing and saving annex tables...")
        
        for formatted_table, data_table, var_name in annex_tables:
            try:
                print(f"   🔄 Processing {var_name}...")
                
                success, message = create_annex_tables(
                    formatted_table=formatted_table,
                    table_data=data_table,
                    var_name=var_name,
                    module='AnnexModule'
                )
                
                if success:
                    table_results.append((True, message))
                    successful_tables[var_name] = {
                        'table': data_table,
                        'formatted_table': formatted_table,
                        'description': var_name.replace('_', ' ').title(),
                        'is_empty': data_table.empty if hasattr(data_table, 'empty') else False
                    }
                    print(f"   ✅ {message}")
                    
                else:
                    table_results.append((False, message))
                    failed_tables.append(message)
                    print(f"   ❌ {message}")
                    
            except Exception as e:
                error_msg = f"Unexpected error processing {var_name}: {str(e)}"
                table_results.append((False, error_msg))
                failed_tables.append(error_msg)
                print(f"   💥 {error_msg}")

        # ═══════════════════════════════════════════════════════════════════
        # STORE RAW GENERATED TABLES FOR REFERENCE
        # ═══════════════════════════════════════════════════════════════════
        
        # Store the raw generated tables regardless of formatting/saving success
        generated_tables = {
            'h2020_net': h2020_net,
            'heu_net': heu_net,
            'h2020_gross': h2020_gross,
            'heu_gross': heu_gross,
            'h2020_effect': h2020_effect,
            'heu_effect': heu_effect
        }

        # ═══════════════════════════════════════════════════════════════════
        # DETERMINE OVERALL SUCCESS AND PREPARE RESULTS
        # ═══════════════════════════════════════════════════════════════════
        
        successful_count = sum(1 for success, _ in table_results if success)
        failed_count = len(failed_tables)
        total_attempted = len(table_results)
        total_possible = 4  # H2020 NET, HEU NET, H2020 Effect, HEU Effect
        
        print(f"\n📈 Final Annex Tables Summary:")
        print(f"   ✅ Successful: {successful_count}")
        print(f"   ❌ Failed: {failed_count}")
        print(f"   📋 Total Attempted: {total_attempted}")
        print(f"   📊 Total Possible: {total_possible}")
        
        # Prepare return results
        return_results = {
            'annex_tables': successful_tables,
            'generated_tables': generated_tables,  # Raw tables for reference
            'summary': {
                'total_possible': total_possible,
                'total_attempted': total_attempted,
                'successful': successful_count,
                'failed': failed_count,
                'successful_list': [success_msg for success, success_msg in table_results if success],
                'failed_list': failed_tables
            }
        }
        
        # Determine success level
        if successful_count >= 2 and failed_count == 0:
            # At least 2 tables successful and no failures
            success_message = f"All attempted Annex tables generated successfully! " \
                            f"Created {successful_count} tables"
            return True, success_message, return_results
            
        elif successful_count >= 2 and failed_count > 0:
            # At least 2 tables successful but some failures
            warning_message = f"Annex tables partially completed. " \
                            f"Successful: {successful_count}, Failed: {failed_count}"
            return True, warning_message, return_results
            
        elif successful_count >= 1:
            # At least 1 table successful
            warning_message = f"Annex tables minimally completed. " \
                            f"Successful: {successful_count}, Failed: {failed_count}"
            return True, warning_message, return_results
            
        else:
            # No tables successful
            error_message = f"Annex tables generation failed. " \
                          f"No tables were successfully generated. " \
                          f"Failed: {failed_count}"
            return False, error_message, return_results
        


    except Exception as e:
            # Catch any unexpected errors
            error_message = f"Unexpected error in generate_ttp_charts: {str(e)}"
            print(f"❌ {error_message}")
            return False, error_message, None
            
    finally:
        print("🏁 Annex Tables generation process completed")


# ──────────────────────────────────────────────────────────────
# PAYMENTS TABLES : 6. Payment Charts and Summary Tables
# ──────────────────────────────────────────────────────────────    

def paym_charts_summary_tables (df_paym, cutoff, db_path, report, table_colors, report_params, df_forecast ):

        """
        Generate Annex tables for TTP and Effectiveness with comprehensive error handling
        
        Returns:
            tuple: (success: bool, message: str, results: dict or None)
        """
        
        try:
            # ═══════════════════════════════════════════════════════════════════
            # INPUT VALIDATION
            # ═══════════════════════════════════════════════════════════════════
            
            if df_paym is None or df_paym.empty:
                return False, "Input DataFrame (df_paym) is empty or None", None
                
            if cutoff is None:
                return False, "Cutoff date is required", None
                
            if db_path is None:
                return False, "Database path is required", None
                
            if report is None:
                return False, "Report parameter is required", None
                
            if report_params is None:
                return False, "Report parameters are required", None

            # Validate required columns
            required_columns = [
                'Pay Document Date (dd/mm/yyyy)', 
                'Pay Payment Key', 
                'v_TTP_NET', 
                'v_TTP_GROSS', 
                'v_payment_in_time',
                'Programme',
                'v_payment_type',
                'call_type'
            ]
            
             # Validate required columns for payment analysis
            payment_analysis_columns = [
                'Fund Source',
                'v_amount_to_sum'
            ]
            
            missing_payment_columns = [col for col in payment_analysis_columns if col not in df_paym.columns]
            if missing_payment_columns:
                print(f"⚠️ Missing payment analysis columns: {missing_payment_columns}")
                print("⚠️ Payment analysis charts will be skipped")

            print("✅ Input validation completed successfully")

            # ═══════════════════════════════════════════════════════════════════
            # SECTION 1: PAYMENT ANALYSIS CHARTS (if data is available)
            # ═══════════════════════════════════════════════════════════════════

            payment_charts_generated = False
            # df_forecast = None

            print("\n📈 Generating Payment Analysis Charts...")

            vars_all = fetch_vars_for_report(report, db_path)
            if not vars_all:
                return False, "Failed to fetch variables from database", None

            # ──────────────────────────────────────────────────────────────
            # 2.1 Setup Locale and Configuration
            # ──────────────────────────────────────────────────────────────
            try:
                locale.setlocale(locale.LC_ALL, 'en_US.UTF-8')
            except:
                try:
                    locale.setlocale(locale.LC_ALL, 'C')
                except:
                    pass

            # Configuration - Update these as needed
            colorScheme = ['#1f77b4', '#ff7f0e', '#2ca02c', '#d62728', '#9467bd']
            title_color = '#333333'

            def get_current_reporting_date():
                """Get current reporting date - uses current timestamp"""
                return pd.Timestamp.now()

            def get_call_type_column(df):
                """Identify the call type column in the dataframe"""
                possible_columns = ['SCRS_CALL_TYPE', 'call_type', 'Call Type', 'v_call_type', 'Local Position', 'v_1_Program']
                for col in possible_columns:
                    if col in df.columns:
                        return col
                return None

            def prepare_payment_data(df_paym, programme, call_type=None, cutoff_date=None):
                """
                Prepare payment data for chart generation using existing date utilities
                
                Parameters:
                - df_paym: Payment dataframe
                - programme: 'H2020' or 'HEU' 
                - call_type: Specific call type (e.g., 'STG', 'ADG', etc.) or 'all' for all
                - cutoff_date: Cutoff date for reporting (default: current timestamp)
                """
                try:
                    
                    if cutoff_date is None:
                        cutoff_date = get_current_reporting_date()
                    
                    # Ensure cutoff_date is a pandas Timestamp
                    if isinstance(cutoff_date, str):
                        cutoff_date = pd.to_datetime(cutoff_date)
                    elif hasattr(cutoff_date, 'date'):  # datetime object
                        cutoff_date = pd.Timestamp(cutoff_date)
                    elif not isinstance(cutoff_date, pd.Timestamp):
                        cutoff_date = pd.Timestamp(cutoff_date)
                    
                    print(f"Preparing data for {programme} - {call_type} - Cutoff: {cutoff_date}")
                    
                    # Use existing utilities to get scope
                    reporting_year = determine_epoch_year(cutoff_date)
                    scope_start, scope_end = get_scope_start_end(cutoff_date)
                    months_list = months_in_scope(cutoff_date)
                    
                    print(f"Reporting year: {reporting_year}")
                    print(f"Scope: {scope_start} to {scope_end}")
                    print(f"Months in scope: {len(months_list)} months")
                    
                    # Copy and filter data
                    df = df_paym.copy()
                    
                    # Filter by programme
                    df = df[df['Programme'] == programme].copy()
                    
                    # Parse dates if they're strings
                    if df['Pay Document Date (dd/mm/yyyy)'].dtype == 'object':
                        df['Pay Document Date (dd/mm/yyyy)'] = pd.to_datetime(df['Pay Document Date (dd/mm/yyyy)'], 
                                                                            format='%d/%m/%Y', errors='coerce')
                    
                    # Filter by date scope (use scope_start and scope_end)
                    df = df[
                        (df['Pay Document Date (dd/mm/yyyy)'] >= scope_start) & 
                        (df['Pay Document Date (dd/mm/yyyy)'] <= scope_end)
                    ].copy()
                    
                    # Filter by fund source (equivalent to old C1, E0 filter)
                    df = df[df['Fund Source'].notna()].copy()
                    
                    # Handle call type filtering
                    call_type_col = get_call_type_column(df)
                    
                    if call_type and call_type != 'all' and call_type_col:
                        df = df[df[call_type_col] == call_type].copy()
                        print(f"Filtered by {call_type_col} = {call_type}: {len(df)} rows")
                    
                    if len(df) == 0:
                        print("No data after filtering")
                        return create_dummy_payment_data(programme, call_type, cutoff_date, reporting_year)
                    
                    # Create month column
                    df['Month'] = df['Pay Document Date (dd/mm/yyyy)'].dt.month
                    df['Year'] = df['Pay Document Date (dd/mm/yyyy)'].dt.year
                    
                    # Filter for reporting year (already filtered by scope, but double-check)
                    df = df[df['Year'] == reporting_year].copy()
                    
                    # Aggregate by month (sum amounts)
                    monthly_payments = df.groupby(['Month'])['v_amount_to_sum'].sum().reset_index()
                    monthly_payments.rename(columns={'v_amount_to_sum': 'Paid'}, inplace=True)
                    
                    # Add programme and year info
                    monthly_payments['v_1_Program'] = programme
                    monthly_payments['Year'] = reporting_year
                    
                    print(f"Monthly payments aggregated: {len(monthly_payments)} months")
                    return monthly_payments
                
                except Exception as e:
                        print(f"      Error in prepare_payment_data: {str(e)}")
                        raise

            def prepare_forecast_data(df_forecast, programme, call_type=None):
                """
                Prepare forecast data to match payment data structure
                """
                try:
                
                    df = df_forecast.copy()
                    
                    # Map the actual column names from your forecast data
                    programme_col_map = {
                        'SCRS_FMWK': 'SCRS_FMWK',  # Your actual programme column
                        'Programme': 'Programme',
                        'v_1_Program': 'v_1_Program'
                    }
                    
                    call_type_col_map = {
                        'SCRS_CALL_TYPE': 'SCRS_CALL_TYPE',  # Your actual call type column
                        'call_type': 'call_type',
                        'Local Position': 'Local Position'
                    }
                    
                    forecast_amount_col_map = {
                        'Sum(SCRS_C1_MNT)': 'Sum(SCRS_C1_MNT)',  # Your actual forecast amount column
                        'Forecast': 'Forecast',
                        'v_amount_to_sum': 'v_amount_to_sum'
                    }
                    
                    month_col_map = {
                        'Month_Num': 'Month_Num',  # Your actual month number column
                        'Month': 'Month'
                    }
                    
                    # Find programme column
                    programme_col = None
                    for col in programme_col_map.keys():
                        if col in df.columns:
                            programme_col = col
                            break
                    
                    # Find call type column  
                    call_type_col = None
                    for col in call_type_col_map.keys():
                        if col in df.columns:
                            call_type_col = col
                            break
                    
                    # Find forecast amount column
                    forecast_col = None
                    for col in forecast_amount_col_map.keys():
                        if col in df.columns:
                            forecast_col = col
                            break
                            
                    # Find month column
                    month_col = None
                    for col in month_col_map.keys():
                        if col in df.columns:
                            month_col = col
                            break
                    
                    print(f"Forecast data columns detected:")
                    print(f"  Programme: {programme_col}")
                    print(f"  Call Type: {call_type_col}")  
                    print(f"  Amount: {forecast_col}")
                    print(f"  Month: {month_col}")
                    
                    # Filter by programme
                    if programme_col:
                        df = df[df[programme_col] == programme].copy()
                        print(f"Filtered forecast by {programme_col} = {programme}: {len(df)} rows")
                    else:
                        print("No programme column found in forecast data - using all data")
                    
                    # Handle call type filtering
                    if call_type and call_type != 'all' and call_type_col:
                        df = df[df[call_type_col] == call_type].copy()
                        print(f"Filtered forecast by {call_type_col} = {call_type}: {len(df)} rows")
                    
                    if len(df) == 0:
                        print("No forecast data after filtering - creating zero forecast")
                        return pd.DataFrame({
                            'Month': range(1, 13),
                            'Forecast': [0] * 12,
                            'v_1_Program': programme
                        })
                    
                    if not forecast_col:
                        print(f"No forecast amount column found. Available columns: {list(df.columns)}")
                        print("Creating zero forecast")
                        return pd.DataFrame({
                            'Month': range(1, 13),
                            'Forecast': [0] * 12,
                            'v_1_Program': programme
                        })
                    
                    # Aggregate by month if month column exists
                    if month_col:
                        forecast_monthly = df.groupby([month_col])[forecast_col].sum().reset_index()
                        forecast_monthly.rename(columns={
                            month_col: 'Month',
                            forecast_col: 'Forecast'
                        }, inplace=True)
                    else:
                        # Distribute forecast equally across 12 months
                        total_forecast = df[forecast_col].sum()
                        forecast_monthly = pd.DataFrame({
                            'Month': range(1, 13),
                            'Forecast': [total_forecast / 12] * 12
                        })
                    
                    forecast_monthly['v_1_Program'] = programme
                    
                    print(f"Forecast monthly data prepared: {len(forecast_monthly)} months, total: {forecast_monthly['Forecast'].sum():,.0f}")
                    return forecast_monthly
                
                except Exception as e:
                        print(f"      Error in prepare_forecast_data: {str(e)}")
                        # Return zero forecast on error
                        return pd.DataFrame({
                            'Month': range(1, 13),
                            'Forecast': [0] * 12,
                            'v_1_Program': programme
                        })

            def create_dummy_payment_data(programme, call_type, cutoff_date, reporting_year):
                """Create dummy data when no payments exist"""

                try:
                    # Use the existing months_in_scope function to get the right months
                    months_list = months_in_scope(cutoff_date)
                    month_numbers = list(range(1, len(months_list) + 1))
                    
                    dummy_data = []
                    
                    for month in month_numbers:
                        dummy_data.append({
                            'Month': month,
                            'Paid': 0,
                            'v_1_Program': programme
                        })
                    
                    return pd.DataFrame(dummy_data)
                except Exception as e:
                        print(f"      Error creating dummy data: {str(e)}")
                        raise

            def merge_payment_and_forecast_data(df_paid, df_forecast, programme, budget_amount=None):
                """
                Merge payment and forecast data, calculate cumulative values and deviations
                """

                try:
                
                    # Merge on Month
                    df_merged = pd.merge(df_paid, df_forecast[['Month', 'Forecast']], 
                                        on='Month', how='outer').fillna(0)
                    
                    # Ensure we have all months 1-12
                    all_months = pd.DataFrame({'Month': range(1, 13)})
                    df_merged = pd.merge(all_months, df_merged, on='Month', how='left').fillna(0)
                    df_merged['v_1_Program'] = programme
                    
                    # Sort by month
                    df_merged.sort_values('Month', inplace=True)
                    df_merged.reset_index(drop=True, inplace=True)
                    
                    # Calculate cumulative values
                    df_merged['Paid_Cumulative'] = df_merged['Paid'].cumsum()
                    df_merged['Forecast_Cumulative'] = df_merged['Forecast'].cumsum()
                    
                    # Add budget appropriations only if available
                    df_merged['Has_Budget'] = budget_amount is not None and budget_amount > 0
                    if df_merged['Has_Budget'].iloc[0]:
                        df_merged['Appropriations'] = budget_amount
                    else:
                        df_merged['Appropriations'] = None
                    
                    # Calculate total forecast for percentage calculations
                    total_forecast = df_merged['Forecast'].sum()
                    
                    if total_forecast > 0:
                        df_merged['Consumption_Pct'] = df_merged['Paid_Cumulative'] / total_forecast
                        df_merged['Forecast_Pct'] = df_merged['Forecast_Cumulative'] / total_forecast
                        df_merged['Deviation_Pct'] = df_merged['Consumption_Pct'] - df_merged['Forecast_Pct']
                    else:
                        df_merged['Consumption_Pct'] = 0
                        df_merged['Forecast_Pct'] = 0
                        df_merged['Deviation_Pct'] = 0
                    
                    # Calculate deviation in absolute terms
                    df_merged['Deviation_Amount'] = df_merged['Paid_Cumulative'] - df_merged['Forecast_Cumulative']
                    
                    # Calculate deviation vs budget percentage only if budget is available
                    if df_merged['Has_Budget'].iloc[0]:
                        df_merged['Deviation_vs_Budget_Pct'] = df_merged['Deviation_Amount'] / budget_amount
                    else:
                        df_merged['Deviation_vs_Budget_Pct'] = None
                    
                    return df_merged
                
                except Exception as e:
                        print(f"      Error in merge_payment_and_forecast_data: {str(e)}")
                        raise


            def create_payment_chart(df_merged, programme, call_type, year, cutoff_month):
                """
                Create the payment consumption chart using Altair with proper legend
                """

                try:
                    BLUE = table_colors.get("BLUE", "#004A99")
                    LIGHT_BLUE = table_colors.get("LIGHT_BLUE", "#d6e6f4")
                    DARK_BLUE = table_colors.get("DARK_BLUE", "#01244B") 
                    SUB_TOTAL_BACKGROUND = table_colors.get("subtotal_background_color", "#E6E6FA")

                    # Filter data up to cutoff month for actual payments
                    df_display = df_merged.copy()
                    df_current = df_display[df_display['Month'] <= cutoff_month].copy()
                    
                    # Check if budget data is available
                    has_budget = df_merged['Has_Budget'].iloc[0] if len(df_merged) > 0 else False
                    
                    title_text = 'Default'
                    # Chart title
                    if call_type == 'all':
                        title_text = f"{programme} - Consumption All Calls {year}"
                    else:
                        title_text = f"{programme} - Consumption {call_type} {year}"
                    
                    # Get current month deviation for annotation
                    if len(df_current) > 0:
                        current_deviation = df_current.iloc[-1]['Deviation_Pct']
                        current_month = df_current.iloc[-1]['Month']
                    else:
                        current_deviation = 0
                        current_month = cutoff_month
                    
                    # Determine deviation color and annotation
                    if current_deviation < -0.01:  # Less than -1%
                        deviation_color = 'red'
                        deviation_comment = 'Underconsumption'
                        arrow = '➟'
                        angle_value = 90
                        dx_arrow = -50
                        dy_arrow = -5
                        dy_spread = -90
                        dx_spread = -10
                        dy_comment=-70
                        dx_comment=-40

                    elif current_deviation > 0.01:  # Greater than 1%
                        deviation_color = 'green'
                        deviation_comment = 'Overconsumption'
                        arrow = '➟'
                        angle_value = 270
                        dx_arrow = 30
                        dy_arrow = 5
                        dy_spread = -80
                        dx_spread = -10
                        dy_comment=-50
                        dx_comment=-40
                
                    else:
                        deviation_color = '#2b7a30'
                        deviation_comment = 'On Track'
                        arrow = ''
                        angle_value = 180
                        dx_arrow = -50
                        dy_arrow = -5
                        dy_spread = -100
                        dx_spread = -10
                        dy_comment=-70
                        dx_comment=-10
                
                    
                    # # Set up Altair theme
                    # def my_theme():
                    #     return {
                    #         'config': {
                    #             'view': {'continuousHeight': 350, 'continuousWidth': 550},
                    #             'range': {'category': {'scheme': colorScheme}},
                    #             'title': {
                    #                 "fontSize": 18, 
                    #                 "font": 'Lato', 
                    #                 "anchor": "center",
                    #                 'color': BLUE,
                    #                 'fontWeight': 'bold'
                    #             }
                    #         }
                    #     }
                    
                    # alt.themes.register('my_theme', my_theme)
                    # alt.themes.enable('my_theme')
                    
                    # Prepare data for consumption bars with legend
                    df_current_renamed = df_current.copy()
                    df_current_renamed['Consumption'] = df_current_renamed['Paid_Cumulative']
                
                    bar_chart = alt.Chart(df_current_renamed).mark_bar(
                    size=45,
                    opacity=0.7,
                    color='#1f77b4'  # Fixed blue color for consumption bars
                        ).encode(
                            x=alt.X('Month:O', title='Month'),
                            y=alt.Y('Paid_Cumulative:Q', title='Amount (€)', scale=alt.Scale(zero=True))
                        )
                
                    # Prepare data for forecast line with legend
                    df_display_renamed = df_display.copy()
                    df_display_renamed['Forecast'] = df_display_renamed['Forecast_Cumulative']
                    
                    forecast_line = alt.Chart(df_display_renamed).mark_line(
                    opacity=0.8,
                    strokeWidth=3,
                    color='#ff7f0e'  # Fixed orange color for forecast line
                        ).encode(
                            x='Month:O',
                            y=alt.Y('Forecast_Cumulative:Q', title='')
                        )
                    
                    # Start with base charts
                    charts_to_combine = [bar_chart, forecast_line]
                    
                    if has_budget:
                        # Appropriations line WITHOUT LEGEND
                        appropriations_line = alt.Chart(df_display_renamed).mark_line(
                            opacity=0.8,
                            strokeWidth=3,
                            color='#2ca02c'  # Fixed green color for appropriations line
                        ).encode(
                            x='Month:O',
                            y=alt.Y('Appropriations:Q', title='')
                        )
                        charts_to_combine.append(appropriations_line)
                    
                    # Percentage text on bars
                    percentage_text = alt.Chart(df_current).mark_text(
                        baseline='bottom',
                        dx=0,
                        dy=-5,
                        align='center',
                        fontSize=12,
                        fontWeight='bold',
                        color=DARK_BLUE
                    ).encode(
                        x='Month:O',
                        y='Paid_Cumulative:Q',
                        text=alt.Text('Consumption_Pct:Q', format='.1%')
                    )
                    
                    # Deviation annotation
                    deviation_base = alt.Chart(pd.DataFrame([{
                        'Month': current_month,
                        'Paid_Cumulative': df_current.iloc[-1]['Paid_Cumulative'] if len(df_current) > 0 else 0,
                        'Deviation_Pct': current_deviation,
                        'Comment': deviation_comment,
                        'Arrow': arrow
                    }]))
                    
                    deviation_text = deviation_base.mark_text(
                        dx=dx_spread,
                        dy=dy_spread,
                        fontSize=14,
                        fontWeight='bold',
                        color=deviation_color,
                        align='left'
                    ).encode(
                        x='Month:O',
                        y='Paid_Cumulative:Q',
                        text=alt.Text('Deviation_Pct:Q', format='.1%')
                    )
                    
                    deviation_comment_text = deviation_base.mark_text(
                        dx=dx_comment,
                        dy=dy_comment,
                        fontSize=11,
                        fontWeight='bold',
                        color=deviation_color,
                        align='left'
                    ).encode(
                        x='Month:O',
                        y='Paid_Cumulative:Q',
                        text='Comment:N'
                    )
                    
                    arrow_chart = deviation_base.mark_text(
                        dx=dx_arrow,
                        dy=dy_arrow,
                        angle=angle_value,
                        fontSize=35,
                        fontWeight='bold',
                        color=deviation_color,
                        align='center'
                    ).encode(
                        x='Month:O',
                        y='Paid_Cumulative:Q',
                        text='Arrow:N'
                    )

                    # Create unified legend data
                    legend_data = pd.DataFrame({
                        'Legend': ['Consumption', 'Forecast', 'Appropriations'],
                        'Month': [1, 1, 1],  # Dummy values for positioning
                        'Value': [0, 0, 0],   # Dummy values
                        'Color': ['#1f77b4', '#ff7f0e', '#2ca02c']
                    })

                    # Create legend as separate marks
                    legend_bars = alt.Chart(legend_data).mark_rect(
                        width=15,
                        height=15
                    ).encode(
                        x=alt.X('Legend:N', title=None, axis=alt.Axis(labelAngle=0)),
                        color=alt.Color('Color:N', scale=None, legend=None)
                    ).properties(
                        width=400,
                        height=30
                    ).resolve_scale(color='independent')
                
                    # Main chart
                    if has_budget:
                        main_chart = (bar_chart + appropriations_line + forecast_line + percentage_text + deviation_text +  deviation_comment_text + arrow_chart).properties(
                            width=600,
                            height=300,
                            title=alt.TitleParams(
                                text=title_text,
                                fontSize=16,
                                fontWeight='bold',
                                color='#1B5390',
                                align='center',
                                anchor='middle'
                            )
                        ).resolve_scale(
                            color='independent'
                        )
                    else:
                        main_chart = (bar_chart + forecast_line + percentage_text + deviation_text + deviation_comment_text + arrow_chart).properties(
                            width=600,
                            height=300,
                            title=alt.TitleParams(
                                text= title_text,
                                fontSize=16,
                                fontWeight='bold',
                                color='#1B5390',
                                align='center',
                                anchor='middle'
                            )
                        ).resolve_scale(
                            color='independent'
                        )

                        
                    # Combine main chart with legend
                    final_chart = alt.vconcat(
                        main_chart,
                        legend_bars
                    ).resolve_scale(
                        color='independent'
                    )
                
                    return final_chart
                   
                except Exception as e:
                        print(f"      Error in create_payment_chart: {str(e)}")
                        raise


            # Final working function with complete styling
            def format_tables_payments_complete(df, table_colors=None, program = 'HEU', call='STG', table_subtitle='Monthly Overview', stub_width=300):
                """
                Complete working version with all styling properly applied
                """

                try:
                
                    if table_colors is None:
                        table_colors = {}
                    
                    BLUE = table_colors.get("BLUE", "#004A99")
                    DARK_BLUE = table_colors.get("DARK_BLUE", "#01244B")
                    LIGHT_BLUE = table_colors.get("LIGHT_BLUE", "#d6e6f4")
                    
                    # Process dataframe
                    df_formatted = df.copy()
                    first_col = df_formatted.columns[0]
                    df_formatted = df_formatted.rename(columns={first_col: ""})
                    
                    data_columns = df_formatted.columns[1:].tolist()
                    last_month_col = data_columns[-1] if data_columns else None
                    
                    # Calculate table width
                    base_width_per_column = 80
                    table_width = f"{stub_width + (len(data_columns) * base_width_per_column)}px"
                    table_width_px = stub_width + (len(data_columns) * base_width_per_column)

                    # Payment table specific height components
                    title_height = 30        # Main title
                    subtitle_height = 20     # Subtitle
                    column_header_height = 35  # Column headers
                    row_height = 40          # Each data row (payment tables tend to be compact)
                    footer_padding = 30      # Bottom padding
                    border_padding = 20      # Extra space for borders and margins

                    # Height calculation (NEW)
                    num_rows = len(df_formatted)
                    
                    # Calculate total height
                    total_header_height = title_height + subtitle_height + column_header_height
                    total_data_height = num_rows * row_height
                    table_height_px = total_header_height + total_data_height + footer_padding + border_padding
                    
                    # Payment table adjustments
                    # Add extra height if we have deviation rows (they might need more space)
                    deviation_rows = sum(1 for idx, row in df_formatted.iterrows() 
                                    if 'deviation' in str(row.iloc[0]).lower())
                    if deviation_rows > 0:
                        table_height_px += deviation_rows * 5  # Small extra height for colored cells
                    
                    # Safety margins - ensure minimum and maximum bounds
                    table_height_px = max(300, min(table_height_px, 1200))  # Min 300px, Max 1200px
                    table_width_px = max(600, min(table_width_px, 1800))    # Min 600px, Max 1800px
                    
                    # Find deviation rows
                    money_rows = []
                    pct_rows = []
                    
                    for idx, row in df_formatted.iterrows():
                        row_text = str(row.iloc[0]).lower()
                        if 'deviation' in row_text and 'million' in row_text:
                            money_rows.append(idx)
                        elif 'deviation' in row_text and '%' in row_text:
                            pct_rows.append(idx)
                    
                    table_title = f'{program} {call} - Payment Analysis'

                    # Create table with comprehensive styling
                    tbl = (
                        GT(df_formatted)
                        .tab_header(title=md(f'**{table_title}**'), subtitle=md(f'**{table_subtitle}**'))
                        
                        # Basic table options
                        .tab_options(
                            table_background_color="white",
                            table_font_size='small',
                            table_font_color = DARK_BLUE,
                            table_width=table_width,
                            heading_title_font_size="16px",
                            heading_subtitle_font_size="12px",
                            heading_title_font_weight="bold",
                            row_striping_include_table_body=False,
                            row_striping_include_stub=False,
                            column_labels_background_color="#004d80"
                        )
                        .opt_table_font(
                                font='Arial',
                            )
                        
                        # Style ALL stub cells (row labels) - DARK BLUE background
                        .tab_style(
                            style=[
                                style.fill(color=DARK_BLUE),
                                style.text(color="white", weight="bold", size="small"),
                                style.borders(sides="all", color="white", weight="1px")
                            ],
                            locations=loc.stub()
                        )
                        
                        # Style ALL column headers - BLUE background
                        .tab_style(
                            style=[
                                style.fill(color=BLUE),
                                style.text(color="white", weight="bold", size="small"),
                                style.borders(sides="all", color="white", weight="1px")
                            ],
                            locations=loc.column_labels()
                        )
                        
                        # Style ALL body cells - basic formatting
                        .tab_style(
                            style=[
                                style.text(size="small"),
                                style.borders(sides="all", color="#cccccc", weight="1px")
                            ],
                            locations=loc.body()
                        )
                        
                        # Center align all data columns
                        .cols_align(align='center', columns=data_columns)
                        
                        # Set column widths
                        .cols_width({col: f"{base_width_per_column}px" for col in data_columns})
                    )
                    
                    # Apply conditional coloring using tab_style (more reliable than data_color)
                    if last_month_col:
                        
                        # Color money deviations
                        for row_idx in money_rows:
                            value = df_formatted.loc[row_idx, last_month_col]
                            try:
                                numeric_value = float(value)
                                if numeric_value < -10:
                                    color = "#cc0000"  # Dark red
                                    text_color = "white"
                                elif numeric_value < -1:
                                    color = "#ff6666"  # Light red
                                    text_color = "white"
                                elif numeric_value < 1:
                                    color = "#ffff99"  # Yellow
                                    text_color = "black"
                                elif numeric_value < 50:
                                    color = "#99ff99"  # Light green
                                    text_color = "black"
                                else:
                                    color = "#00cc00"  # Dark green
                                    text_color = "white"
                                
                                tbl = tbl.tab_style(
                                    style=[
                                        style.fill(color=color),
                                        style.text(color=text_color, weight="bold"),
                                        style.borders(sides="all", color="white", weight="1px")
                                    ],
                                    locations=loc.body(columns=[last_month_col], rows=[row_idx])
                                )
                                print(f"Colored money row {row_idx} with value {numeric_value} as {color}")
                            except:
                                pass
                        
                        # Color percentage deviations
                        for row_idx in pct_rows:
                            value = str(df_formatted.loc[row_idx, last_month_col])
                            try:
                                # Remove % sign and convert to float
                                numeric_value = float(value.replace('%', ''))
                                if numeric_value < -5:
                                    color = "#cc0000"  # Dark red
                                    text_color = "white"
                                elif numeric_value < -1:
                                    color = "#ff6666"  # Light red
                                    text_color = "white"
                                elif numeric_value < 1:
                                    color = "#ffff99"  # Yellow
                                    text_color = "black"
                                elif numeric_value < 10:
                                    color = "#99ff99"  # Light green
                                    text_color = "black"
                                else:
                                    color = "#00cc00"  # Dark green
                                    text_color = "white"
                                
                                tbl = tbl.tab_style(
                                    style=[
                                        style.fill(color=color),
                                        style.text(color=text_color, weight="bold"),
                                        style.borders(sides="all", color="white", weight="1px")
                                    ],
                                    locations=loc.body(columns=[last_month_col], rows=[row_idx])
                                )
                                print(f"Colored percentage row {row_idx} with value {numeric_value}% as {color}")
                            except:
                                pass
                    
                    return tbl, table_width_px, table_height_px
                except Exception as e:
                        print(f"      Error in format_tables_payments_complete: {str(e)}")
                        raise


            def create_summary_table(df_merged, cutoff_month):
                """
                Create the summary table below the chart - excludes budget rows when no budget data available
                """

                try:
                
                    # Filter to current data
                    df_table = df_merged[df_merged['Month'] <= cutoff_month].copy()
                    
                    # Check if budget data is available
                    has_budget = df_merged['Has_Budget'].iloc[0] if len(df_merged) > 0 else False
                    
                    # Convert to millions for display
                    df_table = df_table.copy()
                    df_table['Paid_M'] = df_table['Paid_Cumulative'] / 1_000_000
                    df_table['Forecast_M'] = df_table['Forecast_Cumulative'] / 1_000_000
                    df_table['Deviation_M'] = df_table['Deviation_Amount'] / 1_000_000
                    
                    # Format numbers
                    df_table['Paid_Formatted'] = df_table['Paid_M'].map('{:,.3f}'.format)
                    df_table['Forecast_Formatted'] = df_table['Forecast_M'].map('{:,.3f}'.format)
                    df_table['Consumption_Pct_Formatted'] = df_table['Consumption_Pct'].map('{:.1%}'.format)
                    df_table['Forecast_Pct_Formatted'] = df_table['Forecast_Pct'].map('{:.1%}'.format)
                    df_table['Deviation_Pct_Formatted'] = df_table['Deviation_Pct'].map('{:.1%}'.format)
                    df_table['Deviation_M_Formatted'] = df_table['Deviation_M'].map('{:,.1f}'.format)
                    
                    # Define base columns for output table
                    output_cols = [
                        'Month', 'Paid_Formatted', 'Forecast_Formatted',
                        'Deviation_M_Formatted', 'Deviation_Pct_Formatted'
                    ]
                    
                    # Add budget-related columns only if budget data is available
                    if has_budget:
                        df_table['Budget_M'] = df_table['Appropriations'] / 1_000_000
                        df_table['Budget_Formatted'] = df_table['Budget_M'].map('{:,.1f}'.format)
                        df_table['Deviation_vs_Budget_Formatted'] = df_table['Deviation_vs_Budget_Pct'].map('{:.1%}'.format)
                        
                        # Insert budget column at the beginning and deviation vs budget at the end
                        output_cols.insert(1, 'Budget_Formatted')
                        output_cols.append('Deviation_vs_Budget_Formatted')
                    
                    # Create output table
                    table_output = df_table[output_cols].copy()
                    table_output.set_index('Month', inplace=True)
                    
                    # Transpose for month columns
                    table_transposed = table_output.T
                    table_transposed.reset_index(inplace=True)
                    table_transposed.rename(columns={'index': 'Metric'}, inplace=True)
                    
                    # Define metric names based on whether budget data is available
                    if has_budget:
                        metric_names = {
                            'Budget_Formatted': 'Current Inscribed Budget',
                            'Paid_Formatted': 'Cumulative Consumption (€ million)',
                            'Forecast_Formatted': 'Cumulative Forecast (€ million)',
                            'Deviation_M_Formatted': 'Deviation vs. Cumulative Forecast (€ million)',
                            'Deviation_Pct_Formatted': 'Deviation vs. Cumulative Forecast (%)',
                            'Deviation_vs_Budget_Formatted': 'Deviation vs. Budget (%)'
                        }
                    else:
                        metric_names = {
                            'Paid_Formatted': 'Cumulative Consumption (€ million)',
                            'Forecast_Formatted': 'Cumulative Forecast (€ million)',
                            'Deviation_M_Formatted': 'Deviation vs. Cumulative Forecast (€ million)',
                            'Deviation_Pct_Formatted': 'Deviation vs. Cumulative Forecast (%)'
                        }
                    
                    table_transposed['Metric'] = table_transposed['Metric'].map(metric_names)
                    
                    # Rename month columns
                    month_names = {
                        1: 'JAN', 2: 'FEB', 3: 'MAR', 4: 'APR', 5: 'MAY', 6: 'JUN',
                        7: 'JUL', 8: 'AUG', 9: 'SEP', 10: 'OCT', 11: 'NOV', 12: 'DEC'
                    }
                    
                    table_transposed.columns = ['Metric'] + [month_names.get(col, str(col)) for col in table_transposed.columns[1:]]
                    
                    return table_transposed
                
                except Exception as e:
                        print(f"      Error in create_summary_table: {str(e)}")
                        raise


            def create_budget_config():
                """
                Create budget configuration by fetching the latest budget data
                This should be called once when setting up the charts
                """
                try:
                    # Get budget data (assuming vars_all is available globally)
                    if 'vars_all' not in globals():
                        print("Warning: vars_all not found - budget data will not be available")
                        return {
                            'H2020': {'all': None},
                            'HEU': {'all': None, 'EXPERTS': None}
                        }
                    
                    vars_data = vars_all.get('table_2a_HE')
                    h2020_vars_data = vars_all.get('table_2a_H2020')

                    # Extract Total Available Payment Appropriations
                    heu_total_appropriations = next(
                        (item['Available_Payment_Appropriations'] for item in vars_data 
                        if item['Budget Address Type'] == 'Total'), 
                        None
                    ) if vars_data else None
                    
                    heu_total_appropriations_expt = next(
                        (item['Available_Payment_Appropriations'] for item in vars_data 
                        if item['Budget Address Type'] == 'Experts'), 
                        None
                    ) if vars_data else None

                    h2020_total_appropriations = next(
                        (item['Available_Payment_Appropriations'] for item in h2020_vars_data 
                        if item['Budget Address Type'] == 'Total'), 
                        None
                    ) if h2020_vars_data else None

                    # Budget configuration - Only available for specific cases
                    budget_config = {
                        'H2020': {
                            'all': h2020_total_appropriations  # Total H2020 budget - ONLY available for 'all' call types
                        },
                        'HEU': {
                            'all': heu_total_appropriations,    # Total HEU budget - available for 'all' call types
                            'EXPERTS': heu_total_appropriations_expt   # HEU Experts budget - ONLY individual call type with budget
                        }
                    }
                    
                    print("Budget configuration created:")
                    for programme, budgets in budget_config.items():
                        for call_type, amount in budgets.items():
                            if amount:
                                print(f"  {programme} {call_type}: €{amount:,.0f}")
                            else:
                                print(f"  {programme} {call_type}: No budget data")
                    
                    return budget_config
                
                except Exception as e:
                        print(f"      Error in create_budget_config: {str(e)}")
                        return {
                            'H2020': {'all': None},
                            'HEU': {'all': None, 'EXPERTS': None}
                        }


            def check_budget_availability(programme, call_type):
                """
                Check if budget appropriations are available for the given programme/call_type combination
                """
                if programme == 'H2020':
                    return call_type == 'all'
                elif programme == 'HEU':
                    return call_type in ['all', 'EXPERTS']
                else:
                    return False

            def get_budget_amount(programme, call_type, budget_config=None):
                """
                Get budget amount for programme/call_type if available
                """
                try:
                    if budget_config is None:
                        budget_config = AVAILABLE_BUDGET_CONFIG
                    
                    if not check_budget_availability(programme, call_type):
                        return None
                    
                    try:
                        return budget_config[programme][call_type]
                    except (KeyError, TypeError):
                        return None
                except Exception as e:
                        print(f"      Error getting budget amount: {str(e)}")
                        return None

            def generate_payment_chart_and_table(df_paym, df_forecast, programme, call_type='all', 
                                            budget_amount=None, cutoff_date=None):
                """
                Main function to generate both chart and table for a specific programme/call type
                
                Parameters:
                - df_paym: Payment dataframe
                - df_forecast: Forecast dataframe  
                - programme: 'H2020' or 'HEU'
                - call_type: Specific call type or 'all'
                - budget_amount: Budget appropriation amount (if None, will try to auto-lookup)
                - cutoff_date: Reporting cutoff date
                
                Returns:
                - Dictionary with 'chart' and 'table' keys
                """
                
                try:
                    # Auto-lookup budget if not provided
                    if budget_amount is None:
                        try:
                            budget_amount = get_budget_amount(programme, call_type, AVAILABLE_BUDGET_CONFIG)
                            if budget_amount:
                                print(f"Auto-detected budget for {programme} {call_type}: €{budget_amount:,.0f}")
                            else:
                                print(f"No budget available for {programme} {call_type}")
                        except Exception as e:
                            print(f"Error looking up budget: {e}")
                            budget_amount = None
                    
                    # Get cutoff date
                    if cutoff_date is None:
                        cutoff_date = get_current_reporting_date()
                    
                    # Ensure cutoff_date is a pandas Timestamp
                    if isinstance(cutoff_date, str):
                        cutoff_date = pd.to_datetime(cutoff_date)
                    elif not isinstance(cutoff_date, pd.Timestamp):
                        cutoff_date = pd.Timestamp(cutoff_date)
                    
                    # Use existing date utilities
                    reporting_year = determine_epoch_year(cutoff_date)
                    scope_start, scope_end = get_scope_start_end(cutoff_date)
                    months_list = months_in_scope(cutoff_date)
                    cutoff_month = len(months_list)  # Number of months in scope
                    
                    print(f"\n=== Generating chart for {programme} - {call_type} ===")
                    print(f"Cutoff date: {cutoff_date}")
                    print(f"Reporting year: {reporting_year}")
                    print(f"Scope: {scope_start} to {scope_end}")
                    print(f"Months in scope: {cutoff_month}")
                    if budget_amount:
                        print(f"Budget amount: €{budget_amount:,.0f}")
                    else:
                        print("No budget amount - appropriations line will not be shown")
                    
                    # Prepare payment data
                    df_paid = prepare_payment_data(df_paym, programme, call_type, cutoff_date)
                    
                    # Prepare forecast data
                    df_forecast_prep = prepare_forecast_data(df_forecast, programme, call_type)
                    
                    # Merge and calculate
                    df_merged = merge_payment_and_forecast_data(df_paid, df_forecast_prep, programme, budget_amount)
                    
                    # Create chart
                    chart = create_payment_chart(df_merged, programme, call_type, reporting_year, cutoff_month)
                    
                    # Create table
                    table = create_summary_table(df_merged, cutoff_month)
                    
                    return {
                        'chart': chart,
                        'table': table,
                        'data': df_merged,
                        'success': True,
                        'metadata': {
                            'reporting_year': reporting_year,
                            'scope_start': scope_start,
                            'scope_end': scope_end,
                            'cutoff_month': cutoff_month,
                            'months_in_scope': months_list,
                            'budget_amount': budget_amount,
                            'has_budget': budget_amount is not None and budget_amount > 0
                        }
                    }
                    
                except Exception as e:
                    print(f"Error generating chart for {programme} - {call_type}: {str(e)}")
                    import traceback
                    traceback.print_exc()
                    return {
                        'chart': None,
                        'table': None,
                        'data': None,
                        'success': False,
                        'error': str(e)
                    }

            
            def generate_all_charts_tables(df_paym, df_forecast, programme_budgets=None, cutoff_date=None, report=report, table_colors=table_colors, db_path=db_path):
                """
                Generate charts for all available call types in both programmes
                Only applies budget appropriations where available (H2020 'all', HEU 'all' and 'EXPERTS')
                
                Parameters:
                - df_paym: Payment dataframe (required)
                - df_forecast: Forecast dataframe (optional)
                - programme_budgets: Budget configuration dict (optional, will use global if None)
                - cutoff_date: Cutoff date for analysis (optional, will use current date if None)
                
                Returns:
                - dict: Results dictionary with programme -> call_type -> result structure
                """
                
                # ═══════════════════════════════════════════════════════════════════
                # INPUT VALIDATION
                # ═══════════════════════════════════════════════════════════════════
                
                try:
                    # Validate required inputs
                    if df_paym is None or df_paym.empty:
                        print("❌ Error: df_paym is required and cannot be empty")
                        return {}
                    
                    # Check for required global variables with fallbacks
                    global logger
                    
                    # Initialize logger if not available
                    if 'logger' not in globals():
                        import logging
                        logger = logging.getLogger(__name__)
                        if not logger.handlers:
                            # Set up basic console handler if no handlers exist
                            handler = logging.StreamHandler()
                            formatter = logging.Formatter('%(asctime)s - %(name)s - %(levelname)s - %(message)s')
                            handler.setFormatter(formatter)
                            logger.addHandler(handler)
                            logger.setLevel(logging.INFO)
                    
                    # Initialize table_colors if not available
                    if 'table_colors' is None:
                        table_colors = {
                            "BLUE": "#004A99",
                            "DARK_BLUE": "#01244B",
                            "LIGHT_BLUE": "#d6e6f4",
                            "subtotal_background_color": "#E6E6FA"
                        }
                        print("⚠️ Warning: table_colors not found, using default colors")
                    
                    # Check for report and db_path
                    if report is None:
                        print("❌ Error: 'report' variable not found in global scope")
                        return {}
                    
                    if 'db_path' is None:
                        print("❌ Error: 'db_path' variable not found in global scope")
                        return {}
                    
                    # Validate programme_budgets
                    if programme_budgets is None:
                        try:
                            programme_budgets = AVAILABLE_BUDGET_CONFIG
                        except NameError:
                            print("⚠️ Warning: AVAILABLE_BUDGET_CONFIG not found, creating default budget config")
                            programme_budgets = {
                                'H2020': {'all': None},
                                'HEU': {'all': None, 'EXPERTS': None}
                            }
                    
                    print(f"🚀 Starting chart generation for {len(df_paym)} payment records...")
                    
                except Exception as e:
                    print(f"❌ Error during input validation: {str(e)}")
                    return {}
                
                # ═══════════════════════════════════════════════════════════════════
                # CORE FUNCTION LOGIC WITH ERROR HANDLING
                # ═══════════════════════════════════════════════════════════════════
                
                results = {}
                generation_stats = {
                    'total_attempted': 0,
                    'total_successful': 0,
                    'total_failed': 0,
                    'successful_items': [],
                    'failed_items': []
                }
                
                try:
                    # Get available call types
                    call_type_col = get_call_type_column(df_paym)
                    
                    if call_type_col:
                        print(f"📊 Found call type column: {call_type_col}")
                        
                        # Get unique call types per programme
                        programmes = ['H2020', 'HEU']
                        
                        for programme in programmes:
                            try:
                                print(f"\n🔄 Processing programme: {programme}")
                                
                                # Filter data for this programme
                                prog_data = df_paym[df_paym['Programme'] == programme]
                                
                                if prog_data.empty:
                                    print(f"⚠️ Warning: No data found for programme {programme}")
                                    results[programme] = {}
                                    continue
                                
                                # Get call types for this programme
                                call_types = prog_data[call_type_col].dropna().unique().tolist()
                                call_types.append('all')  # Add overall summary
                                
                                print(f"   📋 Found {len(call_types)-1} call types + 'all' summary: {call_types}")
                                
                                results[programme] = {}
                                
                                for call_type in call_types:
                                    generation_stats['total_attempted'] += 1
                                    
                                    try:
                                        print(f"   🔄 Generating chart for {programme} - {call_type}...")
                                        
                                        # Get budget only if available for this combination
                                        budget = None
                                        try:
                                            budget = get_budget_amount(programme, call_type, programme_budgets)
                                        except Exception as budget_error:
                                            print(f"      ⚠️ Warning: Could not get budget for {programme} {call_type}: {str(budget_error)}")
                                            budget = None
                                        
                                        # Generate chart and table
                                        try:
                                            result = generate_payment_chart_and_table(
                                                df_paym, df_forecast, programme, call_type, budget, cutoff_date
                                            )
                                        except Exception as generation_error:
                                            print(f"      ❌ Error generating chart/table for {programme} {call_type}: {str(generation_error)}")
                                            result = {'success': False, 'error': str(generation_error)}
                                        
                                        results[programme][call_type] = result
                                        
                                        if result.get('success', False):
                                            try:
                                                budget_status = "with budget" if budget else "without budget"
                                                
                                                chart = result.get('chart')
                                                table = result.get('table')
                                                data = result.get('data')
                                                
                                                if chart is None or table is None or data is None:
                                                    raise ValueError("Chart, table, or data is None despite success flag")
                                                
                                                # Format table with error handling
                                                try:
                                                    formatted_table, width, height = format_tables_payments_complete(
                                                        table, 
                                                        table_colors=table_colors, 
                                                        program=programme, 
                                                        call=call_type, 
                                                        table_subtitle='Monthly Overview', 
                                                        stub_width=300
                                                    )
                                                except Exception as format_error:
                                                    print(f"      ❌ Error formatting table for {programme} {call_type}: {str(format_error)}")
                                                    generation_stats['total_failed'] += 1
                                                    generation_stats['failed_items'].append(f"{programme}_{call_type} (table formatting)")
                                                    continue
                                    
                                                # Generate variable names
                                                var_name_chart = f'{programme}_{call_type}_paym_analysis_chart'
                                                var_name_table = f'{programme}_{call_type}_paym_analysis_table'

                                                # Save to database with comprehensive error handling
                                                save_success = True
                                                
                                                try:
                                                    logger.debug(f"Saving {var_name_chart} to database")
                                                    
                                                    # Prepare data for database (ensure JSON serializable)
                                                    data_for_db = data
                                                    if hasattr(data, 'to_dict'):
                                                        try:
                                                            data_for_db = data.to_dict('records')
                                                        except Exception as serialization_error:
                                                            print(f"      ⚠️ Warning: Could not serialize data to records format: {str(serialization_error)}")
                                                            data_for_db = str(data)  # Fallback to string representation
                                                    
                                                    # Save chart
                                                    insert_variable(
                                                        report=report, 
                                                        module="PaymentsModule", 
                                                        var=var_name_chart,
                                                        value=data_for_db,
                                                        db_path=db_path, 
                                                        anchor=var_name_chart, 
                                                        altair_chart=chart
                                                    )
                                                    
                                                    logger.debug(f"Successfully saved {var_name_chart} to database")
                                                    
                                                except Exception as chart_save_error:
                                                    print(f"      ❌ Failed to save chart {var_name_chart}: {str(chart_save_error)}")
                                                    save_success = False
                                                
                                                try:
                                                    logger.debug(f"Saving {var_name_table} to database")
                                                    
                                                    # Save table
                                                    insert_variable(
                                                        report=report, 
                                                        module="PaymentsModule", 
                                                        var=var_name_table,
                                                        value=data_for_db,
                                                        db_path=db_path, 
                                                        anchor=var_name_table, 
                                                        gt_table=formatted_table,
                                                        simple_gt_save= True,
                                                        table_width=width,      # Automatically calculated
                                                        table_height=height,    # Automatically calculated  
                                                    
                                                    )
                                                    
                                                    logger.debug(f"Successfully saved {var_name_table} to database")
                                                    
                                                except Exception as table_save_error:
                                                    print(f"      ❌ Failed to save table {var_name_table}: {str(table_save_error)}")
                                                    save_success = False
                                                
                                                # Update statistics
                                                if save_success:
                                                    generation_stats['total_successful'] += 1
                                                    generation_stats['successful_items'].append(f"{programme}_{call_type}")
                                                    print(f"      ✅ Generated: {programme} - {call_type} ({budget_status})")
                                                else:
                                                    generation_stats['total_failed'] += 1
                                                    generation_stats['failed_items'].append(f"{programme}_{call_type} (database save)")
                                                    print(f"      ⚠️ Generated but failed to save: {programme} - {call_type}")
                                                
                                            except Exception as processing_error:
                                                generation_stats['total_failed'] += 1
                                                generation_stats['failed_items'].append(f"{programme}_{call_type} (processing)")
                                                print(f"      ❌ Error processing successful result for {programme} {call_type}: {str(processing_error)}")
                                                
                                        else:
                                            generation_stats['total_failed'] += 1
                                            error_detail = result.get('error', 'Unknown error')
                                            generation_stats['failed_items'].append(f"{programme}_{call_type} ({error_detail})")
                                            print(f"      ❌ Failed: {programme} - {call_type} - {error_detail}")
                                            
                                    except Exception as call_type_error:
                                        generation_stats['total_failed'] += 1
                                        generation_stats['failed_items'].append(f"{programme}_{call_type} (unexpected error)")
                                        print(f"   💥 Unexpected error processing {programme} {call_type}: {str(call_type_error)}")
                                        
                                        # Store error result
                                        if programme not in results:
                                            results[programme] = {}
                                        results[programme][call_type] = {
                                            'success': False, 
                                            'error': str(call_type_error)
                                        }
                            
                            except Exception as programme_error:
                                print(f"💥 Unexpected error processing programme {programme}: {str(programme_error)}")
                                results[programme] = {'error': str(programme_error)}
                    
                    else:
                        print("❌ Error: Could not identify call type column in data")
                        return {}
                
                except Exception as main_error:
                    print(f"💥 Unexpected error in main generation logic: {str(main_error)}")
                    return {}
                
                # ═══════════════════════════════════════════════════════════════════
                # FINAL SUMMARY AND RETURN
                # ═══════════════════════════════════════════════════════════════════
                
                try:
                    print(f"\n📊 Chart Generation Summary:")
                    print(f"   ✅ Successful: {generation_stats['total_successful']}")
                    print(f"   ❌ Failed: {generation_stats['total_failed']}")
                    print(f"   📋 Total Attempted: {generation_stats['total_attempted']}")
                    
                    if generation_stats['successful_items']:
                        print(f"   🎯 Successful items: {', '.join(generation_stats['successful_items'])}")
                    
                    if generation_stats['failed_items']:
                        print(f"   ⚠️ Failed items: {', '.join(generation_stats['failed_items'])}")
                    
                    # Add summary to results
                    results['_generation_summary'] = generation_stats
                    
                except Exception as summary_error:
                    print(f"⚠️ Warning: Error generating summary: {str(summary_error)}")
                
                return results


            def refresh_budget_config():
                """
                Refresh the global budget configuration with latest data
                Call this if vars_all has been updated
                
                Returns:
                - dict: Updated budget configuration
                """
                try:
                    global AVAILABLE_BUDGET_CONFIG
                    
                    # Check if create_budget_config function exists
                    if 'create_budget_config' not in globals():
                        print("❌ Error: create_budget_config function not found")
                        return None
                    
                    AVAILABLE_BUDGET_CONFIG = create_budget_config()
                    print("✅ Budget configuration refreshed successfully")
                    return AVAILABLE_BUDGET_CONFIG
                    
                except Exception as e:
                    print(f"❌ Error refreshing budget configuration: {str(e)}")
                    return None


            def set_budget_config(config):
                """
                Manually set the budget configuration
                Useful for testing or when vars_all is not available
                
                Parameters:
                - config: Budget configuration dictionary
                
                Returns:
                - dict: The set budget configuration
                """
                try:
                    global AVAILABLE_BUDGET_CONFIG
                    
                    # Validate config structure
                    if not isinstance(config, dict):
                        raise ValueError("Budget config must be a dictionary")
                    
                    # Basic validation of expected structure
                    expected_programmes = ['H2020', 'HEU']
                    for programme in expected_programmes:
                        if programme not in config:
                            print(f"⚠️ Warning: Programme {programme} not found in budget config")
                    
                    AVAILABLE_BUDGET_CONFIG = config
                    print("✅ Budget configuration set successfully")
                    return AVAILABLE_BUDGET_CONFIG
                    
                except Exception as e:
                    print(f"❌ Error setting budget configuration: {str(e)}")
                    return None

            # # Create global budget configuration
            # AVAILABLE_BUDGET_CONFIG = create_budget_config()

            # ═══════════════════════════════════════════════════════════════════
            # GLOBAL BUDGET CONFIGURATION INITIALIZATION
            # ═══════════════════════════════════════════════════════════════════

            try:
                # Create global budget configuration with error handling
                if 'create_budget_config' in globals():
                    AVAILABLE_BUDGET_CONFIG = create_budget_config()
                    print("✅ Global budget configuration initialized successfully")
                else:
                    # Fallback configuration if create_budget_config is not available
                    AVAILABLE_BUDGET_CONFIG = {
                        'H2020': {'all': None},
                        'HEU': {'all': None, 'EXPERTS': None}
                    }
                    print("⚠️ Warning: create_budget_config not found, using fallback budget configuration")
                    
            except Exception as config_error:
                print(f"❌ Error initializing budget configuration: {str(config_error)}")
                # Emergency fallback
                AVAILABLE_BUDGET_CONFIG = {
                    'H2020': {'all': None},
                    'HEU': {'all': None, 'EXPERTS': None}
                }
                print("🆘 Using emergency fallback budget configuration")

            
            # ═══════════════════════════════════════════════════════════════════
            # MAIN EXECUTION WITH COMPREHENSIVE ERROR HANDLING
            # ═══════════════════════════════════════════════════════════════════

            def execute_chart_generation(df_paym, df_forecast, cutoff):
                """
                Wrapper function to execute chart generation with comprehensive error handling
                
                Parameters:
                - df_paym: Payment dataframe
                - df_forecast: Forecast dataframe (optional)
                - cutoff: Cutoff date
                
                Returns:
                - tuple: (success: bool, message: str, results: dict or None)
                """
                
                try:
                    print("🚀 Starting chart generation execution...")
                    
                    # Validate inputs
                    if df_paym is None or df_paym.empty:
                        return False, "Payment dataframe is required and cannot be empty", None
                    
                    if cutoff is None:
                        print("⚠️ Warning: No cutoff date provided, using current date")
                        cutoff = pd.Timestamp.now()
                    
                    # Execute chart generation
                    results = generate_all_charts_tables(
                        df_paym=df_paym, 
                        df_forecast=df_forecast, 
                        programme_budgets=None, 
                        cutoff_date=cutoff
                    )
                    
                    # Check if we got any results
                    if not results:
                        return False, "Chart generation returned no results", None
                    
                    # Extract summary statistics
                    summary = results.get('_generation_summary', {})
                    total_attempted = summary.get('total_attempted', 0)
                    total_successful = summary.get('total_successful', 0)
                    total_failed = summary.get('total_failed', 0)
                    
                    # Determine success level
                    if total_attempted == 0:
                        return False, "No charts were attempted", results
                    
                    elif total_successful == total_attempted:
                        success_message = f"All {total_successful} charts generated successfully!"
                        return True, success_message, results
                    
                    elif total_successful > 0:
                        partial_message = f"Partial success: {total_successful}/{total_attempted} charts generated successfully"
                        return True, partial_message, results
                    
                    else:
                        failure_message = f"All {total_attempted} chart generations failed"
                        return False, failure_message, results
                
                except Exception as e:
                    error_message = f"Unexpected error during chart generation execution: {str(e)}"
                    print(f"💥 {error_message}")
                    import traceback
                    traceback.print_exc()
                    return False, error_message, None

            # generate_all_charts_tables(df_paym, df_forecast, programme_budgets=None, cutoff_date=cutoff)

            try:
                # Check if required variables exist
                # # required_vars = ['df_paym']
                # missing_vars = [var for var in required_vars if var not in globals()]
                
                # if missing_vars:
                #     print(f"❌ Cannot execute: Missing required variables: {missing_vars}")
                # else:
                #     # Get df_forecast if available, otherwise use None
                #     df_forecast_for_execution = globals().get('df_forecast', None)
                #     cutoff_for_execution = globals().get('cutoff', pd.Timestamp.now())
                    
                #     print("🎯 Executing chart generation with error handling...")
                    
                    success, message, results = execute_chart_generation(
                        df_paym=df_paym,
                        df_forecast=df_forecast,
                        cutoff=cutoff
                    )
                    
                    if success:
                        print(f"✅ Chart Generation: {message}")
                    else:
                        print(f"❌ Chart Generation: {message}")
                        
            except Exception as execution_error:
                print(f"💥 Error during chart generation execution: {str(execution_error)}")


        except Exception as e:
                # Catch any unexpected errors
                error_message = f"Unexpected error in generate_ttp_charts: {str(e)}"
                print(f"❌ {error_message}")
                return False, error_message, None
                
        finally:
            print("🏁 Annex Tables generation process completed")