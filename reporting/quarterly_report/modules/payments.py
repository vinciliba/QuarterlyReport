from __future__ import annotations

import logging, sqlite3, datetime
from pathlib import Path
import pandas as pd
from ingestion.db_utils import (
    fetch_latest_table_data,
)
from reporting.quarterly_report.utils import RenderContext, BaseModule
from ingestion.db_utils import load_report_params
from reporting.quarterly_report.report_utils.payments_m_builder import (quarterly_tables_generation_main, 
                                                                        generate_ttp_summary_overview, 
                                                                        generate_ttp_tables,
                                                                        generate_ttp_charts,
                                                                        annex_tables_ttp_eff,
                                                                        paym_charts_summary_tables
                                                                        )
from typing import List, Tuple,Union
import numpy as np
import re
# ──────────────────────────────────────────────────────────────
# COSTANTS
# ──────────────────────────────────────────────────────────────
PAYMENTS_ALIAS = "payments_summa"
CALLS_ALIAS = 'call_overview'
PAYMENTS_TIMES_ALIAS = 'payments_summa_time'
PO_ALIAS = 'c0_po_summa'
FORECAST_ALIAS = 'forecast'
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


# Option 1: Pass po_map as parameter (RECOMMENDED)
def apply_conditional_mapping(row, po_mapping_dict):
    """
    Fixed version that receives po_mapping as parameter instead of using global
    """
    current_call_type = row['call_type']
    po_key = row['PO Purchase Order Key']
    
    should_map = (
        pd.isna(current_call_type) or 
        current_call_type == '' or 
        current_call_type not in CALLS_TYPES_LIST or 
        current_call_type in ['EXPERTS', 'CSA']
    )
    
    if should_map:
        mapped_value = safe_map_project_to_call_type(po_key, po_mapping_dict)
        return mapped_value if mapped_value is not None else current_call_type
    else:
        return current_call_type



# ──────────────────────────────────────────────────────────────
# MAIN CLASS
# ──────────────────────────────────────────────────────────────

class PaymentsModule(BaseModule):
    name = "Payments"           # shows up in UI
    description = "Payments Statistics, Tables and Charts"

    def run(self, ctx: RenderContext) -> RenderContext:
        log = logging.getLogger(self.name)
        conn = ctx.db.conn
        cutoff = pd.to_datetime(ctx.cutoff)
        db_path = Path(conn.execute("PRAGMA database_list").fetchone()[2])
        report = ctx.report_name

        # Load report parameters
        report_params = load_report_params(report_name=report, db_path=db_path)
        table_colors = report_params.get('TABLE_COLORS', {})

        # Module-level error tracking
        module_errors = []
        module_warnings = []
        
        print("🚀 Starting Payments Module...")
        print(f'CUTOFF:{cutoff}')

        try:
            # ══════════════════════════════════════════════════════════════════
            # 1. DATA LOADING
            # ══════════════════════════════════════════════════════════════════
            
            print("📂 Loading data...")
            
            df_paym = fetch_latest_table_data(conn, PAYMENTS_ALIAS, cutoff)
            df_paym_times = fetch_latest_table_data(conn, PAYMENTS_TIMES_ALIAS, cutoff)
            df_calls = fetch_latest_table_data(conn, CALLS_ALIAS, cutoff)
            df_po = fetch_latest_table_data(conn, PO_ALIAS, cutoff)
            df_forecast = fetch_latest_table_data(conn, FORECAST_ALIAS, cutoff)
            
            if df_paym is None or df_paym.empty:
                error_msg = "Critical error: df_paym is empty or None"
                module_errors.append(error_msg)
                print(f"❌ {error_msg}")
                return ctx  # Return context even with errors
                
            print(f"✅ Data loaded: {len(df_paym)} payment records")

        except Exception as e:
            error_msg = f"Data loading failed: {str(e)}"
            module_errors.append(error_msg)
            print(f"❌ {error_msg}")
            return ctx

        try:
            # ══════════════════════════════════════════════════════════════════
            # 2. DATA TRANSFORMATION
            # ══════════════════════════════════════════════════════════════════
            
            print("🔄 Starting data transformation...")

            # Apply payment type mapping
            df_paym['v_payment_type'] = df_paym.apply(map_payment_type, axis=1)
            
            # Filter the dataframe
            df_paym = df_paym[df_paym['Pay Document Type Desc'].isin(['Payment Directive', 'Exp Pre-financing'])]
            df_paym = df_paym[df_paym['v_payment_type'] != 'Other']
            df_paym = df_paym[df_paym['Pay Payment Key'].notnull()]

            df_paym['project_number'] = df_paym.apply(extract_project_number, axis=1)

            # Create call type mappings
            df_calls['CALL_TYPE'] = df_calls.apply(determine_po_category, axis=1)
            grant_map = df_calls.set_index('Grant Number')['CALL_TYPE'].to_dict()

            # PO ORDERS MAP
            df_po['CALL_TYPE'] = df_po.apply(determine_po_category_po_list, axis=1)
            po_map = df_po[
                df_po['CALL_TYPE'].notna() &
                (df_po['CALL_TYPE'].str.strip() != '')
            ].set_index('PO Purchase Order Key')['CALL_TYPE'].to_dict()

            # Apply the mapping
            df_paym['call_type'] = df_paym['project_number'].apply(
                lambda x: map_project_to_call_type(x, grant_map))
            df_paym['call_type'] = df_paym.apply(
                lambda row: map_call_type_with_experts(row, grant_map), axis=1)

            # Clean call_type column
            df_paym['call_type'] = df_paym['call_type'].astype(str).str.strip().replace(['nan', ''], np.nan)
            # df_paym['call_type'] = df_paym.apply(apply_conditional_mapping, axis=1)
             # ✅ FIXED: Pass po_map as parameter instead of relying on global
            df_paym['call_type'] = df_paym.apply(
                lambda row: apply_conditional_mapping(row, po_map), axis=1
            )
            
            # Data type conversions
            df_paym['PO Purchase Order Key'] = pd.to_numeric(
                df_paym['PO Purchase Order Key'], errors='coerce').astype('Int64')

            df_paym['Pay Document Date (dd/mm/yyyy)'] = pd.to_datetime(
                df_paym['Pay Document Date (dd/mm/yyyy)'],
                format='%Y-%m-%d %H:%M:%S',
                errors='coerce'
            )

            # Filter by date scope
            quarter_dates = get_scope_start_end(cutoff=cutoff)
            last_valid_date = quarter_dates[1]
            print(f'LAST VALID DATE PAYMENTS {last_valid_date}')
            df_paym = df_paym[df_paym['Pay Document Date (dd/mm/yyyy)'] <= last_valid_date].copy()
            df_paym = df_paym[df_paym['call_type'] != 'CSA']

            print(f"✅ Data transformation completed: {len(df_paym)} records after filtering")

        except Exception as e:
            error_msg = f"Data transformation failed: {str(e)}"
            module_errors.append(error_msg)
            print(f"❌ {error_msg}")
            # Continue with partial data if possible

        try:
            # ══════════════════════════════════════════════════════════════════
            # 3. TTP DATA PROCESSING
            # ══════════════════════════════════════════════════════════════════
            
            print("⏱️ Processing TTP data...")

            # Convert Pay Delay Late Payment Flag
            df_paym_times['v_payment_in_time'] = df_paym_times['Pay Delay Late Payment Flag (Y/N)'].apply(
                lambda x: 1 if x == 'N' else 0
            )

            print("v_payment_in_time value counts:")
            print(df_paym_times['v_payment_in_time'].value_counts())

            # Clean Pay Payment Key and create mappings
            df_times_clean = df_paym_times.dropna(subset=['Pay Payment Key']).copy()

            def safe_convert_to_int(value):
                """Safely convert payment key to integer"""
                try:
                    if pd.isna(value):
                        return None
                    if isinstance(value, str):
                        if value.endswith('.0'):
                            return int(value[:-2])
                        else:
                            return int(float(value))
                    else:
                        return int(float(value))
                except (ValueError, TypeError, OverflowError):
                    return None

            # Convert Pay Payment Key to integers for mapping
            df_times_clean['Pay_Payment_Key_Int'] = df_times_clean['Pay Payment Key'].apply(safe_convert_to_int)

            conversion_failed = df_times_clean['Pay_Payment_Key_Int'].isna().sum()
            conversion_success = df_times_clean['Pay_Payment_Key_Int'].notna().sum()
            print(f"Payment key conversions - Success: {conversion_success}, Failed: {conversion_failed}")

            # Create mappings
            valid_conversions = df_times_clean['Pay_Payment_Key_Int'].notna()
            mapping_data = df_times_clean[valid_conversions].copy()
            mapping_data['Pay_Payment_Key_Int'] = mapping_data['Pay_Payment_Key_Int'].astype(int)

            payment_key_to_ttp_gross = mapping_data.set_index('Pay_Payment_Key_Int')['Pay Delay With Suspension'].to_dict()
            payment_key_to_ttp_net = mapping_data.set_index('Pay_Payment_Key_Int')['Pay Delay Without Suspension'].to_dict()
            payment_key_to_payment_in_time = mapping_data.set_index('Pay_Payment_Key_Int')['v_payment_in_time'].to_dict()

            # Split dataframe and apply mappings
            exp_prefi_mask = df_paym['Pay Document Type Desc'] == 'Exp Pre-financing'
            payment_directive_mask = df_paym['Pay Document Type Desc'] == 'Payment Directive'
            other_mask = ~(exp_prefi_mask | payment_directive_mask)

            df_exp_prefi = df_paym[exp_prefi_mask].copy()
            df_payment_directive = df_paym[payment_directive_mask].copy()
            df_other = df_paym[other_mask].copy()

            # Mapping function for payment data
            def map_payment_data(pay_key, mapping_dict):
                try:
                    if pd.isna(pay_key):
                        return np.nan
                    if isinstance(pay_key, str):
                        if pay_key.endswith('.0'):
                            numeric_key = int(pay_key[:-2])
                        else:
                            numeric_key = int(float(pay_key))
                    else:
                        numeric_key = int(float(pay_key))

                    if numeric_key in mapping_dict:
                        return mapping_dict[numeric_key]
                    else:
                        return np.nan
                except (ValueError, TypeError, OverflowError):
                    return np.nan

            # Apply mapping only to Exp Pre-financing dataframe
            if len(df_exp_prefi) > 0:
                df_exp_prefi['v_TTP_GROSS'] = df_exp_prefi['Pay Payment Key'].apply(
                    lambda x: map_payment_data(x, payment_key_to_ttp_gross))
                df_exp_prefi['v_TTP_NET'] = df_exp_prefi['Pay Payment Key'].apply(
                    lambda x: map_payment_data(x, payment_key_to_ttp_net))
                df_exp_prefi['v_payment_in_time'] = df_exp_prefi['Pay Payment Key'].apply(
                    lambda x: map_payment_data(x, payment_key_to_payment_in_time))

            # Add columns to other dataframes
            for df_part in [df_payment_directive, df_other]:
                if len(df_part) > 0:
                    for col in ['v_TTP_GROSS', 'v_TTP_NET', 'v_payment_in_time']:
                        if col not in df_part.columns:
                            df_part[col] = np.nan

            # Merge dataframes back together
            dataframes_to_merge = [df for df in [df_exp_prefi, df_payment_directive, df_other] if len(df) > 0]
            if dataframes_to_merge:
                df_paym = pd.concat(dataframes_to_merge, ignore_index=True)

            print(f"✅ TTP data processing completed: {df_paym.shape}")

        except Exception as e:
            error_msg = f"TTP data processing failed: {str(e)}"
            module_errors.append(error_msg)
            print(f"❌ {error_msg}")

        # ══════════════════════════════════════════════════════════════════
        # 4. QUARTERLY TABLES GENERATION
        # ══════════════════════════════════════════════════════════════════
        
        print("📊 Starting quarterly tables generation...")
        
        try:
            success, message, results = quarterly_tables_generation_main(
                df_paym=df_paym,
                cutoff=cutoff,
                report=report,
                db_path=db_path,
                table_colors=table_colors,
            )

            if success:
                print(f"✅ Quarterly Tables: {message}")
                
                if results and 'summary' in results:
                    summary = results['summary']
                    print(f"   📈 Details: {summary['successful']} successful, {summary['failed']} failed, {summary['skipped']} skipped")
                    
                    if summary['failed'] > 0:
                        module_warnings.append(f"Some quarterly tables had issues: {message}")
                        
            else:
                error_msg = f"Quarterly tables generation failed: {message}"
                print(f"❌ {error_msg}")
                module_errors.append(error_msg)

        except Exception as e:
            error_msg = f"Unexpected error during quarterly tables generation: {str(e)}"
            print(f"💥 {error_msg}")
            module_errors.append(error_msg)

        # ══════════════════════════════════════════════════════════════════
        # 5. TTP SUMMARY AND OVERVIEW GENERATION
        # ══════════════════════════════════════════════════════════════════
        
        print("📋 Starting TTP summary and overview generation...")
        
        try:
            success, message, results = generate_ttp_summary_overview(
                df_paym=df_paym,
                cutoff=cutoff,
                db_path=db_path,
                report=report,
                table_colors=table_colors,
                report_params=report_params
            )

            if success:
                print(f"✅ TTP Summary: {message}")
                
                if results and results.get('summary', {}).get('failed', 0) > 0:
                    module_warnings.append(f"Some TTP summary tables had issues: {message}")
                    
            else:
                error_msg = f"TTP summary generation failed: {message}"
                print(f"❌ {error_msg}")
                module_errors.append(error_msg)

        except Exception as e:
            error_msg = f"Unexpected error during TTP summary generation: {str(e)}"
            print(f"💥 {error_msg}")
            module_errors.append(error_msg)

        # ══════════════════════════════════════════════════════════════════
        # 6. TTP TABLES GENERATION
        # ══════════════════════════════════════════════════════════════════
        
        print("📊 Starting TTP tables and charts generation...")
        
        quarterly_tables = None
        
        try:
            success, message, results = generate_ttp_tables(
                df_paym=df_paym,
                cutoff=cutoff,
                db_path=db_path,
                report=report,
                table_colors=table_colors,
                report_params=report_params
            )

            # Extract quarterly_tables regardless of success level
            if results and 'quarterly_tables' in results:
                quarterly_tables = results['quarterly_tables']
                print(f"📊 Retrieved {len(quarterly_tables)} quarterly tables")

            if success:
                print(f"✅ TTP Charts: {message}")
                
                if results and 'summary' in results:
                    summary = results['summary']
                    print(f"   📋 Details: {summary['successful']} successful, {summary['failed']} failed")
                    
                    if summary['failed'] > 0:
                        module_warnings.append(f"Some TTP charts had issues: {message}")
                        
            else:
                error_msg = f"TTP tables and charts failed: {message}"
                print(f"❌ {error_msg}")
                module_errors.append(error_msg)
                
                # Even if failed, might have partial results
                if quarterly_tables:
                    print(f"📊 Partial results available: {len(quarterly_tables)} tables")

        except Exception as e:
            error_msg = f"Unexpected error during TTP charts generation: {str(e)}"
            print(f"💥 {error_msg}")
            module_errors.append(error_msg)

        # ══════════════════════════════════════════════════════════════════
        # 7. TTP CHARTS GENERATION
        # ══════════════════════════════════════════════════════════════════
        try:
            print("🚀 Starting TTP charts generation...")
            
            success, message, results = generate_ttp_charts(
                df_paym=df_paym,
                cutoff=cutoff,
                db_path=db_path,
                report=report,
                table_colors=table_colors,
                report_params=report_params,
                quarterly_tables=quarterly_tables
            )
            
            # Extract charts regardless of success level
            ttp_charts = None
            if results and 'ttp_charts' in results:
                ttp_charts = results['ttp_charts']
                print(f"📊 Retrieved {len(ttp_charts)} charts")
            
            if success:
                print(f"✅ TTP Charts: {message}")
                
                # Log details if available
                if results and 'summary' in results:
                    summary = results['summary']
                    print(f"   📋 Details: {summary['successful']} successful, {summary['failed']} failed")
                    
                    # Show successful charts
                    successful_list = summary.get('successful_list', [])
                    if successful_list:
                        print(f"   ✅ Success: {', '.join(successful_list[:3])}{'...' if len(successful_list) > 3 else ''}")
                    
                    # Show failed charts if any
                    failed_list = summary.get('failed_list', [])
                    if failed_list:
                        print(f"   ⚠️ Issues: {', '.join(failed_list[:2])}{'...' if len(failed_list) > 2 else ''}")
                        module_warnings.append(f"Some TTP charts had issues: {message}")
                    
            else:
                error_msg = f"TTP charts generation failed: {message}"
                print(f"❌ {error_msg}")
                module_errors.append(error_msg)
                
                # Even if failed, might have partial results
                if ttp_charts:
                    print(f"   📊 Partial results available: {len(ttp_charts)} charts")
                    module_warnings.append(f"Partial TTP chart results: {len(ttp_charts)} charts available")

            # Log chart variables that were saved to database (if available)
            if results and 'summary' in results:
                summary = results['summary']
                successful_list = summary.get('successful_list', [])
                if successful_list:
                    print(f"   💾 Saved to database:")
                    for chart_name in successful_list[:5]:  # Show first 5
                        print(f"     - {chart_name}")
                    if len(successful_list) > 5:
                        print(f"     - ... and {len(successful_list) - 5} more")

        except Exception as e:
            error_msg = f"Unexpected error during TTP charts generation: {str(e)}"
            print(f"💥 {error_msg}")
            module_errors.append(error_msg)
            
            # Add traceback for debugging
            import traceback
            print(f"📋 Traceback: {traceback.format_exc()}")

        
        # ══════════════════════════════════════════════════════════════════
        # 8. Annex TABLES - TTP - EFFECTIVENESS
        # ══════════════════════════════════════════════════════════════════
        try:
            success, message, results = annex_tables_ttp_eff(
                df_paym=df_paym,
                cutoff=cutoff,
                db_path=db_path,
                report=report,
                table_colors=table_colors,
                report_params=report_params
            )

            # Extract results
            annex_tables = None
            generated_tables = None
            
            if results:
                annex_tables = results.get('annex_tables', {})
                generated_tables = results.get('generated_tables', {})
                print(f"📊 Retrieved {len(annex_tables)} processed annex tables")

            if success:
                print(f"✅ Annex Tables: {message}")
                
                # Log detailed summary
                if results and 'summary' in results:
                    summary = results['summary']
                    print(f"   📋 Details: {summary['successful']} successful out of {summary['total_attempted']} attempted")
                    
                    # Show successful tables
                    if annex_tables:
                        table_names = list(annex_tables.keys())
                        print(f"   ✅ Generated: {', '.join(table_names)}")
                    
                    # Show any failures
                    if summary['failed'] > 0:
                        print(f"   ⚠️ Some issues occurred: {summary['failed']} failures")
                        module_warnings.append(f"Some annex tables had issues: {message}")
                        
            else:
                error_msg = f"Annex tables generation failed: {message}"
                print(f"❌ {error_msg}")
                module_errors.append(error_msg)
                
                # Check if we have partial results
                if annex_tables and len(annex_tables) > 0:
                    print(f"   📊 Partial results available: {len(annex_tables)} tables")
                    module_warnings.append(f"Partial annex table results: {len(annex_tables)} tables available")

            # Access specific tables if needed
            if annex_tables:
                # Access formatted tables that were saved to database
                if 'Overview_h2020_tp_net' in annex_tables:
                    h2020_net_table = annex_tables['Overview_h2020_tp_net']['table']
                    print(f"   📊 H2020 NET table available: {h2020_net_table.shape}")
                
                if 'Overview_h2020_eff' in annex_tables:
                    h2020_eff_table = annex_tables['Overview_h2020_eff']['table']
                    print(f"   📊 H2020 effectiveness table available: {h2020_eff_table.shape}")

            # Access raw generated tables if needed
            if generated_tables:
                # Access raw tables before formatting
                raw_h2020_net = generated_tables.get('h2020_net')
                raw_h2020_effect = generated_tables.get('h2020_effect')
                
                if raw_h2020_net is not None:
                    print(f"   📊 Raw H2020 NET table available: {raw_h2020_net.shape}")
                if raw_h2020_effect is not None:
                    print(f"   📊 Raw H2020 effect table available: {raw_h2020_effect.shape}")

        except Exception as e:
            error_msg = f"Unexpected error during annex tables generation: {str(e)}"
            print(f"💥 {error_msg}")
            module_errors.append(error_msg)

        # ══════════════════════════════════════════════════════════════════
        # 9. PAYMENT ANALYSIS CHARTS & SUMMARY TABLES
        # ══════════════════════════════════════════════════════════════════
        try:
            success, message, results = paym_charts_summary_tables(
                df_paym=df_paym,
                cutoff=cutoff,
                db_path=db_path,
                report=report,
                table_colors=table_colors,
                report_params=report_params,
                df_forecast=df_forecast  # Note: parameter order corrected
            )

            # Extract results
            payment_charts = None
            payment_tables = None
            generation_summary = None
            
            if results:
                payment_charts = results.get('payment_charts', {})
                payment_tables = results.get('payment_tables', {})
                generation_summary = results.get('_generation_summary', {})
                
                print(f"📊 Retrieved payment analysis results")

            if success:
                print(f"✅ Payment Charts & Tables: {message}")
                
                # Log detailed summary
                if generation_summary:
                    total_attempted = generation_summary.get('total_attempted', 0)
                    total_successful = generation_summary.get('total_successful', 0)
                    total_failed = generation_summary.get('total_failed', 0)
                    successful_items = generation_summary.get('successful_items', [])
                    failed_items = generation_summary.get('failed_items', [])
                    
                    print(f"   📋 Details: {total_successful} successful out of {total_attempted} attempted")
                    
                    # Show successful charts/tables
                    if successful_items:
                        print(f"   ✅ Generated: {', '.join(successful_items)}")
                    
                    # Show any failures
                    if total_failed > 0:
                        print(f"   ⚠️ Some issues occurred: {total_failed} failures")
                        if failed_items:
                            print(f"   ❌ Failed items: {', '.join(failed_items)}")
                        module_warnings.append(f"Some payment charts had issues: {total_failed} failures")
                        
                # Access specific charts and tables by programme and call type
                if results:
                    # Check for H2020 results
                    h2020_results = results.get('H2020', {})
                    if h2020_results:
                        print(f"   📊 H2020 charts available for: {list(h2020_results.keys())}")
                        
                        # Access specific H2020 charts/tables
                        if 'all' in h2020_results and h2020_results['all'].get('success'):
                            h2020_all_chart = h2020_results['all']['chart']
                            h2020_all_table = h2020_results['all']['table']
                            h2020_all_data = h2020_results['all']['data']
                            print(f"   📈 H2020 'all' chart and table generated successfully")
                    
                    # Check for HEU results
                    heu_results = results.get('HEU', {})
                    if heu_results:
                        print(f"   📊 HEU charts available for: {list(heu_results.keys())}")
                        
                        # Access specific HEU charts/tables
                        if 'all' in heu_results and heu_results['all'].get('success'):
                            heu_all_chart = heu_results['all']['chart']
                            heu_all_table = heu_results['all']['table']
                            heu_all_data = heu_results['all']['data']
                            print(f"   📈 HEU 'all' chart and table generated successfully")
                        
                        if 'EXPERTS' in heu_results and heu_results['EXPERTS'].get('success'):
                            heu_experts_chart = heu_results['EXPERTS']['chart']
                            heu_experts_table = heu_results['EXPERTS']['table']
                            heu_experts_data = heu_results['EXPERTS']['data']
                            print(f"   📈 HEU 'EXPERTS' chart and table generated successfully")
                            
            else:
                error_msg = f"Payment charts generation failed: {message}"
                print(f"❌ {error_msg}")
                module_errors.append(error_msg)
                
                # Check if we have partial results
                if results and generation_summary:
                    total_successful = generation_summary.get('total_successful', 0)
                    if total_successful > 0:
                        print(f"   📊 Partial results available: {total_successful} charts/tables")
                        module_warnings.append(f"Partial payment chart results: {total_successful} items available")

            # Log chart variables that were saved to database
            if results and generation_summary and generation_summary.get('successful_items'):
                print(f"   💾 Saved to database:")
                for item in generation_summary['successful_items']:
                    chart_var = f"{item}_paym_analysis_chart"
                    table_var = f"{item}_paym_analysis_table"
                    print(f"     - {chart_var}")
                    print(f"     - {table_var}")

        except Exception as e:
            error_msg = f"Unexpected error during payment charts generation: {str(e)}"
            print(f"💥 {error_msg}")
            module_errors.append(error_msg)
            
            # Add traceback for debugging
            import traceback
            print(f"📋 Traceback: {traceback.format_exc()}")

        # ══════════════════════════════════════════════════════════════════
        # 10. MODULE COMPLETION STATUS
        # ══════════════════════════════════════════════════════════════════
        
        print("\n" + "="*60)
        print("📈 PAYMENTS MODULE COMPLETION SUMMARY")
        print("="*60)
        
        if module_errors:
            print(f"⚠️ Module completed with {len(module_errors)} errors:")
            for i, error in enumerate(module_errors, 1):
                print(f"   {i}. {error}")
            
            if module_warnings:
                print(f"\n⚠️ Additional warnings ({len(module_warnings)}):")
                for i, warning in enumerate(module_warnings, 1):
                    print(f"   {i}. {warning}")
                    
            print("\n❌ Module status: COMPLETED WITH ERRORS")
            
        elif module_warnings:
            print(f"✅ Module completed with {len(module_warnings)} warnings:")
            for i, warning in enumerate(module_warnings, 1):
                print(f"   {i}. {warning}")
            print("\n⚠️ Module status: COMPLETED WITH WARNINGS")
            
        else:
            print("✅ All components completed successfully!")
            print("\n🎉 Module status: FULLY SUCCESSFUL")

        # Log quarterly_tables status for next module
        if quarterly_tables:
            print(f"\n📊 Quarterly tables ready for next module: {len(quarterly_tables)} tables available")
        else:
            print("\n⚠️ No quarterly tables available for next module")

        print("="*60)
        print("🏁 Payments Module completed")
        print("="*60)

        # Return the context (don't return boolean success/failure)
        return ctx