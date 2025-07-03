
# reporting/quarterly_report/report_utils/invoices_builder.py

from __future__ import annotations
import logging
import pandas as pd
import numpy as np
import re
from pathlib import Path
from typing import Tuple, Dict, Any
from great_tables import GT, style, loc, html

from ingestion.db_utils import (
    fetch_latest_table_data,
    insert_variable
)


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


def determine_po_category(row):
    """Determine purchase order category from instrument or topic"""
    CALLS_TYPES_LIST = ['STG', 'ADG', 'POC', 'COG', 'SYG', 'StG', 'CoG', 'AdG', 'SyG', 'PoC', 'CSA']
    
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


def extract_project_number(row):
    """Extract project number from 'Inv Text' if 'v_check_payment_type' contains RP patterns"""
    payment_type = row['v_check_payment_type']
    inv_text = row['Inv Text']
    
    if pd.isna(payment_type):
        return payment_type
    
    payment_type_str = str(payment_type)
    
    # Check for RP patterns
    rp_patterns = [
        r'RP\d+=(?:FP|IP)',  # Original pattern: RP4=FP, RP2=IP, etc.
        r'RP\d+-(?:FP|IP)'   # New pattern: RP4-FP, RP2-IP, etc.
    ]
    
    has_rp_pattern = any(re.search(pattern, payment_type_str) for pattern in rp_patterns)
    
    if has_rp_pattern:
        if pd.notna(inv_text):
            inv_text_str = str(inv_text).strip()
            number_match = re.match(r'^(\d+)', inv_text_str)
            if number_match:
                return number_match.group(1)
        return payment_type
    
    return payment_type


def map_project_to_call_type(project_num, mapping_dict):
    """Map project number to call type using grant mapping"""
    try:
        numeric_key = int(project_num)
        if numeric_key in mapping_dict:
            return mapping_dict[numeric_key]
    except (ValueError, TypeError):
        pass
    return project_num


def map_call_type_with_experts(row, grant_map):
    """Map call_type based on project_number and Inv Parking Person Id"""
    project_num = row['project_number']
    parking_person_id = row['Inv Parking Person Id']
    
    # Try to map using grant_map
    try:
        numeric_key = int(project_num)
        if numeric_key in grant_map:
            return grant_map[numeric_key]
    except (ValueError, TypeError):
        pass
    
    # If project_number is 'EXPERTS', keep it as 'EXPERTS'
    if str(project_num).upper() == 'EXPERTS':
        return 'EXPERTS'
    
    # Check Inv Parking Person Id for EXPERTS_C0
    if pd.notna(parking_person_id):
        parking_person_str = str(parking_person_id).upper()
        if parking_person_str in ['KACZMUR', 'WALASOU']:
            return 'EXPERTS_C0'
    
    return project_num

def create_registration_pivot_table(df, programme_name):
    """Create a pivot table for a specific programme (H2020 or HEU)"""
    prog_data = df[df['Programme'] == programme_name].copy()
    
    if len(prog_data) == 0:
        return pd.DataFrame()
    
    pivot_rows = []
    
    # GRANTS section: anything not starting with EXPERTS
    grants_data = prog_data[~prog_data['call_type'].astype(str).str.startswith('EXPERTS')].copy()
    
    if len(grants_data) > 0:
        for call_type in sorted(grants_data['call_type'].unique()):
            ct_data = grants_data[grants_data['call_type'] == call_type]
            total = ct_data['Inv Supplier Invoice Key'].count()
            on_time = ct_data[ct_data['registered_on_time'] == 1]['Inv Supplier Invoice Key'].count()
            late = ct_data[ct_data['registered_on_time'] == 0]['Inv Supplier Invoice Key'].count()
            pivot_rows.append({
                'Category': 'GRANTS',
                'Type': call_type,
                'No of Invoices': total,
                '% registered on time': f"{(on_time / total * 100) if total > 0 else 0:.2f}%",
                '% registered late': f"{(late / total * 100) if total > 0 else 0:.2f}%"
            })
        
        # GRANTS TOTAL
        total = grants_data['Inv Supplier Invoice Key'].count()
        on_time = grants_data[grants_data['registered_on_time'] == 1]['Inv Supplier Invoice Key'].count()
        late = grants_data[grants_data['registered_on_time'] == 0]['Inv Supplier Invoice Key'].count()
        pivot_rows.append({
            'Category': 'GRANTS',
            'Type': 'TOTAL:',
            'No of Invoices': total,
            '% registered on time': f"{(on_time / total * 100) if total > 0 else 0:.2f}%",
            '% registered late': f"{(late / total * 100) if total > 0 else 0:.2f}%"
        })
    
    # EXPERTS section: dynamically group any call_type starting with EXPERTS
    experts_data = prog_data[prog_data['call_type'].astype(str).str.startswith('EXPERTS')].copy()
    
    if len(experts_data) > 0:
        for expert_type in sorted(experts_data['call_type'].unique()):
            exp_data = experts_data[experts_data['call_type'] == expert_type]
            total = exp_data['Inv Supplier Invoice Key'].count()
            on_time = exp_data[exp_data['registered_on_time'] == 1]['Inv Supplier Invoice Key'].count()
            late = exp_data[exp_data['registered_on_time'] == 0]['Inv Supplier Invoice Key'].count()
            pivot_rows.append({
                'Category': 'EXPERTS',
                'Type': expert_type,
                'No of Invoices': total,
                '% registered on time': f"{(on_time / total * 100) if total > 0 else 0:.2f}%",
                '% registered late': f"{(late / total * 100) if total > 0 else 0:.2f}%"
            })
        
        # EXPERTS TOTAL
        total = experts_data['Inv Supplier Invoice Key'].count()
        on_time = experts_data[experts_data['registered_on_time'] == 1]['Inv Supplier Invoice Key'].count()
        late = experts_data[experts_data['registered_on_time'] == 0]['Inv Supplier Invoice Key'].count()
        pivot_rows.append({
            'Category': 'EXPERTS',
            'Type': 'TOTAL:',
            'No of Invoices': total,
            '% registered on time': f"{(on_time / total * 100) if total > 0 else 0:.2f}%",
            '% registered late': f"{(late / total * 100) if total > 0 else 0:.2f}%"
        })

    # PROGRAMME TOTAL
    total = prog_data['Inv Supplier Invoice Key'].count()
    on_time = prog_data[prog_data['registered_on_time'] == 1]['Inv Supplier Invoice Key'].count()
    late = prog_data[prog_data['registered_on_time'] == 0]['Inv Supplier Invoice Key'].count()
    pivot_rows.append({
        'Category': programme_name,
        'Type': 'TOTAL:',
        'No of Invoices': total,
        '% registered on time': f"{(on_time / total * 100) if total > 0 else 0:.2f}%",
        '% registered late': f"{(late / total * 100) if total > 0 else 0:.2f}%"
    })
    
    return pd.DataFrame(pivot_rows)



def create_registration_great_table_grouped(df_pivot, programme_name, colors):
    """Create a great_table with row grouping by Category"""
    
    BLUE = colors.get("BLUE", "#004A99")
    LIGHT_BLUE = colors.get("LIGHT_BLUE", "#d6e6f4")
    DARK_BLUE = colors.get("DARK_BLUE", "#01244B")
    SUB_TOTAL_BACKGROUND = colors.get("subtotal_background_color", "#E6E6FA")
    
    df_for_gt = df_pivot.copy()
    
    tbl = (
        GT(df_for_gt, rowname_col='Type', groupname_col='Category')
        .opt_table_font(font="Arial")
        .opt_table_outline(style="solid", width='1px', color="#cccccc") 
        .tab_options(
            table_font_size="12px",
            table_width="100%",
            table_background_color="#ffffff",
            table_font_color=DARK_BLUE
        )
        .tab_style(
            style=style.borders(sides="all", color="#cccccc", weight="1px"),
            locations=loc.body()
        )
        
        # Row group styling
        .tab_style(
            style=[
                style.text(color=DARK_BLUE, weight="bold", size='medium'),
                style.fill(color=LIGHT_BLUE),
                style.css("line-height:1.2; padding:5px; width:25%;")
            ],
            locations=loc.row_groups()
        )
        
        # Column labels styling
        .tab_style(
            style=[
                style.fill(color=BLUE),
                style.text(color="white", weight="bold", align="center", size='small'),
                style.css("padding:8px; line-height:1.2; width:25%;")
            ],
            locations=loc.column_labels()
        )
        
        # Stubhead styling
        .tab_style(
            style=[
                style.fill(color=BLUE),
                style.text(color="white", weight="bold", align="center", size='small'),
                style.css("padding:8px; line-height:1.2; width:25%;")
            ],
            locations=loc.stubhead()
        )
        
        # Body cell styling
        .tab_style(
            style=[
                style.text(align="center", size='small'),
                style.css("padding:8px; width:25%;")
            ],
            locations=loc.body()
        )
        
        # Stub cell styling
        .tab_style(
            style=[
                style.text(size='small', align="left"),
                style.css("padding:8px; width:25%;")
            ],
            locations=loc.stub()
        )
        
        # TOTAL rows styling
        .tab_style(
            style=[
                style.fill(color=SUB_TOTAL_BACKGROUND),
                style.text(color=DARK_BLUE, weight="bold")
            ],
            locations=loc.body(
                rows=lambda df: df['Type'].apply(lambda x: 'TOTAL:' in str(x))
            )
        )
        
        .tab_style(
            style=[
                style.fill(color=SUB_TOTAL_BACKGROUND),
                style.text(color=DARK_BLUE, weight="bold")
            ],
            locations=loc.stub(
                rows=lambda df: df['Type'].apply(lambda x: 'TOTAL:' in str(x))
            )
        )
        
        # Column labels with balanced spacing
        .cols_label(**{
            'No of Invoices': html(
                "<span style='display:block; text-align:center;'>No of</span>"
                "<span style='display:block; text-align:center;'>Invoices</span>"
            ),
            '% registered on time': html(
                "<span style='display:block; text-align:center;'>%</span>"
                "<span style='display:block; text-align:center;'>registered</span>"
                "<span style='display:block; text-align:center;'>on time</span>"
            ),
            '% registered late': html(
                "<span style='display:block; text-align:center;'>%</span>"
                "<span style='display:block; text-align:center;'>registered</span>"
                "<span style='display:block; text-align:center;'>late</span>"
            )
        })
        
        # Add title with BOLD styling
        .tab_header(
            title=html(f"<b>{programme_name} - Proportion of Invoices registered within 7 working days</b>")
        )
        
        # Title styling
        .tab_style(
            style=[
                style.text(weight="bold", size="large", align="center"),
                style.css("padding:10px;")
            ],
            locations=loc.title()
        )
        
        # Footer
        .tab_source_note("Source: SUMMA DWH")
        .tab_source_note("Report: C0_INVOICES_SUMMA")
        .tab_style(
            style=[
                style.text(size="small", color=DARK_BLUE),
                style.fill(color="#ffffff"),
                style.css("padding:5px; line-height:1.2; border:none;")
            ],
            locations=loc.footer()
        )
    )
    
    return tbl


def generate_invoices_report(
    conn,
    cutoff: pd.Timestamp,
    alias_inv: str,
    alias_calls: str,
    report: str,
    db_path: Path,
    report_params: Dict[str, Any],
    save_to_db: bool = True,
    export_dir: Path = None
) -> Dict[str, Any]:
    """
    Generate invoices registration report for both H2020 and HEU programmes
    """
    logger = logging.getLogger(__name__)
    
    try:
        # Fetch data
        logger.info("Fetching invoice and calls data...")
        df_inv = fetch_latest_table_data(conn, alias_inv, cutoff)
        df_calls = fetch_latest_table_data(conn, alias_calls, cutoff)
        
        # Process calls data
        logger.info("Processing calls data...")
        df_calls['CALL_TYPE'] = df_calls.apply(determine_po_category, axis=1)
        grant_map = df_calls.set_index('Grant Number')['CALL_TYPE'].to_dict()
        
        # Process invoices data
        logger.info("Processing invoices data...")
        
        # Filter expenditure invoices only
        df_inv = df_inv.loc[df_inv['Inv Fin Document Type Desc'] == 'Expenditure Invoice']
        
        # Remove duplicates
        df_inv = df_inv.drop_duplicates(subset=['Inv Supplier Invoice Key', 'Inv Reception Date (dd/mm/yyyy)'])
        
        # Extract project numbers
        df_inv['project_number'] = df_inv.apply(extract_project_number, axis=1)
        
        # Map programmes
        df_inv['Programme'] = np.where(df_inv['Official Budget Line'] == '01 02 01 01', 'HEU',
                              np.where(df_inv['Official Budget Line'] == '01 02 99 01', 'H2020', 
                                      df_inv['Official Budget Line']))
        
        # Map call types
        df_inv['call_type'] = df_inv['project_number'].apply(lambda x: map_project_to_call_type(x, grant_map))
        df_inv['call_type'] = df_inv.apply(lambda row: map_call_type_with_experts(row, grant_map), axis=1)
        
        # Convert dates
        logger.info("Processing dates and calculating time to invoice...")
        df_inv['Inv Reception Date (dd/mm/yyyy)'] = pd.to_datetime(
            df_inv['Inv Reception Date (dd/mm/yyyy)'], 
            format='%Y-%m-%d %H:%M:%S',
            errors='coerce'
        )
        
        df_inv['Inv Creation Date (dd/mm/yyyy)'] = pd.to_datetime(
            df_inv['Inv Creation Date (dd/mm/yyyy)'], 
            format='%Y-%m-%d %H:%M:%S',
            errors='coerce'
        )
        
        # Calculate time to invoice
        df_inv['Time_to_Invoice'] = (df_inv['Inv Creation Date (dd/mm/yyyy)'] - 
                                    df_inv['Inv Reception Date (dd/mm/yyyy)']).dt.days
        
        # Create binary column for on-time registration
        df_inv['registered_on_time'] = (df_inv['Time_to_Invoice'] <= 7).astype(int)
        df_inv.to_excel('test_inv.xlsx')
        

        # Filter valid call types (standard + all EXPERTS variants)
        valid_call_types = set(grant_map.values())
        df_filtered = df_inv[
            df_inv['call_type'].apply(
                lambda x: x in valid_call_types or (isinstance(x, str) and x.startswith('EXPERTS'))
            )
        ].copy()

        # Apply date scope filter
        quarter_dates = get_scope_start_end(cutoff=cutoff)
        last_valid_date = quarter_dates[1]
        df_filtered = df_filtered[df_filtered['Inv Reception Date (dd/mm/yyyy)'] <= last_valid_date].copy()

        logger.info(f"Filtered to {len(df_filtered)} invoices within scope")
        
        logger.info(f"Filtered to {len(df_filtered)} invoices within scope")
        
        # Create pivot tables
        logger.info("Creating pivot tables...")
        df_h2020_pivot = create_registration_pivot_table(df_filtered, 'H2020')
        df_heu_pivot = create_registration_pivot_table(df_filtered, 'HEU')
        
        # Get colors from report params
        table_colors = report_params.get('TABLE_COLORS', {})
        
        # Create great tables
        logger.info("Creating visualization tables...")
        tbl_h2020_grouped = create_registration_great_table_grouped(df_h2020_pivot, "H2020", table_colors)
        tbl_heu_grouped = create_registration_great_table_grouped(df_heu_pivot, "HEU", table_colors)
        
        # Prepare results
        results = {
            'df_filtered': df_filtered,
            'df_h2020_pivot': df_h2020_pivot,
            'df_heu_pivot': df_heu_pivot,
            'tbl_h2020_grouped': tbl_h2020_grouped,
            'tbl_heu_grouped': tbl_heu_grouped,
            'summary_stats': {
                'total_invoices': len(df_filtered),
                'on_time_invoices': df_filtered['registered_on_time'].sum(),
                'late_invoices': len(df_filtered) - df_filtered['registered_on_time'].sum(),
                'h2020_invoices': len(df_filtered[df_filtered['Programme'] == 'H2020']),
                'heu_invoices': len(df_filtered[df_filtered['Programme'] == 'HEU'])
            }
        }
        
        # Save to database if requested
        if save_to_db:
            logger.info("Saving results to database...")
            
            for programme in ['H2020', 'HEU']:
                pivot_df = results[f'df_{programme.lower()}_pivot']
                great_table = results[f'tbl_{programme.lower()}_grouped']
                
                for var_name, value, table in [
                    (f'{programme}_invoice_registration_pivot', pivot_df, great_table),
                ]:
                    try:
                        logger.debug(f"Saving {var_name} to database")
                        insert_variable(
                            report=report, 
                            module="InvoicesModule", 
                            var=var_name,
                            value=value.to_dict() if isinstance(value, pd.DataFrame) else value,
                            db_path=db_path, 
                            anchor=var_name, 
                            gt_table=table
                        )
                        logger.debug(f"Saved {var_name} to database")
                    except Exception as e:
                        logger.error(f"Failed to save {var_name}: {str(e)}")
            
            # Save summary statistics
            try:
                insert_variable(
                    report=report,
                    module="InvoicesModule",
                    var="invoice_summary_stats",
                    value=results['summary_stats'],
                    db_path=db_path,
                    anchor="invoice_summary_stats"
                )
                logger.debug("Saved summary statistics to database")
            except Exception as e:
                logger.error(f"Failed to save summary statistics: {str(e)}")
        
        logger.info("✔︎ Invoice registration report generation completed successfully")
        return results
        
    except Exception as e:
        logger.error(f"Error in generate_invoices_report: {str(e)}")
        raise