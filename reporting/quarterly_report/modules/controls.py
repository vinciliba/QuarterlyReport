from __future__ import annotations

import logging
from pathlib import Path
import pandas as pd
from ingestion.db_utils import (
    fetch_latest_table_data,
    insert_variable,
    load_report_params,
)
from reporting.quarterly_report.utils import RenderContext, BaseModule
from ingestion.db_utils import load_report_params
from typing import Tuple
# ──────────────────────────────────────────────────────────────
# COSTANTS
# ──────────────────────────────────────────────────────────────
R_MONITORING = "reinforced_monitoring"

# Configure logging
logging.basicConfig(
    level=logging.DEBUG,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s',
    handlers=[
        logging.FileHandler('reinforced_monitoring_report.log'),
        logging.StreamHandler()
    ]
)
logger = logging.getLogger("Reinforced Monitoring")

# ──────────────────────────────────────────────────────────────
# HELPERS
# ──────────────────────────────────────────────────────────────

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


# STYLING FUNCTION - Similar to your payments table
def format_due_date_table(df, table_colors=None, program='HEU', call='STG', table_subtitle='Due Date Analysis'):
    """
    Style the due date pivot table with color coding from red (overdue) to green (above 6 months)
    """
    from great_tables import GT, md, style, loc
    
    if table_colors is None:
        table_colors = {}
    
    BLUE = table_colors.get("BLUE", "#004A99")
    DARK_BLUE = table_colors.get("DARK_BLUE", "#01244B")
    LIGHT_BLUE = table_colors.get("LIGHT_BLUE", "#d6e6f4")
    
    # Reset index to make Unit a regular column
    df_styled = df.reset_index()
    
    # Define color scheme for due date periods
    color_scheme = {
        'Overdue': {'bg': '#cc0000', 'text': 'white'},  # Dark red
        'Approaching below 1 month': {'bg': '#ff6666', 'text': 'white'},  # Light red
        'Due date within 3 months': {'bg': '#ffff99', 'text': 'black'},  # Yellow
        'Due date within 6 months': {'bg': '#99ff99', 'text': 'black'},  # Light green
        'Above 6 months': {'bg': '#00cc00', 'text': 'white'},  # Dark green
        'Grand Total': {'bg': LIGHT_BLUE, 'text': DARK_BLUE},  # Total row,

    }
    
    # Calculate table width
    data_columns = [col for col in df_styled.columns if col != 'Unit']
    base_width_per_column = 120
    stub_width = 150
    table_width = f"{stub_width + (len(data_columns) * base_width_per_column)}px"
    
    table_title = f'{program} {call} - Due Date Analysis'
    
    # Create table with comprehensive styling
    tbl = (
        GT(df_styled)
        .tab_header(title=md(f'**{table_title}**'), subtitle=md(f'**{table_subtitle}**'))
        
        # Basic table options
        .tab_options(
            table_background_color="white",
            table_font_size='small',
            table_font_color=DARK_BLUE,
            table_width=table_width,
            heading_title_font_size="16px",
            heading_subtitle_font_size="12px",
            heading_title_font_weight="bold",
            row_striping_include_table_body=False,
            row_striping_include_stub=False,
            column_labels_background_color='#004d80'
        )
        .opt_table_font(font='Arial')
        
        # Style Unit column (stub) - DARK BLUE background
        .tab_style(
            style=[
                style.fill(color='#004d80'),
                style.text(color="white", weight="bold", size="small"),
                style.borders(sides="all", color="white", weight="1px")
            ],
            locations=loc.body(columns=['Unit'])
        )
        
        # Style column headers - BLUE background
        .tab_style(
            style=[
                style.fill(color='#004d80'),
                style.text(color="white", weight="bold", size="small"),
                style.borders(sides="all", color="white", weight="1px")
            ],
            locations=loc.column_labels()
        )
        
        # Center align all data columns
        .cols_align(align='center', columns=data_columns)
        
        # Set column widths
        .cols_width({'Unit': f"{stub_width}px"})
        .cols_width({col: f"{base_width_per_column}px" for col in data_columns if col != 'Unit'})
    )
    
    # Apply color coding to each due date period column
    for col in data_columns:
        if col in color_scheme and col != 'Grand Total':
            tbl = tbl.tab_style(
                style=[
                    style.fill(color=color_scheme[col]['bg']),
                    style.text(color=color_scheme[col]['text'], weight="bold", size="small"),
                    style.borders(sides="all", color="white", weight="1px")
                ],
                locations=loc.body(columns=[col])
            )
        elif col == 'Grand Total':
            # Special styling for Grand Total column
            tbl = tbl.tab_style(
                style=[
                    style.fill(color=color_scheme['Grand Total']['bg']),
                    style.text(color=color_scheme['Grand Total']['text'], weight="bold", size="small"),
                    style.borders(sides="all", color="white", weight="1px")
                ],
                locations=loc.body(columns=[col])
            )
    
    # Style Grand Total row
    if 'Grand Total' in df_styled['Unit'].values:
        grand_total_row_idx = df_styled[df_styled['Unit'] == 'Grand Total'].index[0]
        tbl = tbl.tab_style(
            style=[
                style.fill(color=DARK_BLUE),
                style.text(color="white", weight="bold", size="small"),
                style.borders(sides="all", color="white", weight="1px")
            ],
            locations=loc.body(rows=[grand_total_row_idx])
        )
    
    return tbl

# ──────────────────────────────────────────────────────────────
# MAIN CLASS
# ──────────────────────────────────────────────────────────────

class ControlsModule(BaseModule):
    name        = "Controls"          # shows up in UI
    description = "Reinfoced Monitoring Table"

    def run(self, ctx: RenderContext) -> RenderContext:
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

        print("🚀 Starting Reinforced Monitoring Module...")

        try:
            # ══════════════════════════════════════════════════════════════════
            # 1. DATA LOADING
            # ══════════════════════════════════════════════════════════════════
            
            print("📂 Loading data...")
            df_mon = fetch_latest_table_data(conn, R_MONITORING, cutoff)
            start_period, last_valid_date = get_scope_start_end(cutoff)  
        
        except Exception as e:
            error_msg = f"Data loading failed: {str(e)}"
            module_errors.append(error_msg)
            print(f"❌ {error_msg}")
            return ctx
        
        # ══════════════════════════════════════════════════════════════════
        # 2. DATA TRANSFORMATION
        # ══════════════════════════════════════════════════════════════════
    
        try:
            print("🔄 Starting data transformation...")
            # Try automatic date parsing first (pandas will guess the format)
            print(f"\nTrying automatic date parsing...")
            df_mon['Activated Date'] = pd.to_datetime(df_mon['Activated Date'], errors='coerce')
            df_mon['Due Date'] = pd.to_datetime(df_mon['Due Date'], errors='coerce')

            # If automatic parsing still fails, try common formats
            if df_mon['Activated Date'].isna().all():
                print(f"\nAutomatic parsing failed. Trying common date formats...")
                
                # Reload original data to try different formats
                df_mon = fetch_latest_table_data(conn, R_MONITORING, cutoff)
                
                # Try different common formats
                date_formats = [
                    '%Y-%m-%d',           # 2025-06-12
                    '%Y-%m-%d %H:%M:%S',  # 2025-06-12 14:30:00
                    '%m/%d/%Y',           # 06/12/2025
                    '%d/%m/%Y',           # 12/06/2025
                    '%Y/%m/%d',           # 2025/06/12
                    '%d-%m-%Y',           # 12-06-2025
                    '%m-%d-%Y',           # 06-12-2025
                ]
                
                for fmt in date_formats:
                    try:
                        df_mon['Activated Date'] = pd.to_datetime(df_mon['Activated Date'], format=fmt, errors='coerce')
                        df_mon['Due Date'] = pd.to_datetime(df_mon['Due Date'], format=fmt, errors='coerce')
                        
                        activated_na = df_mon['Activated Date'].isna().sum()
                        due_na = df_mon['Due Date'].isna().sum()
                        if activated_na < len(df_mon) or due_na < len(df_mon):  # Found a working format
                            break
                    except:
                        continue
                else:
                    print("  ✗ No standard format worked. Manual inspection needed.")

            start_period, last_valid_date = get_scope_start_end(cutoff)  

            # Apply the filtering only if we have valid dates
            if not df_mon['Activated Date'].isna().all():
                df_mon = df_mon[df_mon['Activated Date'] <= last_valid_date].copy()
                print(f"After filtering by Activated Date <= {last_valid_date}:")
                print(f"  - Remaining rows: {len(df_mon)}")
            else:
                print("WARNING: Skipping date filtering because all Activated Dates are NaN")

            # Get current date as pandas Timestamp (this keeps everything in pandas datetime format)
            current_date = pd.Timestamp.now().normalize()  # normalize() sets time to 00:00:00

            # Calculate days difference between Due Date and current date
            # NOTE: Use ONLY this line, not the .dt.date version
            df_mon['days_diff'] = (df_mon['Due Date'] - current_date).dt.days

            # Create the Due Date Period column based on categorization
            def categorize_due_date(days_diff):
                if days_diff < 0:
                    return 'Overdue'
                elif days_diff <= 30:  # 0-30 days
                    return 'Approaching below 1 month'
                elif days_diff <= 90:  # 31-90 days (approximately 3 months)
                    return 'Due date within 3 months'
                elif days_diff <= 180:  # 91-180 days (approximately 6 months)
                    return 'Due date within 6 months'
                else:  # > 180 days
                    return 'Above 6 months'

            df_mon['Due Date Period'] = df_mon['days_diff'].apply(categorize_due_date)

            df_mon.to_excel('df_mon.xlsx')

            # Create the pivot table
            pivot_table = pd.pivot_table(
                df_mon, 
                values='Due Date',  # We're counting, so any column works
                index='Unit', 
                columns='Due Date Period', 
                aggfunc='count',
                fill_value=0
            )

            # Reorder columns to match your desired order
            column_order = [
                'Overdue',
                'Approaching below 1 month', 
                'Due date within 3 months',
                'Due date within 6 months',
                'Above 6 months'
            ]

            # Reorder columns (only include columns that exist)
            existing_columns = [col for col in column_order if col in pivot_table.columns]
            pivot_table = pivot_table[existing_columns]

            # Add Grand Total row and column
            pivot_table.loc['Grand Total'] = pivot_table.sum()
            pivot_table['Grand Total'] = pivot_table.sum(axis=1)
       
        
        except Exception as e:
            error_msg = f"Data transformation failed: {str(e)}"
            module_errors.append(error_msg)
            print(f"❌ {error_msg}")
            # Continue with partial data if possible
        
        # ══════════════════════════════════════════════════════════════════
        # 1. REINFORCED MONITORING GENERATION
        # ══════════════════════════════════════════════════════════════════

        try:
            print("🔄 Generating and saving reinforced monitoring table...")
            # Apply styling to your pivot table

            if not pivot_table.empty and len(pivot_table.columns) > 0:
                try:
                    styled_table = format_due_date_table(
                        pivot_table, 
                        table_colors=table_colors, 
                        program='ERCEA', 
                        call='Reinforced Monitoring',
                        table_subtitle='Project Due Date Distribution'
                    )
                    print("\n" + "="*50)
                    print("STYLED TABLE CREATED SUCCESSFULLY!")
                    print("="*50)
                    # Display the styled table
                    
                    # Save to database with GT table
                    var_name = 'reinforced_monitoring_table'
                    try:
                        logger.debug(f"Saving {var_name} to database")
                        insert_variable(
                            report=report, module="ReinMonModule", var=var_name,
                            value=pivot_table,
                            db_path=db_path, anchor=var_name, 
                            gt_table=styled_table,  # Use GT table instead of pandas Styler
                            simple_gt_save=True,    # Use simple save method
                            table_width=1200,       # Specify width for better rendering
                            table_height=600        # Specify height
                        )
                        logger.debug(f"Saved {var_name} to database")
                        print(f'\n🎉 SUCCESSFULLY saved {var_name} to database ')
                        
                    except Exception as e:
                        logger.error(f"Failed to save {var_name}: {str(e)}")
                        
                except Exception as e:
                    print(f"Styling failed: {e}")
                    print("Displaying basic pivot table instead:")
                    print(pivot_table)
                 
            else:
                print("Cannot create styled table - pivot table is empty")

        
        except Exception as e:
            error_msg = f"Generation of Reinforced Monitoring table failed: {str(e)}"
            module_errors.append(error_msg)
            print(f"❌ {error_msg}")

        
        # ══════════════════════════════════════════════════════════════════
        # 2. MODULE COMPLETION STATUS
        # ══════════════════════════════════════════════════════════════════
        
        print("\n" + "="*60)
        print("📈 REINFORCED MONITORING MODULE COMPLETION SUMMARY")
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


        print("="*60)
        print("🏁 Reinforced Monitoring Module completed")
        print("="*60)

        # Return the context (don't return boolean success/failure)
        return ctx
