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
from great_tables import GT, exibble, md, style, loc

# ──────────────────────────────────────────────────────────────
# COSTANTS
# ──────────────────────────────────────────────────────────────
EDES_ALIAS = "edes_warnings"

# Configure logging
logging.basicConfig(
    level=logging.DEBUG,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s',
    handlers=[
        logging.FileHandler('edes_monitoring_report.log'),
        logging.StreamHandler()
    ]
)
logger = logging.getLogger("Edes Monitoring")

CALLS_TYPES_LIST = ['STG', 'ADG', 'POC', 'COG', 'SYG', 'StG', 'CoG', 'AdG', 'SyG', 'PoC', 'CSA']
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


def determine_call_type(row):

    call = str(row.get('CALL', '')).strip()

    try:
        call and any(call_type in call for call_type in CALLS_TYPES_LIST)
        category = next(call_type for call_type in CALLS_TYPES_LIST if call_type in call).upper()
        return category
    except Exception as e:
        raise

def format_edes_table(df, cutoff, table_colors):
    
    year_value = cutoff.year
    stub_width=250
    DARK_BLUE = table_colors.get("DARK_BLUE", "#01244B")

    # Calculate table width
    base_width_per_column = 80
    data_columns = df.columns[1:].tolist()
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
    num_rows = len(df)
    
    # Calculate total height
    total_header_height = title_height + subtitle_height + column_header_height
    total_data_height = num_rows * row_height
    table_height_px = total_header_height + total_data_height + footer_padding + border_padding
    # Payment table adjustments
    # Add extra height if we have deviation rows (they might need more space)
    deviation_rows = sum(1 for idx, row in df.iterrows() 
                    if 'deviation' in str(row.iloc[0]).lower())
    if deviation_rows > 0:
        table_height_px += deviation_rows * 5  # Small extra height for colored cells
    
    # Safety margins - ensure minimum and maximum bounds
    table_height_px = max(300, min(table_height_px, 1200))  # Min 300px, Max 1200px
    table_width_px = max(600, min(table_width_px, 1800))    # Min 600px, Max 1800px
                    
    try:
        gt_ex = (
            GT(df, rowname_col="UNIT")
            .tab_header(
                title=md(f"Results of the **EDES** screening in **{year_value}**"),
                subtitle=md("This is a breakdown by Unit."),
            )
            .opt_table_font(font="Arial")
            .opt_stylize(style=3, color="blue")
            .tab_style(
                style.text(color=DARK_BLUE, align="center"),
                locations=loc.header()
            )
            .tab_stubhead(label="UNIT")
            .tab_options(
                table_width=table_width
            )
            .tab_source_note(source_note="Source Data: Compass/Sygma")
            .opt_vertical_padding(scale=0.5)
        )
        return gt_ex, table_width_px, table_height_px
    except Exception as e:
        return (f"Error processing: {str(e)}")


# ──────────────────────────────────────────────────────────────
# MAIN CLASS
# ──────────────────────────────────────────────────────────────

class EdesModule(BaseModule):
    name        = "Edes"          # shows up in UI
    description = "Edes flags by call type"

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
            df_edes = fetch_latest_table_data(conn, EDES_ALIAS, cutoff)
            start_period, last_valid_date = get_scope_start_end(cutoff)
            report_params = load_report_params(report_name=report, db_path=db_path)
            table_colors = report_params.get('TABLE_COLORS', {})
            
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
             
            # Normalize 'VALID_FROM' date
            df_edes['VALID_FROM'] = pd.to_datetime(
                df_edes['VALID_FROM'], 
                format='%Y-%m-%d %H:%M:%S',
                errors='coerce'
                )
            # Apply the date filtering to keep data in period scope
            df_edes = df_edes[ 
                df_edes['VALID_FROM'] <= last_valid_date
                ].copy()
            
            # Apply Counter column
            df_edes['COUNTER'] = 1

            # Normalize call type 
            df_edes['CALL_TYPE'] = df_edes.apply(determine_call_type, axis=1)

            # Create the pivot table
            edes_pivot = pd.pivot_table(df_edes, values='COUNTER', index=['UNIT'],
                       columns=['CALL_TYPE'],  fill_value=0, aggfunc="sum")
            
            # Drop MultiIndex and reset index
            if isinstance(edes_pivot.columns, pd.MultiIndex):
                    edes_pivot.columns = edes_pivot.columns.rename_axis(None, axis=1).reset_index(drop=True)

            edes_pivot.reset_index(inplace=True)

        except Exception as e:
            error_msg = f"Data transformation failed: {str(e)}"
            module_errors.append(error_msg)
            print(f"❌ {error_msg}")
        
        # ══════════════════════════════════════════════════════════════════
        # 1. EDES TABLE GENERATION
        # ══════════════════════════════════════════════════════════════════

        try:
            print("🔄 Generating and saving reinforced monitoring table...")

            if not edes_pivot.empty and len(edes_pivot.columns) > 0:
                var_name = 'EDES_Table'
                logger.debug(f"Creating {var_name}")
                try:
                    tbl,table_width_px, table_height_px = format_edes_table(edes_pivot, cutoff, table_colors)
                    print("\n" + "="*50)
                    print("STYLED TABLE CREATED SUCCESSFULLY!")
                    print("="*50)
                except Exception as format_error:
                    print (f"      ❌ Error formatting table for Edes: {str(format_error)}")
                
                try:    
                    logger.debug(f"Saving {var_name} to database")
                    # Save chart
                    insert_variable(
                        report=report, 
                        module="EdesModule", 
                        var=var_name,
                        value=edes_pivot,
                        db_path=db_path, 
                        anchor=var_name, 
                        simple_gt_save= True,
                        gt_table=tbl,
                        table_width=table_width_px,      # Automatically calculated
                        table_height=table_height_px ,    # Automatically calculated  
                    )
                    logger.debug(f"Saved {var_name} to database")
                    print(f'\n🎉 SUCCESSFULLY saved {var_name} to database ')

                except Exception as save_error:
                    print(f'❌ Failed to save Table Edes: {str(save_error)}')

        except Exception as e:
            error_msg = f"Generation of Reinforced Monitoring table failed: {str(e)}"
            module_errors.append(error_msg)
            print(f"❌ {error_msg}")
        
         
        # ══════════════════════════════════════════════════════════════════
        # 2. MODULE COMPLETION STATUS
        # ══════════════════════════════════════════════════════════════════
        
        print("\n" + "="*60)
        print("📈 EDES MONITORING MODULE COMPLETION SUMMARY")
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
        print("🏁 EDES Monitoring Module completed")
        print("="*60)

        # Return the context (don't return boolean success/failure)
        return ctx
          
    
