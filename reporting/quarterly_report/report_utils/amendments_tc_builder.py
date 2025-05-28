from __future__ import annotations

import logging
from pathlib import Path
import pandas as pd
import numpy as np
import calendar
from datetime import date
from ingestion.db_utils import (
    fetch_latest_table_data,
    insert_variable,
    load_report_params,
)
from reporting.quarterly_report.utils import RenderContext
from great_tables import GT, loc, style, html
import altair as alt
from typing import List, Tuple , Union, Dict, Any
import sqlite3
from altair_saver import save
import tempfile
import calendar
from selenium import webdriver
from webdriver_manager.chrome import ChromeDriverManager
from selenium.webdriver.chrome.service import Service
from PIL import Image, ImageDraw
import pkg_resources
from selenium.webdriver.common.by import By
import time
from pathlib import Path



logger = logging.getLogger("Amendments")


# Configure logging
logging.basicConfig(
    level=logging.DEBUG,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s',
    handlers=[
        logging.FileHandler('amendments_report.log'),
        logging.StreamHandler()
    ]
)
logger = logging.getLogger("Amendments")

CALLS_TYPES_LIST = ['STG', 'ADG', 'POC', 'COG', 'SYG', 'StG', 'CoG', 'AdG', 'SyG', 'PoC', 'CSA']



# Helper functions (copied from original script)
def determine_epoch_year(cutoff_date: pd.Timestamp) -> int:
    logger.debug(f"Determining epoch year for cutoff_date: {cutoff_date}")
    return cutoff_date.year - 1 if cutoff_date.month == 1 else cutoff_date.year


def get_scope_start_end(cutoff: pd.Timestamp, amendments_report_date: pd.Timestamp) -> Tuple[pd.Timestamp, pd.Timestamp]:
    """
    Unified scope logic with year transition:
    • If cutoff is in January → report full previous year
    • Otherwise → return start of year to quarter-end
    """
    logger.debug(f"Calculating scope for cutoff: {cutoff}, amendments_report_date: {amendments_report_date}")
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
    
    start = pd.Timestamp(year=cutoff.year, month=1, day=1)
    end = quarter_end(cutoff)
    logger.debug(f"Scope start: {start}, end: {end}")
    return start, end


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


## function for detecting nan cells
def is_nan(x):
    result = (x != x)
    return result


def update_start_date(row, amd_report_date):
    status = row['STATUS']
    tta_ongoing = row['TTA\nONGOING']
    tta = row['TTA']
    end_date = pd.to_datetime(row['END\nDATE'], errors='coerce')
    amd_report_date = pd.to_datetime(amd_report_date, errors='coerce')
    try:
        if status in ['SIGNED_CR', 'REJECTED_CR', 'WITHDRAWN_CR']:
            delta = pd.to_timedelta(tta or tta_ongoing, unit='D')
            row['START\nDATE'] = (end_date or amd_report_date) - delta
        elif status in ['RECEIVED_CR', 'ASSESSED_CR', 'MISSING_INFO', 'OPENED_EXT_CR', 'OPENED_INT_CR']:
            if pd.notna(tta_ongoing):
                row['START\nDATE'] = amd_report_date - pd.to_timedelta(tta_ongoing, unit='D')
    except Exception as e:
        raise
    return row

def determine_po_category(row):
    instrument = str(row.get('INSTRUMENT', '')).strip()
    topic = str(row.get('TOPIC', '')).strip()
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


def chart_machine_tta(df: pd.DataFrame, prog: str, rolling_tta: pd.DataFrame) -> Image:
    logger.info(f"Rendering of {prog} TTA chart started ")
    """
    Render TTA performance chart with monthly values, rolling averages,
    contractual time limits, and annotation.
    """
    import altair_saver
    import os
    colorScheme = "blues"
    title_color = '#1B5390'
    
    def my_theme():
        return {
            'config': {
                'view': {'continuousHeight': 300, 'continuousWidth': 400},  # from the default theme
                'range': {'category': {'scheme': colorScheme }},
                "title": {"fontSize": 18, "font": 'Lato', "anchor": "center",'color':title_color,'fontWeight':'bold',},
                "legend" :{'labelColor':'black',"strokeColor":"black","padding": 10,"strokeColor": '#49b2d0','fillColor':'#d6eef4'}
            }
            }
    alt.themes.register('my_theme', my_theme)
    alt.themes.enable('my_theme')

    # Convert month names to numbers (safe mapping for altair x-axis)
    month_abbr_map = {abbr: i for i, abbr in enumerate(calendar.month_abbr) if abbr}
    df = df.copy()
    df = df[df['Month'] != 'Total']  # Drop total row if present
    df['MonthNum'] = df['Month'].map(month_abbr_map)
    df['TTA'] = df['Total']  # assume Total col is the TTA to plot per month

    # Time limit line (static for 12 months)
    df_time_limit = pd.DataFrame({
        'MonthNum': list(range(1, 13)),
        'time_limit': [45] * 12
    })

    # Clean rolling_tta input
    rolling_tta = rolling_tta.dropna(subset=['TTA']).copy()
    rolling_tta['MonthNum'] = rolling_tta['Month'].astype(int)
    rolling_tta['TTA'] = pd.to_numeric(rolling_tta['TTA'], errors='coerce')

    # TTA bars
    bar = alt.Chart(df).mark_bar(
        size=45,
        opacity=0.7,
        color='#4c78a8'
    ).encode(
        x=alt.X('MonthNum:O', title='Month', axis=alt.Axis(labelExpr='datum.label', labelAngle=0)),
        y=alt.Y('TTA:Q', title='Number of Days'),
    )

    # Labels
    labels = alt.Chart(df).mark_text(
        dy=-10,
        color='black',
        fontSize=12,
        fontWeight='bold'
    ).encode(
        x='MonthNum:O',
        y='TTA:Q',
        text=alt.Text('TTA:Q', format='.1f')
    )

    # Rolling average line
    avg_line = alt.Chart(rolling_tta).mark_line(
        strokeDash=[4, 3],
        color='red',
        size=2
    ).encode(
        x='MonthNum:O',
        y='TTA:Q'
    )

    # Time limit line
    limit_line = alt.Chart(df_time_limit).mark_line(
        color='orange',
        size=2
    ).encode(
        x='MonthNum:O',
        y='time_limit:Q'
    )

    # Optional annotation (only if data exists)
    if not rolling_tta.empty:
        last_row = rolling_tta.iloc[-1]
        annotation_df = pd.DataFrame([{
            'MonthNum': last_row['MonthNum'],
            'TTA': last_row['TTA'],
            'Comment': f'{prog} Avg TTA {date.today().year}: {last_row["TTA"]:.1f}',
            'Arrow': '➟'
        }])

        annotation_text = alt.Chart(annotation_df).mark_text(
            dx=-50, dy=-50, fontSize=13, fontWeight='bold', color='black'
        ).encode(
            x='MonthNum:O',
            y='TTA:Q',
            text='Comment'
        )

        arrow = alt.Chart(annotation_df).mark_text(
            dx=-30, dy=-5, angle=90, fontSize=26, color='orange'
        ).encode(
            x='MonthNum:O',
            y='TTA:Q',
            text='Arrow'
        )

        chart = (bar + labels + avg_line + limit_line + annotation_text + arrow)
    else:
        chart = (bar + labels + limit_line)

    return chart.properties(title=f'{prog} - TTA Target', width=600, height=300)

# ──────────────────────────────────────────────────────────────
# TABLES FUNCTIONS
# ──────────────────────────────────────────────────────────────

def amendment_cases(
    df_amd_cases: pd.DataFrame,
    programme: str,
    months_scope: list[int],
    epoch_year: int
) -> pd.DataFrame:
    
    try:
        df = df_amd_cases.copy()
        # Normalize DESCRIPTION
        df['DESCRIPTION'] = df['DESCRIPTION'].astype(str).str.split('\n')
        df = df.explode('DESCRIPTION')

        df['DESCRIPTION'] = (
            df['DESCRIPTION']
            .astype(str)
            .str.strip()
            .str.replace(r'_x000D_', '', regex=True)
            .str.lstrip('+-')
        )
        df['DESCRIPTION'] = df['DESCRIPTION'].replace("", np.nan)
        df.dropna(subset=['DESCRIPTION'], inplace=True)

        # Filter
        df = df[
            (df['FRAMEWORK'] == programme) &
            (df['StartYear'] == epoch_year) &
            (df['StartMonth'].isin(months_scope))
        ]
        # Add counter
        df['COUNTER'] = 1
        # Pivot
        pivot = df.pivot_table(
            index='DESCRIPTION',
            columns='CALL_TYPE',
            values='COUNTER',
            fill_value=0,
            aggfunc='sum'
        ).reset_index()
     
        # Compute percentages
        instrument_cols = [col for col in pivot.columns if col != 'DESCRIPTION']
        for col in instrument_cols:
            pct_col = f'As % of Total {col}'
            pivot[pct_col] = pivot[col] / pivot[col].sum()

        new_cols = []
        for col in instrument_cols:
            pct_col = f'As % of Total {col}'
            new_cols.extend([col, pct_col])
        pivot = pivot[['DESCRIPTION'] + new_cols]

        # Total row
        totals = pivot[instrument_cols].sum()
        totals['DESCRIPTION'] = 'Total'
        for col in instrument_cols:
            totals[f'As % of Total {col}'] = 1.0
        pivot = pd.concat([pivot, pd.DataFrame([totals])], ignore_index=True)

        # Final total column
        pivot['Total No'] = pivot[instrument_cols].sum(axis=1)
        total_of_totals = pivot.loc[pivot['DESCRIPTION'] == 'Total', 'Total No'].values[0]
        pivot['Total No Pct'] = pivot['Total No'] / total_of_totals

        # Format percentages
        for col in pivot.columns:
            if 'As % of Total' in col or col == 'Total No Pct':
                pivot[col] = pivot[col].map('{:.1%}'.format)
        return pivot
    except Exception as e:
        raise e


def generate_amendment_pivot(
    df: pd.DataFrame,
    programme: str,
    statuses: List[str],
    value_col: str,
    aggfunc: Union[str, callable],
    column_name: str,
    months_scope: List[int],
    epoch_year: int,
    month_col: str = 'EndMonth',
    year_col: str = 'EndYear',
    fill_value=0,
    margin_name='Total',
    rename_col='Month'
) -> pd.DataFrame:

    try:
        df = df.copy()
        # Normalize aggregation function
        if isinstance(aggfunc, str):
            aggfunc_str = aggfunc
        elif aggfunc == np.sum:
            aggfunc_str = 'sum'
        elif aggfunc == np.mean:
            aggfunc_str = 'mean'
        else:
            aggfunc_str = aggfunc

        # Filter data
        df_filtered = df[
            (df['FRAMEWORK'] == programme) &
            (df[year_col] == epoch_year) &
            (df['STATUS'].isin(statuses)) &
            (df[month_col].isin(months_scope))
        ].copy()

        df_filtered['Counter'] = 1

        # Generate pivot
        pivot = df_filtered.pivot_table(
            index=[month_col],
            columns='CALL_TYPE',
            values=value_col,
            fill_value=fill_value,
            aggfunc=aggfunc_str,
            margins=True,
            margins_name=margin_name
        )

        pivot.reset_index(inplace=True)

        if isinstance(pivot.columns, pd.MultiIndex):
            pivot.columns = pivot.columns.droplevel()
        pivot.rename(columns={pivot.columns[0]: rename_col}, inplace=True)

        # Fill missing months
        existing_months = pivot[rename_col].tolist()
        missing_months = [m for m in months_scope if m not in existing_months]
  
        if missing_months:
            for m in missing_months:
                pivot = pd.concat([pivot, pd.DataFrame([{rename_col: m}])], ignore_index=True)
            not_total = pivot[pivot[rename_col] != margin_name]
            total_row = pivot[pivot[rename_col] == margin_name]
            pivot = pd.concat([not_total.sort_values(by=rename_col), total_row], ignore_index=True)

        # Replace month numbers with names
        month_map = {i: calendar.month_abbr[i] for i in range(1, 13)}
        pivot[rename_col] = pivot[rename_col].map(month_map).fillna(pivot[rename_col])
        pivot = pivot.fillna(0)
        return pivot
    except Exception as e:
        raise e

def rolling_tta(df: pd.DataFrame, programme: str, months_scope: list[int], epoch_year: int) -> pd.DataFrame:

    try:
        df_filtered = df[
            (df['FRAMEWORK'] == programme) &
            (df['EndYear'] == epoch_year) &
            (df['STATUS'] == 'SIGNED_CR') &
            (~df['TTA'].isna())
        ]
      
        results = []
        for month in months_scope:
            df_subset = df_filtered[df_filtered['EndMonth'] <= month]
            mean_val = df_subset['TTA'].mean()
            results.append({'Month': month, 'TTA': mean_val})
        result_df = pd.DataFrame(results)
        return result_df
    except Exception as e:
        raise e

def table_signed_function(
    df: pd.DataFrame,
    programme: str,
    months_scope: list[int],
    epoch_year: int
) -> tuple[pd.DataFrame, pd.DataFrame]:

    try:
        df = df.copy()
        df['INSTRUMENT'] = df['INSTRUMENT'].apply(lambda x: 'ERC-POC' if 'POC' in str(x) else x)

        signed_mask = (
            (df['FRAMEWORK'] == programme) &
            (df['EndYear'] == epoch_year) &
            (df['STATUS'] == 'SIGNED_CR') &
            (~df['TTA'].isna() | ~df['TTA\nONGOING'].isna())
        )
        df_signed = df[signed_mask].copy()

        pivot_signed = generate_amendment_pivot(
            df_signed,
            programme=programme,
            statuses=['SIGNED_CR'],
            value_col='Counter',
            aggfunc=np.sum,
            column_name='Signed',
            months_scope=months_scope,
            epoch_year=epoch_year,
            month_col='EndMonth',
            year_col='EndYear'
        )
     
        pivot_tta = generate_amendment_pivot(
            df_signed,
            programme=programme,
            statuses=['SIGNED_CR'],
            value_col='TTA',
            aggfunc=np.mean,
            column_name='TTA Average',
            months_scope=months_scope,
            epoch_year=epoch_year,
            month_col='EndMonth',
            year_col='EndYear'
        ).round(1)
        return pivot_signed, pivot_tta
    except Exception as e:
        raise e

def tta_summary_metrics(df: pd.DataFrame, months_scope: list[int], epoch_year: int) -> pd.DataFrame:
    # Define the programmes to compute metrics for
    programmes = ['H2020', 'HORIZON']
    results = []

    for programme in programmes:
        # Filter the DataFrame based on the criteria
        df_filtered = df[
            (df['FRAMEWORK'] == programme) &
            (df['EndYear'] == epoch_year) &
            (df['STATUS'] == 'SIGNED_CR') &
            (~df['TTA'].isna()) &
            (df['EndMonth'].isin(months_scope))
        ].copy()

        # Determine the label for the first column
        tta_label = 'Average number of days H2020' if programme == 'H2020' else 'Average number of days HEU'

        # If no data after filtering, append a row with NaN values
        if df_filtered.empty:
            results.append({
                'Time-to-Amend H2020 - HEU': tta_label,
                'average number of days': float('nan'),
                '% within the contractual time limit': float('nan')
            })
            continue

        # Calculate the average TTA
        avg_tta = df_filtered['TTA'].mean()

        # Calculate the percentage of amendments with TTA <= 45 days
        total_amendments = len(df_filtered)
        amendments_within_45 = len(df_filtered[df_filtered['TTA'] <= 45])
        percent_within_45 = (amendments_within_45 / total_amendments) if total_amendments > 0 else 0.0

        # Append the result
        results.append({
            'Time-to-Amend H2020 - HEU': tta_label,
            'average_number_of_days': avg_tta,
            'rate': percent_within_45
        })

    # Create the final DataFrame
    return pd.DataFrame(results)

#####  !!!!!  Main report generation !!!!!  ########
def generate_amendments_report(
    conn: sqlite3.Connection,
    cutoff: pd.Timestamp,
    alias: str,
    report: str,
    db_path: Path,
    report_params: Dict,
    save_to_db: bool = True,
    export_dir: Path = Path("exports")
) -> Dict[str, Any]:

    try:
        db_path = Path(conn.execute("PRAGMA database_list").fetchone()[2])
        report_params = load_report_params(report_name=report, db_path=db_path)
        amd_report_date = pd.to_datetime(report_params.get("amendments_report_date"))

        table_colors = report_params.get('TABLE_COLORS', {})
        BLUE = table_colors.get("BLUE", "#004A99")
        LIGHT_BLUE = table_colors.get("LIGHT_BLUE", "#d6e6f4")
        DARK_BLUE = table_colors.get("DARK_BLUE", "#01244B")
        SUB_TOTAL_BACKGROUND = table_colors.get("subtotal_background_color", "#E6E6FA")

        df_amd = fetch_latest_table_data(conn, alias, cutoff)
        df_amd['CALL_TYPE'] = df_amd.apply(determine_po_category, axis=1)
        df_amd = df_amd[df_amd['AMENDMENT\nTYPE'] == 'CONSORTIUM_REQUESTED'].copy()
        df_amd = df_amd[df_amd['CALL_TYPE'] != 'CSA'].copy()
        df_amd = df_amd.apply(lambda row: update_start_date(row, amd_report_date), axis=1)
   
        df_amd['START\nDATE'] = pd.to_datetime(df_amd['START\nDATE'], errors='coerce')
        df_amd['StartYear'] = df_amd['START\nDATE'].dt.year
        df_amd['StartMonth'] = df_amd['START\nDATE'].dt.month
        df_amd['END\nDATE'] = pd.to_datetime(df_amd['END\nDATE'], errors='coerce')
        df_amd['EndYear'] = df_amd['END\nDATE'].dt.year
        df_amd['EndMonth'] = df_amd['END\nDATE'].dt.month
        
        # df_amd.to_excel('amd.xlsx')
        epoch_year = determine_epoch_year(cutoff)
        months_scope = list(range(1, cutoff.month if cutoff.month != 1 else 13))
        # logging.info(f'months in scope {months_scope}')

        df_tta_summary_metrics = tta_summary_metrics (df_amd, months_scope, epoch_year)

        tbl_tta_summary_metrics= (
            GT(
                df_tta_summary_metrics,
            )
            .opt_table_font(font="Arial")
            .opt_table_outline(style = "solid", width = '1px', color ="#cccccc") 
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
            .tab_style(
                style=style.borders(sides="all", color="#ffffff", weight="2px"),
                locations=loc.column_labels()
            )
            # Row group styling
            .tab_style(
                style=[
                    style.text(color=DARK_BLUE, weight="bold", size='medium'),
                    style.fill(color=LIGHT_BLUE),
                    style.css(f"line-height:1.2; padding:5px;")
                ],
                locations=loc.row_groups()
            )

            # Column labels styling
            .tab_style(
                style=[
                    style.fill(color=BLUE),
                    style.text(color="white", weight="bold", align="center", size='small'),
                    style.css("min-width:50px; padding:5px; line-height:1.2")
                
                ],
                locations=loc.column_labels()
            )

            # Stubhead styling
            .tab_style(
                style=[
                    style.fill(color='white'),
                    style.text(color=DARK_BLUE, weight="bold", align="center", size='small'),
                    style.css("min-width:150px; padding:20px; line-height:1.2")
                ],
                locations=loc.stubhead()
            )

            # Body cell styling
            .tab_style(
                style=[
                    style.text(align="center", size='small'),
                    style.css("padding:5px")
                ],
                locations=loc.body()
            )

            # Stub cell styling
            .tab_style(
                style=[
                    style.text(size='small'),
                    style.css("padding:5px")
                ],
                locations=loc.stub()
            )
            # BODY
            .fmt_percent(
                columns=[ 'rate' ],  # or use `where` with a condition
                decimals=1
            )
            .fmt_number(
                columns=['average_number_of_days'],
                decimals=1,
                accounting=False
            )
            .cols_label(
                average_number_of_days=html(
                    "<span>average number</span><br>"
                    "<span style='text-align:center; display:block;'> of days</span>"
                )
            )
        .cols_label(
            rate=html(
                "<span>% within the </span><br>"
                "<span>contractual</span><br>"
                "<span>time limit</span>"
            )
        )

        .tab_style(
            style=[style.fill(color=BLUE), style.text(color=DARK_BLUE, weight="bold", v_align="middle")],
            locations=loc.body(rows=[-1])
        )

        # Footer styling
        .tab_source_note("Source: Compass")
        .tab_source_note("Report: Amendments Report")
        .tab_style(
            style=[
                style.text(size="small"),
                style.css("padding:5px; line-height:1.2")
            ],
            locations=loc.footer()
        )
    )

        try:
            logger.debug(f"Saving tbl_tta_summary_metrics to database")
            insert_variable(
                report=report, module="AmendmentModule", var='tbl_tta_summary_metrics',
                value=df_tta_summary_metrics.to_dict(),
                db_path=db_path, anchor='overview_tta_summary', gt_table=tbl_tta_summary_metrics
            )
            logger.debug(f"Saved tbl_tta_summary_metrics  to database")
        except Exception as e:
            logger.error(f"Failed to save tbl_tta_summary_metrics : {str(e)}")

        results = {}
        for programme in ['H2020', 'HORIZON']:
            received_statuses = ['SIGNED_CR', 'ASSESSED_CR', 'OPENED_EXT_CR', 'OPENED_INT_CR', 'RECEIVED_CR', 'WITHDRAWN_CR', 'REJECTED_CR']

            amd_received = generate_amendment_pivot(df_amd, programme, received_statuses, 'Counter', 'sum', 'Received', months_scope, epoch_year, 'StartMonth', 'StartYear')
            amd_rejected = generate_amendment_pivot(df_amd, programme, ['REJECTED_CR', 'WITHDRAWN_CR'], 'Counter', 'sum', 'Rejected', months_scope, epoch_year,'EndMonth', 'EndYear')
            amd_signed = generate_amendment_pivot(df_amd, programme, ['SIGNED_CR'], 'Counter', 'sum', 'Signed', months_scope, epoch_year,'EndMonth', 'EndYear')
            
            amd_received.to_excel(f'{programme}_amd_received.xlsx')
            amd_overview = pd.concat([
                amd_received.assign(TYPE_ROW_NAME='Amendments Received'),
                amd_signed.assign(TYPE_ROW_NAME='Amendments Signed'),
                amd_rejected.assign(TYPE_ROW_NAME='Amendments Rejected or Withdrawn')
            ])

            amd_overview .fillna(0, inplace=True)
            
            # amd_overview.to_excel(f'{programme}_amd_overview.xlsx')
            total_rows_indices = amd_overview.reset_index().query('Month == "Total"').index.tolist()
 
            overview_table = (
                GT(amd_overview, rowname_col="Month", groupname_col="TYPE_ROW_NAME")
                .tab_header(title=programme)
                .opt_table_font(font="Arial")
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
                .tab_style(
                    style=style.borders(sides="all", color="#ffffff", weight="2px"),
                    locations=loc.column_labels()
                )
                 # Header style
                .tab_style(
                    style.text(color=DARK_BLUE, weight="bold", align="center"),
                    locations=loc.header()
                )
                
                .tab_stubhead(label="Month")

                # Row group styling
                .tab_style(
                    style=[
                        style.text(color=DARK_BLUE, weight="bold", size='small'),
                        style.fill(color=LIGHT_BLUE),
                        style.css(f"line-height:1.2; padding:5px;")
                    ],
                    locations=loc.row_groups()
                )

                # Column labels styling
                .tab_style(
                    style=[
                        style.fill(color=BLUE),
                        style.text(color="white", weight="bold", align="center", size='small'),
                        style.css("min-width:60px; padding:5px; line-height:1.2")
                    
                    ],
                    locations=loc.column_labels()
                )

                # Stubhead styling
                .tab_style(
                    style=[
                        style.fill(color=BLUE),
                        style.text(color="white", weight="bold", align="center", size='small'),
                        style.css("padding:5px; line-height:1.2")
                    ],
                    locations=loc.stubhead()
                )

                # Body cell styling
                .tab_style(
                    style=[
                        style.text(align="center", size='small', font='Arial'),
                        style.css("padding:5px")
                    ],
                    locations=loc.body()
                )

                # Stub cell styling
                .tab_style(
                    style=[
                        style.text(size='small', font='Arial'),
                        style.css("padding:5px")
                    ],
                    locations=loc.stub()
                )

                # Format all "Total" rows consistently in stub and body
                .tab_style(
                    style=[
                        style.fill(color=SUB_TOTAL_BACKGROUND),
                        style.text(color=DARK_BLUE, weight="bold"),
                    ],
                    locations=[
                        loc.body(rows=total_rows_indices ),
                        loc.stub(rows=total_rows_indices )
                    ]
                )

                # Footer styling
                .tab_source_note("Source: Compass")
                .tab_source_note("Report: Amendments Report")
                .tab_style(
                    style=[
                        style.text(size="small", font='Arial'),
                        style.css("padding:5px; line-height:1.2")
                    ],
                    locations=loc.footer()
                )
                )
            
            pivot_signed, pivot_tta = table_signed_function(df_amd, programme, months_scope, epoch_year)
    
            tta_table = (
                GT(pivot_tta)
                .tab_header(title=f"Time to Amend - {programme}")
                .opt_table_font(font="Arial")
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
                .tab_style(
                    style=style.borders(sides="all", color="#ffffff", weight="2px"),
                    locations=loc.column_labels()
                )
                 # Header style
                .tab_style(
                    style.text(color=DARK_BLUE, weight="bold", align="center", size = 'medium'),
                    locations=loc.header()
                )
                
                .tab_stubhead(label="Amd Description")

                # Row group styling
                .tab_style(
                    style=[
                        style.text(color=DARK_BLUE, weight="bold", size='small'),
                        style.fill(color=LIGHT_BLUE),
                        style.css(f"line-height:1.2; padding:5px;")
                    ],
                    locations=loc.row_groups()
                )

                # Column labels styling
                .tab_style(
                    style=[
                        style.fill(color=BLUE),
                        style.text(color="white", weight="bold", align="center", size='small'),
                        style.css("min-width:50px; padding:5px; line-height:1.2")
                    
                    ],
                    locations=loc.column_labels()
                )

                # Stubhead styling
                .tab_style(
                    style=[
                        style.fill(color=BLUE),
                        style.text(color="white", weight="bold", align="center", size='small'),
                        style.css("padding:5px; line-height:1.2")
                    ],
                    locations=loc.stubhead()
                )

                # Body cell styling
                .tab_style(
                    style=[
                        style.text(align="center", size='small'),
                        style.css("padding:5px")
                    ],
                    locations=loc.body()
                )

                # Stub cell styling
                .tab_style(
                    style=[
                        style.text(size='small'),
                        style.css("padding:5px")
                    ],
                    locations=loc.stub()
                )

                # Format all "Total" rows consistently in stub and body

                .tab_style(
                        style=[style.fill(color=SUB_TOTAL_BACKGROUND), style.text(color=DARK_BLUE, weight="bold")],
                        locations=loc.body(rows=pivot_tta.index[pivot_tta["Month"] == "Total"].tolist())
                    )
                .tab_style(
                    style=[style.fill(color=SUB_TOTAL_BACKGROUND), style.text(color=DARK_BLUE, weight="bold")],
                    locations=loc.stub(rows=pivot_tta.index[pivot_tta["Month"] == "Total"].tolist())
                )

                # Footer styling
                .tab_source_note("Source: Compass")
                .tab_source_note("Report: Amendments Report")
                .tab_style(
                    style=[
                        style.text(size="small"),
                        style.css("padding:5px; line-height:1.2")
                    ],
                    locations=loc.footer()
                    )
                )
            logger.debug(f"Created tta_table for {programme}")

            cases_df = amendment_cases(df_amd, programme, months_scope, epoch_year)
            # cases_df.to_excel(f'{programme}_cases.xlsx')
   
            cases_table = (
                GT(cases_df)
                .tab_header(title=programme)
                .opt_table_font(font="Arial")
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
                .tab_style(
                    style=style.borders(sides="all", color="#ffffff", weight="2px"),
                    locations=loc.column_labels()
                )
               # Header style
                .tab_style(
                    style.text(color=DARK_BLUE, weight="bold", align="center"),
                    locations=loc.header()
                )
                
                .tab_stubhead(label="Amd Description")

                # Row group styling
                .tab_style(
                    style=[
                        style.text(color=DARK_BLUE, weight="bold", size='small'),
                        style.fill(color=LIGHT_BLUE),
                        style.css(f"line-height:1.2; padding:5px;")
                    ],
                    locations=loc.row_groups()
                )

                # Column labels styling
                .tab_style(
                    style=[
                        style.fill(color=BLUE),
                        style.text(color="white", weight="bold", align="center", size='small'),
                        style.css("min-width:50px; padding:5px; line-height:1.2")
                    
                    ],
                    locations=loc.column_labels()
                )

                # Stubhead styling
                .tab_style(
                    style=[
                        style.fill(color=BLUE),
                        style.text(color="white", weight="bold", align="center", size='small'),
                        style.css("padding:5px; line-height:1.2")
                    ],
                    locations=loc.stubhead()
                )

                # Body cell styling
                .tab_style(
                    style=[
                        style.text(align="center", size='small'),
                        style.css("padding:5px")
                    ],
                    locations=loc.body()
                )

                # Stub cell styling
                .tab_style(
                    style=[
                        style.text(size='small'),
                        style.css("padding:5px")
                    ],
                    locations=loc.stub()
                )
                # Format all "Total" rows consistently in stub and body

                .tab_style(
                        style=[style.fill(color=SUB_TOTAL_BACKGROUND), style.text(color=DARK_BLUE, weight="bold")],
                        locations=loc.body(rows=cases_df.index[cases_df["DESCRIPTION"] == "Total"].tolist())
                    )
                .tab_style(
                    style=[style.fill(color=SUB_TOTAL_BACKGROUND), style.text(color=DARK_BLUE, weight="bold")],
                    locations=loc.stub(rows=cases_df.index[cases_df["DESCRIPTION"] == "Total"].tolist())
                )

                # Footer styling
                .tab_source_note("Source: Compass")
                .tab_source_note("Report: Amendments Report")
                .tab_style(
                    style=[
                        style.text(size="small", font='Arial'),
                        style.css("padding:5px; line-height:1.2")
                    ],
                    locations=loc.footer()
                )
                )
            
            rolling_tta_df = rolling_tta(df_amd, programme, months_scope, epoch_year)
    
            # Assuming chart_machine_tta returns an alt.Chart
            tta_chart_img = chart_machine_tta(pivot_tta, programme, rolling_tta_df)
            logger.debug(f"Generated tta_chart_img for {programme}, type: {type(tta_chart_img)}")

            if save_to_db:
                for var_name, value, table in [
                    (f'{programme}_overview', amd_overview, overview_table),
                    (f'{programme}_cases', cases_df, cases_table),
                    (f'{programme}_tta', pivot_tta, tta_table),
                ]:
                    try:
                        logger.debug(f"Saving {var_name} to database")
                        insert_variable(
                            report=report, module="AmendmentModule", var=var_name,
                            value=value.to_dict() if isinstance(value, pd.DataFrame) else value,
                            db_path=db_path, anchor=var_name, gt_table=table
                        )
                        logger.debug(f"Saved {var_name} to database")
                    except Exception as e:
                        logger.error(f"Failed to save {var_name}: {str(e)}")

            if save_to_db:
                for var_name, value, table in [
                    (f'{programme}_tta_chart', pivot_tta, tta_chart_img),  # value=None for chart
                ]:
                    try:
                        logger.debug(f"Saving {var_name} to database")
                        insert_variable(
                            report=report, module="AmendmentModule", var=var_name,
                            value=value,
                            db_path=db_path, anchor=var_name, altair_chart=table
                        )
                        logger.debug(f"Saved {var_name} to database")
                    except Exception as e:
                        logger.error(f"Failed to save {var_name}: {str(e)}")

            results.update({
                f'{programme}_overview': amd_overview,
                f'{programme}_cases': cases_df,
                f'{programme}_tta': pivot_tta,
                f'{programme}_tta_chart': tta_chart_img
            })
        logger.info("Amendments report generation complete")
        return results
    except Exception as e:
        logger.error(f"Error in generate_amendments_report: {str(e)}", exc_info=True)
        raise



