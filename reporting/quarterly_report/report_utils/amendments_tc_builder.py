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
from great_tables import GT, loc, style
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
    logger.debug(f"Determining months in scope for cutoff: {cutoff}")
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
    logger.debug(f"Months in scope: {months}")
    return months


## function for detecting nan cells
def is_nan(x):
    result = (x != x)
    logger.debug(f"Checking if value is NaN: {x} -> {result}")
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



# # HTML template with Vega-Lite
# HTML_TEMPLATE = """
# <!DOCTYPE html>
# <html>
# <head>
#     <script src="https://cdn.jsdelivr.net/npm/vega@5"></script>
#     <script src="https://cdn.jsdelivr.net/npm/vega-lite@5"></script>
#     <script src="https://cdn.jsdelivr.net/npm/vega-embed@6"></script>
# </head>
# <body>
#     <div id="vis"></div>
# </body>
# </html>
# """

def chart_machine_tta(df: pd.DataFrame, prog: str, rolling_tta: pd.DataFrame) -> Image:
    """Return a PIL.Image containing the TTA chart for *prog*.

    The routine:
    1. Builds an Altair chart (bars + labels + rolling average + 45‑day limit).
    2. Writes the VEGALite spec to JSON and an HTML scaffold.
    3. Opens the HTML with Selenium/Chrome headless, renders to canvas, grabs PNG.
    4. Falls back to a blank placeholder image if anything goes wrong.
    """
    import os, json, time, tempfile
    from PIL import Image, ImageDraw
    from selenium import webdriver
    from selenium.webdriver.common.by import By
    from selenium.webdriver.chrome.service import Service
    from webdriver_manager.chrome import ChromeDriverManager
    import altair_saver

    logger = logging.getLogger("Amendments")
    logger.info("📊 Building TTA chart for %s", prog)

    # ── 0) Sanity checks on incoming DF ──────────────────────
    if df.empty or {'Month', 'Total'}.issubset(df.columns) is False:
        logger.warning("pivot_tta for %s missing critical columns; returning placeholder", prog)
        img = Image.new('RGB', (600, 300), 'white')
        ImageDraw.Draw(img).text((10, 10), f"No TTA data for {prog}", fill='black')
        return img

    # Strip aggregate row if present
    if 'Total' in df['Month'].values:
        df = df[df['Month'] != 'Total'].copy()

    # Map month names → ordinal
    month_abbr = {calendar.month_abbr[i].lower(): i for i in range(1, 13)}
    df['MonthNum'] = df['Month'].str.lower().map(month_abbr)
    df['TTA'] = df['Total']

    # Clean rolling‑avg DF
    rolling_tta = rolling_tta.dropna(subset=['TTA']).copy()
    rolling_tta['MonthNum'] = rolling_tta['Month'].astype(int)
    rolling_tta['TTA'] = pd.to_numeric(rolling_tta['TTA'], errors='coerce')

    # Altair theme
    def _theme():
        return {
            'config': {
                'view': {'continuousHeight': 300, 'continuousWidth': 400},
                'range': {'category': {'scheme': 'tableau10'}},
                'title': {'fontSize': 18, 'font': 'Lato', 'anchor': 'center', 'color': '#333', 'fontWeight': 'bold'},
            }
        }
    alt.themes.register('tta_theme', _theme); alt.themes.enable('tta_theme')

    # Build layers
    bar = alt.Chart(df).mark_bar(size=45, opacity=0.7, color='#4c78a8').encode(
        x=alt.X('MonthNum:O', title='Month'),
        y=alt.Y('TTA:Q', title='Number of Days')
    )
    labels = bar.mark_text(dy=-10, fontSize=12, fontWeight='bold', color='black').encode(text='TTA:Q')
    avg_line = alt.Chart(rolling_tta).mark_line(color='red', strokeDash=[4, 3]).encode(
        x='MonthNum:O', y='TTA:Q')
    limit = alt.Chart(pd.DataFrame({'MonthNum': range(1,13), 'limit': [45]*12})).mark_line(color='orange').encode(
        x='MonthNum:O', y='limit:Q')

    chart = (bar + labels + avg_line + limit).properties(title=f'{prog} Quarterly – TTA', width=600, height=300)

    try:
        tmp_png = tempfile.NamedTemporaryFile(suffix='.png', delete=False)
        altair_saver.save(chart, tmp_png.name, method="cairo")
        img = Image.open(tmp_png.name)

        # SAVE COPY TO DEBUG FOLDER
        save_dir = "charts_out"
        os.makedirs(save_dir, exist_ok=True)
        out_path = os.path.join(save_dir, f"{prog}_tta_chart.png")
        img.save(out_path)
        print("Saving chart image to:", out_path)
        logger.info(f"Saving chart image to: {os.path.abspath(out_path)}")
        return img
    except Exception as exc:
        logger.error("Failed PNG render for %s: %s", prog, exc, exc_info=True)
        img = Image.new('RGB', (600,300), 'white')
        ImageDraw.Draw(img).text((10,10), f"Chart error for {prog}", fill='black')
        # SAVE COPY TO DEBUG FOLDER
        save_dir = "charts_out"
        os.makedirs(save_dir, exist_ok=True)
        out_path = os.path.join(save_dir, f"{prog}_tta_chart.png")
        img.save(out_path)
        print("Saving chart image to:", out_path)
        logger.info(f"Saving chart image to: {os.path.abspath(out_path)}")
        return img

    # # ── 1) Serialize Vega‑Lite spec & HTML scaffold ────────────
    # tmp_png = tempfile.NamedTemporaryFile(suffix='.png', delete=False)
    # json_path = tmp_png.name.replace('.png', '.json')
    # html_path = tmp_png.name.replace('.png', '.html')
    # with open(json_path, 'w') as f: json.dump(json.loads(chart.to_json()), f)
    # with open(html_path, 'w') as f: f.write(HTML_TEMPLATE)

    # # ── 2) Render with headless Chrome ────────────────────────
    # try:
    #     opts = webdriver.ChromeOptions(); opts.add_argument('--headless'); opts.add_argument('--no-sandbox'); opts.add_argument('--disable-gpu')
    #     driver = webdriver.Chrome(service=Service(ChromeDriverManager().install()), options=opts)
    #     driver.get(f'file:///{html_path}')
    #     time.sleep(1)
    #     # Inject spec & render to canvas → dataURL
    #     driver.execute_script(
    #         """
    #         var spec = JSON.parse(window.fetch(arguments[0]).then(r=>r.text()));
    #         """, json_path)
    #     script = f"""
    #         fetch('file://{json_path}').then(r=>r.json()).then(spec=>{{
    #             vegaEmbed('#vis', spec, {{renderer:'canvas'}}).then(v=>{{
    #                 v.view.toImageURL('png').then(url=>{{document.body.insertAdjacentHTML('beforeend',`<img id='grab' src='${{url}}'>`)}});
    #             }});
    #         }});
    #     """
    #     driver.execute_script(script)
    #     time.sleep(3)
    #     data_uri = driver.find_element(By.ID, 'grab').get_attribute('src')
    #     driver.get(data_uri)
    #     driver.save_screenshot(tmp_png.name)
    #     logger_msg = f"Chart PNG saved to {tmp_png.name} ({os.path.getsize(tmp_png.name)} bytes)"
    #     logger.info(logger_msg)
    # except Exception as exc:
    #     logger.error("Failed PNG render for %s: %s", prog, exc, exc_info=True)
    #     img = Image.new('RGB', (600,300), 'white'); ImageDraw.Draw(img).text((10,10), f"Chart error for {prog}", fill='black')
    #     return img
    # finally:
    #     try: driver.quit()
    #     except: pass
    # return Image.open(tmp_png.name)


# ──────────────────────────────────────────────────────────────
# TABLES FUNCTIONS
# ──────────────────────────────────────────────────────────────

def amendment_cases(
    df_amd_cases: pd.DataFrame,
    programme: str,
    months_scope: list[int],
    epoch_year: int
) -> pd.DataFrame:
    logger.info(f"Starting amendment_cases for programme: {programme}, epoch_year: {epoch_year}, months_scope: {months_scope}")
    logger.debug(f"Input DataFrame shape: {df_amd_cases.shape}, columns: {df_amd_cases.columns.tolist()}")
    
    try:
        df = df_amd_cases.copy()
        logger.debug("Copied input DataFrame")

        # Normalize DESCRIPTION
        df['DESCRIPTION'] = df['DESCRIPTION'].astype(str).str.split('\n')
        logger.debug("Split DESCRIPTION column")
        df = df.explode('DESCRIPTION')
        logger.debug(f"After explode, DataFrame shape: {df.shape}")

        df['DESCRIPTION'] = (
            df['DESCRIPTION']
            .astype(str)
            .str.strip()
            .str.replace(r'_x000D_', '', regex=True)
            .str.lstrip('+-')
        )
        logger.debug("Normalized DESCRIPTION column")

        # Filter
        df = df[
            (df['FRAMEWORK'] == programme) &
            (df['StartYear'] == epoch_year) &
            (df['StartMonth'].isin(months_scope))
        ]
        logger.debug(f"Filtered DataFrame shape: {df.shape}")

        # Add counter
        df['COUNTER'] = 1
        logger.debug("Added COUNTER column")

        # Pivot
        pivot = df.pivot_table(
            index='DESCRIPTION',
            columns='CALL_TYPE',
            values='COUNTER',
            fill_value=0,
            aggfunc='sum'
        ).reset_index()
        logger.debug(f"Pivot table created, shape: {pivot.shape}, columns: {pivot.columns.tolist()}")

        # Compute percentages
        instrument_cols = [col for col in pivot.columns if col != 'DESCRIPTION']
        logger.debug(f"Instrument columns: {instrument_cols}")
        for col in instrument_cols:
            pct_col = f'As % of Total {col}'
            pivot[pct_col] = pivot[col] / pivot[col].sum()
            logger.debug(f"Computed percentage column: {pct_col}")

        new_cols = []
        for col in instrument_cols:
            pct_col = f'As % of Total {col}'
            new_cols.extend([col, pct_col])
        pivot = pivot[['DESCRIPTION'] + new_cols]
        logger.debug(f"Reordered columns: {pivot.columns.tolist()}")

        # Total row
        totals = pivot[instrument_cols].sum()
        totals['DESCRIPTION'] = 'Total'
        for col in instrument_cols:
            totals[f'As % of Total {col}'] = 1.0
        pivot = pd.concat([pivot, pd.DataFrame([totals])], ignore_index=True)
        logger.debug(f"Added Total row, pivot shape: {pivot.shape}")

        # Final total column
        pivot['Total No'] = pivot[instrument_cols].sum(axis=1)
        total_of_totals = pivot.loc[pivot['DESCRIPTION'] == 'Total', 'Total No'].values[0]
        pivot['Total No Pct'] = pivot['Total No'] / total_of_totals
        logger.debug(f"Added Total No and Total No Pct, total_of_totals: {total_of_totals}")

        # Format percentages
        for col in pivot.columns:
            if 'As % of Total' in col or col == 'Total No Pct':
                pivot[col] = pivot[col].map('{:.1%}'.format)
        logger.debug("Formatted percentage columns")

        logger.info(f"amendment_cases completed for {programme}, output shape: {pivot.shape}")
        return pivot
    except Exception as e:
        logger.error(f"Error in amendment_cases for {programme}: {str(e)}", exc_info=True)
        raise



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
    logger.info(f"Starting generate_amendment_pivot for programme: {programme}, statuses: {statuses}, value_col: {value_col}")
    logger.debug(f"Input DataFrame shape: {df.shape}, columns: {df.columns.tolist()}")

    try:
        df = df.copy()
        logger.debug("Copied input DataFrame")

        # Normalize aggregation function
        if isinstance(aggfunc, str):
            aggfunc_str = aggfunc
        elif aggfunc == np.sum:
            aggfunc_str = 'sum'
        elif aggfunc == np.mean:
            aggfunc_str = 'mean'
        else:
            aggfunc_str = aggfunc
        logger.debug(f"Aggregation function: {aggfunc_str}")

        # Filter data
        df_filtered = df[
            (df['FRAMEWORK'] == programme) &
            (df[year_col] == epoch_year) &
            (df['STATUS'].isin(statuses)) &
            (df[month_col].isin(months_scope))
        ].copy()
        logger.debug(f"Filtered DataFrame shape: {df_filtered.shape}")

        df_filtered['Counter'] = 1
        logger.debug("Added Counter column")

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
        logger.debug(f"Pivot table created, shape: {pivot.shape}, columns: {pivot.columns.tolist()}")

        pivot.reset_index(inplace=True)
        logger.debug("Reset index")

        if isinstance(pivot.columns, pd.MultiIndex):
            pivot.columns = pivot.columns.droplevel()
            logger.debug("Dropped MultiIndex level")
        pivot.rename(columns={pivot.columns[0]: rename_col}, inplace=True)
        logger.debug(f"Renamed first column to: {rename_col}")

        # Fill missing months
        existing_months = pivot[rename_col].tolist()
        missing_months = [m for m in months_scope if m not in existing_months]
        logger.debug(f"Existing months: {existing_months}, missing months: {missing_months}")

        if missing_months:
            for m in missing_months:
                pivot = pd.concat([pivot, pd.DataFrame([{rename_col: m}])], ignore_index=True)
            not_total = pivot[pivot[rename_col] != margin_name]
            total_row = pivot[pivot[rename_col] == margin_name]
            pivot = pd.concat([not_total.sort_values(by=rename_col), total_row], ignore_index=True)
            logger.debug(f"Added missing months, new pivot shape: {pivot.shape}")

        # Replace month numbers with names
        month_map = {i: calendar.month_abbr[i] for i in range(1, 13)}
        pivot[rename_col] = pivot[rename_col].map(month_map).fillna(pivot[rename_col])
        logger.debug(f"Month names mapped: {pivot[rename_col].tolist()}")

        pivot = pivot.fillna(0)
        logger.debug("Filled NaN values with 0")
        logger.info(f"generate_amendment_pivot completed for {programme}, output shape: {pivot.shape}")
        return pivot
    except Exception as e:
        logger.error(f"Error in generate_amendment_pivot for {programme}: {str(e)}", exc_info=True)
        raise



def rolling_tta(df: pd.DataFrame, programme: str, months_scope: list[int], epoch_year: int) -> pd.DataFrame:
    logger.info(f"Starting rolling_tta for programme: {programme}, epoch_year: {epoch_year}, months_scope: {months_scope}")
    logger.debug(f"Input DataFrame shape: {df.shape}, columns: {df.columns.tolist()}")

    try:
        df_filtered = df[
            (df['FRAMEWORK'] == programme) &
            (df['EndYear'] == epoch_year) &
            (df['STATUS'] == 'SIGNED_CR') &
            (~df['TTA'].isna())
        ]
        logger.debug(f"Filtered DataFrame shape: {df_filtered.shape}")

        results = []
        for month in months_scope:
            df_subset = df_filtered[df_filtered['EndMonth'] <= month]
            mean_val = df_subset['TTA'].mean()
            results.append({'Month': month, 'TTA': mean_val})
            logger.debug(f"Month {month}: mean TTA = {mean_val}, subset shape: {df_subset.shape}")

        result_df = pd.DataFrame(results)
        logger.debug(f"Rolling TTA DataFrame: {result_df.to_dict()}")
        logger.info(f"rolling_tta completed for {programme}, output shape: {result_df.shape}")
        return result_df
    except Exception as e:
        logger.error(f"Error in rolling_tta for {programme}: {str(e)}", exc_info=True)
        raise

def table_signed_function(
    df: pd.DataFrame,
    programme: str,
    months_scope: list[int],
    epoch_year: int
) -> tuple[pd.DataFrame, pd.DataFrame]:
    logger.info(f"Starting table_signed_function for programme: {programme}, epoch_year: {epoch_year}")
    logger.debug(f"Input DataFrame shape: {df.shape}, columns: {df.columns.tolist()}")

    try:
        df = df.copy()
        df['INSTRUMENT'] = df['INSTRUMENT'].apply(lambda x: 'ERC-POC' if 'POC' in str(x) else x)
        logger.debug("Normalized INSTRUMENT column")

        signed_mask = (
            (df['FRAMEWORK'] == programme) &
            (df['EndYear'] == epoch_year) &
            (df['STATUS'] == 'SIGNED_CR') &
            (~df['TTA'].isna() | ~df['TTA\nONGOING'].isna())
        )
        df_signed = df[signed_mask].copy()
        logger.debug(f"Signed DataFrame shape: {df_signed.shape}")

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
        logger.debug(f"Pivot signed shape: {pivot_signed.shape}")

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
        logger.debug(f"Pivot TTA shape: {pivot_tta.shape}")

        logger.info(f"table_signed_function completed for {programme}")
        return pivot_signed, pivot_tta
    except Exception as e:
        logger.error(f"Error in table_signed_function for {programme}: {str(e)}", exc_info=True)
        raise

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
    logger.info(f"Starting generate_amendments_report with cutoff: {cutoff}, alias: {alias}, report: {report}")
    logger.debug(f"Database path: {db_path}, save_to_db: {save_to_db}")

    try:
        db_path = Path(conn.execute("PRAGMA database_list").fetchone()[2])
        logger.debug(f"Database path from connection: {db_path}")

        report_params = load_report_params(report_name=report, db_path=db_path)
        logger.debug(f"Report params: {report_params}")
        amd_report_date = pd.to_datetime(report_params.get("amendments_report_date"))
        logger.debug(f"Amendments report date: {amd_report_date}")

        table_colors = report_params.get('TABLE_COLORS', {})
        BLUE = table_colors.get("BLUE", "#004A99")
        LIGHT_BLUE = table_colors.get("LIGHT_BLUE", "#d6e6f4")
        DARK_BLUE = table_colors.get("DARK_BLUE", "#01244B")
        SUB_TOTAL_BACKGROUND = table_colors.get("subtotal_background_color", "#E6E6FA")
        logger.debug(f"Table colors: {table_colors}")

        df_amd = fetch_latest_table_data(conn, alias, cutoff)
        logger.debug(f"Fetched df_amd shape: {df_amd.shape}, columns: {df_amd.columns.tolist()}")

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
      

        epoch_year = determine_epoch_year(cutoff)
        months_scope = list(range(1, cutoff.month if cutoff.month != 1 else 13))
      

        results = {}
        for programme in ['H2020', 'HORIZON']:
            logger.info(f"Processing programme: {programme}")
            received_statuses = ['SIGNED_CR', 'ASSESSED_CR', 'OPENED_EXT_CR', 'OPENED_INT_CR', 'RECEIVED_CR', 'WITHDRAWN_CR', 'REJECTED_CR']

            amd_received = generate_amendment_pivot(df_amd, programme, received_statuses, 'Counter', 'sum', 'Received', months_scope, epoch_year, 'StartMonth', 'StartYear')
            logger.debug(f"amd_received shape: {amd_received.shape}")
            amd_rejected = generate_amendment_pivot(df_amd, programme, ['REJECTED_CR', 'WITHDRAWN_CR'], 'Counter', 'sum', 'Rejected', months_scope, epoch_year)
            logger.debug(f"amd_rejected shape: {amd_rejected.shape}")
            amd_signed = generate_amendment_pivot(df_amd, programme, ['SIGNED_CR'], 'Counter', 'sum', 'Signed', months_scope, epoch_year)
         
            amd_overview = pd.concat([
                amd_received.assign(TYPE_ROW_NAME='Amendments Received'),
                amd_signed.assign(TYPE_ROW_NAME='Amendments Signed'),
                amd_rejected.assign(TYPE_ROW_NAME='Amendments Rejected or Withdrawn')
            ])
     
            total_rows_indices = amd_overview.reset_index().query('Month == "Total"').index.tolist()
 

            overview_table = (
                GT(amd_overview, rowname_col="Month", groupname_col="TYPE_ROW_NAME")
                .tab_header(title=programme)
                .opt_table_font(font="Arial")
                .opt_table_outline(style="solid", width="2px", color=DARK_BLUE)
                 # Header style
            .tab_style(
                style.text(color=DARK_BLUE, weight="bold", align="center", font='Arial'),
                locations=loc.header()
            )
            
            .tab_stubhead(label="Month")

            # Row group styling
            .tab_style(
                style=[
                    style.text(color=DARK_BLUE, weight="bold", font='Arial', size='small'),
                    style.fill(color=LIGHT_BLUE),
                    style.css(f"border: 1px solid {DARK_BLUE}; line-height:1.2; padding:5px;")
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
                    style.borders(sides="all", color=DARK_BLUE, weight="1px"),
                    style.text(align="center", size='small', font='Arial'),
                    style.css("padding:5px")
                ],
                locations=loc.body()
            )

            # Stub cell styling
            .tab_style(
                style=[
                    style.borders(sides="all", color=DARK_BLUE, weight="1px"),
                    style.text(size='small', font='Arial'),
                    style.css("padding:5px")
                ],
                locations=loc.stub()
            )

            # Consistent and strong external border
            .tab_options(
                table_body_border_bottom_color=DARK_BLUE,
                table_body_border_bottom_width="1px",
                table_border_right_color=DARK_BLUE,
                table_border_right_width="1px",
                table_border_left_color=DARK_BLUE,
                table_border_left_width="1px",
                table_border_top_color=DARK_BLUE,
                table_border_top_width="1px",
                column_labels_border_top_color=DARK_BLUE,
                column_labels_border_top_width="1px"
            )

            # Format all "Total" rows consistently in stub and body
            .tab_style(
                style=[
                    style.fill(color=SUB_TOTAL_BACKGROUND),
                    style.text(color="black", weight="bold", font='Arial'),
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
            
            logger.debug(f"Created overview_table for {programme}")

            pivot_signed, pivot_tta = table_signed_function(df_amd, programme, months_scope, epoch_year)
            logger.debug(f"pivot_signed shape: {pivot_signed.shape}, pivot_tta shape: {pivot_tta.shape}")

            tta_table = (
                GT(pivot_tta)
                .tab_header(title=f"Time to Amend - {programme}")
                .opt_table_font(font="Arial")
                .opt_table_outline(style="solid", width="2px", color=DARK_BLUE)
                 # Header style
            .tab_style(
                style.text(color=DARK_BLUE, weight="bold", align="center", font='Arial', size = 'medium'),
                locations=loc.header()
            )
            
            .tab_stubhead(label="Amd Description")

            # Row group styling
            .tab_style(
                style=[
                    style.text(color=DARK_BLUE, weight="bold", font='Arial', size='small'),
                    style.fill(color=LIGHT_BLUE),
                    style.css(f"border: 1px solid {DARK_BLUE}; line-height:1.2; padding:5px;")
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
                    style.borders(sides="all", color=DARK_BLUE, weight="1px"),
                    style.text(align="center", size='small', font='Arial'),
                    style.css("padding:5px")
                ],
                locations=loc.body()
            )

            # Stub cell styling
            .tab_style(
                style=[
                    style.borders(sides="all", color=DARK_BLUE, weight="1px"),
                    style.text(size='small', font='Arial'),
                    style.css("padding:5px")
                ],
                locations=loc.stub()
            )

            # Consistent and strong external border
            .tab_options(
                table_body_border_bottom_color=DARK_BLUE,
                table_body_border_bottom_width="1px",
                table_border_right_color=DARK_BLUE,
                table_border_right_width="1px",
                table_border_left_color=DARK_BLUE,
                table_border_left_width="1px",
                table_border_top_color=DARK_BLUE,
                table_border_top_width="1px",
                column_labels_border_top_color=DARK_BLUE,
                column_labels_border_top_width="1px"
            )
            # Format all "Total" rows consistently in stub and body

            .tab_style(
                    style=[style.fill(color="#E6E6FA"), style.text(color="black", weight="bold")],
                    locations=loc.body(rows=pivot_tta.index[pivot_tta["Month"] == "Total"].tolist())
                )
            .tab_style(
                style=[style.fill(color="#E6E6FA"), style.text(color="black", weight="bold")],
                locations=loc.stub(rows=pivot_tta.index[pivot_tta["Month"] == "Total"].tolist())
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

            logger.debug(f"Created tta_table for {programme}")

            cases_df = amendment_cases(df_amd, programme, months_scope, epoch_year)
            logger.debug(f"cases_df shape: {cases_df.shape}")

            cases_table = (
                GT(cases_df)
                .tab_header(title=programme)
                .opt_table_font(font="Arial")
                .opt_table_outline(style="solid", width="2px", color=DARK_BLUE)
               # Header style
            .tab_style(
                style.text(color=DARK_BLUE, weight="bold", align="center", font='Arial'),
                locations=loc.header()
            )
            
            .tab_stubhead(label="Amd Description")

            # Row group styling
            .tab_style(
                style=[
                    style.text(color=DARK_BLUE, weight="bold", font='Arial', size='small'),
                    style.fill(color=LIGHT_BLUE),
                    style.css(f"border: 1px solid {DARK_BLUE}; line-height:1.2; padding:5px;")
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
                    style.borders(sides="all", color=DARK_BLUE, weight="1px"),
                    style.text(align="center", size='small', font='Arial'),
                    style.css("padding:5px")
                ],
                locations=loc.body()
            )

            # Stub cell styling
            .tab_style(
                style=[
                    style.borders(sides="all", color=DARK_BLUE, weight="1px"),
                    style.text(size='small', font='Arial'),
                    style.css("padding:5px")
                ],
                locations=loc.stub()
            )

            # Consistent and strong external border
            .tab_options(
                table_body_border_bottom_color=DARK_BLUE,
                table_body_border_bottom_width="1px",
                table_border_right_color=DARK_BLUE,
                table_border_right_width="1px",
                table_border_left_color=DARK_BLUE,
                table_border_left_width="1px",
                table_border_top_color=DARK_BLUE,
                table_border_top_width="1px",
                column_labels_border_top_color=DARK_BLUE,
                column_labels_border_top_width="1px"
            )

            # Format all "Total" rows consistently in stub and body

            .tab_style(
                    style=[style.fill(color="#E6E6FA"), style.text(color="black", weight="bold")],
                    locations=loc.body(rows=cases_df.index[cases_df["DESCRIPTION"] == "Total"].tolist())
                )
            .tab_style(
                style=[style.fill(color="#E6E6FA"), style.text(color="black", weight="bold")],
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
            
            logger.debug(f"Created cases_table for {programme}")

            rolling_tta_df = rolling_tta(df_amd, programme, months_scope, epoch_year)
            logger.debug(f"rolling_tta_df shape: {rolling_tta_df.shape}")

            tta_chart_img = chart_machine_tta(pivot_tta, programme, rolling_tta_df)
            logger.debug(f"Generated tta_chart_img for {programme}")

            if save_to_db:
                for var_name, value, table in [
                    (f'{programme}_overview', amd_overview, overview_table),
                    (f'{programme}_cases', cases_df, cases_table),
                    (f'{programme}_tta', pivot_tta, tta_table),
                    (f'{programme}_tta_chart', pivot_tta, tta_chart_img),
                ]:
                    logger.debug(f"Saving {var_name} to database")
                    insert_variable(
                        report=report, module="AmendmentModule", var=var_name,
                        value=value.to_dict() if isinstance(value, pd.DataFrame) else value,
                        db_path=db_path, anchor=var_name, gt_table=table
                    )
                    logger.debug(f"Saved {var_name} to database")

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



