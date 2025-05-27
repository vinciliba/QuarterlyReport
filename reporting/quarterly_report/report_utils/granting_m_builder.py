from __future__ import annotations

import logging
import sqlite3
import datetime
from pathlib import Path
from typing import List, Dict, Tuple, Any, Union

import numpy as np
import pandas as pd
from great_tables import GT, loc, style, html, md

# our project
from ingestion.db_utils import (
    init_db,
    fetch_latest_table_data,
    get_alias_last_load,
    get_variable_status,
    insert_variable,
)
from reporting.quarterly_report.utils import RenderContext, BaseModule
from reporting.quarterly_report.report_utils.granting_utils import enrich_grants, _ensure_timedelta_cols, _coerce_date_columns
from ingestion.db_utils import load_report_params
from reporting.quarterly_report.utils import Database, RenderContext
import traceback
import functools

def debug_wrapper(func):
    """Wrapper to add detailed error logging to functions"""
    @functools.wraps(func)
    def wrapper(*args, **kwargs):
        try:
            log.debug(f"Starting {func.__name__}")
            result = func(*args, **kwargs)
            log.debug(f"Completed {func.__name__} successfully")
            return result
        except Exception as e:
            log.error(f"Error in {func.__name__}: {str(e)}")
            log.error(f"Error type: {type(e).__name__}")
            log.error(f"Traceback:\n{traceback.format_exc()}")
            raise
    return wrapper

# Constants
CALL_OVERVIEW_ALIAS = "call_overview"
BUDGET_FOLLOWUP_ALIAS = "budget_follow_up_report"
ETHICS_ALIAS = "ethics_requirements_and_issues"
EXCLUDE_TOPICS = [
    "ERC-2023-SJI-1", "ERC-2023-SJI",
    "ERC-2024-PERA",
    "HORIZON-ERC-2022-VICECHAIRS-IBA",
    "HORIZON-ERC-2023-VICECHAIRS-IBA",
    "ERC-2025-NCPS-IBA",
    "ERC"
]
MONTHS_ORDER = list(pd.date_range("2000-01-01", periods=12, freq="ME").strftime("%B"))

PROJECT_STATUS = ['SIGNED', 'TERMINATED', 'SUSPENDED', 'CLOSED']
CALLS_TYPES_LIST = ['STG', 'ADG', 'POC', 'COG', 'SYG', 'StG', 'CoG', 'AdG', 'SyG', 'PoC']
PROGRAMMES_LIST = ['HORIZONEU_21_27', 'H2020_14_20']
FUND_SOURCES = ['VOBU', 'EARN/N', 'EFTA', 'IAR2/2']

outline_b = '2px'

def _df_to_gt(df: pd.DataFrame, title: str) -> GT:
    return (
        GT(df.reset_index(drop=True))
        .tab_header(title)
        .opt_table_font(font="Arial")
        .tab_style(style=[style.text(weight="bold")], locations=loc.column_labels())
    )

# Setup logging
logging.basicConfig(level=logging.DEBUG)
log = logging.getLogger("Granting")

@debug_wrapper
def get_quarter_dates(cutoff: pd.Timestamp, earliest_date: pd.Timestamp) -> Tuple[pd.Timestamp, pd.Timestamp]:
    """
    Determine the start date (earliest GA Signature - Commission date) and the end of the quarter
    containing the last full month before the cutoff.
    """
    first_day_of_cutoff = cutoff.replace(day=1)
    last_full_month = first_day_of_cutoff - pd.offsets.MonthBegin()
    last_full_month_number = last_full_month.month

    period_start = earliest_date

    if last_full_month_number in [1, 2, 3]:  # Q1
        period_end = pd.Timestamp(year=cutoff.year, month=3, day=31)
    elif last_full_month_number in [4, 5, 6]:  # Q2
        period_end = pd.Timestamp(year=cutoff.year, month=6, day=30)
    elif last_full_month_number in [7, 8, 9]:  # Q3
        period_end = pd.Timestamp(year=cutoff.year, month=9, day=30)
    else:  # Q4: [10, 11, 12]
        period_end = pd.Timestamp(year=cutoff.year, month=12, day=31)

    return period_start, period_end

@debug_wrapper
def compute_quantiles(call_list: List[str],
                      df_filtered: pd.DataFrame,
                      cutoff: pd.Timestamp,
                      earliest_date: pd.Timestamp
                     ) -> List[pd.DataFrame]:
    """
    Build two data-frames — one for TTS, one for TTG — with 25 % and 50 % percentiles
    for each call in *call_list*.

    Returns
    -------
    [df_tts, df_ttg]
    """
    # log.debug(f"COMPUTED QUANTILES STEP 1")
    df_filtered = df_filtered[df_filtered['Call'].isin(call_list)].copy()
    df_filtered['GA Signature - Commission'] = pd.to_datetime(
        df_filtered['GA Signature - Commission'], errors='coerce')

    period_start, period_end = get_quarter_dates(cutoff, earliest_date)

    signed_statuses = ['SIGNED', 'SUSPENDED', 'CLOSED', 'TERMINATED']

    df_filtered['ACTIVE'] = (df_filtered['Project Status'] == 'UNDER_PREPARATION').astype(int)
    df_filtered['SIGNED'] = (
        (df_filtered['Project Status'].isin(signed_statuses)) &
        (df_filtered['GA Signature - Commission'].notna()) &
        (df_filtered['GA Signature - Commission'] >= period_start) &
        (df_filtered['GA Signature - Commission'] <= period_end)
    ).astype(int)

    df_filtered.loc[df_filtered['SIGNED'] == 0, 'ACTIVE'] = 1

    results = {
        'Call': [],
        'Total number of grants excluding rejected': [],
        'Total number of signed grants': [],          # ← consistent name
        'TTS_25': [], 'TTS_50': [],
        'TTG_25': [], 'TTG_50': []
    }
    
    # log.debug(f"COMPUTED QUANTILES STEP 2")
    for call in call_list:
        kpi = df_filtered[df_filtered['Call'] == call].copy()

        if kpi.empty:
            for key in ['Call',
                        'Total number of grants excluding rejected',
                        'Total number of signed grants',
                        'TTS_25', 'TTS_50', 'TTG_25', 'TTG_50']:
                results[key].append(np.nan if key != 'Call' else call)
            continue

        kpi['Class'] = np.where(kpi['TTG_timedelta'] != pd.Timedelta(0), 'A', 'B')
        kpi = kpi.sort_values(['Class', 'TTG_timedelta'])

        active_n = kpi['ACTIVE'].sum()
        signed_n = kpi['SIGNED'].sum()

        def _percentile(series: pd.Series, pct: float):
            if len(series) == 0:
                return np.nan
            idx = int(np.floor(len(series) * pct))
            return series.iloc[idx]

        tts_25 = _percentile(kpi['TTS_timedelta'], 0.25) / pd.Timedelta('1D')
        tts_50 = _percentile(kpi['TTS_timedelta'], 0.50) / pd.Timedelta('1D')
        ttg_25 = _percentile(kpi['TTG_timedelta'], 0.25) / pd.Timedelta('1D')
        ttg_50 = _percentile(kpi['TTG_timedelta'], 0.50) / pd.Timedelta('1D')

        results['Call'].append(call)
        results['Total number of grants excluding rejected'].append(active_n + signed_n)
        results['Total number of signed grants'].append(signed_n)
        results['TTS_25'].append(tts_25)
        results['TTS_50'].append(tts_50)
        results['TTG_25'].append(ttg_25)
        results['TTG_50'].append(ttg_50)

        # log.debug(f"COMPUTED QUANTILES STEP 3: {results}")

    df_tts = pd.DataFrame({
        'Call': results['Call'],
        'Total number of grants excluding rejected':
            results['Total number of grants excluding rejected'],
        'Total Number of Signed Grants':
            results['Total number of signed grants'],
        'First 25% (days)': results['TTS_25'],
        'First 50% (days)': results['TTS_50']
    })

    df_ttg = pd.DataFrame({
        'Call': results['Call'],
        'Total number of grants excluding rejected':
            results['Total number of grants excluding rejected'],
        'Total Number of Signed Grants':
            results['Total number of signed grants'],
        'First 25% (days)': results['TTG_25'],
        'First 50% (days)': results['TTG_50']
    })

    for df in (df_tts, df_ttg):
        for col in ['Total number of grants excluding rejected',
                    'Total Number of Signed Grants']:
            df[col] = df[col].astype('Int64')

    return [df_tts, df_ttg]

@debug_wrapper
def transpose_table_quantiles(df: pd.DataFrame) -> pd.DataFrame:
    """
    Transpose table for quantile data (df_tts, df_ttg).
    """
    log.debug(f"Input to transpose_table_quantiles: shape {df.shape}, columns {df.columns.tolist()}")
    log.debug(f"Input data:\n{df.head()}")
    df_pivot = df.set_index('Call')
    df_transposed = pd.DataFrame({
        'Total number of grants excluding rejected': df_pivot['Total number of grants excluding rejected'],
        'Total number of signed grants': df_pivot['Total Number of Signed Grants'],  # Updated to match expected name
        'First 25%': df_pivot['First 25% (days)'],
        'First 50%': df_pivot['First 50% (days)']
    }).T
    log.debug(f"Transposed DataFrame columns: {df_transposed.columns.tolist()}")
    return df_transposed

@debug_wrapper
def transpose_table_metrics(df: pd.DataFrame, metric: str) -> pd.DataFrame:
    """
    Transposes selected metrics for TTG or TTS based on 'Call'.

    Parameters:
        df (pd.DataFrame): Input dataframe with columns including 'Call', 
                           'Number of Signed Grants', '<metric>', 
                           '<metric> Target', and 'Completion Rate'.
        metric (str): Either 'TTG' or 'TTS'.

    Returns:
        pd.DataFrame: Transposed dataframe with renamed rows.
    """
    if metric not in ['TTG', 'TTS']:
        raise ValueError("Metric must be 'TTG' or 'TTS'")

    expected_cols = {'Call', 'Number of Signed Grants', metric, f'{metric} Target', 'Completion Rate'}
    log.debug(f"Input to transpose_table_metrics (metric={metric}): shape {df.shape}, columns {df.columns.tolist()}")
    log.debug(f"Input data:\n{df.head()}")

    missing = expected_cols - set(df.columns)
    if missing:
        raise ValueError(f"Missing columns in input DataFrame: {missing}")

    # Handle NaN values by filling with defaults
    df = df.copy()
    df['Number of Signed Grants'] = df['Number of Signed Grants'].fillna(0).astype('Int64')
    df['Completion Rate'] = df['Completion Rate'].fillna(0.0)
    df[metric] = df[metric].fillna(0.0)
    df[f'{metric} Target'] = df[f'{metric} Target'].fillna(0.0)

    df_pivot = df.set_index('Call')
    df_transposed = pd.DataFrame({
        'Total number of signed grants': df_pivot['Number of Signed Grants'],  # Updated to match expected name
        f'Average {metric}': df_pivot[metric],
        'Target': df_pivot[f'{metric} Target'],
        'Completion Rate': df_pivot['Completion Rate']
    }).T

    log.debug(f"Transposed DataFrame columns: {df_transposed.columns.tolist()}")
    return df_transposed

@debug_wrapper
def clean_dataframe(df, standardize_columns=True):
    """
    Clean a dataframe by handling common data quality issues
    
    Args:
        df: pandas DataFrame to clean
        standardize_columns: if True, standardize column names
    
    Returns:
        Cleaned DataFrame
    """
    # Create a copy to avoid modifying the original
    df_clean = df.copy()
    
    # 1. Strip whitespace from column names
    df_clean.columns = df_clean.columns.str.strip()
    
    if standardize_columns:
        # 2. Optional: Convert to uppercase and replace spaces with underscores
        df_clean.columns = df_clean.columns.str.upper().str.replace(' ', '_')
        # Remove multiple underscores
        df_clean.columns = df_clean.columns.str.replace('_+', '_', regex=True)
    
    # 3. Strip whitespace from string values in all object columns
    string_columns = df_clean.select_dtypes(include=['object']).columns
    for col in string_columns:
        df_clean[col] = df_clean[col].apply(lambda x: x.strip() if isinstance(x, str) else x)
    
    return df_clean

@debug_wrapper
def process_granting_data(
    conn: sqlite3.Connection,
    cutoff: pd.Timestamp,
    report: str,
    db_path: Path,
    report_params: Dict,
    save_to_db: bool = True,
    export_dir: Path = Path("exports")
) -> Dict[str, Any]:
    """
    Process granting data to compute KPIs, state-of-play, and generate tables.
    """
    log.debug("Starting process_granting_data function ")
    calls_list = report_params.get("calls_list", [])
    heu_calls_list = report_params.get("HEU_Calls", [])
    table_colors = report_params.get("TABLE_COLORS", {})
    BLUE = table_colors.get("BLUE", "#0000FF")

    # Load data
    call_overview = fetch_latest_table_data(conn, CALL_OVERVIEW_ALIAS, cutoff)
    budget_follow = fetch_latest_table_data(conn, BUDGET_FOLLOWUP_ALIAS, cutoff)
    ethics_df = fetch_latest_table_data(conn, ETHICS_ALIAS, cutoff)

    for df, alias in [(call_overview, CALL_OVERVIEW_ALIAS),
                      (budget_follow, BUDGET_FOLLOWUP_ALIAS),
                      (ethics_df, ETHICS_ALIAS)]:
        if df.empty:
            raise RuntimeError(f"No rows found for alias '{alias}'. Upload data first.")
        log.debug(f"Loaded {alias} with shape {df.shape} and columns {df.columns.tolist()}")
    
    # Clean the budget_follow dataframe
    budget_follow = clean_dataframe(budget_follow, standardize_columns=False)
    
    # Debug: Print column names to see what we have
    log.debug(f"Budget follow columns after cleaning: {budget_follow.columns.tolist()}")
    
    # Try to find the INVITED column with various names
    invited_column = None
    possible_names = ['INVITED', 'invited', 'Invited']
    
    for col_name in possible_names:
        if col_name in budget_follow.columns:
            invited_column = col_name
            break
    
    # If not found in simple variations, check for columns containing 'INVITED'
    if invited_column is None:
        for col in budget_follow.columns:
            if 'INVITED' in col.upper():
                invited_column = col
                log.debug(f"Found INVITED column as: '{invited_column}'")
                break
    
    if invited_column is None:
        # List all columns for debugging
        log.error(f"Could not find INVITED column. Available columns: {budget_follow.columns.tolist()}")
        raise KeyError("Could not find INVITED column in budget_follow DataFrame")
    
    # Use the found column name
    try:
        budget_follow = budget_follow.loc[budget_follow[invited_column] == 1]
    except Exception as e:
        log.error(f"Error filtering by INVITED column '{invited_column}': {str(e)}")
        log.error(f"Column data type: {budget_follow[invited_column].dtype}")
        log.error(f"Unique values in column: {budget_follow[invited_column].unique()}")
        raise

    # Continue with the rest of the function...
    df_grants = (
        call_overview
        .merge(budget_follow, left_on="Grant Number", right_on="Project Number", how="inner")
        .reset_index()
        .drop_duplicates(subset="Grant Number", keep="last")
        .set_index("Grant Number")
        .sort_index()
    )

    df_grants = df_grants.merge(ethics_df, left_on="Grant Number", right_on="PROPOSAL\nNUMBER", how="inner")
    _coerce_date_columns(df_grants)

    # Rest of the function continues as before...
    for status in PROJECT_STATUS:
        df_grants['GA Signature - Commission'] = np.where(
            (df_grants['GA Signature - Commission'].isnull()) &
            (df_grants['Project Status'] == status) &
            (df_grants['Commitment AO visa'].isnull() == False),
            df_grants['Commitment AO visa'],
            df_grants['GA Signature - Commission']
        )

    _ensure_timedelta_cols(df_grants)

    # HEU metrics
    df_heu_total = df_grants.loc[
        (df_grants['Call'].isin(heu_calls_list)) &
        (df_grants['Project Status'].isin(PROJECT_STATUS))
    ]
    HEU_TTG_TOTAL = round(df_heu_total['TTG_timedelta'].mean().total_seconds() / 86400, 2) if not df_heu_total.empty else 0

    df_grants = df_grants[~df_grants["Topic"].isin(EXCLUDE_TOPICS)]
    df_grants = df_grants[df_grants["Call"].isin(calls_list)]

    if df_grants.empty:
        empty_df = pd.DataFrame()
        return {
            "df_grants": empty_df,
            "final_df_with_targets": empty_df,
            "df_tts": empty_df,
            "df_ttg": empty_df,
            "q_tts": empty_df,
            "q_ttg": empty_df,
            "df_TTS": empty_df,
            "df_TTG": empty_df,
            "final_df_tts_overview": empty_df,
            "tbl_ttg": None,
            "tbl_tts": None,
            "tbl_q_ttg": None,
            "tbl_q_tts": None,
            "tbl_grants_tts_overview": None,
            "HEU_TTG_TOTAL": 0,
            "HEU_TTG_C_Y": 0
        }


    earliest_date = df_grants['GA Signature - Commission'].min()
    if pd.isna(earliest_date):
        earliest_date = pd.Timestamp(cutoff.year, 1, 1)
    period_start, period_end = get_quarter_dates(cutoff, earliest_date)

    in_scope = (
        (df_grants['Project Status'].isin(PROJECT_STATUS)) &
        (df_grants['GA Signature - Commission'].notna()) &
        (df_grants['GA Signature - Commission'] >= period_start) &
        (df_grants['GA Signature - Commission'] <= period_end)
    )
    signed = df_grants.loc[in_scope].copy()
    HEU_TTG_C_Y = round(signed['TTG_timedelta'].mean().total_seconds() / 86400, 2) if not signed.empty else 0

    dfkpi = signed.loc[:, ['Call', 'TTG_timedelta', 'TTS_timedelta', 'TTI_timedelta']]
    if dfkpi.empty:
        log.warning("dfkpi is empty. No signed grants in the reporting period.")
        dfkpi = pd.DataFrame(columns=['Call', 'TTG', 'TTS', 'TTI'])
    else:
        dfkpi.set_index(["Call"], inplace=True, drop=True)
        dfkpi['TTG'] = dfkpi['TTG_timedelta'] / pd.to_timedelta(1, unit='D')
        dfkpi['TTS'] = dfkpi['TTS_timedelta'] / pd.to_timedelta(1, unit='D')
        dfkpi['TTI'] = dfkpi['TTI_timedelta'] / pd.to_timedelta(1, unit='D')
        dfkpi.drop(['TTG_timedelta', 'TTS_timedelta', 'TTI_timedelta'], axis=1, inplace=True)
    df_kpi_prov = dfkpi.groupby(['Call']).mean().reset_index()

    df_g_running = df_grants.loc[df_grants['Project Status'] != 'REJECTED']
    df_g_running = df_g_running[['Call']].assign(Counter=1).groupby('Call').sum().reset_index()
    df_g_running.columns = ['Call', 'Total number of grants excluding rejected']

    df_signed_temp = signed[['Call']].assign(Counter=1)
    df_signed = df_signed_temp.groupby('Call')['Counter'].sum().reset_index()
    df_signed.columns = ['Call', 'Number of Signed Grants']

    merged_df = pd.merge(df_g_running, df_signed, on=["Call"], how="outer").fillna(0)
    merged_df['Completion Rate'] = merged_df['Number of Signed Grants'] / merged_df['Total number of grants excluding rejected']
    merged_df = pd.merge(merged_df, df_kpi_prov, on=["Call"], how="outer")

    TTS_targets = report_params.get("TTS_Targets", {})
    df_TTS = pd.DataFrame(list(TTS_targets.items()), columns=["Call", "TTS Target"])
    TTG_targets = report_params.get("TTG_Targets", {})
    df_TTG = pd.DataFrame(list(TTG_targets.items()), columns=["Call", "TTG Target"])
    df_Targets = pd.merge(df_TTS, df_TTG, on="Call", how="outer").query('Call in @calls_list')

    final_df_with_targets = pd.merge(merged_df, df_Targets, on=["Call"], how="outer")
    log.debug(f"final_df_with_targets: shape {final_df_with_targets.shape}, columns {final_df_with_targets.columns.tolist()}")

    df_filtered = df_grants.loc[df_grants['Project Status'] != 'REJECTED']
    [df_tts, df_ttg] = compute_quantiles(calls_list, df_filtered, cutoff, earliest_date)

    # Transpose quantiles
    q_tts = transpose_table_quantiles(df_tts)
    q_ttg = transpose_table_quantiles(df_ttg)

    # Transpose metrics
    df_TTG = transpose_table_metrics(final_df_with_targets, 'TTG')
    df_TTS = transpose_table_metrics(final_df_with_targets, 'TTS')

    final_df_overview = pd.merge(final_df_with_targets, df_tts, on='Call', how="outer")
    final_df_tts_overview = final_df_overview[['Call', 'First 25% (days)', 'First 50% (days)', 'TTS', 'Completion Rate']].rename(columns={"Completion Rate": "Completion_Rate"})

    df_TTG.reset_index(inplace=True)
    df_TTS.reset_index(inplace=True)
    q_ttg.reset_index(inplace=True)
    q_tts.reset_index(inplace=True)
    final_df_tts_overview = final_df_tts_overview.rename(columns={"Completion Rate": "Completion_Rate"})
    
    # Define colors if table_colors is not provided
    BLUE = table_colors.get("BLUE", "#004A99") if table_colors else "#004A99"
    LIGHT_BLUE = table_colors.get("LIGHT_BLUE", "#d6e6f4") if table_colors else "#d6e6f4"
    # GRID_CLR = table_colors.get("GRID_CLR", "#004A99") if table_colors else "#004A99"
    DARK_BLUE = table_colors.get("DARK_BLUE", "#01244B") if table_colors else "#01244B"
    # DARK_GREY = table_colors.get("DARK_GREY", "#242425") if table_colors else "#242425"
    # Build GreatTables object

    # Build GreatTables object
    try:
        tbl_ttg = (
            GT(df_TTG, rowname_col="index")
            .tab_header(
                    title=html(
                        f"<strong style='color: {BLUE};'>TIME TO GRANT</strong>  "
                        f"<span style='color: {BLUE}; font-style: italic; font-size: smaller;'>(Main list & Reserve list)</span>"
                    )
            )
            .tab_stubhead(
                    label=html(f"<span style='color: white ; font-size: large; align-text: center; margin-left: 5px; margin-bottom: 80px;' >Call</span>")
                    )
            # GENERAL FORMATTING
            # Table Outline
            # .opt_table_outline(style = "solid", width = outline_b, color =  DARK_BLUE) 
            # Arial font
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
            # Header and stub styling
            .tab_style(
                style=[
                    style.fill(color=BLUE),
                    style.text(color="white", weight="bold", align='center'),
                    style.css("text-align: center; vertical-align: middle; max-width:200px; line-height:1.2; font-size: smaller;")
                ],
                locations=loc.column_labels()
            )

             .tab_style(
                style=[
                    style.fill(color=BLUE),
                    style.text( align='left', size ='small' ),
                    style.css("text-align: center; vertical-align: middle; max-width:200px; line-height:1.2; font-size: smaller;")
                ],
                locations=loc.stubhead()
            )
            .tab_style(
                style=[
                    style.fill(color=LIGHT_BLUE),
                    style.text( align='center'),
                    style.css("text-align: center; vertical-align: middle; max-width:200px; line-height:1.2; font-size: smaller;")
                ],
                locations=loc.stub()
            )
      
            # BODY
            .fmt_percent(
                rows=["Completion Rate"],  # or use `where` with a condition
                decimals=1
            )
            .fmt_number(
                rows=["Average TTG"],
                decimals=1,
                accounting=False
            )
            # Source notes
            .tab_source_note("Source: Compass")
            .tab_source_note("Reports: Budgetary Execution Details - Call Overview Report")
            )
    except Exception as e:
            logging.error(f"Error building GreatTables object: {str(e)}")
                
            
    try:
        tbl_tts = (
            GT(
                df_TTS,
                rowname_col="index"
            )
            .tab_header(
                    title=html(
                        f"<strong style='color: {BLUE};'>TIME TO SIGN</strong>  "
                        f"<span style='color: {BLUE}; font-style: italic; font-size: smaller;'>(Main list & Reserve list)</span>"
                    )
                )
            .tab_stubhead(
                    label=html(f"<span style='color: white ; font-size: large; align-text: center; margin-left: 5px; margin-bottom: 80px;' >Call</span>")
                    )
            # GENERAL FORMATTING
            # Table Outline
            # .opt_table_outline(style = "solid", width = outline_b, color =  DARK_BLUE) 
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
            # Arial font
            .opt_table_font(font="Arial")
            # Header and stub styling
            .tab_style(
                style=[
                    style.fill(color=BLUE),
                    style.text(color="white", weight="bold", align='center'),
                    style.css("text-align: center; vertical-align: middle; max-width:200px; line-height:1.2; font-size: smaller;")
                ],
                locations=loc.column_labels()
            )
             .tab_style(
                style=[
                    style.fill(color=BLUE),
                    style.text( align='left', size ='small' ),
                    style.css("text-align: center; vertical-align: middle; max-width:200px; line-height:1.2; font-size: smaller;")
                ],
                locations=loc.stubhead()
            )
            .tab_style(
                style=[
                    style.fill(color=LIGHT_BLUE),
                    style.text(align='center'),
                    style.css("text-align: center; vertical-align: middle; max-width:200px; line-height:1.2; font-size: smaller;")
                ],
                locations=loc.stub()
            )
            # BODY
            .fmt_percent(
                rows=["Completion Rate"],  # or use `where` with a condition
                decimals=1
            )
            .fmt_number(
                rows=["Average TTS"],
                decimals=1,
                accounting=False
            )
            # Source notes
            .tab_source_note("Source: Compass")
            .tab_source_note("Reports: Budgetary Execution Details - Call Overview Report")
        )
    except Exception as e:
                logging.error(f"Error building GreatTables object: {str(e)}")
                # Return the aggregated DataFrame without styling if table creation fails
           
    # Build GreatTables object
    try:
        tbl_q_ttg = (
            GT(q_ttg, rowname_col="index")
            .tab_header(
                    title=html(
                        f"<strong style='color: {BLUE};'>TIME TO GRANT - Quantiles </strong>  "
                        #  f"<span style='color: {BLUE}; font-style: italic; font-size: smaller;'>With Quantiles</span>"
                        f"<span style='color: {BLUE}; font-style: italic; font-size: smaller;'>(Main list & Reserve list)</span>"
                    )
                )
            .tab_stubhead(
                    label=html(f"<span style='color: white ; font-size: large; align-text: center; margin-left: 5px; margin-bottom: 80px;' >Call</span>")
                    )
            # GENERAL FORMATTING
            # Table Outline
            # .opt_table_outline(style = "solid", width = outline_b, color =  DARK_BLUE) 
            # Arial font
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
            # Header and stub styling
            .tab_style(
                style=[
                    # style.borders(weight="1px", color=DARK_BLUE),
                    style.fill(color=BLUE),
                    style.text(color="white", weight="bold", align='center'),
                    style.css("text-align: center; vertical-align: middle; max-width:200px; line-height:1.2; font-size: smaller;")
                ],
                locations=loc.column_labels()
            )

             .tab_style(
                style=[
                    style.fill(color=BLUE),
                    style.text( align='left', size ='small' ),
                    style.css("text-align: center; vertical-align: middle; max-width:200px; line-height:1.2; font-size: smaller;")
                ],
                locations=loc.stubhead()
            )
            .tab_style(
                style=[
                    style.fill(color=LIGHT_BLUE),
                    style.text(align='center'),
                    style.css("text-align: center; vertical-align: middle; max-width:200px; line-height:1.2; font-size: smaller;")
                ],
                locations=loc.stub()
            )
          
            # BODY
            .fmt_percent(
                rows=["Completion Rate"],  # or use `where` with a condition
                decimals=1
            )
            # Source notes
            .tab_source_note("Source: Compass")
            .tab_source_note("Reports: Budgetary Execution Details - Call Overview Report")
            )
    except Exception as e:
                logging.error(f"Error building GreatTables object: {str(e)}")
                # Return the aggregated DataFrame without styling if table creation fails
           

    # Build GreatTables object
    try:
        tbl_q_tts = (
            GT(
            q_tts,
                rowname_col="index"
            )
            .tab_header(
                    title=html(
                        f"<strong style='color: {BLUE};'>TIME TO SIGN - Quantiles </strong>  "
                        #  f"<span style='color: {BLUE}; font-style: italic; font-size: smaller;'>With Quantiles</span>"
                        f"<span style='color: {BLUE}; font-style: italic; font-size: smaller;'>(Main list & Reserve list)</span>"
                    )
            )
            .tab_stubhead(
                    label=html(f"<span style='color: white ; font-size: large; align-text: center; margin-left: 5px; margin-bottom: 80px;' >Call</span>")
            )
            # GENERAL FORMATTING
            # Table Outline
            # .opt_table_outline(style = "solid", width = outline_b, color =  DARK_BLUE) 
            # Arial font
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

            # Header and stub styling
            .tab_style(
                style=[
                    style.fill(color=BLUE),
                    style.text(color="white", weight="bold", align='center'),
                    style.css("text-align: center; vertical-align: middle; max-width:200px; line-height:1.2; font-size: smaller;")
                ],
                locations=loc.column_labels()
            )
            .tab_style(
                style=[
                    style.fill(color=BLUE),
                    style.text( align='left', size ='small' ),
                    style.css("text-align: center; vertical-align: middle; max-width:200px; line-height:1.2; font-size: smaller;")
                ],
                locations=loc.stubhead()
            )
            .tab_style(
                style=[
                    style.fill(color=LIGHT_BLUE),
                    style.text( align='center'),
                    style.css("text-align: center; vertical-align: middle; max-width:200px; line-height:1.2; font-size: smaller;")
                ],
                locations=loc.stub()
            )
            # BODY
            .fmt_percent(
                rows=["Completion Rate"],  # or use `where` with a condition
                decimals=1
            )
            # Source notes
            .tab_source_note("Source: Compass")
            .tab_source_note("Reports: Budgetary Execution Details - Call Overview Report")
            )
    except Exception as e:
                logging.error(f"Error building GreatTables object: {str(e)}")
                # Return the aggregated DataFrame without styling if table creation fails

    # Build GreatTables object
    try:
        tbl_grants_tts_overview = (
            GT(
            final_df_tts_overview,
                rowname_col="Call"
            )
            .tab_header(
                title=html(
                f"<strong style=' font-size: medium; text-align: left; display: block;'>Time-to-Sign HEU</strong>"
                    )
            )
            .tab_stubhead(
                label=html(
                    f"<span style=' font-size: smaller; text-align: left; display: block; white-space: normal; max-width: 400px;'>"
                    "Time to Sign: From the information letter<br>sent to the signature of the Grant Agreement"
                    "</span>"
                )
            )
            # GENERAL FORMATTING
            # Table Outline
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
                .tab_style(
                    style=style.borders(sides="all", color="#ffffff", weight="2px"),
                    locations=loc.stubhead()
                )
            # Arial font
            .opt_table_font(font="Arial")
            # Header and stub styling
            .tab_style(
                style=[
                    # style.borders(weight="1px", color=DARK_BLUE),
                    style.fill(color=LIGHT_BLUE),
                    style.text( weight="bold", align='center', size = 'medium'),
                    style.css("text-align: center; vertical-align: middle; max-width:200px; line-height:1.2; font-size: smaller;")
                ],
                locations=loc.column_labels()
            )
              .tab_style(
                style=[
                    style.fill(color=LIGHT_BLUE),
                ],
                locations=loc.stubhead()
            )

            .cols_label(
            Completion_Rate=html(
                "Completion Rate <span style='font-size: smaller;'> (8)</span> <br> --------------------------</br> "
                "<span style='font-size: smaller;'>(Main + Reserve lists)</span>"
                )   
            )
            .tab_style(
            style.text(size = 'small'),
            
            loc.body()
        )
            # BODY
            .fmt_percent(
                columns=["Completion_Rate"],  # or use `where` with a condition
                decimals=1
            )
            .fmt_number(
                columns=["TTS"],
                decimals=1,
                accounting=False
            )
        )
    except Exception as e:
        logging.error(f"Error building GreatTables object: {str(e)}")
    # Store TIME TO GRANT table and data
    try:
        insert_variable(
            report=report,
            module="GrantingModule",
            var="table_ttg",
            value=df_TTG.to_dict(orient="records"),
            db_path=db_path,
            anchor="table_ttg",
            gt_table=tbl_ttg,
        )
        log.debug("Stored TTG table and data")
    except Exception as e:
        logging.error(f"Error storing table for table_ttg: {str(e)}")

    # Store TIME TO SIGN table and data
    if df_TTS is not None and not df_TTS.empty:
        insert_variable(
            report=report,
            module="GrantingModule",
            var="table_tts",
            value=df_TTS.to_dict(orient="records"),
            db_path=db_path,
            anchor="table_tts",
            gt_table=tbl_tts,
        )
        # log.debug("Stored TTS table and data")
    else:
        log.debug("No data to store for TTS table (empty DataFrame).")

    # Store TIME TO GRANT Quantiles table and data
    try:
        insert_variable(
            report=report,
            module="GrantingModule",
            var="table_q_ttg",
            value=q_ttg.to_dict(orient="records"),
            db_path=db_path,
            anchor="table_q_ttg",
            gt_table=tbl_q_ttg,
        )
        # log.debug("Stored TTG Quantiles table and data")
    except Exception as e:
        logging.error(f"Error storing table for table_q_ttg: {str(e)}")

    # Store TIME TO SIGN Quantiles table and data
    try:
        insert_variable(
            report=report,
            module="GrantingModule",
            var="table_q_tts",
            value=q_tts.to_dict(orient="records"),
            db_path=db_path,
            anchor="table_q_tts",
            gt_table=tbl_q_tts,
        )
        log.debug("Stored TTS Quantiles table and data")
    except Exception as e:
        logging.error(f"Error storing table for table_q_tts: {str(e)}")

    # Store Time-to-Sign HEU overview table and data
    try:
        insert_variable(
            report=report,
            module="GrantingModule",
            var="table_grants_tts_overview",
            value=final_df_tts_overview.to_dict(orient="records"),
            db_path=db_path,
            anchor="table_grants_tts_overview",
            gt_table=tbl_grants_tts_overview,
        )
        log.debug("Stored Time-to-Sign HEU overview table and data")
    except Exception as e:
        logging.error(f"Error storing table for table_grants_tts_overview: {str(e)}")
    
    try:
        insert_variable(
            report=report,
            module="GrantingModule",
            var="heu_ttg_total",
            value=HEU_TTG_TOTAL,
            db_path=db_path,
            anchor="HEU_TTG_TOTAL",
            gt_table=None,
        )
        log.debug("Stored Time-to-Sign HEU overview table and data")
    except Exception as e:
        logging.error(f"Error storing table for HEU_TTG_TOTAL: {str(e)}")

    try:
        insert_variable(
            report=report,
            module="GrantingModule",
            var="heu_ttg_current_year",
            value=HEU_TTG_C_Y,
            db_path=db_path,
            anchor="HEU_TTG_C_Y",
            gt_table=None,
        )
        log.debug("Stored Time-to-Sign HEU overview table and data")
    except Exception as e:
        logging.error(f"Error storing table for HEU_TTG_C_Y: {str(e)}")

    log.debug("Complete processing process_granting_data function ")
    return {
        "df_grants": df_grants,
        "final_df_with_targets": final_df_with_targets,
        "df_tts": df_tts,
        "df_ttg": df_ttg,
        "q_tts": q_tts,
        "q_ttg": q_ttg,
        "df_TTS": df_TTS,
        "df_TTG": df_TTG,
        "final_df_tts_overview": final_df_tts_overview,
        "tbl_ttg": tbl_ttg,
        "tbl_tts": tbl_tts,
        "tbl_q_ttg": tbl_q_ttg,
        "tbl_q_tts": tbl_q_tts,
        "tbl_grants_tts_overview": tbl_grants_tts_overview,
        "HEU_TTG_TOTAL": HEU_TTG_TOTAL,
        "HEU_TTG_C_Y": HEU_TTG_C_Y
        }

@debug_wrapper
def build_po_exceeding_FDI_tb_3c(df_summa: pd.DataFrame, current_year: int, cutoff: pd.Timestamp, report: str, db_path: str, table_colors: Dict = None, save_to_db: bool = False) -> pd.DataFrame:
    """
    Build table 3c for PO exceeding FDI commitments.

    Parameters:
    -----------
    df_summa : pd.DataFrame
        Input DataFrame with relevant columns.
    current_year : int
        Current year for date filtering.
    report : str
        Report name.
    db_path : str
        Path to the database file.
    table_colors : dict, optional
        Dictionary of colors for styling. Defaults to None.
    save_to_db : bool, optional
        Whether to save the table to the database. Defaults to False.

    Returns:
    --------
    pd.DataFrame
        Processed DataFrame with commitments data.
    """
    log.debug("Start processing build_po_exceeding_FDI_tb_3c")
    # Define colors if table_colors is not provided
    BLUE = table_colors.get("BLUE", "#004A99") if table_colors else "#004A99"
    LIGHT_BLUE = table_colors.get("LIGHT_BLUE", "#d6e6f4") if table_colors else "#d6e6f4"
    # GRID_CLR = table_colors.get("GRID_CLR", "#004A99") if table_colors else "#004A99"
    DARK_BLUE = table_colors.get("DARK_BLUE", "#01244B") if table_colors else "#01244B"
    # DARK_GREY = table_colors.get("DARK_GREY", "#242425") if table_colors else "#242425"
    SUB_TOTAL_BACKGROUND = table_colors.get("subtotal_background_color", "#E6E6FA")

    df_summa_filtered = df_summa[df_summa['Functional Area'].isin(PROGRAMMES_LIST)]
    df_summa_filtered = df_summa_filtered[df_summa_filtered['Fund Source'].isin(FUND_SOURCES)]
   
    # Function to determine PO_CATEGORY based on the rules
    def determine_po_category(row):
        import time 

        po_category_desc = str(row.get('PO Category Desc', '')).strip()
        po_abac_sap_ref = str(row.get('PO ABAC SAP Reference', '')).strip()
        po_purchase_order_desc = str(row.get('PO Purchase Order Desc', '')).strip()

        if po_category_desc == 'Grant':
            # Check PO ABAC SAP Reference first
            if po_abac_sap_ref and any(call_type in po_abac_sap_ref for call_type in CALLS_TYPES_LIST):
                return next(call_type for call_type in CALLS_TYPES_LIST if call_type in po_abac_sap_ref).upper()
            # If empty or no match, check PO Purchase Order Desc
            elif po_purchase_order_desc and any(call_type in po_purchase_order_desc for call_type in CALLS_TYPES_LIST):
                return next(call_type for call_type in CALLS_TYPES_LIST if call_type in po_purchase_order_desc).upper()
            return 'CSA/SJI'  # Return empty if no match found
        elif po_category_desc in ['Direct Contract', 'Specific Contract']:
            return 'Experts'
        return ''  # Default value for other cases
   
    # Ensure df_summa_filtered is a new DataFrame to avoid SettingWithCopyWarning
    df_summa_filtered = df_summa_filtered.copy()
   
    # Apply the function to create the new column using .loc
    df_summa_filtered.loc[:, 'PO_CATEGORY'] = df_summa_filtered.apply(determine_po_category, axis=1)

    # Define the mapping dictionary
    programme_mapping = {
        'HORIZONEU_21_27': 'HE',
        'H2020_14_20': 'H2020'
    }

    # Create the new column 'Programme' based on 'Functional Area'
    df_summa_filtered['Programme'] = df_summa_filtered['Functional Area'].map(programme_mapping).fillna('')
    
    # Perform aggregation by PO Purchase Order Key
    aggregated_df = df_summa_filtered.groupby('PO Purchase Order Key').agg({
        'PO Open Amount - RAL - Payments Made (PD Approved)': 'sum',  # Sum the numeric column
        'Programme': 'first',  # Take the first non-null value (assuming consistency)
        'PO_CATEGORY': 'first',  # Take the first non-null value (assuming consistency)
        'PO Final Date of Implementation (dd/mm/yyyy)': 'max'  # Take the maximum (latest) date
    }).reset_index()
   
    # Rename the aggregated column for clarity
    aggregated_df = aggregated_df.rename(columns={
        'PO Open Amount - RAL - Payments Made (PD Approved)': 'Total_Open_Amount',
        'PO Final Date of Implementation (dd/mm/yyyy)': 'PO Final Date of Implementation'
    })

    # Filter to keep only rows where Total_Open_Amount > 0
    aggregated_df = aggregated_df[aggregated_df['Total_Open_Amount'] > 0]

    # Pivot the agg_result to align with the table structure for total commitments
    pivot_open = pd.pivot_table(
        aggregated_df,
        index=['Programme', 'PO_CATEGORY'],
        values=['PO Purchase Order Key'],
        aggfunc="count",
        fill_value=0
    ).reset_index()
    
    pivot_open.columns = [
        "Programme",
        "PO Type",
        "Total Commitments with RAL",
    ]
    
    # Ensure PO Final Date of Implementation is in datetime format after aggregation
    aggregated_df['PO Final Date of Implementation'] = pd.to_datetime(
        aggregated_df['PO Final Date of Implementation'], 
        format='%Y-%m-%d %H:%M:%S',  # Match the format from the table
        errors='coerce'
    )
  
    # Filter to keep only rows where PO Final Date of Implementation <= cutoff
    aggregated_df = aggregated_df[
        aggregated_df['PO Final Date of Implementation'].notna() &
        (aggregated_df['PO Final Date of Implementation'] <= cutoff)
    ]

    # Compute the number of days elapsed from PO Final Date of Implementation to cutoff
    aggregated_df['Days_Elapsed_From_Cutoff'] = (cutoff - aggregated_df['PO Final Date of Implementation']).dt.days

    # Categorize based on Days_Elapsed_From_Cutoff
    def categorize_days(days):
        if 0 <= days <= 60:
            return "Within 2 months"
        elif 61 <= days <= 180:
            return "Between 2 and 6 months"
        elif days > 180:
            return "More than 6 months"
        else:
            return "Overdue"  # Should not occur due to the <= cutoff filter

    aggregated_df['Category'] = aggregated_df['Days_Elapsed_From_Cutoff'].apply(categorize_days)
    
    # Aggregate by Programme, PO_CATEGORY, and Category
    agg_result = aggregated_df.groupby(['Programme', 'PO_CATEGORY', 'Category']).agg({
        'PO Purchase Order Key': 'count',
    }).reset_index()
    
    # Pivot the agg_result to align with the table structure
    pivot_result = pd.pivot_table(
        agg_result,
        index=['Programme', 'PO_CATEGORY'],
        columns='Category',
        values=['PO Purchase Order Key'],
        aggfunc="sum",
        fill_value=0
    ).reset_index()

    # Flatten MultiIndex columns
    pivot_result.columns = ['__'.join(col).strip() if isinstance(col, tuple) else col for col in pivot_result.columns]

    # Reset index
    pivot_result = pivot_result.reset_index()
   
    # Rename the columns to cleaner names
    pivot_result.columns = [
        "Index",
        "Programme",
        "PO Type",
        "Between 2 and 6 Months",
        "More Than 6 Months",
        "Within 2 Months"
    ]
   
    pivot_result.drop(columns=["Index"], inplace=True)

    pivot_result = pivot_result[["Programme", "PO Type", "Within 2 Months", "Between 2 and 6 Months", "More Than 6 Months"]]
    
    # Merge with pivot_open for total commitments
    merged_df = pd.merge(pivot_result, pivot_open, on=["Programme", "PO Type"], how="outer")
    merged_df = merged_df.fillna(0)

    # Calculate total overdue
    merged_df['Total Overdue'] = merged_df['More Than 6 Months'] + merged_df['Between 2 and 6 Months'] + merged_df['Within 2 Months']
  
    # Calculate percentage of overdue/running grants
    merged_df['% of Overdue/running grants'] = merged_df['Total Overdue'] / merged_df['Total Commitments with RAL']
    merged_df['% of Overdue/running grants'] = merged_df['% of Overdue/running grants'].fillna(0)  # Handle division by zero
    

    # Compute totals for each Programme, excluding '% of Overdue/running grants'
    numeric_cols = ['Within 2 Months', 'Between 2 and 6 Months', 'More Than 6 Months',
                    'Total Overdue', 'Total Commitments with RAL']

    # Group by Programme and sum the numeric columns
    totals = merged_df.groupby('Programme')[numeric_cols].sum().reset_index()
  
    # Calculate '% of Overdue/running grants' for the total rows
    totals['% of Overdue/running grants'] = totals['Total Overdue'] / totals['Total Commitments with RAL']
    totals['% of Overdue/running grants'] = totals['% of Overdue/running grants'].fillna(0)  # Handle division by zero

    # Create total rows with "PO Type" as "Total <Programme>"
    total_rows = []
    for _, row in totals.iterrows():
        programme = row['Programme']
        total_row = row.copy()
        total_row['PO Type'] = f'Total {programme}'
        total_rows.append(total_row)

    # Convert total rows to DataFrame
    total_df = pd.DataFrame(total_rows)
  
    # Concatenate the original DataFrame with the total rows
    df_with_totals = pd.concat([merged_df, total_df], ignore_index=True)

    # Sort by Programme and then by PO Type to ensure totals appear at the end of each group
    df_with_totals = df_with_totals.sort_values(by=['Programme', 'PO Type']).reset_index(drop=True)

    # Create the GreatTables object
    if not df_with_totals.empty:
        tbl = (
            GT(
                df_with_totals,
                rowname_col="PO Type",
                groupname_col="Programme"
            )
            .tab_header(
            title=html(
                f"<strong style='color: white; font-size: large;'>PO Purchase Orders exceeding the Final Date of Implementation</strong>  "
            )
            )
             # Table Outline
            # .opt_table_outline(style="solid", width=outline_b, color=DARK_BLUE) 
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
            # Arial font
            .opt_table_font(font="Arial")
            .opt_table_outline(width='1px', color="#cccccc")
            .tab_style(
                    style=[
                        # style.borders(weight="1px", color=DARK_BLUE),
                        style.fill(color=BLUE),
                        style.text(color="white", weight="bold", align="center"),
                        style.css("max-width:200px; line-height:1.2")
                    ],
                    locations=loc.column_labels()
               )

            .tab_style(
                style=[
                    style.text(color='white', weight="bold", align="center", size='small'),
                    style.fill(color=BLUE),
                    style.css("text-align: center; vertical-align: middle; max-width:200px; line-height:1.2")
                ],
                locations=loc.stubhead()
            )
            # Format numeric columns as integers (except percentage column)
            .fmt_number(
                columns=[col for col in df_with_totals.columns[2:-1] if col != '% of Overdue/running grants'],
                decimals=0,
                use_seps=True
            )
            # Format '% of Overdue/running grants' as percentage with 2 decimal places
            .fmt_percent(
                columns='% of Overdue/running grants',
                decimals=1,
            )
            .tab_style(
                style.text(color=DARK_BLUE, weight="bold", align="center", font='Arial'),
                locations=loc.header()
            )
            .tab_stubhead(label="PO Type")
            .tab_style(
                style=[
                    style.text(color=DARK_BLUE, weight="bold", font='Arial', size='medium'),
                    style.fill(color=LIGHT_BLUE),
                    style.css("max-width:200px; line-height:1.2"),
                ],
                locations=loc.row_groups()
            )
           
           
            .tab_style(
                style=[
                    #    style.borders(weight="1px", color=DARK_BLUE),
                       style.text(size='small')],
                locations=loc.stub()
            )
            .tab_style(
                style=[
                    #    style.borders(sides="all", color=DARK_BLUE, weight="1px"),
                       style.text(align="center", size='small')],
                locations=loc.body()
            )
            .tab_style(
                style=[style.fill(color=SUB_TOTAL_BACKGROUND), style.text( weight="bold")],
                locations=loc.body(rows=df_with_totals.index[df_with_totals["PO Type"].str.contains("Total")].tolist())
            )
            .tab_style(
                style=[style.fill(color=SUB_TOTAL_BACKGROUND), style.text( weight="bold")],
                locations=loc.stub(rows=df_with_totals.index[df_with_totals["PO Type"].str.contains("Total")].tolist())
            )
            .tab_source_note("Source: Compass")
            .tab_source_note("Reports: Call Overview Report - Budget Follow-Up Report - Ethics Requirements and Issues")
            .tab_style(
                style=[style.text(size="small")],
                locations=loc.footer()
            )
        )
    else:
        log.warning("No data to display for table 3c.")
        tbl = None

    # Store the table and data if save_to_db is True
    try:
        insert_variable(
            report=report,
            module="GrantsModule",
            var="table_3c_PO_exceeding_FDI",
            value=df_with_totals.to_dict(orient="records"),
            db_path=db_path,
            anchor="table_3c",
            gt_table=tbl,
        )
        log.debug("Stored 3c table and data")
    except Exception as e:
        logging.error(f"Error storing table for table_3c: {str(e)}")
    log.debug("Complete processing build_po_exceeding_FDI_tb_3c ")
    return df_with_totals  # Corrected return value from agg_with_subtotals to df_with_totals

@debug_wrapper
def build_signatures_table(
    df: pd.DataFrame,
    cutoff: datetime,
    scope_months: list,
    exclude_topics: list,
    report: str,
    db_path: str,
    table_colors: dict = None
) -> Dict[str, Union[pd.DataFrame, GT]]:
    """
    Build a table summarizing grant signatures and under-preparation projects by topic.

    Args:
        df (pd.DataFrame): Input DataFrame with grant data.
        cutoff (datetime): Cutoff date to determine the current quarter and filter data.
        scope_months (list): List of months to include in the report.
        exclude_topics (list): List of topics to exclude from the analysis.
        report (str): Name of the report for storing the table.
        db_path (str): Path to the SQLite database for storing the table.
        table_colors (dict, optional): Dictionary of colors for styling. Defaults to None.

    Returns:
        dict
            keys = "data" (DataFrame), "table" (GreatTable object)
    """
    log.debug("Start processing build_signatures_table")
    # Define expected columns for validation
    df['SIGNED'] = df['Project Status'].isin(PROJECT_STATUS).astype('int8')  

    # df.to_excel('df_grants.xlsx')
    expected_columns = ["GA Signature - Commission", "Topic", "SIGNED", "STATUS"]

    # Validate input DataFrame
    if not isinstance(df, pd.DataFrame):
        logging.error("Input 'df' is not a pandas DataFrame.")
        return {"data": pd.DataFrame(), "table": None}

    if df.empty:
        logging.warning("Input DataFrame is empty. Returning empty DataFrame and None table.")
        return {"data": pd.DataFrame(), "table": None}

    # Check for missing columns
    missing_columns = [col for col in expected_columns if col not in df.columns]
    if missing_columns:
        logging.error(f"Missing required columns: {missing_columns}")
        return {"data": pd.DataFrame(), "table": None}

    # Log input DataFrame info
    logging.debug(f"Input DataFrame shape: {df.shape}")
    logging.debug(f"Input DataFrame columns: {df.columns.tolist()}")

    try:
        # Use default colors if table_colors is None
        table_colors = table_colors or {
            "BLUE": "#0000FF",
            "LIGHT_BLUE": "#ADD8E6",
            "DARK_BLUE": "#00008B"
        }

        # Unpack the ones you need
        BLUE = table_colors['BLUE']
        LIGHT_BLUE = table_colors['LIGHT_BLUE']
        DARK_BLUE = table_colors['DARK_BLUE']
        SUB_TOTAL_BACKGROUND = table_colors.get("subtotal_background_color", "#E6E6FA")

        # Filter in-scope data for signed grants
        in_scope = (
            df["GA Signature - Commission"].dt.year.eq(cutoff.year) &
            df["GA Signature - Commission"].dt.month_name().isin(scope_months)
        )
       
        signed = df.loc[in_scope].copy()
        signed = signed[~signed["Topic"].isin(exclude_topics)]

        

        # Create pivot table for signed data
        tab3_signed = (
            signed.pivot_table(
                index=signed["GA Signature - Commission"].dt.month_name(),
                columns="Topic",
                values="SIGNED",
                aggfunc="sum",
                fill_value=0,
            )
            .reindex(scope_months)
            .reset_index(names="Signature Month")
        )

        # Add TOTAL column for signed data
        tab3_signed["TOTAL"] = tab3_signed.iloc[:, 1:].sum(axis=1)

        # Define a mapping of months to quarters
        month_to_quarter = {
            "January": 1, "February": 1, "March": 1,
            "April": 2, "May": 2, "June": 2,
            "July": 3, "August": 3, "September": 3,
            "October": 4, "November": 4, "December": 4
        }

        # Add a quarter column to tab3_signed
        tab3_signed["Quarter"] = tab3_signed["Signature Month"].map(month_to_quarter)

        # Determine the current quarter based on cutoff date
        current_quarter = (cutoff.month - 1) // 3 + 1

        # Prepare final DataFrame for signed data with conditional quarterly aggregation
        if not tab3_signed.empty:
            final_rows = []
            
            # Check if the data contains exactly three months
            unique_months = tab3_signed["Signature Month"].nunique()
            max_quarter = tab3_signed["Quarter"].max()

            if unique_months == 3 and max_quarter == 1:
                # Special case: exactly three months, all in Quarter 1, show individually
                final_rows.append(tab3_signed.drop(columns=["Quarter"]))
            else:
                # General case: aggregate previous quarters, show current quarter months individually
                for quarter in sorted(tab3_signed["Quarter"].unique()):
                    quarter_data = tab3_signed[tab3_signed["Quarter"] == quarter].copy()
                    
                    if quarter < current_quarter:
                        # Aggregate previous quarters into a single row
                        quarter_sum = quarter_data.iloc[:, 1:-1].sum(numeric_only=True)
                        quarter_row = pd.DataFrame({
                            "Signature Month": [f"Quarter {quarter}"],
                            **{col: [quarter_sum[col]] for col in quarter_data.columns[1:-2]},
                            "TOTAL": [quarter_sum["TOTAL"]]
                        })
                        final_rows.append(quarter_row)
                    else:
                        # Keep individual months for the current quarter
                        quarter_data = quarter_data.drop(columns=["Quarter"])
                        final_rows.append(quarter_data)

            # Compute Grand Total for signed data
            col_totals = pd.DataFrame(tab3_signed.iloc[:, 1:-1].sum(), columns=["Grand Total"]).T
            col_totals.insert(0, "Signature Month", "Grand Total")
            for col in tab3_signed.columns[1:-2]:
                col_totals[col] = tab3_signed[col].sum()

            # Combine signed data rows
            agg_with_totals = pd.concat(final_rows + [col_totals], ignore_index=True)
        else:
            agg_with_totals = tab3_signed

        agg_with_totals['Status'] = 'Signed'
        logging.debug(f"Signed data processed. Shape: {agg_with_totals.shape}")

        # Under-preparation in-scope
        under_prep = df[df['STATUS'].eq("UNDER_PREPARATION")]
        under_prep = under_prep[~under_prep["Topic"].isin(exclude_topics)]

        # --- UNDER-PREPARATION • one “total” row --------------------------------
        tab3_prep = (
            under_prep
            .assign(UNDER_PREP=1)
            .pivot_table(
                columns="Topic",
                values="UNDER_PREP",
                aggfunc="sum",
                fill_value=0,
            )
        )

        # Label the row
        tab3_prep.index = ["Total Under Prep"]
        tab3_prep = tab3_prep.reset_index(names="Signature Month")

        # Add TOTAL column for under preparation data
        tab3_prep["TOTAL"] = tab3_prep.iloc[:, 1:].sum(axis=1)

        tab3_prep['Status'] = 'Under Preparation'
        logging.debug(f"Under Preparation data processed. Shape: {tab3_prep.shape}")

        # Merge agg_with_totals with tab3_prep
        final_df = pd.concat([agg_with_totals, tab3_prep], ignore_index=True)
        logging.debug(f"Final DataFrame shape: {final_df.shape}")

        # Define columns to display in the table (starting from index 1)
        display_columns = final_df.columns[1:-1].tolist()  # Exclude "Signature Month" and "Status"

        # Create the great table
        if not final_df.empty:
            tbl = (
                GT(
                    final_df,
                    rowname_col="Signature Month",
                    groupname_col="Status"
                )
                .tab_header(
                    title="Signatures and Under Preparation by Topic"
                )
        
                # .tab_style(
                #     style.text(color=BLUE, weight="bold", align="center", font='Arial'),
                #     locations=loc.header()
                # )
                 # Table Outline
                # .opt_table_outline(style="solid", width=outline_b, color=DARK_BLUE) 
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
                # Arial font
                .opt_table_font(font="Arial")

                .tab_stubhead(label="Signature Month")
                .tab_style(
                    style=[
                        # style.borders(weight="1px", color=DARK_BLUE),
                        style.text(color=DARK_BLUE, weight="bold", font='Arial'),
                        style.fill(color=LIGHT_BLUE),
                        style.css(f"border-left: 1px solid #cccccc;" ),
                        style.css("max-width:200px; line-height:1.2;"),
                    ],
                    locations=loc.row_groups()
                )
                .fmt_number(
                    columns=display_columns,
                    decimals=0,
                    use_seps=True
                )
                .cols_label(
                    **{col: html(col.replace("_", " ").replace("SyG", "SyG")) for col in display_columns}
                )
                .tab_style(
                    style=[
                        # style.borders(weight="1px", color=DARK_BLUE),
                        style.fill(color=BLUE),
                        style.text(color="white", weight="bold", align="center"),
                        style.css("max-width:200px; line-height:1.2")
                    ],
                    locations=loc.column_labels(columns=display_columns)
                )
                .tab_style(
                    style=[
                        # style.borders(weight="1px", color=DARK_BLUE),
                        style.fill(color=BLUE),
                        style.text(color="white", weight="bold", align="center"),
                        style.css("text-align: center; vertical-align: middle; max-width:200px; line-height:1.2")
                    ],
                    locations=loc.stubhead()
                )

                .tab_style(
                    style=[style.fill(color=SUB_TOTAL_BACKGROUND), style.text(color="black", weight="bold")],
                    locations=loc.body(
                        rows=final_df.index[final_df["Signature Month"] == "Grand Total"].tolist(),
                        columns=display_columns
                    )
                )
                .tab_style(
                    style=[style.fill(color=SUB_TOTAL_BACKGROUND), style.text(weight="bold")],
                    locations=loc.stub(rows=final_df.index[final_df["Signature Month"] == "Grand Total"].tolist())
                )
                .tab_source_note("Source: Quarterly Report Data")
            )
        else:
            tbl = None
            logging.warning("Final DataFrame is empty. Skipping table creation.")

        # Store the table (similar to the example)
        try:
            insert_variable(
                report=report,
                module="GrantsModule",
                var="table_3a_signatures_data",
                value=final_df.to_dict(orient="records"),
                db_path=db_path,
                anchor="table_3_signatures",
                gt_table=tbl
            )
            logging.debug(f"Stored table_3a_signatures_data ({len(final_df)} rows)")
        except Exception as e:
            logging.error(f"Error storing table: {str(e)}")
        log.debug("End processing build_signatures_table")
        return {"data": final_df, "table": tbl}
    except Exception as e:
        logging.error(f"Unexpected error in build_signatures_table: {str(e)}")
        log.debug("End processing build_signatures_table")
        return {"data": pd.DataFrame(), "table": None}

@debug_wrapper
def build_commitments_table(
    df: pd.DataFrame,
    cutoff: datetime,
    scope_months: list,
    exclude_topics: list,
    report: str,
    db_path: str,
    table_colors: dict = None
) -> Dict[str, Union[pd.DataFrame, GT]]:
    """
    Build a table summarizing grant commitments by topic, including both amounts and counts.

    Args:
        df (pd.DataFrame): Input DataFrame with grant data.
        cutoff (datetime): Cutoff date to determine the current quarter and filter data.
        scope_months (list): List of months to include in the report.
        exclude_topics (list): List of topics to exclude from the analysis.
        report (str): Name of the report for storing the table.
        db_path (str): Path to the SQLite database for storing the table.
        table_colors (dict, optional): Dictionary of colors for styling. Defaults to None.

    Returns:
        dict
            keys = "data" (DataFrame), "table" (GreatTable object)
    """
    log.debug("Start processing build_commitments_table")
    # Define expected columns for validation
    expected_columns = ["Commitment AO visa", "Topic", "Eu contribution"]

    # Validate input DataFrame
    if not isinstance(df, pd.DataFrame):
        logging.error("Input 'df' is not a pandas DataFrame.")
        return {"data": pd.DataFrame(), "table": None}

    if df.empty:
        logging.warning("Input DataFrame is empty. Returning empty DataFrame and None table.")
        return {"data": pd.DataFrame(), "table": None}

    # Check for missing columns
    missing_columns = [col for col in expected_columns if col not in df.columns]
    if missing_columns:
        logging.error(f"Missing required columns: {missing_columns}")
        return {"data": pd.DataFrame(), "table": None}

    # Log input DataFrame info
    logging.debug(f"Input DataFrame shape: {df.shape}")
    logging.debug(f"Input DataFrame columns: {df.columns.tolist()}")

    try:
        # Use default colors if table_colors is None
        table_colors = table_colors or {
            "BLUE": "#0000FF",
            "LIGHT_BLUE": "#ADD8E6",
            "DARK_BLUE": "#00008B"
        }

        # Unpack the ones you need
        BLUE = table_colors['BLUE']
        LIGHT_BLUE = table_colors['LIGHT_BLUE']
        DARK_BLUE = table_colors['DARK_BLUE']
        SUB_TOTAL_BACKGROUND = table_colors.get("subtotal_background_color", "#E6E6FA")


        in_scope = (
            df["Commitment AO visa"].dt.year.eq(cutoff.year) &
            df["Commitment AO visa"].dt.month_name().isin(scope_months)
        )

        committed = df.loc[in_scope].copy()
        committed = committed[~committed["Topic"].isin(exclude_topics)]

        # --- COMMITMENTS (AMOUNTS) ---
        tab3_commit = (
            committed.pivot_table(
                index=committed["Commitment AO visa"].dt.month_name(),
                columns="Topic",
                values="Eu contribution",
                aggfunc="sum",
                fill_value=0,
            )
            .reindex(scope_months)
            .reset_index(names="Commitment Month")
        )

        tab3_commit["TOTAL"] = tab3_commit.iloc[:, 1:].sum(axis=1)

        # Define a mapping of months to quarters
        month_to_quarter = {
            "January": 1, "February": 1, "March": 1,
            "April": 2, "May": 2, "June": 2,
            "July": 3, "August": 3, "September": 3,
            "October": 4, "November": 4, "December": 4
        }

        # Add a quarter column to tab3_commit
        tab3_commit["Quarter"] = tab3_commit["Commitment Month"].map(month_to_quarter)

        # Determine the current quarter based on cutoff date (May 15, 2025 -> Quarter 2)
        current_quarter = (cutoff.month - 1) // 3 + 1  # Quarter 2 for May

        # Prepare final DataFrame with conditional quarterly aggregation
        if not tab3_commit.empty:
            final_rows = []
            
            # Check if the data contains exactly three months
            unique_months = tab3_commit["Commitment Month"].nunique()
            max_quarter = tab3_commit["Quarter"].max()

            if unique_months == 3 and max_quarter == 1:
                # Special case: exactly three months, all in Quarter 1, show individually
                final_rows.append(tab3_commit.drop(columns=["Quarter"]))
            else:
                # General case: aggregate previous quarters, show current quarter months individually
                for quarter in sorted(tab3_commit["Quarter"].unique()):
                    quarter_data = tab3_commit[tab3_commit["Quarter"] == quarter].copy()
                    
                    if quarter < current_quarter:
                        # Aggregate previous quarters into a single row
                        quarter_sum = quarter_data.iloc[:, 1:-1].sum(numeric_only=True)
                        quarter_row = pd.DataFrame({
                            "Commitment Month": [f"Quarter {quarter}"],
                            **{col: [quarter_sum[col]] for col in quarter_data.columns[1:-2]},
                            "TOTAL": [quarter_sum["TOTAL"]]
                        })
                        final_rows.append(quarter_row)
                    else:
                        # Keep individual months for the current quarter
                        quarter_data = quarter_data.drop(columns=["Quarter"])
                        final_rows.append(quarter_data)

            # Compute Grand Total
            col_totals = pd.DataFrame(tab3_commit.iloc[:, 1:-1].sum(), columns=["Grand Total"]).T
            col_totals.insert(0, "Commitment Month", "Grand Total")
            for col in tab3_commit.columns[1:-2]:
                col_totals[col] = tab3_commit[col].sum()

            # Combine all rows
            agg_with_totals = pd.concat(final_rows + [col_totals], ignore_index=True)
        else:
            agg_with_totals = tab3_commit

        agg_with_totals['Type'] = 'Committed Amounts(EUR)'

        # --- COMMITMENTS (COUNTS) ---
        tab3_commit_n = (
            committed.pivot_table(
                index=committed["Commitment AO visa"].dt.month_name(),
                columns="Topic",
                values="Eu contribution",
                aggfunc="count",
                fill_value=0,
            )
            .reindex(scope_months)
            .reset_index(names="Commitment Month")
        )

        tab3_commit_n["TOTAL"] = tab3_commit_n.iloc[:, 1:].sum(axis=1)

        # Add a quarter column to tab3_commit_n
        tab3_commit_n["Quarter"] = tab3_commit_n["Commitment Month"].map(month_to_quarter)

        # Prepare final DataFrame with conditional quarterly aggregation
        if not tab3_commit_n.empty:
            final_rows = []
            
            # Check if the data contains exactly three months
            unique_months = tab3_commit_n["Commitment Month"].nunique()
            max_quarter = tab3_commit_n["Quarter"].max()

            if unique_months == 3 and max_quarter == 1:
                # Special case: exactly three months, all in Quarter 1, show individually
                final_rows.append(tab3_commit_n.drop(columns=["Quarter"]))
            else:
                # General case: aggregate previous quarters, show current quarter months individually
                for quarter in sorted(tab3_commit_n["Quarter"].unique()):
                    quarter_data = tab3_commit_n[tab3_commit_n["Quarter"] == quarter].copy()
                    
                    if quarter < current_quarter:
                        # Aggregate previous quarters into a single row
                        quarter_sum = quarter_data.iloc[:, 1:-1].sum(numeric_only=True)
                        quarter_row = pd.DataFrame({
                            "Commitment Month": [f"Quarter {quarter}"],
                            **{col: [quarter_sum[col]] for col in quarter_data.columns[1:-2]},
                            "TOTAL": [quarter_sum["TOTAL"]]
                        })
                        final_rows.append(quarter_row)
                    else:
                        # Keep individual months for the current quarter
                        quarter_data = quarter_data.drop(columns=["Quarter"])
                        final_rows.append(quarter_data)

            # Compute Grand Total
            col_totals = pd.DataFrame(tab3_commit_n.iloc[:, 1:-1].sum(), columns=["Grand Total"]).T
            col_totals.insert(0, "Commitment Month", "Grand Total")
            for col in tab3_commit_n.columns[1:-2]:
                col_totals[col] = tab3_commit_n[col].sum()

            # Combine all rows
            agg_with_totals_n = pd.concat(final_rows + [col_totals], ignore_index=True)
        else:
            agg_with_totals_n = tab3_commit_n

        agg_with_totals_n['Type'] = 'Number of Commitments'

        # Append agg_with_totals_n to agg_with_totals to create the final combined table
        final_agg_table = pd.concat([agg_with_totals, agg_with_totals_n], ignore_index=True)

        # Define columns to display in the table (starting from index 1)
        display_columns = final_agg_table.columns[1:-1].tolist()  # Exclude "Commitment Month" and "Type"

        # Create the great table
        if not final_agg_table.empty:
            tbl = (
                GT(
                    final_agg_table,
                    rowname_col="Commitment Month",
                    groupname_col="Type"
                )
                .tab_header(
                    title="HE Commitment Activity"
                )
                 # Table Outline
                # .opt_table_outline(style="solid", width=outline_b, color=DARK_BLUE) 
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
                # Arial font
                .opt_table_font(font="Arial")
                # Format "amounts" group as currency (EUR with 2 decimal places)
                .fmt_number(
                    columns=display_columns,
                    rows=final_agg_table.index[final_agg_table["Type"] == 'Committed Amounts(EUR)'].tolist(),
                    accounting=True,
                    decimals=2,
                    use_seps=True
                )
                # Format "numbers" group as integers
                .fmt_number(
                    columns=display_columns,
                    rows=final_agg_table.index[final_agg_table["Type"] == 'Number of Commitments'].tolist(),
                    decimals=0,
                    use_seps=True
                )
                .tab_style(
                    style.text(color=BLUE, weight="bold", align="center", font='Arial'),
                    locations=loc.header()
                )
                .tab_stubhead(label="Commitment Month")
                .tab_style(
                    style=[
                        # style.borders(weight="1px", color=DARK_BLUE),
                        style.text(color=DARK_BLUE, weight="bold", font='Arial', size='medium'),
                        style.fill(color=LIGHT_BLUE),
                        style.css(f" border-left: 1px solid #cccccc;"),
                        style.css("max-width:200px; line-height:1.2"),
                    ],
                    locations=loc.row_groups()
                )
                .opt_table_font(font="Arial")
                .tab_style(
                    style=[
                        # style.borders(weight="1px", color=DARK_BLUE),
                        style.fill(color=BLUE),
                        style.text(color="white", weight="bold", align="center", size='small'),
                        style.css("max-width:200px; line-height:1.2")
                    ],
                    locations=loc.column_labels()
                )
                .tab_style(
                    style=[
                        # style.borders(weight="1px", color=DARK_BLUE),
                        style.fill(color=BLUE),
                        style.text(color="white", weight="bold", align="center", size='small'),
                        style.css("text-align: center; vertical-align: middle; max-width:200px; line-height:1.2")
                    ],
                    locations=loc.stubhead()
                )
                .tab_style(
                     style=[
                        #    style.borders(weight="1px", color=DARK_BLUE),
                           style.text(size='small')],
                    locations=loc.stub()
                )
                .tab_style(
                    style=[
                        #    style.borders(sides="all", color=DARK_BLUE, weight="1px"),
                           style.text(align="center", size='small')],
                    locations=loc.body()
                )
                # .tab_style(
                #     style=style.borders(color=DARK_BLUE, weight="1px"),
                #     locations=[loc.column_labels(), loc.stubhead()]
                # )
                .tab_style(
                    style=[style.fill(color=SUB_TOTAL_BACKGROUND ), style.text(color="black", weight="bold")],
                    locations=loc.body(rows=final_agg_table.index[final_agg_table["Commitment Month"] == "Grand Total"].tolist())
                )
                .tab_style(
                    style=[style.fill(color=SUB_TOTAL_BACKGROUND ), style.text(color="black", weight="bold")],
                    locations=loc.stub(rows=final_agg_table.index[final_agg_table["Commitment Month"] == "Grand Total"].tolist())
                )
                .tab_source_note("Source: Compass")
                .tab_source_note("Reports: Call Overview Report - Budget Follow-Up Report - Ethics Requirements and Issues")
                .tab_style(
                    style=[style.text(size="small")],
                    locations=loc.footer()
                )
            )
        else:
            tbl = None
            logging.warning("Final DataFrame is empty. Skipping table creation.")

        # Store the table
        try:
            insert_variable(
                report=report,
                module="GrantsModule",
                var="table_3b_commitments_data",
                value=final_agg_table.to_dict(orient="records"),
                db_path=db_path,
                anchor="table_3b_commitments",
                gt_table=tbl
            )
            logging.debug(f"Stored table_3b_commitments_data ({len(final_agg_table)} rows)")
        except Exception as e:
            logging.error(f"Error storing table: {str(e)}")
        log.debug("Complete processing build_commitments_table")
        return {"data": final_agg_table, "table": tbl}
    except Exception as e:
        logging.error(f"Unexpected error in build_commitments_table: {str(e)}")
        log.debug("Complete processing build_commitments_table")
        return {"data": pd.DataFrame(), "table": None}
       