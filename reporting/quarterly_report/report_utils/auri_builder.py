from __future__ import annotations

import logging
import pandas as pd
from pathlib import Path
import sqlite3
from datetime import datetime
from reporting.quarterly_report.utils import Database
from ingestion.db_utils import insert_variable

# our project
from ingestion.db_utils import (
    init_db,                                 # create tables if missing
    fetch_latest_table_data,                 # new version!
    get_alias_last_load,
    get_variable_status, 
    load_report_params                   # to inspect results
)
from great_tables import GT, style, loc
from typing import List, Dict, Optional
import numpy as np


# ──────────────────────────────────────────────────────────────
# HELPERS
# ──────────────────────────────────────────────────────────────
OUTLINE_B = '1px'


def _snapshot(
    df: pd.DataFrame,
    period_end: pd.Timestamp,
    label: str,
    *,
    sources: tuple[str, ...] = ("CAS audits", "ECA audits", "Extensions"),
    amount_filter: int = -200,          # negative adjustments only
) -> pd.DataFrame:
    """
    Return a wide one-period dataframe:

        Indicator | <label> number of cases | <label> in € | <label> % cases
    """
    rows: list[dict[str, object]] = []

    period_df = df[
        (df["AURI_START"] <= period_end) &
        (df["AUDEX_TOTAL_COST_ADJUSTMENT"] <= amount_filter)
    ]

    # ── three ex-post sources ─────────────────────────────────────────
    for src in sources:
        subset = period_df[period_df["AURI_SOURCE"] == src]

        total_cases = len(subset)
        implemented = subset[
            subset["AURI_END_DT"].notna() &
            (subset["AURI_END_DT"] <= period_end)
        ]

        impl_cnt = len(implemented)
        impl_eur = implemented["AUDEX_TOTAL_COST_ADJUSTMENT"].sum()
        pct_impl = (impl_cnt / total_cases * 100) if total_cases else 0

        nice_name = f"{src.split()[0]} participation implemented" \
                    if src != "Extensions" else "Extensions participation implemented"

        rows.append({
            "Indicator": nice_name,
            f"{label} number of cases": impl_cnt,
            f"{label} in €": impl_eur,
            f"{label} % cases": pct_impl,
        })

    return pd.DataFrame(rows)


def determine_epoch_year(cutoff_date: pd.Timestamp) -> int:
    """
    Returns the correct reporting year.
    If the cutoff is in January, then we are reporting for the *previous* year.
    """
    return cutoff_date.year - 1 if cutoff_date.month == 1 else cutoff_date.year


def get_months_in_scope(cutoff: pd.Timestamp) -> list[int]:
    """
    Returns list of month numbers from 1 (January) to the last *full* month before cutoff.
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
    ).month.tolist()

    return months


def get_last_date_in_scope(cutoff: pd.Timestamp, months_scope: list[int]) -> pd.Timestamp:
    """
    Returns the last date of the last month in scope.
    For example, if months_scope is [1, 2, 3] (Jan to Mar) and cutoff is in 2025,
    returns 2025-03-31.
    """
    last_month = max(months_scope)
    year = cutoff.year if cutoff.month != 1 else cutoff.year - 1
    # Create a timestamp for the first day of the next month, then subtract 1 day
    next_month_start = pd.Timestamp(year=year, month=last_month + 1, day=1)
    last_date = next_month_start - pd.Timedelta(days=1)
    return last_date


def load_auri_data(db_path: str, cutoff_date: datetime, current_year: int, months_scope: list[int]) -> pd.DataFrame:
    """Load and preprocess AURI data from the database."""

    db = Database(str(db_path))         # thin sqlite3 wrapper
    conn = db.conn
    auri_raw_df = fetch_latest_table_data(conn , "audit_result_implementation", cutoff_date)

    last_date = get_last_date_in_scope(cutoff_date, months_scope)

    date_cols = ['AURI_START', 'AURI_END_DATE']

    # ▸ strip spaces, then convert
    auri_raw_df[date_cols] = (
        auri_raw_df[date_cols]
        .apply(lambda s: pd.to_datetime(
            s.str.strip(),                 # clean up
            errors='coerce',               # bad → NaT
            format='%Y-%m-%d %H:%M:%S'     # matches 2022-12-19 00:00:00
            ))
        )   

    # filter
    auri_raw_df = auri_raw_df[auri_raw_df['AURI_START'] <= last_date].copy()

    # # set any AURI_END_DATE past cutoff to NaT
    auri_raw_df.loc[
        auri_raw_df['AURI_END_DATE'] > last_date,
        'AURI_END_DATE'
    ] = pd.NaT

    # ───── keep a pure-datetime copy for internal calculations ─┐
    auri_raw_df["AURI_END_DT"] = auri_raw_df["AURI_END_DATE"]     # NEW COLUMN
    # └───────────────────────────────────────────────────────────┘

    # Debug: Check if DataFrame is empty after loading
    if auri_raw_df.empty:
        print("Warning: auri_raw_df is empty after loading from database.")
        return auri_raw_df

    auri_raw_df = auri_raw_df.copy()

    auri_raw_df['TTI_AURI'] = (pd.to_datetime(auri_raw_df['AURI_END_DATE']) - pd.to_datetime(auri_raw_df['AURI_START'])).dt.days
    auri_raw_df['TTI_AURI'] = auri_raw_df['TTI_AURI'].fillna(0).astype(int)
    auri_raw_df['AURI_END_YEAR'] = pd.to_datetime(auri_raw_df['AURI_END_DATE']).dt.year
    auri_raw_df['AURI_END_YEAR'] = auri_raw_df['AURI_END_YEAR'].fillna(0).astype(int)
    auri_raw_df['AURI_END_DATE'] = auri_raw_df['AURI_END_DATE'].fillna('no')
    auri_raw_df['AMOUNT_TO_RECOVER'] = auri_raw_df['AMOUNT_TO_RECOVER'].fillna(0)

    auri_raw_df.insert(2, 'AURI_SOURCE', auri_raw_df['AUDIT_KEY'].astype(str).str[:4])
    auri_raw_df['AURI_SOURCE'] = auri_raw_df['AURI_SOURCE'].replace({'CCIA': 'ECA audits'})
    auri_raw_df.loc[auri_raw_df['AUDIT_EXTENSION'] == 'Y', 'AURI_SOURCE'] = 'Extensions'
    auri_raw_df.loc[(auri_raw_df['AUDIT_EXTENSION'] == 'N') & (auri_raw_df['AURI_SOURCE'] != 'ECA audits'), 'AURI_SOURCE'] = 'CAS audits'

    return auri_raw_df

def auri_overview_df(auri_df: pd.DataFrame) -> pd.DataFrame:
    """Generate DataFrame for AURI Overview by Source with reordered columns."""
    sources =  ['CAS audits', 'ECA audits', 'Extensions', 'Total']
    data = []

    for source in sources:
        if source == 'Total':
            df_subset = auri_df
        else:
            df_subset = auri_df[auri_df['AURI_SOURCE'] == source]

        total = len(df_subset)
        processed = len(df_subset[df_subset['AURI_END_DATE'] != 'no'])
        pending = len(df_subset[df_subset['AURI_END_DATE'] == 'no'])
        pct_processed = (processed / total * 100) if total > 0 else 0
        pct_pending = (pending / total * 100) if total > 0 else 0

        data.append({
            'Source': source,
            'Audit results processed': processed,
            '% Audit results processed': pct_processed,
            'Audit results pending': pending,
            '% Audit results pending': pct_pending,
            'Total': total
        })

    return pd.DataFrame(data)

def tti_closed_projects_df(auri_df: pd.DataFrame, year: int) -> pd.DataFrame:
    """Generate DataFrame for TTI of Closed Projects with a Total row."""
    categories = [
        ('Negative with Recoveries*', lambda df: df[(df['AUDEX_TOTAL_COST_ADJUSTMENT'] < 0) & (df['AMOUNT_TO_RECOVER'] > 0)]),
        ('Negative without Recoveries', lambda df: df[(df['AUDEX_TOTAL_COST_ADJUSTMENT'] < 0) & (df['AMOUNT_TO_RECOVER'] == 0)]),
        ('Positive/Zero Adjustments', lambda df: df[df['AUDEX_TOTAL_COST_ADJUSTMENT'] >= 0])
    ]
    data = []

    base_df = auri_df[(auri_df['AURI_END_YEAR'] == year) & (auri_df['AURI_MODE'] == 'RO')]
    for category, filter_func in categories:
        df_subset = filter_func(base_df)
        total = len(df_subset)
        below_6m = len(df_subset[df_subset['TTI_AURI'] <= 180])
        above_6m = len(df_subset[df_subset['TTI_AURI'] > 180])
        pct_below = (below_6m / total * 100) if total > 0 else 0
        pct_above = (above_6m / total * 100) if total > 0 else 0

        data.append({
            'Adjustment Type': category,
            '0-6 months': below_6m,
            '% total (0-6 months)': pct_below,
            'above 6 months': above_6m,
            '% above 6 months': pct_above,
            'Total': total
        })

    df = pd.DataFrame(data)

    # Add Total row for Closed Projects
    total_row = pd.DataFrame([{
        'Adjustment Type': 'Closed Projects',
        '0-6 months': df['0-6 months'].sum(),
        '% total (0-6 months)': (df['0-6 months'].sum() / df['Total'].sum() * 100) if df['Total'].sum() > 0 else 0,
        'above 6 months': df['above 6 months'].sum(),
        '% above 6 months': (df['above 6 months'].sum() / df['Total'].sum() * 100) if df['Total'].sum() > 0 else 0,
        'Total': df['Total'].sum()
    }], index=['Closed_Total'])

    return pd.concat([total_row, df]).reset_index(drop=True)

def tti_open_projects_df(auri_df: pd.DataFrame, year: int) -> pd.DataFrame:
    """Generate DataFrame for TTI of Open Projects with a Total row."""
    categories = [
        ('Negative with REPA**', lambda df: df[df['AUDEX_TOTAL_COST_ADJUSTMENT'] < 0]),
        ('Positive/Zero Adjustments', lambda df: df[df['AUDEX_TOTAL_COST_ADJUSTMENT'] >= 0])
    ]
    data = []

    base_df = auri_df[(auri_df['AURI_END_YEAR'] == year) & (auri_df['AURI_MODE'] == 'REPA')]
    for category, filter_func in categories:
        df_subset = filter_func(base_df)
        total = len(df_subset)
        below_6m = len(df_subset[df_subset['TTI_AURI'] <= 180])
        above_6m = len(df_subset[df_subset['TTI_AURI'] > 180])
        pct_below = (below_6m / total * 100) if total > 0 else 0
        pct_above = (above_6m / total * 100) if total > 0 else 0

        data.append({
            'Adjustment Type': category,
            '0-6 months': below_6m,
            '% total (0-6 months)': pct_below,
            'above 6 months': above_6m,
            '% above 6 months': pct_above,
            'Total': total
        })

    df = pd.DataFrame(data)

    # Add Total row for On-going Projects
    total_row = pd.DataFrame([{
        'Adjustment Type': 'On-going Projects',
        '0-6 months': df['0-6 months'].sum(),
        '% total (0-6 months)': (df['0-6 months'].sum() / df['Total'].sum() * 100) if df['Total'].sum() > 0 else 0,
        'above 6 months': df['above 6 months'].sum(),
        '% above 6 months': (df['above 6 months'].sum() / df['Total'].sum() * 100) if df['Total'].sum() > 0 else 0,
        'Total': df['Total'].sum()
    }], index=['Open_Total'])

    return pd.concat([total_row, df]).reset_index(drop=True)


def negative_adjustments_df(auri_df: pd.DataFrame) -> pd.DataFrame:
    """Generate DataFrame for Negative Adjustments by Source with updated column names and order."""
    sources = ['CAS audits', 'ECA audits', 'Extensions', 'Total']
    data = []

    for source in sources:
        if source == 'Total':
            df_subset = auri_df[auri_df['AUDEX_TOTAL_COST_ADJUSTMENT'] <= -200]
        else:
            df_subset = auri_df[(auri_df['AURI_SOURCE'] == source) & (auri_df['AUDEX_TOTAL_COST_ADJUSTMENT'] <= -200)]

        total_count = len(df_subset)
        total_amount = df_subset['AUDEX_TOTAL_COST_ADJUSTMENT'].sum()
        processed = df_subset[df_subset['AURI_END_DATE'] != 'no']
        processed_count = len(processed)
        processed_amount = processed['AUDEX_TOTAL_COST_ADJUSTMENT'].sum()
        pending = df_subset[df_subset['AURI_END_DATE'] == 'no']
        pending_count = len(pending)
        pending_amount = pending['AUDEX_TOTAL_COST_ADJUSTMENT'].sum()

        data.append({
            'Source': source,
            'Processed No. of AURIs': processed_count,
            'Processed Adjustment Amount (AUDEX)': processed_amount,
            'Pending No. of AURIs': pending_count,
            'Pending Adjustment Amount (AUDEX)': pending_amount,
            'Total No. of AURIs': total_count,
            'Total Adjustment Amount (AUDEX)': total_amount,
        })

    # Create DataFrame with the desired column order
    df = pd.DataFrame(data)
    df = df[[
        'Source',
        'Processed No. of AURIs',
        'Processed Adjustment Amount (AUDEX)',
        'Pending No. of AURIs',
        'Pending Adjustment Amount (AUDEX)',
        'Total No. of AURIs',
        'Total Adjustment Amount (AUDEX)',
    ]]

    return df

def deviations_df(auri_df: pd.DataFrame, year: int) -> pd.DataFrame:
    """Generate DataFrame for Deviations."""
    deviations = auri_df[
        (auri_df['AURI_END_DATE'] != 'no') &
        (auri_df['AURI_END_YEAR'] == year) &
        (auri_df['AUDEX_TOTAL_COST_ADJUSTMENT'] <= -200) &
        (auri_df['DEVIATION'] == 'Y') &
        (auri_df['DEVIATION_AMOUNT'] < -1)
    ].copy()

    if deviations.empty:
        return pd.DataFrame(columns=[
            'Beneficiary', 'Project Number', 'Acronym', 'Count of AURI',
            'AURI Deviation Comment', 'Audex Total Cost Adjustment',
            'Deviation Amount', 'AURI Cost Adjustments'
        ])

    deviations = deviations[[
        'BENEFICIARY', 'PROJECT_NUMBER', 'ACRONYM', 'AURI_DEVIATION_COMMENT',
        'AUDEX_TOTAL_COST_ADJUSTMENT', 'DEVIATION_AMOUNT', 'AURI_COST_ADJUSTMENTS'
    ]]
    deviations['Count of AURI'] = 1
    deviations = deviations[[
        'BENEFICIARY', 'PROJECT_NUMBER', 'ACRONYM', 'Count of AURI',
        'AURI_DEVIATION_COMMENT', 'AUDEX_TOTAL_COST_ADJUSTMENT',
        'DEVIATION_AMOUNT', 'AURI_COST_ADJUSTMENTS'
    ]]

    total_row = pd.DataFrame([{
        'BENEFICIARY': 'TOTAL',
        'PROJECT_NUMBER': '',
        'ACRONYM': '',
        'Count of AURI': deviations['Count of AURI'].sum(),
        'AURI_DEVIATION_COMMENT': '',
        'AUDEX_TOTAL_COST_ADJUSTMENT': deviations['AUDEX_TOTAL_COST_ADJUSTMENT'].sum(),
        'DEVIATION_AMOUNT': deviations['DEVIATION_AMOUNT'].sum(),
        'AURI_COST_ADJUSTMENTS': deviations['AURI_COST_ADJUSTMENTS'].sum()
    }], index=['Total'])

    deviations = pd.concat([deviations, total_row])
    return deviations


def implementation_comparison_df(
    auri_df: pd.DataFrame,
    cutoff: pd.Timestamp,
    months_scope: list[int]
) -> pd.DataFrame:
    """
    CAS + ECA + Extensions rows, followed by
    'Total participation implemented'. All numeric columns are
    float/int or NaN (no strings).
    """
    last_date   = get_last_date_in_scope(cutoff, months_scope)
    current_lab = last_date.strftime("%b-%y")               # e.g. Mar-25
    prev_end    = pd.Timestamp(year=last_date.year - 1, month=12, day=31)
    prev_lab    = f"Dec-{str(prev_end.year)[-2:]}"          # e.g. Dec-24

    cur  = _snapshot(auri_df, last_date, current_lab)
    prev = _snapshot(auri_df, prev_end,  prev_lab)

    out = cur.merge(prev, on="Indicator", how="outer")

    # ── grand total ---------------------------------------------------
    num_cols = [c for c in out.columns if c != "Indicator" and not c.endswith("% cases")]
    pct_cols = [c for c in out.columns if c.endswith("% cases")]

    total = {"Indicator": "Total participation implemented"}
    for col in num_cols:
        total[col] = out[col].sum()

    # recompute % columns
    for lab in (current_lab, prev_lab):
        impl_sum = total[f"{lab} number of cases"]
        # estimate population = impl / (%/100) for each row, then sum
        pop_sum  = (
            out.apply(
                lambda r: (
                    r[f"{lab} number of cases"] * 100 / r[f"{lab} % cases"]
                    if pd.notna(r[f"{lab} % cases"]) and r[f"{lab} % cases"] > 0
                    else r[f"{lab} number of cases"]
                ),
                axis=1,
            ).sum()
        )
        total[f"{lab} % cases"] = (impl_sum / pop_sum * 100) if pop_sum else np.nan

    out = pd.concat([out, pd.DataFrame([total])], ignore_index=True)

    # make every non-stub column strictly numeric (NaN where coercion fails)
    for col in out.columns:
        if col != "Indicator":
            out[col] = pd.to_numeric(out[col], errors="coerce")

    return out

import pandas as pd
from typing import Dict, Any
def calculate_table_dimensions(
    df: pd.DataFrame,
    config: Dict[str, Any]
) -> tuple[int, int]:
    """
    Calculates the optimal width and height for a DataFrame table based on a detailed config.
    """
    # --- Configuration Extraction ---
    stub_width = config.get("stub_width", 250)
    base_width_per_column = config.get("base_width_per_column", 80)
    normal_row_height = config.get("row_height", 40)
    group_row_height = config.get("group_row_height", 45)
    summary_row_height = config.get("summary_row_height", 45)
    has_spanners = config.get("has_spanners", False)
    wrap_text_column = config.get("wrap_text_column", None)
    line_height = config.get("line_height", 20)
    chars_per_line = config.get("chars_per_line", 60)
    has_groups = config.get("has_groups", False)
    group_col_name = config.get("group_col_name", None)
    group_header_height = config.get("group_header_height", 25)
    summary_row_count = config.get("summary_row_count", 0)

    # --- Static Component Heights (in pixels) ---
    title_height, subtitle_height, column_header_height = 30, 20, 35
    spanner_height = 30 if has_spanners else 0
    footer_padding, border_padding = 30, 20

    # --- Width Calculation ---
    if config.get("total_width_override"):
        table_width_px = config["total_width_override"]
    else:
        num_data_cols = len(df.columns) - 1
        table_width_px = stub_width + (num_data_cols * base_width_per_column)

    # --- Height Calculation ---
    total_data_height = 0
    group_headers_height = 0

    if wrap_text_column and wrap_text_column in df.columns:
        height_from_summary = summary_row_count * summary_row_height
        height_from_data = 0
        data_df = df.iloc[:-summary_row_count] if summary_row_count > 0 else df
        for _, row in data_df.iterrows():
            text_length = len(str(row[wrap_text_column]))
            num_lines = max(1, (text_length // chars_per_line) + 1)
            row_height = max(normal_row_height, (num_lines * line_height) + 15)
            height_from_data += row_height
        total_data_height = height_from_data + height_from_summary
    elif has_groups and group_col_name and group_col_name in df.columns:
        num_groups = df[group_col_name].nunique()
        group_headers_height = num_groups * group_header_height
        special_rows = config.get("special_rows_in_group", [])
        num_special_rows = df.iloc[:, 1].isin(special_rows).sum()
        num_normal_rows = len(df) - num_special_rows
        total_data_height = (num_special_rows * group_row_height) + (num_normal_rows * normal_row_height)
    else:
        group_like_rows = config.get("group_like_rows", [])
        num_group_like_rows = df.iloc[:, 0].isin(group_like_rows).sum()
        num_summary_rows = summary_row_count
        num_normal_rows = len(df) - num_group_like_rows - num_summary_rows
        total_data_height = (num_normal_rows * normal_row_height) + (num_group_like_rows * group_row_height) + (num_summary_rows * summary_row_height)

    base_header_footer_height = (
        title_height + subtitle_height + column_header_height +
        spanner_height + footer_padding + border_padding + group_headers_height
    )
    table_height_px = base_header_footer_height + total_data_height

    # --- Safety Margins ---
    final_width = max(600, min(int(table_width_px), 2000)) # Increased max width
    final_height = max(400, min(int(table_height_px), 1500)) # Increased min/max height

    return final_width, final_height

# --- NEW HELPER FOR tti_combined TO FIX MISSING TOTAL ROW ---
def build_tti_combined_df(auri_df: pd.DataFrame, current_year: int) -> pd.DataFrame:
    """
    Builds the complete tti_combined DataFrame, including the final total row.
    """
    tti_closed = tti_closed_projects_df(auri_df, current_year)
    tti_open = tti_open_projects_df(auri_df, current_year)
    df = pd.concat([tti_closed, tti_open], ignore_index=True)

    # Add grand total row
    subtotal_df = df[df['Adjustment Type'].isin(['Closed Projects', 'On-going Projects'])]
    total_row_data = {
        'Adjustment Type': 'Total',
        '0-6 months': subtotal_df['0-6 months'].sum(),
        'above 6 months': subtotal_df['above 6 months'].sum(),
        'Total': subtotal_df['Total'].sum()
    }
    total_row = pd.DataFrame([total_row_data])
    
    df = pd.concat([df, total_row], ignore_index=True)

    # Calculate percentages after all rows are present
    df['% total (0-6 months)'] = (df['0-6 months'] / df['Total'] * 100).where(df['Total'] > 0, 0)
    df['% above 6 months'] = (df['above 6 months'] / df['Total'] * 100).where(df['Total'] > 0, 0)
    
    return df

# ──────────────────────────────────────────────────────────────
# utils for RO tables
# ──────────────────────────────────────────────────────────────
def load_ro_data(
    db_path: str,
    alias: str,
    cutoff: pd.Timestamp,
) -> pd.DataFrame:
    """
    Load the RO yearly overview table (alias=`c0_ro_yearly_overview`) and
    parse the three date columns. Nothing else.
    """
    db = Database(str(db_path))
    conn = db.conn

    # raw pull
    ro_df = fetch_latest_table_data(conn, alias, cutoff)

    # ---- clean dates --------------------------------------------------
    date_cols = [
        "RO Cashing Date (dd/mm/yyyy)",
        "RO Posting Date (SAP Format yyyymmdd)",
    ]
    for col in date_cols:
        ro_df[col] = pd.to_datetime(
            ro_df[col].astype(str).str.strip(), errors="coerce", dayfirst=True
        )

    # yyyymmdd → datetime (dayfirst=False because format is yyyymmdd or yyyyddmm?)
    ro_df["RO Posting Date (SAP Format yyyymmdd)"] = pd.to_datetime(
        ro_df["RO Posting Date (SAP Format yyyymmdd)"].astype(str).str.strip(),
        errors="coerce",
        format="%d/%m/%Y" if "/" in ro_df["RO Posting Date (SAP Format yyyymmdd)"].iloc[0] else "%d/%m/%Y"
    )

    # simple programme mapper (feel free to extend)
    mapper = {"H2020_14_20": "H2020", "HORIZONEU_21_27": "HEU"}
    ro_df["Programme"] = (
        ro_df["Functional Area"]
        .map(mapper)
        .fillna("Other")
    )

    return ro_df

def recovery_activity_df(ro_df: pd.DataFrame, epoch_year: int) -> pd.DataFrame:
    """
    Build a tidy dataframe with one line per programme for each of the
    four ‘indicators’ that appear in the mock-up.
    """
    # --- helpers -------------------------------------------------------
    def _fmt(num):
        return float(num) if pd.notna(num) else 0.0

    # split once, reuse many times
    cashed = ro_df[
        ro_df["RO Cashing Date (dd/mm/yyyy)"].dt.year == epoch_year
    ]
    issued = ro_df[
        ro_df["RO Posting Date (SAP Format yyyymmdd)"].dt.year == epoch_year
    ]

    frames = []
    for pgm, group in ro_df.groupby("Programme"):
        # ---------------- ROs cashed -----------------------------------
        g_cashed = cashed[cashed["Programme"] == pgm]

        cashed_n  = len(g_cashed)
        cashed_eur = g_cashed["RO Cashing Amount"].sum()

        # offsets are negative cashing amounts
        offsets   = g_cashed[g_cashed["RO Cashing Amount"] < 0]
        offset_n  = len(offsets)
        offset_eur = offsets["RO Cashing Amount"].sum()

        # ---------------- ROs issued -----------------------------------
        g_issued = issued[issued["Programme"] == pgm]

        issued_n   = len(g_issued)
        issued_eur = g_issued["RO Amount"].sum()

        open_ro    = g_issued[g_issued["RO Open Amount"] > 0]
        open_n     = len(open_ro)
        open_eur   = open_ro["RO Open Amount"].sum()

        frames.extend([
            {
                "Row group": "ROs Cashed",
                "Reason for Recovery": f"Total RO cashed/offset in {epoch_year}",
                "Programme": pgm,
                "Number": cashed_n,
                "Amount": _fmt(cashed_eur),
            },
            {
                "Row group": "ROs Cashed",
                "Reason for Recovery": "Out of which are RO offset",
                "Programme": pgm,
                "Number": offset_n,
                "Amount": _fmt(offset_eur),
            },
            {
                "Row group": "ROs Issued",
                "Reason for Recovery": f"Total RO issued in {epoch_year}",
                "Programme": pgm,
                "Number": issued_n,
                "Amount": _fmt(issued_eur),
            },
            {
                "Row group": "ROs Issued",
                "Reason for Recovery": "Out of which are open RO",
                "Programme": pgm,
                "Number": open_n,
                "Amount": _fmt(open_eur),
            },
        ])

    tidy = pd.DataFrame(frames)
    return tidy

# ──────────────────────────────────────────────────────────────
# Recovery‐activity helper (one tidy df, no extra imports needed)
# ──────────────────────────────────────────────────────────────

def _recovery_activity_df(ro_df: pd.DataFrame, epoch_year: int) -> pd.DataFrame:

    # 0) map programme codes -------------------------------------------------
    # 0) map programme codes -------------------------------------------------
    mapper = {"H2020_14_20": "H2020", "HORIZONEU_21_27": "HEU"}
    ro_df["Programme"] = ro_df["Functional Area"].map(mapper).fillna("Other")

    col_cash = "RO Cashing Date (dd/mm/yyyy)"   # the column in your DF

    ro_df["RO Cash Year"] = (
        # 1️⃣  make everything a clean string (NaN → 'nan')
        ro_df[col_cash].astype(str).str.strip()
        # 2️⃣  pull the FIRST 4-digit run you see
        .str.extract(r"(\d{4})")[0]          # returns a Series
        # 3️⃣  to number, NaN where no match
        .astype("Int64")                     # nullable integer dtype
    )

    def _money(series: pd.Series, *, absolute: bool = False) -> pd.Series:
        """Convert '€1 234,56' → 1234.56  (optionally |value|)."""
        cleaned = (
            series.astype(str)
                .str.replace(r"[^\d\-.]", "", regex=True)  # keep digits . and -
                .replace("", "0")
                .astype(float)
        )
        return cleaned.abs() if absolute else cleaned

    # RO Cashing Amount   (make positive)
    ro_df["RO Cashing Amount"] = _money(ro_df["RO Cashing Amount"], absolute=True)

    # RO Amount and RO Open Amount keep the original sign
    ro_df["RO Amount"]       = _money(ro_df["RO Amount"])
    ro_df["RO Open Amount"]  = _money(ro_df["RO Open Amount"])

    # 3) build the indicators ------------------------------------------------
    rows = []
    for pgm in ["H2020", "HEU"]:

        # —— ROs Cashed ——————————————————————————————
        # ROs Cashed
        cashed = ro_df[
            (ro_df["Programme"] == pgm) &
            (ro_df["RO Cash Year"] == epoch_year)        # <-- no .dt.year
        ]

        rows.append((
        "ROs Cashed",
        f"Total RO cashed in {epoch_year}",
        pgm,
        cashed["RO Recovery Order Key"].nunique(),
        cashed["RO Cashing Amount"].sum(),   # ← no negative sign
    ))

        # —— ROs Issued ——————————————————————————————
        issued = ro_df[
        (ro_df["Programme"] == pgm) &
        (ro_df["RO Year Of Origin"] == epoch_year)   # <-- no .dt.year
        ]
        open_ro = issued[issued["RO Open Amount"] > 0]

        rows.extend([
            ("ROs Issued",
                f"Total RO issued in {epoch_year}",
                pgm,
                issued["RO Recovery Order Key"].nunique(),
                issued["RO Amount"].sum()
            ),
            ("ROs Issued",
                "Out of which are open RO",
                pgm,
                open_ro["RO Recovery Order Key"].nunique(),
                open_ro["RO Open Amount"].sum()
            ),
        ])

        # ---- build tidy table -------------------------------------------------
    df_rows = pd.DataFrame(
        rows,
        columns=["Row group", "Reason", "Programme", "Number", "Amount"]
    )

    tidy = (
        df_rows
        .pivot_table(                       # <- use pivot_table
            index=["Row group", "Reason"],
            columns="Programme",
            values=["Number", "Amount"],
            aggfunc="first",                # exactly one value per cell
            sort=False                      # keep the row order you appended
        )
    )

    # add total columns -----------------------------------------------------
    tidy[("Number", "Total")] = tidy["Number"].sum(axis=1)
    tidy[("Amount", "Total")] = tidy["Amount"].sum(axis=1)

    tidy.columns = [" ".join(c).strip() for c in tidy.columns.to_flat_index()]
    return tidy.reset_index()



from typing import Dict, List, Optional
from great_tables import GT, style, loc
import logging

from typing import Dict, List, Optional
from great_tables import GT, style, loc
import logging

# def apply_table_styling(
#     gt: GT,
#     table_type: str,
#     df_columns: Optional[List[str]] = None,
#     params: Optional[Dict[str, str]] = None
# ) -> GT:
#     """
#     Apply consistent styling to great_tables objects based on table type.

#     Args:
#         gt: The great_tables object to style.
#         table_type: Type of table (e.g., 'auri_overview', 'tti_combined', etc.).
#         df_columns: List of column names for the table (optional).
#         params: Dictionary of styling parameters, including colors (optional).

#     Returns:
#         Styled great_tables object.
#     """
#     logger = logging.getLogger(__name__)  # Add this line
  
#     rows_to_style = []

#     # Define column groups based on table type
#     if table_type == "auri_overview":
#         first_col = ["Source"]
#         other_cols = ["Audit results processed", "% Audit results processed", "Audit results pending", "% Audit results pending", "Total"]
#         location_stub =  "Total"
#         location_body = "Total"

#     elif table_type == "tti_combined":
#         first_col = ["Adjustment Type"]
#         other_cols = ["0-6 months", "% total (0-6 months)", "above 6 months", "% above 6 months", "Total"]
#         rows_to_style = ["Closed Projects", "On-going Projects", "Total"]
      

#     elif table_type == "negative_adj":
#         first_col = ["Source"]
#         other_cols = [
#             "Processed No. of AURIs", "Processed Adjustment Amount (AUDEX)",
#             "Pending No. of AURIs", "Pending Adjustment Amount (AUDEX)",
#             "Total No. of AURIs", "Total Adjustment Amount (AUDEX)"
#         ]
#     elif table_type == "deviations":
#         first_col = ["Beneficiary"]
#         other_cols = [
#             "Project Number", "Acronym", "Count of AURI", "AURI Deviation Comment",
#             "Audex Total Cost Adjustment", "Deviation Amount", "AURI Cost Adjustments"
#         ]
#     elif table_type == "participation_impl":
#         first_col = ["Indicator"]
#         if df_columns is None:
#             df_columns = list(gt.collect().columns)
#         other_cols = [c for c in df_columns if c not in first_col]
#         location_stub =  "Total participation implemented"
#         location_body = "Total participation implemented"
    
#     elif table_type == "negative_adj":
#         location_stub =  "Total"
#         location_body = "Total"

#     elif table_type == "negative_adj":
#         location_stub =  ["Closed Projects", "On-going Projects", "Total"]
#         location_body = ["Closed Projects", "On-going Projects", "Total"]

#     elif table_type == "ro_activity":
#         first_col = ["Reason"]
#         other_cols = [c for c in df_columns if c not in first_col + ["Row group"]]
#     else:
#         raise ValueError(f"Unknown table type: {table_type}")

#     gt = gt.opt_table_font(font="Arial").opt_stylize(style=6, color="blue")
    
#     # Only apply the row styling if there are rows to style
#     if rows_to_style:
#         background_color = params.get("subtotal_background_color", '#1f77b4')
        
#         gt = gt.tab_style(
#             style=[
#                 style.fill(color=background_color),
#                 style.text(color="white", weight="bold")
#             ],
#             locations=[
#                 # Apply to the stub cell (e.g., the "Total" label itself)
#                 loc.stub(rows=rows_to_style),
#                 # Apply to the rest of the cells in that row
#                 loc.body(rows=rows_to_style)
#             ]
#         )
#     return gt

def apply_table_styling(
    gt: GT,
    styling_config: Dict[str, Any],
    params: Optional[Dict[str, str]] = None
) -> GT:
    """
    Apply consistent styling to a great_tables object based on a dedicated config.
    """
    # Apply base theme and font
    gt = gt.opt_table_font(font="Arial").opt_stylize(style=6, color="blue")
    
    # Apply styling for specific rows (e.g., "Total" rows)
    rows_to_style = styling_config.get("rows_to_style", [])
    if rows_to_style:
        params = params or {}
        background_color = params.get("subtotal_background_color", '#1f77b4')
        
        gt = gt.tab_style(
            style=[
                style.fill(color=background_color),
                style.text(color="white", weight="bold")
            ],
            locations=[
                loc.stub(rows=rows_to_style),
                loc.body(rows=rows_to_style)
            ]
        )
    
    # --- FINAL, CORRECTED HEADER STYLING ---

    # Step 1: Style the column labels. This works for all tables.
    gt = gt.tab_style(
        style=style.text(weight="bold"),
        locations=loc.column_labels()
    )

    # Step 2: Safely style spanner labels.
    # We use a try/except block to make this robust for tables that don't have spanners.
    try:
        gt = gt.tab_style(
            style=style.text(weight="bold"),
            locations=loc.spanner_labels()
        )
    except Exception:
        # If a table has no spanners, great_tables might raise an error.
        # This block catches it and allows the code to continue safely.
        pass
    
    return gt
    


# Helper function to check if a table has spanners
def has_spanners(gt: GT) -> bool:
    """
    Check if a GT table has spanner labels.
    
    Args:
        gt: The great_tables object
        
    Returns:
        Boolean indicating if spanners exist
    """
    try:
        # Try to access the spanners through various attributes
        if hasattr(gt, '_build_data') and hasattr(gt._build_data, '_spanners'):
            return len(gt._build_data._spanners) > 0
        elif hasattr(gt, '_spanners'):
            return len(gt._spanners) > 0
        else:
            return False
    except:
        return False


# Safe spanner styling function
def style_spanners_safe(gt: GT, fill_color: str, text_color: str = "white") -> GT:
    """
    Safely apply styling to spanner labels if they exist.
    
    Args:
        gt: The great_tables object
        fill_color: Background color for spanners
        text_color: Text color for spanners
        
    Returns:
        Styled GT object
    """
    if has_spanners(gt):
        try:
            gt = gt.tab_style(
                style=[
                    style.fill(color=fill_color),
                    style.text(color=text_color, weight='bold', align="center", size='small'),
                    style.css("padding:5px; line-height:1.2")
                ],
                locations=loc.spanner_labels()
            )
        except Exception as e:
            logging.debug(f"Could not style spanners: {e}")
    
    return gt


# Alternative approach: Create a complete styling function for tables with spanners
def create_styled_table_with_spanners(
    df: pd.DataFrame, 
    table_name: str, 
    spanner_config: Dict[str, Any], 
    report_params: Dict[str, str],
    rowname_col: Optional[str] = None  # <-- 1. ADD THIS ARGUMENT
) -> GT:
    """
    Create a GT table with spanners and apply styling in one go.
    
    Args:
        df: DataFrame to convert to GT
        table_type: Type of table for styling
        spanner_config: Dict with configuration including:
            - 'rowname_col': Column to use as row names (optional)
            - 'groupname_col': Column to use for row groups (optional)
            - 'spanners': List of dicts with spanner configuration
            - 'format_config': List of formatting configurations
        report_params: Styling parameters
        
    Returns:
        Styled GT object
        
    Example usage:
        spanner_config = {
            'spanners': [
                {'label': 'Group 1', 'columns': ['col1', 'col2']},
                {'label': 'Group 2', 'columns': ['col3', 'col4']}
            ],
            'format_config': [
                {'type': 'number', 'columns': ['col1', 'col3'], 'decimals': 0},
                {'type': 'percent', 'columns': ['col2', 'col4'], 'decimals': 1}
            ]
        }
        gt = create_styled_table_with_spanners(df, 'my_table', spanner_config, report_params)
    """
    # Create base GT table with optional parameters
    gt = GT(df,  rowname_col=rowname_col)
   
    # Apply formatting if specified
    format_config = spanner_config.get('format_config', [])
    for fmt in format_config:
        fmt_type = fmt.pop('type')
        if fmt_type == 'number':
            gt = gt.fmt_number(**fmt)
        elif fmt_type == 'percent':
            gt = gt.fmt_percent(**fmt)

    spanners = spanner_config.get('spanners', [])

    for spanner in spanners:
        gt = gt.tab_spanner(label=spanner['label'], columns=spanner['columns'])

    # Apply general table styling
    gt = apply_table_styling(gt, table_type=table_name, params=report_params)

    
    return gt

# def generate_auri_report(
#     conn: sqlite3.Connection,
#     cutoff: pd.Timestamp,
#     alias: str,
#     report: str,
#     db_path: Path,
#     report_params: dict,
#     save_to_db: bool = True,
#     export_dir: Path | None = None
# ) -> dict:
#     """
#     Generate AURI (Audit Result Implementation) report tables and save them to the database if specified.

#     Args:
#         conn: SQLite database connection.
#         cutoff: Timestamp for the reporting cutoff date.
#         alias: Table alias for fetching data (e.g., 'audit_result_implementation').
#         report: Name of the report (e.g., 'Quarterly_Report').
#         db_path: Path to the SQLite database.
#         report_params: Dictionary containing report parameters, including table colors.
#         save_to_db: Whether to save the results to the database.
#         export_dir: Directory path for exporting tables as HTML (optional).

#     Returns:
#         Dictionary containing generated DataFrames and tables.
#     """
#     logger = logging.getLogger("Auri")
#     logger.info("Starting AURI report generation")

#     try:
#         # Initialize constants and parameters
#         months_scope = get_months_in_scope(cutoff)
#         current_year = determine_epoch_year(cutoff) # e.g., current_year = 2025
#         subtotal_background_color = report_params.get("subtotal_background_color", '#1f77b4')

#         # 2. NOW, define the table configurations using the dynamic variables
#         table_configs = {
#             'auri_overview': {
#                 "stub_width": 300,
#                 "summary_row_count": 1
#             },
#             'tti_combined': {
#                 "stub_width": 250,
#                 "group_like_rows": ['Closed Projects', 'On-going Projects'],
#                 "summary_row_count": 1,
#                 "group_row_height": 55  # Tuned value for taller group headers
#             },
#             'negative_adjustments': {
#                 "has_spanners": True,
#                 "stub_width": 150,
#                 "summary_row_count": 1
#             },
#             'deviations': {
#                 "stub_width": 200, 
#                 "base_width_per_column": 110,
#                 "wrap_text_column": "AURI_DEVIATION_COMMENT",
#                 "chars_per_line": 45,
#                 "summary_row_count": 1  # <-- Crucial addition
#             },
#             'implementation_comparison': {
#                 "has_spanners": True,
#                 "stub_width": 250
#             },
#             'recovery_activity': {
#                 "has_spanners": True,
#                 "has_groups": True,
#                 "group_col_name": "Row group",
#                 "stub_width": 200,
#                 "group_header_height": 35, # <-- Tuned value for "ROs Cashed", etc.
#                 "group_row_height": 50,    # <-- Tuned value for "Total RO..." rows
#                 "special_rows_in_group": [
#                     f'Total RO cashed in {current_year}', 
#                     f'Total RO issued in {current_year}'
#                 ]
#             }
#         }

#         # Load and preprocess AURI data
#         auri_df = load_auri_data(db_path, cutoff, current_year, months_scope)
#         if auri_df.empty:
#             logger.warning("No AURI data loaded for the given cutoff and parameters")
#             return {}

#         # Load RO (Recovery Order) data
#         ro_df = fetch_latest_table_data(conn, "c0_ro_yearly_overview", cutoff)
#         if ro_df.empty:
#             logger.warning("No RO data loaded for the given cutoff")

#         # Generate tables
#         # Table 1: AURI Overview
#         auri_overview = auri_overview_df(auri_df)

#         tbl1 = (
#             GT(auri_overview)
#             .fmt_number(columns=['Audit results processed', 'Audit results pending', 'Total'], decimals=0)
#             .fmt_percent(columns=['% Audit results processed', '% Audit results pending'], decimals=1, scale_values=False)
#         )
#         tbl1 = apply_table_styling(tbl1, table_type="auri_overview", params=report_params)

#         # Table 2: TTI Combined (Closed and Open Projects)
#         tti_closed = tti_closed_projects_df(auri_df, current_year)
#         tti_open = tti_open_projects_df(auri_df, current_year)
#         tti_combined = pd.concat([tti_closed, tti_open]).reset_index(drop=True)

#         # Add grand total row
#         subtotal_df = tti_combined[tti_combined['Adjustment Type'].isin(['Closed Projects', 'On-going Projects'])]
#         total_row = pd.DataFrame([{
#             'Adjustment Type': 'Total',
#             '0-6 months': subtotal_df['0-6 months'].sum(),
#             '% total (0-6 months)': (subtotal_df['0-6 months'].sum() / subtotal_df['Total'].sum() * 100) if subtotal_df['Total'].sum() > 0 else 0,
#             'above 6 months': subtotal_df['above 6 months'].sum(),
#             '% above 6 months': (subtotal_df['above 6 months'].sum() / subtotal_df['Total'].sum() * 100) if subtotal_df['Total'].sum() > 0 else 0,
#             'Total': subtotal_df['Total'].sum()
#         }], index=['Grand_Total'])
#         tti_combined = pd.concat([tti_combined, total_row]).reset_index(drop=True)

#         tbl2 = (
#             GT(tti_combined,rowname_col="Adjustment Type")
#             .fmt_number(columns=['0-6 months', 'above 6 months', 'Total'], decimals=0)
#             .fmt_percent(columns=['% total (0-6 months)', '% above 6 months'], decimals=1, scale_values=False)
#         )
#         tbl2 = apply_table_styling(tbl2, table_type="tti_combined", params=report_params)

#         # Table 3: Negative Adjustments
#         negative_adj = negative_adjustments_df(auri_df)
       
#         def get_negative_adjustments_config():
#             """Get spanner configuration for negative adjustments table."""
#             return {
#                 'spanners': [
#                     {
#                         'label': 'Audit results processed',
#                         'columns': ['Processed No. of AURIs', 'Processed Adjustment Amount (AUDEX)']
#                     },
#                     {
#                         'label': 'Audit results pending implementation',
#                         'columns': ['Pending No. of AURIs', 'Pending Adjustment Amount (AUDEX)']
#                     },
#                     {
#                         'label': 'Total Negative Adjustments',
#                         'columns': ['Total No. of AURIs', 'Total Adjustment Amount (AUDEX)']
#                     }
#                 ],
#                 'format_config': [
#                     {
#                         'type': 'number',
#                         'columns': ['Total No. of AURIs', 'Processed No. of AURIs', 'Pending No. of AURIs'],
#                         'decimals': 0
#                     },
#                     {
#                         'type': 'number',
#                         'columns': [
#                             'Total Adjustment Amount (AUDEX)', 
#                             'Processed Adjustment Amount (AUDEX)', 
#                             'Pending Adjustment Amount (AUDEX)'
#                         ],
#                         'decimals': 2,
#                         'use_seps': True
#                     }
#                 ]
#             }

#         spanner_config = get_negative_adjustments_config()
#         # tbl3 = create_styled_table_with_spanners(negative_adj, 'negative_adj', spanner_config, report_params)
#         tbl3 = create_styled_table_with_spanners(
#             negative_adj, 
#             'negative_adj', 
#             spanner_config, 
#             report_params,
#             rowname_col="Source"  # <-- ADD THIS ARGUMENT
#         )
#         # Then add the column labels
#         tbl3 = tbl3.cols_label(
#             Source="Source",
#             **{
#                 "Processed No. of AURIs": "No. of AURIs",
#                 "Processed Adjustment Amount (AUDEX)": "Adjustment Amount (AUDEX)",
#                 "Pending No. of AURIs": "No. of AURIs",
#                 "Pending Adjustment Amount (AUDEX)": "Adjustment Amount (AUDEX)",
#                 "Total No. of AURIs": "No. of AURIs",
#                 "Total Adjustment Amount (AUDEX)": "Adjustment Amount (AUDEX)"
#             }
#         )
      
#         # Table 4: Deviations
#         deviations = deviations_df(auri_df, current_year)
#         tbl4 = (
#             GT(deviations)
#             .fmt_number(columns=['Count of AURI'], decimals=0)
#             .fmt_number(columns=['AUDEX_TOTAL_COST_ADJUSTMENT', 'DEVIATION_AMOUNT', 'AURI_COST_ADJUSTMENTS'], decimals=2, use_seps=True)
#             .cols_label(
#                 BENEFICIARY="Beneficiary",
#                 PROJECT_NUMBER="Project Number",
#                 ACRONYM="Acronym",
#                 AURI_DEVIATION_COMMENT="AURI Deviation Comment",
#                 AUDEX_TOTAL_COST_ADJUSTMENT="Audex Total Cost Adjustment",
#                 DEVIATION_AMOUNT="Deviation Amount",
#                 AURI_COST_ADJUSTMENTS="AURI Cost Adjustments"
#             )
#         )
#         tbl4 = apply_table_styling(tbl4, table_type="deviations", params=report_params)

#         # Table 5: Implementation Comparison
#         impl_comparison = implementation_comparison_df(auri_df, cutoff, months_scope)
#         current_lab = impl_comparison.columns[1].split(" ", 1)[0]
#         prev_lab = impl_comparison.columns[4].split(" ", 1)[0]

#         stub_width=300
#         # Calculate table width
#         base_width_per_column = 80
#         data_columns = impl_comparison.columns[1:].tolist()
#         table_width = f"{stub_width + (len(data_columns) * base_width_per_column)}px"

#         tbl5 = (
#             GT(impl_comparison, rowname_col="Indicator")
#             .tab_options(
#                 table_width=table_width
#             )
#             .fmt_number(columns=[c for c in impl_comparison.columns if c.endswith("number of cases")], decimals=0)
#             .fmt_number(columns=[c for c in impl_comparison.columns if c.endswith("in €")], decimals=2, use_seps=True)
#             .fmt_percent(columns=[c for c in impl_comparison.columns if c.endswith("% cases")], decimals=1, scale_values=False)
#             .tab_spanner(label=current_lab, columns=[c for c in impl_comparison.columns if c.startswith(current_lab)])
#             .tab_spanner(label=prev_lab, columns=[c for c in impl_comparison.columns if c.startswith(prev_lab)])
#         )
#         tbl5 = apply_table_styling(
#             tbl5,
#             table_type="participation_impl",
#             df_columns=list(impl_comparison.columns),
#             params = report_params
#         )

#         # Table 6: Recovery Activity
#         ro_activity = _recovery_activity_df(ro_df, current_year)
#         tbl6 = (
#             GT(ro_activity, rowname_col="Reason", groupname_col="Row group")
#             .fmt_number(columns=[c for c in ro_activity.columns if c.startswith("Number")], decimals=0)
#             .fmt_number(columns=[c for c in ro_activity.columns if c.startswith("Amount")], decimals=2, use_seps=True)
#             .tab_spanner(label="H2020", columns=[c for c in ro_activity.columns if "H2020" in c])
#             .tab_spanner(label="HEU", columns=[c for c in ro_activity.columns if "HEU" in c])
#             .tab_spanner(label="Total", columns=[c for c in ro_activity.columns if "Total" in c])
#         )
#         tbl6 = apply_table_styling(
#             tbl6,
#             table_type="ro_activity",
#             df_columns=list(ro_activity.columns),
#             params = report_params
#         )

#         # Save to database if requested
#         results = {
#             'auri_overview': auri_overview,
#             'tti_combined': tti_combined,
#             'negative_adjustments': negative_adj,
#             'deviations': deviations,
#             'implementation_comparison': impl_comparison,
#             'recovery_activity': ro_activity
#         }
        
#         table_configs = {
#             'auri_overview': {
#                 "stub_width": 300,
#                 "summary_row_count": 1  # <-- ADD THIS LINE
#             },
#             'tti_combined': {
#                 "stub_width": 250,
#                 # NEW: Identify rows that act as group headers
#                 "group_like_rows": ['Closed Projects', 'On-going Projects'],
#                 # NEW: Specify how many summary rows are at the bottom
#                 "summary_row_count": 1 
#             },
#             'negative_adjustments': {"has_spanners": True, "stub_width": 150},
#             'deviations': {
#                 "stub_width": 200, 
#                 "base_width_per_column": 110,
#                 "wrap_text_column": "AURI_DEVIATION_COMMENT",
#                 "chars_per_line": 45  # <-- ADD THIS FOR FINE-TUNING
#             },
#             'implementation_comparison': {"has_spanners": True, "stub_width": 300},
#             'recovery_activity': {
#                 "has_spanners": True,
#                 "has_groups": True,
#                 "group_col_name": "Row group",
#                 "stub_width": 200,
#                 # This now uses the 'current_year' variable to build the strings
#                 "special_rows_in_group": [
#                     f'Total RO cashed in {current_year}', 
#                     f'Total RO issued in {current_year}'
#                 ]
#             }
#         }
#         if save_to_db:
#             tables_to_save = [
#                 ('auri_overview', auri_overview, tbl1),
#                 ('tti_combined', tti_combined, tbl2),
#                 ('negative_adjustments', negative_adj, tbl3),
#                 ('deviations', deviations, tbl4),
#                 ('implementation_comparison', impl_comparison, tbl5),
#                 ('recovery_activity', ro_activity, tbl6)
#             ]
#             for var_name, df_value, gt_table in tables_to_save:
#                 try:
#                     # Step 1: Get the specific configuration for the current table
#                     config = table_configs.get(var_name, {})
                    
#                     # Step 2: Calculate the dimensions in pixels for THIS table
#                     table_width_px, table_height_px = calculate_table_dimensions(df_value, config)
                    
#                     # Step 3: APPLY THE CALCULATED WIDTH to the Great Tables object
#                     # This is the crucial step that was missing.
#                     gt_table = gt_table.tab_options(table_width=f"{table_width_px}px")
                    
#                     logger.debug(f"Saving {var_name} with width={table_width_px}px height={table_height_px}px")
                    
#                     # Step 4: Pass the MODIFIED gt_table and pixel dimensions to the database
#                     insert_variable(
#                         report=report,
#                         module="AuritModule",
#                         var=var_name,
#                         value=df_value.to_dict(orient='records'),
#                         db_path=db_path,
#                         anchor=var_name,
#                         gt_table=gt_table,  # Pass the updated table
#                         simple_gt_save=True,
#                         table_width=table_width_px,
#                         table_height=table_height_px,
#                     )
#                     logger.debug(f"Saved {var_name} to database")
#                 except Exception as e:
#                     logger.error(f"Failed to save {var_name}: {str(e)}", exc_info=True)

#         logger.info("AURI report generation finished successfully.")
#         return results
    
#     except Exception as e:
#             logger.error(f"Failed to save to generate auri reports", exc_info=True)

# In auri_builder.py

def get_negative_adjustments_config():
            """Get spanner configuration for negative adjustments table."""
            return {
                'spanners': [
                    {
                        'label': 'Audit results processed',
                        'columns': ['Processed No. of AURIs', 'Processed Adjustment Amount (AUDEX)']
                    },
                    {
                        'label': 'Audit results pending implementation',
                        'columns': ['Pending No. of AURIs', 'Pending Adjustment Amount (AUDEX)']
                    },
                    {
                        'label': 'Total Negative Adjustments',
                        'columns': ['Total No. of AURIs', 'Total Adjustment Amount (AUDEX)']
                    }
                ],
                'format_config': [
                    {
                        'type': 'number',
                        'columns': ['Total No. of AURIs', 'Processed No. of AURIs', 'Pending No. of AURIs'],
                        'decimals': 0
                    },
                    {
                        'type': 'number',
                        'columns': [
                            'Total Adjustment Amount (AUDEX)', 
                            'Processed Adjustment Amount (AUDEX)', 
                            'Pending Adjustment Amount (AUDEX)'
                        ],
                        'decimals': 2,
                        'use_seps': True
                    }
                ]
            }

# --- FINAL, REFACTORED MAIN FUNCTION ---
def generate_auri_report(
    conn: sqlite3.Connection,
    cutoff: pd.Timestamp,
    alias: str,
    report: str,
    db_path: Path,
    report_params: dict,
    save_to_db: bool = True,
    export_dir: Path | None = None
) -> dict:
    """
    Generate AURI report tables using a robust, definition-driven, and correctly-sized pattern.
    """
    logger = logging.getLogger("Auri")
    logger.info("Starting AURI report generation")

    try:
        # --- 1. Initial Setup ---
        months_scope = get_months_in_scope(cutoff)
        current_year = determine_epoch_year(cutoff)
        
        # --- 2. Load all necessary data once ---
        auri_df = load_auri_data(db_path, cutoff, current_year, months_scope)
        ro_df = fetch_latest_table_data(conn, "c0_ro_yearly_overview", cutoff)

        if auri_df.empty:
            logger.warning("No AURI data loaded; report will be empty.")
            return {}

        # --- 3. THE DEFINITIVE TABLE DEFINITIONS ---
        TABLE_DEFINITIONS = [
            {
                "name": "auri_overview",
                "builder_func": lambda: auri_overview_df(auri_df),
                "gt_kwargs": {"rowname_col": "Source"},
                "styling_config": {"rows_to_style": ["Total"]},
                "sizing_config": {"stub_width": 300, "summary_row_count": 1},
                "formatters": [
                    {"method": "fmt_number", "kwargs": {"columns": ['Audit results processed', 'Audit results pending', 'Total'], "decimals": 0}},
                    {"method": "fmt_percent", "kwargs": {"columns": ['% Audit results processed', '% Audit results pending'], "decimals": 1, "scale_values": False}}
                ]
            },
            {
                "name": "tti_combined",
                "builder_func": lambda: build_tti_combined_df(auri_df, current_year), # Use new builder
                "gt_kwargs": {"rowname_col": "Adjustment Type"},
                "styling_config": {"rows_to_style": ["Closed Projects", "On-going Projects", "Total"]},
                 "sizing_config": {
                    "stub_width": 250,
                    "group_like_rows": ['Closed Projects', 'On-going Projects'],
                    "summary_row_count": 1,
                    "group_row_height": 60,
                    "summary_row_height": 60  # <-- INCREASE THIS VALUE FROM 50 to 60
                },
                "formatters": [
                    {"method": "fmt_number", "kwargs": {"columns": ['0-6 months', 'above 6 months', 'Total'], "decimals": 0}},
                    {"method": "fmt_percent", "kwargs": {"columns": ['% total (0-6 months)', '% above 6 months'], "decimals": 1, "scale_values": False}}
                ]
            },
           {
                "name": "negative_adjustments",
                "builder_func": lambda: negative_adjustments_df(auri_df),
                "gt_kwargs": {"rowname_col": "Source"},
                "styling_config": {"rows_to_style": ["Total"]},
                "sizing_config": {"has_spanners": True, "stub_width": 150, "summary_row_count": 1, "total_width_override": 1400},
                # FIX: Spanner logic is now handled by the generic spanner key below
                "spanners": get_negative_adjustments_config().get('spanners', []),
                # FIX: Formatting is now handled by the generic formatters key
                "formatters": get_negative_adjustments_config().get('format_config', [])
            },
            {
                "name": "deviations",
                "builder_func": lambda: deviations_df(auri_df, current_year),
                "gt_kwargs": {"rowname_col": "BENEFICIARY"},
                "styling_config": {"rows_to_style": ["TOTAL"]},
                 "sizing_config": {
                        "stub_width": 200, 
                        "base_width_per_column": 120,
                        "wrap_text_column": "AURI_DEVIATION_COMMENT",
                        "chars_per_line": 38,
                        "line_height": 24,
                        "row_height": 50,
                        "summary_row_count": 1
                    },
                "formatters": [
                    {"method": "fmt_number", "kwargs": {"columns": ['Count of AURI'], "decimals": 0}},
                    {"method": "fmt_number", "kwargs": {"columns": ['AUDEX_TOTAL_COST_ADJUSTMENT', 'DEVIATION_AMOUNT', 'AURI_COST_ADJUSTMENTS'], "decimals": 2, "use_seps": True}}
                ],
                "rename_cols_from_snake_case": True  # <-- ADD THIS NEW KEY
            },
             {
                "name": "implementation_comparison",
                "builder_func": lambda: implementation_comparison_df(auri_df, cutoff, months_scope),
                "gt_kwargs": {"rowname_col": "Indicator"},
                "styling_config": {"rows_to_style": ["Total participation implemented"]},
                "sizing_config": {
                    "has_spanners": True, "stub_width": 350, "summary_row_count": 1, 
                    "base_width_per_column": 100, 
                    "summary_row_height": 50 # FIX: Increase height for the bolded total row
                },
                # FIX: Define spanners here so they can be styled
                "spanners": lambda df: [
                    {'label': df.columns[1].split(" ", 1)[0], 'columns': [c for c in df.columns if c.startswith(df.columns[1].split(" ", 1)[0])]},
                    {'label': df.columns[4].split(" ", 1)[0], 'columns': [c for c in df.columns if c.startswith(df.columns[4].split(" ", 1)[0])]}
                ],
                "formatters": [
                    {"method": "fmt_number", "kwargs": {"columns": lambda df: [c for c in df.columns if c.endswith("number of cases")], "decimals": 0}},
                    {"method": "fmt_number", "kwargs": {"columns": lambda df: [c for c in df.columns if c.endswith("in €")], "decimals": 2, "use_seps": True}},
                    {"method": "fmt_percent", "kwargs": {"columns": lambda df: [c for c in df.columns if c.endswith("% cases")], "decimals": 1, "scale_values": False}}
                ]
            },
            {
                "name": "recovery_activity",
                "builder_func": lambda: _recovery_activity_df(ro_df, current_year),
                "gt_kwargs": {"rowname_col": "Reason", "groupname_col": "Row group"},
                "styling_config": {"rows_to_style": [f'Total RO cashed in {current_year}', f'Total RO issued in {current_year}']},
                "sizing_config": {"has_spanners": True, "has_groups": True, "group_col_name": "Row group", "stub_width": 200, "group_header_height": 45, "group_row_height": 55, "normal_row_height": 45, "special_rows_in_group": [f'Total RO cashed in {current_year}', f'Total RO issued in {current_year}']},
                 # FIX: Define spanners here so they can be styled
                "spanners": [
                    {'label': "H2020", 'columns': lambda df: [c for c in df.columns if "H2020" in c]},
                    {'label': "HEU", 'columns': lambda df: [c for c in df.columns if "HEU" in c]},
                    {'label': "Total", 'columns': lambda df: [c for c in df.columns if "Total" in c]}
                ],
                "formatters": [
                    {"method": "fmt_number", "kwargs": {"columns": lambda df: [c for c in df.columns if c.startswith("Number")], "decimals": 0}},
                    {"method": "fmt_number", "kwargs": {"columns": lambda df: [c for c in df.columns if c.startswith("Amount")], "decimals": 2, "use_seps": True}}
                ]
            }
        ]
 
        # --- 4. Process all tables using their definitions ---
        results = {}
        if save_to_db:
            for definition in TABLE_DEFINITIONS:
                var_name = definition["name"]
                logger.info(f"Processing table: {var_name}")
                try:
                    df_value = definition["builder_func"]()
                    results[var_name] = df_value
                    
                    gt_table = GT(df_value, **definition.get("gt_kwargs", {}))

                    # FIX: Apply spanners BEFORE styling
                    spanners = definition.get("spanners", [])
                    if callable(spanners):
                        spanners = spanners(df_value)
                    for spanner in spanners:
                        # Handle dynamic column selectors in spanners
                        if callable(spanner.get('columns')):
                            spanner['columns'] = spanner['columns'](df_value)
                        gt_table = gt_table.tab_spanner(label=spanner['label'], columns=spanner['columns'])

                    # Apply formatters
                    formatters = definition.get("formatters", [])
                    for rule in formatters:
                        # Handle type for negative_adjustments special case
                        if rule.get('type'):
                            method_name = f"fmt_{rule['type']}"
                            kwargs = {k: v for k, v in rule.items() if k != 'type'}
                        else:
                            method_name = rule["method"]
                            kwargs = rule["kwargs"].copy()

                        if callable(kwargs.get("columns")):
                            kwargs["columns"] = kwargs["columns"](df_value)
                        gt_table = getattr(gt_table, method_name)(**kwargs)
                    
                    # Special column labeling for negative_adjustments
                    if var_name == 'negative_adjustments':
                         gt_table = gt_table.cols_label(Source="Source", **{"Processed No. of AURIs": "No. of AURIs", "Processed Adjustment Amount (AUDEX)": "Adjustment Amount (AUDEX)", "Pending No. of AURIs": "No. of AURIs", "Pending Adjustment Amount (AUDEX)": "Adjustment Amount (AUDEX)", "Total No. of AURIs": "No. of AURIs", "Total Adjustment Amount (AUDEX)": "Adjustment Amount (AUDEX)"})

                    # Rename columns from snake_case if requested
                    if definition.get("rename_cols_from_snake_case"):
                        rename_dict = {col: col.replace('_', ' ').title() for col in df_value.columns}
                        gt_table = gt_table.cols_label(**rename_dict)

                    # Apply common styling (now that spanners exist)
                    gt_table = apply_table_styling(gt_table, definition["styling_config"], report_params)
                    
                    # Calculate dimensions and apply width
                    table_width_px, table_height_px = calculate_table_dimensions(df_value, definition.get("sizing_config", {}))
                    gt_table = gt_table.tab_options(table_width=f"{table_width_px}px")
                    
                    insert_variable(
                        report=report, module="AuritModule", var=var_name,
                        value=df_value.to_dict(orient='records'), db_path=db_path, anchor=var_name,
                        gt_table=gt_table, simple_gt_save=True,
                        table_width=table_width_px, table_height=table_height_px
                    )
                except Exception as e:
                    logger.error(f"Failed to process and save table '{var_name}': {e}", exc_info=True)

        logger.info("AURI report generation finished successfully.")
        return results
    
    except Exception as e:
        logger.error(f"A fatal error occurred during report generation: {e}", exc_info=True)
        return {}