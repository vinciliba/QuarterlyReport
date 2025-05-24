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
OUTLINE_B = '2px'


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


from great_tables import GT, style, loc
from typing import List, Dict, Optional

def apply_table_styling(
    gt: GT,
    table_type: str,
    df_columns: Optional[List[str]] = None,
    params: Optional[Dict[str, str]] = None
) -> GT:
    """
    Apply consistent styling to great_tables objects based on table type.

    Args:
        gt: The great_tables object to style.
        table_type: Type of table (e.g., 'auri_overview', 'tti_combined', etc.).
        df_columns: List of column names for the table (optional).
        params: Dictionary of styling parameters, including colors (optional).

    Returns:
        Styled great_tables object.
    """
    # Default colors if params is not provided or missing keys
    default_colors = {
        "BLUE": "#004A99",
        "LIGHT_BLUE": "#d6e6f4",
        "DARK_BLUE": "#01244B",
        "subtotal_background_color": "#E6E6FA"
    }
    
    # Use params if provided, otherwise fall back to defaults
    colors = params if params is not None else default_colors
    BLUE = colors.get("BLUE", default_colors["BLUE"])
    LIGHT_BLUE = colors.get("LIGHT_BLUE", default_colors["LIGHT_BLUE"])
    DARK_BLUE = colors.get("DARK_BLUE", default_colors["DARK_BLUE"])
    subtotal_background_color = colors.get("subtotal_background_color", default_colors["subtotal_background_color"])

    OUTLINE_B = '2px'

    # Define column groups based on table type
    if table_type == "auri_overview":
        first_col = ["Source"]
        other_cols = ["Audit results processed", "% Audit results processed", "Audit results pending", "% Audit results pending", "Total"]
    elif table_type == "tti_combined":
        first_col = ["Adjustment Type"]
        other_cols = ["0-6 months", "% total (0-6 months)", "above 6 months", "% above 6 months", "Total"]
    elif table_type == "negative_adj":
        first_col = ["Source"]
        other_cols = [
            "Processed No. of AURIs", "Processed Adjustment Amount (AUDEX)",
            "Pending No. of AURIs", "Pending Adjustment Amount (AUDEX)",
            "Total No. of AURIs", "Total Adjustment Amount (AUDEX)"
        ]
    elif table_type == "deviations":
        first_col = ["Beneficiary"]
        other_cols = [
            "Project Number", "Acronym", "Count of AURI", "AURI Deviation Comment",
            "Audex Total Cost Adjustment", "Deviation Amount", "AURI Cost Adjustments"
        ]
    elif table_type == "participation_impl":
        first_col = ["Indicator"]
        if df_columns is None:
            df_columns = list(gt.collect().columns)
        other_cols = [c for c in df_columns if c not in first_col]
    elif table_type == "ro_activity":
        first_col = ["Reason"]
        other_cols = [c for c in df_columns if c not in first_col + ["Row group"]]
    else:
        raise ValueError(f"Unknown table type: {table_type}")

    # Apply styling
    gt = (
        gt
        .opt_table_font(font="Arial")
        .opt_table_outline(style="solid", width=OUTLINE_B, color=DARK_BLUE)

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
                style.text(color="white", weight="bold", align="left", size='small'),
                style.css("min-width:50px; padding:5px; line-height:1.2")
            ],
            locations=loc.column_labels(columns=first_col)
        )
        .tab_style(
            style=[
                style.fill(color=BLUE),
                style.text(color="white", weight="bold", align="right", size='small'),
                style.css("min-width:50px; padding:5px; line-height:1.2")
            ],
            locations=loc.column_labels(columns=other_cols)
        )

        # Stubhead styling
        .tab_style(
            style=[
                style.fill(color=BLUE),
                style.text(color="white", weight="bold", align="center", size='small'),
                style.css("min-width:150px; padding:20px; line-height:1.2")
            ],
            locations=loc.stubhead()
        )

        # Body cell styling
        .tab_style(
            style=[
                style.borders(sides="all", color=DARK_BLUE, weight="1px"),
                style.text(align="left", size='small', font='Arial'),
                style.css("padding:5px")
            ],
            locations=loc.body(columns=first_col)
        )
        .tab_style(
            style=[
                style.borders(sides="all", color=DARK_BLUE, weight="1px"),
                style.text(align="right", size='small', font='Arial'),
                style.css("padding:5px")
            ],
            locations=loc.body(columns=other_cols)
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

        # Table options
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

        # Footer styling
        .tab_source_note("Source: Compass")
        .tab_source_note("Report: Audit Result Implementation")
        .tab_style(
            style=[
                style.text(size="small", font='Arial'),
                style.css("padding:5px; line-height:1.2")
            ],
            locations=loc.footer()
        )
    )

    # Additional styling for specific table types
    if table_type == "tti_combined":
        gt = gt.tab_style(
            style=[
                style.fill(color=subtotal_background_color),
                style.text(weight="bold")
            ],
            locations=[
                loc.stub(rows=["Closed Projects", "On-going Projects", "Total"]),
                loc.body(rows=["Closed Projects", "On-going Projects", "Total"])
            ]
        )
    elif table_type == "negative_adj":
        gt = gt.tab_style(
            style=[
                style.fill(color=BLUE),
                style.text(color="white", weight="bold")
            ],
            locations=[
                loc.stub(rows=["Total"]),
                loc.body(rows=["Total"])
            ]
        )
    elif table_type == "participation_impl":
        gt = gt.tab_style(
            style=[
                style.fill(color=BLUE),
                style.text(color="white", weight="bold")
            ],
            locations=[
                loc.stub(rows=["Total participation implemented"]),
                loc.body(rows=["Total participation implemented"])
            ]
        )
    elif table_type == "deviations":
        # Style the "Total" row to match column headers (BLUE background, white bold text)
        gt = gt.tab_style(
            style=[
                style.fill(color=BLUE),
                style.text(color="white", weight="bold")
            ],
            locations=[
                loc.stub(rows=["TOTAL"]),
                loc.body(rows=["TOTAL"])
            ]
        )

        # Wrap content in "AURI Deviation Comment" column
        gt = gt.tab_style(
            style=[
                style.css("white-space: normal; word-wrap: break-word; min-width: 200px; max-width: 300px;")
            ],
            locations=loc.body(columns=["AURI Deviation Comment"])
        )

        # Set stub cells (Beneficiary column, excluding stubhead) to LIGHT_BLUE
        gt = gt.tab_style(
            style=[
                style.fill(color=LIGHT_BLUE)
            ],
            locations=loc.stub()
        )

    return gt
    # Apply subtotal background color to specific rows in tti_combined table


    if table_type == "tti_combined":
        gt = gt.tab_style(
            style=[
                style.fill(color=SUB_TOTAL_BACKGROUND),
                style.text(weight="bold")
            ],
            locations=[
                loc.stub(rows=["Closed Projects", "On-going Projects", "Total"]),
                loc.body(rows=["Closed Projects", "On-going Projects", "Total"])
            ]
        )

    # Apply subtotal background color to specific rows in tti_combined table
    elif table_type == "negative_adj":
        # Apply styling to the Total row to match the column headers and spanners
        gt = gt.tab_style(
            style=[
                style.fill(color=BLUE),
                style.text(color="white", weight="bold")
            ],
            locations=[
                loc.stub(rows=["Total"]),
                loc.body(rows=["Total"])
            ]
        )
    elif table_type == "auri_overview":
        # Apply styling to the Total row to match the column headers and spanners
        gt = gt.tab_style(
            style=[
                style.fill(color=BLUE),
                style.text(color="white", weight="bold")
            ],
            locations=[
                loc.stub(rows=["Total"]),
                loc.body(rows=["Total"])
            ]
        )
    elif table_type == "participation_impl":
        # Apply styling to the Total row to match the column headers and spanners
        gt = gt.tab_style(
            style=[
                style.fill(color=BLUE),
                style.text(color="white", weight="bold")
            ],
            locations=[
                loc.stub(rows=["Total participation implemented"]),
                loc.body(rows=["Total participation implemented"])
            ]
        )

    return gt

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
    Generate AURI (Audit Result Implementation) report tables and save them to the database if specified.

    Args:
        conn: SQLite database connection.
        cutoff: Timestamp for the reporting cutoff date.
        alias: Table alias for fetching data (e.g., 'audit_result_implementation').
        report: Name of the report (e.g., 'Quarterly_Report').
        db_path: Path to the SQLite database.
        report_params: Dictionary containing report parameters, including table colors.
        save_to_db: Whether to save the results to the database.
        export_dir: Directory path for exporting tables as HTML (optional).

    Returns:
        Dictionary containing generated DataFrames and tables.
    """
    logger = logging.getLogger("Auri")
    logger.info("Starting AURI report generation")

    try:
        # Initialize constants and parameters
        months_scope = get_months_in_scope(cutoff)
        current_year = determine_epoch_year(cutoff)
        subtotal_background_color = report_params.get("subtotal_background_color", "#E6E6FA")

        # Load and preprocess AURI data
        auri_df = load_auri_data(db_path, cutoff, current_year, months_scope)
        if auri_df.empty:
            logger.warning("No AURI data loaded for the given cutoff and parameters")
            return {}

        # Load RO (Recovery Order) data
        ro_df = fetch_latest_table_data(conn, "c0_ro_yearly_overview", cutoff)
        if ro_df.empty:
            logger.warning("No RO data loaded for the given cutoff")

        # Generate tables
        # Table 1: AURI Overview
        auri_overview = auri_overview_df(auri_df)
        tbl1 = (
            GT(auri_overview)
            .fmt_number(columns=['Audit results processed', 'Audit results pending', 'Total'], decimals=0)
            .fmt_percent(columns=['% Audit results processed', '% Audit results pending'], decimals=1, scale_values=False)
        )
        tbl1 = apply_table_styling(tbl1, table_type="auri_overview", params=report_params)

        # Table 2: TTI Combined (Closed and Open Projects)
        tti_closed = tti_closed_projects_df(auri_df, current_year)
        tti_open = tti_open_projects_df(auri_df, current_year)
        tti_combined = pd.concat([tti_closed, tti_open]).reset_index(drop=True)

        # Add grand total row
        subtotal_df = tti_combined[tti_combined['Adjustment Type'].isin(['Closed Projects', 'On-going Projects'])]
        total_row = pd.DataFrame([{
            'Adjustment Type': 'Total',
            '0-6 months': subtotal_df['0-6 months'].sum(),
            '% total (0-6 months)': (subtotal_df['0-6 months'].sum() / subtotal_df['Total'].sum() * 100) if subtotal_df['Total'].sum() > 0 else 0,
            'above 6 months': subtotal_df['above 6 months'].sum(),
            '% above 6 months': (subtotal_df['above 6 months'].sum() / subtotal_df['Total'].sum() * 100) if subtotal_df['Total'].sum() > 0 else 0,
            'Total': subtotal_df['Total'].sum()
        }], index=['Grand_Total'])
        tti_combined = pd.concat([tti_combined, total_row]).reset_index(drop=True)

        tbl2 = (
            GT(tti_combined)
            .fmt_number(columns=['0-6 months', 'above 6 months', 'Total'], decimals=0)
            .fmt_percent(columns=['% total (0-6 months)', '% above 6 months'], decimals=1, scale_values=False)
        )
        tbl2 = apply_table_styling(tbl2, table_type="tti_combined", params=report_params)

        # Table 3: Negative Adjustments
        negative_adj = negative_adjustments_df(auri_df)
        tbl3 = (
            GT(negative_adj)
            .fmt_number(columns=[
                'Total No. of AURIs', 'Processed No. of AURIs', 'Pending No. of AURIs'
            ], decimals=0)
            .fmt_number(columns=[
                'Total Adjustment Amount (AUDEX)', 'Processed Adjustment Amount (AUDEX)', 'Pending Adjustment Amount (AUDEX)'
            ], decimals=2, use_seps=True)
            .tab_spanner(
                label="Audit results processed",
                columns=["Processed No. of AURIs", "Processed Adjustment Amount (AUDEX)"]
            )
            .tab_spanner(
                label="Audit results pending implementation",
                columns=["Pending No. of AURIs", "Pending Adjustment Amount (AUDEX)"]
            )
            .tab_spanner(
                label="Total Negative Adjustments",
                columns=["Total No. of AURIs", "Total Adjustment Amount (AUDEX)"]
            )
            .cols_label(
                Source="Source",
                **{
                    "Processed No. of AURIs": "No. of AURIs",
                    "Processed Adjustment Amount (AUDEX)": "Adjustment Amount (AUDEX)",
                    "Pending No. of AURIs": "No. of AURIs",
                    "Pending Adjustment Amount (AUDEX)": "Adjustment Amount (AUDEX)",
                    "Total No. of AURIs": "No. of AURIs",
                    "Total Adjustment Amount (AUDEX)": "Adjustment Amount (AUDEX)"
                }
            )
        )
        tbl3 = apply_table_styling(tbl3, table_type="negative_adj", params=report_params)

        # Table 4: Deviations
        deviations = deviations_df(auri_df, current_year)
        tbl4 = (
            GT(deviations)
            .fmt_number(columns=['Count of AURI'], decimals=0)
            .fmt_number(columns=['AUDEX_TOTAL_COST_ADJUSTMENT', 'DEVIATION_AMOUNT', 'AURI_COST_ADJUSTMENTS'], decimals=2, use_seps=True)
            .cols_label(
                BENEFICIARY="Beneficiary",
                PROJECT_NUMBER="Project Number",
                ACRONYM="Acronym",
                AURI_DEVIATION_COMMENT="AURI Deviation Comment",
                AUDEX_TOTAL_COST_ADJUSTMENT="Audex Total Cost Adjustment",
                DEVIATION_AMOUNT="Deviation Amount",
                AURI_COST_ADJUSTMENTS="AURI Cost Adjustments"
            )
        )
        tbl4 = apply_table_styling(tbl4, table_type="deviations", params=report_params)

        # Table 5: Implementation Comparison
        impl_comparison = implementation_comparison_df(auri_df, cutoff, months_scope)
        current_lab = impl_comparison.columns[1].split(" ", 1)[0]
        prev_lab = impl_comparison.columns[4].split(" ", 1)[0]
        tbl5 = (
            GT(impl_comparison, rowname_col="Indicator")
            .fmt_number(columns=[c for c in impl_comparison.columns if c.endswith("number of cases")], decimals=0)
            .fmt_number(columns=[c for c in impl_comparison.columns if c.endswith("in €")], decimals=2, use_seps=True)
            .fmt_percent(columns=[c for c in impl_comparison.columns if c.endswith("% cases")], decimals=1, scale_values=False)
            .tab_spanner(label=current_lab, columns=[c for c in impl_comparison.columns if c.startswith(current_lab)])
            .tab_spanner(label=prev_lab, columns=[c for c in impl_comparison.columns if c.startswith(prev_lab)])
        )
        tbl5 = apply_table_styling(
            tbl5,
            table_type="participation_impl",
            df_columns=list(impl_comparison.columns),
            params = report_params
        )

        # Table 6: Recovery Activity
        ro_activity = _recovery_activity_df(ro_df, current_year)
        tbl6 = (
            GT(ro_activity, rowname_col="Reason", groupname_col="Row group")
            .fmt_number(columns=[c for c in ro_activity.columns if c.startswith("Number")], decimals=0)
            .fmt_number(columns=[c for c in ro_activity.columns if c.startswith("Amount")], decimals=2, use_seps=True)
            .tab_spanner(label="H2020", columns=[c for c in ro_activity.columns if "H2020" in c])
            .tab_spanner(label="HEU", columns=[c for c in ro_activity.columns if "HEU" in c])
            .tab_spanner(label="Total", columns=[c for c in ro_activity.columns if "Total" in c])
        )
        tbl6 = apply_table_styling(
            tbl6,
            table_type="ro_activity",
            df_columns=list(ro_activity.columns),
            params = report_params
        )

        # Save to database if requested
        results = {
            'auri_overview': auri_overview,
            'tti_combined': tti_combined,
            'negative_adjustments': negative_adj,
            'deviations': deviations,
            'implementation_comparison': impl_comparison,
            'recovery_activity': ro_activity
        }

        if save_to_db:
            for var_name, value, table in [
                ('auri_overview', auri_overview, tbl1),
                ('tti_combined', tti_combined, tbl2),
                ('negative_adjustments', negative_adj, tbl3),
                ('deviations', deviations, tbl4),
                ('implementation_comparison', impl_comparison, tbl5),
                ('recovery_activity', ro_activity, tbl6)
            ]:
                try:
                    logger.debug(f"Saving {var_name} to database")
                    insert_variable(
                        report=report,
                        module="AuritModule",
                        var=var_name,
                        value=value.to_dict() if isinstance(value, pd.DataFrame) else value,
                        db_path=db_path,
                        anchor=var_name,
                        gt_table=table
                    )
                    logger.debug(f"Saved {var_name} to database")
                except Exception as e:
                    logger.error(f"Failed to save {var_name}: {str(e)}")

        # Export to HTML files if export_dir is provided
        if export_dir:
            export_dir.mkdir(parents=True, exist_ok=True)
            for var_name, table in [
                ('auri_overview', tbl1),
                ('tti_combined', tbl2),
                ('negative_adjustments', tbl3),
                ('deviations', tbl4),
                ('implementation_comparison', tbl5),
                ('recovery_activity', tbl6)
            ]:
                try:
                    output_path = export_dir / f"{var_name}.html"
                    table.to_html(output_path)
                    logger.debug(f"Exported {var_name} to {output_path}")
                except Exception as e:
                    logger.error(f"Failed to export {var_name}: {str(e)}")

        logger.info("AURI report generation complete")
        return results

    except Exception as e:
        logger.error(f"Error in generate_amendments_report: {str(e)}", exc_info=True)
        raise