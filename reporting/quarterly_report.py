# file: reporting/quarterly_report.py
"""
Quarterly Report generator.

This module **does not use Streamlit**.  It provides one public function
`run_report()` that performs:

1.  Validation – all required table_aliases uploaded and fresh
2.  Report-generation logic (placeholder)

Returns
-------
Tuple[bool, str]
    (True, "success message") on success
    (False, "reason / error") on failure
"""
from __future__ import annotations

import sqlite3
from datetime import datetime, date
import pandas as pd
from ingestion.db_utils import get_expected_tables


DB_PATH_DEFAULT = "database/reporting.db"
REPORT_NAME = "Quarterly_Report"


# ----------------------------------------------------------------------
# helpers
# ----------------------------------------------------------------------
def _validate_uploads(
    report_name: str,
    cutoff_date: date | datetime,
    tolerance_days: int,
    db_path: str,
) -> tuple[pd.DataFrame, bool]:
    """Return validation dataframe + ready flag (internal use)."""
    cutoff_dt = pd.to_datetime(cutoff_date) - pd.Timedelta(days=tolerance_days)
    expected = get_expected_tables(report_name, db_path)

    if not expected:
        # No strict requirements means always ready
        return pd.DataFrame(
            [{"Required Table Alias": "(none)", "Status": "⚠️ No requirements", "Last Upload": "-"}]
        ), True

    with sqlite3.connect(db_path) as conn:
        uploaded_df = pd.read_sql_query(
            """
            SELECT table_alias, MAX(uploaded_at) as last_uploaded
            FROM upload_log
            WHERE report_name = ?
            GROUP BY table_alias
            """,
            conn,
            params=(report_name,),
        )

    uploaded = dict(zip(uploaded_df["table_alias"], uploaded_df["last_uploaded"]))

    rows: list[dict] = []
    ready = True

    for alias in expected:
        if alias in uploaded:
            ts = pd.to_datetime(uploaded[alias])
            if ts >= cutoff_dt:
                rows.append(
                    {
                        "Required Table Alias": alias,
                        "Status": "✅ Fresh Upload",
                        "Last Upload": ts.strftime("%Y-%m-%d %H:%M"),
                    }
                )
            else:
                rows.append(
                    {
                        "Required Table Alias": alias,
                        "Status": "⚠️ Too Old",
                        "Last Upload": ts.strftime("%Y-%m-%d %H:%M"),
                    }
                )
                ready = False
        else:
            rows.append(
                {
                    "Required Table Alias": alias,
                    "Status": "❌ Missing",
                    "Last Upload": "-",
                }
            )
            ready = False

    return pd.DataFrame(rows), ready


# ----------------------------------------------------------------------
# public entry-point
# ----------------------------------------------------------------------
def run_report(
    cutoff_date: date | datetime | None = None,
    tolerance_days: int = 3,
    db_path: str = DB_PATH_DEFAULT,
) -> tuple[bool, str]:
    """
    Execute Quarterly Report.

    Parameters
    ----------
    cutoff_date : date | datetime
        The reporting cutoff.  If None, uses today.
    tolerance_days : int
        Uploads must be within this many days before cutoff.
    db_path : str
        Path to SQLite db.

    Returns
    -------
    (ok: bool, message: str)
    """
    cutoff_date = cutoff_date or datetime.today().date()

    # 1) Validate uploads
    _, ready = _validate_uploads(REPORT_NAME, cutoff_date, tolerance_days, db_path)
    if not ready:
        return False, "Missing or outdated uploads – report aborted."

    # 2) ---------- YOUR REAL REPORT LOGIC HERE ----------
    try:
        # Example: fetch some tables, do calculations, save XLSX/PDF etc.
        # with sqlite3.connect(db_path) as conn:
        #     df_calls = pd.read_sql_query("SELECT * FROM call_overview", conn)
        #     ...
        # pretend work:
        import time

        time.sleep(1)  # simulate heavy work
        # --------------------------------------------------

        return True, "Quarterly report generated successfully."
    except Exception as exc:
        return False, f"Report generation failed: {exc}"
