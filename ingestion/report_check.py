# file: ingestion/report_check.py  (new utility)

from __future__ import annotations
import sqlite3
from datetime import datetime, date
import pandas as pd
from ingestion.db_utils import get_expected_tables


def check_report_readiness(
    report_name: str,
    cutoff: date | datetime,
    tolerance_days: int,
    db_path: str = "database/reporting.db",
) -> tuple[pd.DataFrame, bool]:
    """
    Returns a dataframe with columns:
        Required Table Alias | Status | Last Upload
    and a bool `is_ready` telling if every required alias
    is present and fresh (>= cutoff – tolerance_days).

    No side-effects, no Streamlit – pure logic.
    """
    cutoff_dt = pd.to_datetime(cutoff) - pd.Timedelta(days=tolerance_days)

    expected = get_expected_tables(report_name, db_path)

    with sqlite3.connect(db_path) as conn:
        uploaded_df = pd.read_sql_query(
            """
            SELECT table_alias, MAX(uploaded_at) AS last_uploaded
            FROM upload_log
            WHERE report_name = ?
            GROUP BY table_alias
            """,
            conn,
            params=(report_name,),
        )

    uploaded = dict(zip(uploaded_df["table_alias"], uploaded_df["last_uploaded"]))

    rows: list[dict] = []
    is_ready = True

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
                is_ready = False
        else:
            rows.append(
                {
                    "Required Table Alias": alias,
                    "Status": "❌ Missing",
                    "Last Upload": "-",
                }
            )
            is_ready = False

    return pd.DataFrame(rows), is_ready
