from __future__ import annotations

import logging, sqlite3, datetime
from pathlib import Path
from typing import List
import numpy as np
import pandas as pd
from great_tables import GT, loc, style, html
from ingestion.db_utils import (
    fetch_latest_table_data,
    insert_variable,
)
from reporting.quarterly_report.utils import RenderContext, BaseModule
from reporting.quarterly_report.report_utils.granting_utils import enrich_grants, _ensure_timedelta_cols, _coerce_date_columns
from ingestion.db_utils import load_report_params
from reporting.quarterly_report.report_utils.granting_m_builder import process_granting_data, build_signatures_table,build_commitments_table, build_po_exceeding_FDI_tb_3c
# Constants
CALL_OVERVIEW_ALIAS = "call_overview"
BUDGET_FOLLOWUP_ALIAS = "budget_follow_up_report"
PO_SUMMA_ALIAS = "c0_po_summa"
ETHICS_ALIAS = "ethics_requirements_and_issues"
EXCLUDE_TOPICS = [
    "ERC-2023-SJI-1",
    "ERC-2023-SJI",
    "ERC-2024-PERA",
    "HORIZON-ERC-2022-VICECHAIRS-IBA",
    "HORIZON-ERC-2023-VICECHAIRS-IBA",
]
MONTHS_ORDER = list(pd.date_range("2000-01-01", periods=12, freq="ME").strftime("%B"))


def months_in_scope(cutoff: pd.Timestamp) -> list[str]:
    """
    Return month-names from January up to the **last month that ended
    *before* the cut-off month**.

    • cut-off 15-Apr-2025 → Jan Feb Mar
    • cut-off 1-May-2025 → Jan … Apr
    """
    first_day_of_cutoff = cutoff.replace(day=1)
    last_full_month = first_day_of_cutoff - pd.offsets.MonthBegin()  # One month earlier

    months = pd.date_range(
        start=pd.Timestamp(year=cutoff.year, month=1, day=1),
        end=last_full_month,
        freq="MS",
    ).strftime("%B").tolist()

    return months

class GrantsModule(BaseModule):
    """
    GAP (“Granting”) KPIs and state-of-play tables.

    Anchors written to DB
    ----------------------
    • grants_raw_df
    • kpi_table
    • state_of_play
    • signatures_tab3
    • commitments_eur_tab4
    • commitments_n_tab4
    • table_3a_signatures_data
    • table_3b_commitments_data
    """

    name = "Granting"
    description = "Granting statistics / KPI / GAP state"

    def run(self, ctx: RenderContext) -> RenderContext:
        log = logging.getLogger(self.name)
        conn = ctx.db.conn
        cutoff = pd.to_datetime(ctx.cutoff)
        db_path = Path(conn.execute("PRAGMA database_list").fetchone()[2])
        report = ctx.report_name

        # Load report parameters
        report_params = load_report_params(report_name=report, db_path=db_path)
        table_colors = report_params.get("TABLE_COLORS", {})
        df_summa = fetch_latest_table_data(conn, PO_SUMMA_ALIAS, cutoff)

        # Determine scope months dynamically
        scope_months = months_in_scope(cutoff)
        log.debug(f"Scope months for cutoff {cutoff}: {scope_months}")

        # Toggle for saving to DB or exporting
        SAVE_TO_DB = False  # Switch to True when ready
        EXPORT_DIR = Path("exports")

        # Process the data
        results = process_granting_data(
            conn=conn,
            cutoff=cutoff,
            report=report,
            db_path=db_path,
            report_params=report_params,
            save_to_db=SAVE_TO_DB,
            export_dir=EXPORT_DIR
        )
        # # Unpack results from process_granting_data
        df_grants = results["df_grants"]
  
        # Build signatures table
        build_signatures_table(
            df=df_grants,
            cutoff=cutoff,
            scope_months=scope_months,
            exclude_topics=EXCLUDE_TOPICS,
            report=report,
            db_path=str(db_path),
            table_colors=table_colors
        )
        # Build commitments table
        build_commitments_table(
            df=df_grants,
            cutoff=cutoff,
            scope_months=scope_months,
            exclude_topics=EXCLUDE_TOPICS,
            report=report,
            db_path=str(db_path),
            table_colors=table_colors
        )
        build_po_exceeding_FDI_tb_3c (
            df_summa=df_summa,
            current_year=cutoff.year,
            cutoff=cutoff,
            report=report,
            db_path=str(db_path),
            table_colors=table_colors

        )
        # Save to DB if requested (already handled in process_granting_data and build functions, but log here)
        if SAVE_TO_DB:
            log.info("✔︎ Data saved to database")

        log.info("GrantsModule finished – %s rows processed.", len(df_grants))
        return ctx