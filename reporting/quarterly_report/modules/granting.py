# reporting/quarterly_report/modules/granting.py
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

# ──────────────────────────────────────────────────────────────
# constants – adapt whenever a file-alias changes
# ──────────────────────────────────────────────────────────────
CALL_OVERVIEW_ALIAS   = "call_overview"
BUDGET_FOLLOWUP_ALIAS = "budget_follow_up_report"
ETHICS_ALIAS          = "ethics_requirements_and_issues"

EXCLUDE_TOPICS        = [
    "ERC-2023-SJI-1", 
    "ERC-2023-SJI",
    "ERC-2024-PERA",
    "HORIZON-ERC-2022-VICECHAIRS-IBA",
    "HORIZON-ERC-2023-VICECHAIRS-IBA",
]

MONTHS_ORDER = list(
    pd.date_range("2000-01-01", periods=12, freq="M").strftime("%B")
)

# ---------------------------------------------------------------------------
# little helper – converts a DataFrame → Great-Tables with very plain style
# ---------------------------------------------------------------------------
def _df_to_gt(df: pd.DataFrame, title: str) -> GT:
    return (
        GT(df.reset_index(drop=True))
        .tab_header(title)
        .opt_table_font(font="Arial")
        .tab_style(style=[style.text(weight="bold")], locations=loc.column_labels())
    )


# ---------------------------------------------------------------------------
# main module
# ---------------------------------------------------------------------------
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
    """

    name = "Granting"
    description = "Granting statistics / KPI / GAP state"

    


    def run(self, ctx: RenderContext) -> RenderContext:

        log     = logging.getLogger(self.name)
        conn    = ctx.db.conn
        cutoff  = pd.to_datetime(ctx.cutoff)
        db_path = conn.execute("PRAGMA database_list").fetchone()[2]

        # toggle: write to DB or not
        SAVE_TO_DB = False                 # ← switch back to True when ready
        EXPORT_DIR = Path("exports")       # all xlsx go here

        # ────────────────────────────────────────────────────────────
        # 1) load the freshest snapshots
        # ────────────────────────────────────────────────────────────
        call_overview  = fetch_latest_table_data(conn, CALL_OVERVIEW_ALIAS,   cutoff)
        budget_follow  = fetch_latest_table_data(conn, BUDGET_FOLLOWUP_ALIAS, cutoff)
        ethics_df      = fetch_latest_table_data(conn, ETHICS_ALIAS,          cutoff)

        for df, alias in [
            (call_overview, CALL_OVERVIEW_ALIAS),
            (budget_follow, BUDGET_FOLLOWUP_ALIAS),
            (ethics_df, ETHICS_ALIAS),
        ]:
            if df.empty:
                raise RuntimeError(
                    f"GAP module: no rows found for alias '{alias}'. "
                    "Upload data first (Single / Mass upload)."
                )

        # ────────────────────────────────────────────────────────────
        # 2) merge & clean
        # ────────────────────────────────────────────────────────────
        df1 = (
            call_overview
            .merge(budget_follow, left_on="Grant Number", right_on="Project Number")
            .reset_index()
            .drop_duplicates(subset="Grant Number", keep="last")
            .set_index("Grant Number")
            .sort_index()
        )

        df_grants = df1.merge(
            ethics_df,
            left_on="Grant Number", right_on="PROPOSAL\nNUMBER", how="inner"
        )

        COLS_TO_DROP: List[str] = []          #  ← fill when you know them
        df_grants.drop(columns=[c for c in COLS_TO_DROP if c in df_grants.columns],
                    inplace=True, errors="ignore")

        _ensure_timedelta_cols(df_grants)
        df_grants = enrich_grants(df_grants)
        # make sure every date column really **is** datetime
        _coerce_date_columns(df_grants)

        # ────────────────────────────────────────────────────────────
        # 3) calculations
        # ────────────────────────────────────────────────────────────
        kpi_gb = (
            df_grants.groupby("Call")
            .agg(
                TTG_days=("TTG", "mean"),
                TTS_days=("TTS", "mean"),
                TTI_days=("TTI", "mean"),
                SIGNED=("SIGNED", "sum"),
                ACTIVE=("ACTIVE", "sum"),
            )
            .assign(COMPLETION_RATE=lambda d: d["SIGNED"] / d["ACTIVE"])
            .reset_index()
        )

        piv = (
            df_grants.assign(ProjectCount=1)
            .pivot_table(
                values="ProjectCount", index=["ETHIC_STATUS", "GAP_STEP "],
                columns="Call", aggfunc="sum", fill_value=0
            )
            .reset_index()
        )

        this_year = cutoff.year
        signed = df_grants[df_grants["GA Signature - Commission"].dt.year == this_year]
        signed = signed[~signed["Topic"].isin(EXCLUDE_TOPICS)]

        tab3_signed = (
            signed.pivot_table(
                index=signed["GA Signature - Commission"].dt.month_name(),
                columns="Topic", values="SIGNED", aggfunc="sum", fill_value=0
            )
            .reindex(MONTHS_ORDER)
            .reset_index(names="Signature Month")
        )

        committed = df_grants[df_grants["Commitment AO visa"].dt.year == this_year]
        committed = committed[~committed["Topic"].isin(EXCLUDE_TOPICS)]

        tab4_eur = (
            committed.pivot_table(
                index=committed["Commitment AO visa"].dt.month_name(),
                columns="Topic", values="Eu contribution",
                aggfunc="sum", fill_value=0
            )
            .reindex(MONTHS_ORDER)
            .reset_index(names="Commitment Month")
        )

        tab4_n = (
            committed.assign(ACTIVE_1=1)
            .pivot_table(
                index=committed["Commitment AO visa"].dt.month_name(),
                columns="Topic", values="ACTIVE_1",
                aggfunc="sum", fill_value=0
            )
            .reindex(MONTHS_ORDER)
            .reset_index(names="Commitment Month")
        )

        # ────────────────────────────────────────────────────────────
        # 4) export every frame to a single Excel file
        # ────────────────────────────────────────────────────────────
        EXPORT_DIR.mkdir(exist_ok=True)
        ts   = datetime.datetime.now().strftime("%Y-%m-%d_%H%M")
        path = EXPORT_DIR / f"grants-{ts}.xlsx"

        with pd.ExcelWriter(path, engine="openpyxl") as xl:
            df_grants.to_excel(xl, sheet_name="01_raw_grants", index=False)
            kpi_gb.to_excel(xl,      sheet_name="02_kpi",       index=False)
            piv.to_excel(xl,         sheet_name="03_stateplay", index=False)
            tab3_signed.to_excel(xl, sheet_name="04_tab3_sign", index=False)
            tab4_eur.to_excel(xl,    sheet_name="05_tab4_eur",  index=False)
            tab4_n.to_excel(xl,      sheet_name="06_tab4_num",  index=False)

        log.info("📤 exported helper workbook → %s", path)

        # ────────────────────────────────────────────────────────────
        # 5) OPTIONAL: persist artefacts                                   
        #    • Excel files →   ./exports/<report>/<YYYY-MM-DD>/*.xlsx
        #    • GT objects  →   inserted only when SAVE_TO_DB is True       
        # ────────────────────────────────────────────────────────────
        EXPORT_ROOT = Path.cwd() / "exports"          # <── feel free to adapt
        today_stamp = pd.Timestamp.today().strftime("%Y-%m-%d")
        export_dir  = EXPORT_ROOT / ctx.report_name / today_stamp
        export_dir.mkdir(parents=True, exist_ok=True)

        # -------- Excel exports (always) --------------------------------
        (
            kpi_gb.to_excel(      export_dir / "kpi_table.xlsx",          index=False),
            piv.to_excel(         export_dir / "state_of_play.xlsx",      index=False),
            tab3_signed.to_excel( export_dir / "signatures_tab3.xlsx",    index=False),
            tab4_eur.to_excel(    export_dir / "commitments_eur_tab4.xlsx", index=False),
            tab4_n.to_excel(      export_dir / "commitments_n_tab4.xlsx",  index=False),
            df_grants.to_excel(   export_dir / "grants_raw_df.xlsx",      index=False),
        )
        log.info("✔︎ Excel exports written to %s", export_dir.resolve())

        # -------- Save to DB (optional flag) ----------------------------
        if SAVE_TO_DB:
            kpi_tbl      = _df_to_gt(kpi_gb,      "KPI by Call")
            sop_tbl      = _df_to_gt(piv,         "State of Play")
            tab3_tbl     = _df_to_gt(tab3_signed, "Signatures (current year)")
            tab4_eur_tbl = _df_to_gt(tab4_eur,    "Commitments (€)")
            tab4_n_tbl   = _df_to_gt(tab4_n,      "Commitments (# projects)")

            insert_variable(ctx.report_name, self.name, "grants_raw_df",
                            df_grants.to_dict("records"), db_path, "grants_raw_df")
            insert_variable(ctx.report_name, self.name, "kpi_table",
                            {}, db_path, "kpi_table",          kpi_tbl)
            insert_variable(ctx.report_name, self.name, "state_of_play",
                            {}, db_path, "state_of_play",      sop_tbl)
            insert_variable(ctx.report_name, self.name, "signatures_tab3",
                            {}, db_path, "signatures_tab3",    tab3_tbl)
            insert_variable(ctx.report_name, self.name, "commitments_eur_tab4",
                            {}, db_path, "commitments_eur_tab4", tab4_eur_tbl)
            insert_variable(ctx.report_name, self.name, "commitments_n_tab4",
                            {}, db_path, "commitments_n_tab4",   tab4_n_tbl)

        log.info("GrantsModule finished – %s rows processed.", len(df_grants))
        return ctx

