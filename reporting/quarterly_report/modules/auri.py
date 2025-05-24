
from __future__ import annotations

import logging, sqlite3, datetime
from pathlib import Path
import pandas as pd
from ingestion.db_utils import (
    fetch_latest_table_data,
)
from reporting.quarterly_report.utils import RenderContext, BaseModule
from ingestion.db_utils import load_report_params
from reporting.quarterly_report.report_utils.auri_builder import generate_auri_report
# Constants
AURI_ALIAS = "audit_result_implementation"


class AuriModule(BaseModule):
    name        = "Auri"          # shows up in UI
    description = "Auri tables"


    def run(self, ctx: RenderContext) -> RenderContext:
        log = logging.getLogger(self.name)
        conn = ctx.db.conn
        cutoff = pd.to_datetime(ctx.cutoff)
        db_path = Path(conn.execute("PRAGMA database_list").fetchone()[2])
        report = ctx.report_name

        # Load report parameters
        report_params = load_report_params(report_name=report, db_path=db_path)

        # Toggle for saving to DB or exporting
        SAVE_TO_DB = True  # Switch to True when ready
        EXPORT_DIR = Path("exports")

        # Process the data
        results = generate_auri_report(
            conn=conn,
            cutoff=cutoff,
            alias=AURI_ALIAS ,
            report=report,
            db_path=db_path,
            report_params=report_params,
            save_to_db=SAVE_TO_DB,
            export_dir=EXPORT_DIR
        )
        # # Unpack results from process_granting_data
        # df_grants = results["df_amendments"]
  
      
       
        # Save to DB if requested (already handled in process_granting_data and build functions, but log here)
        if SAVE_TO_DB:
            log.info("✔︎ Data saved to database")

        # log.info("AmendmentsModule finished – %s rows processed.", len(df_grants))
        return ctx