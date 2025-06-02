# reporting/quarterly_report/modules/invoices.py

from __future__ import annotations
import logging
import pandas as pd
from pathlib import Path

from ingestion.db_utils import (
    fetch_latest_table_data,
    load_report_params
)
from reporting.quarterly_report.utils import RenderContext, BaseModule
from reporting.quarterly_report.report_utils.invoices_builder import generate_invoices_report

# Constants
INVOICES_ALIAS = "c0_invoices_summa"
CALLS_ALIAS = 'call_overview'


class InvoicesModule(BaseModule):
    name = "Invoices"  # shows up in UI
    description = "Invoice Registration Statistics and Analysis"

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
        log.info("Starting invoice registration report generation...")
        
        results = generate_invoices_report(
            conn=conn,
            cutoff=cutoff,
            alias_inv=INVOICES_ALIAS,
            alias_calls=CALLS_ALIAS,
            report=report,
            db_path=db_path,
            report_params=report_params,
            save_to_db=SAVE_TO_DB,
            export_dir=EXPORT_DIR
        )

        # Save to DB if requested (already handled in generate_invoices_report)
        if SAVE_TO_DB:
            log.info("✔︎ Invoice registration data saved to database")

        # Log summary statistics
        stats = results['summary_stats']
        log.info(f"Invoice Registration Summary:")
        log.info(f"  Total invoices processed: {stats['total_invoices']:,}")
        log.info(f"  On-time registrations: {stats['on_time_invoices']:,}")
        log.info(f"  Late registrations: {stats['late_invoices']:,}")
        log.info(f"  H2020 invoices: {stats['h2020_invoices']:,}")
        log.info(f"  HEU invoices: {stats['heu_invoices']:,}")
        
        if stats['total_invoices'] > 0:
            on_time_percentage = (stats['on_time_invoices'] / stats['total_invoices']) * 100
            log.info(f"  Overall on-time rate: {on_time_percentage:.2f}%")

        log.info("InvoicesModule finished successfully")
        
        # Store results in context for potential use by other modules
        ctx.invoice_results = results
        
        return ctx