from reporting.quarterly_report.utils import RenderContext, BaseModule
from reporting.quarterly_report.report_utils.tables import (
    build_commitment_summary_table,
    build_commitment_detail_table_1,
    build_commitment_detail_table_2,
    build_payment_summary_tables,
    fetch_latest_table_data
)
import pandas as pd
import sqlite3
import logging
import traceback
import sys

class DebugError(Exception):
    """Custom exception to halt execution and trigger debugging."""
    pass

class BudgetModule(BaseModule):
    name = "Budget"
    description = "Budget execution tables & charts"

    def run(self, ctx: RenderContext, cutoff=None, db_path=None, report_name=None) -> RenderContext:
        logging.debug("Starting BudgetModule.run")
        
        # Set defaults with validation
        cutoff = pd.to_datetime(ctx.cutoff) if cutoff is None else pd.to_datetime(cutoff)
        report = report_name or getattr(ctx, 'report_name', 'Quarterly_Report')
        db_path = db_path or getattr(ctx, 'db_path', getattr(ctx.db, 'path', None))
        if db_path is None:
            logging.error("db_path is not set in context or provided. Using default: database/reporting.db")
            db_path = "database/reporting.db"  # Fallback path
        conn = ctx.db.conn

        # Fetch data
        try:
            df_exec = fetch_latest_table_data(conn, "c0_budgetary_execution_details", cutoff)
            logging.debug(f"Fetched {len(df_exec)} rows from c0_budgetary_execution_details")
        except Exception as e:
            logging.error(f"Error fetching c0_budgetary_execution_details: {str(e)}\n{traceback.format_exc()}")
            raise DebugError(f"Failed to fetch c0_budgetary_execution_details: {str(e)}")

        try:
            df_comm = fetch_latest_table_data(conn, "c0_commitments_summa", cutoff)
            logging.debug(f"Fetched {len(df_comm)} rows from c0_commitments_summa")
        except Exception as e:
            logging.error(f"Error fetching c0_commitments_summa: {str(e)}\n{traceback.format_exc()}")
            raise DebugError(f"Failed to fetch c0_commitments_summa: {str(e)}")

        year = cutoff.year

        # Build tables with debugging
        try:
            tbl_commit_summary = build_commitment_summary_table(df_exec, year, report, db_path)
            logging.debug(f"Commitment summary table shape: {tbl_commit_summary.shape}")
            if tbl_commit_summary.empty:
                logging.warning("Commitment summary table is empty.")
        except Exception as e:
            logging.error(f"Error building commitment_summary table: {str(e)}\n{traceback.format_exc()}")
            raise DebugError(f"Failed to build commitment_summary table: {str(e)}")

        try:
            tbl_commit_detail_1 = build_commitment_detail_table_1(df_comm, year, report, db_path)
            logging.debug(f"Commitment detail 1a table shape: {tbl_commit_detail_1.shape}")
            if tbl_commit_detail_1.empty:
                logging.warning("Commitment detail 1a table is empty.")
        except Exception as e:
            logging.error(f"Error building commitment_detail_1a table: {str(e)}\n{traceback.format_exc()}")
            raise DebugError(f"Failed to build commitment_detail_1a table: {str(e)}")

        try:
            tbl_commit_detail_2 = build_commitment_detail_table_2(df_comm, year, report, db_path)
            logging.debug(f"Commitment detail 1b table shape: {tbl_commit_detail_2.shape}")
            if tbl_commit_detail_2.empty:
                logging.warning("Commitment detail 1b table is empty.")
        except Exception as e:
            logging.error(f"Error building commitment_detail_1b table: {str(e)}\n{traceback.format_exc()}")
            raise DebugError(f"Failed to build commitment_detail_1b table: {str(e)}")

        try:
            tbl_payments = build_payment_summary_tables(df_exec, year, report, db_path)
            logging.debug(f"Payment tables shape - HE: {tbl_payments['HE'].shape}, H2020: {tbl_payments['H2020'].shape}")
            if tbl_payments['HE'].empty and tbl_payments['H2020'].empty:
                logging.warning("Payment tables for both HE and H2020 are empty.")
        except Exception as e:
            logging.error(f"Error building payment tables: {str(e)}\n{traceback.format_exc()}")
            raise DebugError(f"Failed to build payment tables: {str(e)}")

        # Store results in context
        ctx.out["tables"] = {
            "commitment_summary": tbl_commit_summary,
            "commitment_detail_1a": tbl_commit_detail_1,
            "commitment_detail_1b": tbl_commit_detail_2,
            "payments": tbl_payments
        }

        # Final check for empty tables
        if all(isinstance(table, (pd.DataFrame, dict)) and 
               (getattr(table, 'empty', True) if isinstance(table, pd.DataFrame) else 
                all(df.empty for df in table.values() if isinstance(df, pd.DataFrame)))):
            logging.error("All tables are empty. Report generation failed.")
            raise DebugError("All generated tables are empty.")
        else:
            logging.debug("Stored tables successfully.")

        return ctx

    def debug_on_error(self, error: Exception) -> None:
        """Handle debugging when an error occurs."""
        logging.error(f"Debugging stopped due to: {str(error)}")
        logging.error(f"Full traceback: {traceback.format_exc()}")
        import streamlit as st
        if 'streamlit' in sys.modules:
            st.error(f"Process halted due to error: {str(error)}")
            st.text(f"Traceback:\n{traceback.format_exc()}")
            if st.button("Continue despite error"):
                logging.warning("Continuing execution despite error.")
            else:
                st.stop()  # Halt Streamlit execution
        else:
            import pdb; pdb.set_trace()  # Drop into debugger in non-Streamlit environments
            raise error  # Re-raise to stop execution

if __name__ == "__main__":
    try:
        module = BudgetModule()
        ctx = RenderContext(
            db=sqlite3.connect("F:\\vinci\\Projects\\QuarterlyReport\\database\\reporting.db"),
            cutoff=pd.to_datetime("2025-04-16"),
            report_name="Quarterly_Report",
            db_path="F:\\vinci\\Projects\\QuarterlyReport\\database\\reporting.db"
        )
        module.run(ctx)
    except DebugError as e:
        module.debug_on_error(e)
    except Exception as e:
        module.debug_on_error(e)