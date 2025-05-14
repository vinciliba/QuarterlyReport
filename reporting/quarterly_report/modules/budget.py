import pandas as pd
import sqlite3
import json
import logging
import traceback
import sys
from reporting.quarterly_report.utils import RenderContext, BaseModule
from reporting.quarterly_report.report_utils.tables import (
    build_commitment_summary_table,
    build_commitment_detail_table_1,
    build_commitment_detail_table_2,
    build_payment_summary_tables,
)
from ingestion.db_utils import fetch_latest_table_data, load_report_params
import pdb


class DebugError(Exception):
    """Custom exception to halt execution and trigger debugging."""
    pass


class BudgetModule(BaseModule):
    name = "Budget"
    description = "Budget execution tables & charts"

    def run(self, ctx: RenderContext, cutoff=None, db_path=None, report_name=None) -> RenderContext:
        logging.debug("Starting BudgetModule.run")
        
        # Set defaults with validation, using ctx attributes
        cutoff = pd.to_datetime(ctx.cutoff) if cutoff is None else pd.to_datetime(cutoff)
        report = report_name or getattr(ctx, 'report_name', 'Quarterly_Report')
        db_path = db_path or getattr(ctx, 'db_path', getattr(ctx.db, 'path', None))
        if db_path is None:
            logging.error("db_path is not set in context or provided. Using default: database/reporting.db")
            db_path = "database/reporting.db"  # Fallback path
        conn = ctx.db.conn

        # Load TABLE_COLORS from the database
        try:
            report_params = load_report_params(report_name=report, db_path=db_path)
            table_colors = report_params.get("TABLE_COLORS")
            logging.debug("Successfully loaded TABLE_COLORS from database")
        except Exception as e:
            logging.error(f"Error loading TABLE_COLORS from database: {str(e)}\n{traceback.format_exc()}")
            table_colors = {
                "BLUE" :"#004A99",
                "LIGHT_BLUE" :"#d6e6f4",
                "GRID_CLR" :"#004A99",
                "DARK_BLUE" :"#01244B",
                "DARK_GREY" :'#242425',
                "heading_background_color": "#004A99",
                "row_group_background_color": "#d6e6f4",
                "border_color": "#01244B",
                "stub_background_color": "#d6e6f4",
                "body_background_color": "#ffffff",
                "subtotal_background_color": "#E6E6FA",
                "text_color": "#01244B"
            }
            logging.warning("Using default TABLE_COLORS due to error")

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

        # Build tables with debugging, passing table_colors
        try:
            tbl_commit_summary = build_commitment_summary_table(df_exec, year, report, db_path, table_colors=table_colors)
            logging.debug(f"Commitment summary table shape: {tbl_commit_summary.shape}")
            if tbl_commit_summary.empty:
                logging.warning("Commitment summary table is empty.")
            ctx.out["tables"]["commitment_summary"] = tbl_commit_summary  # Store in ctx

        except Exception as e:
            logging.error(f"Error building commitment_summary table: {str(e)}\n{traceback.format_exc()}")
            raise DebugError(f"Failed to build commitment_summary table: {str(e)}")

        try:
            tbl_commit_detail_1 = build_commitment_detail_table_1(df_comm, year, report, db_path, table_colors=table_colors)
            logging.debug(f"Commitment detail 1a table shape: {tbl_commit_detail_1.shape}")
            if tbl_commit_detail_1.empty:
                logging.warning("Commitment detail 1a table is empty.")
            ctx.out["tables"]["commitment_detail_1a"] = tbl_commit_detail_1  # Store in ctx

        except Exception as e:
            logging.error(f"Error building commitment_detail_1a table: {str(e)}\n{traceback.format_exc()}")
            raise DebugError(f"Failed to build commitment_detail_1a table: {str(e)}")

        try:
            tbl_commit_detail_2 = build_commitment_detail_table_2(df_comm, year, report, db_path, table_colors=table_colors)
            logging.debug(f"Commitment detail 1b table shape: {tbl_commit_detail_2.shape}")
            if tbl_commit_detail_2.empty:
                logging.warning("Commitment detail 1b table is empty.")
            ctx.out["tables"]["commitment_detail_1b"] = tbl_commit_detail_2  # Store in ctx

        except Exception as e:
            logging.error(f"Error building commitment_detail_1b table: {str(e)}\n{traceback.format_exc()}")
            raise DebugError(f"Failed to build commitment_detail_1b table: {str(e)}")

        try:
            tbl_payments = build_payment_summary_tables(df_exec, year, report, db_path, table_colors=table_colors)
            logging.debug(f"Payment tables shape - HE: {tbl_payments['HE'].shape}, H2020: {tbl_payments['H2020'].shape}")
            if tbl_payments['HE'].empty and tbl_payments['H2020'].empty:
                logging.warning("Payment tables for both HE and H2020 are empty.")
            ctx.out["tables"]["payments"] = tbl_payments  # Store in ctx

        except Exception as e:
            logging.error(f"Error building payment tables: {str(e)}\n{traceback.format_exc()}")
            raise DebugError(f"Failed to build payment tables: {str(e)}")

        # Log success and return context
        logging.debug("Stored tables successfully in ctx.out")
        return ctx

    def debug_on_error(self, error: Exception) -> None:
        """Handle debugging when an error occurs, compatible with Streamlit runner."""
        logging.error(f"Debugging stopped due to: {str(error)}")
        logging.error(f"Full traceback: {traceback.format_exc()}")
        import streamlit as st
        if 'streamlit' in sys.modules:
            st.error(f"Process halted due to error: {str(error)}")
            st.text(f"Traceback:\n{traceback.format_exc()}")
            if st.button("Continue despite error"):
                logging.warning("Continuing execution despite error.")
                return  # Allow continuation without raising
            else:
                st.stop()  # Halt Streamlit execution as per runner
        else:
            pdb.set_trace()  # Drop into debugger in non-Streamlit
            raise error  # Re-raise to stop execution