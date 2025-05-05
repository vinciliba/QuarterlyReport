# In reporting/quarterly_report/modules/budget.py
from reporting.quarterly_report.utils import RenderContext, BaseModule
from reporting.quarterly_report.report_utils.tables import (
    build_commitment_summary_table,
    build_commitment_detail_table_1,
    build_commitment_detail_table_2,
    build_payment_summary_table,
    fetch_latest_table_data
)
import pandas as pd
import sqlite3

class BudgetModule(BaseModule):
    name = "Budget"
    description = "Budget execution tables & charts"

    def run(self, ctx: RenderContext, cutoff=None, db_path=None, report_name=None) -> RenderContext:
        print("DEBUG: Starting BudgetModule.run")
        cutoff = pd.to_datetime(ctx.cutoff)
        report = report_name or getattr(ctx, 'report_name', 'Quarterly_Report')
        db_path = db_path or getattr(ctx.db, 'path', None)
        conn = ctx.db.conn

        df_exec = fetch_latest_table_data(conn, "c0_budgetary_execution_details", cutoff)
        df_comm = fetch_latest_table_data(conn, "c0_commitments_summa", cutoff)

        year = cutoff.year

        tbl_commit_summary = build_commitment_summary_table(df_comm, year, report, db_path)
        tbl_commit_detail_1 = build_commitment_detail_table_1(df_comm, year, report, db_path)
        tbl_commit_detail_2 = build_commitment_detail_table_2(df_comm, year, report, db_path)
        tbl_payments = build_payment_summary_table(df_exec, year, report, db_path)

        ctx.out["tables"]["commitment_summary"] = tbl_commit_summary
        ctx.out["tables"]["commitment_detail_1a"] = tbl_commit_detail_1
        ctx.out["tables"]["commitment_detail_1b"] = tbl_commit_detail_2
        ctx.out["tables"]["payments"] = tbl_payments

        return ctx

