# reporting/quarterly_report/modules/budget.py
from __future__ import annotations
from reporting.quarterly_report.utils import RenderContext, BaseModule

class BudgetModule(BaseModule):
    name        = "Budget"          # shows up in UI
    description = "Budget execution tables & charts"

    def run(self, ctx: RenderContext) -> RenderContext:
        """
        1. Pull what you need from ctx.db (already-connected sqlite3)
        2. Perform calculations – ideally through utils classes
        3. Save artefacts into ctx.out  (tables, figures, text, …)
        4. Return ctx  (so downstream modules can reuse)
        """
        df  = ctx.db.read_table("budget_actuals")           # pseudo-helper
        tbl = self.make_table(df)                           # your formatting util
        fig = self.make_chart(df)                           # altair / plotly / …
        ctx.out["tables"]["budget"]  = tbl
        ctx.out["charts"]["budget"]  = fig
        return ctx
