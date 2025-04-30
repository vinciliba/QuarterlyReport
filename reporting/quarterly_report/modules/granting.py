from __future__ import annotations
from reporting.quarterly_report.utils import RenderContext, BaseModule

class GrantsModule(BaseModule):
    name = "GAP"
    description = "GAP statistics, and tables"

    def run(self, ctx: RenderContext, cutoff, db_path: str):
        print("✅ Hello fDaniele rom GrantsModule!")
        return True, "✅ GrantsModule ran successfully."
