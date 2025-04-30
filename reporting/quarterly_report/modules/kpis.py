from __future__ import annotations
from reporting.quarterly_report.utils import RenderContext, BaseModule

class KPIsModule(BaseModule):
    name        = "KPIs"          # shows up in UI
    description = "All KPIs statistics and tables"