from __future__ import annotations
from reporting.quarterly_report.utils import RenderContext, BaseModule

class RevenueModule(BaseModule):
    name        = "Recovery Orders"          # shows up in UI
    description = "Revenue statistics and tables"