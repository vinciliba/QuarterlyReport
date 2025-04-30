from __future__ import annotations
from reporting.quarterly_report.utils import RenderContext, BaseModule

class GFModule(BaseModule):
    name        = "Gurantee Fund"          # shows up in UI
    description = "Gurantee Fund balance and charts"