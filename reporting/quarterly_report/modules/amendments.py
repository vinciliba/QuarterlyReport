from __future__ import annotations
from reporting.quarterly_report.utils import RenderContext, BaseModule

class AmendmentModule(BaseModule):
    name        = "Amendment"          # shows up in UI
    description = "Amendment execution tables & charts"