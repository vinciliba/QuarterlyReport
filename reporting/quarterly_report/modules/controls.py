from __future__ import annotations
from reporting.quarterly_report.utils import RenderContext, BaseModule

class ControlsModule(BaseModule):
    name        = "Controls"          # shows up in UI
    description = "Controls execution table"