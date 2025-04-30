from __future__ import annotations
from reporting.quarterly_report.utils import RenderContext, BaseModule

class PaymentsModule(BaseModule):
    name        = "Payments"          # shows up in UI
    description = "Payments Statistics, Tables and Charts"