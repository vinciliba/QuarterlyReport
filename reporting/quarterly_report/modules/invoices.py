from __future__ import annotations
from reporting.quarterly_report.utils import RenderContext, BaseModule

class InvoicesModule(BaseModule):
    name        = "Invoice"          # shows up in UI
    description = "Invoicing Statstics and table"