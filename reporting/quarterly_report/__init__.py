from reporting.quarterly_report.modules.budget import BudgetModule
from reporting.quarterly_report.modules.granting import GrantsModule
from reporting.quarterly_report.modules.recoveries import RevenueModule
from reporting.quarterly_report.modules.auri import AuriModule
from reporting.quarterly_report.modules.controls import ControlsModule
from reporting.quarterly_report.modules.edes import EdesModule
from reporting.quarterly_report.modules.gf import GFModule
from reporting.quarterly_report.modules.invoices import InvoicesModule
from reporting.quarterly_report.modules.kpis import KPIsModule
from reporting.quarterly_report.modules.payments import PaymentsModule
from reporting.quarterly_report.modules.amendments import AmendmentModule
MODULES = {
    "Budget": BudgetModule,
    "Grants": GrantsModule,
    "Revenue": RevenueModule,
    "Auri": AuriModule,
    "Controls": ControlsModule,
    "Edes": EdesModule,
    "GF": GFModule,
    "Invoices": InvoicesModule,
    "KPIs": KPIsModule,
    "Payments": PaymentsModule,
    "Amendments": AmendmentModule
}
