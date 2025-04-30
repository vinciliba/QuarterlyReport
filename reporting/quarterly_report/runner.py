from ingestion.db_utils import list_report_modules
from importlib import import_module
from ingestion.registry import REPORT_MODULES_REGISTRY
from utils import get_modules

pkg = import_module(__package__)          # the package we are in
MODULES = get_modules("Quarterly_Report")                   # registry

def _ordered_enabled(report_name, db_path):
    df = list_report_modules(report_name, db_path)
    if df.empty:                          # nothing configured ⇒ run all
        return MODULES.values()
    enabled = df[df.enabled == 1].sort_values("run_order")
    return [MODULES[m] for m in enabled.module_name if m in MODULES]

def run_report(cutoff_date, tolerance, db_path):
    ctx = {}                              # mutable context dict
    for mod_cls in _ordered_enabled("Quarterly_Report", db_path):
        ctx = mod_cls().run(ctx, cutoff_date, db_path)
    return ctx
