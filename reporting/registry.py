# In reporting/registry.py
from .quarterly_report.modules_registry import MODULES as QUARTERLY_MODULES  # Fix: Import from modules_registry

REPORT_MODULES_REGISTRY = {
    "Quarterly_Report": QUARTERLY_MODULES,
    # Add more mappings here
}

#