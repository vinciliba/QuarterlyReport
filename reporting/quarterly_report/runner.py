
from ingestion.db_utils import list_report_modules
from importlib import import_module
from reporting.registry import REPORT_MODULES_REGISTRY
from .utils import get_modules
import streamlit as st
pkg = import_module(__package__)          # the package we are in
MODULES = get_modules("Quarterly_Report")                   # registry

def _ordered_enabled(report_name, db_path):
    df = list_report_modules(report_name, db_path)
    if df.empty:
        return MODULES.values()  # run all
    enabled = df[df.enabled == 1].sort_values("run_order")
    return [MODULES[m] for m in enabled.module_name if m in MODULES]

def run_report(cutoff_date, tolerance, db_path, selected_modules=None):
    """
    Run enabled modules for the Quarterly Report.
    Optionally accepts a subset of modules (selected_modules) for partial runs.
    Returns: (ctx, results) where results is a list of (module_name, status, message)
    """
    ctx = {}
    results = []

    # Determine modules to run
    if selected_modules:
        modules_to_run = selected_modules.values()
    else:
        modules_to_run = _ordered_enabled("Quarterly_Report", db_path)

    for mod_cls in modules_to_run:
        mod_name = mod_cls.__name__
        try:
            ctx = mod_cls().run(ctx, cutoff_date, db_path)

            # Optional: write status to a staged docx if active
            if "staged_docx" in st.session_state:
                doc = st.session_state.staged_docx
                doc.add_paragraph(f"✅ {mod_name} completed successfully.")

            results.append((mod_name, "✅ Success", ""))
        except Exception as e:
            error_msg = str(e)
            results.append((mod_name, "❌ Failed", error_msg))

            # Also optionally write failure to staged_docx
            if "staged_docx" in st.session_state:
                st.session_state.staged_docx.add_paragraph(f"❌ {mod_name} failed: {error_msg}")

            break  # Optional: fail-fast
    return ctx, results
