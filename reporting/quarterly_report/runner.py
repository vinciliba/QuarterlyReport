# # In reporting/quarterly_report/runner.py
# from ingestion.db_utils import list_report_modules
# from importlib import import_module
# from reporting.registry import REPORT_MODULES_REGISTRY
# from reporting.quarterly_report.utils import get_modules
# from ingestion.db_utils import insert_variable
# import streamlit as st
# from reporting.quarterly_report.utils import RenderContext, BaseModule

# pkg = import_module(__package__)  # the package we are in
# MODULES = get_modules("Quarterly_Report")  # registry

# def _ordered_enabled(report_name, db_path):
#     df = list_report_modules(report_name, db_path)
#     if df.empty:
#         return MODULES.values()  # run all
#     enabled = df[df.enabled == 1].sort_values("run_order")
#     return [MODULES[m] for m in enabled.module_name if m in MODULES]

# def run_report(cutoff_date, tolerance, db_path, selected_modules=None):
#     ctx = {}
#     results = []

#     if selected_modules:
#         modules_to_run = selected_modules.values()
#     else:
#         modules_to_run = _ordered_enabled("Quarterly_Report", db_path)

#     for mod_cls in modules_to_run:
#         mod_name = mod_cls.__name__
#         try:
#             updated_ctx, (success, msg) = mod_cls().run(ctx, cutoff_date, db_path)
#             ctx.update(updated_ctx)

#             for k, v in updated_ctx.items():
#                 insert_variable("Quarterly_Report", mod_name, k, v, db_path, anchor=k)

#             if "staged_docx" in st.session_state:
#                 st.session_state.staged_docx.add_paragraph(f"✅ {mod_name} completed successfully.")

#             results.append((mod_name, "✅ Success", msg))
#         except Exception as e:
#             error_msg = str(e)
#             results.append((mod_name, "❌ Failed", error_msg))
#             if "staged_docx" in st.session_state:
#                 st.session_state.staged_docx.add_paragraph(f"❌ {mod_name} failed: {error_msg}")
#             break
#     return ctx, results

# In reporting/quarterly_report/runner.py
from ingestion.db_utils import list_report_modules, insert_variable
from importlib import import_module
from reporting.quarterly_report.utils import get_modules, RenderContext,Database
from reporting.quarterly_report.utils import BaseModule
import streamlit as st


pkg = import_module(__package__)
MODULES = get_modules("Quarterly_Report")

def _ordered_enabled(report_name, db_path):
    df = list_report_modules(report_name, db_path)
    if df.empty:
        return MODULES.values()
    enabled = df[df.enabled == 1].sort_values("run_order")
    return [MODULES[m] for m in enabled.module_name if m in MODULES]

def run_report(cutoff_date, tolerance, db_path, selected_modules=None):
    ctx = RenderContext(
        db=Database(db_path),
        params={"tolerance_days": tolerance},
        cutoff=cutoff_date,
        out={"tables": {}, "charts": {}}
    )
    ctx.report_name = "Quarterly_Report"  # manually inject this attribute

    results = []
    modules_to_run = selected_modules.values() if selected_modules else _ordered_enabled(ctx.report_name, db_path)

    for mod_cls in modules_to_run:
        mod_name = mod_cls.__name__
        try:
            mod_instance = mod_cls()
            ctx = mod_instance.run(ctx)

            for k, v in ctx.out.items():
                insert_variable(ctx.report_name, mod_name, k, v, db_path, anchor=k)

            if "staged_docx" in st.session_state:
                st.session_state.staged_docx.add_paragraph(f"✅ {mod_name} completed successfully.")

            results.append((mod_name, "✅ Success", None))

        except Exception as e:
            error_msg = str(e)
            results.append((mod_name, "❌ Failed", error_msg))

            if "staged_docx" in st.session_state:
                st.session_state.staged_docx.add_paragraph(f"❌ {mod_name} failed: {error_msg}")
            break

    return ctx, results
