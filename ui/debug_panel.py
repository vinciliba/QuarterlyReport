import streamlit as st
import importlib, inspect, traceback
import sys
import os
from io import StringIO
from ingestion.db_utils import load_report_params
from reporting.quarterly_report.utils import RenderContext, Database, BaseModule
from datetime import date
from code_editor import code_editor

# ─── Setup ───────────────────────────────────────────────────────────
st.title("🧠 Module Debugger + Editor")

# ─── Dynamic Report Selection ─────────────────────────────────────────
report_lookup = {
    "quarterly_report": "reporting.quarterly_report"
    # Extend here if more reports added
}

report_name = st.selectbox("Select report", list(report_lookup.keys()))
mod_pkg_path = report_lookup[report_name]

# Dynamically import the report module registry
try:
    report_mod_pkg = importlib.import_module(mod_pkg_path)
    modules_dict = report_mod_pkg.MODULES
except Exception as e:
    st.error(f"❌ Failed to import module registry: {e}")
    st.stop()

# ─── Module Selection ─────────────────────────────────────────────────
module_name = st.selectbox("Module", list(modules_dict.keys()))

# Use session state to detect changes in module selection
if 'last_module' not in st.session_state:
    st.session_state['last_module'] = module_name

# Fetch the module class
mod_cls = modules_dict[module_name]

cutoff = st.date_input("Cut-off date", date.today())

# ─── Display Module Code ──────────────────────────────────────────────
st.markdown("### ✍️ Module Code (editable)")

# Required imports for the module
required_imports = """from __future__ import annotations
from reporting.quarterly_report.utils import RenderContext, BaseModule
"""

# Initialize or update the editor content when the module changes
if st.session_state['last_module'] != module_name:
    st.session_state['last_module'] = module_name
    try:
        # Fetch the new module's source code
        code_txt = inspect.getsource(mod_cls)
        # Prepend the required imports to the code
        code_txt = required_imports + "\n" + code_txt
        # Reset the edited code in session state to the new module's code
        st.session_state['edited_code'] = code_txt
    except Exception as e:
        st.error(f"Failed to load code: {e}")
        st.stop()

# If the edited code isn't set (first load), initialize it
if 'edited_code' not in st.session_state:
    code_txt = inspect.getsource(mod_cls)
    st.session_state['edited_code'] = required_imports + "\n" + code_txt

# Use a stable key for the editor to prevent unnecessary re-renders
editor_key = f"editable_module_code_{module_name}"

# Display the editor using streamlit_code_editor
st_edited_code = code_editor(
    code=st.session_state['edited_code'],
    lang="python",
    theme="vs-dark",
    key=editor_key,
    height=400,  # Use integer for height (pixels)
    options={
        "fontSize": 15,
        "scrollBeyondLastLine": False,
        "minimap": {"enabled": False},
        "lineNumbers": "on",
        "wordWrap": "on"
    }
)

# Update session state with the edited code
if st_edited_code["text"] and st_edited_code["text"] != st.session_state['edited_code']:
    st.session_state['edited_code'] = st_edited_code["text"]

# ─── Save Edited Code to Source File ──────────────────────────────────
if st.button("💾 Save changes to source file"):
    try:
        # Get the path to the source file
        source_file = inspect.getsourcefile(mod_cls)
        if not source_file:
            st.error("Could not determine the source file location.")
        else:
            # Create a backup of the original file
            backup_file = source_file + ".backup"
            with open(source_file, "r", encoding="utf-8") as f:
                original_content = f.read()
            with open(backup_file, "w", encoding="utf-8") as f:
                f.write(original_content)
            
            # Write the edited code to the source file with utf-8 encoding
            with open(source_file, "w", encoding="utf-8") as f:
                f.write(st.session_state['edited_code'])
            
            st.success(f"✅ Changes saved to {source_file}. A backup was created at {backup_file}.")
            
            # Reload the module to reflect the changes
            importlib.reload(inspect.getmodule(mod_cls))
            
    except Exception as e:
        st.error(f"💥 Failed to save changes: {e}")
        st.code(traceback.format_exc())

# ─── Execute Module ───────────────────────────────────────────────────
if st.button("🚀 Run module"):
    try:
        # Prepare the namespace for exec
        local_vars = {}
        exec_globals = globals().copy()
        exec_globals['BaseModule'] = BaseModule
        exec_globals['RenderContext'] = RenderContext

        # Capture print output
        old_stdout = sys.stdout
        captured_output = StringIO()
        sys.stdout = captured_output

        try:
            # Dynamically compile the edited code
            exec(st.session_state['edited_code'], exec_globals, local_vars)

            # Extract the class from the compiled code
            new_class = local_vars.get(module_name, mod_cls)  # Fallback to original if class not found

            # Instantiate the new class
            mod = new_class()

            # Create the RenderContext
            ctx = RenderContext(
                db     = Database("database/reporting.db"),
                params = load_report_params(report_name, "database/reporting.db"),
                cutoff = cutoff.isoformat(),
                out    = {"tables":{}, "charts":{}, "text":{}},
            )

            # Call the run method with the correct arguments
            if "ctx" in st.session_state['edited_code']:
                result = mod.run(ctx=ctx, cutoff=cutoff.isoformat(), db_path="database/reporting.db")
            else:
                result = mod.run(ctx)

            # Display any captured print output
            print_output = captured_output.getvalue()
            if print_output:
                st.text("Print Output:")
                st.code(print_output)

            st.success(f"✅ {module_name} finished.")

            # Handle the result (assuming run returns a tuple of (success, message))
            if isinstance(result, tuple) and len(result) == 2:
                success, message = result
                if success:
                    st.write(message)
                else:
                    st.error(message)
            else:
                st.write(result)

            if ctx.out["tables"]:
                for name, tbl in ctx.out["tables"].items():
                    st.subheader(f"📊 Table: {name}")
                    st.dataframe(tbl)

            if ctx.out["charts"]:
                for name, fig in ctx.out["charts"].items():
                    st.subheader(f"📈 Chart: {name}")
                    st.plotly_chart(fig)

            if ctx.out["text"]:
                for name, txt in ctx.out["text"].items():
                    st.subheader(f"📝 Text: {name}")
                    st.code(txt)

        finally:
            # Restore stdout
            sys.stdout = old_stdout
            captured_output.close()

    except Exception as e:
        st.error(f"💥 Module crashed: {e}")
        st.code(traceback.format_exc())