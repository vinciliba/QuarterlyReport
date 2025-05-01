import os
import sys
import sqlite3, json, textwrap, pathlib, base64
from jinja2 import Environment, FileSystemLoader, select_autoescape
import streamlit as st
from streamlit.components.v1 import html    
from streamlit_ace import st_ace
import pandas as pd
from datetime import datetime
import traceback # Import traceback for better error reporting
from ingestion.db_utils import (
    get_all_reports, create_new_report,
    define_expected_table, get_suggested_structure,
    load_report_params, upsert_report_param, save_report_object,
    get_report_object, list_report_objects, delete_report_object, get_variable_status,
    fetch_vars_for_report
)
from ingestion.report_check import check_report_readiness
import mammoth, io, docx
import pyperclip
from pathlib import Path

DB_PATH = 'database/reporting.db'


# Adjust path as needed
sys.path.append(os.path.abspath(os.path.join(os.path.dirname(__file__), '..')))

# =================================
# === Session State Initialization ===
# =================================
# Initialize session state variables used across reruns

# For multi-select report metadata deletion confirmation
if 'pending_delete_reports' not in st.session_state:
    st.session_state.pending_delete_reports = None

# For multi-select upload record deletion confirmation
if 'pending_delete_uploads' not in st.session_state:
    st.session_state.pending_delete_uploads = None

# For auto-selecting the report after a new one is created in Single Upload tab
if 'selected_report_after_create' not in st.session_state:
    st.session_state.selected_report_after_create = None

# Add this new state variable to track the file uploader state
if 'file_uploader_key_counter' not in st.session_state:
    st.session_state.file_uploader_key_counter = 0

# Track completed modules for staged report generation
if 'completed_modules' not in st.session_state:
    st.session_state.completed_modules = []

# Track the last chosen report to persist across reruns
if 'last_chosen_report' not in st.session_state:
    st.session_state.last_chosen_report = None

# Track the last cutoff date to persist across reruns
if 'last_cutoff_date' not in st.session_state:
    st.session_state.last_cutoff_date = None
# =================================
# === End Session State Init ===
# =================================

# --- Rest of your script starts here ---
# DB_PATH = 'database/reporting.db'
# os.makedirs("app_files", exist_ok=True)
# init_db(db_path=DB_PATH)
# etc.
# Use absolute imports for clarity and robustness
try:
    from ingestion.db_utils import (
        init_db,
        get_existing_rule,
        insert_sheet_rule,
        get_transform_rules,
        save_transform_rules,
        insert_upload_log,
        create_new_report,
        get_all_reports,
        is_report_complete,
        get_expected_tables,
        # Import other necessary functions from db_utils
        register_file_alias, # Added based on likely use
        get_alias_for_file, # Added
        update_alias_status, # Added
        get_alias_last_load, # Added
        log_cutoff # Added
    )
except ImportError as e:
    st.error(f"Failed to import db_utils: {e}")
    st.stop() # Stop execution if core imports fail


os.makedirs("app_files", exist_ok=True)

# --- Page config, style ---
st.set_page_config(layout="centered")
st.markdown("""
<style>
.main .block-container {
    max-width: 1000px;
    margin: auto;
}
.stToast {
    max-width: 500px;
    margin: 0 auto;
}
</style>
""", unsafe_allow_html=True)

# --- Init DB ---
init_db(db_path=DB_PATH)

# --- Navigation Selectbox ---
# Define your sections
sections = {
    "🚀 Choose Workflow": "workflow",
    "📤 Export Report"  : "export_report",
    "📂 Single File Upload": "single_upload",
    "📦 Mass Upload": "mass_upload",
    "🔎 View History": "history",
    "🛠️ Admin" :  "report_structure",
    "🖋️ Template Editor": "template_editor",
    
}

# Use a selectbox for navigation
st.sidebar.title("Navigation")
selected_section_key = st.sidebar.selectbox("Go to", list(sections.keys()))
selected_section = sections[selected_section_key]

# --- Content Area based on Selection ---

# Add a debug print to confirm script execution and selected section
print(f"DEBUG: Script rerun. Selected section: {selected_section_key} ({selected_section})")
st.write(f"", unsafe_allow_html=True) # HTML comment for browser source check
# ──────────────────────────────────────────────────
# WORKFLOW – Launch & Validation (Refactored)
# ──────────────────────────────────────────────────
# In ui/streamlit_app.py (replace the existing "workflow" section)
if selected_section == "workflow":
    from ingestion.report_check import check_report_readiness
    import importlib
    from pathlib import Path
    from docx import Document

    st.title("📊 Report Launch & Validation")

    # Step 1: Pick a report
    reports_df = get_all_reports(DB_PATH)
    if reports_df.empty:
        st.info("No reports defined yet. Create one in “Single File Upload”.")
        st.stop()

    default_report = st.session_state.last_chosen_report if st.session_state.last_chosen_report in reports_df["report_name"].tolist() else reports_df["report_name"].iloc[0]
    chosen_report = st.selectbox("Choose report to validate", reports_df["report_name"].tolist(), index=reports_df["report_name"].tolist().index(default_report))
    st.session_state.last_chosen_report = chosen_report

    # Step 2: Choose cutoff & tolerance
    default_cutoff = st.session_state.last_cutoff_date if st.session_state.last_cutoff_date else datetime.today()
    cutoff_date = st.date_input("📅 Reporting cutoff date", value=default_cutoff)
    st.session_state.last_cutoff_date = cutoff_date
    tolerance_days = st.slider("⏱️ Tolerance (days before cutoff)", 0, 15, 3)

    # Step 3: Validate readiness
    validation_df, ready = check_report_readiness(chosen_report, cutoff_date, tolerance_days, db_path=DB_PATH)
    st.markdown("### Validation results")
    st.dataframe(validation_df, hide_index=True, use_container_width=True)

    if not ready:
        st.error("⛔ Missing or stale uploads detected. Fix them before launch.")
        st.markdown("Please go to the **Single File Upload** or **Mass Upload** section to upload the required data files.")
        st.stop()

    st.success("🎉 All required tables uploaded and within the valid window!")

    # Step 4: Show available DOCX template
    template_dir = Path("reporting/templates/docx")
    template_file = next(
        (f for f in template_dir.glob("*.docx") if chosen_report.replace(" ", "_") in f.name),
        None
    )

    if template_file:
        st.markdown(f"🖋️ **Template in use:** `{template_file.name}`")
    else:
        st.warning(f"No template found for `{chosen_report}` in `/templates/docx/`")
        st.stop()

    # Step 5: Load report module + registry
    report_to_module = {
        "Quarterly_Report": "reporting.quarterly_report",
        "Invoice_Summary": "reporting.invoice_summary",
    }

    mod_path = report_to_module.get(chosen_report)
    if not mod_path:
        st.warning(f"No Python module path mapped for `{chosen_report}`.")
        st.stop()

    try:
        mod = importlib.import_module(mod_path)
        print(f"Loaded module: {mod}, dir: {dir(mod)}")  # Debug print
        MODULES = getattr(mod, "MODULES", {})
    except Exception as e:
        st.error(f"Error loading report modules: {e}")
        st.stop()

    if not MODULES:
        st.warning("No modules found for this report.")
        st.stop()

    # Step 6: Choose subset of modules
    st.markdown("### 🧩 Select modules to run")
    remaining_modules = [k for k in MODULES.keys() if k not in st.session_state.completed_modules]
    enabled_module_names = st.multiselect(
        "Choose modules",
        list(MODULES.keys()),
        default=remaining_modules
    )
    selected_modules = {k: MODULES[k] for k in enabled_module_names}

    # Step 7: Show progress of completed modules
    st.markdown("### 📈 Progress")
    if st.session_state.completed_modules:
        st.write(f"Completed modules: {', '.join(st.session_state.completed_modules)}")
    else:
        st.write("No modules completed yet.")

    # Step 8: Option to reset the staged report
    if st.button("🧹 Reset staged report"):
        if template_file:
            st.session_state.staged_docx = Document(str(template_file))
            st.toast("Staged DOCX initialized from template.", icon="📝")
        else:
            st.session_state.staged_docx = Document()
            st.toast("Staged DOCX initialized empty.", icon="🆕")
        st.session_state.completed_modules = []
        st.toast("Progress reset.", icon="🔄")

    # Step 9: Run report with selected modules
    if st.button("🚀 Run Report"):
        if not selected_modules:
            st.warning("Please select at least one module to run.")
        else:
            status = st.empty()
            status.info(f"Launching report **{chosen_report}** …")

            try:
                with st.spinner("Running selected modules…"):
                    if not hasattr(mod, "run_report"):
                        raise RuntimeError(f"Module `{mod_path}` has no `run_report()`")

                    ctx, run_results = mod.run_report(
                        cutoff_date=cutoff_date,
                        tolerance=tolerance_days,
                        db_path=DB_PATH,
                        selected_modules=selected_modules
                    )

            except Exception as e:
                status.empty()
                st.error(f"💥 Error running report: {e}")
                st.code(traceback.format_exc())
            else:
                status.empty()
                st.markdown("### 📝 Module Run Results")
                all_ok = True
                for mod_name, state, msg in run_results:
                    if state == "✅ Success":
                        st.success(f"{mod_name}: {state}")
                        if mod_name not in st.session_state.completed_modules:
                            st.session_state.completed_modules.append(mod_name)
                    else:
                        st.error(f"{mod_name}: {state}")
                        if msg:
                            st.code(msg)
                        all_ok = False

                if all_ok:
                    st.toast("Report finished", icon="✅")
                    st.success(f"Report **{chosen_report}** completed successfully.")

                    # Call the report-specific render_and_export function
                    if hasattr(mod, "render_and_export"):
                        final_report_path = mod.render_and_export(chosen_report, cutoff_date, ctx)
                        st.success(f"Final report saved as `{final_report_path}` in `app_files/`")
                    else:
                        st.warning(f"No render_and_export function found for `{chosen_report}`.")

                else:
                    st.warning("Some modules failed. See details above.")

                log_cutoff(
                    chosen_report,
                    f"Validation_{cutoff_date}",
                    cutoff_date.isoformat(),
                    validated=True,
                    db_path=DB_PATH,
                )

# === streamlit_app.py (new section) ===
elif selected_section == "export_report":
    st.title("📤 Export Final Report")
    reports_df = get_all_reports(DB_PATH)
    if reports_df.empty:
        st.info("No reports available.")
        st.stop()
        
    chosen_report = st.selectbox("Select Report", reports_df["report_name"].tolist())
    df_vars = get_variable_status(chosen_report, DB_PATH)

    def highlight_age(row):
        return ["background-color: #ffd6d6" if row.age_days > 5 else "" for _ in row]

    st.markdown("### 🧠 Variables Snapshot")
    st.dataframe(df_vars.style.apply(highlight_age, axis=1), use_container_width=True)

    st.markdown("### 📄 Select Template or Use Existing Partial")
    template_dir = Path("reporting/templates/docx")
    available_templates = list(template_dir.glob("*.docx"))
    template_choices = [str(t.name) for t in available_templates]
    template_choice = st.selectbox("Choose a template:", template_choices)
    template_path = template_dir / template_choice

    if st.button("📄 Render Final Report"):
        from docxtpl import DocxTemplate
        from jinja2 import Environment, DebugUndefined

        tpl = DocxTemplate(str(template_path))
        context = fetch_vars_for_report(chosen_report, DB_PATH)

        # Show missing anchors
        missing = tpl.get_undeclared_template_variables() - context.keys()
        if missing:
            st.warning(f"⚠️ Missing anchors: {', '.join(missing)}")
           
        env = Environment(undefined=DebugUndefined)
        tpl.render(context, jinja_env=env)

        output_dir = Path("app_files") / chosen_report / datetime.today().strftime("%Y-%m-%d")
        output_dir.mkdir(parents=True, exist_ok=True)
        output_path = output_dir / "Final_Report.docx"
        tpl.save(str(output_path))

        st.success(f"Final report saved to: {output_path}")
        with open(output_path, "rb") as f:
            st.download_button("📥 Download Final Report", f.read(), file_name="Final_Report.docx")

    if "staged_docx" in st.session_state:
        with st.expander("💾 Save or Preview Staged Report", expanded=True):
            filename = f"partial_{chosen_report}_{datetime.today().strftime('%Y-%m-%d')}.docx"
            if st.button("💾 Save to app_files"):
                out_path = Path("app_files") / filename
                st.session_state.staged_docx.save(str(out_path))
                st.success(f"Saved partial report as `{filename}` in `app_files/`")

    #--------------------------------------------------------------------------
    # --- Section for Creating a New Report -----------------------------------
    #--------------------------------------------------------------------------
elif selected_section == "single_upload":
    st.title("📂 Single File Upload")
    # Load reports from DB
    reports_df = get_all_reports(DB_PATH)
    report_names = ["-- Create new --"] + reports_df["report_name"].tolist()

    # Default selection logic
    default_report_name = (
        st.session_state.selected_report_after_create
        if st.session_state.selected_report_after_create in report_names
        else report_names[0]
    )

    chosen_report = st.selectbox("Choose a report:", report_names, index=report_names.index(default_report_name))
    if chosen_report == "-- Create new --":
        st.markdown("### Create New Report")
        new_report_name = st.text_input("Enter the name for the new report:", key="new_report_name_input")

        if st.button("➕ Create Report", key="create_report_button"):
            if new_report_name.strip():
                try:
                    # Create the report in the database
                    create_new_report(new_report_name.strip(), DB_PATH)

                    # Store the new report name in session state for auto-selection after rerun
                    st.session_state.selected_report_after_create = new_report_name.strip()

                    # Show feedback and rerun
                    st.success(f"Report '{new_report_name.strip()}' created!")
                    st.toast(f"Report '{new_report_name.strip()}' created!", icon="✅") # Use toast for notification across rerun
                    st.rerun()

                except ValueError as e:
                    st.error(str(e)) # Report specific database error (e.g., name already exists)
                except Exception as e:
                    st.error(f"An unexpected error occurred during report creation: {e}")
                    print(traceback.format_exc()) # Print traceback

            else:
                st.warning("Please provide a name for the new report.")

        # Use st.stop() to prevent the file upload UI from showing when in "Create new" mode
        st.stop()

    else: # A report *other* than "-- Create new --" is selected
            st.markdown(f"### Upload file for report: `{chosen_report}`")

            # File uploader
            uploaded_file = st.file_uploader("📁 Upload .xlsx or .csv file", type=["xlsx", "xls", "csv"], key=f"single_upload_file_uploader_{st.session_state.file_uploader_key_counter}")

            # Stop here if no file is uploaded yet, otherwise proceed with file processing
            if not uploaded_file:
                st.info("Awaiting file upload...")
                st.stop() # Keep st.stop() here

            # --- File Processing and Transformation Logic ---
            filename = uploaded_file.name
            extension = os.path.splitext(filename)[1].lower()
            filename_wo_ext = os.path.splitext(filename)[0]
            # Derive a raw table name (used for the 'table_name' column in upload_log)
            default_raw_table_name = f"raw_{filename_wo_ext.lower()}"

            st.success(f"📥 File received: `{filename}`")
            st.info(f"Linking upload to report: `{chosen_report}`")


            # --- Sheet Selection ---
            existing_sheet, saved_start_row = get_existing_rule(filename, DB_PATH)
            sheet_to_use = None
            start_row = 0

            if extension in [".xlsx", ".xls"]:
                try:
                    uploaded_file.seek(0)
                    xls = pd.ExcelFile(uploaded_file)
                    sheet_names = xls.sheet_names

                    if existing_sheet and existing_sheet in sheet_names:
                        st.success(f"✅ Found saved sheet: `{existing_sheet}`")
                        use_existing = st.checkbox("Use saved sheet rule?", value=True, key="use_existing_sheet_checkbox")
                        sheet_to_use = existing_sheet if use_existing else None
                        start_row = saved_start_row if (use_existing and saved_start_row is not None) else 0

                    if sheet_to_use is None:
                        sheet_to_use = st.selectbox("📑 Select sheet to upload:", sheet_names, key="sheet_select_box")
                        start_row = st.number_input("Start row (0-indexed header):", min_value=0, value=0, key="sheet_start_row_input")
                    else:
                        start_row = st.number_input("Start row (0-indexed header):", min_value=0, value=saved_start_row or 0, key="sheet_start_row_input_existing")

                    if not sheet_to_use:
                        st.warning("Please select a sheet.")
                        st.stop()

                    if st.button("💾 Save sheet rule", key="save_sheet_rule_button"):
                        insert_sheet_rule(filename, sheet_to_use, start_row, DB_PATH)
                        st.success(f"Rule saved for `{filename}`: sheet `{sheet_to_use}`, header row {start_row + 1} (0-indexed row {start_row})")
                        st.rerun()

                except Exception as excel_error:
                    st.error(f"Error reading Excel file or selecting sheet: {excel_error}")
                    print(traceback.format_exc())
                    st.stop()

            elif extension == ".csv":
                sheet_to_use = "CSV_SHEET"
                start_row = st.number_input("Start row (0-indexed header):", min_value=0, value=saved_start_row or 0, key="csv_start_row_input")

                if st.button("💾 Save sheet rule", key="save_csv_sheet_rule_button"):
                    insert_sheet_rule(filename, sheet_to_use, start_row, DB_PATH)
                    st.success(f"Rule saved for `{filename}`: CSV, header row {start_row + 1} (0-indexed row {start_row})")
                    st.rerun()

            else:
                st.error("❌ Unsupported file format.")
                st.stop()


            # --- Load Preview DataFrame based on Sheet/Row Rules (Common Block) ---
            preview_df = None
            try:
                if extension in [".xlsx", ".xls"] and sheet_to_use:
                    uploaded_file.seek(0)
                    xls = pd.ExcelFile(uploaded_file)
                    preview_df = xls.parse(sheet_to_use, skiprows=start_row)
                elif extension == ".csv":
                    uploaded_file.seek(0)
                    preview_df = pd.read_csv(uploaded_file, skiprows=start_row)
            except Exception as load_preview_error:
                st.error(f"Error loading data preview from file: {load_preview_error}")
                print(traceback.format_exc())
                st.stop()


            if preview_df is None or preview_df.empty:
                st.warning("Could not load data preview or data is empty after applying start row.")
                st.stop()


            # --- Transformation Rules ---
            rules = get_transform_rules(filename, sheet_to_use, DB_PATH)
            current_columns = preview_df.columns.tolist()

            if not rules:
                st.info("No saved transformation rules found for this file/sheet. Proposing initial rules based on preview columns.")
                rules = [
                    {"original_column": col, "renamed_column": col, "included": True, "filename": filename, "sheet": sheet_to_use, "created_at": datetime.now().isoformat()}
                    for col in current_columns
                ]

            st.markdown("### Column Transformations")
            st.write("Review and rename/exclude columns. Changes are saved upon Upload.")

            cols_header = st.columns([0.4, 0.4, 0.2])
            with cols_header[0]: st.write("**Renamed Column**")
            with cols_header[1]: st.write("**Original Column**")
            with cols_header[2]: st.write("**Include?**")
            st.markdown("---")

            edited_rules = []
            for rule in rules:
                original_col = rule.get("original_column", "Unknown Column")

                if original_col in preview_df.columns:
                    widget_key_prefix = f"{filename}_{sheet_to_use}_{original_col}"

                    col_r, col_o, col_i = st.columns([0.4, 0.4, 0.2])
                    with col_r:
                        renamed_col = st.text_input(f"Rename {original_col}", value=rule.get("renamed_column", original_col), label_visibility="collapsed", key=f"rename_{widget_key_prefix}")
                    with col_o:
                        st.text_input(f"Original {original_col}", value=original_col, disabled=True, label_visibility="collapsed", key=f"original_{widget_key_prefix}")
                    with col_i:
                        included = st.checkbox(f"Include {original_col}", value=rule.get("included", True), label_visibility="collapsed", key=f"include_{widget_key_prefix}")

                    edited_rules.append({
                        "filename": filename,
                        "sheet": sheet_to_use,
                        "original_column": original_col,
                        "renamed_column": renamed_col,
                        "included": included,
                        "created_at": rule.get("created_at", datetime.now().isoformat())
                    })


            # --- Determine Default / Proposed Database Table Name ---
            # Derive a default name from the filename (cleaned)
            default_derived_table_name = filename_wo_ext.lower().replace(" ", "_").replace("-", "_").replace(".", "_")
            import re
            default_derived_table_name = re.sub(r'\W+', '_', default_derived_table_name) # Replace non-alphanumeric with _
            default_derived_table_name = re.sub(r'^_+', '', default_derived_table_name) # Remove leading underscores from cleaning
            if not default_derived_table_name:
                default_derived_table_name = "uploaded_data" # Fallback if cleaning results in empty string


            # --- Database Table Naming Section ---
            st.markdown("### Database Table Naming")
            st.write("Specify the name for the database table where this file's data will be stored.")
            st.write(f"Proposed default name based on filename: `{default_derived_table_name}`")


            # Provide a text input defaulted to the derived name
            user_defined_table_name = st.text_input(
                "Database Table Name:",
                value=default_derived_table_name, # Default to the derived name
                help="Must be unique and contain only letters, numbers, and underscores.",
                key="database_table_name_input"
            )

            # Validate the user's input name
            cleaned_input_name = user_defined_table_name.strip()
            # More robust validation pattern
            is_valid_name = bool(re.fullmatch(r'[a-zA-Z_][a-zA-Z0-9_]*', cleaned_input_name))

            if not cleaned_input_name:
                st.warning("Table name cannot be empty.")
                is_valid_name = False
            elif not is_valid_name:
                st.warning("Table name must start with a letter or underscore and contain only letters, numbers, or underscores.")


            # --- Transformed Data Preview ---
            # Apply rules to preview and display it
            cols_to_include_original = [r["original_column"] for r in edited_rules if r["included"] and r["original_column"] in preview_df.columns]
            preview_rename_map = {r["original_column"]: r["renamed_column"] for r in edited_rules if r["included"] and r["original_column"] in preview_df.columns}

            final_preview_df = pd.DataFrame() # Initialize
            if cols_to_include_original: # Only proceed if there are columns to include
                try:
                    final_preview_df = preview_df[cols_to_include_original].copy()
                    final_preview_df.rename(columns=preview_rename_map, inplace=True)
                    st.info("✅ Preview updated based on transformations.")
                except KeyError as e:
                    st.error(f"Error applying transformations for preview: Column missing or misspelled after rules applied: {e}. Showing empty preview.")
                    print(traceback.format_exc())
                except Exception as e:
                    st.error(f"An unexpected error occurred applying transformations for preview: {e}. Showing empty preview.")
                    print(traceback.format_exc())
            else:
                st.warning("No columns selected for inclusion after transformations.")


            st.markdown("### 👀 Transformed Data Preview (First 10 rows)")
            if final_preview_df.empty:
                st.info("Preview is empty or no columns selected for display.")
            else:
                st.dataframe(final_preview_df.head(10))


            # --- Save Rules & Upload Button ---
            st.markdown("---")
            # The button should only be enabled if a valid table name is provided
            if st.button("✅ Save Rules & Upload Data", key="save_rules_and_upload_button", disabled=not is_valid_name):
                # This code block only runs when the button is clicked AND the name is valid

                st.info("Starting data upload process...")
                now = datetime.now().isoformat()

                # --- Save Edited Transform Rules ---
                for rule in edited_rules:
                    rule["created_at"] = now
                save_transform_rules(edited_rules, DB_PATH)
                st.success("✅ Transformation rules saved.")


                # --- Apply Rules to FULL DataFrame and Upload ---
                full_df = None
                try:
                    uploaded_file.seek(0)
                    if extension in [".xlsx", ".xls"]:
                        xls = pd.ExcelFile(uploaded_file)
                        full_df = xls.parse(sheet_to_use, skiprows=start_row)
                    elif extension == ".csv":
                        full_df = pd.read_csv(uploaded_file, skiprows=start_row)
                    st.write("🧪 DEBUG: Full DataFrame shape:", full_df.shape)
                except Exception as load_full_error:
                    st.error(f"Error loading full data from file: {load_full_error}")
                    print(traceback.format_exc())
                    # Don't stop here, let the error message show, the check below handles full_df being None
                    full_df = None # Ensure full_df is None if loading fails

                # --- Validate + Transform ---
                if full_df is not None and not full_df.empty:
                    try:
                        # Filter and rename the *full* DataFrame based on the saved/edited rules
                        cols_to_include_original_full = [r["original_column"] for r in edited_rules if r["included"] and r["original_column"] in full_df.columns]
                        upload_df = full_df[cols_to_include_original_full].copy()
                        upload_rename_map = {r["original_column"]: r["renamed_column"] for r in edited_rules if r["included"] and r["original_column"] in full_df.columns}
                        upload_df.rename(columns=upload_rename_map, inplace=True)

                        st.write("🧪 DEBUG: Upload DF shape:", upload_df.shape)
                        st.write("🧪 DEBUG: Upload DF columns:", upload_df.columns.tolist())

                        if upload_df.empty:
                            st.warning("Upload resulted in an empty dataset after applying transformations. No data uploaded.")
                            # Don't stop here, let the warning show, the rest of the block handles upload_df being empty
                            # exit the try block gracefully
                            raise ValueError("Upload resulted in empty data after transformations.") # Raise to jump to except


                        # --- Final Alias & Database Save using User-Defined Name ---

                        # Use the user-defined name as the actual table name
                        final_table_name_in_db = cleaned_input_name # Use the validated name from the text input
                        st.write("🧪 DEBUG: Final table name:", final_table_name_in_db)

                        # Register the user's chosen table name AS the alias for this filename.
                        # This makes the alias match the table name for simplicity.
                        file_alias = final_table_name_in_db
                        register_file_alias(filename, file_alias, db_path=DB_PATH)
                        st.info(f"Alias `{file_alias}` registered for file `{filename}`.")

                        with sqlite3.connect(DB_PATH) as conn:
                            # Insert upload log using the original filename, the derived *raw* table_name (for log detail),
                            # the selected report, AND the user-defined table name AS the alias
                            upload_id = insert_upload_log(
                                filename, 
                                default_raw_table_name, 
                                upload_df.shape[0], 
                                upload_df.shape[1],
                                chosen_report, 
                                table_alias=file_alias, 
                                db_path=DB_PATH # Log the alias (user's name)
                            )
                            st.success(f"✅ Upload log created (ID: {upload_id}).")
                            # Append metadata
                            upload_df["upload_id"] = upload_id
                            upload_df["uploaded_at"] = now
                            
                            st.write("🧪 DEBUG: Saving table", final_table_name_in_db)
                            st.write("🧪 DEBUG: Upload DF preview", upload_df.head(3))

                            # Save to SQL
                            upload_df.to_sql(final_table_name_in_db, conn, index=False, if_exists="replace")
                            conn.commit()
                            sample_df = pd.read_sql_query(f"SELECT * FROM `{final_table_name_in_db}` LIMIT 5", conn)
                            st.markdown(f"### 🧪 Sample of `{final_table_name_in_db}` from DB")
                            st.dataframe(sample_df)

                            # Only show success if data is there
                            if not sample_df.empty:
                                st.success(f"📦 Uploaded to table `{final_table_name_in_db}` with {len(sample_df)}+ rows visible.")
                            else:
                                st.warning(f"⚠️ Upload to `{final_table_name_in_db}` completed, but no data appears in preview. Check start row / rules.")

                            # Verify insert
                            try:
                                result = pd.read_sql_query(f"SELECT COUNT(*) AS cnt FROM `{final_table_name_in_db}`", conn)
                                st.write(f"🧪 DEBUG: Row count in `{final_table_name_in_db}` after insert:", result['cnt'].iloc[0])
                            except Exception as verify_error:
                                st.error(f"⚠️ Failed to verify row count: {verify_error}")

                    except ValueError as ve: # Catch the ValueError for empty data gracefully
                        st.warning(f"Upload failed: {ve}")
                    except Exception as e:
                        st.error(f"❌ Upload failed: {e}")
                        import traceback
                        st.code(traceback.format_exc())

                    # 🔄 Trigger reset
                    st.toast(f"✅ Upload complete for `{filename}` → `{final_table_name_in_db}`", icon="📥")
                    st.session_state.file_uploader_key_counter += 1
                    st.rerun()

                elif full_df is not None and full_df.empty:
                    st.warning("Upload failed: Loaded data from file is empty after applying start row.")

                else: # full_df is None
                    st.error("❌ Upload failed: Could not load data from file.")


    # --- End of single upload section --- 

elif selected_section == "mass_upload":
    # --- Section: Mass Upload (Original Tab 2) ---
    print("DEBUG: Entering mass_upload section") # Debug print
    try:
        st.subheader("📦 Mass Upload from `app_files/`")
        st.write("Place .xlsx or .csv files in the `app_files/` directory.")


        reports_df = get_all_reports(DB_PATH)
        # st.write("🛠️ Debug: Reports loaded:", reports_df.shape) # Keep debug if helpful

        if reports_df.empty:
            st.warning("⚠️ No reports found. Please create a report in 'Single File Upload' section first.")
        else:
            chosen_report = st.selectbox("Select report to upload files for:", reports_df["report_name"].tolist())

            all_files = [f for f in os.listdir("app_files") if f.endswith((".csv", ".xlsx", ".xls"))]
            # st.write("🛠️ Debug: Files found:", all_files) # Keep debug if helpful

            if not all_files:
                st.info("📂 No files found in the `app_files/` folder.")
            else:
                st.write("🗂️ Files ready for upload:", all_files)

                if st.button("🚀 Upload All"):
                    st.info("Starting mass upload...")
                    upload_successful_count = 0
                    upload_failed_count = 0
                    for file in all_files:
                        st.write(f"--- Processing `{file}` ---")
                        file_path = os.path.join("app_files", file)
                        filename_wo_ext = os.path.splitext(file)[0]
                        ext = os.path.splitext(file)[1].lower()

                        # --- Get Rules ---
                        sheet, start_row = get_existing_rule(file, DB_PATH)
                        start_row = start_row or 0
                        sheet_to_use = sheet # Initialize sheet_to_use


                        df = None
                        try:
                            # --- Read File ---
                            if ext in [".xlsx", ".xls"]:
                                try:
                                    xls = pd.ExcelFile(file_path)
                                    if not sheet or sheet not in xls.sheet_names:
                                        st.warning(f"⚠️ `{file}`: No valid sheet rule found or sheet '{sheet}' not in file. Skipping.")
                                        upload_failed_count += 1
                                        continue # Skip this file
                                    sheet_to_use = sheet # Use saved sheet
                                    df = xls.parse(sheet_to_use, skiprows=start_row)
                                except FileNotFoundError:
                                     st.error(f"❌ `{file}`: File not found during processing. Skipping.")
                                     upload_failed_count += 1
                                     continue
                                except Exception as parse_error:
                                     st.error(f"❌ `{file}`: Error parsing sheet '{sheet_to_use}' starting at row {start_row}: {parse_error}. Skipping.")
                                     print(traceback.format_exc())
                                     upload_failed_count += 1
                                     continue

                            elif ext == ".csv":
                                sheet_to_use = "CSV_SHEET" # Placeholder for CSV rule lookup
                                try:
                                     df = pd.read_csv(file_path, skiprows=start_row)
                                except FileNotFoundError:
                                     st.error(f"❌ `{file}`: File not found during processing. Skipping.")
                                     upload_failed_count += 1
                                     continue
                                except Exception as csv_error:
                                     st.error(f"❌ `{file}`: Error reading CSV file starting at row {start_row}: {csv_error}. Skipping.")
                                     print(traceback.format_exc())
                                     upload_failed_count += 1
                                     continue
                            else:
                                st.warning(f"⚠️ `{file}`: Unsupported file format. Skipping.")
                                upload_failed_count += 1
                                continue

                            # --- Apply Transform Rules ---
                            rules = get_transform_rules(file, sheet_to_use, DB_PATH)
                            if rules and df is not None:
                                included_cols_original_names = [r["original_column"] for r in rules if r["included"] and r["original_column"] in df.columns]
                                rename_map = {r["original_column"]: r["renamed_column"] for r in rules if r["included"] and r["original_column"] in df.columns}

                                try:
                                     df = df[included_cols_original_names]
                                     df.rename(columns=rename_map, inplace=True)
                                     st.write(f"Applied transform rules for `{file}`.")
                                except KeyError as e:
                                     st.error(f"❌ `{file}`: Error applying transformations - column missing: {e}. Skipping.")
                                     upload_failed_count += 1
                                     continue
                                except Exception as e:
                                     st.error(f"❌ `{file}`: Unexpected error applying transformations: {e}. Skipping.")
                                     print(traceback.format_exc())
                                     upload_failed_count += 1
                                     continue


                            # --- Upload to DB ---
                            if df is not None and not df.empty:
                                # --- Alias Handling ---
                                # In mass upload, you need a way to get the alias for each file.
                                # Option A: Assume alias is filename_wo_ext (less flexible)
                                # Option B: Look up alias in file_alias_map based on filename
                                # Option B is better. Need to ensure files were 'registered' with an alias first (e.g., via Single Upload or another process)
                                # If no alias is found, should it be skipped?
                                file_alias = get_alias_for_file(file, DB_PATH)
                                if not file_alias:
                                     st.warning(f"⚠️ `{file}`: No alias registered for this file. Skipping upload. Please register it first (e.g., via Single Upload).")
                                     upload_failed_count += 1
                                     continue

                                now = datetime.now().isoformat()
                                default_raw_table_name = f"raw_{filename_wo_ext.lower()}" # Still track this

                                with sqlite3.connect(DB_PATH) as conn:
                                     # Insert upload log using the alias
                                    upload_id = insert_upload_log(
                                         file, default_raw_table_name, df.shape[0], df.shape[1], chosen_report, table_alias=file_alias, db_path=DB_PATH
                                     )

                                    # Update alias status (needs file_id)
                                    cur = conn.cursor()
                                    cur.execute("SELECT id FROM file_alias_map WHERE filename = ?", (file,))
                                    file_id_row = cur.fetchone()
                                    file_id = file_id_row[0] if file_id_row else None # Should exist if get_alias_for_file worked

                                    if file_id:
                                         update_alias_status(file_alias, file, db_path=DB_PATH)


                                    # Save the data to a table named after the alias
                                    final_table_name_in_db = file_alias # Use alias for final tables
                                    df["upload_id"] = upload_id # Add upload_id column
                                    df["uploaded_at"] = now # Add uploaded_at column

                                    # Save to DB
                                    df.to_sql(final_table_name_in_db, conn, index=False, if_exists="replace") # Use replace


                                st.success(f"✅ Uploaded `{file}` to table `{final_table_name_in_db}` (Alias: `{file_alias}`).")
                                upload_successful_count += 1
                            elif df is not None and df.empty:
                                st.warning(f"⚠️ `{file}`: File processed successfully, but resulted in an empty dataframe after applying rules. No data uploaded.")
                                upload_failed_count += 1
                            # else df is None -> already handled by continue

                        except Exception as e:
                            st.error(f"❌ An unhandled error occurred processing `{file}`: {e}")
                            print(traceback.format_exc()) # Print traceback to console
                            upload_failed_count += 1
                        st.write("--- Done processing `{file}` ---")

                    st.markdown("---") # Separator after mass upload
                    st.info(f"Mass Upload Summary: {upload_successful_count} successful, {upload_failed_count} failed.")
                    st.rerun() # Rerun to show updated status/history

    except Exception as e:
        st.error(f"💥 An error occurred in the Mass Upload section: {e}")
        print(traceback.format_exc()) # Print traceback to console

elif selected_section == "history":
    # --- Section: Upload History & Management ---
    print("DEBUG: Entering history section")
    try:
        st.subheader("🔎 Upload History & Management")

        # --- Confirmation Step for Upload Deletion (appears when state is set) ---
        if st.session_state.pending_delete_uploads is not None:
            upload_ids_to_confirm_delete = st.session_state.pending_delete_uploads
            num_uploads_to_delete = len(upload_ids_to_confirm_delete)

            st.error("🛑 **Critical Warning: Confirm Upload Deletion** 🛑")
            st.markdown(f"""
            You are about to delete the following **{num_uploads_to_delete} specific upload record(s)**:
            """)

            # Fetch details for the uploads being deleted to display in the warning
            try:
                with sqlite3.connect(DB_PATH) as conn:
                    placeholders = ','.join('?' for _ in upload_ids_to_confirm_delete)
                    query = f"""
                        SELECT filename, table_alias, uploaded_at, rows, cols, id
                        FROM upload_log
                        WHERE id IN ({placeholders})
                        ORDER BY uploaded_at DESC
                    """
                    uploads_to_delete_df = pd.read_sql_query(query, conn, params=upload_ids_to_confirm_delete)
                    uploads_to_delete_df.rename(columns={'id': 'upload_id'}, inplace=True)

                if not uploads_to_delete_df.empty:
                    st.dataframe(uploads_to_delete_df)
                else:
                    st.warning("Details for selected uploads could not be retrieved.")

            except Exception as fetch_error:
                 st.error(f"Error fetching upload details for confirmation: {fetch_error}")
                 print(traceback.format_exc())


            st.markdown("""
            **Deleting an upload record will:**
            * **Permanently remove the history entry** for this specific file upload from the logs.
            * **Permanently delete the data rows from the corresponding data table** (e.g., the table named after the alias) that belong to this specific upload (`WHERE upload_id = this_id`).
            * **This action CANNOT be undone.**
            """)

            col_confirm_upload, col_cancel_upload = st.columns(2)

            with col_confirm_upload:
                if st.button(f"✅ **Confirm Delete {num_uploads_to_delete} Upload Record(s) AND Data**", key="confirm_execute_delete_uploads"):
                    st.info("Executing upload and data deletion...")
                    deleted_count = 0
                    uploads_list_to_delete = st.session_state.pending_delete_uploads

                    with sqlite3.connect(DB_PATH) as conn:
                         cursor = conn.cursor()
                         for upload_id in uploads_list_to_delete:
                             try:
                                 cursor.execute("SELECT table_alias FROM upload_log WHERE id = ?", (upload_id,))
                                 row = cursor.fetchone()
                                 if row and row[0]:
                                     table_to_clean = row[0]

                                     delete_data_sql = f"DELETE FROM {table_to_clean} WHERE upload_id = ?"
                                     try:
                                        cursor.execute(delete_data_sql, (upload_id,))
                                        st.write(f"✅ Deleted data from `{table_to_clean}` for upload ID `{upload_id}`")
                                     except sqlite3.OperationalError as oe:
                                         st.warning(f"⚠️ Data table `{table_to_clean}` or upload_id column not found for upload ID `{upload_id}`. Skipping data deletion for this upload.")
                                     except Exception as data_delete_e:
                                         st.error(f"❌ Error deleting data for upload ID `{upload_id}` from `{table_to_clean}`: {data_delete_e}")
                                         print(traceback.format_exc())

                                 cursor.execute("DELETE FROM upload_log WHERE id = ?", (upload_id,))
                                 st.write(f"✅ Deleted log entry for upload ID `{upload_id}`")
                                 deleted_count += 1

                             except Exception as e:
                                 st.error(f"❌ Failed to delete upload record for ID `{upload_id}`: {e}")
                                 print(traceback.format_exc())

                         conn.commit()
                    st.session_state.pending_delete_uploads = None
                    st.success(f"Completed deleting {deleted_count} upload record(s) and associated data.")
                    st.toast(f"Deleted {deleted_count} uploads!", icon="🗑️")
                    st.rerun()

            with col_cancel_upload:
                if st.button("↩️ Cancel Upload Deletion", key="cancel_delete_uploads"):
                    st.session_state.pending_delete_uploads = None
                    st.info("Upload deletion cancelled.")
                    st.rerun()

            st.stop()


        # --- Confirmation Step for Metadata Deletion (appears when state is set, secondary) ---
        elif st.session_state.pending_delete_reports is not None:
            reports_to_confirm_delete = st.session_state.pending_delete_reports
            num_reports_to_delete = len(reports_to_confirm_delete)

            st.error("🛑 **Critical Warning: Confirm Metadata Deletion** 🛑")
            st.markdown(f"""
            You are about to delete the metadata for the following **{num_reports_to_delete} report(s)**:
            """)
            for report_name in reports_to_confirm_delete:
                st.write(f"- `{report_name}`")

            st.markdown("""
            **Deleting metadata will:**
            * Remove the report definition itself from the app's list.
            * **Permanently delete all upload history records associated with these specific reports.** (You will lose information about which files were uploaded when *for this report*).
            * Delete any defined expected structure or cutoff logs linked to these reports.
            * **Effectively "reset" the application's knowledge and validation logic specifically for these reports.**

            **This action does NOT delete the actual data tables** (like tables named after aliases) in the database that contain the content of the uploaded files.
            """)

            col_confirm_meta, col_cancel_meta = st.columns(2)

            with col_confirm_meta:
                if st.button(f"✅ **Confirm Delete Metadata for {num_reports_to_delete} Report(s)**", key="confirm_execute_multi_delete"):
                    st.info("Executing deletion...")
                    deleted_count = 0
                    reports_list_to_delete = st.session_state.pending_delete_reports

                    with sqlite3.connect(DB_PATH) as conn:
                         cursor = conn.cursor()
                         for report_name in reports_list_to_delete:
                             try:
                                 cursor.execute("SELECT COUNT(*) FROM reports WHERE report_name = ?", (report_name,))
                                 if cursor.fetchone()[0] > 0:
                                     cursor.execute("DELETE FROM reports WHERE report_name = ?", (report_name,))
                                     cursor.execute("DELETE FROM upload_log WHERE report_name = ?", (report_name,))
                                     cursor.execute("DELETE FROM report_structure WHERE report_name = ?", (report_name,))
                                     cursor.execute("DELETE FROM report_cutoff_log WHERE report_name = ?", (report_name,))
                                     deleted_count += 1
                                     st.write(f"✅ Deleted metadata for '{report_name}'")
                                 else:
                                     st.write(f"ℹ️ Report '{report_name}' not found, skipping deletion.")

                             except Exception as e:
                                 st.error(f"❌ Failed to delete metadata for '{report_name}': {e}")
                                 print(traceback.format_exc())

                         conn.commit()
                    st.session_state.pending_delete_reports = None
                    st.success(f"Completed deleting metadata for {deleted_count} report(s).")
                    st.toast(f"Metadata deleted for {deleted_count} report(s)!", icon="🗑️")
                    st.rerun()

            with col_cancel_meta:
                if st.button("↩️ Cancel Deletion", key="cancel_multi_delete"):
                    st.session_state.pending_delete_reports = None
                    st.info("Metadata deletion cancelled.")
                    st.rerun()

            st.stop()


        # --- Normal History View (if no confirmation pending) ---
        else:
            # --- Multi-Select Report Metadata Deletion (checkboxes) ---
            st.markdown("### Delete Report Metadata")
            reports_df_all = get_all_reports(DB_PATH)

            if reports_df_all.empty:
                 st.info("No reports defined to delete metadata for.")
            else:
                st.write("Select reports whose metadata you want to delete:")
                selected_reports_for_deletion = []
                for index, row in reports_df_all.iterrows():
                    report_name = row["report_name"]
                    checkbox_key = f"delete_report_metadata_checkbox_{report_name}"
                    if st.checkbox(report_name, key=checkbox_key):
                        selected_reports_for_deletion.append(report_name)

                if selected_reports_for_deletion:
                    num_selected = len(selected_reports_for_deletion)
                    st.warning(f"You have selected {num_selected} report(s). Deleting metadata cannot be undone.")
                    if st.button(f"Initiate Metadata Deletion for Selected ({num_selected})", key="initiate_multi_delete_checkbox_button"):
                        st.session_state.pending_delete_reports = selected_reports_for_deletion
                        st.rerun()

            st.markdown("---")


            # --- Report Last Upload Summary ---
            st.markdown("### Report Last Upload Summary")
            with sqlite3.connect(DB_PATH) as conn:
                logs_summary = pd.read_sql_query("""
                    SELECT report_name, MAX(uploaded_at) AS last_refresh
                    FROM upload_log
                    GROUP BY report_name
                    ORDER BY last_refresh DESC
                """, conn)

            if logs_summary.empty:
                st.info("No uploads recorded for any report.")
            else:
                st.dataframe(logs_summary)

            st.markdown("---")

            # --- Report-Specific Upload Details (Single Select & Data Editor with Checkbox Workaround) ---
            st.markdown("### Report-Specific Upload Details")
            report_names_with_uploads = logs_summary["report_name"].tolist()

            if not report_names_with_uploads:
                 st.info("Upload history is empty. Upload a file first to see details.")
            else:
                 selected_report_history = st.selectbox(
                     "Select report to inspect history",
                     report_names_with_uploads,
                     key="selectbox_inspect_history"
                )

                 # Fetch detailed logs using the correct 'id' column
                 with sqlite3.connect(DB_PATH) as conn:
                     report_logs_detail = pd.read_sql_query("""
                         SELECT filename, table_alias, uploaded_at, rows, cols, id
                         FROM upload_log
                         WHERE report_name = ?
                         ORDER BY uploaded_at DESC
                     """, conn, params=(selected_report_history,))

                 st.markdown(f"#### 📁 Files uploaded for `{selected_report_history}`")
                 if report_logs_detail.empty:
                      st.info(f"No specific upload logs found for '{selected_report_history}'.")
                 else:
                      # --- WORKAROUND for selection using a Checkbox Column ---
                      # 1. Rename 'id' to 'upload_id' for display (optional, but matches previous display)
                      report_logs_detail_display = report_logs_detail.rename(columns={'id': 'upload_id'})

                      # 2. Create a copy and insert the 'Select' checkbox column at the beginning
                      df_with_selections = report_logs_detail_display.copy()
                      df_with_selections.insert(0, "Select", False) # Insert boolean column

                      st.write("Tick the box next to the upload(s) you want to delete:")

                      # 3. Use st.data_editor with column_config for the checkbox
                      # Disable editing on original columns
                      # Get the names of the original columns BEFORE inserting 'Select'
                      original_columns = report_logs_detail_display.columns.tolist()
                      disabled_columns = original_columns # Disable editing on all original columns


                      edited_df = st.data_editor(
                          df_with_selections, # Pass the dataframe *with* the Select column
                          use_container_width=True,
                          hide_index=True, # Hide pandas index
                          # Configure the Select column as a checkbox
                          # Also configure other columns like upload_id to be non-editable if desired
                          column_config={
                              "Select": st.column_config.CheckboxColumn(required=True, help="Select this upload record for deletion"),
                              "upload_id": st.column_config.NumberColumn("Upload ID", disabled=True), # Make ID column non-editable in editor
                              "filename": st.column_config.TextColumn("Filename", disabled=True),
                              "table_alias": st.column_config.TextColumn("Table Alias", disabled=True),
                              "uploaded_at": st.column_config.DatetimeColumn("Uploaded At", disabled=True),
                              "rows": st.column_config.NumberColumn("Rows", disabled=True),
                              "cols": st.column_config.NumberColumn("Cols", disabled=True),
                          },
                          disabled=disabled_columns, # Disable editing on all original data columns
                          # REMOVE selection="multiple-rows" <-- This parameter is NOT USED in this workaround
                          key=f"data_editor_upload_history_checkbox_{selected_report_history}" # Unique key
                      )

                      # 4. Filter the edited dataframe to find selected rows (where 'Select' is True)
                      # This filters the dataframe returned by the editor based on the checkbox column
                      selected_rows = edited_df[edited_df.Select == True]


                      if not selected_rows.empty:
                          # 5. Get the actual 'id' (DB primary key) from the selected rows
                          # The 'upload_id' column in the edited_df (which came from original 'id')
                          selected_upload_ids = selected_rows['upload_id'].tolist()

                          num_selected_uploads = len(selected_upload_ids)
                          st.warning(f"You have selected {num_selected_uploads} upload record(s). Initiating deletion will show a final confirmation step.")

                          # Button to initiate deletion of selected uploads
                          if st.button(f"Initiate Deletion of Selected Uploads ({num_selected_uploads})", key="initiate_delete_uploads_button"):
                              st.session_state.pending_delete_uploads = selected_upload_ids
                              st.rerun()

                      # --- End WORKAROUND ---


            st.markdown("---")

            # --- Danger Zone: Delete ALL ---
            st.markdown("### Danger Zone: Delete ALL Reports AND Data")
            st.warning("This will permanently delete ALL report definitions, ALL metadata, AND ALL data tables created by uploads!")

            confirm_delete_all_data = st.checkbox("I understand this will delete ALL data tables.", key="confirm_delete_all_data_checkbox")
            confirm_delete_all_reports = st.checkbox("I also confirm deletion of ALL report definitions and metadata.", key="confirm_delete_all_reports_checkbox")


            if confirm_delete_all_data and confirm_delete_all_reports and st.button("🔥 Execute Delete EVERYTHING", key="execute_delete_all_button"):
                st.info("Starting global deletion...")
                with sqlite3.connect(DB_PATH) as conn:
                    cursor = conn.cursor()
                    st.info("Deleting all data tables...")
                    cursor.execute("SELECT name FROM sqlite_master WHERE type='table';")
                    all_tables = [row[0] for row in cursor.fetchall()]
                    metadata_tables = ['upload_log', 'file_alias_map', 'alias_upload_status', 'sheet_rules', 'transform_rules', 'reports', 'report_structure', 'report_cutoff_log', 'sqlite_sequence', 'geometry_columns', 'spatial_ref_sys']
                    data_tables_to_drop = [tbl for tbl in all_tables if tbl not in metadata_tables]

                    dropped_count = 0
                    for table in data_tables_to_drop:
                        try:
                            cursor.execute(f"DROP TABLE IF EXISTS {table};")
                            st.write(f"Dropped table: `{table}`")
                            dropped_count += 1
                        except Exception as drop_e:
                             st.error(f"Error dropping table `{table}`: {drop_e}")

                    conn.commit()
                    st.success(f"Completed dropping {dropped_count} data table(s).")

                    st.info("Deleting all report metadata...")
                    try:
                        conn.execute("DELETE FROM reports")
                        conn.execute("DELETE FROM upload_log")
                        conn.execute("DELETE FROM report_structure")
                        conn.execute("DELETE FROM report_cutoff_log")
                        conn.execute("DELETE FROM file_alias_map")
                        conn.execute("DELETE FROM alias_upload_status")
                        conn.execute("DELETE FROM sheet_rules")
                        conn.execute("DELETE FROM transform_rules")
                        conn.commit()
                        st.success("🧨 All report metadata (definitions, logs, rules) were deleted.")
                    except Exception as e:
                        conn.rollback()
                        st.error(f"Error deleting metadata tables: {e}")
                        print(traceback.format_exc())

                st.success("Global deletion process finished.")
                st.toast("All data and metadata deleted!", icon="💥")
                st.rerun()


    except Exception as e:
        st.error(f"💥 An error occurred in the Upload History & Management section: {e}")
        print(traceback.format_exc())
        
# ──────────────────────────────────────────────────
# TAB 5: Admin – Report Structure
# selected_section == "report_structure"
# ──────────────────────────────────────────────────


elif selected_section == "report_structure":

    from ingestion.db_utils import (
        get_suggested_structure,   # ⬅️ NEW helper
        define_expected_table      # re-use save helper
    )

    st.subheader("🛠️ Admin – Define Required Tables for a Report")

    # --------------------------------------------------
    # 1) Choose or create a report
    # --------------------------------------------------
    reports_df = get_all_reports(DB_PATH)
    report_names = ["-- Create new --"] + reports_df["report_name"].tolist() \
                   if not reports_df.empty else ["-- Create new --"]

    chosen_report = st.selectbox("Select Report", report_names, key="admin_report_select")

    if chosen_report == "-- Create new --":
        new_report_name = st.text_input("New report name")
        if st.button("➕ Create Report (Admin)"):
            if new_report_name.strip():
                try:
                    create_new_report(new_report_name.strip(), DB_PATH)
                    st.success(f"Report '{new_report_name}' created.")
                    st.rerun()
                except ValueError as e:
                    st.error(e)
            else:
                st.warning("Enter a name.")
        st.stop()

    # --------------------------------------------------
    # 2) Current structure
    # --------------------------------------------------
    with sqlite3.connect(DB_PATH) as conn:
        structure_df = pd.read_sql_query(
            """
            SELECT id, table_alias, required, expected_cutoff
            FROM report_structure
            WHERE report_name = ?
            ORDER BY table_alias
            """,
            conn, params=(chosen_report,)
        )

    st.markdown("### Current Structure")
    if structure_df.empty:
        st.info("No aliases defined yet.")
    else:
        st.dataframe(structure_df, use_container_width=True)

    st.markdown("---")

    # --------------------------------------------------
    # 2b) Inline edit existing rows
    # --------------------------------------------------
    if not structure_df.empty:
        st.markdown("### ✏️ Edit existing rows")
        editable_df = structure_df.copy()

        edited_df = st.data_editor(
            editable_df,
            column_config={
                "id": st.column_config.NumberColumn(disabled=True),
                "table_alias": st.column_config.TextColumn(disabled=True),
                "required": st.column_config.CheckboxColumn(),
                "expected_cutoff": st.column_config.TextColumn(),
            },
            hide_index=True,
            use_container_width=True,
            num_rows="fixed",  # prevent adding new rows here
        )

        if st.button("💾 Save Changes to Existing Rows"):
            diff_ct = 0
            with sqlite3.connect(DB_PATH) as conn:
                cur = conn.cursor()
                for _, row in edited_df.iterrows():
                    orig = structure_df.loc[structure_df.id == row["id"]].iloc[0]
                    if (
                        int(row["required"]) != int(orig["required"])
                        or (row["expected_cutoff"] or "") != (orig["expected_cutoff"] or "")
                    ):
                        cur.execute(
                            """
                            UPDATE report_structure
                            SET required = ?, expected_cutoff = ?
                            WHERE id = ?
                            """,
                            (int(row["required"]), row["expected_cutoff"], int(row["id"])),
                        )
                        diff_ct += 1
                conn.commit()

            if diff_ct:
                st.success(f"Updated {diff_ct} row(s).")
                st.rerun()
            else:
                st.info("No changes detected.")

    # --------------------------------------------------
    # 3) Suggested aliases from upload_log (NEW)
    # --------------------------------------------------
    suggested = get_suggested_structure(chosen_report, DB_PATH)
    if suggested:
        st.markdown("### 🚀 Suggested aliases from recent uploads")
        st.write("Tick the aliases you want to add as **required**:")
        accept_states = {}
        for alias in suggested:
            accept_states[alias] = st.checkbox(f"Add `{alias}`", value=True, key=f"suggest_{alias}")

        if st.button("➕ Accept Selected Suggestions"):
            added = 0
            for alias, do_add in accept_states.items():
                if do_add:
                    define_expected_table(chosen_report, alias, required=True, db_path=DB_PATH)
                    added += 1
            st.success(f"Added {added} alias(es) to report structure.")
            st.rerun()
    else:
        st.info("No new aliases found in uploads that are missing from the structure.")

    st.markdown("---")

    # --------------------------------------------------
    # 4) Manual Add / Edit
    # --------------------------------------------------
    st.markdown("### ➕ Add or Edit an Alias")

    col1, col2, col3 = st.columns([0.4, 0.2, 0.4])
    with col1:
        alias_input = st.text_input("Table alias", key="alias_input")
    with col2:
        required_input = st.checkbox("Required?", value=True, key="required_input")
    with col3:
        cutoff_input = st.text_input("Expected cutoff (free text)", placeholder="e.g. Month-End", key="cutoff_input")

    if st.button("💾 Save Alias"):
        alias = alias_input.strip()
        if not alias:
            st.warning("Alias cannot be empty.")
        else:
            try:
                define_expected_table(
                    chosen_report,
                    alias,
                    required=required_input,
                    expected_cutoff=cutoff_input or None,
                    db_path=DB_PATH
                )
                st.success(f"Alias '{alias}' saved / updated.")
                st.rerun()
            except Exception as e:
                st.error(f"Error saving alias: {e}")

    # --------------------------------------------------
    # 5) Delete aliases
    # --------------------------------------------------
    if not structure_df.empty:
        st.markdown("---")
        st.markdown("### 🗑️ Delete Aliases")

        ids_to_del = st.multiselect(
            "Select IDs to delete",
            structure_df["id"].tolist(),
            format_func=lambda x: f"{x} – "
                + structure_df.loc[structure_df.id == x, "table_alias"].values[0],
        )

        if ids_to_del and st.button(f"Delete {len(ids_to_del)} alias(es)"):
            with sqlite3.connect(DB_PATH) as conn:
                placeholders = ",".join("?" for _ in ids_to_del)
                conn.execute(
                    f"DELETE FROM report_structure WHERE id IN ({placeholders})",
                    ids_to_del,
                )
                conn.commit()
            st.success("Selected aliases deleted.")
            st.rerun()

    # ------------------------------------------------------------------
    # 6) parameters editor
    # ------------------------------------------------------------------
    st.markdown("## ⚙️ Report parameters")
    DEFAULTS_BY_REPORT = {                       # only used if nothing in DB yet
        "Quarterly_Report": { "H2020_positions": ['all','EXPERTS','ADG','STG','POC','COG','SYG'],
            "HEU_positions":   ['all','EXPERTS','ADG','STG','POC','COG','SYG'],
            "list_H2020_payTypes": ['Interim Payments','Final Payments','Experts Payment '],
            "list_HE_payTypes":    ['Prefinancing Payments','Interim Payments','Final Payments','Experts Payment '],
            "calls_list": [
                'ERC-2023-POC (01/23)','ERC-2023-POC (04/23)','ERC-2023-POC (09/23)',
                'ERC-2023-STG','ERC-2023-COG','ERC-2023-SyG','ERC-2023-ADG',
                'ERC-2024-POC (03/24)','ERC-2024-STG','ERC-2024-COG','ERC-2024-SyG'
            ],
            "TTI_Targets" : {
                'ERC-2023-POC (01/23)' : 106, 'ERC-2023-POC (04/23)' : 98,'ERC-2023-POC (09/23)':97,
                'ERC-2023-STG': 304,'ERC-2023-COG' : 309,'ERC-2023-SyG' : 371,'ERC-2023-ADG' : 332,
                'ERC-2024-POC (03/24)' : 106,'ERC-2024-STG' : 300,'ERC-2024-COG' : 309,'ERC-2024-SyG' : 371
            },
            "TTS_Targets" : {
                'ERC-2023-POC (01/23)' : 120, 'ERC-2023-POC (04/23)' : 120,'ERC-2023-POC (09/23)':120,
                'ERC-2023-STG': 120,'ERC-2023-COG' : 120,'ERC-2023-SyG' : 140,'ERC-2023-ADG' : 120,
                'ERC-2024-POC (03/24)' : 120,'ERC-2024-STG' : 120,'ERC-2024-COG' : 120,'ERC-2024-SyG' :140
            },
            "TTG_Targets" : {
                'ERC-2023-POC (01/23)' : 226, 'ERC-2023-POC (04/23)' : 218,'ERC-2023-POC (09/23)':217,
                'ERC-2023-STG': 424,'ERC-2023-COG' : 429,'ERC-2023-SyG' : 511,'ERC-2023-ADG' : 452,
                'ERC-2024-POC (03/24)' :226,'ERC-2024-STG' : 420,'ERC-2024-COG' : 429,'ERC-2024-SyG' :511
            }
      }
    }
    params = load_report_params(chosen_report, DB_PATH) or DEFAULTS_BY_REPORT.get(chosen_report, {})

    # ----- defaults only for the selected report ----------------------
    DEFAULTS_FOR_THIS = DEFAULTS_BY_REPORT.get(chosen_report, {})

    # ------------------------------------------------------------------
    # 0) bootstrap the authoritative dict just once
    # ------------------------------------------------------------------
   
    if "params_full" not in st.session_state:
        # first pull what we already have in the database
        from_db = load_report_params(chosen_report, DB_PATH)  # may be {}
        # DB values win over defaults
        st.session_state.params_full = {**DEFAULTS_FOR_THIS, **from_db}

    # make a short alias for readability
    params = st.session_state.params_full


    # ---------- editable JSON textarea ----------------------------------
    txt_key = "param_editor"           # 1-line alias for the textarea’s key


    # ----------  one-off fill of the textarea -------------------------
    def refresh_editor_from_params() -> None:
        """Put the current `params` dict into the text-area."""
        st.session_state[txt_key] = json.dumps(params, indent=2)


    # --- if a widget (Add-key / Quick-add) set a _params_draft, promote it
    if "_params_draft" in st.session_state:
        st.session_state[txt_key] = st.session_state.pop("_params_draft")
        st.toast("✅ Parameters updated – review & save", icon="🎉")

    # Ensure the textarea has an initial value (first render only)
    if txt_key not in st.session_state:
        refresh_editor_from_params()

    # ---------- editable JSON textarea --------------------------------
    txt = st.text_area(
        "Edit JSON",
        key=txt_key,
        height=320,
    )

    # Always keep `params` as the object that drives the UI -------------
    try:
        params: dict = json.loads(st.session_state[txt_key] or "{}")
    except json.JSONDecodeError as e:
        st.error(f"JSON syntax error: {e}")
        st.stop()

    # ---------- 3. ADD NEW TOP-LEVEL KEY  ----------------------------------------
    with st.expander("➕ Add a new top-level key", expanded=False):
        new_key   = st.text_input("Key name", key="new_top_key")
        init_type = st.selectbox("Initial type", ["list", "dict", "str", "int"], key="init_type")
        if st.button("Create key"):
            if new_key in params:
                st.warning("Key already exists!")
            else:
                params[new_key] = [] if init_type == "list" else {} if init_type == "dict" \
                                else "" if init_type == "str"  else 0
                refresh_editor_from_params()
                st.session_state.pop("new_top_key", None)
                st.toast("✅ key created")
                st.rerun()

    st.markdown("---")

    # ---------- 4. QUICK PATCH EXISTING KEYS  -----------------------------------
    # -------------------------------------------------------------------
    # 4. QUICK PATCH / EDIT / DELETE
    # -------------------------------------------------------------------
    st.markdown("### 🔄 Quick add / update")

    def commit_and_rerun(params_dict: dict, toast_msg: str):
        """
        Write json draft → session-state, toast, force rerun
        (we cannot touch param_editor while it’s mounted)
        """
        st.session_state["_params_draft"] = json.dumps(params_dict, indent=2)
        st.toast(toast_msg, icon="✅")
        st.rerun()

    gkey = st.selectbox("Existing key", list(params.keys()), key="qa_sel")

    # detect the value type of the selected key
    current_val = params[gkey]
    is_list = isinstance(current_val, list)
    is_dict = isinstance(current_val, dict)

    vtype = st.selectbox(
        "Action",
        [
            "replace scalar",            # for str / number
            "append to list",            # for list
            "edit list item",            # for list
            "delete list item",          # for list
            "add / update dict entry",   # for dict
            "delete dict entry"          # for dict
        ],
        key="qa_action"
    )

    # ───────── scalar replace ──────────────────────────────────────────
    if vtype == "replace scalar":
        if is_list or is_dict:
            st.warning("Selected key is not a scalar.")
        else:
            new_val = st.text_input("New value", key="qa_scalar_in")
            if st.button("💾 Replace value", key="qa_scalar_btn"):
                params[gkey] = new_val
                commit_and_rerun(params, "Scalar replaced")

    # ───────── list append  ────────────────────────────────────────────
    elif vtype == "append to list":
        if not is_list:
            st.warning("Selected key is not a list.")
        else:
            new_item = st.text_input("Item to append", key="qa_append_in")
            if st.button("➕ Append", key="qa_append_btn"):
                params[gkey].append(new_item)
                commit_and_rerun(params, "Item appended")

    # ───────── list edit  ──────────────────────────────────────────────
    elif vtype == "edit list item":
        if not is_list:
            st.warning("Selected key is not a list.")
        else:
            sel = st.selectbox("Pick item", current_val, key="qa_list_pick")
            new_item = st.text_input("New value", value=sel, key="qa_list_edit")
            if st.button("✏️ Update item", key="qa_list_edit_btn"):
                idx = current_val.index(sel)
                params[gkey][idx] = new_item
                commit_and_rerun(params, "List item updated")

    # ───────── list delete  ────────────────────────────────────────────
    elif vtype == "delete list item":
        if not is_list:
            st.warning("Selected key is not a list.")
        else:
            sel = st.selectbox("Pick item to delete", current_val, key="qa_list_del_pick")
            if st.button("🗑️ Delete item", key="qa_list_del_btn"):
                params[gkey].remove(sel)
                commit_and_rerun(params, "List item deleted")

    # ───────── dict upsert  ────────────────────────────────────────────
    elif vtype == "add / update dict entry":
        if not is_dict:
            st.warning("Selected key is not a dict.")
        else:
            subk = st.text_input("Sub-key", key="qa_dict_key")
            subv = st.text_input("Sub-value", key="qa_dict_val")
            if st.button("💾 Add / update", key="qa_dict_upd_btn"):
                params[gkey][subk] = subv
                commit_and_rerun(params, "Dict entry saved")

    # ───────── dict delete  ────────────────────────────────────────────
    elif vtype == "delete dict entry":
        if not is_dict:
            st.warning("Selected key is not a dict.")
        else:
            subk = st.selectbox("Pick sub-key to delete", list(current_val.keys()), key="qa_dict_del_pick")
            if st.button("🗑️ Delete entry", key="qa_dict_del_btn"):
                params[gkey].pop(subk, None)
                commit_and_rerun(params, "Dict entry deleted")


    st.markdown("---")

    # ---------- 5. CALL-TARGETS MANAGER (optional) -------------------------------
    if isinstance(params.get("call_targets"), dict):
        st.markdown("### 🎯 Maintain `call_targets`")

        ct_dict = params["call_targets"]
        call_names = list(ct_dict.keys())

        sel = st.selectbox("Select call (or '-- New call --')",
                        ["-- New call --"] + call_names, key="ct_sel")

        call_key = st.text_input("Call name", value="" if sel == "-- New call --" else sel,
                                key="ct_name")
        if call_key:
            entry = ct_dict.get(call_key, {"TTI": "", "TTS": "", "TTG": ""})
            c1,c2,c3 = st.columns(3)
            with c1:  tti = st.text_input("TTI", value=str(entry["TTI"]), key="ct_tti")
            with c2:  tts = st.text_input("TTS", value=str(entry["TTS"]), key="ct_tts")
            with c3:  ttg = st.text_input("TTG", value=str(entry["TTG"]), key="ct_ttg")

            def _num(v):
                try: return int(v) if float(v).is_integer() else float(v)
                except: return v.strip()

            if st.button("💾 Save / update call", key="ct_save"):
                ct_dict[call_key] = {"TTI": _num(tti), "TTS": _num(tts), "TTG": _num(ttg)}
                params["call_targets"] = ct_dict
                st.session_state[txt_key] = json.dumps(params, indent=2)
                st.success("Saved (remember to **Save parameters to DB**)")

            if sel != "-- New call --" and st.button("🗑️ Delete call", key="ct_del"):
                del ct_dict[call_key]
                params["call_targets"] = ct_dict
                st.session_state[txt_key] = json.dumps(params, indent=2)
                st.success("Deleted (remember to **Save parameters to DB**)")

        st.markdown("---")

    # --------------------------------------------------
    # 7) ☰ Report-Modules mapping
    # --------------------------------------------------
    from ingestion.db_utils import (
        list_report_modules,
        upsert_report_module,
        delete_report_module
    )

    # Dynamically load the MODULES registry that lives inside
    # reporting/<report-package>/__init__.py   (or modules.py, see notes)
    import importlib

    try:
        report_pkg_lookup = {
            "Quarterly_Report": "reporting.quarterly_report", 
            # Add more mappings as needed
        }
        

        report_pkg_name = report_pkg_lookup.get(chosen_report)
        print("Importing report module from:", report_pkg_name)
        if not report_pkg_name:
            st.error(f"⚠️ No module path defined for report “{chosen_report}”.")
            st.stop()
        MODULES = importlib.import_module(report_pkg_name).MODULES
    except (ModuleNotFoundError, AttributeError):
        st.error(f"⚠️ No MODULES registry found for report “{chosen_report}”.")
        st.stop()

    st.markdown("## ⚙️ Modules for this report")

    # ── current mapping ────────────────────────────────────────────────
    mods_df = list_report_modules(chosen_report, DB_PATH)
    if mods_df.empty:
        st.info("No modules assigned yet.")
    else:
        st.dataframe(mods_df, hide_index=True, use_container_width=True)

    # ── add / edit mapping ─────────────────────────────────────────────
    with st.expander("➕ Add / modify module mapping", expanded=False):
        mod_name  = st.selectbox("Module class", list(MODULES.keys()))
        run_order = st.number_input("Run order (1 = first)", min_value=1, value=1, step=1)
        enabled   = st.checkbox("Enabled", value=True)

        if st.button("💾 Save / update mapping"):
            upsert_report_module(
                report_name = chosen_report,
                module_name = mod_name,
                run_order   = run_order,
                enabled     = enabled,
                db_path     = DB_PATH
            )
            st.success("Mapping saved.")
            st.rerun()

    # ── delete mapping ────────────────────────────────────────────────
    if not mods_df.empty:
        del_id = st.selectbox("Select row-ID to delete", mods_df.id.tolist())
        if st.button("🗑️ Delete selected mapping"):
            delete_report_module(del_id, DB_PATH)
            st.success("Mapping deleted.")
            st.rerun()


    # ---------- 6. SAVE EVERYTHING ----------------------------------------------
    if st.button("💾 Save parameters to DB", key="save_to_db"):
        try:
            final_params = json.loads(st.session_state[txt_key])
            for k, v in final_params.items():
                upsert_report_param(chosen_report, k, v, DB_PATH)
            st.success("Parameters saved to database.")
        except json.JSONDecodeError as e:
            st.error(f"JSON error: {e}")


# ---------------------------------------------------
# Template Management
# ---------------------------------------------------
elif selected_section == "template_editor":
    st.subheader("🛠️ Template Management")

    # ─────────────────────── Imports ───────────────────────
    import base64
    import docx
    import docxedit
    from docx import Document
    from pathlib import Path
    import mammoth

    # ─────────────────────── Streamlit Imports ───────────────────────
    import streamlit as st

    # ─────────────────────── Constants ───────────────────────
    TEMPLATE_DIRECTORY = Path("reporting/templates/docx")
    TEMPLATE_DIRECTORY.mkdir(parents=True, exist_ok=True)
    KEY_TEMPLATE_PATH = "template_docx_path"

    # ─────────────────────── Session State Initialization ───────────────────────
    if KEY_TEMPLATE_PATH not in st.session_state:
        st.session_state[KEY_TEMPLATE_PATH] = None

    # ─────────────────────── Helper Functions ───────────────────────
    def list_template_files():
        """Return sorted list of *.docx files in the template directory."""
        return sorted(path.name for path in TEMPLATE_DIRECTORY.glob("*.docx"))

    def extract_docx_paragraphs(doc):
        """Extract paragraphs from the DOCX for display in the placeholder selector."""
        paragraphs = []
        for para in doc.paragraphs:
            text = "".join(run.text for run in para.runs)
            if text.strip():  # Only include non-empty paragraphs
                paragraphs.append(text)
        return paragraphs

    def add_placeholder_to_docx(doc, placeholder_name, para_index=None):
        """Add a Jinja2 placeholder to the DOCX at a specific paragraph using docxedit."""
        placeholder = f"{{{{ {placeholder_name} }}}}"
        if para_index is not None and 0 <= para_index < len(doc.paragraphs):
            # Use docxedit to insert the placeholder at the specified paragraph
            current_text = "".join(run.text for run in doc.paragraphs[para_index].runs)
            if current_text.strip():
                # Append the placeholder to the existing text in the paragraph
                new_text = current_text + " " + placeholder
                docxedit.replace_string(doc, old_string=current_text, new_string=new_text)
            else:
                # If the paragraph is empty, just set the placeholder
                docxedit.replace_string(doc, old_string="", new_string=placeholder)
        else:
            # Append to the end if no valid index is provided
            doc.add_paragraph(placeholder)
        return doc

    def docx_to_html(docx_path):
        """Convert DOCX to HTML using mammoth for preview."""
        try:
            with open(docx_path, "rb") as docx_file:
                result = mammoth.convert_to_html(docx_file)
                html_content = result.value  # The converted HTML
                messages = result.messages  # Any conversion messages/warnings
                if messages:
                    st.warning("Conversion warnings: " + "; ".join(str(msg) for msg in messages))
                return html_content
        except Exception as e:
            st.error(f"Failed to convert DOCX to HTML: {e}")
            return None

    # ─────────────────────── Template Management ───────────────────────
    st.markdown("### 📄 Template File")
    template_files = ["-- new --"] + list_template_files()
    template_selector = st.selectbox(
        "Choose a file",
        template_files,
        key="template_file_selector"
    )

    if template_selector == "-- new --":
        new_file_name = st.text_input(
            "New file name",
            value="template.docx",
            help="Must end with .docx"
        )
        active_template_path = TEMPLATE_DIRECTORY / new_file_name
        # Create a blank DOCX if it doesn't exist
        if not active_template_path.exists():
            doc = Document()
            doc.add_paragraph("New Template - Edit in Word and re-upload")
            doc.save(active_template_path)
    else:
        active_template_path = TEMPLATE_DIRECTORY / template_selector

    # Load the template
    st.session_state[KEY_TEMPLATE_PATH] = str(active_template_path)

    # Upload DOCX
    uploaded_docx = st.file_uploader("Upload DOCX", type="docx", key="docx_upload")
    if uploaded_docx:
        active_template_path.write_bytes(uploaded_docx.read())
        st.session_state[KEY_TEMPLATE_PATH] = str(active_template_path)

    # ─────────────────────── Download for Editing ───────────────────────
    st.markdown("### ✏️ Edit Template")
    st.info(
        "Streamlit cannot edit DOCX files directly with pixel-perfect fidelity. "
        "Download the template, edit it in Microsoft Word (or a compatible editor), "
        "and re-upload it to make formatting changes. Use the section below to add Jinja2 placeholders."
    )
    if active_template_path.exists():
        with open(active_template_path, "rb") as f:
            docx_bytes = f.read()
        b64 = base64.b64encode(docx_bytes).decode()
        st.markdown(
            f'<a download="{active_template_path.name}" href="data:application/vnd.openxmlformats-officedocument.wordprocessingml.document;base64,{b64}">Download Template for Editing</a>',
            unsafe_allow_html=True
        )
    else:
        st.warning("No template selected or uploaded.")

    # ─────────────────────── File Actions ───────────────────────
    st.markdown("### 💾 File Actions")
    save_button, download_button, delete_button = st.columns(3)

    if save_button.button("💾 Save", disabled=not active_template_path.name.endswith(".docx")):
        st.success(f"Template is saved at: {active_template_path}")

    if download_button.button("📥 Download", disabled=not active_template_path.exists()):
        try:
            with open(active_template_path, "rb") as f:
                docx_bytes = f.read()
            b64 = base64.b64encode(docx_bytes).decode()
            st.markdown(
                f'<a download="{active_template_path.name}" href="data:application/vnd.openxmlformats-officedocument.wordprocessingml.document;base64,{b64}">Click to download</a>',
                unsafe_allow_html=True
            )
        except Exception as e:
            st.error(f"Download failed: {e}")

    if delete_button.button("🗑️ Delete", disabled=not active_template_path.exists()):
        try:
            active_template_path.unlink()
            st.warning(f"Deleted: {active_template_path.name}")
            st.session_state[KEY_TEMPLATE_PATH] = None
            st.rerun()
        except Exception as e:
            st.error(f"Delete failed: {e}")