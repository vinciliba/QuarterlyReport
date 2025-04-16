import os
import sys
import sqlite3
import streamlit as st
import pandas as pd
import re
from datetime import datetime
from st_aggrid import GridOptionsBuilder, AgGrid, GridUpdateMode, DataReturnMode

# Adjust path as needed
sys.path.append(os.path.abspath(os.path.join(os.path.dirname(__file__), '..')))
from ingestion.db_utils import (
    init_db, 
    get_existing_rule, insert_sheet_rule, get_transform_rules, save_transform_rules,
    insert_upload_log, create_new_report, get_all_reports
)

DB_PATH = 'database/reporting.db'
os.makedirs("app_files", exist_ok=True)

# 1) Page config, style
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

# 2) Init DB
init_db(db_path=DB_PATH)

# 3) Tabs
tabs = st.tabs(["🚀 Choose Workflow", "📂 Single File Upload", "📦 Mass Upload", "🔎 View History"])

# ──────────────────────────────────────────────────
# TAB 0: Info
# ──────────────────────────────────────────────────
with tabs[0]:
    st.title("📊 Quarterly Report Ingestion Console")
    st.subheader("1) Choose a workflow")
    st.markdown("""
    - **Single File Upload**: upload a file, define or override a rule (Tab 2).
    - **Mass Upload**: handle multiple files for a single “report” (Tab 3).
    - **View History**: see a chronological log of all file uploads (Tab 4).
    """)

# ──────────────────────────────────────────────────
# TAB 1: Single File Upload & Transformation
# ──────────────────────────────────────────────────
with tabs[1]:
    st.subheader("📂 Single File Upload & Transformation")

    reports_df = get_all_reports(DB_PATH)
    report_names = ["-- Create new --"] + reports_df["report_name"].tolist() if not reports_df.empty else ["-- Create new --"]
    chosen_report = st.selectbox("Select a report", report_names)

    if chosen_report == "-- Create new --":
        new_report_name = st.text_input("New Report Name")
        if st.button("➕ Create Report"):
            if new_report_name.strip():
                create_new_report(new_report_name, DB_PATH)
                st.success(f"Report '{new_report_name}' created!")
                st.experimental_rerun()
            else:
                st.warning("Please provide a name.")
        st.stop()
    else:
        st.markdown(f"**Selected Report**: `{chosen_report}`")

    uploaded_file = st.file_uploader("📁 Upload .xlsx or .csv file", type=["xlsx", "xls", "csv"])
    if not uploaded_file:
        st.info("Awaiting file upload...")
        st.stop()

    filename = uploaded_file.name
    extension = os.path.splitext(filename)[1].lower()
    st.success(f"📥 File received: `{filename}`")

    preview_df = None
    sheet_to_use = None
    saved_rule = get_existing_rule(filename, DB_PATH)

    try:
        if extension in [".xlsx", ".xls"]:
            xls = pd.ExcelFile(uploaded_file)
            all_sheets = xls.sheet_names

            if saved_rule:
                st.success(f"💾 Found saved sheet rule: `{saved_rule}`")
                use_saved = st.checkbox("Use saved sheet rule?", value=True)
                if use_saved:
                    sheet_to_use = saved_rule
                else:
                    selected = st.selectbox("📑 Select sheet to override the saved rule:", all_sheets)
                    confirm_override = st.button("✅ Confirm Sheet Selection")
                    if confirm_override:
                        sheet_to_use = selected
                        st.success(f"✅ Using sheet: `{sheet_to_use}`")
                        insert_sheet_rule(filename, sheet_to_use, DB_PATH)
                    else:
                        st.stop()
            else:
                selected = st.selectbox("📑 Select sheet to upload:", all_sheets)
                confirm_select = st.button("✅ Confirm Sheet Selection")
                if confirm_select:
                    sheet_to_use = selected
                    st.success(f"✅ Using sheet: `{sheet_to_use}`")
                    insert_sheet_rule(filename, sheet_to_use, DB_PATH)
                else:
                    st.stop()

            preview_df = xls.parse(sheet_to_use)

        elif extension == ".csv":
            sheet_to_use = "CSV_SHEET"
            preview_df = pd.read_csv(uploaded_file)
        else:
            st.error("❌ Unsupported file format.")
            st.stop()

        # 🔁 Apply transform rules
        rules = get_transform_rules(filename, sheet_to_use, DB_PATH)
        if rules:
            included_cols = [r['original_column'] for r in rules if r['included']]
            rename_map = {r['original_column']: r['renamed_column'] for r in rules if r['included']}
            
            # ✅ Extra check to avoid empty DataFrame bug
            existing = [c for c in included_cols if c in preview_df.columns]
            if existing:
                preview_df = preview_df[existing]
                preview_df.rename(columns=rename_map, inplace=True)
            else:
                st.warning("⚠️ No matching columns found to apply transform rules. Showing full dataset.")
        else:
            st.info("ℹ️ No transform rules applied.")

        # ➕ Add New Column (Optional)
        st.markdown("### ➕ Add a New Column (Optional)")
        st.markdown("Use a Python expression referencing `preview_df`. For example: `Call.str[-3:]`")

        new_col = st.text_input("New column name", value="NewColumn")
        expression = st.text_input("Python expression (optional)")

        if st.button("➕ Insert Column"):
            try:
                if expression.strip():
                    preview_df[new_col] = eval(f"preview_df.{expression}")
                    st.success(f"✅ New column `{new_col}` added.")
                else:
                    preview_df[new_col] = ""
                    st.success(f"✅ Blank column `{new_col}` created.")
            except Exception as err:
                st.error(f"❌ Error: {err}")

        # 🧠 AgGrid for editing
        st.markdown("### Edit Data in Grid (Rename / Remove Columns, Adjust Values)")
        max_rows = st.slider("🔢 Rows to preview:", 5, 1000, 10)

        # Step 1: GridOptionsBuilder
        gb = GridOptionsBuilder.from_dataframe(preview_df.head(max_rows))

        gb.configure_default_column(
            editable=True,
            resizable=True,
            filter=True,
            sortable=True
        )

        # Step 2: Explicitly configure each column with hide toggle
        for col in preview_df.columns:
            gb.configure_column(field=col, header_name=col, hide=False)

        # Step 3: Build the grid options
        gb.configure_grid_options(domLayout="autoHeight")

        # Step 4: Render AgGrid with correct mode for column state tracking
        grid_response = AgGrid(
            preview_df.head(max_rows),
            gridOptions=gb.build(),
            update_mode=GridUpdateMode.MODEL_CHANGED,
            data_return_mode=DataReturnMode.FILTERED_AND_SORTED,
            allow_unsafe_jscode=True,
            fit_columns_on_grid_load=False,
            enable_enterprise_modules=True,
            theme="material")
        
        edited_df = grid_response["data"]
        # ✅ Extract column visibility state safely
        column_state = grid_response.get("column_state", [])
        hidden_columns = [col.get("colId") for col in column_state if col.get("hide", False)]
        # ✅ Still use `columns` for ordering/renaming
        updated_columns = grid_response.get("columns", [])
        # hidden_columns = [col["field"] for col in updated_columns if col.get("hide", False)]
        # Step 2: Show for debugging (optional)
        st.markdown("### 🕵️ Hidden columns (will be excluded):")
        st.write(hidden_columns)

        # Optional: Prompt the user to apply visibility/rename changes
        st.markdown("📝 After hiding or renaming columns, click **🔄 Update** to apply your changes.")
        
        if st.button("🔄 Update"):
            # Extract edited data
            edited_df = grid_response["data"]

            # Extract column metadata
            updated_columns = grid_response.get("columns", [])
            hidden_columns = [col["colId"] for col in column_state if col.get("hide", False)]

            # Show which columns are hidden
            st.markdown("### 🕵️ Hidden columns (will be excluded):")
            st.write(hidden_columns)

            # Build rename map from AgGrid header renames
            rename_map = {
                col['field']: col.get('headerName', col['field']) 
                for col in updated_columns 
                if col['field'] != col.get('headerName', col['field'])
            }

            # Apply renames and column order to full dataset
            final_df = preview_df.rename(columns=rename_map).copy()
            current_fields = [col["field"] for col in updated_columns]

            if current_fields:
                final_df = final_df[current_fields]
            else:
                st.warning("⚠️ Could not determine column order from AgGrid. Keeping original order.")

            # Sanitize column names to avoid SQLite syntax issues
            def sanitize(col):
                if not col or not str(col).strip():
                    return "col_unnamed"
                col = re.sub(r"[^a-zA-Z0-9_]+", "_", str(col))
                if col[0].isdigit():
                    col = f"col_{col}"
                return col

            final_df.columns = [sanitize(c) for c in final_df.columns]

            # Store result in session state for Save/Upload step
            st.session_state["final_df"] = final_df
            st.session_state["rename_map"] = rename_map
            st.session_state["hidden_columns"] = hidden_columns
            st.success("✅ Changes applied. You can now proceed to Save/Upload.")

        
        # ──────────────────────────────────────────────────────────
        # SAVE/UPLOAD block
        # ──────────────────────────────────────────────────────────
        if st.button("✅ Save/Upload"):
            # STEP 1: Load from session state
            final_df = st.session_state.get("final_df")
            rename_map = st.session_state.get("rename_map", {})
            hidden_columns = st.session_state.get("hidden_columns", [])

              # STEP 2: Safety check
            if final_df is None:
                st.error("❌ No changes found. Please click 🔄 Update before saving.")
                st.stop()

            # Debugging: show final columns
            st.write("**Final columns going into the DB:**", list(final_df.columns))
            st.write(f"**Final shape:** {final_df.shape}")

            # Safety check: ensure at least one column
            if final_df.shape[1] == 0:
                st.error("⚠️ No columns left to upload. Please keep at least one column.")
                st.stop()

            now = datetime.now().isoformat()
            rule_payload = []

            #  STEP 6: Handle original columns
            # (We iterate over the original preview_df columns before rename)
            reverse_rename_map = {v: k for k, v in rename_map.items()}
            original_preview_cols = list(preview_df.rename(columns={v: k for k, v in rename_map.items()}).columns)
            for col in original_preview_cols:
                # included = col in preview_df.columns  # for clarity, but you can adjust logic
                included = col not in hidden_columns
                new_name = rename_map.get(col, col)
                if col:  # guard
                    rule_payload.append({
                        "filename": filename,
                        "sheet": sheet_to_use,
                        "original_column": col,
                        "renamed_column": sanitize(new_name),
                        "included": included,
                        "created_at": now
                    })

            #  STEP 7:  Handle brand-new columns (added with the "Insert Column" logic)
            for new_col_name in final_df.columns:
                # If new_col_name not in original_preview_cols, then it's brand new
                if new_col_name not in original_preview_cols:
                    rule_payload.append({
                        "filename": filename,
                        "sheet": sheet_to_use,
                        "original_column": new_col_name,
                        "renamed_column": new_col_name,
                        "included": True,
                        "created_at": now
                    })
            
          

            #  STEP 8:  ✅ Only save if non-empty
            if rule_payload:
                if not any(rule["included"] for rule in rule_payload):
                    st.error("❌ All columns are excluded. Please include at least one column.")
                    st.stop()
                try:
                    save_transform_rules(rule_payload, DB_PATH)
                    st.toast("✅ Transform rules saved")
                except Exception as err:
                    st.error(f"❌ Failed saving rules: {err}")
            else:
                st.warning("⚠️ No valid transformation rules to save.")

            # STEP 9: Upload to SQLite
            table_name = f"raw_data_{datetime.now().strftime('%Y%m%d_%H%M')}"
            try:
                with sqlite3.connect(DB_PATH) as conn:
                    final_df.to_sql(table_name, conn, index=False, if_exists="replace")
                insert_upload_log(filename, table_name, final_df.shape[0], final_df.shape[1], DB_PATH)
                st.toast(f"📦 Data uploaded to `{table_name}`", icon="✅")
            except Exception as err:
                st.error(f"❌ Failed uploading to DB: {err}")

    except Exception as err:
        st.error(f"❌ Error processing file: {err}")


# ──────────────────────────────────────────────────
# TAB 2: Mass Upload
# ──────────────────────────────────────────────────
with tabs[2]:
    st.subheader("📦 Mass Upload for a Report")

    reports_df = get_all_reports(DB_PATH)
    if reports_df.empty:
        st.warning("No reports found. Please create a report in the Single File tab first.")
        st.stop()

    chosen_report_for_mass = st.selectbox("Select Report", reports_df["report_name"].tolist())
    st.info(f"You are uploading multiple files for: {chosen_report_for_mass}")

    files = st.file_uploader("Drop multiple files here", type=["csv","xlsx","xls"], accept_multiple_files=True)
    if files:
        st.write(f"Number of files selected: {len(files)}")
        for f in files:
            st.write(f"- {f.name}")

        if st.button("🚀 Perform Mass Upload"):
            st.success("Mass upload completed! (Implement your logic...)")

# ──────────────────────────────────────────────────
# TAB 3: View Upload History
# ──────────────────────────────────────────────────
with tabs[3]:
    st.subheader("🔎 Upload History")
    try:
        with sqlite3.connect(DB_PATH) as conn:
            logs = pd.read_sql_query("SELECT * FROM upload_log ORDER BY uploaded_at DESC", conn)
        st.dataframe(logs)
    except Exception as e:
        st.error(f"Could not load upload log: {e}")
