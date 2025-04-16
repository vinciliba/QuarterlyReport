# ui/streamlit_app.py

import os
import sys
import sqlite3
import streamlit as st
import pandas as pd
from datetime import datetime
from st_aggrid import AgGrid, GridOptionsBuilder, GridUpdateMode

# 🔁 Local imports
sys.path.append(os.path.abspath(os.path.join(os.path.dirname(__file__), '..')))
from ingestion.db_utils import (
    init_db, insert_sheet_rule, get_existing_rule, insert_upload_log
)

# ───────────────────────────────
# Constants & Init
# ───────────────────────────────
DB_PATH = 'database/reporting.db'
APP_FILES_DIR = "app_files"
os.makedirs(APP_FILES_DIR, exist_ok=True)
init_db()

st.set_page_config(layout='wide')
st.title("📊 Quarterly Report Ingestion Console")

# ───────────────────────────────
# UI Tabs
# ───────────────────────────────
tab_logic, tab_upload, tab_history = st.tabs([
    "🧠 Upload Logic & Rules", 
    "📂 Upload Files & Transform", 
    "🕓 Upload History"
])

# ───────────────────────────────
# TAB 1: Rules Viewer
# ───────────────────────────────
with tab_logic:
    st.subheader("🧠 Upload Rules Overview")
    file_list = os.listdir(APP_FILES_DIR)
    xlsx_files = [f for f in file_list if f.endswith(('.xlsx', '.xls'))]

    if xlsx_files:
        selected_file = st.selectbox("Choose a file to inspect rules:", xlsx_files)
        rule = get_existing_rule(selected_file)

        if rule:
            st.success(f"Rule exists for `{selected_file}` → Sheet: `{rule}`")
        else:
            st.warning(f"No upload rule found for `{selected_file}`.")
    else:
        st.info("📂 No files found in `app_files/` to check rules.")

# ───────────────────────────────
# TAB 2: Upload + Transform
# ───────────────────────────────
with tab_upload:
    uploaded_file = st.file_uploader("📂 Drop a .csv or Excel file", type=['csv', 'xlsx'])

    if uploaded_file:
        st.success(f"File received: `{uploaded_file.name}`")
        extension = os.path.splitext(uploaded_file.name)[1].lower()
        preview_df = None
        sheet_to_use = None
        saved_rule = None

        try:
            if extension in ['.xls', '.xlsx']:
                xl = pd.ExcelFile(uploaded_file)
                all_sheets = xl.sheet_names
                saved_rule = get_existing_rule(uploaded_file.name)

                if saved_rule and saved_rule in all_sheets:
                    st.info(f"🧠 Saved sheet rule found for `{uploaded_file.name}` → Using: `{saved_rule}`")
                    override = st.checkbox("🔁 Override saved sheet rule?", value=False)

                    if override:
                        sheet_to_use = st.selectbox("Select sheet to upload:", all_sheets)
                    else:
                        sheet_to_use = saved_rule
                else:
                    sheet_to_use = st.selectbox("Select sheet to upload:", all_sheets)

                if sheet_to_use:
                    preview_df = xl.parse(sheet_to_use)

            elif extension == '.csv':
                sheet_to_use = None
                preview_df = pd.read_csv(uploaded_file)

            # ───────────────────────────────
            # Transform UI
            # ───────────────────────────────
            if preview_df is not None:
                st.subheader("🔧 Interactive Table (Transform, Rename, Clean)")

                # ➕ New Column UI
                st.markdown("### ➕ Add New Column")
                with st.expander("📐 Define New Column"):
                    new_col_name = st.text_input("New column name", value="NewColumn")
                    formula = st.text_input("Formula (e.g. `Grant Number * 2`, or leave blank for empty column)")

                    if st.button("➕ Insert Column"):
                        try:
                            if formula.strip():
                                try:
                                    # Eval directly on the dataframe (e.g. Call.str[-3:])
                                    preview_df[new_col_name] = eval(f"preview_df.{formula}")
                                    st.success(f"✅ `{new_col_name}` created from formula.")
                                except Exception as e:
                                    st.error(f"❌ Formula Error: {e}")
                            else:
                                preview_df[new_col_name] = ""
                                st.success(f"✅ Blank column `{new_col_name}` added.")
                        except Exception as e:
                            st.error(f"❌ Formula Error: {e}")

                # AGGrid Config
                max_rows = st.slider("🔢 Rows to preview:", 10, 1000, 10, 10)
                gb = GridOptionsBuilder.from_dataframe(preview_df.head(max_rows))
                gb.configure_default_column(editable=True, resizable=True, filter=True, sortable=True)
                gb.configure_grid_options(domLayout='autoHeight')
                grid_options = gb.build()

                # AGGrid Display
                grid_response = AgGrid(
                    preview_df.head(max_rows),
                    gridOptions=grid_options,
                    update_mode=GridUpdateMode.MANUAL,
                    allow_unsafe_jscode=True,
                    theme="material",
                    fit_columns_on_grid_load=False,
                    height=None
                )

                edited_df = grid_response['data']
                updated_columns = grid_response.get("columns", [])
                rename_map = {
                    col['field']: col.get('headerName', col['field'])
                    for col in updated_columns
                    if col['field'] != col.get('headerName', col['field'])
                }

                if rename_map:
                    edited_df.rename(columns=rename_map, inplace=True)

                if st.button("🚀 Upload Data to SQLite"):
                    table_name = f"raw_data_{datetime.now().strftime('%Y%m%d_%H%M')}"
                    with sqlite3.connect(DB_PATH) as conn:
                        edited_df.to_sql(table_name, conn, index=False, if_exists='replace')
                    insert_upload_log(uploaded_file.name, table_name, edited_df.shape[0], edited_df.shape[1])
                    if extension in ['.xls', '.xlsx'] and not saved_rule:
                        insert_sheet_rule(uploaded_file.name, sheet_to_use)
                    st.success(f"✅ Uploaded to table `{table_name}`")

        except Exception as e:
            st.error(f"❌ Error processing file: {e}")

# ───────────────────────────────
# TAB 3: Upload History
# ───────────────────────────────
with tab_history:
    st.subheader("🕓 Previous Uploads")
    try:
        with sqlite3.connect(DB_PATH) as conn:
            logs = pd.read_sql_query("SELECT * FROM upload_log ORDER BY uploaded_at DESC", conn)
        st.dataframe(logs)
    except Exception as e:
        st.error(f"Could not load upload log: {e}")
