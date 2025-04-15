# ui/streamlit_app.py

import streamlit as st
import os
import sys
import pandas as pd

# 🔁 Add parent dir to sys.path so we can import ingestion modules
sys.path.append(os.path.abspath(os.path.join(os.path.dirname(__file__), '..')))

from ingestion.db_utils import (
    init_db, insert_sheet_rule, get_existing_rule, insert_upload_log
)

from st_aggrid import AgGrid, GridOptionsBuilder, GridUpdateMode

import sqlite3
from datetime import datetime

DB_PATH = 'database/reporting.db'
APP_FILES_DIR = "app_files"

# Make sure folders exist
os.makedirs(APP_FILES_DIR, exist_ok=True)
init_db()


def ingest_dataframe(df: pd.DataFrame, original_filename: str, db_path=DB_PATH):
    """Save the edited dataframe into SQLite with a timestamped name."""
    now = datetime.now().strftime('%Y%m%d_%H%M')
    table_name = f"raw_data_{now}"

    with sqlite3.connect(db_path) as conn:
        df.to_sql(table_name, conn, index=False, if_exists='replace')

    insert_upload_log(original_filename, table_name, df.shape[0], df.shape[1], db_path)
    return df, table_name


# 🔥 UI starts here
st.set_page_config(layout='wide')
st.title("📥 Data Ingestion - Quarterly Report System")

uploaded_file = st.file_uploader("Upload a file (.csv or Excel)", type=['csv', 'xlsx'])

if uploaded_file:
    st.success(f"File received: `{uploaded_file.name}`")
    extension = os.path.splitext(uploaded_file.name)[1].lower()

    sheet_to_use = None
    preview_df = None

    if extension in ['.xls', '.xlsx']:
        xl = pd.ExcelFile(uploaded_file)
        all_sheets = xl.sheet_names

        saved_rule = get_existing_rule(uploaded_file.name)
        if saved_rule:
            st.info(f"⚙️ Using saved rule for `{uploaded_file.name}` → Sheet: `{saved_rule}`")
            override = st.checkbox("⬆️ Override saved sheet rule?", value=False)
            if override:
                sheet_to_use = st.selectbox("Select a sheet to upload:", all_sheets)
            else:
                sheet_to_use = saved_rule
        else:
            sheet_to_use = st.selectbox("Select a sheet to upload:", all_sheets)

        if sheet_to_use:
            st.subheader("🔍 Sheet Preview")
            try:
                preview_df = xl.parse(sheet_to_use, nrows=100)
                st.dataframe(preview_df.head())
            except Exception as e:
                st.error(f"❌ Could not preview sheet: {e}")

    elif extension == '.csv':
        try:
            preview_df = pd.read_csv(uploaded_file, nrows=100)
            st.subheader("🔍 CSV Preview")
            st.dataframe(preview_df.head())
        except Exception as e:
            st.error(f"❌ Could not preview CSV: {e}")

    # --- Interactive AG Grid ---
    if preview_df is not None:
        st.subheader("🧠 Interactive Table (Edit + Rename + Reorder Columns)")

        gb = GridOptionsBuilder.from_dataframe(preview_df)
        gb.configure_default_column(editable=True, resizable=True, sortable=True, filter=True)
        gb.configure_grid_options(domLayout='normal')
        grid_options = gb.build()

        grid_response = AgGrid(
            preview_df,
            gridOptions=grid_options,
            update_mode=GridUpdateMode.MANUAL,
            allow_unsafe_jscode=True,
            fit_columns_on_grid_load=True,
            height=500,
            theme="material"
        )

        edited_df = grid_response['data']
        updated_columns = grid_response.get("columns", [])

        rename_map = {}
        for col_def in updated_columns:
            orig = col_def['field']
            new = col_def.get('headerName', orig)
            if orig != new:
                rename_map[orig] = new

        if rename_map:
            edited_df.rename(columns=rename_map, inplace=True)

        st.success("✅ Live preview after inline edits")
        st.dataframe(edited_df.head())

        # --- Ingest button ---
        if st.button("✅ Ingest Edited Data"):
            try:
                df, table_name = ingest_dataframe(edited_df, uploaded_file.name)
                if extension in ['.xls', '.xlsx'] and not saved_rule:
                    insert_sheet_rule(uploaded_file.name, sheet_to_use)
                st.success(f"✅ Data uploaded to SQLite as `{table_name}`")
                st.dataframe(df.head(10))
            except Exception as e:
                st.error(f"❌ Ingestion failed: {e}")
    else:
        st.warning("⚠️ No valid data loaded yet.")

# --- Folder file list ---
st.markdown("---")
st.header("📁 Load Files from `/app_files/` Directory")
folder_files = [f for f in os.listdir(APP_FILES_DIR) if f.endswith(('.csv', '.xlsx'))]
if folder_files:
    for fname in folder_files:
        st.write(f"📄 {fname}")
else:
    st.info("📂 No files found in `/app_files`.")
