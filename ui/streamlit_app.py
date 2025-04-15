# ui/streamlit_app.py
import streamlit as st
import os
import sys
# Add parent dir to sys.path so we can import ingestion and others
sys.path.append(os.path.abspath(os.path.join(os.path.dirname(__file__), '..')))
import pandas as pd
from ingestion.data_ingestion import ingest_data
from ingestion.db_utils import (
    init_db, insert_sheet_rule, get_existing_rule
)

st.set_page_config(layout='wide')
init_db()

st.title("📥 Data Ingestion - Quarterly Report System")

### DRAG AND DROP AREA
uploaded_file = st.file_uploader("Upload a file (.csv or Excel)", type=['csv', 'xlsx'])

if uploaded_file:
    st.success(f"File received: `{uploaded_file.name}`")
    extension = os.path.splitext(uploaded_file.name)[1].lower()

    sheet_to_use = None
    if extension in ['.xls', '.xlsx']:
        xl = pd.ExcelFile(uploaded_file)
        all_sheets = xl.sheet_names

        saved_rule = get_existing_rule(uploaded_file.name)
        if saved_rule:
            st.info(f"⚙️ Existing rule found for `{uploaded_file.name}`: Using sheet: `{saved_rule}`")
            sheet_to_use = saved_rule
        else:
            sheet_to_use = st.selectbox("Select a sheet to upload:", all_sheets)

    if st.button("✅ Ingest Data"):
        try:
            df, table_name = ingest_data(uploaded_file, sheet_to_use)
            if extension in ['.xls', '.xlsx'] and not saved_rule:
                insert_sheet_rule(uploaded_file.name, sheet_to_use)

            st.success(f"Data uploaded to SQLite as `{table_name}`")
            st.dataframe(df.head(10))
        except Exception as e:
            st.error(f"Error: {e}")

### BULK LOAD FROM FOLDER
st.markdown("---")
st.header("📁 Load Files from `/app_files/` Directory")

folder_files = [f for f in os.listdir("app_files") if f.endswith(('.csv', '.xlsx'))]
for fname in folder_files:
    st.write(f"📄 {fname}")
