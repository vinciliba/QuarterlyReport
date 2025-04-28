import os
import sys
import sqlite3
import streamlit as st
import pandas as pd
from datetime import datetime
from ingestion.db_utils import define_expected_table


# Add project root to sys.path to allow absolute imports
sys.path.append(os.path.abspath(os.path.join(os.path.dirname(__file__), '..')))

from ingestion.db_utils import (
    init_db,
    get_expected_tables,
    get_all_reports,
)

# Initialize DB
DB_PATH = 'database/reporting.db'
os.makedirs("app_files", exist_ok=True)
init_db(db_path=DB_PATH)


st.title("📊 Quarterly Report Launch & Validation")
st.markdown("Use this section to validate that all required uploads are present and fresh before running the report.")

# --- Report Selection ---
reports_df = get_all_reports(DB_PATH)
report_names = reports_df["report_name"].tolist()

if not report_names:
    st.warning("No reports defined. Please create a report first.")
    st.stop()

chosen_report = st.selectbox("Select report to validate", report_names)

# --- Cutoff Date and Tolerance ---
cutoff_date = st.date_input("📅 Reporting Cutoff Date", value=datetime.today())
tolerance_days = st.slider("⏱️ Tolerance (days before cutoff allowed)", 0, 15, 3)
cutoff_datetime = pd.to_datetime(cutoff_date) - pd.Timedelta(days=tolerance_days)

# --- Fetch Expected Tables ---
expected_aliases = get_expected_tables(chosen_report, DB_PATH)

if not expected_aliases:
    st.warning(f"No required tables defined for report `{chosen_report}`.")
    st.info("Assuming all uploads OK since no strict requirements.")
    st.stop()

# --- Fetch Uploaded Tables ---
with sqlite3.connect(DB_PATH) as conn:
    uploaded_df = pd.read_sql_query("""
        SELECT table_alias, MAX(uploaded_at) as last_uploaded
        FROM upload_log
        WHERE report_name = ?
        GROUP BY table_alias
    """, conn, params=(chosen_report,))

uploaded_aliases = dict(zip(uploaded_df['table_alias'], uploaded_df['last_uploaded']))

# --- Validation ---
validation_result = []
report_is_complete = True

for alias in expected_aliases:
    if alias in uploaded_aliases:
        upload_time = pd.to_datetime(uploaded_aliases[alias])
        if upload_time >= cutoff_datetime:
            validation_result.append({
                "Required Table Alias": alias,
                "Status": "✅ Fresh Upload",
                "Last Upload": upload_time.strftime("%Y-%m-%d %H:%M")
            })
        else:
            validation_result.append({
                "Required Table Alias": alias,
                "Status": "⚠️ Too Old",
                "Last Upload": upload_time.strftime("%Y-%m-%d %H:%M")
            })
            report_is_complete = False
    else:
        validation_result.append({
            "Required Table Alias": alias,
            "Status": "❌ Missing",
            "Last Upload": "-"
        })
        report_is_complete = False

validation_df = pd.DataFrame(validation_result)

st.markdown("### Validation Results")
st.dataframe(validation_df, hide_index=True, use_container_width=True)

# --- Final Decision ---
if report_is_complete:
    st.success("🎉 All required tables are uploaded and fresh!")

    if st.button("🚀 Run Report"):
        st.info(f"Launching report generation for `{chosen_report}`...")
        # TODO: CALL your real report logic here, e.g., run_report(chosen_report)
        st.success("✅ Report successfully launched!")
else:
    st.error("⛔ Some required tables are missing or outdated.")
    st.stop()