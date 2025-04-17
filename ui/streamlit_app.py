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
    get_existing_rule,
    insert_sheet_rule,
    get_transform_rules,
    save_transform_rules,
    insert_upload_log, 
    create_new_report, 
    get_all_reports,
    is_report_complete, 
    get_expected_tables,
    get_alias_for_file,  
    update_alias_status
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
    st.title("📊 Report Launch & Validation")
    st.markdown("Use this section to validate if all required data is present before running a report.")

    reports_df = get_all_reports(DB_PATH)
    report_names = reports_df["report_name"].tolist()

    chosen_report = st.selectbox("Choose report to validate", report_names)

    # Step 1: Cutoff input
    cutoff_date = st.date_input("📅 Enter reporting cutoff date")
    tolerance_days = st.slider("⏱️ Allow uploads within how many days before cutoff?", 0, 15, 3)

    # Step 2: Validate presence of all required tables
    complete, missing = is_report_complete(chosen_report, DB_PATH)
    st.markdown("### ✅ Required Tables")
    st.write(get_expected_tables(chosen_report, DB_PATH))

    if not complete:
        st.error(f"⛔ Missing required uploads: {', '.join(missing)}")
        st.stop()

    # Step 3: Validate upload timestamps against cutoff
    import pandas as pd
    with sqlite3.connect(DB_PATH) as conn:
        df_uploads = pd.read_sql_query("""
            SELECT table_alias, uploaded_at
            FROM upload_log
            WHERE report_name = ?
        """, conn, params=(chosen_report,))

    df_uploads["uploaded_at"] = pd.to_datetime(df_uploads["uploaded_at"])
    too_old = df_uploads[df_uploads["uploaded_at"] < pd.to_datetime(cutoff_date) - pd.Timedelta(days=tolerance_days)]

    if not too_old.empty:
        st.warning("⚠️ Some tables were uploaded too early:")
        st.dataframe(too_old)
        st.stop()

    st.success("🎉 All required tables uploaded and within valid cutoff window!")

    if st.button("🚀 Run Report"):
        st.info("✨ Your report logic goes here!")


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
                try:
                    create_new_report(new_report_name.strip(), DB_PATH)
                    st.success(f"Report '{new_report_name}' created!")
                    st.rerun()
                except ValueError as e:
                    st.error(str(e))
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
    filename_wo_ext = os.path.splitext(filename)[0]
    table_name = f"raw_{filename_wo_ext.lower()}"
    st.success(f"📥 File received: `{filename}`")

    existing_sheet, saved_start_row = get_existing_rule(filename, DB_PATH)
    sheet_to_use = None

    if extension in [".xlsx", ".xls"]:
        xls = pd.ExcelFile(uploaded_file)
        sheet_names = xls.sheet_names

        if existing_sheet and existing_sheet in sheet_names:
            st.success(f"✅ Found saved sheet: `{existing_sheet}`")
            use_existing = st.checkbox("Use saved sheet rule?", value=True)
            sheet_to_use = existing_sheet if use_existing else None

        if sheet_to_use is None:
            sheet_to_use = st.selectbox("📑 Select sheet to upload:", sheet_names)

        start_row = st.number_input("Start row:", min_value=0, value=saved_start_row or 0)

        if st.button("💾 Save sheet rule"):
            insert_sheet_rule(filename, sheet_to_use, start_row, DB_PATH)
            st.success(f"Rule saved for `{filename}`: `{sheet_to_use}`, row {start_row}")

        preview_df = xls.parse(sheet_to_use, skiprows=start_row)

    elif extension == ".csv":
        sheet_to_use = "CSV_SHEET"
        start_row = st.number_input("Start row:", min_value=0, value=saved_start_row or 0)
        preview_df = pd.read_csv(uploaded_file, skiprows=start_row)

    else:
        st.error("❌ Unsupported file format.")
        st.stop()

    rules = get_transform_rules(filename, sheet_to_use, DB_PATH)
    if rules:
        included = [r["original_column"] for r in rules if r["included"]]
        rename_map = {r["original_column"]: r["renamed_column"] for r in rules if r["included"]}
        preview_df = preview_df[[c for c in included if c in preview_df.columns]]
        preview_df.rename(columns=rename_map, inplace=True)
        st.info("✅ Applied saved transformation rules.")

    st.markdown("### 👀 Preview")
    st.dataframe(preview_df.head(10))

    excluded_cols = st.multiselect("Exclude columns:", preview_df.columns)

    if st.button("✅ Save/Upload"):
        final_df = preview_df.drop(columns=excluded_cols, errors="ignore")
        now = datetime.now().isoformat()

        rule_payload = []
        for col in preview_df.columns:
            rule_payload.append({
                "filename": filename,
                "sheet": sheet_to_use,
                "original_column": col,
                "renamed_column": col,
                "included": col not in excluded_cols,
                "created_at": now
            })

        save_transform_rules(rule_payload, DB_PATH)

        try:
            with sqlite3.connect(DB_PATH) as conn:
                upload_id = insert_upload_log(
                    filename, table_name, final_df.shape[0], final_df.shape[1],
                    chosen_report, db_path=DB_PATH
                )
                final_df["upload_id"] = upload_id
                final_df["uploaded_at"] = now

                existing = pd.read_sql_query("SELECT name FROM sqlite_master WHERE type='table'", conn)
                if table_name not in existing["name"].tolist():
                    final_df.to_sql(table_name, conn, index=False, if_exists="replace")
                else:
                    final_df.to_sql(table_name, conn, index=False, if_exists="append")

            st.success(f"📦 Uploaded to `{table_name}` (Upload ID: {upload_id})")

        except Exception as e:
            st.error(f"❌ Upload failed: {e}")


# ──────────────────────────────────────────────────
# TAB 2: Mass Upload from `app_files`
# ──────────────────────────────────────────────────

with tabs[2]:
    st.subheader("📦 Mass Upload from `app_files/`")

    reports_df = get_all_reports(DB_PATH)
    if reports_df.empty:
        st.warning("Please create a report first.")
        st.stop()

    chosen_report = st.selectbox("Select report to upload files for:", reports_df["report_name"].tolist())

    all_files = [f for f in os.listdir("app_files") if f.endswith((".csv", ".xlsx", ".xls"))]
    if not all_files:
        st.info("Place files in the `app_files/` folder.")
        st.stop()

    if st.button("🚀 Upload All"):
        for file in all_files:
            file_path = os.path.join("app_files", file)
            filename_wo_ext = os.path.splitext(file)[0]
            table_name = f"raw_{filename_wo_ext.lower()}"

            ext = os.path.splitext(file)[1].lower()
            sheet, start_row = get_existing_rule(file, DB_PATH)

            try:
                if ext in [".xlsx", ".xls"]:
                    xls = pd.ExcelFile(file_path)
                    if not sheet or sheet not in xls.sheet_names:
                        st.warning(f"⚠️ No sheet rule for `{file}`")
                        continue
                    df = xls.parse(sheet, skiprows=start_row)
                elif ext == ".csv":
                    sheet = "CSV_SHEET"
                    df = pd.read_csv(file_path, skiprows=start_row or 0)
                else:
                    st.warning(f"⚠️ Unsupported file type `{file}`")
                    continue

                rules = get_transform_rules(file, sheet, DB_PATH)
                if rules:
                    included = [r["original_column"] for r in rules if r["included"]]
                    rename_map = {r["original_column"]: r["renamed_column"] for r in rules if r["included"]}
                    df = df[[c for c in included if c in df.columns]]
                    df.rename(columns=rename_map, inplace=True)

                with sqlite3.connect(DB_PATH) as conn:
                    upload_id = insert_upload_log(
                        file, table_name, df.shape[0], df.shape[1], chosen_report, db_path=DB_PATH
                    )
                    df["upload_id"] = upload_id
                    df["uploaded_at"] = datetime.now().isoformat()

                    existing = pd.read_sql_query("SELECT name FROM sqlite_master WHERE type='table'", conn)
                    if table_name not in existing["name"].tolist():
                        df.to_sql(table_name, conn, index=False, if_exists="replace")
                    else:
                        df.to_sql(table_name, conn, index=False, if_exists="append")

                st.success(f"✅ Uploaded `{file}` to `{table_name}`")

            except Exception as e:
                st.error(f"❌ Failed to upload `{file}`: {e}")

# ──────────────────────────────────────────────────
# TAB 3: Upload History & Report Management
# ──────────────────────────────────────────────────

with tabs[3]:
    st.subheader("🔎 Upload History")

    try:
        with sqlite3.connect(DB_PATH) as conn:
            logs = pd.read_sql_query("""
                SELECT report_name, MAX(uploaded_at) AS last_refresh
                FROM upload_log
                GROUP BY report_name
                ORDER BY last_refresh DESC
            """, conn)
        if logs.empty:
            st.info("No uploads found.")
        else:
            selected_report = st.selectbox("Select report to inspect", logs["report_name"])
            st.dataframe(logs)

            with sqlite3.connect(DB_PATH) as conn:
                report_logs = pd.read_sql_query("""
                    SELECT filename, table_alias, uploaded_at, rows, cols
                    FROM upload_log
                    WHERE report_name = ?
                    ORDER BY uploaded_at DESC
                """, conn, params=(selected_report,))
            st.markdown(f"### 📁 Files uploaded for `{selected_report}`")
            st.dataframe(report_logs)

            col1, col2 = st.columns(2)
            if col1.button("❌ Delete selected report"):
                with sqlite3.connect(DB_PATH) as conn:
                    conn.execute("DELETE FROM reports WHERE report_name = ?", (selected_report,))
                    conn.execute("DELETE FROM upload_log WHERE report_name = ?", (selected_report,))
                    conn.execute("DELETE FROM report_structure WHERE report_name = ?", (selected_report,))
                    conn.execute("DELETE FROM report_cutoff_log WHERE report_name = ?", (selected_report,))
                st.success(f"✅ Report `{selected_report}` and its logs were deleted.")
                st.rerun()

            if col2.button("🔥 Delete ALL reports"):
                with sqlite3.connect(DB_PATH) as conn:
                    conn.execute("DELETE FROM reports")
                    conn.execute("DELETE FROM upload_log")
                    conn.execute("DELETE FROM report_structure")
                    conn.execute("DELETE FROM report_cutoff_log")
                    conn.execute("DELETE FROM alias_upload_status")
                st.success("🧨 All reports and their logs were deleted.")
                st.rerun()

        # Show alias freshness
        st.markdown("### ⏱️ Alias Freshness")
        with sqlite3.connect(DB_PATH) as conn:
            freshness_df = pd.read_sql_query("""
                SELECT a.alias, a.last_loaded_at, f.filename
                FROM alias_upload_status a
                LEFT JOIN file_alias_map f ON a.file_id = f.id
                ORDER BY a.last_loaded_at DESC
            """, conn)
        if freshness_df.empty:
            st.info("No alias freshness records yet.")
        else:
            st.dataframe(freshness_df)

    except Exception as e:
        st.error(f"Could not load history: {e}")

