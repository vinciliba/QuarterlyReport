import pandas as pd
from datetime import date, timedelta
from great_tables import GT, md, google_font, style, loc
import sqlite3
from ingestion.db_utils import insert_variable
import logging

# Configure logging
logging.basicConfig(level=logging.DEBUG)

def fetch_latest_table_data(conn: sqlite3.Connection, table_name: str, cutoff: pd.Timestamp) -> pd.DataFrame:
    """
    Fetch data from the upload with uploaded_at closest to cutoff.
    """
    cutoff_str = cutoff.isoformat()

    # Find the closest uploaded_at for the given table_alias
    query = f"""
        SELECT uploaded_at
        FROM upload_log
        WHERE table_alias = ?
        ORDER BY ABS(strftime('%s', uploaded_at) - strftime('%s', ?))
        LIMIT 1
    """
    result = conn.execute(query, (table_name, cutoff_str)).fetchone()
    if not result:
        raise ValueError(f"No uploads found for table '{table_name}' near cutoff {cutoff_str}")

    closest_uploaded_at = result[0]

    # Fetch data from the actual table, filtering by uploaded_at
    df = pd.read_sql_query(
        f"SELECT * FROM {table_name} WHERE uploaded_at = ?",
        conn,
        params=(closest_uploaded_at,)
    )
    logging.debug(f"Fetched {len(df)} rows from {table_name}")
    return df

def build_commitment_summary_table(df: pd.DataFrame, current_year: int, report: str, db_path: str) -> pd.DataFrame:
    df = df[df["Budget Period"] == current_year]
    df = df[df["Fund Source"].isin(["VOBU", "EFTA"])]
    df["Programme"] = df["Functional Area Desc"].replace({
        "HORIZONEU_21_27": "HE",
        "H2020_14_20": "H2020"
    })
    agg = df.groupby("Programme")[
        ["Commitment Appropriation", "Committed Amount", "Commitment Available"]
    ].sum().reset_index()
    agg["%"] = agg["Committed Amount"] / agg["Commitment Appropriation"]
    agg = agg.rename(columns={
        "Commitment Appropriation": "Available Commitment Appropriations (1)",
        "Committed Amount": "L1 Commitment (2)",
        "Commitment Available": "RAL on Appropriation (7)=(1)-(6)",
        "%": "% consumed of L1 and L2 against Commitment Appropriations (8) = (6)/(1)"
    })

    # Create GreatTables object
    table = (
        GT(agg)
        .tab_header(
            title="Commitment Appropriations",
            subtitle="General Overview"
        )
        .tab_style(
            style=style.text(font=google_font(name="IBM Plex Mono")),
            locations=loc.body()
        )
        .tab_stub(rowname_col="Programme")
        .tab_source_note(source_note="Source: Summa DataWharehouse")
        .tab_source_note(source_note=md("Table_1a"))
        .tab_stubhead(label="Programme")
        .fmt_number(columns=[
            "Available Commitment Appropriations (1)",
            "L1 Commitment (2)",
            "RAL on Appropriation (7)=(1)-(6)"
        ], accounting=True)
        .fmt_percent(columns=[
            "% consumed of L1 and L2 against Commitment Appropriations (8) = (6)/(1)"
        ])
    )

    # Insert data and GT table image
    insert_variable(report, "BudgetModule", "table_1a_commitment_summary", agg.to_dict(orient="records"), db_path, anchor="table_1a", gt_table=table)
    insert_variable(report, "BudgetModule", "anchor_table_1a_commitment_summary", table.to_dict(), db_path, anchor="table_1a_view", gt_table=table)
    logging.debug(f"Inserted commitment summary table data and image for {report}")
    return agg

def build_payment_summary_table(df: pd.DataFrame, current_year: int, report: str, db_path: str) -> pd.DataFrame:
    df = df[df["Budget Period"] == current_year]
    df = df[df["Fund Source"].isin(["VOBU", "EFTA"])]
    df["Programme"] = df["Functional Area Desc"].replace({
        "HORIZONEU_21_27": "HE",
        "H2020_14_20": "H2020"
    })
    agg = df.groupby("Programme")[
        ["Payment Appropriation", "Paid Amount", "Payment Available"]
    ].sum().reset_index()
    agg["%"] = agg["Paid Amount"] / agg["Payment Appropriation"]
    agg = agg.rename(columns={
        "Payment Appropriation": "Payment Appropriations (1)",
        "Paid Amount": "Payment Credits consumed (2)",
        "Payment Available": "Remaining Payment Appropriations (3)=(1)-(2)",
        "%": "% Payment Consumed (4)=(2)/(1)"
    })

    # Create GreatTables object
    table = (
        GT(agg)
        .tab_header(
            title="Payment Summary",
            subtitle="General Overview"
        )
        .tab_style(
            style=style.text(font=google_font(name="IBM Plex Mono")),
            locations=loc.body()
        )
        .tab_stub(rowname_col="Programme")
        .tab_source_note(source_note="Source: Summa DataWharehouse")
        .tab_source_note(source_note=md("Table_2a"))
        .tab_stubhead(label="Programme")
        .fmt_number(columns=[
            "Payment Appropriations (1)",
            "Payment Credits consumed (2)",
            "Remaining Payment Appropriations (3)=(1)-(2)"
        ], accounting=True)
        .fmt_percent(columns=[
            "% Payment Consumed (4)=(2)/(1)"
        ])
    )

    # Insert data and GT table image
    insert_variable(report, "BudgetModule", "table_2a_payment_summary", agg.to_dict(orient="records"), db_path, anchor="table_2a", gt_table=table)
    insert_variable(report, "BudgetModule", "anchor_table_2a_payment_summary", table.to_dict(), db_path, anchor="table_2a_view", gt_table=table)
    logging.debug(f"Inserted payment summary table data and image for {report}")
    return agg

def build_commitment_detail_table_1(df: pd.DataFrame, current_year: int, report: str, db_path: str) -> pd.DataFrame:
    df["FR ILC Date (dd/mm/yyyy)"] = pd.to_datetime(df["FR ILC Date (dd/mm/yyyy)"], errors="coerce")
    next_year = current_year + 1
    eoy_next = pd.Timestamp(f"{next_year}-12-31")
    eoy_this = pd.Timestamp(f"{current_year}-12-31")

    global_df = df[(df["FR Earmarked Document Type Desc"] == "Global Commitment") &
                   (df["FR ILC Date (dd/mm/yyyy)"] == eoy_next)].copy()
    global_df = global_df.rename(columns={
        "FR Accepted Amount": "L1 Commitment (1)",
        "FR Consumption by PO Amount": "L2 Commitment (2)",
        "FR Fund Reservation Desc": "Fund Reservation Description"
    })
    global_df["RAL on L1 Commitment (3)=(1)-(2)"] = global_df["L1 Commitment (1)"] - global_df["L2 Commitment (2)"]
    global_df["% L2 on L1 Commitment (4)=(2)/(1)"] = global_df["L2 Commitment (2)"] / global_df["L1 Commitment (1)"]
    global_df["Commitment Type"] = "Global"

    prov_df = df[(df["FR Earmarked Document Type Desc"] == "Provisional Commitment") &
                 (df["FR ILC Date (dd/mm/yyyy)"] == eoy_this) &
                 (df["FR Fund Reservation Desc"] == "Experts")].copy()
    prov_df = prov_df.rename(columns={
        "FR Accepted Amount": "Direct L2 Commitment (5)",
        "FR Consumption by Payment Amount": "Consumed Direct L2 Commitment"
    })
    prov_df["RAL on Direct L2 Commitment (6)=(5)-(Consumed)"] = prov_df["Direct L2 Commitment (5)"] - prov_df["Consumed Direct L2 Commitment"]
    prov_df["% Direct L2 Consumed (7)=(Consumed)/(5)"] = prov_df["Consumed Direct L2 Commitment"] / prov_df["Direct L2 Commitment (5)"]
    prov_df["Fund Reservation Description"] = "Experts"
    prov_df["Commitment Type"] = "Provisional"

    global_cols = [
        "Commitment Type", "Fund Reservation Description", "L1 Commitment (1)", "L2 Commitment (2)",
        "RAL on L1 Commitment (3)=(1)-(2)", "% L2 on L1 Commitment (4)=(2)/(1)"
    ]
    prov_cols = [
        "Commitment Type", "Fund Reservation Description", "Direct L2 Commitment (5)",
        "Consumed Direct L2 Commitment", "RAL on Direct L2 Commitment (6)=(5)-(Consumed)", "% Direct L2 Consumed (7)=(Consumed)/(5)"
    ]

    combined = pd.concat([
        global_df[global_cols],
        prov_df[prov_cols]
    ], axis=0, ignore_index=True)

    # Create GreatTables object
    table = (
        GT(combined)
        .tab_header(
            title="Commitment Detail Table 1",
            subtitle="Global and Provisional Commitments"
        )
        .tab_style(
            style=style.text(font=google_font(name="IBM Plex Mono")),
            locations=loc.body()
        )
        .tab_stub(rowname_col="Commitment Type")
        .tab_source_note(source_note="Source: Summa DataWharehouse")
        .tab_source_note(source_note=md("Table_1a_detail"))
        .tab_stubhead(label="Commitment Type")
        .fmt_number(columns=[
            "L1 Commitment (1)", "L2 Commitment (2)", "RAL on L1 Commitment (3)=(1)-(2)",
            "Direct L2 Commitment (5)", "Consumed Direct L2 Commitment", "RAL on Direct L2 Commitment (6)=(5)-(Consumed)"
        ], accounting=True)
        .fmt_percent(columns=[
            "% L2 on L1 Commitment (4)=(2)/(1)", "% Direct L2 Consumed (7)=(Consumed)/(5)"
        ])
    )

    # Insert data and GT table image
    insert_variable(report, "BudgetModule", "table_1a_commitment_detail", combined.to_dict(orient="records"), db_path, anchor="table_1a_detail", gt_table=table)
    insert_variable(report, "BudgetModule", "anchor_table_1a_commitment_detail", table.to_dict(), db_path, anchor="table_1a_detail_view", gt_table=table)
    logging.debug(f"Inserted commitment detail table 1 data and image for {report}")
    return combined

def build_commitment_detail_table_2(df: pd.DataFrame, current_year: int, report: str, db_path: str) -> pd.DataFrame:
    df["FR ILC Date (dd/mm/yyyy)"] = pd.to_datetime(df["FR ILC Date (dd/mm/yyyy)"], errors="coerce")
    eoy_this = pd.Timestamp(f"{current_year}-12-31")

    filtered = df[(df["FR Earmarked Document Type Desc"] == "Global Commitment") &
                  (df["FR ILC Date (dd/mm/yyyy)"] == eoy_this)].copy()
    filtered = filtered.rename(columns={
        "FR Fund Reservation Desc": "Fund Reservation Description",
        "FR Accepted Amount": "L1 Commitment (1)",
        "FR Consumption by PO Amount": "L2 Commitment (2)"
    })

    filtered["RAL on L1 Commitment (3)=(1)-(2)"] = filtered["L1 Commitment (1)"] - filtered["L2 Commitment (2)"]
    filtered["% L2 on L1 Commitment (4)=(2)/(1)"] = filtered["L2 Commitment (2)"] / filtered["L1 Commitment (1)"]

    result = filtered[[
        "Fund Reservation Description", "L1 Commitment (1)", "L2 Commitment (2)",
        "RAL on L1 Commitment (3)=(1)-(2)", "% L2 on L1 Commitment (4)=(2)/(1)"
    ]]

    # Create GreatTables object
    table = (
        GT(result)
        .tab_header(
            title="Commitment Detail Table 2",
            subtitle="Global Commitments"
        )
        .tab_style(
            style=style.text(font=google_font(name="IBM Plex Mono")),
            locations=loc.body()
        )
        .tab_source_note(source_note="Source: Summa DataWharehouse")
        .tab_source_note(source_note=md("Table_1b"))
        .tab_stubhead(label="Fund Reservation Description")
        .fmt_number(columns=[
            "L1 Commitment (1)", "L2 Commitment (2)", "RAL on L1 Commitment (3)=(1)-(2)"
        ], accounting=True)
        .fmt_percent(columns=[
            "% L2 on L1 Commitment (4)=(2)/(1)"
        ])
    )

    # Insert data and GT table image
    insert_variable(report, "BudgetModule", "table_1b_commitment_detail", result.to_dict(orient="records"), db_path, anchor="table_1b", gt_table=table)
    insert_variable(report, "BudgetModule", "anchor_table_1b_commitment_detail", table.to_dict(), db_path, anchor="table_1b_view", gt_table=table)
    logging.debug(f"Inserted commitment detail table 2 data and image for {report}")
    return result

