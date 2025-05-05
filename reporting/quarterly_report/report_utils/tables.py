
import pandas as pd
from datetime import date,timedelta
from ingestion.db_utils import insert_variable
from plottable import Table
import sqlite3

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

    # Now fetch data from the actual table, filtering by uploaded_at
    df = pd.read_sql_query(
        f"SELECT * FROM {table_name} WHERE uploaded_at = ?",
        conn,
        params=(closest_uploaded_at,)
    )
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
    insert_variable(report, "BudgetModule", "table_1a_commitment_summary", agg.to_dict(orient="records"), db_path, anchor="table_1a")
    table = Table(agg)
    insert_variable(report, "BudgetModule", "anchor_table_1a_commitment_summary", table.to_dict(), db_path, anchor="table_1a_view")
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
    insert_variable(report, "BudgetModule", "table_2a_payment_summary", agg.to_dict(orient="records"), db_path, anchor="table_2a")
    table = Table(agg)
    insert_variable(report, "BudgetModule", "anchor_table_2a_payment_summary", table.to_dict(), db_path, anchor="table_2a_view")
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

    insert_variable(report, "BudgetModule", "table_1a_commitment_detail", combined.to_dict(orient="records"), db_path, anchor="table_1a_detail")
    table = Table(combined)
    insert_variable(report, "BudgetModule", "anchor_table_1a_commitment_detail", table.to_dict(), db_path, anchor="table_1a_detail_view")
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

    insert_variable(report, "BudgetModule", "table_1b_commitment_detail", result.to_dict(orient="records"), db_path, anchor="table_1b")
    table = Table(result)
    insert_variable(report, "BudgetModule", "anchor_table_1b_commitment_detail", table.to_dict(), db_path, anchor="table_1b_view")
    return result 
