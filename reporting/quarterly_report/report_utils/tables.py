import pandas as pd
from datetime import datetime, date
from great_tables import GT, md, google_font, style, loc, html
import sqlite3
from ingestion.db_utils import insert_variable, upsert_report_param 
import logging
from typing import Optional, Dict, Union

# Configure logging
logging.basicConfig(level=logging.DEBUG)
    
# BLUE        = "#004A99"
# LIGHT_BLUE = "#d6e6f4"
# GRID_CLR    = "#004A99"
# DARK_BLUE   = "#01244B"
# DARK_GREY =   '#242425'

# ------------------------------------------------------------------
# 1. BUDGET MODULE
# ------------------------------------------------------------------

def build_commitment_summary_table(df: pd.DataFrame, current_year: int, report: str, db_path: str, table_colors: dict = None) -> pd.DataFrame:
    """
    Builds a commitment summary table for the HE programme, grouped by Fund Source and Budget Address Type.
    
    Args:
        df (pd.DataFrame): Input DataFrame with commitment data.
        current_year (int): The year to filter the Budget Period.
        report (str): Name of the report for storing the table.
        db_path (str): Path to the SQLite database for storing the table.
    
    Returns:
        pd.DataFrame: Aggregated table with commitment summary.
    """
    # Define expected columns for validation
    expected_columns = [
        "Budget Period", "Fund Source", "Functional Area Desc", 
        "Budget Address", "Commitment Appropriation", 
        "Committed Amount", "Commitment Available "
    ]
    
    # Validate input DataFrame
    if not isinstance(df, pd.DataFrame):
        logging.error("Input 'df' is not a pandas DataFrame.")
        return pd.DataFrame(columns=[
            "Fund Source", "Budget Address Type", "Available_Commitment_Appropriations",
            "L1_Commitment", "RAC_on_Appropriation", 
            "ratio_consumed_of_L1_and_L2_against_Commitment_Appropriations"
        ])
    
    if df.empty:
        logging.warning("Input DataFrame is empty. Returning empty DataFrame with expected columns.")
        return pd.DataFrame(columns=[
            "Fund Source", "Budget Address Type", "Available_Commitment_Appropriations",
            "L1_Commitment", "RAC_on_Appropriation", 
            "ratio_consumed_of_L1_and_L2_against_Commitment_Appropriations"
        ])
    
    # Check for missing columns
    missing_columns = [col for col in expected_columns if col not in df.columns]
    if missing_columns:
        logging.error(f"Missing required columns: {missing_columns}")
        return pd.DataFrame(columns=[
            "Fund Source", "Budget Address Type", "Available_Commitment_Appropriations",
            "L1_Commitment", "RAC_on_Appropriation", 
            "ratio_consumed_of_L1_and_L2_against_Commitment_Appropriations"
        ])
    
    # Log input DataFrame info
    logging.debug(f"Input DataFrame shape: {df.shape}")
    logging.debug(f"Input DataFrame columns: {df.columns.tolist()}")
    
    try:
        # Filter for current year and specific Fund Sources
        df = df[df["Budget Period"] == current_year].copy()
        logging.debug(f"After filtering Budget Period == {current_year}: {df.shape[0]} rows")
        
        df = df[df["Fund Source"].isin(["VOBU", "EFTA"])].copy()
        logging.debug(f"After filtering Fund Source in ['VOBU', 'EFTA']: {df.shape[0]} rows")
        
        # Map Functional Area to Programme
        df.loc[:, "Programme"] = df["Functional Area Desc"].replace({
            "HORIZONEU_21_27": "HE",
            "H2020_14_20": "H2020"
        })
        
        # Filter for HE programme
        df = df.loc[df["Programme"] == "HE"].copy()
        logging.debug(f"After filtering Programme == 'HE': {df.shape[0]} rows")
        
        if df.empty:
            logging.warning("DataFrame is empty after filtering. Returning empty DataFrame.")
            return pd.DataFrame(columns=[
                "Fund Source", "Budget Address Type", "Available_Commitment_Appropriations",
                "L1_Commitment", "RAC_on_Appropriation", 
                "ratio_consumed_of_L1_and_L2_against_Commitment_Appropriations"
            ])
        
        # Budget-type mapping helper
        def map_budget_type(val):
            if pd.isna(val):
                return None
            v = str(val).upper()
            if "EMPTY" in v:
                return "Main Calls"
            if "EXPERTS" in v:
                return "Experts"
            return val
        
        df.loc[:, "Budget_Address_Type"] = df["Budget Address"].apply(map_budget_type)
        logging.debug(f"Unique Budget_Address_Type values: {df['Budget_Address_Type'].unique()}")
        
        # Fund-type mapping helper
        def map_fund_type(val):
            if pd.isna(val):
                return None
            v = str(val).upper()
            if "VOBU" in v:
                return "VOBU/EFTA"
            if "EFTA" in v:
                return "VOBU/EFTA"
            if "IAR2/2" in v:
                return "VOBU/EFTA"
            return val
        
        df['Fund Source'] = df['Fund Source'].apply(map_fund_type)
        logging.debug(f"Unique Fund Source values after mapping: {df['Fund Source'].unique()}")
        
        # Group by Fund Source and Budget_Address_Type
        agg = df.groupby(["Fund Source", "Budget_Address_Type"])[
            ["Commitment Appropriation", "Committed Amount", "Commitment Available "]
        ].sum().reset_index()
        logging.debug(f"Aggregated DataFrame shape: {agg.shape}")
        
        # Compute percentage
        agg["%"] = agg["Committed Amount"] / agg["Commitment Appropriation"].replace(0, pd.NA)
        agg["%"] = agg["%"].fillna(0)  # Handle division by zero
        
        # Rename columns
        agg = agg.rename(columns={
            "Commitment Appropriation": "Available_Commitment_Appropriations",
            "Committed Amount": "L1_Commitment",
            "Commitment Available ": "RAC_on_Appropriation",
            "%": "ratio_consumed_of_L1_and_L2_against_Commitment_Appropriations",
            "Budget_Address_Type": "Budget Address Type"
        })
        
        # Add total row
        try:
            total_row = pd.DataFrame({
                "Fund Source": ["VOBU/EFTA"],
                "Budget Address Type": ["Total"],
                "Available_Commitment_Appropriations": [agg["Available_Commitment_Appropriations"].sum()],
                "L1_Commitment": [agg["L1_Commitment"].sum()],
                "RAC_on_Appropriation": [agg["RAC_on_Appropriation"].sum()],
                "ratio_consumed_of_L1_and_L2_against_Commitment_Appropriations": [
                    agg["L1_Commitment"].sum() / agg["Available_Commitment_Appropriations"].sum()
                    if agg["Available_Commitment_Appropriations"].sum() != 0 else 0
                ]
            })
            agg = pd.concat([agg, total_row], ignore_index=True)
            logging.debug(f"Final aggregated DataFrame with total row: {agg.shape}")
        except Exception as e:
            logging.error(f"Error adding total row: {str(e)}")
            return agg  # Proceed without total row if it fails
        
        # Build GreatTables object
        try:
            tbl = (
                GT(
                    agg,
                    rowname_col="Budget Address Type",
                    groupname_col="Fund Source"
                )
                .tab_header("HE")
                .tab_stubhead(label="Budget Address Type")
                # Formats
                .fmt_number(columns=[
                    "Available_Commitment_Appropriations",
                    "L1_Commitment",
                    "RAC_on_Appropriation"
                ], accounting=True, decimals=2)
                .fmt_percent(
                    columns="ratio_consumed_of_L1_and_L2_against_Commitment_Appropriations",
                    decimals=2
                )
                # Custom column labels
                .cols_label(
                    Available_Commitment_Appropriations=html("Available Commitment Appropriations<br>(1)"),
                    L1_Commitment=html("L1 Commitments or <br> Direct L2 <br>(2)"),
                    RAC_on_Appropriation=html("RAC on Appropriation<br>(3) = (1) - (2)"),
                    ratio_consumed_of_L1_and_L2_against_Commitment_Appropriations=html(
                        "% Commitment Appropriations by <br> L1 and Direct L2 Commitments <br> (4) = (2) / (1)"
                    )
                )
                # Arial font
                .opt_table_font(font="Arial")
                # Header and stub styling
                .tab_style(
                    style=[
                        style.text(color=DARK_BLUE, weight="bold"),
                        style.fill(color=LIGHT_BLUE),
                        style.css(f"border-bottom: 2px solid {DARK_BLUE}; border-right: 2px solid {DARK_BLUE}; border-top: 2px solid {DARK_BLUE}; border-left: 2px solid {DARK_BLUE};"),
                        style.css("max-width:200px; line-height:1.2"),
                    ],
                    locations=loc.row_groups()
                )
                .tab_style(
                    style=[
                        style.fill(color=BLUE),
                        style.text(color="white", weight="bold", align='center'),
                        style.css("text-align: center; vertical-align: middle; max-width:200px; line-height:1.2")
                    ],
                    locations=loc.column_labels()
                )
                .tab_style(
                    style=[
                        style.fill(color=BLUE),
                        style.text(color="white", weight="bold"),
                        style.css("text-align: center; vertical-align: middle; max-width:200px; line-height:1.2")
                    ],
                    locations=loc.stubhead()
                )
                .tab_style(
                    style=[
                        style.text(color=DARK_BLUE, weight="bold"),
                    ],
                    locations=loc.header()
                )
                # Grid lines
                .tab_style(
                    style=style.borders(weight="1px", color=DARK_BLUE),
                    locations=loc.stub()
                )
                .tab_style(
                    style=style.borders(sides=["all"], color=DARK_BLUE, weight='1px'),
                    locations=loc.body()
                )
                .tab_style(
                    style=style.borders(color=DARK_BLUE, weight='2px'),
                    locations=loc.column_labels()
                )
                .tab_style(
                    style=style.borders(color=DARK_BLUE, weight='2px'),
                    locations=loc.stubhead()
                )
                # Style total row
                .tab_style(
                    style=[style.fill(color="#E6E6FA"), style.text(color="black", weight="bold")],
                    locations=loc.body(rows=agg.index[-1])
                )
                .tab_style(
                    style=[style.fill(color="#E6E6FA"), style.text(weight="bold")],
                    locations=loc.stub(rows=[-1])
                )
                # Table borders
                .tab_options(table_body_border_bottom_color=DARK_BLUE, table_body_border_bottom_width="2px")
                .tab_options(table_border_right_color=DARK_BLUE, table_border_right_width="2px")
                .tab_options(table_border_left_color=DARK_BLUE, table_border_left_width="2px")
                .tab_options(table_border_top_color=DARK_BLUE, table_border_top_width="2px")
                .tab_options(column_labels_border_top_color=DARK_BLUE, column_labels_border_top_width="2px")
                # Source notes
                .tab_source_note("Source: Summa DataWarehouse")
                .tab_source_note("BO Report: C0_Budgetary_Execution_Details")
            )
        except Exception as e:
            logging.error(f"Error building GreatTables object: {str(e)}")
            # Return the aggregated DataFrame without styling if table creation fails
            return agg
        
        # Store the table
        try:
            insert_variable(
                report=report,
                module="BudgetModule",
                var="table_1a_commitment_summary",
                value=agg.to_dict(orient="records"),
                db_path=db_path,
                anchor="table_1a",
                gt_table=tbl
            )
            logging.debug("Successfully stored table_1a_commitment_summary")
        except Exception as e:
            logging.error(f"Error storing table: {str(e)}")
            # Continue even if storage fails, as the table is already built
        
        return agg
    
    except Exception as e:
        logging.error(f"Unexpected error in build_commitment_summary_table: {str(e)}")
        return pd.DataFrame(columns=[
            "Fund Source", "Budget Address Type", "Available_Commitment_Appropriations",
            "L1_Commitment", "RAC_on_Appropriation", 
            "ratio_consumed_of_L1_and_L2_against_Commitment_Appropriations"
        ])

# ──────────────────────────────────────────────────────────────────────────────

def build_payment_summary_tables(
    df: pd.DataFrame,
    current_year: int,
    report: str,
    db_path: str,
    table_colors: dict = None
) -> Dict[str, pd.DataFrame]:
    """
    Build & store one payment-summary table per programme (HE, H2020).

    Args:
        df (pd.DataFrame): Input DataFrame with payment data.
        current_year (int): The year to filter the Budget Period.
        report (str): Name of the report for storing the tables.
        db_path (str): Path to the SQLite database for storing the tables.

    Returns:
        dict
            keys = programme code ("HE", "H2020")
            value = aggregated DataFrame for that programme
    """
    # Define expected columns for validation
    expected_columns = [
        "Budget Period", "Fund Source", "Functional Area Desc",
        "Budget Address", "Payment Appropriation", "Paid Amount", "Payment Available"
    ]

    # Validate input DataFrame
    if not isinstance(df, pd.DataFrame):
        logging.error("Input 'df' is not a pandas DataFrame.")
        return {"HE": pd.DataFrame(), "H2020": pd.DataFrame()}

    if df.empty:
        logging.warning("Input DataFrame is empty. Returning empty DataFrames for all programmes.")
        return {"HE": pd.DataFrame(), "H2020": pd.DataFrame()}

    # Check for missing columns
    missing_columns = [col for col in expected_columns if col not in df.columns]
    if missing_columns:
        logging.error(f"Missing required columns: {missing_columns}")
        return {"HE": pd.DataFrame(), "H2020": pd.DataFrame()}

    # Log input DataFrame info
    logging.debug(f"Input DataFrame shape: {df.shape}")
    logging.debug(f"Input DataFrame columns: {df.columns.tolist()}")

    try:
        # Common pre-filtering
        df = df[(df["Budget Period"] == current_year) & (df["Fund Source"].isin(["VOBU", "EFTA"]))].copy()
        logging.debug(f"After pre-filtering (Budget Period == {current_year} & Fund Source in ['VOBU', 'EFTA']): {df.shape[0]} rows")

        # Map Functional Area → Programme
        df.loc[:, "Programme"] = df["Functional Area Desc"].replace({
            "HORIZONEU_21_27": "HE",
            "H2020_14_20": "H2020"
        })
        logging.debug(f"Unique Programme values: {df['Programme'].unique()}")

        # Budget-type mapping helper
        def map_budget_type(val):
            if pd.isna(val):
                return None
            v = str(val).upper()
            if "EMPTY" in v:
                return "Main Calls"
            if "EXPERTS" in v:
                return "Experts"
            return val

        df.loc[:, "Budget_Address_Type"] = df["Budget Address"].apply(map_budget_type)
        logging.debug(f"Unique Budget_Address_Type values: {df['Budget_Address_Type'].unique()}")

        # Set Fund Source for all rows
        df.loc[:, "Fund Source"] = "VOBU/EFTA"
        logging.debug(f"Fund Source set to: {df['Fund Source'].unique()}")

        # Iterate over the two programmes
        results: Dict[str, pd.DataFrame] = {}
        for programme in ("HE", "H2020"):
            df_p = df[df["Programme"] == programme].copy()

            if df_p.empty:
                logging.warning(f"No rows for programme {programme}. Skipping.")
                results[programme] = pd.DataFrame(columns=[
                    "Fund Source", "Budget Address Type", "Available_Payment_Appropriations",
                    "Paid_Amount", "Remaining_Payment_Appropriation", "ratio_consumed_Payment_Appropriations"
                ])
                continue

            try:
                # Aggregation
                agg = (df_p
                       .groupby(["Fund Source", "Budget_Address_Type"],
                                as_index=False)[
                           ["Payment Appropriation", "Paid Amount", "Payment Available"]
                       ].sum())
                logging.debug(f"Aggregated DataFrame for {programme} shape: {agg.shape}")

                # Compute percentage with zero division handling
                agg["%"] = agg["Paid Amount"] / agg["Payment Appropriation"].replace(0, pd.NA)
                agg["%"] = agg["%"].fillna(0)

                # Rename columns
                agg = agg.rename(columns={
                    "Payment Appropriation": "Available_Payment_Appropriations",
                    "Paid Amount": "Paid_Amount",
                    "Payment Available": "Remaining_Payment_Appropriation",
                    "%": "ratio_consumed_Payment_Appropriations",
                    "Budget_Address_Type": "Budget Address Type"
                })

                # Add total row
                try:
                    total_row = pd.DataFrame({
                        "Fund Source": ["VOBU/EFTA"],
                        "Budget Address Type": ["Total"],
                        "Available_Payment_Appropriations": [agg["Available_Payment_Appropriations"].sum()],
                        "Paid_Amount": [agg["Paid_Amount"].sum()],
                        "Remaining_Payment_Appropriation": [agg["Remaining_Payment_Appropriation"].sum()],
                        "ratio_consumed_Payment_Appropriations": [
                            agg["Paid_Amount"].sum() / agg["Available_Payment_Appropriations"].sum()
                            if agg["Available_Payment_Appropriations"].sum() != 0 else 0
                        ]
                    })
                    agg = pd.concat([agg, total_row], ignore_index=True)
                    logging.debug(f"Final aggregated DataFrame for {programme} with total row: {agg.shape}")
                except Exception as e:
                    logging.error(f"Error adding total row for {programme}: {str(e)}")
                    continue  # Skip to next programme if total row fails

                # Build GreatTables object
                try:
                    tbl = (
                        GT(agg,
                           rowname_col="Budget Address Type",
                           groupname_col="Fund Source"
                           )
                        .tab_stubhead(label="Budget Address Type")
                        .tab_style(
                            style=[
                                style.text(color=DARK_BLUE, weight="bold", font='Arial'),
                                style.fill(color=LIGHT_BLUE),
                                style.css(f"border-bottom: 2px solid {DARK_BLUE}; border-right: 2px solid {DARK_BLUE}; border-top: 2px solid {DARK_BLUE}; border-left: 2px solid {DARK_BLUE};"),
                                style.css("max-width:200px; line-height:1.2"),
                            ],
                            locations=loc.row_groups()
                        )
                        .tab_header(title=f"{programme}")
                        .fmt_number(columns=[
                            "Available_Payment_Appropriations",
                            "Paid_Amount",
                            "Remaining_Payment_Appropriation"
                        ], accounting=True, decimals=2)
                        .fmt_percent(
                            columns="ratio_consumed_Payment_Appropriations",
                            decimals=2
                        )
                        .cols_label(
                            Available_Payment_Appropriations=html("Payment Appropriations<br>(1)"),
                            Paid_Amount=html("Payment Credits consumed<br>(Acceptance Date)<br>(2)"),
                            Remaining_Payment_Appropriation=html("Remaining Payment Appropriations<br>(3) = (1) - (2)"),
                            ratio_consumed_Payment_Appropriations=html("% Payment Consumed<br>(4) = (2) / (1)")
                        )
                        .opt_table_font(font="Arial")
                        .tab_style(
                            style=[style.text(weight="bold", color=DARK_BLUE)],
                            locations=loc.header()
                        )
                        .tab_style(
                            style=[style.fill(color=BLUE),
                                   style.text(color="white", weight="bold", align="center"),
                                   style.css("text-align: center; vertical-align: middle; max-width:200px; line-height:1.2")],
                            locations=loc.column_labels()
                        )
                        .tab_style(
                            style=[style.fill(color=BLUE),
                                   style.text(color="white", weight="bold"),
                                   style.css("text-align: center; vertical-align: middle; max-width:200px; line-height:1.2")],
                            locations=loc.stubhead()
                        )
                        .tab_style(
                            style=style.borders(weight="1px", color=DARK_BLUE),
                            locations=loc.stub()
                        )
                        .tab_style(
                            style=style.borders(sides="all", color=DARK_BLUE, weight="1px"),
                            locations=loc.body()
                        )
                        .tab_style(
                            style=style.borders(color=DARK_BLUE, weight="2px"),
                            locations=[loc.column_labels(), loc.stubhead()]
                        )
                        .tab_style(
                            style=[style.fill(color="#E6E6FA"), style.text(color="black", weight="bold")],
                            locations=loc.body(rows=agg.index[-1])
                        )
                        .tab_style(
                            style=[style.fill(color="#E6E6FA"), style.text(weight="bold")],
                            locations=loc.stub(rows=[-1])
                        )
                        .tab_options(table_body_border_bottom_color=DARK_BLUE, table_body_border_bottom_width="2px")
                        .tab_options(table_border_right_color=DARK_BLUE, table_border_right_width="2px")
                        .tab_options(table_border_left_color=DARK_BLUE, table_border_left_width="2px")
                        .tab_options(table_border_top_color=DARK_BLUE, table_border_top_width="2px")
                        .tab_options(column_labels_border_top_color=DARK_BLUE, column_labels_border_top_width="2px")
                        .tab_source_note("Source: Summa DataWarehouse")
                        .tab_source_note("BO Report: C0_Budgetary_Execution_Details")
                    )
                except Exception as e:
                    logging.error(f"Error building GreatTables object for {programme}: {str(e)}")
                    results[programme] = agg  # Store unstyled DataFrame if styling fails
                    continue

                # Store the table
                try:
                    insert_variable(
                        report=report,
                        module="BudgetModule",
                        var=f"table_2a_{programme}_data",
                        value=agg.to_dict(orient="records"),
                        db_path=db_path,
                        anchor=f"table_2a_{programme}",
                        gt_table=tbl
                    )
                    logging.debug(f"Stored table_2a_{programme}_data ({len(agg)} rows)")
                except Exception as e:
                    logging.error(f"Error storing table for {programme}: {str(e)}")
                    # Continue even if storage fails, as the table is already built

                results[programme] = agg

            except Exception as e:
                logging.error(f"Unexpected error processing {programme}: {str(e)}")
                results[programme] = pd.DataFrame(columns=[
                    "Fund Source", "Budget Address Type", "Available_Payment_Appropriations",
                    "Paid_Amount", "Remaining_Payment_Appropriation", "ratio_consumed_Payment_Appropriations"
                ])

        return results

    except Exception as e:
        logging.error(f"Unexpected error in build_payment_summary_tables: {str(e)}")
        return {"HE": pd.DataFrame(), "H2020": pd.DataFrame()}
#-------------------------------------------------------------------------------------------------


def build_commitment_detail_table_1(df: pd.DataFrame, current_year: int, report: str, db_path: str , table_colors: dict = None) -> pd.DataFrame:
    df["FR ILC Date (dd/mm/yyyy)"] = pd.to_datetime(df["FR ILC Date (dd/mm/yyyy)"], errors="coerce")

    next_year = current_year + 1
    eoy_next = pd.Timestamp(f"{next_year}-12-31")
    eoy_this = pd.Timestamp(f"{current_year}-12-31")

    # Budget‑type mapping helper
    def map_budget_type(val):
        if pd.isna(val):
            return None
        v = str(val).upper()
        if "VOBU" in v: return "VOBU/EFTA/IAR2/2"
        if "EFTA" in v: return "VOBU/EFTA/IAR2/2"
        if "IAR2/2" in v: return "VOBU/EFTA/IAR2/2"
        return val

    df.loc[:, 'Fund Source'] = df['Fund Source'].apply(map_budget_type)

    
    def map_fund_type(val):
            if pd.isna(val):
                return None
            v = str(val).strip().upper()
            if "GLOBAL" in v and "COMMITMENT" in v:
                return "MainCalls"
            if "PROVISIONAL" in v and "COMMITMENT" in v:
                return "Experts"
            return val

    df.loc[:, 'Source_Type'] = df['FR Earmarked Document Type Desc'].apply(map_fund_type)

    # ---------- GLOBAL COMMITMENTS -------------------------------------
    global_df = df[(df["Source_Type"] == "MainCalls") &
                   (df["FR ILC Date (dd/mm/yyyy)"] == eoy_next) & (df['Fund Source'] == "VOBU/EFTA/IAR2/2")].copy()

    logging.debug(f"Global Commitments - Rows after filter: {len(global_df)}")

    global_df = global_df.rename(columns={
        "FR Accepted Amount": "L1_Commitment_or_Direct_L2_1",
        "FR Consumption by PO Amount": "L2_Commitment_or_Payment_2"
    })

    agg_global = (global_df
                  .groupby(["FR Fund Reservation Desc", "Fund Source"],
                           as_index=False)[
                      ["L1_Commitment_or_Direct_L2_1", "L2_Commitment_or_Payment_2"]]
                  .sum())

    agg_global["RAC_on_L1_Commitment_or_RAL_Direct_L2_3"] = agg_global["L1_Commitment_or_Direct_L2_1"] + agg_global["L2_Commitment_or_Payment_2"]
    agg_global["Commitment_Implementation_rate_4"] = agg_global["L2_Commitment_or_Payment_2"] / (-1 * agg_global["L1_Commitment_or_Direct_L2_1"])

    global_df = agg_global[[
        "FR Fund Reservation Desc", "Fund Source", "L1_Commitment_or_Direct_L2_1", "L2_Commitment_or_Payment_2",
        "RAC_on_L1_Commitment_or_RAL_Direct_L2_3", "Commitment_Implementation_rate_4"
    ]]

    global_cols = [
        "FR Fund Reservation Desc", "Fund Source", "L1_Commitment_or_Direct_L2_1", "L2_Commitment_or_Payment_2",
        "RAC_on_L1_Commitment_or_RAL_Direct_L2_3", "Commitment_Implementation_rate_4"
    ]

    # ---------- PROVISIONAL COMMITMENTS -------------------------------------
    prov_df = df[(df["Source_Type"] == "Experts") &
                    (df["FR ILC Date (dd/mm/yyyy)"] ==  eoy_this)  & (df['Fund Source'] == "VOBU/EFTA/IAR2/2")].copy()

    logging.debug(f"Provisional Commitments - Rows after filter: {len(prov_df)}")

    prov_df = prov_df.rename(columns={
        "FR Accepted Amount": "L1_Commitment_or_Direct_L2_1",
        "FR Consumption by Payment Amount": "L2_Commitment_or_Payment_2"
    })

    prov_df["FR Fund Reservation Desc"] = 'Experts'

    agg_prov = (prov_df
                .groupby(["FR Fund Reservation Desc", "Fund Source"],
                         as_index=False)[
                    ["L1_Commitment_or_Direct_L2_1", "L2_Commitment_or_Payment_2"]]
                .sum())

    agg_prov["RAC_on_L1_Commitment_or_RAL_Direct_L2_3"] = agg_prov["L1_Commitment_or_Direct_L2_1"] + agg_prov["L2_Commitment_or_Payment_2"]
    agg_prov["Commitment_Implementation_rate_4"] = agg_prov["L2_Commitment_or_Payment_2"] / (-1 * agg_prov["L1_Commitment_or_Direct_L2_1"])

    prov_df = agg_prov[[
        "FR Fund Reservation Desc", "Fund Source", "L1_Commitment_or_Direct_L2_1", "L2_Commitment_or_Payment_2",
        "RAC_on_L1_Commitment_or_RAL_Direct_L2_3", "Commitment_Implementation_rate_4"
    ]]

    prov_cols = [
        "FR Fund Reservation Desc", "Fund Source", "L1_Commitment_or_Direct_L2_1", "L2_Commitment_or_Payment_2",
        "RAC_on_L1_Commitment_or_RAL_Direct_L2_3", "Commitment_Implementation_rate_4"
    ]

    # Check if both DataFrames are empty
    if global_df.empty and prov_df.empty:
        logging.warning("Both Global and Provisional Commitment DataFrames are empty. Returning an empty DataFrame.")
        # Create an empty DataFrame with the correct structure
        empty_df = pd.DataFrame(columns=global_cols)
        return empty_df

    # Prepare DataFrames for concatenation
    concat_dfs = []
    if not global_df.empty:
        concat_dfs.append(global_df[global_cols])
    if not prov_df.empty:
        concat_dfs.append(prov_df[prov_cols])

    # Concatenate only if there are DataFrames to concatenate
    if concat_dfs:
        combined = pd.concat(concat_dfs, axis=0, ignore_index=True)
    else:
        # This case should already be handled above, but adding for completeness
        combined = pd.DataFrame(columns=global_cols)

    # -------- Add subtotals by Fund Source -----------------------------------
    if not combined.empty:
        # Group by Fund Source and compute subtotals
        subtotals = combined.groupby("Fund Source", as_index=False).agg({
            "L1_Commitment_or_Direct_L2_1": "sum",
            "L2_Commitment_or_Payment_2": "sum",
            "RAC_on_L1_Commitment_or_RAL_Direct_L2_3": "sum",
        })

        # Add the ratio for subtotals
        subtotals["Commitment_Implementation_rate_4"] = subtotals["L2_Commitment_or_Payment_2"] / (-1 * subtotals["L1_Commitment_or_Direct_L2_1"])
        subtotals["FR Fund Reservation Desc"] = "Subtotal"

        # Concatenate original data with subtotals
        final_rows = []
        for fund_source in combined["Fund Source"].unique():
            # Rows for this Fund Source
            group_rows = combined[combined["Fund Source"] == fund_source].copy()
            final_rows.append(group_rows)
            # Corresponding subtotal row
            subtotal_row = subtotals[subtotals["Fund Source"] == fund_source].copy()
            final_rows.append(subtotal_row)

        # Combine all rows into the final DataFrame
        agg_with_subtotals = pd.concat(final_rows, ignore_index=True)
    else:
        agg_with_subtotals = combined  # Empty DataFrame with correct columns

    # -------- GreatTables object -------------------------------------------
    # Only create the table if there is data
    if not agg_with_subtotals.empty:
        tbl = (
            GT(
                agg_with_subtotals,
                rowname_col="FR Fund Reservation Desc",
                groupname_col="Fund Source"
            )
            .tab_header(
                title="HE"
            )
            .tab_style(
                style.text(color=DARK_BLUE, weight="bold", align="center", font='Arial'),
                locations=loc.header()
            )
            .tab_stubhead(label="Budget Address Type")
            .tab_style(
                style=[
                    style.text(color=DARK_BLUE, weight="bold", font='Arial'),
                    style.fill(color=LIGHT_BLUE),
                    style.css(f"border-bottom: 2px solid {DARK_BLUE}; border-right: 2px solid {DARK_BLUE}; border-top: 2px solid {DARK_BLUE}; border-left: 2px solid {DARK_BLUE};"),
                    style.css("max-width:200px; line-height:1.2"),
                ],
                locations=loc.row_groups()
            )
            .fmt_number(columns=[
                "L1_Commitment_or_Direct_L2_1",
                "L2_Commitment_or_Payment_2",
                "RAC_on_L1_Commitment_or_RAL_Direct_L2_3"
            ], accounting=True, decimals=2)
            .fmt_percent(
                columns="Commitment_Implementation_rate_4",
                decimals=2
            )
            .cols_label(
                L1_Commitment_or_Direct_L2_1=html("L1 Commitments <br> or Direct L2 Commitments<br>(1)"),
                L2_Commitment_or_Payment_2=html("L2 Commitments or Payments<br>(2)"),
                RAC_on_L1_Commitment_or_RAL_Direct_L2_3=html("RAC on L1 Commitments or RAL Direct L2<br>(3) = (1) + (2)"),
                Commitment_Implementation_rate_4=html("% Commitment Implementation Rate<br>(4) = (2) / (1)")
            )
            .opt_table_font(font="Arial")
            .tab_style(
                style=[
                    style.fill(color=BLUE),
                    style.text(color="white", weight="bold", align="center"),
                    style.css("max-width:200px; line-height:1.2")
                ],
                locations=loc.column_labels()
            )
            .tab_style(
                style=[
                    style.fill(color=BLUE),
                    style.text(color="white", weight="bold", align="center"),
                    style.css("text-align: center; vertical-align: middle; max-width:200px; line-height:1.2")
                ],
                locations=loc.stubhead()
            )
            .tab_style(
                style=style.borders(weight="1px", color=DARK_BLUE),
                locations=loc.stub()
            )
            .tab_style(
                style=style.borders(sides="all", color=DARK_BLUE, weight="1px"),
                locations=loc.body()
            )
            .tab_style(
                style=style.borders(color=DARK_BLUE, weight="2px"),
                locations=[loc.column_labels(), loc.stubhead()]
            )
            .tab_style(
                style=[style.fill(color="#E6E6FA"), style.text(color="black", weight="bold")],
                locations=loc.body(rows=agg_with_subtotals.index[agg_with_subtotals["FR Fund Reservation Desc"] == "Subtotal"].tolist())
            )
            .tab_style(
                style=[style.fill(color="#E6E6FA"), style.text(weight="bold")],
                locations=loc.stub(rows=agg_with_subtotals.index[agg_with_subtotals["FR Fund Reservation Desc"] == "Subtotal"].tolist())
            )
            .tab_options(table_body_border_bottom_color=DARK_BLUE,
                         table_body_border_bottom_width="2px")
            .tab_options(table_border_right_color=DARK_BLUE,
                         table_border_right_width="2px")
            .tab_options(table_border_left_color=DARK_BLUE,
                         table_border_left_width="2px")
            .tab_options(table_border_top_color=DARK_BLUE,
                         table_border_top_width="2px")
            .tab_options(column_labels_border_top_color=DARK_BLUE,
                         column_labels_border_top_width="2px")

            .tab_source_note("Source: Summa DataWarehouse")
            .tab_source_note("BO Report: C0_COMMITMENTS_SUMMA")
        )

        # -------- store --------------------------------------------------------
        insert_variable(
            report=report,
            module="BudgetModule",
            var=f"table_1b_L1_current_year",
            value=agg_with_subtotals.to_dict(orient="records"),
            db_path=db_path,
            anchor=f"table_1b",
            gt_table=tbl,
        )

        logging.debug("Stored 1b table and data")
    else:
        logging.debug("No data to store for table 1b (empty DataFrame).")

    return agg_with_subtotals
#-------------------------------------------------------------------------------------------------

def build_commitment_detail_table_2(df: pd.DataFrame, current_year: int, report: str, db_path: str , table_colors: dict = None) -> pd.DataFrame:
    df["FR ILC Date (dd/mm/yyyy)"] = pd.to_datetime(df["FR ILC Date (dd/mm/yyyy)"], errors="coerce")
    eoy_this = pd.Timestamp(f"{current_year}-12-31")

    # Budget‑type mapping helper
    def map_budget_type(val):
        if pd.isna(val):
            return None
        v = str(val).upper()
        if "VOBU" in v: return "VOBU/EFTA/IAR2/2"
        if "EFTA" in v: return "VOBU/EFTA/IAR2/2"
        if "IAR2/2" in v: return "VOBU/EFTA/IAR2/2"
        return val

    df['Fund Source'] = df['Fund Source'].apply(map_budget_type)

    filtered = df[(df["FR Earmarked Document Type Desc"] == "Global Commitment") &
                  (df["FR ILC Date (dd/mm/yyyy)"] == eoy_this)].copy()

    filtered = filtered.rename(columns={
        "FR Accepted Amount": "L1_Commitment_1",
        "FR Consumption by PO Amount": "L2_Commitment_2"
    })

    agg = (filtered
           .groupby(["FR Fund Reservation Desc", "Fund Source"],
                    as_index=False)[
               ["L1_Commitment_1", "L2_Commitment_2"]]
           .sum())

    agg["RAC_on_L1_Commitment_3"] = agg["L1_Commitment_1"] + agg["L2_Commitment_2"]
    agg["ratio_L2_on_L1_Commitment_4"] = agg["L2_Commitment_2"] / (-1 * agg["L1_Commitment_1"])

    agg = agg[[
        "FR Fund Reservation Desc", "Fund Source", "L1_Commitment_1", "L2_Commitment_2",
        "RAC_on_L1_Commitment_3", "ratio_L2_on_L1_Commitment_4"
    ]]

    # -------- Add subtotals by Fund Source -----------------------------------
    # Group by Fund Source and compute subtotals
    subtotals = agg.groupby("Fund Source", as_index=False).agg({
        "L1_Commitment_1": "sum",
        "L2_Commitment_2": "sum",
        "RAC_on_L1_Commitment_3": "sum",
    })

    # Add the ratio for subtotals
    subtotals["ratio_L2_on_L1_Commitment_4"] = subtotals["L2_Commitment_2"] / (-1 * subtotals["L1_Commitment_1"])
    subtotals["FR Fund Reservation Desc"] = "Subtotal"

    # Concatenate original data with subtotals
    final_rows = []
    for fund_source in agg["Fund Source"].unique():
        # Rows for this Fund Source
        group_rows = agg[agg["Fund Source"] == fund_source].copy()
        final_rows.append(group_rows)
        # Corresponding subtotal row
        subtotal_row = subtotals[subtotals["Fund Source"] == fund_source].copy()
        final_rows.append(subtotal_row)

    # Combine all rows into the final DataFrame
    agg_with_subtotals = pd.concat(final_rows, ignore_index=True)

    # -------- GreatTables object -------------------------------------------
    tbl = (
        GT(
            agg_with_subtotals,
            rowname_col="FR Fund Reservation Desc",
            groupname_col="Fund Source"
        )
        .tab_stubhead(label="L1 Commitments")
        .tab_style(
            style=[
                style.text(color=DARK_BLUE, weight="bold", font='Arial'),
                style.fill(color=LIGHT_BLUE),
                style.css(f"border-bottom: 2px solid {DARK_BLUE}; border-right: 2px solid {DARK_BLUE}; border-top: 2px solid {DARK_BLUE}; border-left: 2px solid {DARK_BLUE};"),
                style.css("max-width:200px; line-height:1.2"),
            ],
            locations=loc.row_groups()
        )
        .fmt_number(columns=[
            "L1_Commitment_1",
            "L2_Commitment_2",
            "RAC_on_L1_Commitment_3"
        ], accounting=True, decimals=2)
        .fmt_percent(
            columns="ratio_L2_on_L1_Commitment_4",
            decimals=2)
        .cols_label(
            L1_Commitment_1=html("L1 Commitments<br>(1)"),
            L2_Commitment_2=html("L2 Commitments<br>(2)"),
            RAC_on_L1_Commitment_3=html("RAC on L1 Commitments <br>(3) = (1) - (2)"),
            ratio_L2_on_L1_Commitment_4=html("% L1 consumed by L2 (indirect)<br> (4) = (2) / (1)")
        )
        .opt_table_font(font="Arial")
        .tab_style(
                    style=[style.text(weight="bold", color=DARK_BLUE)],
                    locations=loc.header()
                    )
        .tab_style(
            style=[style.fill(color=BLUE),
                   style.text(color="white", weight="bold", align="center"),
                   style.css("max-width:200px; line-height:1.2")],
            locations=loc.column_labels())
        .tab_style(
            style=[style.fill(color=BLUE),
                   style.text(color="white", weight="bold"),
                   style.css("text-align: center;vertical-align: middle; max-width:200px; line-height:1.2")],
            locations=loc.stubhead())

        .tab_style(
        style.borders(weight="1px", color=DARK_BLUE),
        loc.stub(),
         )

        .tab_style(
            style=style.borders(sides="all", color=DARK_BLUE, weight="1px"),
            locations=loc.body())
        .tab_style(
            style=style.borders(color=DARK_BLUE, weight="2px"),
            locations=[loc.column_labels(), loc.stubhead()])
        .tab_style(
            style=[style.fill(color="#E6E6FA"), style.text(color="black", weight="bold")],
            locations=loc.body(rows=agg_with_subtotals.index[agg_with_subtotals["FR Fund Reservation Desc"] == "Subtotal"].tolist())
        )
        .tab_style(
                style=[style.fill(color="#E6E6FA"), style.text(weight="bold")],
                locations=loc.stub(rows=agg_with_subtotals.index[agg_with_subtotals["FR Fund Reservation Desc"] == "Subtotal"].tolist())
            )
        .tab_options(table_body_border_bottom_color=DARK_BLUE,
                     table_body_border_bottom_width="2px")
        .tab_options(table_border_right_color=DARK_BLUE,
                     table_border_right_width="2px")
        .tab_options(table_border_left_color=DARK_BLUE,
                     table_border_left_width="2px")
        .tab_options(table_border_top_color=DARK_BLUE,
                     table_border_top_width="2px")
        .tab_options(column_labels_border_top_color=DARK_BLUE,
                     column_labels_border_top_width="2px")

        .tab_source_note("Source: Summa DataWarehouse")
        .tab_source_note("BO Report: C0_COMMITMENTS_SUMMA")
    )

    # -------- store --------------------------------------------------------
    insert_variable(
        report=report,
        module="BudgetModule",
        var=f"table_1c_L1_previous_year",
        value=agg_with_subtotals.to_dict(orient="records"),
        db_path=db_path,
        anchor=f"table_1c",
        gt_table=tbl,
    )

    logging.debug("Stored 1c table and data")

    return agg_with_subtotals
          
# ------------------------------------------------------------------
# 1. build the context in ONE pass directly from the table
# ------------------------------------------------------------------
def build_docx_context(report_name: str, db_path: str , table_colors: dict = None) -> dict:
    con = sqlite3.connect(db_path)
    df = pd.read_sql_query(
        """
        SELECT anchor_name, value, gt_image
        FROM   report_variables
        WHERE  report_name = ?
        ORDER  BY created_at
        """,
        con,
        params=(report_name,),
    )
    con.close()

    context = {}
    for _, row in df.iterrows():
        anchor = row["anchor_name"]          # e.g. "table_1a"
        if row["gt_image"]:                  # <- BLOB is not NULL → use picture
            context[anchor] = InlineImage(
                tpl,                         #  tpl is the DocxTemplate instance
                BytesIO(row["gt_image"]),
                width=docx.shared.Inches(5))
        else:                                # no picture → use the data
            try:
                context[anchor] = json.loads(row["value"])
            except (TypeError, ValueError):
                context[anchor] = row["value"]   # plain string / number
    return context

# ------------------------------------------------------------------
# 2. GRANTING MODULE
# ------------------------------------------------------------------

def build_signatures_table(
    df: pd.DataFrame,
    cutoff: datetime,
    scope_months: list,
    exclude_topics: list,
    report: str,
    db_path: str,
    table_colors: dict = None
) -> Dict[str, Union[pd.DataFrame, GT]]:
    """
    Build a table summarizing grant signatures and under-preparation projects by topic.

    Args:
        df (pd.DataFrame): Input DataFrame with grant data.
        cutoff (datetime): Cutoff date to determine the current quarter and filter data.
        scope_months (list): List of months to include in the report.
        exclude_topics (list): List of topics to exclude from the analysis.
        report (str): Name of the report for storing the table.
        db_path (str): Path to the SQLite database for storing the table.

    Returns:
        dict
            keys = "data" (DataFrame), "table" (GreatTable object)
    """
    # Define expected columns for validation
    expected_columns = ["GA Signature - Commission", "Topic", "SIGNED", "STATUS "]

    # Validate input DataFrame
    if not isinstance(df, pd.DataFrame):
        logging.error("Input 'df' is not a pandas DataFrame.")
        return {"data": pd.DataFrame(), "table": None}

    if df.empty:
        logging.warning("Input DataFrame is empty. Returning empty DataFrame and None table.")
        return {"data": pd.DataFrame(), "table": None}

    # Check for missing columns
    missing_columns = [col for col in expected_columns if col not in df.columns]
    if missing_columns:
        logging.error(f"Missing required columns: {missing_columns}")
        return {"data": pd.DataFrame(), "table": None}

    # Log input DataFrame info
    logging.debug(f"Input DataFrame shape: {df.shape}")
    logging.debug(f"Input DataFrame columns: {df.columns.tolist()}")

    try:
        # Filter in-scope data for signed grants
        in_scope = (
            df["GA Signature - Commission"].dt.year.eq(cutoff.year) &
            df["GA Signature - Commission"].dt.month_name().isin(scope_months)
        )

        signed = df.loc[in_scope].copy()
        signed = signed[~signed["Topic"].isin(exclude_topics)]

        # Create pivot table for signed data
        tab3_signed = (
            signed.pivot_table(
                index=signed["GA Signature - Commission"].dt.month_name(),
                columns="Topic",
                values="SIGNED",
                aggfunc="sum",
                fill_value=0,
            )
            .reindex(scope_months)
            .reset_index(names="Signature Month")
        )

        # Add TOTAL column for signed data
        tab3_signed["TOTAL"] = tab3_signed.iloc[:, 1:].sum(axis=1)

        # Define a mapping of months to quarters
        month_to_quarter = {
            "January": 1, "February": 1, "March": 1,
            "April": 2, "May": 2, "June": 2,
            "July": 3, "August": 3, "September": 3,
            "October": 4, "November": 4, "December": 4
        }

        # Add a quarter column to tab3_signed
        tab3_signed["Quarter"] = tab3_signed["Signature Month"].map(month_to_quarter)

        # Determine the current quarter based on cutoff date
        current_quarter = (cutoff.month - 1) // 3 + 1

        # Prepare final DataFrame for signed data with conditional quarterly aggregation
        if not tab3_signed.empty:
            final_rows = []
            
            # Check if the data contains exactly three months
            unique_months = tab3_signed["Signature Month"].nunique()
            max_quarter = tab3_signed["Quarter"].max()

            if unique_months == 3 and max_quarter == 1:
                # Special case: exactly three months, all in Quarter 1, show individually
                final_rows.append(tab3_signed.drop(columns=["Quarter"]))
            else:
                # General case: aggregate previous quarters, show current quarter months individually
                for quarter in sorted(tab3_signed["Quarter"].unique()):
                    quarter_data = tab3_signed[tab3_signed["Quarter"] == quarter].copy()
                    
                    if quarter < current_quarter:
                        # Aggregate previous quarters into a single row
                        quarter_sum = quarter_data.iloc[:, 1:-1].sum(numeric_only=True)
                        quarter_row = pd.DataFrame({
                            "Signature Month": [f"Quarter {quarter}"],
                            **{col: [quarter_sum[col]] for col in quarter_data.columns[1:-2]},
                            "TOTAL": [quarter_sum["TOTAL"]]
                        })
                        final_rows.append(quarter_row)
                    else:
                        # Keep individual months for the current quarter
                        quarter_data = quarter_data.drop(columns=["Quarter"])
                        final_rows.append(quarter_data)

            # Compute Grand Total for signed data
            col_totals = pd.DataFrame(tab3_signed.iloc[:, 1:-1].sum(), columns=["Grand Total"]).T
            col_totals.insert(0, "Signature Month", "Grand Total")
            for col in tab3_signed.columns[1:-2]:
                col_totals[col] = tab3_signed[col].sum()

            # Combine signed data rows
            agg_with_totals = pd.concat(final_rows + [col_totals], ignore_index=True)
        else:
            agg_with_totals = tab3_signed

        agg_with_totals['Status'] = 'Signed'
        logging.debug(f"Signed data processed. Shape: {agg_with_totals.shape}")

        # Under-preparation in-scope
        under_prep = df[df['STATUS '].eq("UNDER_PREPARATION")]
        under_prep = under_prep[~under_prep["Topic"].isin(exclude_topics)]

        # --- UNDER-PREPARATION • one “total” row --------------------------------
        tab3_prep = (
            under_prep
            .assign(UNDER_PREP=1)
            .pivot_table(
                columns="Topic",
                values="UNDER_PREP",
                aggfunc="sum",
                fill_value=0,
            )
        )

        # Label the row
        tab3_prep.index = ["Total Under Prep"]
        tab3_prep = tab3_prep.reset_index(names="Signature Month")

        # Add TOTAL column for under preparation data
        tab3_prep["TOTAL"] = tab3_prep.iloc[:, 1:].sum(axis=1)

        tab3_prep['Status'] = 'Under Preparation'
        logging.debug(f"Under Preparation data processed. Shape: {tab3_prep.shape}")

        # Merge agg_with_totals with tab3_prep
        final_df = pd.concat([agg_with_totals, tab3_prep], ignore_index=True)
        logging.debug(f"Final DataFrame shape: {final_df.shape}")

        # Define columns to display in the table (starting from index 1)
        display_columns = final_df.columns[1:-1].tolist()  # Exclude "Signature Month" and "Status"

        # Define colors
        DARK_BLUE = "#00008B"
        LIGHT_BLUE = "#ADD8E6"
        BLUE = "#0000FF"

        # Create the great table
        if not final_df.empty:
            tbl = (
                GT(
                    final_df,
                    rowname_col="Signature Month",
                    groupname_col="Status"
                )
                .tab_header(
                    title="Signatures and Under Preparation by Topic"
                )
                .tab_style(
                    style.text(color=DARK_BLUE, weight="bold", align="center", font='Arial'),
                    locations=loc.header()
                )
                .tab_stubhead(label="Signature Month")
                .tab_style(
                    style=[
                        style.text(color=DARK_BLUE, weight="bold", font='Arial'),
                        style.fill(color=LIGHT_BLUE),
                        style.css(f"border-bottom: 2px solid {DARK_BLUE}; border-right: 2px solid {DARK_BLUE}; border-top: 2px solid {DARK_BLUE}; border-left: 2px solid {DARK_BLUE};"),
                        style.css("max-width:200px; line-height:1.2"),
                    ],
                    locations=loc.row_groups()
                )
                .fmt_number(
                    columns=display_columns,
                    decimals=0,
                    use_seps=True
                )
                .cols_label(
                    **{col: html(col.replace("_", " ").replace("SyG", "SyG")) for col in display_columns}
                )
                .opt_table_font(font='Arial')
                .tab_style(
                    style=[
                        style.fill(color=BLUE),
                        style.text(color="white", weight="bold", align="center"),
                        style.css("max-width:200px; line-height:1.2")
                    ],
                    locations=loc.column_labels(columns=display_columns)
                )
                .tab_style(
                    style=[
                        style.fill(color=BLUE),
                        style.text(color="white", weight="bold", align="center"),
                        style.css("text-align: center; vertical-align: middle; max-width:200px; line-height:1.2")
                    ],
                    locations=loc.stubhead()
                )
                .tab_style(
                    style=style.borders(weight="1px", color=DARK_BLUE),
                    locations=loc.stub()
                )
                .tab_style(
                    style=style.borders(sides="all", color=DARK_BLUE, weight="1px"),
                    locations=loc.body(columns=display_columns)
                )
                .tab_style(
                    style=style.borders(color=DARK_BLUE, weight="2px"),
                    locations=[loc.column_labels(columns=display_columns), loc.stubhead()]
                )
                .tab_style(
                    style=[style.fill(color="#D3D3D3"), style.text(color="black", weight="bold")],
                    locations=loc.body(
                        rows=final_df.index[final_df["Signature Month"] == "Grand Total"].tolist(),
                        columns=display_columns
                    )
                )
                .tab_style(
                    style=[style.fill(color="#D3D3D3"), style.text(weight="bold")],
                    locations=loc.stub(rows=final_df.index[final_df["Signature Month"] == "Grand Total"].tolist())
                )
                .tab_options(
                    table_body_border_bottom_color=DARK_BLUE,
                    table_body_border_bottom_width="2px",
                    table_border_right_color=DARK_BLUE,
                    table_border_right_width="2px",
                    table_border_left_color=DARK_BLUE,
                    table_border_left_width="2px",
                    table_border_top_color=DARK_BLUE,
                    table_border_top_width="2px",
                    column_labels_border_top_color=DARK_BLUE,
                    column_labels_border_top_width="2px"
                )
                .tab_source_note("Source: Quarterly Report Data")
            )
        else:
            tbl = None
            logging.warning("Final DataFrame is empty. Skipping table creation.")

        # Store the table (similar to the example)
        try:
            insert_variable(
                report=report,
                module="GrantsModule",
                var="table_3a_signatures_data",
                value=final_df.to_dict(orient="records"),
                db_path=db_path,
                anchor="table_3_signatures",
                gt_table=tbl
            )
            logging.debug(f"Stored table_3a_signatures_data ({len(final_df)} rows)")
        except Exception as e:
            logging.error(f"Error storing table: {str(e)}")

        return {"data": final_df, "table": tbl}

    except Exception as e:
        logging.error(f"Unexpected error in build_signatures_table: {str(e)}")
        return {"data": pd.DataFrame(), "table": None}


def build_commitments_table(
    df: pd.DataFrame,
    cutoff: datetime,
    scope_months: list,
    exclude_topics: list,
    report: str,
    db_path: str,
    table_colors: dict = None
) -> Dict[str, Union[pd.DataFrame, GT]]:
    """
    Build a table summarizing grant signatures and under-preparation projects by topic.

    Args:
        df (pd.DataFrame): Input DataFrame with grant data.
        cutoff (datetime): Cutoff date to determine the current quarter and filter data.
        scope_months (list): List of months to include in the report.
        exclude_topics (list): List of topics to exclude from the analysis.
        report (str): Name of the report for storing the table.
        db_path (str): Path to the SQLite database for storing the table.

    Returns:
        dict
            keys = "data" (DataFrame), "table" (GreatTable object)
    """
    # Define expected columns for validation
    expected_columns = ["GA Signature - Commission", "Topic", "SIGNED", "STATUS "]

    # Validate input DataFrame
    if not isinstance(df, pd.DataFrame):
        logging.error("Input 'df' is not a pandas DataFrame.")
        return {"data": pd.DataFrame(), "table": None}

    if df.empty:
        logging.warning("Input DataFrame is empty. Returning empty DataFrame and None table.")
        return {"data": pd.DataFrame(), "table": None}

    # Check for missing columns
    missing_columns = [col for col in expected_columns if col not in df.columns]
    if missing_columns:
        logging.error(f"Missing required columns: {missing_columns}")
        return {"data": pd.DataFrame(), "table": None}

    # Log input DataFrame info
    logging.debug(f"Input DataFrame shape: {df.shape}")
    logging.debug(f"Input DataFrame columns: {df.columns.tolist()}")

    try:

        in_scope = (
                    df["Commitment AO visa"].dt.year.eq(cutoff.year)  &
                    df["Commitment AO visa"].dt.month_name().isin(scope_months)
                )

        committed = df.loc[in_scope].copy()
        committed = committed[~committed["Topic"].isin(exclude_topics)]

        tab3_commit   = (
                committed.pivot_table(
                    index=committed["Commitment AO visa"].dt.month_name(),
                    columns="Topic",
                    values="Eu contribution",
                    aggfunc="sum",
                    fill_value=0,
                )
                .reindex(scope_months)           # Jan-… only those in scope
                .reset_index(names="Commitment Month")
            )

        tab3_commit["TOTAL"] = tab3_commit.iloc[:, 1:].sum(axis=1)

        # Define a mapping of months to quarters
        month_to_quarter = {
            "January": 1, "February": 1, "March": 1,
            "April": 2, "May": 2, "June": 2,
            "July": 3, "August": 3, "September": 3,
            "October": 4, "November": 4, "December": 4
        }

        # Add a quarter column to tab3_signed
        tab3_commit["Quarter"] = tab3_commit["Commitment Month"].map(month_to_quarter)

        # Determine the current quarter based on cutoff date (May 12, 2025 -> Quarter 2)
        current_quarter = (cutoff.month - 1) // 3 + 1  # Quarter 2 for May

        # Prepare final DataFrame with conditional quarterly aggregation
        if not tab3_commit.empty:
            final_rows = []
            
            # Check if the data contains exactly three months
            unique_months = tab3_commit["Commitment Month"].nunique()
            max_quarter = tab3_commit["Quarter"].max()

            if unique_months == 3 and max_quarter == 1:
                # Special case: exactly three months, all in Quarter 1, show individually
                final_rows.append(tab3_commit.drop(columns=["Quarter"]))
            else:
                # General case: aggregate previous quarters, show current quarter months individually
                for quarter in sorted(tab3_commit["Quarter"].unique()):
                    quarter_data = tab3_commit[tab3_commit["Quarter"] == quarter].copy()
                    
                    if quarter < current_quarter:
                        # Aggregate previous quarters into a single row
                        quarter_sum = quarter_data.iloc[:, 1:-1].sum(numeric_only=True)
                        quarter_row = pd.DataFrame({
                            "Commitment Month": [f"Quarter {quarter}"],
                            **{col: [quarter_sum[col]] for col in quarter_data.columns[1:-2]},  # Topics
                            "TOTAL": [quarter_sum["TOTAL"]]
                        })
                        final_rows.append(quarter_row)
                    else:
                        # Keep individual months for the current quarter
                        quarter_data = quarter_data.drop(columns=["Quarter"])
                        final_rows.append(quarter_data)

            # Compute Grand Total
            col_totals = pd.DataFrame(tab3_commit.iloc[:, 1:-1].sum(), columns=["Grand Total"]).T
            col_totals.insert(0, "Commitment Month", "Grand Total")
            for col in tab3_commit.columns[1:-2]:  # Add totals for each topic column
                col_totals[col] = tab3_commit[col].sum()

            # Combine all rows
            agg_with_totals = pd.concat(final_rows + [col_totals], ignore_index=True)
        else:
            agg_with_totals = tab3_commit

        agg_with_totals['Type'] = 'Committed Amounts(EUR)'

        # Define columns to display in the table (starting from index 1)
        display_columns = agg_with_totals.columns[1:-1].tolist()  # Exclude "Signature Month" and "Status"
        
        #***************** Second table ****************************
        tab3_commit_n   = (
        committed.pivot_table(
            index=committed["Commitment AO visa"].dt.month_name(),
            columns="Topic",
            values="Eu contribution",
            aggfunc="count",
            fill_value=0,
        )
        .reindex(scope_months)           # Jan-… only those in scope
        .reset_index(names="Commitment Month")
       )

        tab3_commit_n["TOTAL"] = tab3_commit_n.iloc[:, 1:].sum(axis=1)

        # Define a mapping of months to quarters
        month_to_quarter = {
            "January": 1, "February": 1, "March": 1,
            "April": 2, "May": 2, "June": 2,
            "July": 3, "August": 3, "September": 3,
            "October": 4, "November": 4, "December": 4
        }

        # Add a quarter column to tab3_signed
        tab3_commit_n["Quarter"] = tab3_commit_n["Commitment Month"].map(month_to_quarter)

        # Determine the current quarter based on cutoff date (May 12, 2025 -> Quarter 2)
        current_quarter = (cutoff.month - 1) // 3 + 1  # Quarter 2 for May

        # Prepare final DataFrame with conditional quarterly aggregation
        if not tab3_commit_n.empty:
            final_rows = []
            
            # Check if the data contains exactly three months
            unique_months = tab3_commit_n["Commitment Month"].nunique()
            max_quarter = tab3_commit_n["Quarter"].max()

            if unique_months == 3 and max_quarter == 1:
                # Special case: exactly three months, all in Quarter 1, show individually
                final_rows.append(tab3_commit_n.drop(columns=["Quarter"]))
            else:
                # General case: aggregate previous quarters, show current quarter months individually
                for quarter in sorted(tab3_commit_n["Quarter"].unique()):
                    quarter_data = tab3_commit_n[tab3_commit_n["Quarter"] == quarter].copy()
                    
                    if quarter < current_quarter:
                        # Aggregate previous quarters into a single row
                        quarter_sum = quarter_data.iloc[:, 1:-1].sum(numeric_only=True)
                        quarter_row = pd.DataFrame({
                            "Commitment Month": [f"Quarter {quarter}"],
                            **{col: [quarter_sum[col]] for col in quarter_data.columns[1:-2]},  # Topics
                            "TOTAL": [quarter_sum["TOTAL"]]
                        })
                        final_rows.append(quarter_row)
                    else:
                        # Keep individual months for the current quarter
                        quarter_data = quarter_data.drop(columns=["Quarter"])
                        final_rows.append(quarter_data)

            # Compute Grand Total
            col_totals = pd.DataFrame(tab3_commit_n.iloc[:, 1:-1].sum(), columns=["Grand Total"]).T
            col_totals.insert(0, "Commitment Month", "Grand Total")
            for col in tab3_commit_n.columns[1:-2]:  # Add totals for each topic column
                col_totals[col] = tab3_commit_n[col].sum()

            # Combine all rows
            agg_with_totals_n = pd.concat(final_rows + [col_totals], ignore_index=True)
        else:
            agg_with_totals_n = tab3_commit_n

        agg_with_totals_n['Type'] = 'Number of Commitments'

        # Append agg_with_totals_n to agg_with_totals to create the final combined table
        final_agg_table = pd.concat([agg_with_totals, agg_with_totals_n], ignore_index=True)

        # Define columns to display in the table (starting from index 1)
        display_columns = final_agg_table.columns[1:-1].tolist()  # Exclude "Signature Month" and "Status"
        # Create the great table
        if not final_agg_table.empty:
            tbl = (
                GT(
                    final_agg_table,
                    rowname_col="Commitment Month",
                    groupname_col="Type"
                )
                .tab_header(
                    title="HE Commitment Activity"
                )

                # Format "amounts" group as currency (EUR with 2 decimal places)
                .fmt_number(
                    columns=display_columns,
                    rows=final_agg_table.index[final_agg_table["Type"] == 'Committed Amounts(EUR)'].tolist(),
                    accounting=True,
                    decimals=2,
                    use_seps=True
                )
                # Format "numbers" group as integers
                .fmt_number(
                    columns=display_columns,
                    rows=final_agg_table.index[final_agg_table["Type"] == 'Number of Commitments'].tolist(),
                    decimals=0,
                    use_seps=True
                )
                .tab_style(
                    style.text(color=DARK_BLUE, weight="bold", align="center", font='Arial'),
                    locations=loc.header()
                )
                .tab_stubhead(label="Commitment Month")
                .tab_style(
                    style=[
                        style.text(color=DARK_BLUE, weight="bold", font='Arial', size='medium'),
                        style.fill(color=LIGHT_BLUE),
                        style.css(f"border-bottom: 2px solid {DARK_BLUE}; border-right: 2px solid {DARK_BLUE}; border-top: 2px solid {DARK_BLUE}; border-left: 2px solid {DARK_BLUE};"),
                        style.css("max-width:200px; line-height:1.2"),
                    ],
                    locations=loc.row_groups()
                )

                .opt_table_font(font="Arial")
                .tab_style(
                    style=[
                        style.fill(color=BLUE),
                        style.text(color="white", weight="bold", align="center", size='small'),
                        style.css("max-width:200px; line-height:1.2")
                    ],
                    locations=loc.column_labels()
                )
                .tab_style(
                    style=[
                        style.fill(color=BLUE),
                        style.text(color="white", weight="bold", align="center",  size='small'),
                        style.css("text-align: center; vertical-align: middle; max-width:200px; line-height:1.2")
                    ],
                    locations=loc.stubhead()
                )
                .tab_style(
                    style=[style.borders(weight="1px", color=DARK_BLUE),
                        style.text( size='small')],
                    locations=loc.stub()
                )
                .tab_style(
                    style=[style.borders(sides="all", color=DARK_BLUE, weight="1px"),
                        style.text( align="center",  size='small')],
                    locations=loc.body()
                )
                .tab_style(
                    style=style.borders(color=DARK_BLUE, weight="2px"),
                    locations=[loc.column_labels(), loc.stubhead()]
                )
                .tab_style(
                    style=[style.fill(color="#D3D3D3"), style.text(color="black", weight="bold")],
                    locations=loc.body(rows=final_agg_table.index[final_agg_table["Commitment Month"] == "Grand Total"].tolist())
                )
                .tab_style(
                    style=[style.fill(color="#D3D3D3"), style.text(color="black", weight="bold")],
                    locations=loc.stub(rows=final_agg_table.index[final_agg_table["Commitment Month"] == "Grand Total"].tolist())
                )
                .tab_options(
                    table_body_border_bottom_color=DARK_BLUE,
                    table_body_border_bottom_width="2px",
                    table_border_right_color=DARK_BLUE,
                    table_border_right_width="2px",
                    table_border_left_color=DARK_BLUE,
                    table_border_left_width="2px",
                    table_border_top_color=DARK_BLUE,
                    table_border_top_width="2px",
                    column_labels_border_top_color=DARK_BLUE,
                    column_labels_border_top_width="2px"
                )
                .tab_source_note("Source: Compass")
                .tab_source_note("Reports : Call Overview Report - Budget Follow-Up Report - Ethics Requirements and Issues " )
                .tab_style(
                            style=[ style.text(size="small")],
                            locations=loc.footer()
                        )
                
            )
        else:
            tbl = None
            logging.warning("Final DataFrame is empty. Skipping table creation.")

        # Store the table (similar to the example)
        try:
            insert_variable(
                report=report,
                module="GrantsModule",
                var="table_3_commitments_data",
                value=final_agg_table.to_dict(orient="records"),
                db_path=db_path,
                anchor="table_3b_commitments",
                gt_table=tbl
            )
            logging.debug(f"Stored table_3b_commitments_data ({len(final_agg_table)} rows)")
        except Exception as e:
            logging.error(f"Error storing table: {str(e)}")

        return {"data": final_agg_table, "table": tbl}

    except Exception as e:
        logging.error(f"Unexpected error in build_commitments_table: {str(e)}")
        return {"data": pd.DataFrame(), "table": None}


def build_po_exceeding_FDI_tb_3c(df: pd.DataFrame, current_year: int, report: str, db_path: str, table_colors: dict = None) -> pd.DataFrame:
    # Convert date column to datetime
    df["FR ILC Date (dd/mm/yyyy)"] = pd.to_datetime(df["FR ILC Date (dd/mm/yyyy)"], errors="coerce")

    next_year = current_year + 1
    eoy_next = pd.Timestamp(f"{next_year}-12-31")
    eoy_this = pd.Timestamp(f"{current_year}-12-31")

    # Budget-type mapping helper
    def map_budget_type(val):
        if pd.isna(val):
            return None
        v = str(val).upper()
        if "VOBU" in v: return "VOBU/EFTA/IAR2/2"
        if "EFTA" in v: return "VOBU/EFTA/IAR2/2"
        if "IAR2/2" in v: return "VOBU/EFTA/IAR2/2"
        return val

    df.loc[:, 'Fund Source'] = df['Fund Source'].apply(map_budget_type)

    def map_fund_type(val):
        if pd.isna(val):
            return None
        v = str(val).strip().upper()
        if "GLOBAL" in v and "COMMITMENT" in v:
            return "MainCalls"
        if "PROVISIONAL" in v and "COMMITMENT" in v:
            return "Experts"
        return val

    df.loc[:, 'Source_Type'] = df['FR Earmarked Document Type Desc'].apply(map_fund_type)

    # ---------- GLOBAL COMMITMENTS -------------------------------------
    global_df = df[(df["Source_Type"] == "MainCalls") &
                   (df["FR ILC Date (dd/mm/yyyy)"] == eoy_next) & (df['Fund Source'] == "VOBU/EFTA/IAR2/2")].copy()

    logging.debug(f"Global Commitments - Rows after filter: {len(global_df)}")

    global_df = global_df.rename(columns={
        "FR Accepted Amount": "L1_Commitment_or_Direct_L2_1",
        "FR Consumption by PO Amount": "L2_Commitment_or_Payment_2"
    })

    agg_global = (global_df
                  .groupby(["FR Fund Reservation Desc", "Fund Source"],
                           as_index=False)[
                      ["L1_Commitment_or_Direct_L2_1", "L2_Commitment_or_Payment_2"]]
                  .sum())

    agg_global["RAC_on_L1_Commitment_or_RAL_Direct_L2_3"] = agg_global["L1_Commitment_or_Direct_L2_1"] + agg_global["L2_Commitment_or_Payment_2"]
    agg_global["Commitment_Implementation_rate_4"] = agg_global["L2_Commitment_or_Payment_2"] / (-1 * agg_global["L1_Commitment_or_Direct_L2_1"])

    global_df = agg_global[[
        "FR Fund Reservation Desc", "Fund Source", "L1_Commitment_or_Direct_L2_1", "L2_Commitment_or_Payment_2",
        "RAC_on_L1_Commitment_or_RAL_Direct_L2_3", "Commitment_Implementation_rate_4"
    ]]

    global_cols = [
        "FR Fund Reservation Desc", "Fund Source", "L1_Commitment_or_Direct_L2_1", "L2_Commitment_or_Payment_2",
        "RAC_on_L1_Commitment_or_RAL_Direct_L2_3", "Commitment_Implementation_rate_4"
    ]

    # ---------- PROVISIONAL COMMITMENTS -------------------------------------
    prov_df = df[(df["Source_Type"] == "Experts") &
                 (df["FR ILC Date (dd/mm/yyyy)"] == eoy_this) & (df['Fund Source'] == "VOBU/EFTA/IAR2/2")].copy()

    logging.debug(f"Provisional Commitments - Rows after filter: {len(prov_df)}")

    prov_df = prov_df.rename(columns={
        "FR Accepted Amount": "L1_Commitment_or_Direct_L2_1",
        "FR Consumption by Payment Amount": "L2_Commitment_or_Payment_2"
    })

    prov_df["FR Fund Reservation Desc"] = 'Experts'

    agg_prov = (prov_df
                .groupby(["FR Fund Reservation Desc", "Fund Source"],
                         as_index=False)[
                    ["L1_Commitment_or_Direct_L2_1", "L2_Commitment_or_Payment_2"]]
                .sum())

    agg_prov["RAC_on_L1_Commitment_or_RAL_Direct_L2_3"] = agg_prov["L1_Commitment_or_Direct_L2_1"] + agg_prov["L2_Commitment_or_Payment_2"]
    agg_prov["Commitment_Implementation_rate_4"] = agg_prov["L2_Commitment_or_Payment_2"] / (-1 * agg_prov["L1_Commitment_or_Direct_L2_1"])

    prov_df = agg_prov[[
        "FR Fund Reservation Desc", "Fund Source", "L1_Commitment_or_Direct_L2_1", "L2_Commitment_or_Payment_2",
        "RAC_on_L1_Commitment_or_RAL_Direct_L2_3", "Commitment_Implementation_rate_4"
    ]]

    prov_cols = [
        "FR Fund Reservation Desc", "Fund Source", "L1_Commitment_or_Direct_L2_1", "L2_Commitment_or_Payment_2",
        "RAC_on_L1_Commitment_or_RAL_Direct_L2_3", "Commitment_Implementation_rate_4"
    ]

    # Check if both DataFrames are empty
    if global_df.empty and prov_df.empty:
        logging.warning("Both Global and Provisional Commitment DataFrames are empty. Returning an empty DataFrame.")
        empty_df = pd.DataFrame(columns=global_cols)
        return empty_df

    # Prepare DataFrames for concatenation
    concat_dfs = []
    if not global_df.empty:
        concat_dfs.append(global_df[global_cols])
    if not prov_df.empty:
        concat_dfs.append(prov_df[prov_cols])

    # Concatenate only if there are DataFrames to concatenate
    if concat_dfs:
        combined = pd.concat(concat_dfs, axis=0, ignore_index=True)
    else:
        combined = pd.DataFrame(columns=global_cols)

    # -------- Add subtotals by Fund Source -----------------------------------
    if not combined.empty:
        # Group by Fund Source and compute subtotals
        subtotals = combined.groupby("Fund Source", as_index=False).agg({
            "L1_Commitment_or_Direct_L2_1": "sum",
            "L2_Commitment_or_Payment_2": "sum",
            "RAC_on_L1_Commitment_or_RAL_Direct_L2_3": "sum",
        })

        # Add the ratio for subtotals
        subtotals["Commitment_Implementation_rate_4"] = subtotals["L2_Commitment_or_Payment_2"] / (-1 * subtotals["L1_Commitment_or_Direct_L2_1"])
        subtotals["FR Fund Reservation Desc"] = "Subtotal"

        # Concatenate original data with subtotals
        final_rows = []
        for fund_source in combined["Fund Source"].unique():
            # Rows for this Fund Source
            group_rows = combined[combined["Fund Source"] == fund_source].copy()
            final_rows.append(group_rows)
            # Corresponding subtotal row
            subtotal_row = subtotals[subtotals["Fund Source"] == fund_source].copy()
            final_rows.append(subtotal_row)

        # Combine all rows into the final DataFrame
        agg_with_subtotals = pd.concat(final_rows, ignore_index=True)
    else:
        agg_with_subtotals = combined  # Empty DataFrame with correct columns

    # -------- GreatTables object -------------------------------------------
    # Use provided table_colors or default values
    table_colors = table_colors or {
        "heading_background_color": "#004A99",
        "row_group_background_color": "#d6e6f4",
        "border_color": "#01244B",
        "stub_background_color": "#d6e6f4",
        "body_background_color": "#ffffff",
        "subtotal_background_color": "#E6E6FA",
        "text_color": "#01244B"
    }

    # Only create the table if there is data
    if not agg_with_subtotals.empty:
        tbl = (
            GT(
                agg_with_subtotals,
                rowname_col="FR Fund Reservation Desc",
                groupname_col="Fund Source"
            )
            .tab_header(
                title="HE"
            )
            .tab_style(
                style.text(color=table_colors["text_color"], weight="bold", align="center", font='Arial'),
                locations=loc.header()
            )
            .tab_stubhead(label="Budget Address Type")
            .tab_style(
                style=[
                    style.text(color=table_colors["text_color"], weight="bold", font='Arial'),
                    style.fill(color=table_colors["row_group_background_color"]),
                    style.css(f"border-bottom: 2px solid {table_colors['border_color']}; border-right: 2px solid {table_colors['border_color']}; border-top: 2px solid {table_colors['border_color']}; border-left: 2px solid {table_colors['border_color']};"),
                    style.css("max-width:200px; line-height:1.2"),
                ],
                locations=loc.row_groups()
            )
            .fmt_number(columns=[
                "L1_Commitment_or_Direct_L2_1",
                "L2_Commitment_or_Payment_2",
                "RAC_on_L1_Commitment_or_RAL_Direct_L2_3"
            ], accounting=True, decimals=2)
            .fmt_percent(
                columns="Commitment_Implementation_rate_4",
                decimals=2
            )
            .cols_label(
                L1_Commitment_or_Direct_L2_1=html("L1 Commitments <br> or Direct L2 Commitments<br>(1)"),
                L2_Commitment_or_Payment_2=html("L2 Commitments or Payments<br>(2)"),
                RAC_on_L1_Commitment_or_RAL_Direct_L2_3=html("RAC on L1 Commitments or RAL Direct L2<br>(3) = (1) + (2)"),
                Commitment_Implementation_rate_4=html("% Commitment Implementation Rate<br>(4) = (2) / (1)")
            )
            .opt_table_font(font="Arial")
            .tab_style(
                style=[
                    style.fill(color=table_colors["heading_background_color"]),
                    style.text(color="white", weight="bold", align="center"),
                    style.css("max-width:200px; line-height:1.2")
                ],
                locations=loc.column_labels()
            )
            .tab_style(
                style=[
                    style.fill(color=table_colors["heading_background_color"]),
                    style.text(color="white", weight="bold", align="center"),
                    style.css("text-align: center; vertical-align: middle; max-width:200px; line-height:1.2")
                ],
                locations=loc.stubhead()
            )
            .tab_style(
                style=style.borders(weight="1px", color=table_colors["border_color"]),
                locations=loc.stub()
            )
            .tab_style(
                style=style.borders(sides="all", color=table_colors["border_color"], weight="1px"),
                locations=loc.body()
            )
            .tab_style(
                style=style.borders(color=table_colors["border_color"], weight="2px"),
                locations=[loc.column_labels(), loc.stubhead()]
            )
            .tab_style(
                style=[style.fill(color=table_colors["subtotal_background_color"]), style.text(color="black", weight="bold")],
                locations=loc.body(rows=agg_with_subtotals.index[agg_with_subtotals["FR Fund Reservation Desc"] == "Subtotal"].tolist())
            )
            .tab_style(
                style=[style.fill(color=table_colors["subtotal_background_color"]), style.text(weight="bold")],
                locations=loc.stub(rows=agg_with_subtotals.index[agg_with_subtotals["FR Fund Reservation Desc"] == "Subtotal"].tolist())
            )
            .tab_options(
                table_body_border_bottom_color=table_colors["border_color"],
                table_body_border_bottom_width="2px",
                table_border_right_color=table_colors["border_color"],
                table_border_right_width="2px",
                table_border_left_color=table_colors["border_color"],
                table_border_left_width="2px",
                table_border_top_color=table_colors["border_color"],
                table_border_top_width="2px",
                column_labels_border_top_color=table_colors["border_color"],
                column_labels_border_top_width="2px",
                heading_background_color=table_colors["heading_background_color"],
                row_group_background_color=table_colors["row_group_background_color"]
            )
            .tab_source_note("Source: Summa DataWarehouse")
            .tab_source_note("BO Report: C0_COMMITMENTS_SUMMA")
        )

        # Store the table and data
        insert_variable(
            report=report,
            module="BudgetModule",
            var=f"table_3c_PO_exceeding_FDI",
            value=agg_with_subtotals.to_dict(orient="records"),
            db_path=db_path,
            anchor=f"table_3c",
            gt_table=tbl,
        )

        logging.debug("Stored 3c table and data")
    else:
        logging.debug("No data to store for table 3c (empty DataFrame).")

    return agg_with_subtotals