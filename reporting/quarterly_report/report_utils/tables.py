import pandas as pd
from datetime import datetime, date
from great_tables import GT, md, google_font, style, loc, html
import sqlite3
from ingestion.db_utils import insert_variable, upsert_report_param 
import logging
from typing import Optional, Dict, Union

# Configure logging
logging.basicConfig(level=logging.DEBUG)
    
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

    # Unpack the ones you need
    BLUE = table_colors['BLUE']
    LIGHT_BLUE = table_colors['LIGHT_BLUE']
    DARK_BLUE = table_colors['DARK_BLUE']
    
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
                .opt_table_outline(style="solid", width='1px', color="#cccccc")
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
                .tab_options(
                    table_font_size="12px",
                    table_width="100%",
                    table_background_color="#ffffff",
                    table_font_color=DARK_BLUE
                )
                .tab_style(
                    style=style.borders(sides="all", color="#cccccc", weight="1px"),
                    locations=loc.body()
                )
                .tab_style(
                    style=style.borders(sides="all", color="#ffffff", weight="2px"),
                    locations=loc.column_labels()
                )
                # Header and stub styling
                .tab_style(
                    style=[
                        style.text(color=DARK_BLUE, weight="bold"),
                        style.fill(color=LIGHT_BLUE),
                        # style.css(f"border-bottom: 2px solid {DARK_BLUE}; border-right: 2px solid {DARK_BLUE}; border-top: 2px solid {DARK_BLUE}; border-left: 2px solid {DARK_BLUE};"),
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
   
                # Style total row
                .tab_style(
                    style=[style.fill(color="#E6E6FA"), style.text(color=DARK_BLUE, weight="bold")],
                    locations=loc.body(rows=agg.index[-1])
                )
                .tab_style(
                    style=[style.fill(color="#E6E6FA"), style.text(weight="bold")],
                    locations=loc.stub(rows=[-1])
                )
                .tab_options(heading_subtitle_font_size="medium", heading_title_font_size="large", table_font_size='medium',  column_labels_font_size='medium',row_group_font_size='medium', stub_font_size='medium')
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
    import time
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

    # Unpack the ones you need
    BLUE = table_colors['BLUE']
    LIGHT_BLUE = table_colors['LIGHT_BLUE']
    DARK_BLUE = table_colors['DARK_BLUE']

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
                        .tab_options(
                            table_font_size="12px",
                            table_width="100%",
                            table_background_color="#ffffff",
                            table_font_color=DARK_BLUE
                        )
                        .opt_table_outline(style="solid", width='1px', color="#cccccc")
                        .tab_style(
                            style=style.borders(sides="all", color="#cccccc", weight="1px"),
                            locations=loc.body()
                        )
                        .tab_style(
                            style=style.borders(sides="all", color="#ffffff", weight="2px"),
                            locations=loc.column_labels()
                        )
                        .tab_style(
                            style=[
                                style.text(color=DARK_BLUE, weight="bold", font='Arial'),
                                style.fill(color=LIGHT_BLUE),
                                # style.css(f"border-bottom: 2px solid {DARK_BLUE}; border-right: 2px solid {DARK_BLUE}; border-top: 2px solid {DARK_BLUE}; border-left: 2px solid {DARK_BLUE};"),
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
                            style=[style.fill(color="#E6E6FA"), style.text(color=DARK_BLUE, weight="bold")],
                            locations=loc.body(rows=agg.index[-1])
                        )
                        .tab_style(
                            style=[style.fill(color="#E6E6FA"), style.text(weight="bold")],
                            locations=loc.stub(rows=[-1])
                        )
                        .tab_options( heading_subtitle_font_size="medium", heading_title_font_size="large", table_font_size='medium',  column_labels_font_size='medium',row_group_font_size='medium', stub_font_size='medium')
                     
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
                    time.sleep(0.2)  # Small delay between tables
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

    # Unpack the ones you need
    BLUE = table_colors['BLUE']
    LIGHT_BLUE = table_colors['LIGHT_BLUE']
    DARK_BLUE = table_colors['DARK_BLUE']

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

            .tab_options(
                    table_font_size="12px",
                    table_width="100%",
                    table_background_color="#ffffff",
                    table_font_color=DARK_BLUE
                )
            .opt_table_outline(style="solid", width='1px', color="#cccccc")
            .tab_style(
                style=style.borders(sides="all", color="#cccccc", weight="1px"),
                locations=loc.body()
            )
            .tab_style(
                style=style.borders(sides="all", color="#ffffff", weight="2px"),
                locations=loc.column_labels()
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
                    # style.css(f"border-bottom: 2px solid {DARK_BLUE}; border-right: 2px solid {DARK_BLUE}; border-top: 2px solid {DARK_BLUE}; border-left: 2px solid {DARK_BLUE};"),
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
                style=[style.fill(color="#E6E6FA"), style.text(color=DARK_BLUE, weight="bold")],
                locations=loc.body(rows=agg_with_subtotals.index[agg_with_subtotals["FR Fund Reservation Desc"] == "Subtotal"].tolist())
            )
            .tab_style(
                style=[style.fill(color="#E6E6FA"), style.text(weight="bold")],
                locations=loc.stub(rows=agg_with_subtotals.index[agg_with_subtotals["FR Fund Reservation Desc"] == "Subtotal"].tolist())
            )
            .tab_options( heading_subtitle_font_size="medium", heading_title_font_size="large", table_font_size='medium',  column_labels_font_size='medium',row_group_font_size='medium', stub_font_size='medium')
      
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
    # Unpack the ones you need
    BLUE = table_colors['BLUE']
    LIGHT_BLUE = table_colors['LIGHT_BLUE']
    DARK_BLUE = table_colors['DARK_BLUE']

    # -------- GreatTables object -------------------------------------------
    tbl = (
        GT(
            agg_with_subtotals,
            rowname_col="FR Fund Reservation Desc",
            groupname_col="Fund Source"
        )
        .tab_stubhead(label="L1 Commitments")
        .tab_options(
                    table_font_size="12px",
                    table_width="100%",
                    table_background_color="#ffffff",
                    table_font_color=DARK_BLUE
                )
        .opt_table_outline(style="solid", width='1px', color="#cccccc")
        .tab_style(
            style=style.borders(sides="all", color="#cccccc", weight="1px"),
            locations=loc.body()
        )
        .tab_style(
            style=style.borders(sides="all", color="#ffffff", weight="2px"),
            locations=loc.column_labels()
        )
        .tab_style(
            style=[
                style.text(color=DARK_BLUE, weight="bold", font='Arial'),
                style.fill(color=LIGHT_BLUE),
                # style.css(f"border-bottom: 2px solid {DARK_BLUE}; border-right: 2px solid {DARK_BLUE}; border-top: 2px solid {DARK_BLUE}; border-left: 2px solid {DARK_BLUE};"),
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
            style=[style.fill(color="#E6E6FA"), style.text(color=DARK_BLUE, weight="bold")],
            locations=loc.body(rows=agg_with_subtotals.index[agg_with_subtotals["FR Fund Reservation Desc"] == "Subtotal"].tolist())
        )
        .tab_style(
                style=[style.fill(color="#E6E6FA"), style.text(weight="bold")],
                locations=loc.stub(rows=agg_with_subtotals.index[agg_with_subtotals["FR Fund Reservation Desc"] == "Subtotal"].tolist())
            )
        .tab_options( heading_subtitle_font_size="medium", heading_title_font_size="large", table_font_size='medium',  column_labels_font_size='medium',row_group_font_size='medium', stub_font_size='medium')
   
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



def build_budget_summary_table(conn, db_path, report, cutoff, table_colors):
    import pandas as pd
    from datetime import datetime
    from ingestion.db_utils import load_report_params, fetch_vars_for_report, fetch_latest_table_data, insert_variable
    from great_tables import GT, style, loc
    import logging

    try:
        previous_year = cutoff.year - 1
        last_month_previous_year = datetime(previous_year, 12, 1)
        formatted_date = last_month_previous_year.strftime("%b %Y")

        report_params = load_report_params(report_name=report, db_path=db_path)
        report_quarter = report_params.get("overviewDate")
        target_history = report_params.get("Budget_Impl")

        BLUE = table_colors.get("BLUE", "#004A99")
        LIGHT_BLUE = table_colors.get("LIGHT_BLUE", "#d6e6f4")
        DARK_BLUE = table_colors.get("DARK_BLUE", "#01244B")

        outline_b = '2px'

        # Commitment Summary
        report_variables = fetch_vars_for_report(report, db_path)
        df_comm = pd.DataFrame(report_variables.get("table_1a"))
        l1_comm = pd.DataFrame(report_variables.get("table_1c"))

        tot = df_comm.iloc[-1]
        ratio_pct = f"{tot['ratio_consumed_of_L1_and_L2_against_Commitment_Appropriations']*100:.2f}%"
        avail_bln = f"{tot['Available_Commitment_Appropriations']/1e9:.2f}"

        overview = f"{ratio_pct} (out of € {avail_bln} bln)"
        target_history_comm = target_history['row2_commit']

        summary_comm = pd.DataFrame({
            'Indicator': "Budget implementation",
            "Indicator_Description": [
                "Consumption of commitment appropriations out of the total available credits (C1/E0-HEU)"
            ],
            f'{report_quarter}': [overview],
            f"{formatted_date}": [target_history_comm],
            'Target': '100%'
        })

        # L1 Summary
        df_l1 = l1_comm.loc[l1_comm["Fund Source"] == "VOBU/EFTA/IAR2/2"]
        total_l1 = df_l1['L1_Commitment_1'].iloc[-1]
        total_l2 = -1 * df_l1['L2_Commitment_2'].iloc[-1]
        pct_L2_L1 = (total_l2 / total_l1) * 100

        output_str = f"{pct_L2_L1:.0f} % (out of € {total_l1/1e9:.1f} bln)"
        target_history_L1 = target_history['row3_L2/L1']

        summary_L1 = pd.DataFrame({
            'Indicator': "Budget implementation",
            "Indicator_Description": [
                "Consumption of Carried Forward Commitment Voted Credits (% L2 on L1) of previous year calls"
            ],
            f'{report_quarter}': [output_str],
            f"{formatted_date}": [target_history_L1],
            'Target': '100%'
        })

        # Payment Summary
        df_p_h2020 = pd.DataFrame(report_variables.get("table_2a_H2020"))
        df_p_he = pd.DataFrame(report_variables.get("table_2a_HE"))

        total_p_cons = df_p_h2020.iloc[-1]['Paid_Amount'] + df_p_he.iloc[-1]['Paid_Amount']
        total_p_appr = df_p_h2020.iloc[-1]['Available_Payment_Appropriations'] + df_p_he.iloc[-1]['Available_Payment_Appropriations']
        ratio_pct = f"{total_p_cons/total_p_appr*100:.2f}%"

        output_str = f"{ratio_pct} (out of € {total_p_appr/1e9:.1f} bln)"
        payments_target = target_history['row4_paym']

        summary_payments = pd.DataFrame({
            'Indicator': "Budget implementation",
            "Indicator_Description": [
                "Consumption of payment appropriations out of the total available credits (operational)(VOBU/EFTA - H2020 + HEU)"
            ],
            f'{report_quarter}': [output_str],
            f"{formatted_date}": [payments_target],
            'Target': '100%'
        })

        # EARN Summary
        df_comm_earn = fetch_latest_table_data(conn, "c0_commitments_summa", cutoff).loc[
            lambda df: df["Fund Source"] == "EARN/N"]
        total_earn_appr = df_comm_earn['FR Accepted Amount'].sum()
        total_earn_committed = -1 * df_comm_earn['FR Consumption by PO Amount'].sum()
        ratio_comm_pct = f"{total_earn_committed/total_earn_appr*100:.2f}%"

        output_str = f"{ratio_comm_pct} (out of € {total_earn_appr/1e9:.1f} bln)"
        earn_consum_target = target_history['row5_EARN']

        summary_earn = pd.DataFrame({
            'Indicator': "Budget implementation",
            "Indicator_Description": [
                "Commitment appropriations concerning a third country contribution (EARN - H2020 + HEU)"
            ],
            f'{report_quarter}': [output_str],
            f"{formatted_date}": [earn_consum_target],
            'Target': '-'
        })

        #SUMMARY VACANCY RATE ####
        vacancy_Rate = report_params.get("Vacancy_Rate")

        try:
            prev_value = vacancy_Rate['previous_year']
            cur_val = vacancy_Rate['current_year']
        except KeyError as e:
            logging.error(f"Missing expected key in Vacancy_Rate: {e}")
            raise


        vacancy_Rate = report_params.get("Vacancy_Rate", {})
        prev_value = vacancy_Rate.get("previous_year", "N/A")
        cur_val = vacancy_Rate.get("current_year", "N/A")

        summary_vacancy = pd.DataFrame(
            {
                'Indicator': ["Resources"],
                'Indicator_Description': ["Vacancy rate for permanent posts"],
                f"{report_quarter}": [f"{cur_val}"],
                f"{formatted_date}": [f"{prev_value}"],
                'Target': ["2%"]
            }
        )

        # Combine all
        combined_summary = pd.concat([
            summary_vacancy, summary_comm, summary_L1, summary_payments, summary_earn
        ], ignore_index=True)

        tbl_summary = (
            GT(
                combined_summary,
                groupname_col="Indicator",
                rowname_col="Indicator_Description"
            )
            .tab_stubhead("Indicator")
            .opt_table_outline(style="solid", width='1px', color="#cccccc")
            .opt_table_font(font="Arial")
            .tab_options(
                    table_font_size="12px",
                    table_width="100%",
                    table_background_color="#ffffff",
                    table_font_color=DARK_BLUE
                )
            .tab_style(
                style=style.borders(sides="all", color="#cccccc", weight="1px"),
                locations=loc.body()
            )
            .tab_style(
                style=style.borders(sides="all", color="#ffffff", weight="2px"),
                locations=loc.column_labels()
            )
            .tab_style(
                style=[
                    style.fill(color=BLUE),
                    style.text(weight="bold", align='center', size='medium'),
                    style.css("text-align: center; vertical-align: middle; max-width:200px; line-height:1.2; font-size: medium;")
                ],
                locations=loc.column_labels()
            )
             .tab_style(
                style=[
                    style.fill(color=BLUE),
                    style.text(weight="bold", align='center', size='medium'),
                    style.css("text-align: center; vertical-align: middle; max-width:200px; line-height:1.2; font-size: medium;")
                ],
                locations=loc.stubhead()
            )
            .tab_style(style.text(size='small'), loc.stub())
            .tab_style(style.text(size='small'), loc.body())
            .tab_options(
                container_width="100%",
                column_labels_background_color=BLUE,
                row_group_background_color=LIGHT_BLUE,
                heading_title_font_size="medium", 
                table_font_size='medium',  
                column_labels_font_size='medium',
                row_group_font_size='medium', 
                stub_font_size='medium'
            )
                            
        )

        insert_variable(
            report=report,
            module="BudgetModule",
            var="table_summary_budget",
            value=combined_summary.to_dict(orient="records"),
            db_path=db_path,
            anchor="overview_budget_table",
            gt_table=tbl_summary,
        )

        return tbl_summary

    except Exception as e:
        logging.error(f"Error in build_budget_summary_table: {str(e)}")
        raise
