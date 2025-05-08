import pandas as pd
from datetime import date, timedelta
from great_tables import GT, md, google_font, style, loc, html
import sqlite3
from ingestion.db_utils import insert_variable
import logging

# Configure logging
logging.basicConfig(level=logging.DEBUG)
    
BLUE        = "#004A99"
LIGHT_BLUE = "#064E9F"
GRID_CLR    = "#004A99"
DARK_BLUE   = "#01244B"
DARK_GREY =   '#242425'

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
    agg = agg.loc[agg["Programme"] == "HE"]

    tbl = (
        GT(agg)
        # .tab_header("Commitment Appropriations", subtitle="General Overview")
        # .tab_stub(rowname_col="Programme")
        .tab_stubhead("Programme")
        # ── formats ────────────────────────────────────────────────────────────
    
        .fmt_number(columns=[
            "Available_Commitment_Appropriations",
            "L1_Commitment",
            "RAL_on_Appropriation"
        ], accounting=True, decimals=2)
        .fmt_percent(
            columns="ratio_consumed_of_L1_and_L2_against_Commitment_Appropriations",
            decimals=2
        )
   
        # ── Set custom column labels with <br> for line breaks ─────────────────
        .cols_label(
        Available_Commitment_Appropriations = html("Available Commitment Appropriations<br>(1)"),
        L1_Commitment = html("L1 Commitment<br>(2)"),
        RAL_on_Appropriation = html("RAL on Appropriation<br>(3)=(1)-(2)"),
        ratio_consumed_of_L1_and_L2_against_Commitment_Appropriations  = html("% consumed of L1 and L2<br>against Commitment Appropriations <br> (4) = (2)/(1)")
    )
        # ── Arial everywhere ──────────────────────────────────────────────────
        .opt_table_font(font="Arial")
        # ── HEADER + STUB COLOUR ──────────────────────────────────────────────
        .tab_style(
                    style = [
                            style.fill(color= BLUE),        
                            style.text(color="white", weight="bold", align='center'),
                            style.css("word-wrap: break-word; white-space: normal; max-width: 200px;"),
                            style.css("line-height: 1.2; margin-bottom: 5px;")
                    ],
                    locations = loc.column_labels()
                  ) 
        .tab_style(
                    style = [
                        style.fill(color= BLUE),
                        style.text(color="white", weight="bold"),
                    ],
                    locations = loc.stubhead()
                   )
        
        # ── GRID LINES ────────────────────────────────────────────────────────
        .tab_style(
            style = style.borders(sides=["all"], color=DARK_BLUE , weight='2px'),
            locations = loc.body()
        )
        .tab_style(
            style = style.borders( color=DARK_BLUE, weight='2px'),
            locations = loc.column_labels()
        )

        .tab_style(
            style = style.borders( color=DARK_BLUE, weight='2px'),
            locations = loc.stubhead()
        )

        # ── ROW STRIPING ───────────────────────────────────────────────────────
        .tab_options(table_body_border_bottom_color = DARK_BLUE , 
                       table_body_border_bottom_width = '2px')

        .tab_options(table_body_border_top_color=DARK_BLUE,
                         table_body_border_top_width="2px")
        # ── SOURCE NOTE ────────────────────────────────────────────────────────
        .tab_source_note("Source: Summa DataWarehouse")
        .tab_source_note("BO Report: C0_Budgetary_Execution_Details")
    )

    # Insert data and GT table image
    insert_variable(
        report=report,
        module="BudgetModule",
        var="table_1a_commitment_summary",
        value=agg.to_dict(orient="records"),
        db_path=db_path,
        anchor="table_1a",
        gt_table=tbl
    )
    return agg


# ──────────────────────────────────────────────────────────────────────────────

def build_payment_summary_tables(
        df: pd.DataFrame,
        current_year: int,
        report: str,
        db_path: str
) -> dict[str, pd.DataFrame]:
    """
    Build & store one payment‑summary table per programme (HE, H2020).

    Returns
    -------
    dict
        keys  = programme code ("HE", "H2020")
        value = aggregated DataFrame for that programme
    """
    # ---------- common pre‑filtering ---------------------------------------------------
    # Create a new DataFrame with a copy to avoid SettingWithCopyWarning
    df = df[(df["Budget Period"] == current_year) & (df["Fund Source"].isin(["VOBU", "EFTA"]))].copy()

    # Map Functional Area → Programme
    df.loc[:, "Programme"] = df["Functional Area Desc"].replace({
        "HORIZONEU_21_27": "HE",
        "H2020_14_20": "H2020"
    })

    # Budget‑type mapping helper
    def map_budget_type(val):
        if pd.isna(val):
            return None
        v = str(val).upper()
        if "EMPTY" in v: return "Main Call"
        if "EXPERTS" in v: return "Experts"
        return val

    df.loc[:, "Budget_Address_Type"] = df["Budget Address"].apply(map_budget_type)

    # Set Fund Source for all rows
    df.loc[:, "Fund Source"] = 'VOBU/EFTA'

    # ---------- iterate over the two programmes ---------------------------------------
    results: dict[str, pd.DataFrame] = {}
    for programme in ("HE", "H2020"):
        df_p = df[df["Programme"] == programme].copy()

        if df_p.empty:
            logging.warning("No rows for programme %s", programme)
            continue

        # -------- aggregation ------------------------------------------------------
        agg = (df_p
               .groupby(["Fund Source", "Budget_Address_Type"],
                        as_index=False)[
                    ["Payment Appropriation", "Paid Amount",
                     "Payment Available"]]
               .sum())

        agg["%"] = agg["Paid Amount"] / agg["Payment Appropriation"]

        agg = agg.rename(columns={
            "Payment Appropriation": "Available_Payment_Appropriations",
            "Paid Amount": "Paid_Amount",
            "Payment Available": "Remaining_Payment_Appropriation",
            "%": "ratio_consumed_Payment_Appropriations",
            "Budget_Address_Type": "Budget Address Type"
        })

        # -------- Add total row ---------------------------------------------------
        total_row = pd.DataFrame({
            # "Programme": ["Total"],
            "Fund Source": ['VOBU/EFTA'],
            "Budget Address Type": ['Total'],
            "Available_Payment_Appropriations": [agg["Available_Payment_Appropriations"].sum()],
            "Paid_Amount": [agg["Paid_Amount"].sum()],
            "Remaining_Payment_Appropriation": [agg["Remaining_Payment_Appropriation"].sum()],
            "ratio_consumed_Payment_Appropriations": [agg["Paid_Amount"].sum() / agg["Available_Payment_Appropriations"].sum()]
        })
        agg = pd.concat([agg, total_row], ignore_index=True)

        # -------- GreatTables object -------------------------------------------
        tbl = (
            GT(agg,
              rowname_col="Budget Address Type",
              groupname_col="Fund Source"
              )
            .tab_stubhead(label="Budget Address Type")
            .tab_style(
                style=[
                    style.text(color="white", weight="bold"),
                    style.fill(color=LIGHT_BLUE),
                    style.css(f"border-bottom: 2px solid {DARK_BLUE}; border-right: 2px solid {DARK_BLUE}; border-top: 2px solid {DARK_BLUE}; border-left: 2px solid {DARK_BLUE};"),
                    style.css("max-width:200px; line-height:1.2"),
                ],
                locations=loc.row_groups()
            )
            .tab_header(
                title=f"{programme}",
            )
            .fmt_number(columns=[
                "Available_Payment_Appropriations",
                "Paid_Amount",
                "Remaining_Payment_Appropriation"
            ], accounting=True, decimals=2)
            .fmt_percent(
                columns="ratio_consumed_Payment_Appropriations",
                decimals=2)
            .cols_label(
                Available_Payment_Appropriations=html("Payment Appropriations<br>(1)"),
                Paid_Amount=html("Payment Credits consumed<br>(Acceptance Date)<br>(2)"),
                Remaining_Payment_Appropriation=html("Remaining Payment Appropriations<br>(3)=(1)-(2)"),
                ratio_consumed_Payment_Appropriations=html("% Payment Consumed<br>(4) = (2)/(1)")
            )
            .opt_table_font(font="Arial")

              .tab_style(
                style = [style.text(weight="bold", color=DARK_BLUE)],
                locations= loc.header(),
                )

            .tab_style(
            style=[style.fill(color=BLUE),
                   style.text(color="white", weight="bold", align="center"),
                   style.css("text-align: center;vertical-align: middle; max-width:200px; line-height:1.2")],
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
                locations=loc.body(rows=agg.index[-1]))  # Apply to the last row (total)

             .tab_style(
                style = [style.fill( color="#E6E6FA"), style.text(weight="bold")],
                locations= loc.stub(rows=[-1]),
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
            .tab_source_note("BO Report: C0_Budgetary_Execution_Details")
        )

        # -------- store --------------------------------------------------------
        insert_variable(
            report=report,
            module="BudgetModule",
            var=f"table_2a_{programme}_data",
            value=agg.to_dict(orient="records"),
            db_path=db_path,
            anchor=f"table_2a_{programme}",
            gt_table=tbl,
        )

        logging.debug("Stored 2a table for programme %s (%d rows)",
                      programme, len(agg))

        results[programme] = agg

    #---- END loop ------
    return results


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
                style.text(color="white", weight="bold"),
                style.fill(color=LIGHT_BLUE),
                style.css(f"border-bottom: 2px solid {DARK_BLUE}; border-right: 2px solid {DARK_BLUE};border-top: 2px solid {DARK_BLUE}; border-left: 2px solid {DARK_BLUE}"),
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
            L1_Commitment_1=html("L1 Commitment<br>(1)"),
            L2_Commitment_2=html("L2 Commitments<br>(2)"),
            RAC_on_L1_Commitment_3=html("RAC on L1 Commitment <br>(3) = (1) - (2)"),
            ratio_L2_on_L1_Commitment_4=html("% L1 consumed by L2 (indirect)<br> (4) = (2) / (1)")
        )
        .opt_table_font(font="Arial")
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
                style = [style.fill( color="#E6E6FA"), style.text(weight="bold")],
                locations= loc.stub(rows=[-1]),
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
def build_docx_context(report_name: str, db_path: str) -> dict:
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
