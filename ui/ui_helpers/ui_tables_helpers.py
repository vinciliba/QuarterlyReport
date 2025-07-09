
import pandas  as pd
from great_tables import GT, style, loc, md

def create_external_audits_dataframe(current_year, last_date, form_data):
    """Create properly structured DataFrame for External Audits table without dummy rows."""
    
    external_audits_data = [
        {
            "Status": f"{current_year} ERCEA TARGETS\n(Audited Participations foreseen acc. to H2020 audit strategy)",
            "CAS": form_data.get('target_cas', "150"),
            "Joint with Court of auditors*": form_data.get('target_joint', "N/A"),
            "Subtotal for error rates and coverage": form_data.get('target_subtotal', "150"),
            "Court of auditors only": form_data.get('target_court', "N/A"),
            "Total": form_data.get('target_total', "150")
        },
        {
            "Status": "ERCEA TARGETS Cumulative",
            "CAS": form_data.get('cumulative_cas', "1020 (900) ***"),
            "Joint with Court of auditors*": form_data.get('cumulative_joint', "N/A"),
            "Subtotal for error rates and coverage": form_data.get('cumulative_subtotal', "1020 (900) ***"),
            "Court of auditors only": form_data.get('cumulative_court', "N/A"),
            "Total": form_data.get('cumulative_total', "1020 (900) ***")
        },
        {
            "Status": "Planned",
            "CAS": form_data.get('planned_cas', "150"),
            "Joint with Court of auditors*": form_data.get('planned_joint', "0"),
            "Subtotal for error rates and coverage": form_data.get('planned_subtotal', "150"),
            "Court of auditors only": form_data.get('planned_court', "0"),
            "Total": form_data.get('planned_total', "150")
        },
        {
            "Status": f"On-going [Launched in {current_year}]",
            "CAS": form_data.get(f'ongoing_{current_year}_cas', "70"),
            "Joint with Court of auditors*": form_data.get(f'ongoing_{current_year}_joint', "0"),
            "Subtotal for error rates and coverage": form_data.get(f'ongoing_{current_year}_subtotal', "70"),
            "Court of auditors only": form_data.get(f'ongoing_{current_year}_court', "5"),
            "Total": form_data.get(f'ongoing_{current_year}_total', "75")
        },
        {
            "Status": "On-going [Launched in previous years]",
            "CAS": form_data.get('ongoing_prev_cas', "73"),
            "Joint with Court of auditors*": form_data.get('ongoing_prev_joint', "0"),
            "Subtotal for error rates and coverage": form_data.get('ongoing_prev_subtotal', "73"),
            "Court of auditors only": form_data.get('ongoing_prev_court', "0"),
            "Total": form_data.get('ongoing_prev_total', "73")
        },
        {
            "Status": f"TOTAL On-going as of {last_date}",
            "CAS": form_data.get(f'total_ongoing_{current_year}_cas', "143"),
            "Joint with Court of auditors*": form_data.get(f'total_ongoing_{current_year}_joint', "0"),
            "Subtotal for error rates and coverage": form_data.get(f'total_ongoing_{current_year}_subtotal', "143"),
            "Court of auditors only": form_data.get(f'total_ongoing_{current_year}_court', "5"),
            "Total": form_data.get(f'total_ongoing_{current_year}_total', "148")
        },
        {
            "Status": "Closed in previous years",
            "CAS": form_data.get('closed_prev_cas', "823"),
            "Joint with Court of auditors*": form_data.get('closed_prev_joint', "13"),
            "Subtotal for error rates and coverage": form_data.get('closed_prev_subtotal', "836"),
            "Court of auditors only": form_data.get('closed_prev_court', "61"),
            "Total": form_data.get('closed_prev_total', "897")
        },
        {
            "Status": f"Closed in {current_year} from\naudited participations launched in {current_year}\n(Letter of Conclusion sent)",
            "CAS": form_data.get(f'audited_{current_year}_cas', "8"),
            "Joint with Court of auditors*": form_data.get(f'audited_{current_year}_joint', "3"),
            "Subtotal for error rates and coverage": form_data.get(f'audited_{current_year}_subtotal', "11"),
            "Court of auditors only": form_data.get(f'audited_{current_year}_court', "15"),
            "Total": form_data.get(f'audited_{current_year}_total', "26")
        },
        {
            "Status": f"Closed in {current_year} from\naudited participations launched in\nprevious years",
            "CAS": form_data.get(f'closed_{current_year}_prev_cas', "132"),
            "Joint with Court of auditors*": form_data.get(f'closed_{current_year}_prev_joint', "0"),
            "Subtotal for error rates and coverage": form_data.get(f'closed_{current_year}_prev_subtotal', "132"),
            "Court of auditors only": form_data.get(f'closed_{current_year}_prev_court', "1"),
            "Total": form_data.get(f'closed_{current_year}_prev_total', "133")
        },
        {
            "Status": f"TOTAL Closed in {current_year}",
            "CAS": form_data.get(f'total_closed_{current_year}_cas', "140"),
            "Joint with Court of auditors*": form_data.get(f'total_closed_{current_year}_joint', "3"),
            "Subtotal for error rates and coverage": form_data.get(f'total_closed_{current_year}_subtotal', "143"),
            "Court of auditors only": form_data.get(f'total_closed_{current_year}_court', "16"),
            "Total": form_data.get(f'total_closed_{current_year}_total', "159")
        },
        {
            "Status": "TOTAL cumulatively Closed",
            "CAS": form_data.get('total_cumulative_closed_cas', "963"),
            "Joint with Court of auditors*": form_data.get('total_cumulative_closed_joint', "16"),
            "Subtotal for error rates and coverage": form_data.get('total_cumulative_closed_subtotal', "979"),
            "Court of auditors only": form_data.get('total_cumulative_closed_court', "77"),
            "Total": form_data.get('total_cumulative_closed_total', "1056")
        },
        {
            "Status": "Total Audited (open & closed) **",
            "CAS": form_data.get('total_audited_cas', "1106"),
            "Joint with Court of auditors*": form_data.get('total_audited_joint', "16"),
            "Subtotal for error rates and coverage": form_data.get('total_audited_subtotal', "1122"),
            "Court of auditors only": form_data.get('total_audited_court', "82"),
            "Total": form_data.get('total_audited_total', "1204")
        }
    ]
    
    return pd.DataFrame(external_audits_data)

def create_error_rates_dataframe(form_data):
    """Create properly structured DataFrame for Error Rates table."""
    
    def safe_float_format(value, default):
        """Safely convert string to float and format with 3 decimal places."""
        try:
            # If it's already a float/int, use it directly
            if isinstance(value, (int, float)):
                return f"{value:.1f}%"
            # If it's a string, try to convert to float
            elif isinstance(value, str):
                # Remove any existing % sign if present
                clean_value = value.replace('%', '').strip()
                if clean_value:  # If not empty
                    return f"{float(clean_value):.1f}%"
                else:
                    return f"{default:.1f}%"
            else:
                return f"{default:.1f}%"
        except (ValueError, TypeError):
            # If conversion fails, use default
            return f"{default:.1f}%"
    
    error_rates_data = [
        {
            "Name": "CAS CRS 1 to 6 - Latest figures",
            "Error Rates (all cumulative)": safe_float_format(form_data.get('cas_error_rate'), 3.55),
             "Comments": form_data.get('cas_comments', 
             "Common Representative Error rate computed by the\n"
             "Common Audit Service (CAS) with top ups included.\n"
            "(source: SAR-Wiki)"),
             "To be reported": form_data.get('cas_to_be_reported', "Quarterly basis")
        },
        {
            "Name": "ERCEA Residual Based on CRS 1 to 5 - Latest figures",
            "Error Rates (all cumulative)": safe_float_format(form_data.get('ercea_residual_error_rate'), 0.92),
            "Comments": form_data.get('ercea_residual_comments', 
              "ERCEA Residual error rate based on the\n"
              "CRS 1, 2, 3 & 4 (source: SAR-Wiki)"),
            "To be reported": form_data.get('ercea_residual_to_be_reported', "Quarterly basis")
        },
        {
            "Name": "ERCEA overall detected average error rate - Latest figures",
            "Error Rates (all cumulative)": safe_float_format(form_data.get('ercea_overall_error_rate'), 1.30),
            "Comments": form_data.get('ercea_overall_comments', 
               "All ERCEA participations audited\n"
               "(source: SAR-Wiki)"),
            "To be reported": form_data.get('ercea_overall_to_be_reported', "Quarterly basis")
        }
    ]
    
    return pd.DataFrame(error_rates_data)

def apply_error_rates_styling(gt_table, report_params):
    """Apply simplified styling for Error Rates table that works with great_tables."""
    from great_tables import GT, style, loc, md
    
    table_colors = report_params.get('TABLE_COLORS', {})
    BLUE = table_colors.get("BLUE", "#004A99")
    LIGHT_BLUE = table_colors.get("LIGHT_BLUE", "#d6e6f4")
    DARK_BLUE = table_colors.get("DARK_BLUE", "#01244B")
    SUB_TOTAL_BACKGROUND = table_colors.get("subtotal_background_color", "#E6E6FA")

    try:
        gt_table = (
            gt_table
            .opt_table_font(font="Arial")
            .cols_width(
                cases={
                    "Comments": "200px",
                    "Name": "200px",
                    "Error Rates (all cumulative)": "200px",
                    "To be reported": "200px"
                }
             )
            # # Header styling - text color and formatting  
            .tab_style(
                style=[
                    style.fill(color=BLUE),
                    style.text(color="white", weight="bold", align="center", size='medium'),
                    style.css("min-width:50px; padding:5px; line-height:1.2")
                
                ],
                locations=loc.column_labels()
            )
            
            # Bold text for name column
            .tab_style(
                style=style.text(color=DARK_BLUE),
                locations=loc.body()
            )
            .tab_style(
                style=style.text(color=DARK_BLUE),
                locations=loc.stub()
            )
            .tab_style(
                style=style.text(weight="bold"),
                locations=loc.body(columns=["Name", "To be reported"])
            )

            .tab_style(
                style=style.text(stretch='extra-condensed', whitespace="pre-wrap" ),
                locations=loc.body(columns=["Comments"])
            )
            
            .tab_style(
                style=style.fill(color=LIGHT_BLUE),
                locations=loc.body(columns=["Name", 'To be reported'])
            )
            
            .tab_style(
                style=style.text(align="center"),
                locations=loc.body(columns=['To be reported'])
            )

            # Center-align error rate column
            .tab_style(
                style=style.text(align="center", weight="bold"),
                locations=loc.body(columns=["Error Rates (all cumulative)"])
            )
            # Add basic borders
            .tab_style(
                style=style.borders(sides="all", color="#cccccc", weight="1px"),
                locations=loc.body()
            )
            .tab_source_note(
                source_note=md("Source:Common Audit Service (CAS)"),
             )
        )
    except Exception as e:
        print(f"Styling error: {e}")
        # Return basic table if styling fails
        pass
    
    return gt_table

# Update the main styling function to use simplified versions
def apply_audit_table_styling(gt_table, table_type="external_audits"):
    """Apply improved styling to audit tables based on type."""
    
    if table_type == "external_audits":
        return apply_external_audits_styling_simple(gt_table)
    elif table_type == "error_rates":
        return apply_error_rates_styling_simple(gt_table)
    else:
        return gt_table
    

def apply_external_audits_styling(gt_table,  report_params):
    """Apply styling specifically for External Audits table (Table 11a)."""
    from great_tables import GT, style, loc, md

    table_colors = report_params.get('TABLE_COLORS', {})
    BLUE = table_colors.get("BLUE", "#004A99")
    LIGHT_BLUE = table_colors.get("LIGHT_BLUE", "#d6e6f4")
    DARK_BLUE = table_colors.get("DARK_BLUE", "#01244B")
    SUB_TOTAL_BACKGROUND = table_colors.get("subtotal_background_color", "#E6E6FA")
    
    gt_table = (
        gt_table
        .opt_table_font(font="Arial")
        .tab_options(
            table_font_size="12px",
            table_width="100%",
            table_background_color="#ffffff",
            table_font_color=DARK_BLUE
        )
        .tab_style(
                style=[
                    style.fill(color=BLUE),
                    style.text(color="white", weight="bold", align="center", size='small'),
                    style.css("min-width:50px; padding:5px; line-height:1.2")
                
                ],
                locations=loc.column_labels()
            )
        .tab_style(
            style=style.text(color="white", weight="bold", align="center"),
            locations=loc.column_labels()
        )
        # 2025 ERCEA TARGETS
        .tab_style(
            style=[style.fill(color=LIGHT_BLUE), style.text(weight="bold", v_align="middle")],
            locations=loc.body(rows=[3,4,9])
        )
        .tab_style(
            style=[style.fill(color=BLUE), style.text(color='white', weight="bold", v_align="middle")],
            locations=loc.body(rows=[1,5,10,11])
        )
        .tab_style(
            style=[style.fill(color='white'), style.text(weight="bold", v_align="middle")],
            locations=loc.body(rows=[0,2,6,7,8])
        )
        .tab_style(
            style=[style.text(v_align="middle", align="center")],
            locations=loc.body(columns=["CAS", "Joint with Court of auditors*", "Subtotal for error rates and coverage", "Court of auditors only", "Total"])
        )

        .tab_style(
            style=style.text(align="left", weight="bold"),
            locations=loc.body(columns=["Status"])
        )
        .tab_style(
            style=style.borders(sides="all", color="#cccccc", weight="1px"),
            locations=loc.body()
        )
        .tab_style(
            style=style.borders(sides="all", color="#ffffff", weight="2px"),
            locations=loc.column_labels()
        )
        .tab_options(heading_subtitle_font_size="medium", heading_title_font_size="large", table_font_size='medium',  column_labels_font_size='medium',row_group_font_size='medium', stub_font_size='medium')
        .tab_source_note(
                source_note=md("Source:Common Audit Service (CAS)"),
             )
        .cols_width(
            Status="25%",
            CAS="15%",
            **{"Joint with Court of auditors*": "15%"},
            **{"Subtotal for error rates and coverage": "20%"},
            **{"Court of auditors only": "15%"},
            Total="10%"
        )
    )
    
    return gt_table