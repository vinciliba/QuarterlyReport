from __future__ import annotations
from reporting.quarterly_report.utils import RenderContext, BaseModule

import logging
import datetime
import json
import sqlite3
from pathlib import Path
from typing import Dict, List, Any, Optional
from dataclasses import dataclass
import pandas as pd
import re # Import re for parsing

# Import the enhanced generator components
from reporting.quarterly_report.report_utils.enhanced_report_generator import (
    EnhancedReportGenerator,
    ReportTemplateLibrary,
    TemplateSectionMatrix
)
from ingestion.db_utils import (
    fetch_vars_for_report,
    load_report_params,
    insert_variable
)


from pprint import pprint

logger=logging.getLogger(__name__)

# ================================================================
# 🎯 MODULE CONFIGURATION
# ================================================================
class CommentsConfig:
    """Configuration for AI comment generation"""

    # 🤖 AI Model Configuration
    AVAILABLE_MODELS = {
        'deepseek-r1:14b': {
            'name': 'DeepSeek R1 14B',
            'description': 'Advanced reasoning model for complex financial analysis',
            'temperature': 0.5,
            'max_tokens_multiplier': 2.0,
            'recommended_for': ['financial', 'analytical', 'executive', 'complex'],
        },
        'qwen2.5:14b': {
            'name': 'Qwen 2.5 14B',
            'description': 'High-quality model for detailed analysis',
            'temperature': 0.4,
            'max_tokens_multiplier': 1.7,
            'recommended_for': ['detailed', 'technical', 'compliance'],
        },
        'gemma3:12b': {
            'name': 'Gemma 3 12B',
            'description': 'Balanced model for comprehensive reports',
            'temperature': 0.4,
            'max_tokens_multiplier': 1.8,
            'recommended_for': ['comprehensive', 'narrative', 'strategic'],
        },
        'codellama:13b': {
            'name': 'Code Llama 13B',
            'description': 'Efficient model for general purpose tasks',
            'temperature': 0.45,
            'max_tokens_multiplier': 1.6,
            'recommended_for': ['general', 'balanced', 'efficient'],
        }
    }

    # Generation Control Settings
    GENERATION_CONTROL = {
        'enable_diagnostics': True,
        'enable_human_validation': False,
        'save_partial_results': True,
        'continue_on_failure': True,
        'diagnostic_sections': ['intro_summary', 'budget_overview'],
        'minimum_acceptable_score': 0.4,
        }

    # 📊 Generation Settings
    DEFAULT_MODEL = "qwen2.5:14b" # More advanced default model
    DEFAULT_TEMPERATURE = 0.4
    API_ENDPOINT = "http://localhost:11434/api/generate"
    API_TIMEOUT = 300

    # Quality Enhancement Settings
    QUALITY_SETTINGS = {
        'min_response_length': 50,
        'max_retries': 2,
        'retry_temperature_increment': 0.1,
     }

    # Section-specific temperature overrides for balancing creativity and factuality
    SECTION_TEMPERATURE_OVERRIDES = {
        'intro_summary': 0.4, 'budget_overview': 0.2, 'ttp_performance': 0.2,
        'heu_payment_overview': 0.2, 'h2020_payment_overview': 0.2,
    }

    # 🎯 Section Configuration - ✅ CORRECTED to match TemplateSectionMatrix keys
    SINGLE_SECTIONS = [
        'intro_summary'
        # 'budget_overview', 
        # 'granting_process_overview', 
        # 'commitment_budgetary_impact', 
        # 'fdi_status_analysis', 
        # 'heu_payment_overview', 
        # 'h2020_payment_overview', 
        # 'ttp_performance'
    ]

    # 🔄 Loop Configuration
    LOOP_PROGRAMS=['HEU', 'H2020']
    LOOP_CALL_TYPES=['STG', 'ADG', 'POC', 'COG', 'SYG']

    # 📋 Detailed Call Type Definitions for granular report generation
    DETAILED_CALL_TYPES=[
        {'code': 'STG', 'description': 'Starting Grant - Early career researchers', 'abbreviation': 'STG'},
        {'code': 'ADG', 'description': 'Advanced Grant - Established researchers', 'abbreviation': 'ADG'},
        {'code': 'COG', 'description': 'Consolidator Grant - Mid-career researchers', 'abbreviation': 'COG'},
        {'code': 'POC', 'description': 'Proof of Concept - Commercialization support', 'abbreviation': 'POC'},
        {'code': 'SYG', 'description': 'Synergy Grant - Collaborative research teams', 'abbreviation': 'SYG'},
        {'code': 'CSA', 'description': 'Coordination and Support Action', 'abbreviation': 'CSA'},
        {'code': 'RIA', 'description': 'Research and Innovation Action', 'abbreviation': 'RIA'},
        {'code': 'IA', 'description': 'Innovation Action', 'abbreviation': 'IA'},
    ]

    # Comprehensive acronym dictionary for providing context to the AI
    ACRONYMS_DICTIONARY={
        # Call Types
        'STG': {'full_name': 'Starting Grant', 'category': 'call_type', 'description': 'ERC Starting Grants for early-career researchers'},
        'ADG': {'full_name': 'Advanced Grant', 'category': 'call_type', 'description': 'ERC Advanced Grants for established researchers'},
        'POC': {'full_name': 'Proof of Concept', 'category': 'call_type', 'description': 'ERC Proof of Concept grants for commercialization'},
        'COG': {'full_name': 'Consolidator Grant', 'category': 'call_type', 'description': 'ERC Consolidator Grants for researchers 7-12 years post-PhD'},
        'SYG': {'full_name': 'Synergy Grant', 'category': 'call_type', 'description': 'ERC Synergy Grants for collaborative research teams'},
        'CSA': {'full_name': 'Coordination and Support Action', 'category': 'call_type', 'description': 'Supporting and coordination measures'},
        'RIA': {'full_name': 'Research and Innovation Action', 'category': 'call_type', 'description': 'Primary research and innovation funding instrument'},
        'IA': {'full_name': 'Innovation Action', 'category': 'call_type', 'description': 'Innovation activities closer to market'},
        'EXPERTS': {'full_name': 'Expert Services', 'category': 'call_type', 'description': 'Expert evaluation and support services'},
        # Programs
        'H2020': {'full_name': 'Horizon 2020', 'category': 'program', 'description': 'EU Research and Innovation Framework Programme 2014-2020'},
        'HEU': {'full_name': 'Horizon Europe', 'category': 'program', 'description': 'EU Research and Innovation Framework Programme 2021-2027'},
        # Time Metrics
        'TTP': {'full_name': 'Time to Pay', 'category': 'time_metric', 'description': 'Processing time from payment request to actual payment'},
        'TTG': {'full_name': 'Time to Grant', 'category': 'time_metric', 'description': 'Processing time from proposal submission to grant decision'},
        'TTA': {'full_name': 'Time to Amend', 'category': 'time_metric', 'description': 'Processing time for grant agreement amendments'},
        # Payment Types
        'FP': {'full_name': 'Final Payment', 'category': 'payment_type', 'description': 'Final payment at project completion'},
        'IP': {'full_name': 'Interim Payment', 'category': 'payment_type', 'description': 'Periodic payments during project implementation'},
        'PF': {'full_name': 'Pre-financing', 'category': 'payment_type', 'description': 'Initial payment made upon grant agreement signature'},
        # Organizations
        'ERCEA': {'full_name': 'European Research Council Executive Agency', 'category': 'organization', 'description': 'Executive agency implementing ERC grants'},
        # Audit and Financial Terms
        'AURI': {'full_name': 'Audit and Recovery Implementation', 'category': 'audit', 'description': 'EU audit and financial recovery processes'},
        'FDI': {'full_name': 'Final Date for Implementation', 'category': 'financial', 'description': 'Deadline for legally committing funds'},
    }
# ================================================================
# 🤖 COMMENTS MODULE
# ================================================================

class CommentsModule(BaseModule):

    name="Comments"
    description="AI GENERATED COMMENTS"


    def _extract_and_contextualize_intro_kpis(self, report_vars: Dict[str, Any], quarter_period: str) -> Dict[str, Any]:
        """
        ✅ MOVED & CORRECTED: This logic now lives in the CommentsModule where it has access to raw report_vars.
        It extracts specific KPIs and returns a clean dictionary for the template.
        """
        kpis = {}

        def _parse_safe(data):
            if isinstance(data, str):
                try:
                    data = json.loads(data)
                except (json.JSONDecodeError, TypeError):
                    return None
            return data

        def _safe_get_avg(data_list, key, default=0):
            if not isinstance(data_list, list): return default
            vals = [float(item.get(key, 0) or 0) for item in data_list if isinstance(item, dict) and item.get(key)]
            return round(sum(vals) / len(vals), 1) if vals else default
            
        def _safe_get_sum(data_list, key, default=0):
            if not isinstance(data_list, list): return default
            return sum(float(item.get(key, 0) or 0) for item in data_list if isinstance(item, dict) and item.get(key))
        
        def _safe_get_count(data_list, default=0):
            return len(data_list) if isinstance(data_list, list) else default
        
        def _transpose_and_tag_ttp_data(data, current_year: str):
            """Transpose columnar TTP data, assign Programme tags, and map column names."""
            if not isinstance(data, dict) or not all(isinstance(v, dict) for v in data.values()):
                return data

            row_count = len(next(iter(data.values())))
            rows = []

            valid_keys = {
                'Type of Payments',
                f'Average Net Time to Pay (in days) {current_year}-YTD',
                f'Average Gross Time to Pay (in days) {current_year}-YTD',
                f'Target Paid on Time - Contractually {current_year}-YTD',
            }

            # Step 1: Convert columnar JSON to row-based records
            for i in range(row_count):
                # row = {col: data[col].get(str(i)) or data[col].get(i) for col in data}
                row = {
                    col: data[col].get(str(i)) or data[col].get(i)
                    for col in data
                    if col in valid_keys
                }

                row['index'] = i
                rows.append(row)

            # Step 2: Detect marker positions
            marker_positions = {}
            for row in rows:
                label = str(row.get('Type of Payments', '')).strip().upper()
                if label == 'H2020':
                    marker_positions['H2020'] = row['index']
                elif label == 'HEU':
                    marker_positions['HEU'] = row['index']
                elif label == 'TOTAL':
                    marker_positions['TOTAL'] = row['index']

            h2020_marker = marker_positions.get('H2020', -1)
            heu_marker = marker_positions.get('HEU', -1)
            total_marker = marker_positions.get('TOTAL', row_count)  # default to end

            result = []
            for row in rows:
                idx = row['index']
                ptype = row.get('Type of Payments')

                # Ignore marker rows
                if ptype in ['H2020', 'HEU', 'TOTAL']:
                    continue

                if idx < h2020_marker:
                    programme = 'H2020'
                elif h2020_marker < idx < heu_marker:
                    programme = 'HEU'

                if programme:
                    row['Programme'] = programme
                    row['Payment_Type'] = ptype
                    # Apply rename mapping
                    standardized_row = _standardize_ttp_row(row, current_year)
                    result.append(standardized_row)

            return result

        def _standardize_ttp_row(row, current_year: str):
            """Renames only the current-year columns and discards old columns."""
            rename_map = {
                f'Average Net Time to Pay (in days) {current_year}-YTD': 'yearly_avg_ttp',
                f'Average Gross Time to Pay (in days) {current_year}-YTD': 'yearly_avg_ttp_gross',
                f'Target Paid on Time - Contractually {current_year}-YTD': 'on_time_target',
                'Type of Payments': 'Payment_Type',
                'Programme': 'Programme',
            }

            # Apply renaming, but ignore any unmatched (like Dec 2024)
            return {v: row[k] for k, v in rename_map.items() if k in row}
        
        def extract_year_from_quarter_period(quarter_period: str) -> str:
            """Safely extract the year from quarter_period string or fallback to current year."""
            try:
                if quarter_period and "-" in quarter_period:
                    return quarter_period.split("-")[-1].strip()
            except Exception:
                pass
            # Fallback: current calendar year
            return str(datetime.datetime.now().year)

        # # --- Performance Timings ---
        # ttp_data = _parse_safe(report_vars.get('TTP_performance_summary_table'))
        # Extract year from quarter_period like "Quarter 1 - 2025"
        current_year = extract_year_from_quarter_period(quarter_period)



        ttp_data = _transpose_and_tag_ttp_data(
            _parse_safe(report_vars.get('TTP_performance_summary_table')),
            current_year
        )
        

        kpis['h2020_ttp_interim'] = _safe_get_avg([r for r in (ttp_data or []) if isinstance(r, dict) and r.get('Programme') == 'H2020' and r.get('Payment_Type') == 'Interim Payments'], 'yearly_avg_ttp', 'none')
        # kpis['h2020_ttp_interim_on_time'] = _safe_get_avg([r for r in (ttp_data or []) if isinstance(r, dict) and r.get('Programme') == 'H2020' and r.get('Payment_Type') == 'Interim Payments'], 'on_time_target', 'none')

        kpis['heu_ttp_interim'] = _safe_get_avg([r for r in (ttp_data or []) if isinstance(r, dict) and r.get('Programme') == 'HEU' and r.get('Payment_Type') == 'Interim Payments'], 'yearly_avg_ttp',  'none')
        # kpis['heu_ttp_interim_on_time'] = _safe_get_avg([r for r in (ttp_data or []) if isinstance(r, dict) and r.get('Programme') == 'HEU' and r.get('Payment_Type') == 'Interim Payments'], 'on_time_target', 'none')

        kpis['h2020_ttp_final'] = _safe_get_avg([r for r in (ttp_data or []) if isinstance(r, dict) and r.get('Programme') == 'H2020' and r.get('Payment_Type') == 'Final Payments'], 'yearly_avg_ttp',  'none')
        # kpis['h2020_ttp_final_on_time'] = _safe_get_avg([r for r in (ttp_data or []) if isinstance(r, dict) and r.get('Programme') == 'H2020' and r.get('Payment_Type') == 'Final Payments'], 'on_time_target', 'none')

        kpis['heu_ttp_final'] = _safe_get_avg([r for r in (ttp_data or []) if isinstance(r, dict) and r.get('Programme') == 'HEU' and r.get('Payment_Type') == 'Final Payments'], 'yearly_avg_ttp',  'none')
        # kpis['heu_ttp_final_on_time'] = _safe_get_avg([r for r in (ttp_data or []) if isinstance(r, dict) and r.get('Programme') == 'HEU' and r.get('Payment_Type') == 'Final Payments'], 'on_time_target', 'none')

        kpis['h2020_tta_avg'] = _safe_get_avg(_parse_safe(report_vars.get('H2020_tta')), 'TTA', 6.4)
        kpis['heu_tta_avg'] = _safe_get_avg(_parse_safe(report_vars.get('HORIZON_tta')), 'TTA', 6.5)

        # --- Amendment Rates & Counts ---
        
        amend_h2020 = _parse_safe(report_vars.get('H2020_overview'))
        print("\n📦 DEBUG: Final AMD H2020 Data (after tagging and mapping):")
        pprint(amend_h2020)
        kpis['h2020_amendment_count'] = _safe_get_count(amend_h2020, 891)
        kpis['h2020_tta_delays'] = _safe_get_sum(amend_h2020, 'Delayed', 4)
        total_signed = _safe_get_sum(amend_h2020, 'Signed', 1)
        kpis['h2020_tta_ontime_rate'] = round((1 - (kpis['h2020_tta_delays'] / total_signed)) * 100, 1) if total_signed > 0 else 99.8
        
        
       
        amend_heu = _parse_safe(report_vars.get('HORIZON_overview'))
        print("\n📦 DEBUG: Final AMD H2020 Data (after tagging and mapping):")
        pprint(amend_heu)
        kpis['heu_amendment_count'] = _safe_get_count(amend_heu, 329)
        heu_delays = _safe_get_sum(amend_heu, 'Delayed', 0)
        total_heu_signed = _safe_get_sum(amend_heu, 'Signed', 1)
        kpis['heu_tta_ontime_rate'] = round((1 - (heu_delays / total_heu_signed)) * 100, 1) if total_heu_signed > 0 else 100.0

        # --- Payments ---
        pay_heu = _parse_safe(report_vars.get('HEU_All_Payments'))
        kpis['heu_payment_count'] = _safe_get_count(pay_heu, 486)
        kpis['heu_payment_total_mil'] = round(_safe_get_sum(pay_heu, 'Amount') / 1e6, 2)
        
        pay_h2020 = _parse_safe(report_vars.get('H2020_All_Payments'))
        kpis['h2020_payment_count'] = _safe_get_count(pay_h2020, 620)
        kpis['h2020_payment_total_mil'] = round(_safe_get_sum(pay_h2020, 'Amount') / 1e6, 2)

        # --- Contextual Hint for Payments ---
        kpis['payment_consumption_context'] = ""
        pay_credits_heu = _parse_safe(report_vars.get('table_2a_HE'))
        if pay_credits_heu:
            paid = _safe_get_sum(pay_credits_heu, 'Paid_Amount')
            available = _safe_get_sum(pay_credits_heu, 'Available_Payment_Appropriations', 1)
            if available > 0:
                consumption_rate = (paid / available) * 100
                if quarter_period == 'Q1' and consumption_rate < 30:
                    kpis['payment_consumption_context'] = "Low consumption at the start of the year is typical and aligns with the budgetary cycle."

        # --- Granting ---
        kpis['ttg_avg'] = _safe_get_avg(_parse_safe(report_vars.get('table_ttg')), 'avg_ttg_days', 105)
        completion_data = _parse_safe(report_vars.get('table_1c'))
        kpis['stg_completion_rate'] = _safe_get_avg([r for r in (completion_data or []) if isinstance(r, dict) and r.get('Call') == 'STG'], 'Completion', 86)
        kpis['poc_completion_rate'] = _safe_get_avg([r for r in (completion_data or []) if isinstance(r, dict) and r.get('Call') == 'POC'], 'Completion', 90)

        # --- Audits ---
        auri_overview = _parse_safe(report_vars.get('auri_overview'))
        kpis['outstanding_audits'] = int(_safe_get_sum(auri_overview, 'Ongoing', 148))
        kpis['error_rate'] = _safe_get_avg(_parse_safe(report_vars.get('error_rates')), 'Rate', 2.1)
        kpis['tti_avg'] = _safe_get_avg(_parse_safe(report_vars.get('tti_combined')), 'TTI', 60)
        kpis['neg_adjustment_total_mil'] = round(_safe_get_sum(_parse_safe(report_vars.get('negative_adjustments')), 'Amount') / 1e6, 2)
        kpis['recovery_total_mil'] = round(_safe_get_sum(_parse_safe(report_vars.get('recovery_activity')), 'Amount') / 1e6, 2)

        # --- Other ---
        fdi_data = _parse_safe(report_vars.get('table_3c'))
        kpis['fdi_breaches'] = _safe_get_count(fdi_data, 3)

        return kpis

    def run(self, ctx: RenderContext) -> RenderContext:
        log=logging.getLogger(self.name)
        conn=ctx.db.conn
        cutoff=pd.to_datetime(ctx.cutoff)
        db_path=Path(conn.execute("PRAGMA database_list").fetchone()[2])
        report=ctx.report_name
        report_params=load_report_params(report_name=report, db_path=db_path)
        module_errors=[]
        module_warnings=[]

        print("🤖 Starting AI Comments Generation Module...")

        try:
            # 1. CONFIGURATION AND INITIALIZATION
            print("⚙️ Initializing AI generation components...")
            model=report_params.get('ai_model', CommentsConfig.DEFAULT_MODEL)
            temperature=report_params.get('ai_temperature', CommentsConfig.DEFAULT_TEMPERATURE)
            if model not in CommentsConfig.AVAILABLE_MODELS:
                module_warnings.append(f"Unknown model {model}, using default: {CommentsConfig.DEFAULT_MODEL}")
                model=CommentsConfig.DEFAULT_MODEL
            model_config=CommentsConfig.AVAILABLE_MODELS[model]
            print(f"🤖 Model configured: {model_config['name']} (temp: {temperature})")
            generator=EnhancedReportGenerator()
            print("✅ AI components initialized successfully")

            # 2. FINANCIAL DATA LOADING AND PRE-PROCESSING
            print("📊 Loading and pre-processing financial data...")
            current_year=report_params.get('current_year')
            quarter_period=report_params.get('quarter_period')
            if not current_year or not quarter_period:
                raise ValueError("Missing required report parameters: current_year or quarter_period")

            print(f"📅 Report period: {quarter_period} {current_year}")
            report_vars=fetch_vars_for_report(report, str(db_path))
            
            # Create the comprehensive financial data dictionary
            financial_data = self._map_financial_data(report_vars)
            
            # ✅ ARCHITECTURE FIX: Pre-process KPIs here and add them to the dictionary
            # This ensures the generator receives everything it needs.
            intro_summary_kpis = self._extract_and_contextualize_intro_kpis(report_vars, quarter_period)
            financial_data['intro_summary_kpis'] = intro_summary_kpis


            if not financial_data:
                raise ValueError("No financial data tables available for generation.")

            print(f"✅ Loaded {len(report_vars)} variables and mapped {len(financial_data)} data tables.")
            detected_acronyms=self._detect_acronyms_in_data(financial_data)
            acronym_context=self.create_acronym_context_for_ai(detected_acronyms)
            print(f"📝 Detected {len(detected_acronyms)} acronyms for AI context.")

        except Exception as e:
            error_msg=f"Initialization or data loading failed: {str(e)}"
            module_errors.append(error_msg)
            print(f"❌ {error_msg}")
            return ctx # Exit early if setup fails

        # 3. SINGLE SECTIONS GENERATION
        print("\n📝 Starting single sections generation...")
        stats = {'successful': 0, 'failed': 0, 'variables_created': []}
        sections_to_generate = CommentsConfig.SINGLE_SECTIONS
        mapping_matrix = TemplateSectionMatrix.get_complete_mapping_matrix()

        for i, section_key in enumerate(sections_to_generate, 1):
            print(f"\n{'='*60}\n📝 [{i}/{len(sections_to_generate)}] SECTION: {section_key}\n{'='*60}")
            
            section_config = mapping_matrix.get(section_key)
            if not section_config:
               raise ValueError(f"Section configuration for '{section_key}' is missing from the mapping matrix.")

            output_conf = section_config.get('output_configuration')
            if not output_conf or 'variable_name' not in output_conf:
                raise ValueError(f"Could not find 'variable_name' in output_configuration for section '{section_key}'. Full config: {section_config}")

            try:
                # Use the centralized generator, which handles all section types internally
                commentary_output = generator.generate_section_commentary(
                    section_key=section_key,
                    quarter_period=quarter_period,
                    current_year=current_year,
                    financial_data=financial_data,
                    model=model,
                    temperature=CommentsConfig.SECTION_TEMPERATURE_OVERRIDES.get(section_key, temperature),
                    acronym_context=acronym_context,
                    cutoff_date=cutoff,
                    verbose=True
                )

                if commentary_output:
                    # Check if the output is for a special "looping" section
                    if "[---END_OF_SECTION---]" in commentary_output:
                        print(f"🔄 Processing multiple outputs for {section_key}...")
                        sub_sections = commentary_output.split("\n[---END_OF_SECTION---]\n")
                        for sub_section in sub_sections:
                            match = re.match(r"VAR_NAME:(.+?)\n(.+)", sub_section, re.DOTALL)
                            if match:
                                var_name, text = match.groups()
                                self._save_variable(var_name, text, report, db_path, stats, module_errors)
                        if sub_sections:
                            stats['successful'] += 1 # Count the parent section as one success
                    else: # Standard single section output
                        # ✅ Use safe .get() access to prevent KeyError
                        output_conf = section_config.get('output_configuration')
                        if not output_conf or 'variable_name' not in output_conf:
                            raise ValueError(f"Could not find 'variable_name' in output_configuration for section '{section_key}'.")

                        var_name = output_conf['variable_name']
                        
                        if var_name:
                            self._save_variable(var_name, commentary_output, report, db_path, stats, module_errors)
                            stats['successful'] += 1
                        else:
                            error_msg = f"Could not find 'variable_name' in output_configuration for section '{section_key}'."
                            module_errors.append(error_msg)
                            print(f"❌ {error_msg}")
                            stats['failed'] += 1
                else:
                    error_msg = f"Failed to generate acceptable content for {section_key} after all retries."
                    module_warnings.append(error_msg)
                    print(f"⚠️ {error_msg}")
                    stats['failed'] += 1

            except Exception as e:
                import traceback
                error_msg = f"An unexpected error occurred while generating section '{section_key}': {str(e)}"
                module_errors.append(error_msg)
                print(f"❌ {error_msg}")
                traceback.print_exc() # Print full traceback for better debugging
                stats['failed'] += 1

        self._print_completion_summary(stats, module_errors, module_warnings, model_config, temperature)
        return ctx

    def _save_variable(self, var_name, value, report, db_path, stats, module_errors):
        """Helper to save a generated variable to the database."""
        try:
            print(f"💾 Saving {var_name} to database...")
            insert_variable(
                report=report,
                module='CommentsModule',
                var=var_name,
                value=value,
                db_path=db_path,
                anchor=var_name,
            )
            print(f"✅ SUCCESS: Saved {var_name} ({len(value.split())} words)")
            stats['variables_created'].append(var_name)
        except Exception as e:
            error_msg = f"Database save failed for {var_name}: {str(e)}"
            module_errors.append(error_msg)
            print(f"❌ {error_msg}")

    def _print_completion_summary(self, stats, errors, warnings, model_config, temp):
        """Prints a final summary of the module's execution."""
        print("\n" + "="*60)
        print("🤖 AI COMMENTS MODULE COMPLETION SUMMARY")
        print("="*60)
        total_generated = stats['successful'] + stats['failed']
        success_rate = (stats['successful'] / total_generated * 100) if total_generated > 0 else 0

        if errors:
            print(f"❌ Module completed with {len(errors)} ERRORS.")
        elif warnings:
            print(f"⚠️ Module completed with {len(warnings)} WARNINGS.")
        else:
            print("🎉 Module status: FULLY SUCCESSFUL")

        print(f"\n📊 GENERATION STATISTICS:")
        print(f"   - Sections Processed: {total_generated}")
        print(f"   - ✅ Successful: {stats['successful']}")
        print(f"   - ❌ Failed: {stats['failed']}")
        print(f"   - 📈 Success Rate: {success_rate:.1f}%")
        print(f"   - 💾 Total variables created: {len(stats['variables_created'])}")
        print(f"   - 🤖 AI Model: {model_config['name']} (Temp: {temp})")

        if warnings:
            print("\n⚠️ Warnings Issued:")
            for w in warnings[:5]: print(f"   - {w}")
            if len(warnings) > 5: print(f"   ...and {len(warnings)-5} more.")

        if errors:
            print("\n❌ Errors Encountered:")
            for e in errors[:5]: print(f"   - {e}")
            if len(errors) > 5: print(f"   ...and {len(errors)-5} more.")

        print("="*60)


    def _map_financial_data(self, report_vars: Dict[str, Any]) -> Dict[str, Any]:
        """
        ✅ FIXED: Map report variables to a comprehensive financial data structure.
        This mapping is critical and now aligns with the `primary_data` and `secondary_data`
        requirements in the TemplateSectionMatrix.
        """
        financial_data = {
            # Core budget and commitment tables
            'summary_budget': report_vars.get('overview_budget_table'),
            'commitments': report_vars.get('table_1a'),
            'pay_credits_H2020': report_vars.get('table_2a_H2020'),
            'pay_credits_HEU': report_vars.get('table_2a_HE'),

            # Granting and call completion
            'grants_signature_activity': report_vars.get('table_3_signatures'),
            'grants_commitment_activity': report_vars.get('table_3b_commitments'),
            'completion_previous_year_calls': report_vars.get('table_1c'),
            'current_year_global_commitment_activity': report_vars.get('table_1c'), # Re-used as per original
            'TTG': report_vars.get('table_ttg'),
            'TTS': report_vars.get('table_tts'),

            # FDI (Final Date for Implementation)
            'grants_exceeding_fdi': report_vars.get('table_3c'),

            # Time-to-Pay (TTP) - Essential for intro_summary and ttp_performance
            'TTP_Overview': report_vars.get('TTP_performance_summary_table'),
            'H2020_TTP_FP': report_vars.get('H2020_FP_ttp_chart'),
            'H2020_TTP_IP': report_vars.get('H2020_IP_ttp_chart'),
            'HEU_TTP_FP': report_vars.get('HEU_FP_ttp_chart'),
            'HEU_TTP_IP': report_vars.get('HEU_IP_ttp_chart'),
            'HEU_TTP_PF': report_vars.get('HEU_PF_ttp_chart'),
            'HEU_TTP_EXPERTS': report_vars.get('HEU_EXPERTS_ttp_chart'),

            # Amendments - Essential for intro_summary
            'amendment_activity_H2020': report_vars.get('H2020_overview'),
            'amendment_activity_HEU': report_vars.get('HORIZON_overview'),
            'amendment_TTA_H2020': report_vars.get('H2020_tta'),
            'amendment_TTA_HEU': report_vars.get('HORIZON_tta'),
            'amendment_cases_H2020': report_vars.get('H2020_cases'),
            'amendment_cases_HEU': report_vars.get('HORIZON_cases'),

            # Audits and Recovery - Essential for intro_summary
            'auri_overview': report_vars.get('auri_overview'),
            'recovery_activity': report_vars.get('recovery_activity'),
            'external_audits_activity': report_vars.get('external_audits'),

            # Detailed Payment and Analysis Tables
            'H2020_All_Payments': report_vars.get('H2020_All_Payments'),
            'HEU_All_Payments': report_vars.get('HEU_All_Payments'),
            'H2020_payments_analysis_ALL': report_vars.get('H2020_all_paym_analysis_table'),
            'HEU_payments_analysis_ALL': report_vars.get('HEU_all_paym_analysis_table'),
            'H2020_payments_analysis_ADG': report_vars.get('H2020_ADG_paym_analysis_table'),
            'H2020_payments_analysis_COG': report_vars.get('H2020_COG_paym_analysis_table'),
            'H2020_payments_analysis_STG': report_vars.get('H2020_STG_paym_analysis_table'),
            'H2020_payments_analysis_SYG': report_vars.get('H2020_SYG_paym_analysis_table'),
            'HEU_payments_analysis_ADG': report_vars.get('HEU_ADG_paym_analysis_table'),
            'HEU_payments_analysis_COG': report_vars.get('HEU_COG_paym_analysis_table'),
            'HEU_payments_analysis_STG': report_vars.get('HEU_STG_paym_analysis_table'),
            'HEU_payments_analysis_SYG': report_vars.get('HEU_SYG_paym_analysis_table'),
            'HEU_payments_analysis_POC': report_vars.get('HEU_POC_paym_analysis_table'),
            'HEU_payments_analysis_EXPERTS': report_vars.get('HEU_EXPERTS_paym_analysis_table'),
        }
        # Filter out None values to prevent errors downstream
        return {k: v for k, v in financial_data.items() if v is not None}

    def _detect_acronyms_in_data(self, financial_data: Dict[str, Any]) -> List[str]:
        """Detect which acronyms from our dictionary appear in the financial data"""
        detected_acronyms=set()
        # Check data keys for acronyms
        for key in financial_data.keys():
            for acronym in CommentsConfig.ACRONYMS_DICTIONARY.keys():
                if acronym in key.upper():
                    detected_acronyms.add(acronym)
        # Always include the most common acronyms
        detected_acronyms.update(['H2020', 'HEU', 'TTP', 'TTG', 'TTA', 'FDI', 'ERCEA'])
        return sorted(list(detected_acronyms))

    def create_acronym_context_for_ai(self, detected_acronyms: List[str]) -> str:
        """Create a context string with acronym definitions for the AI."""
        if not detected_acronyms: return ""
        context_lines = ["📚 ACRONYMS REFERENCE:", "="*60]
        for acronym in sorted(detected_acronyms):
            if acronym in CommentsConfig.ACRONYMS_DICTIONARY:
                definition = CommentsConfig.ACRONYMS_DICTIONARY[acronym]
                context_lines.append(f"• {acronym}: {definition['full_name']} ({definition.get('description', 'No description')})")
        context_lines.append("="*60)
        return "\n".join(context_lines)

    @staticmethod
    def get_acronyms_by_category(category: str) -> Dict[str, Dict]:
        """Get acronyms filtered by category"""
        return {
            acronym: details
            for acronym, details in CommentsConfig.ACRONYMS_DICTIONARY.items()
            if details.get('category') == category
    }

    @staticmethod
    def get_available_programs() -> List[str]:
        """Get list of available programs"""
        return list(PROGRAM_MAPPING.keys())