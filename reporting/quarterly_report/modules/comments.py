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

# Import your existing functions (adjust imports as needed)
from ingestion.db_utils import (
    fetch_vars_for_report,
    load_report_params
)

# Import the enhanced generator components
from reporting.quarterly_report.report_utils.enhanced_report_generator import (
    EnhancedReportGenerator,
    ReportTemplateLibrary,
    TemplateSectionMatrix,
    CallTypeProcessor,
    ProgramProcessor,
    PROGRAMS_LIST,
    CALL_TYPES_LIST
)

from ingestion.db_utils import (
 fetch_vars_for_report,
    load_report_params,
    insert_variable
)

logger = logging.getLogger(__name__)

# ================================================================
# 🧪 TESTING AND UTILITIES
# ================================================================



# ================================================================
# 🧪 TESTING AND UTILITIES
# ================================================================

def test_comments_module(
    report_name: str = "Quarterly_Report",
    db_path: str = "database/reporting.db",
    cutoff_date: str = None
):
    """Test the CommentsModule independently"""
    
    print("🧪 TESTING COMMENTS MODULE")
    print("=" * 60)
    
    try:
        # Create mock context
        from reporting.quarterly_report.utils import RenderContext
        
        if cutoff_date is None:
            cutoff_date = datetime.datetime.now().strftime('%Y-%m-%d')
        
        # Mock database connection
        import sqlite3
        conn = sqlite3.connect(db_path)
        
        # Create context
        ctx = RenderContext(
            db=type('DB', (), {'conn': conn})(),
            cutoff=cutoff_date,
            report_name=report_name
        )
        
        # Run module
        module = CommentsModule()
        result_ctx = module.run(ctx)
        
        print("✅ Comments module test completed")
        return result_ctx
        
    except Exception as e:
        print(f"❌ Comments module test failed: {str(e)}")
        import traceback
        print(f"📋 Traceback: {traceback.format_exc()}")
        return None

def preview_comments_generation(
    report_name: str = "Quarterly_Report",
    db_path: str = "database/reporting.db"
):
    """Preview what comments would be generated"""
    
    print("📋 COMMENTS GENERATION PREVIEW")
    print("=" * 50)
    
    try:
        # Load report data
        report_params = load_report_params(report_name, db_path)
        report_vars = fetch_vars_for_report(report_name, db_path)
        
        # Calculate expected generation
        single_sections = len(CommentsConfig.SINGLE_SECTIONS)
        loop_combinations = len(CommentsConfig.LOOP_PROGRAMS) * len(CommentsConfig.LOOP_CALL_TYPES)
        detailed_combinations = len(CommentsConfig.LOOP_PROGRAMS) * len(CommentsConfig.DETAILED_CALL_TYPES)
        
        # Show summary
        print(f"📊 Report: {report_name}")
        print(f"   Period: {report_params.get('quarter_period')} {report_params.get('current_year')}")
        print(f"   Available data tables: {len([v for v in report_vars.values() if v is not None])}")
        print(f"\n🎯 Expected AI Generation:")
        print(f"   📝 Single sections: {single_sections}")
        print(f"   🔄 Loop combinations: {loop_combinations}")
        print(f"   📊 Detailed combinations: {detailed_combinations} (if enabled)")
        print(f"   💾 Total variables: {single_sections + loop_combinations}")
        
        print(f"\n🤖 AI Configuration:")
        model = report_params.get('ai_model', CommentsConfig.DEFAULT_MODEL)
        print(f"   Model: {CommentsConfig.AVAILABLE_MODELS[model]['name']}")
        print(f"   Temperature: {report_params.get('ai_temperature', CommentsConfig.DEFAULT_TEMPERATURE)}")
        
        return True
        
    except Exception as e:
        print(f"❌ Preview failed: {str(e)}")
        return False



# ================================================================
# 🎯 MODULE CONFIGURATION
# ================================================================

class CommentsConfig:
    """Configuration for AI comment generation"""
    
    # 🤖 AI Model Configuration
    AVAILABLE_MODELS = {
        'deepseek-r1:14b': {
            'name': 'DeepSeek R1 14B',
            'description': 'Balanced model for financial analysis',
            'temperature': 0.3,
            'max_tokens_multiplier': 1.5,
            'recommended_for': ['financial', 'analytical', 'executive']
        },
        'qwen2.5:14b': {
            'name': 'Qwen 2.5 14B',
            'description': 'High-quality model for detailed analysis',
            'temperature': 0.2,
            'max_tokens_multiplier': 1.7,
            'recommended_for': ['detailed', 'technical', 'compliance']
        },
        'llama2:13b': {
            'name': 'Llama 2 13B',
            'description': 'Alternative model for general reports',
            'temperature': 0.4,
            'max_tokens_multiplier': 1.3,
            'recommended_for': ['general', 'narrative']
        }
    }
    
    # 📊 Generation Settings
    DEFAULT_MODEL = 'deepseek-r1:14b'
    DEFAULT_TEMPERATURE = 0.3
    API_ENDPOINT = "http://localhost:11434/api/generate"
    API_TIMEOUT = 180
    DEFAULT_MODULE = "ReinMonModule"
    
    # 🎯 Section Configuration
    SINGLE_SECTIONS = [
        'intro_summary',
        'budget_overview', 
        'payments_workflow',
        'commitments_workflow',
        'amendments_workflow',
        'audit_workflow'
    ]
    
    # 🔄 Loop Configuration
    LOOP_PROGRAMS = ['HEU', 'H2020']
    LOOP_CALL_TYPES = ['STG', 'ADG', 'POC', 'COG', 'SYG', 'CSA']
    
    # 📋 Detailed Call Type Definitions
    DETAILED_CALL_TYPES = [
        {'code': 'A.1', 'description': 'Pre-financing and Interim Payments', 'abbreviation': 'STG'},
        {'code': 'A.2', 'description': 'Final Payments', 'abbreviation': 'ADG'},
        {'code': 'B.1', 'description': 'Coordination Support Actions', 'abbreviation': 'CSA'},
        {'code': 'B.2', 'description': 'Research and Innovation Actions', 'abbreviation': 'RIA'},
        {'code': 'C.1', 'description': 'Innovation Actions', 'abbreviation': 'IA'},
    ]

# ================================================================
# 🤖 COMMENTS MODULE
# ================================================================

class CommentsModule(BaseModule):
    name        = "Comments"          # shows up in UI
    description = "AI GENERATED COMMENTS"

    def run(self, ctx: RenderContext) -> RenderContext:
        log = logging.getLogger(self.name)
        conn = ctx.db.conn
        cutoff = pd.to_datetime(ctx.cutoff)
        db_path = Path(conn.execute("PRAGMA database_list").fetchone()[2])
        report = ctx.report_name

        # Load report parameters
        report_params = load_report_params(report_name=report, db_path=db_path)
        # Module-level error tracking (following PaymentsModule pattern)
        module_errors = []
        module_warnings = []
        
        print("🤖 Starting AI Comments Generation Module...")

        try:
            # ══════════════════════════════════════════════════════════════════
            # 1. CONFIGURATION AND INITIALIZATION
            # ══════════════════════════════════════════════════════════════════
            
            print("⚙️ Initializing AI generation components...")
            
            # Get model configuration from report_params or use default
            model = report_params.get('ai_model', CommentsConfig.DEFAULT_MODEL)
            temperature = report_params.get('ai_temperature', CommentsConfig.DEFAULT_TEMPERATURE)
            
            # Validate model
            if model not in CommentsConfig.AVAILABLE_MODELS:
                warning_msg = f"Unknown model {model}, using default: {CommentsConfig.DEFAULT_MODEL}"
                module_warnings.append(warning_msg)
                print(f"⚠️ {warning_msg}")
                model = CommentsConfig.DEFAULT_MODEL
            
            model_config = CommentsConfig.AVAILABLE_MODELS[model]
            print(f"🤖 Model configured: {model_config['name']} (temp: {temperature})")
            
            # Initialize generator
            generator = EnhancedReportGenerator()
            
            print("✅ AI components initialized successfully")

        except Exception as e:
            error_msg = f"AI component initialization failed: {str(e)}"
            module_errors.append(error_msg)
            print(f"❌ {error_msg}")
            return ctx

        try:
            # ══════════════════════════════════════════════════════════════════
            # 2. FINANCIAL DATA LOADING 
            # ══════════════════════════════════════════════════════════════════
            
            print("📊 Loading financial data from database...")
            
            # Get parameters from database
            current_year = report_params.get('current_year')
            quarter_period = report_params.get('quarter_period')
            
            if not current_year or not quarter_period:
                error_msg = "Missing required parameters: current_year or quarter_period"
                module_errors.append(error_msg)
                print(f"❌ {error_msg}")
                return ctx
            
            print(f"📅 Report period: {quarter_period} {current_year}")
            
            # Fetch all report variables
            report_vars = fetch_vars_for_report(report, str(db_path))
            print(f"📋 Loaded {len(report_vars)} variables from database")
            
            # Map to comprehensive financial data structure (following your pattern)
            financial_data = self._map_financial_data(report_vars)
            print(f"✅ Mapped financial data: {len(financial_data)} tables available")
            
            if len(financial_data) == 0:
                error_msg = "No financial data tables available for generation"
                module_errors.append(error_msg)
                print(f"❌ {error_msg}")
                return ctx
                
            # Log available tables
            print(f"📊 Available tables: {list(financial_data.keys())[:10]}{'...' if len(financial_data) > 10 else ''}")

        except Exception as e:
            error_msg = f"Financial data loading failed: {str(e)}"
            module_errors.append(error_msg)
            print(f"❌ {error_msg}")
            return ctx

        # ══════════════════════════════════════════════════════════════════
        # 3. SINGLE SECTIONS GENERATION
        # ══════════════════════════════════════════════════════════════════
        
        print("📝 Starting single sections generation...")
        single_section_stats = {'successful': 0, 'failed': 0, 'variables_created': []}
        
        try:
            # Get available sections from mapping matrix
            mapping = TemplateSectionMatrix.get_complete_mapping_matrix()
            available_sections = [k for k in mapping.keys() if k not in ['payment_analysis', 'call_type_payment_detail', 'auto_call_type_detail']]
            
            # Use configured sections or all available
            sections_to_generate = [s for s in CommentsConfig.SINGLE_SECTIONS if s in available_sections]
            
            print(f"📋 Generating {len(sections_to_generate)} single sections...")
            
            for i, section_key in enumerate(sections_to_generate, 1):
                print(f"\n📝 [{i}/{len(sections_to_generate)}] Generating: {section_key}")
                
                try:
                    commentary = generator.generate_section_commentary(
                        section_key=section_key,
                        quarter_period=quarter_period,
                        current_year=current_year,
                        financial_data=financial_data,
                        model=model,
                        temperature=temperature,
                        verbose=True
                    )
                    
                    if commentary:
                        # Get variable name from mapping
                        section_config = mapping[section_key]
                        var_name = section_config['output_configuration']['variable_name']
                        
                        # Save to database using your existing function
                        try:
                            print(f"💾 Saving {var_name} to database...")
                            insert_variable(
                                report=report,
                                module=CommentsConfig.DEFAULT_MODULE,
                                var=var_name,
                                value=commentary,
                                db_path=db_path,
                                anchor=var_name,
                            )
                            print(f"🎉 SUCCESSFULLY saved {var_name} to database")
                            
                            single_section_stats['successful'] += 1
                            single_section_stats['variables_created'].append(var_name)
                            
                            # Log generation stats
                            word_count = len(commentary.split())
                            target = section_config['output_configuration']['word_limit']
                            print(f"✅ Generated {word_count} words (target: {target})")
                            
                        except Exception as e:
                            error_msg = f"Failed to save {var_name}: {str(e)}"
                            module_errors.append(error_msg)
                            print(f"❌ {error_msg}")
                            single_section_stats['failed'] += 1
                    else:
                        error_msg = f"Generation failed for section: {section_key}"
                        module_warnings.append(error_msg)
                        print(f"⚠️ {error_msg}")
                        single_section_stats['failed'] += 1
                        
                except Exception as e:
                    error_msg = f"Error generating {section_key}: {str(e)}"
                    module_errors.append(error_msg)
                    print(f"❌ {error_msg}")
                    single_section_stats['failed'] += 1
            
            print(f"\n✅ Single sections completed: {single_section_stats['successful']} successful, {single_section_stats['failed']} failed")

        except Exception as e:
            error_msg = f"Single sections generation failed: {str(e)}"
            module_errors.append(error_msg)
            print(f"❌ {error_msg}")

        # ══════════════════════════════════════════════════════════════════
        # 4. PREDEFINED CALL TYPE LOOPS GENERATION
        # ══════════════════════════════════════════════════════════════════
        
        print("🔄 Starting predefined call type loops generation...")
        loop_stats = {'successful': 0, 'failed': 0, 'variables_created': []}
        
        try:
            # Use configured programs and call types
            programs = CommentsConfig.LOOP_PROGRAMS
            call_types = CommentsConfig.LOOP_CALL_TYPES
            
            print(f"📋 Generating loops for {len(programs)} programs × {len(call_types)} call types = {len(programs) * len(call_types)} combinations")
            
            results = generator.generate_predefined_call_type_loops(
                quarter_period=quarter_period,
                current_year=current_year,
                financial_data=financial_data,
                programs=programs,
                call_types=call_types,
                model=model,
                temperature=temperature,
                verbose=True
            )
            
            # Process results and save to database
            if 'generated_details' in results:
                for var_name, details in results['generated_details'].items():
                    try:
                        print(f"💾 Saving {var_name} to database...")
                        insert_variable(
                            report=report,
                            module=CommentsConfig.DEFAULT_MODULE,
                            var=var_name,
                            value=details['commentary'],
                            db_path=db_path,
                            anchor=var_name,
                        )
                        print(f"🎉 SUCCESSFULLY saved {var_name} to database")
                        
                        loop_stats['successful'] += 1
                        loop_stats['variables_created'].append(var_name)
                        
                    except Exception as e:
                        error_msg = f"Failed to save loop variable {var_name}: {str(e)}"
                        module_errors.append(error_msg)
                        print(f"❌ {error_msg}")
                        loop_stats['failed'] += 1
            
            # Log loop statistics
            results_stats = results.get('statistics', {})
            print(f"✅ Loop generation completed:")
            print(f"   📊 AI Generation: {results_stats.get('successful', 0)} successful, {results_stats.get('failed', 0)} failed")
            print(f"   💾 Database Storage: {loop_stats['successful']} successful, {loop_stats['failed']} failed")
            
            # Add warnings for failed combinations
            if results.get('failed_generations'):
                warning_msg = f"Some loop combinations failed: {', '.join(results['failed_generations'][:3])}{'...' if len(results['failed_generations']) > 3 else ''}"
                module_warnings.append(warning_msg)
                print(f"⚠️ {warning_msg}")

        except Exception as e:
            error_msg = f"Loop generation failed: {str(e)}"
            module_errors.append(error_msg)
            print(f"❌ {error_msg}")

        # ══════════════════════════════════════════════════════════════════
        # 5. DETAILED CALL TYPE GENERATION (Optional)
        # ══════════════════════════════════════════════════════════════════
        
        # Only run detailed generation if enabled in report_params
        if report_params.get('enable_detailed_call_types', False):
            print("📊 Starting detailed call type generation...")
            detailed_stats = {'successful': 0, 'failed': 0, 'variables_created': []}
            
            try:
                results = generator.generate_call_type_payment_details(
                    programmes=CommentsConfig.LOOP_PROGRAMS,
                    call_types=CommentsConfig.DETAILED_CALL_TYPES,
                    quarter_period=quarter_period,
                    current_year=current_year,
                    financial_data=financial_data,
                    model=model,
                    temperature=temperature,
                    verbose=True
                )
                
                # Process and save detailed results
                if 'generated_details' in results:
                    for var_name, details in results['generated_details'].items():
                        try:
                            insert_variable(
                                report=report,
                                module=CommentsConfig.DEFAULT_MODULE,
                                var=var_name,
                                value=details['commentary'],
                                db_path=db_path,
                                anchor=var_name,
                            )
                            print(f"🎉 SUCCESSFULLY saved detailed {var_name} to database")
                            
                            detailed_stats['successful'] += 1
                            detailed_stats['variables_created'].append(var_name)
                            
                        except Exception as e:
                            error_msg = f"Failed to save detailed variable {var_name}: {str(e)}"
                            module_errors.append(error_msg)
                            print(f"❌ {error_msg}")
                            detailed_stats['failed'] += 1
                
                print(f"✅ Detailed call types completed: {detailed_stats['successful']} successful, {detailed_stats['failed']} failed")

            except Exception as e:
                error_msg = f"Detailed call type generation failed: {str(e)}"
                module_errors.append(error_msg)
                print(f"❌ {error_msg}")
        else:
            print("ℹ️ Detailed call type generation disabled (enable with 'enable_detailed_call_types' parameter)")

        # ══════════════════════════════════════════════════════════════════
        # 6. MODULE COMPLETION STATUS (Following PaymentsModule pattern)
        # ══════════════════════════════════════════════════════════════════
        
        print("\n" + "="*60)
        print("🤖 AI COMMENTS MODULE COMPLETION SUMMARY")
        print("="*60)
        
        # Calculate total statistics
        total_successful = single_section_stats['successful'] + loop_stats['successful']
        total_failed = single_section_stats['failed'] + loop_stats['failed']
        total_variables = single_section_stats['variables_created'] + loop_stats['variables_created']
        
        if module_errors:
            print(f"⚠️ Module completed with {len(module_errors)} errors:")
            for i, error in enumerate(module_errors, 1):
                print(f"   {i}. {error}")
            
            if module_warnings:
                print(f"\n⚠️ Additional warnings ({len(module_warnings)}):")
                for i, warning in enumerate(module_warnings, 1):
                    print(f"   {i}. {warning}")
                    
            print(f"\n❌ Module status: COMPLETED WITH ERRORS")
            print(f"📊 Partial results: {total_successful} variables generated successfully")
            
        elif module_warnings:
            print(f"✅ Module completed with {len(module_warnings)} warnings:")
            for i, warning in enumerate(module_warnings, 1):
                print(f"   {i}. {warning}")
            print(f"\n⚠️ Module status: COMPLETED WITH WARNINGS")
            
        else:
            print("✅ All AI generation completed successfully!")
            print("\n🎉 Module status: FULLY SUCCESSFUL")

        # Detailed statistics
        print(f"\n📊 GENERATION STATISTICS:")
        print(f"   📝 Single sections: {single_section_stats['successful']} successful, {single_section_stats['failed']} failed")
        print(f"   🔄 Loop combinations: {loop_stats['successful']} successful, {loop_stats['failed']} failed")
        print(f"   💾 Total variables created: {len(total_variables)}")
        print(f"   🤖 AI Model used: {model_config['name']}")
        print(f"   🌡️ Temperature: {temperature}")
        
        # Show some created variables
        if total_variables:
            print(f"\n📋 CREATED VARIABLES (showing first 10):")
            for var in total_variables[:10]:
                print(f"   • {var}")
            if len(total_variables) > 10:
                print(f"   ... and {len(total_variables) - 10} more")

        print("="*60)
        print("🏁 AI Comments Module completed")
        print("="*60)

        # Return the context (following BaseModule pattern)
        return ctx
    
    def _map_financial_data(self, report_vars: Dict[str, Any]) -> Dict[str, Any]:
        """
        Map report variables to financial data structure
        Following your exact data mapping pattern from the original implementation
        """
        
        financial_data = {
            # 📋 Core commitment and payment tables
            'commitments': report_vars.get('table_1a'),
            'pay_credits_H2020': report_vars.get('table_2a_H2020'),
            'pay_credits_HEU': report_vars.get('table_2a_HE'),
            'summary_budget': report_vars.get('overview_budget_table'),
            
            # 📊 Activity and completion tables
            'completion_previous_year_calls': report_vars.get('table_1c'),
            'current_year_global_commitment_activity': report_vars.get('table_1c'),
            'grants_commitment_activity': report_vars.get('table_3b_commitments'),
            'grants_signature_activity': report_vars.get('table_3_signatures'),
            'grants_exceeding_fdi': report_vars.get('table_3c'),
            
            # ⏱️ Time to grant and signature tables
            'TTG': report_vars.get('table_ttg'),
            'TTS': report_vars.get('table_tts'),
            
            # ✏️ Amendment workflow tables
            'amendment_activity_H2020': report_vars.get('H2020_overview'),
            'amendment_activity_HEU': report_vars.get('HORIZON_overview'),
            'amendment_cases_H2020': report_vars.get('H2020_cases'),
            'amendment_cases_HEU': report_vars.get('HORIZON_cases'),
            'amendment_TTA_H2020': report_vars.get('H2020_tta'),
            'amendment_TTA_HEU': report_vars.get('HORIZON_tta'),
            
            # 🔍 Audit and recovery tables
            'auri_overview': report_vars.get('auri_overview'),
            'auri_negative_adjustments_overview': report_vars.get('negative_adjustments'),
            'auri_implementation_comparison': report_vars.get('implementation_comparison'),
            'auri_time_to_implement_overview': report_vars.get('tti_combined'),
            'recovery_activity': report_vars.get('recovery_activity'),
            'external_audits_activity': report_vars.get('external_audits'),
            'error_rates': report_vars.get('error_rates'),
            
            # 💰 HEU payment tables
            'HEU_payments_all': report_vars.get('HEU_All_Payments'),
            'HEU_payments_final_payments': report_vars.get('HEU_Final Payments'),
            'HEU_payments_pre_financing_payments': report_vars.get('HEU_Pre-financing'),
            'HEU_payments_EXPERTS': report_vars.get('HEU_Experts and Support'),
            
            # 💰 H2020 payment tables
            'H2020_payments_all': report_vars.get('H2020_All_Payments'),
            'H2020_payments_final_payments': report_vars.get('H2020_Final Payments'),
            'H2020_payments_interim_payments': report_vars.get('H2020_Interim Payments'),
            
            # 📊 H2020 payment analysis tables
            'H2020_payments_analysis_ADG': report_vars.get('H2020_ADG_paym_analysis_table'),
            'H2020_payments_analysis_COG': report_vars.get('H2020_COG_paym_analysis_table'),
            'H2020_payments_analysis_STG': report_vars.get('H2020_STG_paym_analysis_table'),
            'H2020_payments_analysis_SYG': report_vars.get('H2020_SYG_paym_analysis_table'),
            'H2020_payments_analysis_ALL': report_vars.get('H2020_all_paym_analysis_table'),
            
            # 📊 HEU payment analysis tables
            'HEU_payments_analysis_ADG': report_vars.get('HEU_ADG_paym_analysis_table'),
            'HEU_payments_analysis_COG': report_vars.get('HEU_COG_paym_analysis_table'),
            'HEU_payments_analysis_EXPERTS': report_vars.get('HEU_EXPERTS_paym_analysis_table'),
            'HEU_payments_analysis_POC': report_vars.get('HEU_POC_paym_analysis_table'),
            'HEU_payments_analysis_STG': report_vars.get('HEU_STG_paym_analysis_table'),
            'HEU_payments_analysis_SYG': report_vars.get('HEU_SYG_paym_analysis_table'),
            'HEU_payments_analysis_ALL': report_vars.get('HEU_all_paym_analysis_table'),
            
            # ⏱️ Time to pay tables
            'TTP_Overview': report_vars.get('TTP_performance_summary_table'),
            'HEU_TTP_FP': report_vars.get('HEU_FP_ttp_chart'),
            'HEU_TTP_IP': report_vars.get('HEU_IP_ttp_chart'),
            'HEU_TTP_PF': report_vars.get('HEU_PF_ttp_chart'),
            'HEU_TTP_EXPERTS': report_vars.get('HEU_EXPERTS_ttp_chart'),
            'H2020_TTP_FP': report_vars.get('H2020_FP_ttp_chart'),
            'H2020_TTP_IP': report_vars.get('H2020_IP_ttp_chart'),
        }
        
        # Filter out None values (following your exact pattern)
        financial_data = {k: v for k, v in financial_data.items() if v is not None}
        
        return financial_data

    