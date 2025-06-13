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
        # 'llama2:13b': {
        #     'name': 'Llama 2 13B',
        #     'description': 'Alternative model for general reports',
        #     'temperature': 0.4,
        #     'max_tokens_multiplier': 1.3,
        #     'recommended_for': ['general', 'narrative']
        # }
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
        'granting_process_overview',
        'commitment_budgetary',
        'fdi_status',
        'payments_workflow',
        'commitments_workflow',
        'amendments_workflow',
        'audit_workflow'
    ]
    
    # 🔄 Loop Configuration
    LOOP_PROGRAMS = ['HEU', 'H2020']
    LOOP_CALL_TYPES = ['STG', 'ADG', 'POC', 'COG', 'SYG']
    
    # 📋 Detailed Call Type Definitions
        # 📋 Call Type Details (corrected - you'll fix the mappings)
    DETAILED_CALL_TYPES = [
        {'code': 'STG', 'description': 'Starting Grant - Early career researchers', 'abbreviation': 'STG'},
        {'code': 'ADG', 'description': 'Advanced Grant - Established researchers', 'abbreviation': 'ADG'},
        {'code': 'COG', 'description': 'Consolidator Grant - Mid-career researchers', 'abbreviation': 'COG'},
        {'code': 'POC', 'description': 'Proof of Concept - Commercialization support', 'abbreviation': 'POC'},
        {'code': 'SYG', 'description': 'Synergy Grant - Collaborative research teams', 'abbreviation': 'SYG'},
        {'code': 'CSA', 'description': 'Coordination and Support Action', 'abbreviation': 'CSA'},
        {'code': 'RIA', 'description': 'Research and Innovation Action', 'abbreviation': 'RIA'},
        {'code': 'IA', 'description': 'Innovation Action', 'abbreviation': 'IA'},
    ]

    ACRONYMS_DICTIONARY = {
        # 🎯 Call Types (from your PaymentsModule CALLS_TYPES_LIST)
        'STG': {'full_name': 'Starting Grant', 'category': 'call_type', 'description': 'ERC Starting Grants for early-career researchers'},
        'ADG': {'full_name': 'Advanced Grant', 'category': 'call_type', 'description': 'ERC Advanced Grants for established researchers'},
        'POC': {'full_name': 'Proof of Concept', 'category': 'call_type', 'description': 'ERC Proof of Concept grants for commercialization'},
        'COG': {'full_name': 'Consolidator Grant', 'category': 'call_type', 'description': 'ERC Consolidator Grants for researchers 7-12 years post-PhD'},
        'SYG': {'full_name': 'Synergy Grant', 'category': 'call_type', 'description': 'ERC Synergy Grants for collaborative research teams'},
        'CSA': {'full_name': 'Coordination and Support Action', 'category': 'call_type', 'description': 'Supporting and coordination measures'},
        'RIA': {'full_name': 'Research and Innovation Action', 'category': 'call_type', 'description': 'Primary research and innovation funding instrument'},
        'IA': {'full_name': 'Innovation Action', 'category': 'call_type', 'description': 'Innovation activities closer to market'},
        'EXPERTS': {'full_name': 'Expert Services', 'category': 'call_type', 'description': 'Expert evaluation and support services'},
        
        # 📊 Programs (from your financial_data mappings)
        'H2020': {'full_name': 'Horizon 2020', 'category': 'program', 'description': 'EU Research and Innovation Framework Programme 2014-2020'},
        'HEU': {'full_name': 'Horizon Europe', 'category': 'program', 'description': 'EU Research and Innovation Framework Programme 2021-2027'},
        'HORIZON': {'full_name': 'Horizon Europe', 'category': 'program', 'description': 'Alternative reference to Horizon Europe programme'},
        
        # ⏱️ Time Metrics (from your PaymentsModule)
        'TTP': {'full_name': 'Time to Pay', 'category': 'time_metric', 'description': 'Processing time from payment request to actual payment'},
        'TTG': {'full_name': 'Time to Grant', 'category': 'time_metric', 'description': 'Processing time from proposal submission to grant decision'},
        'TTS': {'full_name': 'Time to Sign', 'category': 'time_metric', 'description': 'Time from grant decision to grant agreement signature'},
        'TTA': {'full_name': 'Time to Amend', 'category': 'time_metric', 'description': 'Processing time for grant agreement amendments'},
        
        # 💰 Payment Types (from your financial data)
        'FP': {'full_name': 'Final Payment', 'category': 'payment_type', 'description': 'Final payment at project completion'},
        'IP': {'full_name': 'Interim Payment', 'category': 'payment_type', 'description': 'Periodic payments during project implementation'},
        'PF': {'full_name': 'Pre-financing', 'category': 'payment_type', 'description': 'Initial payment made upon grant agreement signature'},
        
        # 🏛️ Organizations (from your PaymentsModule)
        'REA': {'full_name': 'Research Executive Agency', 'category': 'organization', 'description': 'EU executive agency managing research funding'},
        'EACEA': {'full_name': 'European Education and Culture Executive Agency', 'category': 'organization', 'description': 'EU agency for education and culture programs'},
        'ERC': {'full_name': 'European Research Council', 'category': 'organization', 'description': 'EU funding body for frontier research'},
        'ERCEA': {'full_name': 'European Research Council Executive Agency', 'category': 'organization', 'description': 'Executive agency implementing ERC grants'},
        
        # 🔍 Audit and Recovery (from your financial_data)
        'AURI': {'full_name': 'Audit and Recovery Implementation', 'category': 'audit', 'description': 'EU audit and financial recovery processes'},
        'CFS': {'full_name': 'Certificate on Financial Statements', 'category': 'audit', 'description': 'Required audit certificate for large grants'},
        
        # 📋 Document Types
        'MGA': {'full_name': 'Model Grant Agreement', 'category': 'document', 'description': 'Standard EU grant agreement template'},
        'GA': {'full_name': 'Grant Agreement', 'category': 'document', 'description': 'Legal contract between EU and beneficiaries'},
        'PTR': {'full_name': 'Periodic Technical Report', 'category': 'document', 'description': 'Regular technical progress reports'},
        'ESR': {'full_name': 'Evaluation Summary Report', 'category': 'document', 'description': 'Summary of project evaluation results'},
        
        # 🏢 Administrative 
        'PIC': {'full_name': 'Participant Identification Code', 'category': 'administrative', 'description': '9-digit unique identifier for organizations'},
        'LEAR': {'full_name': 'Legal Entity Appointed Representative', 'category': 'administrative', 'description': 'Person authorized to represent organization legally'},
        'PO': {'full_name': 'Purchase Order', 'category': 'administrative', 'description': 'Procurement order reference'},
        'SAP': {'full_name': 'Systems, Applications, Products', 'category': 'system', 'description': 'Enterprise resource planning system'},
        'ABAC': {'full_name': 'Accruals Based Accounting', 'category': 'system', 'description': 'Old EU Commission budgetary system'},
        'SUMMA': {'full_name': "‘Summa de arithmetica, geometria, proportioni et proportionalita’ (Summary of Arithmetic, Geometry, Proportions and Proportionality) is a book on mathematics by Luca Pacioli, first published in 1494. It is the first printed work on algebra and contains the first published description of the double-entry bookkeeping system. It set a new standard for writing and argumentation about algebra, and its impact upon the subsequent development and standardisation of professional accounting methods was so great that Pacioli is sometimes called the 'father of accounting'.", 'category': 'system', 'description': "Current EU Commission budgetary system, SUMMA is a state-of-the-art system that us​​h​​ers in a new era in EU accounting, finance and budgeting, supporting key day-to-day activities in a more efficient and ​simplified way. SUMMA contributes to the rationalisation and modernisation of the EU administration and to a sound EC corporate IT landscape, in line with the EU's Digital Strategy. It is based on a commercial off-the-shelf software, namely S​​AP S/4HANA, adapted to the specific needs of the European Institutions. "},
        'Project FDI':{'full_name':'Project final date for implementation', 'description':'max date for executing a payment for implementing a grant contract'},
        
        # 🌍 Geographical/Political
        'EU': {'full_name': 'European Union', 'category': 'organization', 'description': 'Political and economic union of European countries'},
        'EC': {'full_name': 'European Commission', 'category': 'organization', 'description': 'Executive branch of the European Union'},
        
        # 📊 Financial Terms
        'FDI': {'full_name': 'Foreign Direct Investment', 'category': 'financial', 'description': 'Investment threshold for certain grants'},
        'VAT': {'full_name': 'Value Added Tax', 'category': 'financial', 'description': 'European consumption tax'},
    }
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
            # Detect acronyms in the data for AI context
            detected_acronyms = self._detect_acronyms_in_data(financial_data)
            acronym_context = self.create_acronym_context_for_ai(detected_acronyms)  # ✅ Fixed reference
            print(f"📝 Detected {len(detected_acronyms)} acronyms for AI context")

        except Exception as e:
            error_msg = f"Financial data loading failed: {str(e)}"
            module_errors.append(error_msg)
            print(f"❌ {error_msg}")
            return ctx

        # ══════════════════════════════════════════════════════════════════
        # 3. SINGLE SECTIONS GENERATION - WORKFLOW 1
        # ══════════════════════════════════════════════════════════════════
        """
        SINGLE_SECTIONS = [
            'intro_summary',         # → intro_summary_text
            'budget_overview',       # → budget_overview_text  
            'payments_workflow',     # → payments_workflow_text
            'commitments_workflow',  # → commitments_workflow_text
            'amendments_workflow',   # → amendments_workflow_text
            'audit_workflow'         # → audit_workflow_text
        ]
        """
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
                        acronym_context=acronym_context,      # ✅ Passed here
                        cutoff_date=cutoff,   
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
        # 4. PREDEFINED CALL TYPE LOOPS GENERATION - WORKFLOW 2
        # ══════════════════════════════════════════════════════════════════
        """
        LOOP_PROGRAMS = ['HEU', 'H2020']
        LOOP_CALL_TYPES = ['STG', 'ADG', 'POC', 'COG', 'SYG']

          SINGLE_SECTIONS = [
            'payment_analysis', 
            'call_type_payment_detail', 
            'auto_call_type_detail'
        ]

        """

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
                acronym_context=acronym_context,      # ✅ Passed here
                cutoff_date=cutoff,   
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
        # 5. DETAILED CALL TYPE GENERATION - WORKFLOW 3 (Optional)
        # ══════════════════════════════════════════════════════════════════
        
        """
        # Only runs if enabled
        if report_params.get('enable_detailed_call_types', False):
            
            DETAILED_CALL_TYPES = [
                {'code': 'STG', 'description': 'Starting Grant - Early career researchers'},
                {'code': 'ADG', 'description': 'Advanced Grant - Established researchers'},
                {'code': 'COG', 'description': 'Consolidator Grant - Mid-career researchers'},
                # ... more detailed definitions
            ]
        """
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
                    acronym_context=acronym_context,      # ✅ Passed here
                    cutoff_date=cutoff,   
                    verbose=True
                )
                
                #💾 Process and save detailed results
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
        total_failed = single_section_stats['failed'] + loop_stats['failed']  # ✅ Now will be used
        total_variables = single_section_stats['variables_created'] + loop_stats['variables_created']

        # ✅ FIXED: Now using total_failed in the completion logic
        if module_errors:
            print(f"⚠️ Module completed with {len(module_errors)} errors:")
            for i, error in enumerate(module_errors, 1):
                print(f"   {i}. {error}")
            
            if module_warnings:
                print(f"\n⚠️ Additional warnings ({len(module_warnings)}):")
                for i, warning in enumerate(module_warnings, 1):
                    print(f"   {i}. {warning}")
                    
            print(f"\n❌ Module status: COMPLETED WITH ERRORS")
            print(f"📊 Partial results: {total_successful} successful, {total_failed} failed")  # ✅ Now used
            
        elif module_warnings:
            print(f"✅ Module completed with {len(module_warnings)} warnings:")
            for i, warning in enumerate(module_warnings, 1):
                print(f"   {i}. {warning}")
            print(f"\n⚠️ Module status: COMPLETED WITH WARNINGS")
            print(f"📊 Results: {total_successful} successful, {total_failed} failed")  # ✅ Now used
            
        else:
            print("✅ All AI generation completed successfully!")
            print("\n🎉 Module status: FULLY SUCCESSFUL")
            if total_failed > 0:  # ✅ Show failed count even in success case
                print(f"📊 Final results: {total_successful} successful, {total_failed} failed")

        # Detailed statistics
        print(f"\n📊 GENERATION STATISTICS:")
        print(f"   📝 Single sections: {single_section_stats['successful']} successful, {single_section_stats['failed']} failed")
        print(f"   🔄 Loop combinations: {loop_stats['successful']} successful, {loop_stats['failed']} failed")
        print(f"   💾 Total variables created: {len(total_variables)}")
        print(f"   📈 Overall success rate: {total_successful}/{total_successful + total_failed} ({(total_successful/(total_successful + total_failed)*100):.1f}%)")  # ✅ Now used
        print(f"   🤖 AI Model used: {model_config['name']}")
        print(f"   🌡️ Temperature: {temperature}")

        # Show some created variables
        if total_variables:
            print(f"\n📋 CREATED VARIABLES (showing first 10):")
            for var in total_variables[:10]:
                print(f"   • {var}")
            if len(total_variables) > 10:
                print(f"   ... and {len(total_variables) - 10} more")

        # ✅ NEW: Show failure summary if there were failures
        if total_failed > 0:
            print(f"\n⚠️ FAILURE SUMMARY:")
            print(f"   📝 Single section failures: {single_section_stats['failed']}")
            print(f"   🔄 Loop generation failures: {loop_stats['failed']}")
            print(f"   💡 Check logs above for specific error details")

        print("="*60)
        print("🏁 AI Comments Module completed")
        print("="*60)
    
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
    
    def _detect_acronyms_in_data(self, financial_data: Dict[str, Any]) -> List[str]:
        """Detect which acronyms from our dictionary appear in the financial data"""
        detected_acronyms = set()
        
        # Check data keys for acronyms
        for key in financial_data.keys():
            for acronym in CommentsConfig.ACRONYMS_DICTIONARY.keys():  # ✅ Fixed reference
                if acronym in key.upper():
                    detected_acronyms.add(acronym)
        
        # Add common acronyms that are always relevant
        always_include = ['H2020', 'HEU', 'TTP', 'TTG', 'TTS', 'STG', 'ADG', 'COG', 'POC', 'SYG', 'CSA']
        detected_acronyms.update(always_include)
        
        return sorted(list(detected_acronyms))
    
    def create_acronym_context_for_ai(self, detected_acronyms: List[str]) -> str:
        """
        Create a context string with acronym definitions for AI generation
        This provides the AI with essential acronym knowledge for generating accurate commentary
        """
        if not detected_acronyms:
            return ""
        
        context_lines = [
            "📚 ACRONYMS REFERENCE (for accurate text generation):",
            "=" * 60,
            ""
        ]
        
        # Group acronyms by category for better organization
        acronyms_by_category = {}
        for acronym in detected_acronyms:
            if acronym in CommentsConfig.ACRONYMS_DICTIONARY:
                definition = CommentsConfig.ACRONYMS_DICTIONARY[acronym]
                category = definition.get('category', 'general')
                
                if category not in acronyms_by_category:
                    acronyms_by_category[category] = []
                
                acronyms_by_category[category].append({
                    'acronym': acronym,
                    'full_name': definition.get('full_name', ''),
                    'description': definition.get('description', '')
                })
        
        # Format by category
        category_order = [
            'program', 'call_type', 'payment_type', 'time_metric', 
            'organization', 'audit', 'financial', 'administrative', 
            'document', 'system', 'general'
        ]
        
        for category in category_order:
            if category in acronyms_by_category:
                # Category header
                category_title = category.replace('_', ' ').title()
                context_lines.extend([
                    f"🎯 {category_title}:",
                    ""
                ])
                
                # List acronyms in this category
                for item in sorted(acronyms_by_category[category], key=lambda x: x['acronym']):
                    acronym = item['acronym']
                    full_name = item['full_name']
                    description = item['description']
                    
                    if description:
                        context_lines.append(f"   • {acronym}: {full_name} - {description}")
                    else:
                        context_lines.append(f"   • {acronym}: {full_name}")
                
                context_lines.append("")  # Empty line between categories
        
        # Add usage instructions for AI
        context_lines.extend([
            "📝 AI INSTRUCTIONS:",
            "• Always use full names on first mention: 'Horizon Europe (HEU)'",
            "• Use acronyms consistently thereafter: 'HEU payments', 'HEU analysis'",
            "• Ensure technical accuracy when referencing these terms",
            "• Explain context when introducing complex acronyms",
            "=" * 60,
            ""
        ])
        
        return "\n".join(context_lines)
    
    def _validate_ai_model_configuration(self, model: str, temperature: float) -> Dict[str, Any]:
        """
        Validate and return AI model configuration
        Returns model config with fallbacks if needed
        """
        # Check if model exists
        if model not in CommentsConfig.AVAILABLE_MODELS:
            print(f"⚠️ Unknown model '{model}', using default: {CommentsConfig.DEFAULT_MODEL}")
            model = CommentsConfig.DEFAULT_MODEL
        
        # Validate temperature range
        if not (0.0 <= temperature <= 1.0):
            print(f"⚠️ Invalid temperature {temperature}, using default: {CommentsConfig.DEFAULT_TEMPERATURE}")
            temperature = CommentsConfig.DEFAULT_TEMPERATURE
        
        model_config = CommentsConfig.AVAILABLE_MODELS[model].copy()
        model_config['temperature'] = temperature
        model_config['model_key'] = model
        
        return model_config



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

def manage_acronyms():
    """Utility function to manage the acronym dictionary"""
    
    print("📚 ACRONYM DICTIONARY MANAGEMENT")
    print("=" * 40)
    
    print(f"Total acronyms: {len(CommentsConfig.ACRONYMS_DICTIONARY)}")  # ✅ Fixed reference
    
    # Show by category
    categories = set(details.get('category', 'other') for details in CommentsConfig.ACRONYMS_DICTIONARY.values())  # ✅ Fixed reference
    
    for category in sorted(categories):
        category_acronyms = CommentsModule.get_acronyms_by_category(category)  # ✅ Use class method
        print(f"\n{category.upper()} ({len(category_acronyms)}):")
        for acronym, details in sorted(category_acronyms.items()):
            print(f"  {acronym}: {details.get('full_name', 'N/A')}")
    
    print("\n🔧 To add new acronyms, update ACRONYMS_DICTIONARY in CommentsConfig")
    print("🔧 Categories: call_type, program, time_metric, payment_type, organization, audit, document, administrative, system, financial")


def test_program_mapping():
    """Test the enhanced PROGRAM_MAPPING functionality"""
    
    print("🧪 TESTING ENHANCED PROGRAM MAPPING")
    print("=" * 40)
    
    # Test program information
    programs = CommentsConfig.get_available_programs()
    print(f"Available programs: {programs}")
    
    for program in programs:
        print(f"\n📊 {program} Configuration:")
        config = CommentsConfig.get_program_info(program)
        print(f"   Official name: {config.get('official_name')}")
        print(f"   Period: {config.get('period')}")
        print(f"   Call types: {config.get('call_types')}")
        
        # Show data fields by category
        payment_fields = CommentsConfig.get_program_data_fields(program, 'payment_fields')
        analysis_fields = CommentsConfig.get_program_data_fields(program, 'analysis_tables')
        ttp_fields = CommentsConfig.get_program_data_fields(program, 'ttp_fields')
        
        print(f"   Payment fields ({len(payment_fields)}): {payment_fields}")
        print(f"   Analysis fields ({len(analysis_fields)}): {analysis_fields}")
        print(f"   TTP fields ({len(ttp_fields)}): {ttp_fields}")
    
    # Test alias resolution
    print(f"\n🔍 Alias Resolution:")
    test_aliases = ['HEU', 'Horizon Europe', 'H2020', 'Horizon 2020', 'HORIZON']
    for alias in test_aliases:
        resolved = CommentsConfig.find_program_by_alias(alias)
        print(f"   '{alias}' → {resolved}")
    
    # Test financial data filtering
    print(f"\n📊 Financial Data Filtering:")
    sample_financial_data = {
        'commitments': 'data1',
        'pay_credits_HEU': 'data2', 
        'HEU_payments_analysis_STG': 'data3',
        'H2020_payments_all': 'data4',
        'TTP_Overview': 'data5',
        'auri_overview': 'data6'
    }
    
    for program in programs:
        filtered = CommentsConfig.filter_financial_data_by_program(sample_financial_data, program)
        print(f"   {program} relevant data: {list(filtered.keys())}")
    
    return True

def test_data_field_mapping():
    """Test the data field mapping functionality"""
    
    print("🧪 TESTING DATA FIELD MAPPING")
    print("=" * 40)
    
    mapping = CommentsConfig.get_data_field_mapping()
    
    print(f"Total mapped fields: {len(mapping)}")
    
    # Group by category for display
    categories = {
        'Core Tables': ['commitments', 'pay_credits_H2020', 'pay_credits_HEU', 'summary_budget'],
        'Payment Data': [k for k in mapping.keys() if 'payments_' in k and 'analysis' not in k],
        'Analysis Tables': [k for k in mapping.keys() if 'analysis' in k],
        'TTP Charts': [k for k in mapping.keys() if 'TTP_' in k],
        'Amendment Data': [k for k in mapping.keys() if 'amendment_' in k],
        'Audit Data': [k for k in mapping.keys() if 'auri_' in k or k in ['recovery_activity', 'external_audits_activity', 'error_rates']]
    }
    
    for category, fields in categories.items():
        if fields:
            print(f"\n📊 {category} ({len(fields)}):")
            for field in fields[:3]:  # Show first 3 as examples
                if field in mapping:
                    print(f"   {field} → {mapping[field]}")
            if len(fields) > 3:
                print(f"   ... and {len(fields) - 3} more")
    
    return mapping

def test_acronym_detection():
    """Test acronym detection functionality"""
    
    print("🧪 TESTING ACRONYM DETECTION")
    print("=" * 40)
    
    # Sample financial data keys (from your actual mappings)
    sample_data = {
        'pay_credits_H2020': 'some_data',
        'HEU_payments_STG': 'some_data',
        'TTP_Overview': 'some_data',
        'amendment_TTA_H2020': 'some_data',
        'auri_overview': 'some_data',
    }
    
    module = CommentsModule()
    detected = module._detect_acronyms_in_data(sample_data)
    context = module.create_acronym_context_for_ai(detected)
    
    print(f"Sample data keys: {list(sample_data.keys())}")
    print(f"Detected acronyms: {detected}")
    print(f"\nGenerated context for AI:")
    print("=" * 40)
    print(context)
    print("=" * 40)
    
    return detected, context

def test_ai_model_validation():
    """Test AI model validation functionality"""
    
    print("🧪 TESTING AI MODEL VALIDATION")
    print("=" * 40)
    
    module = CommentsModule()
    
    # Test valid model
    config1 = module._validate_ai_model_configuration('deepseek-r1:14b', 0.3)
    print(f"Valid model: {config1['model_key']} → {config1['name']}")
    
    # Test invalid model (should fallback)
    config2 = module._validate_ai_model_configuration('invalid-model', 0.3)
    print(f"Invalid model fallback: {config2['model_key']} → {config2['name']}")
    
    # Test invalid temperature (should fallback)
    config3 = module._validate_ai_model_configuration('deepseek-r1:14b', 1.5)
    print(f"Invalid temperature fallback: temp={config3['temperature']}")
    
    return config1, config2, config3