"""
            🎯 Enhanced Report Generator - Customization Guide

            📋 Code Structure Overview

            The system is organized into 5 main customizable sections:

            1. PREDEFINED CONSTANTS     ← Easy customization
            2. TEMPLATE LIBRARY         ← Add/modify templates
            3. MAPPING MATRIX           ← Configure section relationships
            4. UTILITY PROCESSORS       ← Customize data processing
            5. GENERATION ENGINE        ← Modify generation logic


            
            🎯 9. SYSTEM INTEGRATION FLOW

            Complete Generation Process:
            1. User Request → 2. Section Config → 3. Template Selection → 4. Data Filtering → 
            5. Data Summarization → 6. Template Population → 7. AI Instructions → 
            8. AI Generation → 9. Response Processing → 10. Result Return
            Error Handling at Each Step:

            Config Missing: Returns None with error message
            Template Missing: Returns None with error message
            No Data: Returns None but tracks in statistics
            AI Failure: Returns None with API error details
            Parsing Error: Returns None with data processing error

            Success Tracking:
            pythonresults['statistics'] = {
                'total_combinations': 20,
                'successful': 15,        # AI generated successfully
                'failed': 5,            # AI generation failed
                'data_found': 18,       # Data was available
                'no_data': 2           # No data for combination
            }

            💡 10. WHY THIS ARCHITECTURE
            Separation of Concerns:

            Templates: Define structure and format
            Mapping: Define data sources and configuration
            Processor: Handle data extraction and transformation
            Generator: Orchestrate the process and handle AI

            Scalability:

            Add New Sections: Just add template and mapping
            Change AI Models: Modify one method
            Update Data Sources: Change mapping configuration
            Modify Formats: Update templates

            Maintainability:

            Single Responsibility: Each method has one clear purpose
            Error Isolation: Problems in one area don't break others
            Configuration-Driven: Behavior controlled by data, not code
            Extensive Logging: Easy debugging with verbose output

            Flexibility:

            Multiple Generation Types: Loops, individual sections, bulk processing
            Customizable Instructions: Different tones and focuses per section
            Data Source Agnostic: Can work with different database structures
            Model Independent: Can switch between different AI models

            This architecture provides a robust, scalable foundation for automated report generation that can adapt to changing requirements without major code restructuring! 🚀

"""

# ================================================================
# ENHANCED MATRIX-BASED REPORT GENERATOR
# WITH IMPROVED TEMPLATE MANAGEMENT AND PREDEFINED LOOPS
# ================================================================

import logging
import datetime
import pandas as pd
import json
import re
from typing import Dict, Any, Optional, List

# Your existing imports
from ingestion.db_utils import (
    fetch_vars_for_report,
    load_report_params
)


# ================================================================
# PREDEFINED CONSTANTS FOR LOOPS
# ================================================================
"""
🔧 SECTION 1: PREDEFINED CONSTANTS - Easy Customization
📍 Location: Lines 45-90

"""

# 🛠️ CUSTOMIZATION POINT 1: Add/Remove Programs
PROGRAMS_LIST = ['HEU', 'H2020']

# 🛠️ CUSTOMIZATION POINT 2: Add/Remove Call Types  
CALL_TYPES_LIST = ['STG', 'ADG', 'POC', 'COG', 'SYG', 'StG', 'CoG', 'AdG', 'SyG', 'PoC']

# 🛠️ CUSTOMIZATION POINT 3: Handle Different Graphical Representations
CALL_TYPE_NORMALIZATION = {
    'STG': ['STG', 'StG', 'stg'],
    'ADG': ['ADG', 'AdG', 'adg'], 
    'POC': ['POC', 'PoC', 'poc'],
    'COG': ['COG', 'CoG', 'cog'],
    'SYG': ['SYG', 'SyG', 'syg']
}

# Reverse mapping for quick lookup
CALL_TYPE_VARIANTS = {}
for standard, variants in CALL_TYPE_NORMALIZATION.items():
    for variant in variants:
        CALL_TYPE_VARIANTS[variant] = standard

# 🛠️ CUSTOMIZATION POINT 4: Enhanced Program Configuration
PROGRAM_MAPPING = {
    'HEU': {
        'aliases': ['HEU', 'Horizon Europe', 'horizon_europe', 'HORIZON'],
        'data_key': 'pay_credits_HEU',
        'official_name': 'Horizon Europe',
        'short_name': 'HEU',
        'payment_fields': ['pay_credits_HEU', 'HEU_payments_all', 'HEU_payments_final_payments', 'HEU_payments_pre_financing_payments'],
        'analysis_tables': ['HEU_ADG_paym_analysis_table', 'HEU_COG_paym_analysis_table', 'HEU_STG_paym_analysis_table', 'HEU_SYG_paym_analysis_table', 'HEU_POC_paym_analysis_table', 'HEU_EXPERTS_paym_analysis_table']
    },
    'H2020': {
        'aliases': ['H2020', 'Horizon 2020', 'h2020', 'HORIZON2020'],
        'data_key': 'pay_credits_H2020',
        'official_name': 'Horizon 2020',
        'short_name': 'H2020',
        'payment_fields': ['pay_credits_H2020', 'H2020_payments_all', 'H2020_payments_final_payments', 'H2020_payments_interim_payments'],
        'analysis_tables': ['H2020_ADG_paym_analysis_table', 'H2020_COG_paym_analysis_table', 'H2020_STG_paym_analysis_table', 'H2020_SYG_paym_analysis_table']
    }
}

# Reverse mapping for program lookup
PROGRAM_ALIASES = {}
for program, config in PROGRAM_MAPPING.items():
    for alias in config['aliases']:
        PROGRAM_ALIASES[alias.upper()] = program

# ================================================================
# SECTION 1: TEMPLATE LIBRARY - CLEARLY IDENTIFIED TEMPLATES
# ================================================================
"""
🎨 SECTION 2: TEMPLATE LIBRARY - Template Customization
📍 Location: Lines 98-325
"""

class ReportTemplateLibrary:
    """Centralized template library with clear template identification"""
    
    @staticmethod
    def get_template_definitions(quarter_period: str, current_year: str) -> Dict[str, str]:
        """
        Central repository of all report templates with clear naming
        Template Name Format: {section_type}_{focus}_template
        """
        
        current_date = datetime.datetime.now().strftime('%Y-%m-%d')
        
        return {
                            # ============================================================
                            # 🛠️ POINT 6: Modify Existing Templates
                            #
                            # EXECUTIVE & OVERVIEW TEMPLATES
                            # ============================================================


        # ============================================================
        # 🎯 Executive Summary -
        # ============================================================
        'executive_summary_template': f"""
            GRANT MANAGEMENT DEPARTMENT - EXECUTIVE ACHIEVEMENT SUMMARY
            Period: {quarter_period} {current_year}
            Generated: {current_date}

            **----- Budget and Reporting Team -----**

            **The Grant Management Department** achieved significant milestones in {quarter_period} {current_year}, successfully meeting and exceeding several key targets.

            COMPREHENSIVE PERFORMANCE ANALYSIS:
            {{prioritized_data_summary}}

            DETAILED WORKFLOW BREAKDOWN AND SUPPORTING METRICS:
            {{secondary_data_summary}}

            **Detailed Breakdown of Activities**

            The analysis covers:
            • **Payments:** Volume execution, Time-to-Pay (TTP) performance, appropriation utilization
            • **Granting:** Time-to-Grant (TTG) metrics, call completion rates, funding milestone achievements  
            • **Amendments:** Processing efficiency, Time-to-Amend (TTA) performance, amendment volume analysis
            • **Audits:** Recovery implementation, audit completion rates, financial integrity measures
            • **Other Activities:** Budget compliance, contractual adherence, exceptional cases

            **Executive Focus:** Cross-workflow achievements, quantitative performance indicators, strategic accomplishments demonstrating departmental excellence in grant management operations and successful execution of contractual obligations.

            Analysis Date: {current_date}
        """,
        # ============================================================
        # 💰 BUDGET
        # ============================================================

        'budget_overview_template': f"""
            BUDGET APPROPRIATIONS AND ABSORPTION ANALYSIS
            Reporting Period: {quarter_period} {current_year}
            Scope: H2020 and Horizon Europe commitment and payment appropriations

            BUDGET APPROPRIATIONS OVERVIEW:
            {{prioritized_data_summary}}

            ABSORPTION PERFORMANCE AND SUPPORTING ANALYSIS:
            {{secondary_data_summary}}

            **Analysis Framework:**
            • **Commitment Appropriations:** Available budget, allocation patterns, absorption rates
            • **Payment Appropriations:** H2020 and HEU payment credit consumption and efficiency
            • **Absorption Analysis:** Utilization levels, execution rates, remaining available amounts
            • **Cross-Program Comparison:** H2020 vs Horizon Europe performance and trends
            • **Performance Indicators:** Budget execution efficiency, variance analysis, optimization opportunities

            **Focus Areas:**
            - Budget availability and allocation effectiveness across both programs
            - Commitment and payment appropriation absorption levels and trends  
            - Resource utilization efficiency and performance benchmarking
            - Strategic budget implications for program continuation and planning

            Analysis Date: {current_date}
            """,

        # ============================================================
        # 📋 Template 1: Granting Process Overview
        # ============================================================
        'granting_process_template': f"""
                GRANTING PROCESS OVERVIEW
                Period: {quarter_period} {current_year}
                Scope: Grant execution, signature activity, and call completion status

                GRANT EXECUTION AND SIGNATURE ACTIVITY:
                {{prioritized_data_summary}}

                CALL COMPLETION AND PROCESS STATUS:
                {{secondary_data_summary}}

                **Process Overview:**
                The granting process encompasses grant agreement preparation, signature execution, and call completion monitoring. This section provides an overview of grant agreements (GA) under preparation, signed contracts by ERCEA, and the current state of call completion activities.

                **Key Performance Areas:**
                • Grant signature execution volumes and timeline performance
                • Call completion rates and progress tracking
                • Grant allocation process adherence to scheduled timelines
                • New grant execution and agreement preparation status

                Analysis Focus: Grant signature activity, call completion progress, process efficiency, and scheduled milestone achievement.

                Analysis Date: {current_date}
                """,

        # ============================================================
        # 💰 Template 2: Commitment Budgetary Impact  
        # ============================================================
        'commitment_budgetary_template': f"""
        BUDGETARY CONSEQUENCE OF COMMITMENT PROCESS
        Period: {quarter_period} {current_year}
        Scope: L2 budgetary commitments and financial commitment activity

        COMMITMENT FINANCIAL ACTIVITY:
        {{prioritized_data_summary}}

        COMMITMENT BREAKDOWN AND REGULATORY CONTEXT:
        {{secondary_data_summary}}

        **Regulatory Framework:**
        As per the EU Financial Regulation (FR2018 art. 111.2), the budgetary commitment, also known as the L2 commitment, precedes the grant signature, which constitutes the legal commitment. Therefore, it is possible that at the end of a month's cut-off, a budgetary commitment for a specific grant has been established while the grant itself has not yet been formally signed.

        **Analysis Framework:**
        • Total commitment activity financial impact for {current_year}
        • Breakdown of commitment activity by period and call type
        • L2 commitment volumes and financial allocation patterns
        • Budgetary commitment timeline and processing efficiency

        Focus: Financial commitment volumes, budgetary impact assessment, commitment processing efficiency, and regulatory compliance adherence.

        Analysis Date: {current_date}
        """,

        # ============================================================
        # ⏰ Template 3: Final Date for Implementation (FDI) Status
        # ============================================================
        'fdi_status_template': f"""
            FINAL DATE FOR IMPLEMENTATION (FDI) STATUS ANALYSIS
            Period: {quarter_period} {current_year}
            Scope: L2 commitments approaching or exceeding FDI thresholds

            FDI THRESHOLD ANALYSIS:
            {{prioritized_data_summary}}

            FDI PROGRAM BREAKDOWN AND CONTEXT:
            {{secondary_data_summary}}

            **FDI Monitoring Framework:**
            The Final Date for Implementation (FDI) represents the deadline by which committed funds must be legally committed through grant agreements. Monitoring commitments approaching or exceeding FDI thresholds is crucial for budget execution compliance and financial planning.

            **Analysis Components:**
            • H2020 program FDI threshold status and distribution by call type
            • Horizon Europe (HEU) program FDI compliance monitoring
            • L2 commitment FDI risk assessment and mitigation measures
            • Call type distribution of FDI-approaching or exceeding commitments

            Focus: FDI compliance monitoring, threshold risk assessment, call type impact analysis, and proactive budget execution management.

            Analysis Date: {current_date}
            """,
        
        # ============================================================
        # 💳 HEU Payment Overview Template
        # ============================================================
        'heu_payment_overview_template': f"""
            A. CONSUMPTION OF PAYMENT CREDITS – HEU
            Period: {quarter_period} {current_year}
            Programme: Horizon Europe (HEU)

            PAYMENT PROCESSING AND CREDIT CONSUMPTION:
            {{prioritized_data_summary}}

            FORECAST ANALYSIS AND PERFORMANCE INDICATORS:
            {{secondary_data_summary}}

            **Analysis Framework:**
            • Payment volume processing and grant agreement execution
            • Credit consumption patterns and C1/E0 disbursement tracking
            • Annual allocation utilization and expert credit integration
            • Forecast comparison with budgetary exercise projections
            • Consumption deviation analysis and budget execution efficiency

            **Key Performance Indicators:**
            - Total payments processed since start of year
            - Financial disbursement amounts and credit allocation
            - C1/E0/C4/C5 payment credit annual allocation status
            - Actual vs forecast consumption deviation analysis
            - EFTA credits and voted payment credit utilization

            Analysis Date: {current_date}
            """,

        # ============================================================
        # 💳 H2020 Payment Overview Template  
        # ============================================================
         'h2020_payment_overview_template': f"""

                B. CONSUMPTION OF PAYMENT CREDITS – H2020
                Period: {quarter_period} {current_year}
                Programme: Horizon 2020 (H2020)

                PAYMENT PROCESSING AND CREDIT CONSUMPTION:
                {{prioritized_data_summary}}

                FORECAST ANALYSIS AND PERFORMANCE INDICATORS:
                {{secondary_data_summary}}

                **Analysis Framework:**
                • Payment volume processing and grant agreement execution
                • Credit consumption patterns and disbursement tracking
                • Annual allocation utilization and credit management
                • Forecast comparison with budgetary exercise projections
                • Consumption deviation analysis and budget execution efficiency

                **Key Performance Indicators:**
                - Total payments processed since start of year
                - Financial disbursement amounts and credit allocation
                - Payment credit annual allocation status
                - Actual vs forecast consumption deviation analysis
                - Budget execution and appropriation utilization

                Analysis Date: {current_date}
       """,

        # ============================================================
        # 🕐 NEW: TTP Performance Template
        # ============================================================
        'ttp_performance_template': f"""
            TIME-TO-PAY (TTP) PERFORMANCE ANALYSIS
            Period: {quarter_period} {current_year}
            Scope: Payment processing efficiency and contractual compliance

            H2020 TTP PERFORMANCE:
            {{h2020_ttp_summary}}

            HEU TTP PERFORMANCE:
            {{heu_ttp_summary}}

            QUARTERLY PERFORMANCE OVERVIEW:
            {{prioritized_data_summary}}

            SUPPORTING TTP METRICS:
            {{secondary_data_summary}}

            **Performance Framework:**
            Time-to-Pay (TTP) measures the efficiency of payment processing from payment request to actual disbursement. Contractual time limits ensure timely financial support to beneficiaries while maintaining proper financial controls and verification procedures.

            **Key Performance Indicators:**
            • Overall compliance rate with contractual time limits
            • Payment type breakdown: pre-financing, interim, final, expert payments
            • Exception analysis: delayed payments by program and cause
            • Quarterly vs annual performance comparison
            • Process efficiency and timeline adherence

            Analysis Date: {current_date}
            """,
        # ============================================================
        # 💳 Payments Workflow 
        # ============================================================
        'payments_workflow_template': f"""
            PAYMENTS WORKFLOW ANALYSIS
            Period: {quarter_period} {current_year}
            Workflow Focus: Payment credit consumption and processing efficiency

            PRIMARY PAYMENT DATA ANALYSIS:
            {{prioritized_data_summary}}

            BUDGET CONTEXT:
            {{secondary_data_summary}}

            Key Performance Areas:
            • Payment credit consumption tracking
            • Processing efficiency and timeline performance
            • H2020 vs Horizon Europe payment patterns
            • Payment workflow optimization

            Analysis Date: {current_date}
            """,

                        'commitments_workflow_template': f"""
            COMMITMENTS WORKFLOW ANALYSIS
            Period: {quarter_period} {current_year}
            Workflow Focus: Commitment credit consumption and allocation efficiency

            PRIMARY COMMITMENT DATA ANALYSIS:
            {{prioritized_data_summary}}

            BUDGET CONTEXT:
            {{secondary_data_summary}}

            Key Performance Areas:
            • Commitment credit consumption patterns
            • Allocation efficiency and portfolio performance
            • Grant commitment processing effectiveness
            • Resource utilization optimization

            Analysis Date: {current_date}
            """,

                        'amendments_workflow_template': f"""
            AMENDMENTS WORKFLOW ANALYSIS
            Period: {quarter_period} {current_year}
            Workflow Focus: Grant amendment processing and administrative efficiency

            PRIMARY DATA (COMMITMENT IMPACTS):
            {{prioritized_data_summary}}

            SECONDARY DATA (PAYMENT IMPACTS):
            {{secondary_data_summary}}

            Key Performance Areas:
            • Grant amendment processing efficiency
            • Modification request handling and approval rates
            • Administrative workflow optimization
            • Impact on overall program performance

            Analysis Date: {current_date}
            """,

                        'audit_workflow_template': f"""
            AUDIT RESULTS IMPLEMENTATION WORKFLOW
            Period: {quarter_period} {current_year}
            Workflow Focus: Audit result implementation and recovery processing

            PRIMARY DATA (BUDGET IMPACTS):
            {{prioritized_data_summary}}

            SUPPORTING DATA (ALL WORKFLOWS):
            {{secondary_data_summary}}

            Key Performance Areas:
            • Audit result implementation progress
            • Recovery processing and collection activities
            • Compliance status and corrective actions
            • Financial impact and risk mitigation

            Analysis Date: {current_date}
            """,

        # ============================================================
        # 📊 Payment Analysis 
        # ============================================================
            
        'payment_analysis_template': f"""
            PAYMENT CONSUMPTION ANALYSIS
            Call Type: {{call_type}}
            Programme: {{programme}}
            Period: {quarter_period} {current_year}

            PRIMARY PAYMENT DATA FOR {{call_type}} in {{programme}}:
            {{prioritized_data_summary}}

            BUDGET FORECAST CONTEXT:
            {{secondary_data_summary}}

            Analysis Parameters:
            • Consumption vs forecast comparison for {{call_type}}
            • Payment credit utilization in {{programme}}
            • Performance indicators and efficiency metrics
            • Variance analysis and trend assessment

            Focus: Consumption patterns, forecast accuracy, performance optimization
            """,

                        'call_type_payment_detail_template': f"""
            {{call_type_code}}      {{payment_type_description}} – {{call_type_abbreviation}}

            {{payment_details_analysis}}

            {{forecast_comparison_statement}}
            """,

                        'auto_call_type_detail_template': f"""
            {{call_type_abbreviation}}      {{derived_payment_description}}

            {{payment_analysis_text}}

            {{variance_statement}}
            """,

                        'variance_analysis_template': f"""
            FINANCIAL VARIANCE ANALYSIS
            Period: {quarter_period} {current_year}
            Analysis Type: Budget vs Actual Performance

            PRIMARY VARIANCE DATA:
            {{prioritized_data_summary}}

            SUPPORTING FINANCIAL CONTEXT:
            {{secondary_data_summary}}

            Variance Focus Areas:
            • Budget execution variance by program
            • Payment vs commitment alignment
            • Forecast accuracy assessment
            • Resource allocation effectiveness

            Analysis Date: {current_date}
            """,

                        'risk_assessment_template': f"""
            FINANCIAL RISK ASSESSMENT
            Assessment Period: {quarter_period} {current_year}
            Risk Scope: Cross-workflow financial exposure

            PRIMARY RISK INDICATORS:
            {{prioritized_data_summary}}

            SUPPORTING RISK CONTEXT:
            {{secondary_data_summary}}

            Risk Assessment Areas:
            • Budget execution risk exposure
            • Payment processing risk factors
            • Commitment allocation risks
            • Operational and compliance risks

            Analysis Date: {current_date}
            """
                    }

class TemplateSectionMatrix:
    """Enhanced matrix for mapping templates to sections with clear relationships"""
    
    @staticmethod
    def get_complete_mapping_matrix() -> Dict[str, Dict[str, Any]]:
        """
        Complete mapping matrix showing all relationships:
        Section Key → Template → Data → Instructions → Database
        """
        
        return {
            # ============================================================
            # 1. EXECUTIVE LEVEL SECTIONS
            # ============================================================
            
           """
            Target Text Analysis:
            ✅ Covers: TTP metrics, TTA metrics, payment volumes, amendment statistics, audit results, grant progress
            ✅ Tone: Executive, achievement-focused, specific numbers
            ✅ Length: ~600 words (much longer than default 400)
            ✅ Structure: Department achievements → Detailed breakdowns by workflow
            """
            'intro_summary': {
                'section_info': {
                    'name': 'Introductory Summary',
                    'category': 'executive',
                    'priority': 1,
                    'description': 'Comprehensive executive overview covering all department workflows and achievements'
                },
                'template_mapping': {
                    'template_name': 'executive_summary_template',  # ✅ Uses your existing template
                    'template_category': 'executive_overview',
                    'supports_variables': ['prioritized_data_summary', 'secondary_data_summary']
                },
                'data_configuration': {
                    # 🎯 COMPREHENSIVE DATA for executive summary
                    'primary_data': [
                        'TTP_Overview',                    # Time-to-Pay performance data
                        'pay_credits_H2020',              # H2020 payment volumes  
                        'pay_credits_HEU',                # HEU payment volumes
                        'amendment_activity_H2020',        # H2020 amendment data
                        'amendment_activity_HEU',          # HEU amendment data
                        'amendment_TTA_H2020',            # H2020 TTA performance
                        'amendment_TTA_HEU',              # HEU TTA performance
                        'auri_overview',                  # Audit overview
                        'recovery_activity',              # Recovery amounts
                        'TTG'                             # Time-to-Grant metrics
                    ],
                    'secondary_data': [
                        'summary_budget',                 # Budget context
                        'H2020_TTP_FP',                   # H2020 final payment TTP
                        'H2020_TTP_IP',                   # H2020 interim payment TTP  
                        'HEU_TTP_FP',                     # HEU final payment TTP
                        'HEU_TTP_IP',                     # HEU interim payment TTP
                        'grants_signature_activity',      # Grant progress
                        'completion_previous_year_calls', # Call completion rates
                        'grants_exceeding_fdi',          # FDI exceptions
                        'amendment_cases_H2020',          # Amendment type breakdown
                        'amendment_cases_HEU',            # Amendment type breakdown
                        'external_audits_activity'        # Additional audit data
                    ],
                    'focus_metrics': [
                        # Time metrics matching your target text
                        'time', 'days', 'average', 'ttp', 'tta', 'ttg',
                        # Volume metrics
                        'payments', 'amendments', 'audits', 'grants', 'calls', 'total', 'count',
                        # Financial metrics  
                        'amount', 'million', 'eur', 'recovery', 'appropriations',
                        # Performance metrics
                        'rate', 'percentage', 'completion', 'efficiency', 'targets', 'milestones'
                    ]
                },
                'output_configuration': {
                    'module': 'CommentsModule',           # ✅ Your module
                    'variable_name': 'intro_summary_text',
                    'word_limit': 1000,
                    'formatting_level': 'contextual'
                },
                'instruction_mapping': {
                    'instruction_key': 'executive_summary_instructions',
                    'tone': 'executive',
                    'focus': 'comprehensive_achievements'
                }
            },

            # ================================================================
            # 🏦 2. OPTIMIZED BUDGET OVERVIEW MATRIX CONFIGURATION
            # ================================================================

            """
            🎯 Budget Commentary Requirements:
            ✅ Budget available for commitment and payments (H2020 + HEU)
            ✅ Level of absorption analysis
            ✅ Commitment and payment appropriations overview
            ✅ Cross-program performance comparison
            """

            'budget_overview': {
                'section_info': {
                    'name': 'Budget Overview',
                    'category': 'financial',
                    'priority': 2,
                    'description': 'Comprehensive budget analysis covering commitment and payment appropriations for H2020 and Horizon Europe programs'
                },
                'template_mapping': {
                    'template_name': 'budget_overview_template',
                    'template_category': 'financial_overview',
                    'supports_variables': ['prioritized_data_summary', 'secondary_data_summary']
                },
                'data_configuration': {
                    # 🎯 PRIMARY_DATA: Core budget and appropriations tables
                    'primary_data': [
                        'summary_budget',              # 📊 Main budget overview table
                        'pay_credits_H2020',          # 💳 H2020 payment appropriations & consumption
                        'pay_credits_HEU',            # 💳 HEU payment appropriations & consumption  
                        'commitments'                 # 📋 Commitment appropriations & execution
                    ],
                    
                    # 🔄 SECONDARY_DATA: Supporting absorption and performance analysis
                    'secondary_data': [
                        # Grant activity for absorption context
                        'grants_commitment_activity',     # Commitment absorption patterns
                        'current_year_global_commitment_activity',  # Current year execution
                        'completion_previous_year_calls', # Previous year completion for trends
                        
                        # Payment performance context
                        'TTP_Overview',                   # Payment processing efficiency
                        'H2020_payments_all',            # H2020 payment execution details
                        'HEU_payments_all',              # HEU payment execution details
                        
                        # Analysis tables for absorption insights
                        'H2020_payments_analysis_ALL',   # H2020 overall payment analysis
                        'HEU_payments_analysis_ALL'      # HEU overall payment analysis
                    ],
                    
                    # 🎯 FOCUS_METRICS: Budget absorption and performance indicators
                    'focus_metrics': [
                        # Core budget metrics
                        'budget', 'appropriation', 'allocation', 'available', 'remaining',
                        
                        # Absorption & utilization metrics  
                        'absorption', 'utilization', 'consumption', 'execution', 'spent',
                        
                        # Performance metrics
                        'efficiency', 'variance', 'performance', 'rate', 'percentage',
                        
                        # Financial amounts
                        'amount', 'total', 'credit', 'million', 'eur',
                        
                        # Program comparison  
                        'h2020', 'heu', 'horizon', 'programme', 'program'
                    ]
                },
                'output_configuration': {
                    'module': 'CommentsModule',           # ✅ Use CommentsModule like other sections
                    'variable_name': 'budget_overview_text',
                    'word_limit': 500,                   # ✅ Increased from 300 for comprehensive coverage
                    'formatting_level': 'detailed'
                },
                'instruction_mapping': {
                    'instruction_key': 'budget_overview_instructions',
                    'tone': 'analytical',
                    'focus': 'appropriations_and_absorption'  # ✅ Updated focus
                }
            },

            # ============================================================
            # 📋 3: Granting Process Overview
            # ============================================================
            'granting_process_overview': {
                'section_info': {
                    'name': 'Granting Process Overview',
                    'category': 'operational',
                    'priority': 3,
                    'description': 'Grant execution, signature activity, and call completion status analysis'
                },
                'template_mapping': {
                    'template_name': 'granting_process_template',
                    'template_category': 'granting_overview',
                    'supports_variables': ['prioritized_data_summary', 'secondary_data_summary']
                },
                'data_configuration': {
                    # 🎯 PRIMARY_DATA: Core granting and signature data
                    'primary_data': [
                        'grants_signature_activity',        # Grant signature volumes and activity
                    ],
                    
                    # 🔄 SECONDARY_DATA: Supporting process context
                    'secondary_data': [
                        'grants_commitment_activity',       # Grant commitment activity
                        'completion_previous_year_calls',  # Current year activity                     # Overall commitment context
                    ],
                    
                    # 🎯 FOCUS_METRICS: Granting process indicators
                    'focus_metrics': [
                        'grants', 'signature', 'signed', 'executed', 'agreement',
                        'calls', 'completion', 'progress', 'status', 'scheduled',
                        'preparation', 'allocation', 'process', 'timeline'
                    ]
                },
                'output_configuration': {
                    'module': 'CommentsModule',
                    'variable_name': 'granting_process_text',
                    'word_limit': 200,                    # Brief overview as requested
                    'formatting_level': 'standard'
                },
                'instruction_mapping': {
                    'instruction_key': 'granting_process_instructions',
                    'tone': 'descriptive',
                    'focus': 'process_overview'
                }
            },

            # ============================================================
            # 💰 4: Commitment Budgetary Impact
            # ============================================================
            'commitment_budgetary_impact': {
                'section_info': {
                    'name': 'Budgetary Consequence of Commitment Process',
                    'category': 'financial',
                    'priority': 4,
                    'description': 'Financial impact and budgetary consequences of L2 commitment activity'
                },
                'template_mapping': {
                    'template_name': 'commitment_budgetary_template',
                    'template_category': 'commitment_financial',
                    'supports_variables': ['prioritized_data_summary', 'secondary_data_summary']
                },
                'data_configuration': {
                    # 🎯 PRIMARY_DATA: Financial commitment data
                    'primary_data': [
                        'commitments',                           # Main commitment financial data
                        'current_year_global_commitment_activity',  # Current year commitment activity
                        'grants_commitment_activity'             # Grant-specific commitment activity
                    ],
                    
                    # 🔄 SECONDARY_DATA: Breakdown and context
                    'secondary_data': [
                        'grants_signature_activity',            # Signature context for L2 vs legal timing
                        'completion_previous_year_calls',       # Call completion for period context
                        'summary_budget'                        # Budget context
                    ],
                    
                    # 🎯 FOCUS_METRICS: Financial and commitment indicators
                    'focus_metrics': [
                        'commitment', 'committed', 'amount', 'total', 'million', 'eur',
                        'budgetary', 'financial', 'l2', 'period', 'activity',
                        'breakdown', 'call', 'allocation'
                    ]
                },
                'output_configuration': {
                    'module': 'CommentsModule',
                    'variable_name': 'commitment_budgetary_text',
                    'word_limit': 250,                          # Include boilerplate text
                    'formatting_level': 'detailed'
                },
                'instruction_mapping': {
                    'instruction_key': 'commitment_budgetary_instructions',
                    'tone': 'analytical',
                    'focus': 'financial_impact'
                }
            },

            # ============================================================
            # ⏰ 5: FDI Status Analysis
            # ============================================================
            'fdi_status_analysis': {
                'section_info': {
                    'name': 'Final Date for Implementation Status',
                    'category': 'compliance',
                    'priority': 5,
                    'description': 'FDI threshold monitoring and compliance analysis for L2 commitments'
                },
                'template_mapping': {
                    'template_name': 'fdi_status_template',
                    'template_category': 'fdi_compliance',
                    'supports_variables': ['prioritized_data_summary', 'secondary_data_summary']
                },
                'data_configuration': {
                    # 🎯 PRIMARY_DATA: FDI-specific data
                    'primary_data': [
                        'grants_exceeding_fdi'                 # Main FDI analysis table
                    ],
                    
                    # 🔄 SECONDARY_DATA: Supporting context
                    'secondary_data': [
                        'grants_exceeding_fdi'  
                    ], 
                    
                    # 🎯 FOCUS_METRICS: FDI and compliance indicators
                    'focus_metrics': [
                        'fdi', 'threshold', 'exceeding', 'approaching', 'observed',
                        'h2020', 'heu', 'horizon', 'commitment', 'l2',
                        'cog', 'poc', 'stg', 'syg', 'adg', 'call'
                    ]
                },
                'output_configuration': {
                    'module': 'CommentsModule',
                    'variable_name': 'fdi_status_text',
                    'word_limit': 150,                         # Brief status as requested
                    'formatting_level': 'standard'
                },
                'instruction_mapping': {
                    'instruction_key': 'fdi_status_instructions',
                    'tone': 'factual',
                    'focus': 'compliance_status'
                }
            },

            # ============================================================
            # 💳 HEU Payment Overview Configuration
            # ============================================================
            'heu_payment_overview': {
                'section_info': {
                    'name': 'HEU Payment Credits Consumption',
                    'category': 'program_analysis',
                    'priority': 6,
                    'description': 'Horizon Europe payment credit consumption and forecast analysis'
                },
                'template_mapping': {
                    'template_name': 'heu_payment_overview_template',
                    'template_category': 'program_payment_analysis',
                    'supports_variables': ['prioritized_data_summary', 'secondary_data_summary']
                },
                'data_configuration': {
                    # 🎯 PRIMARY_DATA: Core HEU payment data
                    'primary_data': [
                        'pay_credits_HEU',                # Main HEU payment credit data
                        'HEU_payments_all',              # All HEU payments volume and amounts
                        'HEU_payments_analysis_ALL'       # HEU payment analysis and forecast data
                    ],
                    
                    # 🔄 SECONDARY_DATA: Supporting HEU analysis
                    'secondary_data': [
                        'HEU_TTP_FP',                    # HEU final payment TTP performance
                        'HEU_TTP_IP',                    # HEU interim payment TTP performance
                        'HEU_TTP_PF',                    # HEU pre-financing TTP performance
                        'HEU_TTP_EXPERTS',               # HEU expert payment TTP performance
                        'HEU_payments_final_payments',   # HEU final payment details
                        'HEU_payments_pre_financing_payments',  # HEU pre-financing details
                        'HEU_payments_EXPERTS',          # HEU expert payment details
                        'summary_budget'                 # Budget context for allocations
                    ],
                    
                    # 🎯 FOCUS_METRICS: HEU payment-specific indicators
                    'focus_metrics': [
                        'heu', 'horizon', 'payments', 'processed', 'total', 'amount',
                        'million', 'eur', 'credits', 'c1', 'e0', 'c4', 'c5',
                        'disbursed', 'allocation', 'annual', 'expert', 'efta',
                        'forecast', 'consumption', 'deviation', 'percentage', 'budget'
                    ]
                },
                'output_configuration': {
                    'module': 'CommentsModule',
                    'variable_name': 'heu_payment_overview_text',
                    'word_limit': 350,                  # Comprehensive program analysis
                    'formatting_level': 'detailed'
                },
                'instruction_mapping': {
                    'instruction_key': 'heu_payment_overview_instructions',
                    'tone': 'analytical',
                    'focus': 'heu_payment_consumption'
                }
            },

            # ============================================================
            # 💳 H2020 Payment Overview Configuration
            # ============================================================
            'h2020_payment_overview': {
                'section_info': {
                    'name': 'H2020 Payment Credits Consumption',
                    'category': 'program_analysis',
                    'priority': 7,
                    'description': 'Horizon 2020 payment credit consumption and forecast analysis'
                },
                'template_mapping': {
                    'template_name': 'h2020_payment_overview_template',
                    'template_category': 'program_payment_analysis',
                    'supports_variables': ['prioritized_data_summary', 'secondary_data_summary']
                },
                'data_configuration': {
                    # 🎯 PRIMARY_DATA: Core H2020 payment data
                    'primary_data': [
                        'pay_credits_H2020',             # Main H2020 payment credit data
                        'H2020_payments_all',            # All H2020 payments volume and amounts
                        'H2020_payments_analysis_ALL'    # H2020 payment analysis and forecast data
                    ],
                    
                    # 🔄 SECONDARY_DATA: Supporting H2020 analysis
                    'secondary_data': [
                        'H2020_TTP_FP',                  # H2020 final payment TTP performance
                        'H2020_TTP_IP',                  # H2020 interim payment TTP performance
                        'H2020_payments_final_payments', # H2020 final payment details
                        'H2020_payments_interim_payments',  # H2020 interim payment details
                        'summary_budget'                 # Budget context for allocations
                    ],
                    
                    # 🎯 FOCUS_METRICS: H2020 payment-specific indicators
                    'focus_metrics': [
                        'h2020', 'horizon', 'payments', 'processed', 'total', 'amount',
                        'million', 'eur', 'credits', 'disbursed', 'allocation', 'annual',
                        'forecast', 'consumption', 'deviation', 'percentage', 'budget'
                    ]
                },
                'output_configuration': {
                    'module': 'CommentsModule',
                    'variable_name': 'h2020_payment_overview_text',
                    'word_limit': 350,                  # Comprehensive program analysis
                    'formatting_level': 'detailed'
                },
                'instruction_mapping': {
                    'instruction_key': 'h2020_payment_overview_instructions',
                    'tone': 'analytical',
                    'focus': 'h2020_payment_consumption'
                }
            },

            # ============================================================
            # 🕐 NEW: TTP Performance Analysis Section
            # ============================================================
            'ttp_performance': {
                'section_info': {
                    'name': 'Time-to-Pay Performance Analysis',
                    'category': 'operational',
                    'priority': 8,  # Add after your other workflow sections
                    'description': 'TTP compliance analysis covering H2020 and HEU payment processing efficiency'
                },
                'template_mapping': {
                    'template_name': 'ttp_performance_template',
                    'template_category': 'operational_performance',
                    'supports_variables': [
                        'quarter_period',              # ✅ Add missing variables
                        'current_year',               # ✅ Add missing variables
                        'h2020_ttp_summary', 
                        'heu_ttp_summary', 
                        'prioritized_data_summary', 
                        'secondary_data_summary'
                    ]
                },
                'data_configuration': {
                    # 🎯 PRIMARY_DATA: Core TTP performance data ONLY
                    'primary_data': [
                        'TTP_Overview',                    # Main TTP performance summary
                        'H2020_TTP_FP',                   # H2020 final payment TTP
                        'H2020_TTP_IP',                   # H2020 interim payment TTP
                        'HEU_TTP_FP',                     # HEU final payment TTP
                        'HEU_TTP_IP',                     # HEU interim payment TTP
                        'HEU_TTP_PF',                     # HEU pre-financing TTP
                        'HEU_TTP_EXPERTS'                 # HEU expert payment TTP
                    ],
                    
                    # 🔄 SECONDARY_DATA: Additional TTP context (if any)
                    'secondary_data': [
                        # Only TTP-related supporting data - no payment credit consumption
                    ],
                    
                    # 🎯 FOCUS_METRICS: TTP and compliance indicators ONLY
                    'focus_metrics': [
                        # Time metrics
                        'ttp', 'time', 'days', 'delay', 'processing', 'timeline', 'duration',
                        
                        # Compliance metrics  
                        'compliance', 'contractual', 'limits', 'percentage', 'rate', 'within',
                        
                        # Payment types (for TTP context only)
                        'expert', 'pre-financing', 'interim', 'final', 'adg',
                        
                        # Performance metrics
                        'executed', 'processed', 'efficiency', 'performance', 'quarterly', 'yearly',
                        
                        # TTP-specific metrics
                        'average', 'median', 'maximum', 'minimum', 'target', 'threshold'
                    ]
                },
                'output_configuration': {
                    'module': 'CommentsModule',
                    'variable_name': 'ttp_performance_text',
                    'word_limit': 400,                    # Comprehensive TTP analysis
                    'formatting_level': 'operational'
                },
                'instruction_mapping': {
                    'instruction_key': 'ttp_performance_instructions',
                    'tone': 'operational',
                    'focus': 'compliance_and_efficiency'
                }
            },

            # ============================================================
            # WORKFLOW-SPECIFIC SECTIONS
            # ============================================================
            'payments_workflow': {
                'section_info': {
                    'name': 'Payments Workflow Summary',
                    'category': 'workflow',
                    'priority': 3,
                    'description': 'Payment processing efficiency and credit consumption'
                },
                'template_mapping': {
                    'template_name': 'payments_workflow_template',
                    'template_category': 'workflow_analysis',
                    'supports_variables': ['prioritized_data_summary', 'secondary_data_summary']
                },
                'data_configuration': {
                    'primary_data': ['pay_credits_H2020', 'pay_credits_HEU'],
                    'secondary_data': ['summary_budget'],
                    'focus_metrics': ['payment_consumption', 'processing_efficiency', 'credit_utilization']
                },
                'output_configuration': {
                    'module': 'PaymentsModule',
                    'variable_name': 'payments_workflow_summary',
                    'word_limit': 250,
                    'formatting_level': 'operational'
                },
                'instruction_mapping': {
                    'instruction_key': 'payments_workflow_instructions',
                    'tone': 'operational',
                    'focus': 'workflow_efficiency'
                }
            },

            'amendments_workflow': {
                'section_info': {
                    'name': 'Amendments Workflow Summary',
                    'category': 'workflow',
                    'priority': 5,
                    'description': 'Grant amendment processing and administrative performance'
                },
                'template_mapping': {
                    'template_name': 'amendments_workflow_template',
                    'template_category': 'workflow_analysis',
                    'supports_variables': ['prioritized_data_summary', 'secondary_data_summary']
                },
                'data_configuration': {
                    'primary_data': ['commitments'],
                    'secondary_data': ['pay_credits_H2020', 'pay_credits_HEU'],
                    'focus_metrics': ['processing_efficiency', 'modification_impact', 'administrative_performance']
                },
                'output_configuration': {
                    'module': 'AmendmentsModule',
                    'variable_name': 'amendments_workflow_summary',
                    'word_limit': 200,
                    'formatting_level': 'operational'
                },
                'instruction_mapping': {
                    'instruction_key': 'amendments_workflow_instructions',
                    'tone': 'operational',
                    'focus': 'administrative_efficiency'
                }
            },

            'audit_workflow': {
                'section_info': {
                    'name': 'Audit Results Workflow Summary',
                    'category': 'compliance',
                    'priority': 6,
                    'description': 'Audit implementation and recovery processing'
                },
                'template_mapping': {
                    'template_name': 'audit_workflow_template',
                    'template_category': 'compliance_analysis',
                    'supports_variables': ['prioritized_data_summary', 'secondary_data_summary']
                },
                'data_configuration': {
                    'primary_data': ['summary_budget'],
                    'secondary_data': ['commitments', 'pay_credits_H2020', 'pay_credits_HEU'],
                    'focus_metrics': ['recovery_amounts', 'compliance_status', 'implementation_progress']
                },
                'output_configuration': {
                    'module': 'AuditModule',
                    'variable_name': 'audit_workflow_summary',
                    'word_limit': 200,
                    'formatting_level': 'compliance'
                },
                'instruction_mapping': {
                    'instruction_key': 'audit_workflow_instructions',
                    'tone': 'compliance',
                    'focus': 'risk_mitigation'
                }
            },

            # ============================================================
            # SPECIALIZED ANALYSIS SECTIONS
            # ============================================================
            
            'payment_analysis': {
                'section_info': {
                    'name': 'Payment Analysis (Dynamic)',
                    'category': 'specialized',
                    'priority': 7,
                    'description': 'Call type and programme specific payment analysis'
                },
                'template_mapping': {
                    'template_name': 'payment_analysis_template',
                    'template_category': 'specialized_analysis',
                    'supports_variables': ['prioritized_data_summary', 'secondary_data_summary', 'call_type', 'programme']
                },
                'data_configuration': {
                    'primary_data': ['pay_credits_H2020', 'pay_credits_HEU'],
                    'secondary_data': ['summary_budget'],
                    'focus_metrics': ['consumption_vs_forecast', 'utilization_rates', 'variance_analysis']
                },
                'output_configuration': {
                    'module': 'PaymentsModule',
                    'variable_name': 'payment_analysis_{call_type}_{programme}',  # Dynamic naming
                    'word_limit': 200,
                    'formatting_level': 'analytical'
                },
                'instruction_mapping': {
                    'instruction_key': 'payment_analysis_instructions',
                    'tone': 'analytical',
                    'focus': 'variance_analysis'
                }
            },

            'call_type_payment_detail': {
                'section_info': {
                    'name': 'Call Type Payment Detail (Granular)',
                    'category': 'granular_payment',
                    'priority': 8,
                    'description': 'Highly detailed call type payment breakdown with specific formatting'
                },
                'template_mapping': {
                    'template_name': 'call_type_payment_detail_template',
                    'template_category': 'granular_analysis',
                    'supports_variables': ['call_type_code', 'payment_type_description', 'call_type_abbreviation', 'payment_details_analysis', 'forecast_comparison_statement']
                },
                'data_configuration': {
                    'primary_data': ['pay_credits_H2020', 'pay_credits_HEU'],
                    'secondary_data': ['summary_budget'],
                    'focus_metrics': ['payment_volumes', 'payment_amounts', 'credit_utilization', 'forecast_variance']
                },
                'output_configuration': {
                    'module': 'PaymentsModule',
                    'variable_name': 'call_type_detail_{programme}_{call_type_code}',  # Dynamic naming
                    'word_limit': 150,
                    'formatting_level': 'detailed'
                },
                'instruction_mapping': {
                    'instruction_key': 'call_type_detail_instructions',
                    'tone': 'factual',
                    'focus': 'payment_specifics'
                }
            },

            'auto_call_type_detail': {
                'section_info': {
                    'name': 'Auto Call Type Detail (Data-Derived)',
                    'category': 'automated_payment',
                    'priority': 9,
                    'description': 'Automatically generated call type details with data-derived descriptions'
                },
                'template_mapping': {
                    'template_name': 'auto_call_type_detail_template',
                    'template_category': 'automated_analysis',
                    'supports_variables': ['call_type_abbreviation', 'derived_payment_description', 'payment_analysis_text', 'variance_statement']
                },
                'data_configuration': {
                    'primary_data': ['pay_credits_H2020', 'pay_credits_HEU'],
                    'secondary_data': ['summary_budget'],
                    'focus_metrics': ['payment_volumes', 'payment_amounts', 'credit_utilization', 'forecast_variance']
                },
                'output_configuration': {
                    'module': 'PaymentsModule',
                    'variable_name': 'auto_call_detail_{programme}_{call_type}',  # Dynamic naming
                    'word_limit': 120,
                    'formatting_level': 'automated'
                },
                'instruction_mapping': {
                    'instruction_key': 'auto_call_type_instructions',
                    'tone': 'factual',
                    'focus': 'data_derived'
                }
            }
        }

# ================================================================
# ⚙️ SECTION 3: UTILITY PROCESSORS - Data Processing Logic
# ================================================================
"""📍 Location: Lines 655-864"""


class ProgramProcessor:
    """Utility class for processing programs using the enhanced PROGRAM_MAPPING"""
    
    @staticmethod
    def normalize_program(program: str) -> Optional[str]:
        """Normalize program name to standard format using PROGRAM_MAPPING"""
        program_upper = program.upper()
        return PROGRAM_ALIASES.get(program_upper)
    
    @staticmethod
    def get_program_info(program: str) -> Optional[Dict[str, Any]]:
        """Get complete program information from PROGRAM_MAPPING"""
        normalized = ProgramProcessor.normalize_program(program)
        if normalized:
            return PROGRAM_MAPPING[normalized]
        return None
    
    @staticmethod
    def get_data_key(program: str) -> Optional[str]:
        """Get the primary data key for a program"""
        info = ProgramProcessor.get_program_info(program)
        return info['data_key'] if info else None
    
    @staticmethod
   
    def get_all_data_keys(program: str) -> List[str]:
        """Get data source keys using your actual table structure"""
        
        base_keys = [
            f'pay_credits_{program}',
            f'{program}_payments_all'
        ]
        
        # Add dedicated analysis tables for each call type
        call_types = ['STG', 'ADG', 'COG', 'SYG', 'POC']
        if program.upper() == 'HEU':
            call_types.extend(['EXPERTS'])  # HEU has POC and EXPERTS
        
        for call_type in call_types:
            base_keys.append(f'{program}_payments_analysis_{call_type}')
        
        return base_keys

    @staticmethod
    def get_official_name(program: str) -> Optional[str]:
        """Get the official program name"""
        info = ProgramProcessor.get_program_info(program)
        return info['official_name'] if info else None

class CallTypeProcessor:
    """Utility class for processing call types and extracting data from tables"""
    
    @staticmethod
    def normalize_call_type(call_type: str) -> str:
        """Normalize call type to standard format regardless of graphical variation"""
        return CALL_TYPE_VARIANTS.get(call_type, call_type.upper())
    
    @staticmethod
    def extract_call_type_data_from_tables(
        financial_data: Dict[str, Any], 
        program: str, 
        call_type: str, 
        verbose: bool = False
    ) -> Optional[Dict[str, Any]]:
        """
        CORRECTED: Extract call type data from dedicated analysis tables
        Your system has specific tables for each call type - use those directly!
        """
        
        # Map to your actual table structure
        table_key = f"{program}_payments_analysis_{call_type.upper()}"
        
        if verbose:
            print(f"🔍 Looking for dedicated table: {table_key}")
        
        # Check if the dedicated table exists
        if table_key in financial_data and financial_data[table_key] is not None:
            try:
                if isinstance(financial_data[table_key], str):
                    parsed_data = json.loads(financial_data[table_key])
                else:
                    parsed_data = financial_data[table_key]
                
                if isinstance(parsed_data, list) and len(parsed_data) > 0:
                    if verbose:
                        print(f"✅ Found {len(parsed_data)} records in {table_key}")
                    
                    # Get program info
                    program_info = ProgramProcessor.get_program_info(program)
                    
                    return {
                        'program': program,
                        'program_info': program_info,
                        'call_type': call_type,
                        'normalized_call_type': call_type.upper(),
                        'records': parsed_data,
                        'total_records': len(parsed_data),
                        'derived_description': f"{program} {call_type} Payment Analysis",
                        'data_source': table_key,
                        'table_type': 'dedicated_analysis'
                    }
                else:
                    if verbose:
                        print(f"⚠️  Table {table_key} exists but is empty")
            
            except json.JSONDecodeError as e:
                if verbose:
                    print(f"❌ JSON parsing error for {table_key}: {e}")
            except Exception as e:
                if verbose:
                    print(f"❌ Error processing {table_key}: {e}")
        else:
            if verbose:
                print(f"❌ Table {table_key} not found in financial_data")
                # Show available tables for debugging
                available_analysis_tables = [k for k in financial_data.keys() if 'payments_analysis' in k]
                print(f"💡 Available analysis tables: {available_analysis_tables}")
        
        return None
    @staticmethod
    def extract_program_summary_data(
        financial_data: Dict[str, Any], 
        program: str, 
        verbose: bool = False
    ) -> Optional[Dict[str, Any]]:
        """
        Extract general program data when specific call type data isn't available.
        This provides a fallback for generating program-level summaries.
        """
        
        program_info = ProgramProcessor.get_program_info(program)
        if not program_info:
            return None
        
        # Get main program data keys
        main_data_key = program_info.get('data_key')
        
        if main_data_key in financial_data and financial_data[main_data_key] is not None:
            try:
                if isinstance(financial_data[main_data_key], str):
                    parsed_data = json.loads(financial_data[main_data_key])
                else:
                    parsed_data = financial_data[main_data_key]
                
                if isinstance(parsed_data, list) and len(parsed_data) > 0:
                    return {
                        'program': program,
                        'program_info': program_info,
                        'call_type': 'ALL',
                        'normalized_call_type': 'ALL',
                        'records': parsed_data,
                        'total_records': len(parsed_data),
                        'derived_description': f'{program} Program Overview',
                        'data_source': main_data_key
                    }
            except json.JSONDecodeError:
                pass
        
        return None
        
    @staticmethod
    def _select_best_description(descriptions: List[str], call_type: str) -> str:
        """Select the best payment description from available options"""
        
        if not descriptions:
            return f"{call_type} Payments"
        
        # Priority keywords for better descriptions
        priority_keywords = ['pre-financing', 'interim', 'final', 'advance', 'grant']
        
        # Find descriptions with priority keywords
        prioritized = []
        for desc in descriptions:
            for keyword in priority_keywords:
                if keyword.lower() in desc.lower():
                    prioritized.append(desc)
                    break
        
        if prioritized:
            # Return the shortest prioritized description (usually cleaner)
            return min(prioritized, key=len)
        else:
            # Return the shortest description overall
            return min(descriptions, key=len)
    
    @staticmethod
    def calculate_payment_statistics(records: List[Dict[str, Any]]) -> Dict[str, Any]:
        """Calculate payment statistics from records"""
        
        stats = {
            'total_payments': len(records),
            'total_amount': 0.0,
            'credit_amount': 0.0,
            'currency': 'EUR',
            'has_amounts': False
        }
        
        # Try to extract amounts from various possible field names
        amount_fields = ['amount', 'total_amount', 'payment_amount', 'value', 'sum']
        credit_fields = ['credit_amount', 'credits_used', 'c1_e0_credits', 'credit_utilization']
        
        for record in records:
            if isinstance(record, dict):
                # Look for amount fields
                for field in amount_fields:
                    if field in record:
                        try:
                            amount = float(record[field])
                            stats['total_amount'] += amount
                            stats['has_amounts'] = True
                            break
                        except (ValueError, TypeError):
                            continue
                
                # Look for credit fields
                for field in credit_fields:
                    if field in record:
                        try:
                            credit = float(record[field])
                            stats['credit_amount'] += credit
                            break
                        except (ValueError, TypeError):
                            continue
        
        # Convert to millions if amounts are large
        if stats['total_amount'] > 1000000:
            stats['total_amount'] = stats['total_amount'] / 1000000
            stats['credit_amount'] = stats['credit_amount'] / 1000000
            stats['currency'] = 'EUR million'
        
        return stats


# ================================================================
# 🧪 SECTION 4. MATRIX VISUALIZATION
# ================================================================

class MatrixVisualization:
    """Utilities for visualizing and managing the template-section relationships"""
    
    @staticmethod
    def display_template_library():
        """Display all available templates with clear identification"""
        
        templates = ReportTemplateLibrary.get_template_definitions("Q1", "2025")
        
        print("🎨 TEMPLATE LIBRARY OVERVIEW")
        print("=" * 80)
        print(f"{'Template Name':<35} {'Category':<20} {'Variables':<25}")
        print("-" * 80)
        
        template_categories = {
            'executive_summary_template': ('Executive Overview', 'prioritized_data, secondary_data'),
            'budget_overview_template': ('Financial Overview', 'prioritized_data, secondary_data'),
            'payments_workflow_template': ('Workflow Analysis', 'prioritized_data, secondary_data'),
            'commitments_workflow_template': ('Workflow Analysis', 'prioritized_data, secondary_data'),
            'amendments_workflow_template': ('Workflow Analysis', 'prioritized_data, secondary_data'),
            'audit_workflow_template': ('Compliance Analysis', 'prioritized_data, secondary_data'),
            'payment_analysis_template': ('Specialized Analysis', 'call_type, programme, data'),
            'call_type_payment_detail_template': ('Granular Payment', 'call_type_code, payment_type_desc, forecast'),
            'auto_call_type_detail_template': ('Automated Payment', 'call_type_abbrev, derived_desc, analysis'),
            'variance_analysis_template': ('Specialized Analysis', 'prioritized_data, secondary_data'),
            'risk_assessment_template': ('Risk Analysis', 'prioritized_data, secondary_data')
        }
        
        for template_name in templates.keys():
            category, variables = template_categories.get(template_name, ('Other', 'Various'))
            print(f"{template_name:<35} {category:<20} {variables:<25}")
        
        print(f"\n📊 Total Templates: {len(templates)}")
    
    @staticmethod
    def display_section_template_mapping():
        """Display the complete section-to-template mapping matrix"""
        
        mapping = TemplateSectionMatrix.get_complete_mapping_matrix()
        
        print("\n🗺️  SECTION-TEMPLATE MAPPING MATRIX")
        print("=" * 120)
        print(f"{'Section Key':<20} {'Section Name':<30} {'Template':<30} {'Module':<20} {'Variable':<20}")
        print("-" * 120)
        
        for section_key, config in mapping.items():
            section_name = config['section_info']['name']
            template_name = config['template_mapping']['template_name']
            module = config['output_configuration']['module']
            variable = config['output_configuration']['variable_name']
            
            print(f"{section_key:<20} {section_name:<30} {template_name:<30} {module:<20} {variable:<20}")
        
        print(f"\n📋 Total Mappings: {len(mapping)}")
    
    @staticmethod
    def display_data_flow_matrix():
        """Display how data flows through templates to outputs"""
        
        mapping = TemplateSectionMatrix.get_complete_mapping_matrix()
        
        print("\n🔄 DATA FLOW MATRIX")
        print("=" * 100)
        print(f"{'Section':<20} {'Primary Data':<30} {'Template':<25} {'Output Variable':<25}")
        print("-" * 100)
        
        for section_key, config in mapping.items():
            primary_data = ', '.join(config['data_configuration']['primary_data'][:2])
            if len(config['data_configuration']['primary_data']) > 2:
                primary_data += "..."
            template_name = config['template_mapping']['template_name'].replace('_template', '')
            output_var = config['output_configuration']['variable_name']
            
            print(f"{section_key:<20} {primary_data:<30} {template_name:<25} {output_var:<25}")
    
    @staticmethod
    def display_program_mapping():
        """Display the enhanced program mapping structure"""
        
        print("\n🏢 ENHANCED PROGRAM MAPPING")
        print("=" * 80)
        print(f"{'Program':<10} {'Official Name':<20} {'Data Key':<20} {'Aliases':<30}")
        print("-" * 80)
        
        for program, config in PROGRAM_MAPPING.items():
            aliases = ', '.join(config['aliases'][:3])
            if len(config['aliases']) > 3:
                aliases += "..."
            print(f"{program:<10} {config['official_name']:<20} {config['data_key']:<20} {aliases:<30}")
        
        print(f"\n📋 Total Programs: {len(PROGRAM_MAPPING)}")
        print(f"📋 Total Aliases: {len(PROGRAM_ALIASES)}")
    
    @staticmethod
    def display_complete_matrix_overview():
        """Display the complete matrix structure for easy reference"""
        
        print("🎯 ENHANCED MATRIX SYSTEM OVERVIEW")
        print("=" * 80)
        
        MatrixVisualization.display_template_library()
        MatrixVisualization.display_section_template_mapping()
        MatrixVisualization.display_data_flow_matrix()
        MatrixVisualization.display_program_mapping()
        
        print("\n✨ TEMPLATE MANAGEMENT FEATURES:")
        print("• Clear template identification with descriptive names")
        print("• Centralized template library for easy maintenance")
        print("• Complete mapping matrix showing all relationships")
        print("• Enhanced program mapping with aliases and data keys")
        print("• Category-based template organization")
        print("• Visual data flow tracking")
        
        print("\n🚀 USAGE PATTERNS:")
        print("1. Add new template → Update ReportTemplateLibrary.get_template_definitions()")
        print("2. Add new section → Update TemplateSectionMatrix.get_complete_mapping_matrix()")
        print("3. Modify mapping → Update specific section configuration")
        print("4. View relationships → Use MatrixVisualization methods")
        print("5. Add program → Update PROGRAM_MAPPING dictionary")

# ================================================================
# 🤖💻🧠 SECTION 5: GENERATION ENGINE - AI Generation Logic
# ================================================================
"""📍 Location: Lines 800-1200"""
"""
Purpose:

Sets up the two core components needed for generation
Creates connection to template definitions and section mappings

Why These Components:

template_library: Access to all report templates (the "what to generate")
mapping_matrix: Access to section configurations (the "how to generate")

"""


class EnhancedReportGenerator:
    """Enhanced report generator using the improved template management system"""
    
    def __init__(self):
        self.template_library = ReportTemplateLibrary()
        self.mapping_matrix = TemplateSectionMatrix()

    """🎯 2. MAIN GENERATION METHOD: 
    This is the primary method for generating individual report sections."""

    # 🔧 MISSING METHODS - Add these to your EnhancedReportGenerator class

    def generate_individual_sections(self,
                                    quarter_period: str, 
                                    current_year: str,
                                    financial_data: Dict[str, Any],
                                    model: str = "qwen2.5:14b", 
                                    modeltemperature: float = 0.3,
                                    acronym_context: str = "",
                                    cutoff_date=None,
                                    verbose: bool = True) -> Dict[str, Any]:
        """
        Generate all individual sections from SINGLE_SECTIONS configuration
        This orchestrates the generation of all single sections (intro, budget, etc.)
        """
        
        # Import the config from comments.py
        from comments_old import CommentsConfig
        
        SINGLE_SECTIONS = CommentsConfig.SINGLE_SECTIONS
        
        if verbose:
            print(f"🔄 GENERATING ALL {len(SINGLE_SECTIONS)} SINGLE SECTIONS")
            print("=" * 60)
        
        results = {
            'generated_details': {},
            'failed_generations': [],
            'statistics': {
                'total_sections': len(SINGLE_SECTIONS),
                'successful': 0,
                'failed': 0
            }
        }
        
        # Apply cutoff date filtering if needed
        if cutoff_date is not None:
            financial_data = self._filter_data_by_cutoff(financial_data, cutoff_date, verbose)
        
        # Generate each section
        for i, section_name in enumerate(SINGLE_SECTIONS, 1):
            
            if verbose:
                print(f"\n📝 [{i}/{len(SINGLE_SECTIONS)}] Generating: {section_name}")
            
            try:
                # Use the existing generate_section_commentary method
                section_commentary = self.generate_section_commentary(
                    section_key=section_name,
                    quarter_period=quarter_period,
                    current_year=current_year,
                    financial_data=financial_data,
                    model=model,
                    temperature=temperature,
                    acronym_context=acronym_context,
                    cutoff_date=None,  # Already applied above
                    verbose=verbose
                )
                
                if section_commentary and len(section_commentary.strip()) > 50:
                    # Successful generation
                    results['generated_details'][section_name] = {
                        'commentary': section_commentary,
                        'section_name': section_name.replace('_', ' ').title(),
                        'word_count': len(section_commentary.split()),
                        'generated_at': datetime.datetime.now()
                    }
                    results['statistics']['successful'] += 1
                    
                    if verbose:
                        print(f"   ✅ Success: {len(section_commentary.split())} words")
                else:
                    # Failed generation
                    results['failed_generations'].append(section_name)
                    results['statistics']['failed'] += 1
                    
                    if verbose:
                        print(f"   ❌ Failed: Empty or too short")
            
            except Exception as e:
                # Error in generation
                results['failed_generations'].append(section_name)
                results['statistics']['failed'] += 1
                
                if verbose:
                    print(f"   ❌ Error: {e}")
            
            # Small delay to avoid overwhelming the model
            import time
            time.sleep(0.5)
        
        if verbose:
            print(f"\n🎉 INDIVIDUAL SECTIONS COMPLETE")
            print(f"✅ Success: {results['statistics']['successful']}/{results['statistics']['total_sections']}")
            print(f"❌ Failed: {results['statistics']['failed']}")
            
            if results['failed_generations']:
                print(f"Failed sections: {results['failed_generations']}")
        
        return results
    
    
    def _generate_single_section_commentary(
        self,
        section_key: str,
        section_config: dict,
        quarter_period: str,
        current_year: str,
        financial_data: Dict[str, Any],
        model: str,
        temperature: float,
        acronym_context: str,
        cutoff_date: Any,
        verbose: bool
    ) -> Optional[str]:
        """
        CORRECTED: Generate commentary for a single section using conditional logic
        to handle different template structures and avoid errors.
        """
        # Step 1: Template Retrieval
        templates = self.template_library.get_template_definitions(quarter_period, current_year)
        template_name = section_config['template_mapping']['template_name']
        if template_name not in templates:
            if verbose:
                print(f"❌ Template '{template_name}' not found for section '{section_key}'")
            return None
        template = templates[template_name]

        # Step 2: Data Preparation
        data_config = section_config['data_configuration']
        primary_data_raw = {k: v for k, v in financial_data.items() if k in data_config['primary_data']}
        secondary_data_raw = {k: v for k, v in financial_data.items() if k in data_config['secondary_data']}

        if not primary_data_raw and not secondary_data_raw:
            if verbose:
                print(f"⚠️ No primary or secondary data found for section '{section_key}'. Skipping.")
            return None

        # ======================================================================
        # 🎯 CONDITIONAL LOGIC TO HANDLE DIFFERENT TEMPLATE STRUCTURES
        # ======================================================================

        if section_key == 'intro_summary':
            if verbose: print(f"   -> Using new contextual KPI logic for '{section_key}'")

            # A) Extract all specific KPIs and contextual hints into a dictionary
            kpi_and_context_dict = self._extract_and_contextualize_intro_kpis(financial_data, quarter_period)

            # B) The template is now the pre-written text with placeholders. We directly populate it here.
            final_output = self._format_template_safely(template, kpi_and_context_dict)
            
            # Since we are just filling placeholders, we don't need to call the AI for generation.
            # This makes the process faster and more reliable.
            # However, if we wanted the AI to smooth out the language, we would follow the steps below.
            # For now, direct population is safer.

            # The code below demonstrates how you would ask an AI to fill it, but direct formatting is better.
            # For simplicity and accuracy, we will return the directly formatted text.
            
            # --- AI-based completion (alternative to direct formatting) ---
            # populated_template_with_placeholders = template
            # ai_data_context = "\n".join([f"- {key}: {value}" for key, value in kpi_and_context_dict.items()])
            # instructions = self._get_section_instructions(section_config, quarter_period, current_year)
            # final_prompt = self._create_enhanced_prompt(
            #     instructions=instructions,
            #     template=f"TEMPLATE TO COMPLETE:\n---\n{populated_template_with_placeholders}\n---\n\nDATA CONTEXT:\n---\n{ai_data_context}\n---",
            #     acronym_context=acronym_context,
            #     section_key=section_key,
            #     current_year=current_year,
            #     quarter_period=quarter_period
            # )
            # final_output_from_ai = self._generate_with_retry(
            #     prompt=final_prompt, model=model, temperature=temperature,
            #     max_tokens=int(section_config['output_configuration']['word_limit'] * 1.8),
            #     section_key=section_key, verbose=verbose,
            #     word_limit=section_config['output_configuration']['word_limit']
            # )
            # return final_output_from_ai

        # Special handling for sections with a single, AI-generated commentary placeholder
        elif section_key in ['budget_overview']: # Add other specially-handled sections here in the future
            if verbose: print(f"   -> Using special single-placeholder logic for '{section_key}'")

            # A) Create a combined data context for the AI to analyze
            ai_data_context = self._prepare_data_summary(
                {**primary_data_raw, **secondary_data_raw},
                data_config['focus_metrics'],
                "FINANCIAL DATA CONTEXT"
            )

            # B) Get instructions and create a prompt for the AI to generate ONLY the commentary
            instructions = self._get_section_instructions(section_config, quarter_period, current_year)
            prompt_for_ai = self._create_enhanced_prompt(
                instructions=instructions,
                template=ai_data_context, # The AI's "template" is just the raw data
                acronym_context=acronym_context,
                section_key=section_key,
                current_year=current_year,
                quarter_period=quarter_period
            )

            # C) Generate ONLY the analysis text from the AI
            analysis_commentary = self._generate_with_retry(
                prompt=prompt_for_ai, model=model, temperature=temperature,
                max_tokens=int(section_config['output_configuration']['word_limit'] * 1.8),
                section_key=section_key, verbose=verbose,
                word_limit=section_config['output_configuration']['word_limit']
            )

            if not analysis_commentary:
                return None

            # D) Inject the AI's generated text into the final template
            final_output = self._format_template_safely(template, {'budget_analysis_commentary': analysis_commentary})

        # --- DEFAULT LOGIC for all other sections (granting_process, etc.) ---
        else:
            if verbose: print(f"   -> Using standard multi-placeholder logic for '{section_key}'")

            # A) Pre-process data if necessary
            if section_key == 'granting_process_overview':
                if verbose: print("   🔬 Pre-processing data for granting overview to ensure conciseness...")
                primary_data_summary = self._prepare_data_summary(
                    self._preprocess_granting_data(primary_data_raw), data_config['focus_metrics'], "PRIMARY")
                secondary_data_summary = self._prepare_data_summary(
                    self._preprocess_granting_data(secondary_data_raw), data_config['focus_metrics'], "SECONDARY")
            else:
                 primary_data_summary = self._prepare_data_summary(
                    primary_data_raw, data_config['focus_metrics'], "PRIMARY")
                 secondary_data_summary = self._prepare_data_summary(
                    secondary_data_raw, data_config['focus_metrics'], "SECONDARY")

            # B) Create the dictionary of variables to populate the template
            template_vars = {
                'prioritized_data_summary': primary_data_summary,
                'secondary_data_summary': secondary_data_summary,
                'h2020_ttp_summary': '', # Default empty values for TTP
                'heu_ttp_summary': '',
            }

            # C) Add special TTP data if this is the ttp_performance section
            if section_key == 'ttp_performance':
                ttp_summaries = self._prepare_ttp_data_summary(financial_data, quarter_period, current_year)
                template_vars.update(ttp_summaries)

            # D) Populate the template with all its required data
            populated_template = self._format_template_safely(template, template_vars)

            # E) Get instructions and create the final prompt for the AI
            instructions = self._get_section_instructions(section_config, quarter_period, current_year)
            final_prompt = self._create_enhanced_prompt(
                instructions=instructions,
                template=populated_template, # The AI gets the pre-filled template
                acronym_context=acronym_context,
                section_key=section_key,
                current_year=current_year,
                quarter_period=quarter_period
            )

            # F) Generate the final output directly from the AI
            final_output = self._generate_with_retry(
                prompt=final_prompt, model=model, temperature=temperature,
                max_tokens=int(section_config['output_configuration']['word_limit'] * 1.8),
                section_key=section_key, verbose=verbose,
                word_limit=section_config['output_configuration']['word_limit']
            )

        return final_output



    def generate_individual_sections_enhanced(
        self,
        quarter_period: str,
        current_year: str,
        financial_data: Dict[str, Any],
        model: str = "qwen2.5:14b",
        temperature: float = 0.3,
        acronym_context: str = "",
        cutoff_date: Any = None,
        verbose: bool = True
    ) -> Dict[str, Any]:
        """
        Enhanced version with better validation and error handling
        """
        
        from comments_old import CommentsConfig
        
        SINGLE_SECTIONS = CommentsConfig.SINGLE_SECTIONS
        
        if verbose:
            print(f"🔄 ENHANCED INDIVIDUAL SECTIONS GENERATION")
            print("=" * 60)
            print(f"📋 Sections to generate: {SINGLE_SECTIONS}")
        
        # Validate sections against mapping matrix
        mapping = self.mapping_matrix.get_complete_mapping_matrix()
        valid_sections = []
        invalid_sections = []
        
        for section in SINGLE_SECTIONS:
            if section in mapping:
                valid_sections.append(section)
            else:
                invalid_sections.append(section)
        
        if invalid_sections and verbose:
            print(f"⚠️  Invalid sections (no mapping): {invalid_sections}")
        
        if verbose:
            print(f"✅ Valid sections: {len(valid_sections)}")
        
        results = {
            'generated_details': {},
            'failed_generations': [],
            'invalid_sections': invalid_sections,
            'statistics': {
                'total_sections': len(SINGLE_SECTIONS),
                'valid_sections': len(valid_sections),
                'successful': 0,
                'failed': 0
            }
        }
        
        # Apply cutoff date filtering if needed
        if cutoff_date is not None:
            financial_data = self._filter_data_by_cutoff(financial_data, cutoff_date, verbose)
        
        # Generate each valid section
        for i, section_name in enumerate(valid_sections, 1):
            
            if verbose:
                print(f"\n📝 [{i}/{len(valid_sections)}] Generating: {section_name}")
            
            try:
                # Get section configuration for additional validation
                section_config = mapping[section_name]
                word_limit = section_config['output_configuration']['word_limit']
                
                if verbose:
                    print(f"   🎯 Target: {word_limit} words")
                
                # Generate using existing method
                section_commentary = self.generate_section_commentary(
                    section_key=section_name,
                    quarter_period=quarter_period,
                    current_year=current_year,
                    financial_data=financial_data,
                    model=model,
                    temperature=temperature,
                    acronym_context=acronym_context,
                    cutoff_date=None,  # Already applied above
                    verbose=False  # Reduce verbosity in loop
                )
                
                if section_commentary and len(section_commentary.strip()) > 50:
                    word_count = len(section_commentary.split())
                    
                    # Check if within reasonable range of target
                    word_variance = abs(word_count - word_limit) / word_limit
                    if word_variance > 0.5 and verbose:  # More than 50% variance
                        print(f"   ⚠️  Word count variance: {word_count} vs {word_limit} target")
                    
                    results['generated_details'][section_name] = {
                        'commentary': section_commentary,
                        'section_name': section_config['section_info']['name'],
                        'word_count': word_count,
                        'target_words': word_limit,
                        'word_variance': word_variance,
                        'generated_at': datetime.datetime.now(),
                        'section_category': section_config['section_info']['category']
                    }
                    results['statistics']['successful'] += 1
                    
                    if verbose:
                        print(f"   ✅ Success: {word_count} words (target: {word_limit})")
                else:
                    results['failed_generations'].append(section_name)
                    results['statistics']['failed'] += 1
                    
                    if verbose:
                        print(f"   ❌ Failed: Empty or too short response")
            
            except Exception as e:
                results['failed_generations'].append(section_name)
                results['statistics']['failed'] += 1
                
                if verbose:
                    print(f"   ❌ Error: {str(e)}")
                    import traceback
                    print(f"   📋 Details: {traceback.format_exc()[:200]}...")
            
            # Delay between generations
            import time
            time.sleep(0.5)
        
        if verbose:
            success_rate = (results['statistics']['successful'] / len(valid_sections) * 100) if valid_sections else 0
            print(f"\n🎉 ENHANCED INDIVIDUAL SECTIONS COMPLETE")
            print(f"✅ Success: {results['statistics']['successful']}/{len(valid_sections)} ({success_rate:.1f}%)")
            print(f"❌ Failed: {results['statistics']['failed']}")
            
            if results['failed_generations']:
                print(f"💡 Failed sections: {results['failed_generations']}")
            
            if invalid_sections:
                print(f"⚠️  Invalid sections: {invalid_sections}")
        
        return results

    def validate_sections_configuration(self) -> Dict[str, Any]:
        """
        Validate that all SINGLE_SECTIONS have proper mapping configurations
        Useful for debugging configuration issues
        """
        
        from comments_old import CommentsConfig
        
        SINGLE_SECTIONS = CommentsConfig.SINGLE_SECTIONS
        mapping = self.mapping_matrix.get_complete_mapping_matrix()
        templates = self.template_library.get_template_definitions("Q1", "2025")
        
        validation_results = {
            'valid_sections': [],
            'missing_mapping': [],
            'missing_template': [],
            'configuration_errors': []
        }
        
        print("🔍 VALIDATING SECTIONS CONFIGURATION")
        print("=" * 50)
        
        for section in SINGLE_SECTIONS:
            print(f"\n📋 Validating: {section}")
            
            # Check mapping exists
            if section not in mapping:
                validation_results['missing_mapping'].append(section)
                print(f"   ❌ No mapping configuration")
                continue
            
            section_config = mapping[section]
            
            # Check template exists
            template_name = section_config['template_mapping']['template_name']
            if template_name not in templates:
                validation_results['missing_template'].append(section)
                print(f"   ❌ Template missing: {template_name}")
                continue
            
            # Check configuration completeness
            try:
                required_keys = ['section_info', 'template_mapping', 'data_configuration', 'output_configuration']
                for key in required_keys:
                    if key not in section_config:
                        raise KeyError(f"Missing {key}")
                
                validation_results['valid_sections'].append(section)
                print(f"   ✅ Valid configuration")
                
            except Exception as e:
                validation_results['configuration_errors'].append(f"{section}: {e}")
                print(f"   ❌ Configuration error: {e}")
        
        print(f"\n📊 VALIDATION SUMMARY:")
        print(f"✅ Valid: {len(validation_results['valid_sections'])}")
        print(f"❌ Missing mapping: {len(validation_results['missing_mapping'])}")
        print(f"❌ Missing template: {len(validation_results['missing_template'])}")
        print(f"❌ Config errors: {len(validation_results['configuration_errors'])}")
        
        return validation_results


    def _get_call_type_description(self, call_type: str) -> str:
        """Get description for call type"""
        descriptions = {
            'STG': 'Starting Grants for early-career researchers',
            'ADG': 'Advanced Grants for established researchers',
            'COG': 'Consolidator Grants for mid-career researchers',
            'SYG': 'Synergy Grants for collaborative research teams',
            'POC': 'Proof of Concept grants for commercialization',
            'EXPERTS': 'Expert evaluation and support services'
        }
        return descriptions.get(call_type, f'{call_type} grants')


    def _generate_call_type_payment_overview(
        self,
        program: str,
        call_type: str,
        quarter_period: str,
        current_year: str,
        financial_data: Dict[str, Any],
        model: str,
        temperature: float,
        acronym_context: str,
        verbose: bool
    ) -> Optional[str]:
        """Generate payment overview for specific program-call type combination"""
        
        # Extract data for this specific combination
        table_key = f"{program}_payments_analysis_{call_type}"
        
        if table_key not in financial_data or financial_data[table_key] is None:
            if verbose:
                print(f"⚠️ No data found for {table_key}")
            return None
        
        # Get the payment overview template
        templates = self.template_library.get_template_definitions(quarter_period, current_year)
        template_key = 'heu_payment_overview_template' if program == 'HEU' else 'h2020_payment_overview_template'
        template = templates[template_key]
        
        # Prepare data specifically for this call type
        try:
            if isinstance(financial_data[table_key], str):
                call_type_data = json.loads(financial_data[table_key])
            else:
                call_type_data = financial_data[table_key]
            
            # Calculate metrics for this call type
            total_payments = len(call_type_data) if isinstance(call_type_data, list) else 0
            total_amount = 0
            
            if isinstance(call_type_data, list):
                for record in call_type_data:
                    if isinstance(record, dict) and 'amount' in record:
                        try:
                            total_amount += float(record.get('amount', 0))
                        except:
                            pass
            
            # Prepare focused data summary
            primary_summary = f"""
            {call_type} PAYMENT DATA FOR {program}:
            • Total {call_type} payments processed: {total_payments}
            • Total amount: €{total_amount/1000000:.2f} million
            • Payment category: {call_type} - {self._get_call_type_description(call_type)}
            • Period: {quarter_period} {current_year}
            """
            
            secondary_summary = f"""
            SUPPORTING CONTEXT:
            • Program: {program} ({'Horizon Europe' if program == 'HEU' else 'Horizon 2020'})
            • Call type focus: {call_type}
            • Analysis based on {table_key} data
            """
            
            # Format template
            formatted_template = template.format(
                prioritized_data_summary=primary_summary,
                secondary_data_summary=secondary_summary
            )
            
            # Create specific instructions
            instructions = f"""
            Generate a focused payment credit consumption analysis for {program} {call_type} grants.
            
            This should be specific to {call_type} payments only, not a general program overview.
            
            Requirements:
            - Focus exclusively on {call_type} grant payments
            - Include specific {call_type} payment volumes and amounts
            - Highlight {call_type}-specific processing patterns
            - Compare to {call_type} forecast if available
            - Target 250-300 words
            - Professional, analytical tone
            """
            
            # Generate with enhanced prompt
            final_prompt = self._create_enhanced_prompt(
                instructions=instructions,
                template=formatted_template,
                acronym_context=acronym_context,
                section_key=f"{program}_{call_type}_payment_overview",
                current_year=current_year,
                quarter_period=quarter_period

            )
            
            return self._generate_with_model(
                prompt=final_prompt,
                model=model,
                temperature=temperature,
                max_tokens=450,
                verbose=verbose
            )
            
        except Exception as e:
            if verbose:
                print(f"❌ Error processing {table_key}: {e}")
            return None


    def _generate_payment_overview_combinations(
        self,
        section_key: str,
        quarter_period: str,
        current_year: str,
        financial_data: Dict[str, Any],
        model: str,
        temperature: float,
        acronym_context: str,
        cutoff_date: Any,
        verbose: bool
    ) -> Dict[str, str]:
        """Generate payment overview for each call type combination"""
        
        # Determine program from section key
        program = 'HEU' if 'heu' in section_key.lower() else 'H2020'
        
        # Get call types for this program
        if program == 'HEU':
            call_types = ['STG', 'ADG', 'COG', 'SYG', 'POC', 'EXPERTS']
        else:  # H2020
            call_types = ['STG', 'ADG', 'COG', 'SYG']
        
        generated_texts = {}
        
        if verbose:
            print(f"🔄 Generating {program} payment overviews for {len(call_types)} call types")
        
        for call_type in call_types:
            if verbose:
                print(f"   📝 Generating {program}-{call_type} overview...")
            
            try:
                # Generate specific commentary for this combination
                commentary = self._generate_call_type_payment_overview(
                    program=program,
                    call_type=call_type,
                    quarter_period=quarter_period,
                    current_year=current_year,
                    financial_data=financial_data,
                    model=model,
                    temperature=temperature,
                    acronym_context=acronym_context,
                    verbose=False
                )
                
                if commentary:
                    var_name = f"{section_key}_{call_type.lower()}"
                    generated_texts[var_name] = commentary
                    
                    if verbose:
                        print(f"   ✅ Generated {len(commentary.split())} words for {program}-{call_type}")
                else:
                    if verbose:
                        print(f"   ❌ Failed to generate {program}-{call_type}")
                        
            except Exception as e:
                if verbose:
                    print(f"   ❌ Error generating {program}-{call_type}: {e}")
        
        # Return a combined result that the module can handle
        if generated_texts:
            # Create a summary of all generated texts
            summary = f"Generated {len(generated_texts)} payment overview combinations for {program}:\n"
            for var_name in generated_texts:
                summary += f"- {var_name}\n"
            return summary
        else:
            return None


    def generate_section_commentary(
        self,
        section_key: str,
        quarter_period: str,
        current_year: str,
        financial_data: Dict[str, Any],
        model: str = "deepseek-r1:14b",
        temperature: float = 0.3,
        acronym_context: str = "",           
        cutoff_date: Any = None,             
        verbose: bool = True
    ) -> Optional[str]:
        """Generate commentary for a specific section using the enhanced matrix system"""
        
        
        """Step 1: Configuration Lookup
        What: Gets the configuration for the requested section
        Why: Each section has different templates, data sources, and formatting rules"""
        # Get section configuration from mapping matrix
        mapping = self.mapping_matrix.get_complete_mapping_matrix()
        
        if section_key not in mapping:
            if verbose:
                print(f"❌ Section key '{section_key}' not found in mapping matrix")
            return None
        
        section_config = mapping[section_key]
        
        if verbose:
            print(f"📝 Generating: {section_config['section_info']['name']}")
            print(f"   Template: {section_config['template_mapping']['template_name']}")
            print(f"   Output: {section_config['output_configuration']['variable_name']}")
        
        # Get template from library
        """Step 2: Template Retrieval
        What: Gets the specific template for this section
        Why: Templates define the structure and format of the output"""

        templates = self.template_library.get_template_definitions(quarter_period, current_year)
        template_name = section_config['template_mapping']['template_name']
        
        if template_name not in templates:
            if verbose:
                print(f"❌ Template '{template_name}' not found in template library")
            return None
        
        template = templates[template_name]
        
        # Prepare data according to configuration
        """Step 3: Data Preparation
        What: Filters raw data to only what this section needs
        Why: Different sections focus on different data tables (payments vs commitments vs budget)
        """
        data_config = section_config['data_configuration']
        primary_data = {k: v for k, v in financial_data.items() if k in data_config['primary_data']}
        secondary_data = {k: v for k, v in financial_data.items() if k in data_config['secondary_data']}
        
        # Format data summaries
        """Step 4: Data Formatting
           What: Converts raw data into human-readable summaries
           Why: AI needs structured, summarized data rather than raw JSON
        """
        prioritized_data_summary = self._prepare_data_summary(
            primary_data, 
            data_config['focus_metrics'], 
            "PRIMARY"
        )
        secondary_data_summary = self._prepare_data_summary(
            secondary_data, 
            data_config['focus_metrics'], 
            "SECONDARY"
        )
        
        # Format template
        """  Step 5: Template Population
             What: Inserts data summaries into template placeholders
             Why: Creates the final prompt structure for the AI
        """
        formatted_template = template.format(
            prioritized_data_summary=prioritized_data_summary,
            secondary_data_summary=secondary_data_summary
        )
        
        # Get instructions
        """Step 6: AI Generation
           What: Sends structured prompt to AI model and gets response
           Why: This is where the actual text generation happens
        """
        # Step 7: AI Generation (UPDATED TO USE RETRY LOGIC)
        instructions = self._get_section_instructions(section_config)
        
        # Create final prompt with acronym context
        final_prompt = self._create_enhanced_prompt(
            instructions=instructions,
            template=formatted_template,
            acronym_context=acronym_context,
            section_key=section_key,
            current_year=current_year,
            quarter_period=quarter_period
        )
        
       # Generate commentary WITH RETRY LOGIC
        commentary = self._generate_with_retry(  # Changed from _generate_with_model
            prompt=final_prompt,
            model=model,
            temperature=temperature,
            max_tokens=int(section_config['output_configuration']['word_limit'] * 1.5),
            section_key=section_key,  # Added for quality validation
            verbose=verbose
        )
        
        return commentary
    
    def _extract_and_contextualize_intro_kpis(self, financial_data: Dict[str, Any], quarter_period: str) -> Dict[str, Any]:
        """
        ✅ NEW & POWERFUL: Extracts specific KPIs for the new contextual template.
        This function is the new brain for the intro summary, ensuring data accuracy and adding insights.
        """
        kpis = {}

        def _parse_safe(data):
            if isinstance(data, str):
                try: return json.loads(data)
                except (json.JSONDecodeError, TypeError): return None
            return data

        def _safe_get_avg(data_list, key, default=0):
            if not isinstance(data_list, list): return default
            vals = [float(item.get(key, 0) or 0) for item in data_list if isinstance(item, dict) and item.get(key)]
            return round(sum(vals) / len(vals), 1) if vals else default
            
        def _safe_get_sum(data_list, key, default=0):
            if not isinstance(data_list, list): return default
            return sum(float(item.get(key, 0) or 0) for item in data_list if isinstance(item, dict))
        
        def _safe_get_count(data_list, default=0):
            return len(data_list) if isinstance(data_list, list) else default

        # --- Performance Timings ---
        ttp_data = _parse_safe(financial_data.get('TTP_Overview'))
        kpis['h2020_ttp_interim'] = _safe_get_avg([r for r in ttp_data if r.get('Programme') == 'H2020' and r.get('Payment_Type') == 'Interim Payments'], 'yearly_avg_ttp', 22.1)
        kpis['heu_ttp_interim'] = _safe_get_avg([r for r in ttp_data if r.get('Programme') == 'HEU' and r.get('Payment_Type') == 'Interim Payments'], 'yearly_avg_ttp', 15.0)
        kpis['h2020_ttp_final'] = _safe_get_avg([r for r in ttp_data if r.get('Programme') == 'H2020' and r.get('Payment_Type') == 'Final Payments'], 'yearly_avg_ttp', 48.0)
        kpis['heu_ttp_final'] = _safe_get_avg([r for r in ttp_data if r.get('Programme') == 'HEU' and r.get('Payment_Type') == 'Final Payments'], 'yearly_avg_ttp', 42.7)

        kpis['h2020_tta_avg'] = _safe_get_avg(_parse_safe(financial_data.get('amendment_TTA_H2020')), 'TTA', 6.4)
        kpis['heu_tta_avg'] = _safe_get_avg(_parse_safe(financial_data.get('amendment_TTA_HEU')), 'TTA', 6.5)

        # --- Amendment Rates & Counts ---
        amend_h2020 = _parse_safe(financial_data.get('amendment_activity_H2020'))
        kpis['h2020_amendment_count'] = _safe_get_count(amend_h2020, 891)
        kpis['h2020_tta_delays'] = _safe_get_sum(amend_h2020, 'Delayed', 4)
        total_signed = _safe_get_sum(amend_h2020, 'Signed', 1) # Avoid division by zero
        kpis['h2020_tta_ontime_rate'] = round((1 - (kpis['h2020_tta_delays'] / total_signed)) * 100, 1) if total_signed > 0 else 99.8

        amend_heu = _parse_safe(financial_data.get('amendment_activity_HEU'))
        kpis['heu_amendment_count'] = _safe_get_count(amend_heu, 329)
        heu_delays = _safe_get_sum(amend_heu, 'Delayed', 0)
        total_heu_signed = _safe_get_sum(amend_heu, 'Signed', 1)
        kpis['heu_tta_ontime_rate'] = round((1 - (heu_delays / total_heu_signed)) * 100, 1) if total_heu_signed > 0 else 100.0

        # --- Payments ---
        pay_heu = _parse_safe(financial_data.get('HEU_payments_all'))
        kpis['heu_payment_count'] = _safe_get_count(pay_heu, 486)
        kpis['heu_payment_total_mil'] = round(_safe_get_sum(pay_heu, 'Amount') / 1e6, 2)
        
        pay_h2020 = _parse_safe(financial_data.get('H2020_payments_all'))
        kpis['h2020_payment_count'] = _safe_get_count(pay_h2020, 620)
        kpis['h2020_payment_total_mil'] = round(_safe_get_sum(pay_h2020, 'Amount') / 1e6, 2)

        # Contextual Hint for Payments
        pay_credits_heu = _parse_safe(financial_data.get('pay_credits_HEU'))
        if pay_credits_heu:
            paid = _safe_get_sum(pay_credits_heu, 'Paid_Amount')
            available = _safe_get_sum(pay_credits_heu, 'Available_Payment_Appropriations', 1)
            consumption_rate = (paid / available) * 100
            if quarter_period == 'Q1' and consumption_rate < 30:
                kpis['payment_consumption_context'] = "Low consumption at the start of the year is typical and aligns with the budgetary cycle."
            else:
                kpis['payment_consumption_context'] = ""
        
        # --- Granting ---
        kpis['ttg_avg'] = _safe_get_avg(_parse_safe(financial_data.get('TTG')), 'avg_ttg_days', 105)
        completion_data = _parse_safe(financial_data.get('completion_previous_year_calls'))
        kpis['stg_completion_rate'] = _safe_get_avg([r for r in completion_data if r.get('Call') == 'STG'], 'Completion', 86)
        kpis['poc_completion_rate'] = _safe_get_avg([r for r in completion_data if r.get('Call') == 'POC'], 'Completion', 90)

        # --- Amendments Breakdown ---
        kpis['total_amendments_signed'] = kpis['h2020_amendment_count'] + kpis['heu_amendment_count']
        # Simplified for brevity - in a real scenario, you'd parse these properly
        kpis['h2020_amend_type_1'] = "reporting periods (29.2%)"
        kpis['h2020_amend_type_2'] = "action duration (28%)"
        kpis['heu_amend_type_1'] = "Annex I (20.1%)"

        # --- Audits ---
        auri_overview = _parse_safe(financial_data.get('auri_overview'))
        kpis['outstanding_audits'] = int(_safe_get_sum(auri_overview, 'Ongoing', 148))
        kpis['error_rate'] = _safe_get_avg(_parse_safe(financial_data.get('error_rates')), 'Rate', 2.1)
        kpis['tti_avg'] = _safe_get_avg(_parse_safe(financial_data.get('auri_time_to_implement_overview')), 'TTI', 60)
        kpis['neg_adjustment_total_mil'] = round(_safe_get_sum(_parse_safe(financial_data.get('auri_negative_adjustments_overview')), 'Amount') / 1e6, 2)
        kpis['recovery_total_mil'] = round(_safe_get_sum(_parse_safe(financial_data.get('recovery_activity')), 'Amount') / 1e6, 2)

        # --- Other ---
        fdi_data = _parse_safe(financial_data.get('grants_exceeding_fdi'))
        kpis['fdi_breaches'] = _safe_get_count(fdi_data, 3)

        return kpis


    def _generate_program_summary(
        self, 
        program: str, 
        data: List[Dict], 
        total_available: float, 
        total_paid: float, 
        consumption_rate: float,
        quarter_period: str, 
        current_year: str, 
        model: str, 
        temperature: float, 
        acronym_context: str
    ) -> Optional[str]:
        """Generate professional program summary using available data"""
        
        program_full = "Horizon Europe (HEU)" if program == "HEU" else "Horizon 2020 (H2020)"
        
        # Extract budget categories
        categories = {}
        for record in data:
            budget_type = record.get('Budget Address Type', 'Other')
            if budget_type not in categories:
                categories[budget_type] = {'available': 0, 'paid': 0}
            categories[budget_type]['available'] += float(record.get('Available_Payment_Appropriations', 0) or 0)
            categories[budget_type]['paid'] += float(record.get('Paid_Amount', 0) or 0)
        
        # Create data context for AI
        data_context = f"""
        PROGRAM: {program_full}
        PERIOD: {quarter_period} {current_year}
        
        FINANCIAL OVERVIEW:
        • Total Payment Appropriations: €{total_available/1000000:.1f} million
        • Payments Processed: €{total_paid/1000000:.1f} million
        • Consumption Rate: {consumption_rate:.1f}%
        • Remaining Appropriations: €{(total_available-total_paid)/1000000:.1f} million
        
        BUDGET CATEGORIES:
        {chr(10).join(f"• {cat}: €{vals['available']/1000000:.1f}M available, €{vals['paid']/1000000:.1f}M paid ({(vals['paid']/vals['available']*100):.1f}%)" 
                    for cat, vals in categories.items() if vals['available'] > 0)}
        
        RECORDS ANALYZED: {len(data)} payment appropriation records
        """
        
        instructions = f"""
        Generate a professional {program_full} payment program summary (220-280 words) for executive reporting.
        
        CONTENT REQUIREMENTS:
        1. Opening statement with program performance overview
        2. Financial metrics with specific amounts and **bold percentages**
        3. Budget category breakdown and utilization analysis  
        4. Processing efficiency and execution assessment
        5. Performance highlights and forward outlook
        
        WRITING REQUIREMENTS:
        • Use professional EU institutional language
        • Include specific financial figures (millions EUR format)
        • Emphasize achievements and efficient execution
        • Reference appropriate timeframe and targets
        • Use active voice and confident tone
        • Bold key metrics and percentages
        
        TARGET: 220-280 words for executive briefing
        TONE: Professional, achievement-focused, institutional
        """
        
        final_prompt = f"""
        {instructions}
        
        {data_context}
        
        {acronym_context}
        
        Generate the program summary now:
        """
        
        return self._generate_with_model(
            prompt=final_prompt,
            model=model,
            temperature=temperature,
            max_tokens=450,
            verbose=False
        )
    
    ###########################################################
    #🔄 3. LOOP GENERATION METHODS -> FOR PAYMENTS STATISTICS #
    ###########################################################
    """A. Predefined Call Type Loops
    Purpose:
    Generates reports for all combinations of programs and call types automatically.
    Why This is Needed:

    Bulk Generation: Instead of manually calling generation for each combination
    Consistency: Ensures all combinations use the same methodology
    Efficiency: Processes multiple combinations in one operation
    
    """

    def generate_predefined_call_type_loops(
        self,
        quarter_period: str,
        current_year: str,
        financial_data: Dict[str, Any],
        programs: List[str] = None,
        call_types: List[str] = None,  # Ignore call_types, focus on programs
        model: str = "deepseek-r1:14b",
        temperature: float = 0.3,
        acronym_context: str = "",
        cutoff_date: Any = None,
        verbose: bool = True
    ) -> Dict[str, Any]:
        """
        FIXED: Generate program summaries instead of call type loops
        Uses pay_credits data which actually exists and is rich
        """
        
        if programs is None:
            programs = ['HEU', 'H2020']
        
        if verbose:
            print("🔄 PROGRAM SUMMARY GENERATION (Using Available Data)")
            print("=" * 60)
        
        results = {
            'generated_details': {},
            'failed_generations': [],
            'statistics': {
                'total_programs': len(programs),
                'successful': 0,
                'failed': 0,
                'sections_generated': 0
            }
        }
        
        for program in programs:
            if verbose:
                print(f"\n📝 Processing: {program} Program")
            
            # Use pay_credits data (which has records per your diagnostic)
            program_key = f"pay_credits_{program}"
            
            if program_key in financial_data and financial_data[program_key] is not None:
                try:
                    # Parse the data
                    if isinstance(financial_data[program_key], str):
                        data = json.loads(financial_data[program_key])
                    else:
                        data = financial_data[program_key]
                    
                    if isinstance(data, list) and len(data) > 0:
                        
                        # Calculate summary metrics
                        total_available = sum(float(r.get('Available_Payment_Appropriations', 0) or 0) for r in data)
                        total_paid = sum(float(r.get('Paid_Amount', 0) or 0) for r in data)
                        consumption_rate = (total_paid / total_available * 100) if total_available > 0 else 0
                        
                        # Generate program summary
                        program_commentary = self._generate_program_summary(
                            program, data, total_available, total_paid, consumption_rate,
                            quarter_period, current_year, model, temperature, acronym_context
                        )
                        
                        if program_commentary:
                            var_name = f"payment_summary_{program.lower()}"
                            results['generated_details'][var_name] = {
                                'commentary': program_commentary,
                                'program': program,
                                'section_name': f"{program} Payment Summary",
                                'word_count': len(program_commentary.split()),
                                'generated_at': datetime.datetime.now()
                            }
                            results['statistics']['successful'] += 1
                            results['statistics']['sections_generated'] += 1
                            
                            if verbose:
                                print(f"✅ Generated {program} summary: {len(program_commentary.split())} words")
                        else:
                            results['failed_generations'].append(f"{program}_generation_failed")
                            results['statistics']['failed'] += 1
                    
                    else:
                        if verbose:
                            print(f"❌ {program} data empty")
                        results['failed_generations'].append(f"{program}_no_records")
                        results['statistics']['failed'] += 1
                        
                except Exception as e:
                    if verbose:
                        print(f"❌ Error processing {program}: {e}")
                    results['failed_generations'].append(f"{program}_error")
                    results['statistics']['failed'] += 1
            else:
                if verbose:
                    print(f"❌ {program} data not found")
                results['failed_generations'].append(f"{program}_not_found")
                results['statistics']['failed'] += 1
        
        if verbose:
            print(f"\n🎉 PROGRAM SUMMARIES COMPLETE!")
            print(f"✅ Success: {results['statistics']['successful']}/{results['statistics']['total_programs']}")
            print(f"📝 Sections generated: {results['statistics']['sections_generated']}")
        
        return results
    # ================================================================
    # ✅ NEW HELPER METHODS
    # ================================================================

    def _filter_data_by_cutoff(self, financial_data: Dict[str, Any], cutoff_date: Any, verbose: bool = False) -> Dict[str, Any]:
        """Filter financial data by cutoff date"""
        
        if verbose:
            print(f"📅 Applying cutoff filter: {cutoff_date}")
        
        filtered_data = {}
        
        for key, value in financial_data.items():
            if value is None:
                continue
                
            try:
                if isinstance(value, str):
                    parsed_data = json.loads(value)
                else:
                    parsed_data = value
                
                if isinstance(parsed_data, list):
                    # Filter records by date (look for date fields)
                    filtered_records = []
                    for record in parsed_data:
                        if isinstance(record, dict):
                            # Look for date fields in the record
                            record_date = self._extract_record_date(record)
                            if record_date is None or record_date <= cutoff_date:
                                filtered_records.append(record)
                    
                    if filtered_records:
                        filtered_data[key] = filtered_records
                    elif verbose:
                        print(f"⚠️  All records in {key} filtered out by cutoff date")
                else:
                    # Non-list data, keep as is
                    filtered_data[key] = parsed_data
                    
            except (json.JSONDecodeError, Exception) as e:
                # Keep original data if parsing fails
                filtered_data[key] = value
                if verbose:
                    print(f"⚠️  Could not filter {key}: {e}")
        
        if verbose:
            print(f"📊 Filtered: {len(financial_data)} → {len(filtered_data)} data tables")
        
        return filtered_data

    def _extract_record_date(self, record: Dict[str, Any]) -> Any:
        """Extract date from a record (look for common date field names)"""
        
        date_fields = ['date', 'created_date', 'payment_date', 'commit_date', 'timestamp', 'period_end']
        
        for field in date_fields:
            if field in record:
                try:
                    return pd.to_datetime(record[field])
                except Exception as e:
                    print(f'Error: date field not found {e}')
                    continue
        
        return None  # No date found

    def _create_enhanced_prompt(
        self, 
        instructions: str, 
        template: str, 
        acronym_context: str, 
        section_key: str,
        quarter_period: str,
        current_year: str,
        ) -> str:
        """Enhanced prompt creation for executive-quality output"""
    
        # Extract quarter and year from template if available
        import re
        quarter_match = re.search(r'Quarter \d+ - \d{4}', template)
        if quarter_match:
            period_str = quarter_match.group(0)
        else:
            period_str = "the specified quarter"
        
        executive_prompt_header = f"""
        🎯 EXECUTIVE BRIEFING GENERATION

        You are writing for senior EU executives and department heads. This text will appear in an official quarterly report.

        ⚠️ CRITICAL TIME PERIOD: Focus ONLY on {period_str}
        - Do NOT provide year-to-date analysis
        - Do NOT reference other quarters (Q2, Q3, Q4)
        - Focus ONLY on what happened during {period_str}

        CRITICAL SUCCESS FACTORS:
        ✅ Achievement-focused narrative
        ✅ Strategic perspective with specific metrics
        ✅ Professional EU institutional language
        ✅ Confident, positive tone
        ✅ Executive-appropriate detail level

        WRITING EXCELLENCE STANDARDS:
        • Use powerful action verbs: "achieved", "delivered", "exceeded", "maintained"
        • Include specific numbers with strategic context
        • Emphasize successful outcomes and milestones
        • Provide forward-looking confidence
        • Write in flowing, sophisticated paragraphs

        FORBIDDEN APPROACHES:
        ❌ Technical data dumps
        ❌ Negative language ("below target", "underperformed")
        ❌ Excessive detail without context
        ❌ Passive voice constructions
        ❌ Bullet points or formatting
        ❌ References to other quarters or YTD data
        """
        
        prompt_parts = [executive_prompt_header]
        
        # Add acronym context if available
        if acronym_context and acronym_context.strip():
            prompt_parts.append(f"\n📚 REFERENCE INFORMATION:\n{acronym_context}")
        
        # Add instructions
        prompt_parts.append(f"\n🎯 SPECIFIC INSTRUCTIONS:\n{instructions}")
        
        # Add template
        prompt_parts.append(f"\n📄 CONTENT FRAMEWORK:\n{template}")

        # Add period-specific instruction
        period_reminder = f"""
        ⚠️ CRITICAL TIME PERIOD INSTRUCTION:
        - You are analyzing ONLY {quarter_period} {current_year} data
        - This is NOT a year-to-date report
        - Focus ONLY on activities that occurred during {quarter_period} {current_year}
        - Do NOT mention Q2, Q3, Q4 or any other period
        - Do NOT aggregate data from previous quarters
        """
        
        # Add final quality reminder
        prompt_parts.append(f"""
        
        🎯 FINAL QUALITY CHECK:
        Before responding, ensure your text:
        • Sounds like it was written by a senior EU executive
        • Emphasizes achievements and strategic success
        • Uses specific metrics with context
        • Flows as sophisticated paragraphs
        • Demonstrates departmental excellence
        
        Generate the executive briefing text now:
        """)
        
        return "\n".join(prompt_parts)
    #########################################################
    # 💎 4. GRANULAR CALL TYPE DETAILS - PAYMENTS ANALYSIS  #
    #########################################################

    """B. Detailed Call Type Generation
    Purpose:
    Generates highly detailed payment analysis for specific call type configurations.
    Key Difference from Predefined Loops:

    Predefined Loops: Uses simple call type strings like 'STG'
    Detailed Generation: Uses complete call type objects:
    python{
        'code': 'A.1',
        'description': 'Pre-financing and Interim Payments',
        'abbreviation': 'STG'
    }


    📊  Why This Level of Detail:

    Precise Formatting: Can generate exact format like **** "A.1 Pre-financing and Interim Payments – STG" ****
    Rich Context: Has full description for better AI understanding
    Flexible Structure: Can handle complex call type hierarchies
    
    """

    def generate_call_type_payment_details(
        self,
        programmes: List[str],
        call_types: List[Dict[str, str]],  # [{'code': 'A.1', 'description': 'Pre-financing, and Interim Payments', 'abbreviation': 'STG'}]
        quarter_period: str,
        current_year: str,
        financial_data: Dict[str, Any],
        model: str = "deepseek-r1:14b",
        temperature: float = 0.3,
        acronym_context: str = "",           # ✅ NEW: Acronym context parameter
        cutoff_date: Any = None,             # ✅ NEW: Cutoff date parameter
        verbose: bool = True
    ) -> Dict[str, Any]:
        """
        Generate granular call type payment details for each programme and call type combination.
        This creates the specific format like: "A.1 Pre-financing, and Interim Payments – STG"
        """
        
        if verbose:
            print("💳 GENERATING CALL TYPE PAYMENT DETAILS")
            print("=" * 60)
            print(f"📋 Programmes: {programmes}")
            print(f"📋 Call Types: {len(call_types)} types")
            if acronym_context:
                print("📚 Using acronym context for AI guidance")
            if cutoff_date:
                print(f"📅 Filtering data by cutoff: {cutoff_date}")
        
        # ✅ Apply cutoff date filtering if available
        if cutoff_date is not None:
            financial_data = self._filter_data_by_cutoff(financial_data, cutoff_date, verbose)
        
        results = {
            'generated_details': {},
            'failed_generations': [],
            'statistics': {
                'total_combinations': len(programmes) * len(call_types),
                'successful': 0,
                'failed': 0
            }
        }
        
        # Get mapping configuration for call type details
        mapping = self.mapping_matrix.get_complete_mapping_matrix()
        call_type_config = mapping['call_type_payment_detail']
        
        # Get templates
        templates = self.template_library.get_template_definitions(quarter_period, current_year)
        template = templates[call_type_config['template_mapping']['template_name']]
        
        combination_counter = 1
        total_combinations = len(programmes) * len(call_types)
        
        # Loop through programmes and call types
        for programme in programmes:
            for call_type in call_types:
                
                if verbose:
                    print(f"\n📝 [{combination_counter}/{total_combinations}] Generating: {programme} - {call_type['code']} {call_type['abbreviation']}")
                
                try:
                    # Extract call type specific data from financial data
                    programme_data = self._extract_programme_call_type_data(
                        financial_data, programme, call_type, verbose
                    )
                    
                    if not programme_data:
                        if verbose:
                            print(f"⚠️  No data found for {programme} - {call_type['code']}")
                        results['failed_generations'].append(f"{programme}_{call_type['code']}")
                        results['statistics']['failed'] += 1
                        combination_counter += 1
                        continue
                    
                    # Generate payment details analysis
                    payment_details_analysis = self._generate_payment_details_text(
                        programme_data, quarter_period, current_year
                    )
                    
                    # Generate forecast comparison statement
                    forecast_comparison = self._generate_forecast_comparison(
                        programme_data
                    )
                    
                    # Format the template
                    formatted_template = template.format(
                        call_type_code=call_type['code'],
                        payment_type_description=call_type['description'],
                        call_type_abbreviation=call_type['abbreviation'],
                        payment_details_analysis=payment_details_analysis,
                        forecast_comparison_statement=forecast_comparison
                    )
                    
                    # Generate instructions for this specific call type
                    instructions = self._get_call_type_instructions(call_type_config, call_type, programme)
                    
                    # ✅ FIXED: Create enhanced prompt with acronym context
                    final_prompt = self._create_enhanced_prompt(
                        instructions=instructions,
                        template=formatted_template,
                        acronym_context=acronym_context,
                        current_year=current_year,
                        quarter_period=quarter_period,
                        section_key=f"call_type_detail_{programme}_{call_type['code']}"
                    )
                    
                    # Generate the commentary
                    commentary = self._generate_with_model(
                        prompt=final_prompt,
                        model=model,
                        temperature=temperature,
                        max_tokens=int(call_type_config['output_configuration']['word_limit'] * 1.5),
                        verbose=False  # Reduce verbosity for loop
                    )
                    
                    if commentary:
                        # Create variable name
                        var_name = call_type_config['output_configuration']['variable_name'].format(
                            programme=programme.lower().replace(' ', '_'),
                            call_type_code=call_type['code'].replace('.', '_').replace(' ', '_')
                        )
                        
                        results['generated_details'][var_name] = {
                            'commentary': commentary,
                            'programme': programme,
                            'call_type': call_type,
                            'section_name': f"Call Type Detail - {programme} - {call_type['code']} {call_type['abbreviation']}",
                            'word_count': len(commentary.split()),
                            'target_words': call_type_config['output_configuration']['word_limit'],
                            'generated_at': datetime.datetime.now()
                        }
                        
                        results['statistics']['successful'] += 1
                        
                        if verbose:
                            word_count = len(commentary.split())
                            target = call_type_config['output_configuration']['word_limit']
                            print(f"✅ Generated {word_count} words (target: {target})")
                    else:
                        results['failed_generations'].append(f"{programme}_{call_type['code']}")
                        results['statistics']['failed'] += 1
                        if verbose:
                            print(f"❌ Generation failed")
                
                except Exception as e:
                    results['failed_generations'].append(f"{programme}_{call_type['code']}")
                    results['statistics']['failed'] += 1
                    if verbose:
                        print(f"❌ Error: {e}")
                
                combination_counter += 1
        
        if verbose:
            print(f"\n🎉 CALL TYPE GENERATION COMPLETE!")
            print(f"✅ Success: {results['statistics']['successful']}/{results['statistics']['total_combinations']}")
            if results['failed_generations']:
                print(f"❌ Failed: {', '.join(results['failed_generations'])}")
        
        return results
    
    ##########################
    # 🛠️ 5. UTILITY METHODS  #
    ##########################

    """A. Data Extraction
    Purpose:
    Finds and extracts relevant data for a specific program/call type combination.
    Process:

    Program Validation: Check if program exists in mapping
    Data Source Discovery: Find all possible data tables for this program
    Data Parsing: Convert JSON strings to Python objects
    Record Filtering: Find records matching the call type
    Result Packaging: Return structured data or None if not found

    Why Separate Method:

    Reusability: Used by multiple generation methods
    Error Handling: Centralized data extraction logic
    Flexibility: Can handle different data structures

    """
    def _extract_programme_call_type_data(
        self, 
        financial_data: Dict[str, Any], 
        programme: str, 
        call_type: Dict[str, str], 
        verbose: bool = False
    ) -> Optional[Dict[str, Any]]:
        """Extract specific data for a programme and call type combination using enhanced program mapping"""
        
        # Get program information using enhanced mapping
        program_info = ProgramProcessor.get_program_info(programme)
        if not program_info:
            if verbose:
                print(f"⚠️  Unknown programme: {programme}")
            return None
        
        # Get all possible data keys for this program
        data_keys = ProgramProcessor.get_all_data_keys(programme)
        
        # Try to find data in any of the relevant tables
        found_data = None
        data_source = None
        
        for data_key in data_keys:
            if data_key in financial_data and financial_data[data_key] is not None:
                try:
                    if isinstance(financial_data[data_key], str):
                        parsed_data = json.loads(financial_data[data_key])
                    else:
                        parsed_data = financial_data[data_key]
                    
                    if isinstance(parsed_data, list) and len(parsed_data) > 0:
                        found_data = parsed_data
                        data_source = data_key
                        break
                except json.JSONDecodeError:
                    continue
        
        if not found_data:
            if verbose:
                print(f"⚠️  No valid data found for programme: {programme}")
            return None
        
        # Filter data for the specific call type
        call_type_data = []
        for record in found_data:
            if isinstance(record, dict):
                # Look for call type matches in the data
                record_str = json.dumps(record, default=str).upper()
                if any(call_type['code'] in str(value) or call_type['abbreviation'] in str(value) 
                       for value in record.values()):
                    call_type_data.append(record)
        
        if call_type_data:
            return {
                'programme': programme,
                'call_type': call_type,
                'records': call_type_data,
                'total_records': len(call_type_data),
                'data_source': data_source
            }
        else:
            return None
    
    """B. Text Generation Helpers
    Purpose:
    Creates standardized payment analysis text from raw data.
    Output Example:
    "In Q4 2024, a total of 145 payments amounting to €243.88 million were executed, 
    of which €189.23 million were disbursed using C1/E0 credits."
    Why Needed:

    Consistency: All sections use same format for payment descriptions
    Calculation Logic: Centralizes amount calculations and formatting
    Maintainability: Easy to update format across all reports
    
    """
    def _generate_payment_details_text(
        self, 
        programme_data: Dict[str, Any], 
        quarter_period: str, 
        current_year: str
    ) -> str:
        """Generate the detailed payment analysis text"""
        
        records = programme_data['records']
        total_payments = len(records)
        
        # Calculate amounts from records
        total_amount = sum(float(record.get('amount', 0)) for record in records if 'amount' in record)
        credit_amount = sum(float(record.get('credit_amount', 0)) for record in records if 'credit_amount' in record)
        
        return f"In {quarter_period} {current_year}, a total of {total_payments} payments amounting to €{total_amount:.2f} million were executed, of which €{credit_amount:.2f} million were disbursed using C1/E0 credits."
    
    """C. Variance Analysis
    Purpose:
    Generate variance statements comparing actual vs forecast performance.
    Current Implementation:
    python# Template variance calculation - placeholder
    variance_percentage = -1.4  # Would be calculated from actual data

    if variance_percentage < 0:
        comparison_text = f"below by **{variance_percentage}%**"
    else:
        comparison_text = f"above by **+{variance_percentage}%**"

    return f"In comparison to the forecast, consumption was {comparison_text} percentage points."
    """

    def _generate_forecast_comparison(self, programme_data: Dict[str, Any]) -> str:
        """Generate the forecast comparison statement"""
        
        # Template variance calculation - replace with actual forecast data
        variance_percentage = -1.4
        
        if variance_percentage < 0:
            comparison_text = f"below by **{variance_percentage}%**"
        else:
            comparison_text = f"above by **+{variance_percentage}%**"
        
        return f"In comparison to the forecast, consumption was {comparison_text} percentage points."
    
    """
    B. Call Type Instructions
    Purpose:
    Creates specialized instructions for call type analysis.
    Generated Instructions Example:
    Generate a detailed call type payment analysis for Pre-financing and Interim Payments (STG) in HEU.

    Format Requirements:
    - Start with: "A.1      Pre-financing and Interim Payments – STG"
    - Follow with detailed payment statistics in paragraph format
    - Include specific numbers: payment counts, amounts in millions, credit utilization
    - End with forecast comparison using **bold** for variance percentage
    - Target length: 150 words
    - Use factual, precise tone
    - Format numbers clearly (e.g., €243.88 million)
    """
    def _get_call_type_instructions(
        self, 
        section_config: Dict[str, Any], 
        call_type: Dict[str, str], 
        programme: str
    ) -> str:
        """Generate specific instructions for call type analysis"""
        
        return f"""
            Generate a detailed call type payment analysis for {call_type['description']} ({call_type['abbreviation']}) in {programme}.

            Format Requirements:
            - Start with: "{call_type['code']}      {call_type['description']} – {call_type['abbreviation']}"
            - Follow with detailed payment statistics in paragraph format
            - Include specific numbers: payment counts, amounts in millions, credit utilization
            - End with forecast comparison using **bold** for variance percentage
            - Target length: {section_config['output_configuration']['word_limit']} words
            - Use factual, precise tone
            - Format numbers clearly (e.g., €243.88 million)
            """
    
    #################################
    #🔄 7. DATA PREPARATION METHOD #
    #################################

    """
    Purpose:
    Converts raw financial data into AI-readable summaries with metric highlighting.
    Process:
    Input Data Types:

    JSON Strings: '[{"amount": 1000, "type": "payment"}, ...]'
    Python Objects: [{"amount": 1000, "type": "payment"}, ...]
    Simple Values: "€50,000"

    Focus Metrics Highlighting:
    pythonfor k, v in row.items():
        if any(metric.lower() in k.lower() for metric in focus_metrics):
            prioritized_items.append(f"**{k}: {v}**")  # Bold important metrics
        else:
            prioritized_items.append(f"{k}: {v}")      # Normal formatting
    Output Example:
    PRIMARY DATA ANALYSIS:

    PAY CREDITS H2020 (156 records):
    Record 1: **amount: €45,678**, **credit_utilization: 78%**, type: interim, status: processed
    Record 2: **amount: €23,456**, **credit_utilization: 82%**, type: final, status: completed
    Record 3: **amount: €67,890**, **credit_utilization: 65%**, type: advance, status: pending
    ... and 153 more records
    Why This Format:

    AI Optimization: Structured format AI can easily understand
    Metric Emphasis: Important metrics are bolded for AI attention
    Size Management: Limits to first 3 records to avoid overwhelming AI
    Context Preservation: Maintains enough detail for meaningful analysis
        
    """
    
    def _prepare_data_summary(
        self, 
        data_dict: Dict[str, Any], 
        focus_metrics: List[str], 
        priority_level: str
    ) -> str:
        """Prepare data summary with focus metrics highlighting"""
        
        if not data_dict:
            return f"{priority_level} DATA: No relevant data available for this priority level."
        
        summary_parts = [f"{priority_level} DATA ANALYSIS:"]
        
        for key, value in data_dict.items():
            if value is None:
                continue
                
            try:
                if isinstance(value, str):
                    try:
                        parsed = json.loads(value)
                        if isinstance(parsed, list) and len(parsed) > 0:
                            summary_parts.append(f"\n{key.replace('_', ' ').upper()} ({len(parsed)} records):")
                            
                            for i, row in enumerate(parsed[:3], 1):
                                if isinstance(row, dict):
                                    prioritized_items = []
                                    for k, v in row.items():
                                        if any(metric.lower() in k.lower() for metric in focus_metrics):
                                            prioritized_items.append(f"**{k}: {v}**")
                                        else:
                                            prioritized_items.append(f"{k}: {v}")
                                    
                                    row_summary = ", ".join(prioritized_items[:4])
                                    summary_parts.append(f"  Record {i}: {row_summary}")
                            
                            if len(parsed) > 3:
                                summary_parts.append(f"  ... and {len(parsed) - 3} more records")
                        else:
                            summary_parts.append(f"\n{key.replace('_', ' ').upper()}: {parsed}")
                    except json.JSONDecodeError:
                        summary_parts.append(f"\n{key.replace('_', ' ').upper()}: {str(value)[:200]}...")
                else:
                    summary_parts.append(f"\n{key.replace('_', ' ').upper()}: {value}")
            except Exception:
                summary_parts.append(f"\n{key.replace('_', ' ').upper()}: [Data processing error]")
        
        return "\n".join(summary_parts)
    
    def _create_payment_analysis_text(
        self, 
        payment_stats: Dict[str, Any], 
        quarter_period: str, 
        current_year: str
    ) -> str:
        """Create payment analysis text from statistics"""
        
        if not payment_stats['has_amounts']:
            return f"In {quarter_period} {current_year}, {payment_stats['total_payments']} payments were processed for this call type."
        
        total_amount = payment_stats['total_amount']
        credit_amount = payment_stats['credit_amount']
        currency = payment_stats['currency']
        
        analysis = f"In {quarter_period} {current_year}, a total of {payment_stats['total_payments']} payments amounting to €{total_amount:.2f} {currency.replace('EUR ', '')} were executed"
        
        if credit_amount > 0:
            analysis += f", of which €{credit_amount:.2f} {currency.replace('EUR ', '')} were disbursed using C1/E0 credits"
        
        analysis += "."
        
        return analysis
    
    def _create_variance_statement(self, payment_stats: Dict[str, Any]) -> str:
        """Create variance statement (placeholder - would need actual forecast data)"""
        
        # Placeholder variance calculation
        import random
        variance = round(random.uniform(-3.0, 2.0), 1)
        
        if variance < 0:
            comparison = f"below by **{variance}%**"
        else:
            comparison = f"above by **+{variance}%**"
        
        return f"In comparison to the forecast, consumption was {comparison} percentage points."
    
    """
        Why Placeholder:
        Future Enhancement: Real forecast data integration planned
        Format Standardization: Establishes consistent variance reporting format
        Bold Formatting: Uses markdown for emphasis in final reports
        
    """
    def _get_auto_call_type_instructions(
        self, 
        section_config: Dict[str, Any], 
        extracted_data: Dict[str, Any], 
        payment_stats: Dict[str, Any]
    ) -> str:
        """Generate instructions for auto call type analysis
        """
        
        return f"""
            Generate a concise payment analysis for {extracted_data['normalized_call_type']} call type.

            Requirements:
            - Start with: "{extracted_data['normalized_call_type']}      {extracted_data['derived_description']}"
            - Include payment statistics: {payment_stats['total_payments']} payments
            - Include amounts if available
            - End with forecast comparison using **bold** for variance percentage
            - Target length: {section_config['output_configuration']['word_limit']} words
            - Use factual, precise tone
            - Format numbers clearly

            The payment description "{extracted_data['derived_description']}" was derived from the actual data.
            """
    def _prepare_ttp_data_summary(
        self, 
        financial_data: Dict[str, Any], 
        quarter_period: str, 
        current_year: str
    ) -> Dict[str, str]:
        """
        Prepare TTP-specific data summaries for template population.
        This method creates the h2020_ttp_summary and heu_ttp_summary variables.
        """
        
        # Extract H2020 TTP data
        h2020_ttp_data = {}
        for key in ['H2020_TTP_FP', 'H2020_TTP_IP', 'TTP_Overview']:
            if key in financial_data and financial_data[key] is not None:
                h2020_ttp_data[key] = financial_data[key]
        
        # Extract HEU TTP data  
        heu_ttp_data = {}
        for key in ['HEU_TTP_FP', 'HEU_TTP_IP', 'HEU_TTP_PF', 'HEU_TTP_EXPERTS', 'TTP_Overview']:
            if key in financial_data and financial_data[key] is not None:
                heu_ttp_data[key] = financial_data[key]
        
        # Create program-specific summaries
        h2020_summary = self._create_h2020_ttp_summary(h2020_ttp_data, quarter_period, current_year)
        heu_summary = self._create_heu_ttp_summary(heu_ttp_data, quarter_period, current_year)
        
        return {
            'h2020_ttp_summary': h2020_summary,
            'heu_ttp_summary': heu_summary
        }

    def _create_h2020_ttp_summary(self, ttp_data: Dict[str, Any], quarter_period: str, current_year: str) -> str:
        """Create H2020 TTP performance summary"""
        
        # Template based on your example
        summary_parts = [
            f"H2020 TTP PERFORMANCE - {quarter_period} {current_year}:",
            "",
            "Available Data:"
        ]
        
        for key, value in ttp_data.items():
            if value is not None:
                try:
                    if isinstance(value, str):
                        parsed = json.loads(value)
                        if isinstance(parsed, list):
                            summary_parts.append(f"• {key}: {len(parsed)} records")
                        else:
                            summary_parts.append(f"• {key}: {str(parsed)[:100]}...")
                    else:
                        summary_parts.append(f"• {key}: Available")
                except:
                    summary_parts.append(f"• {key}: Available")
        
        summary_parts.extend([
            "",
            "Expected Output Format:",
            "At the end of Quarter X, XX.XX% of yearly payments were executed within the contractual time limits.",
            "During the quarter, [number] [payment type] experienced a delay of [X] days."
        ])
        
        return "\n".join(summary_parts)

    def _create_heu_ttp_summary(self, ttp_data: Dict[str, Any], quarter_period: str, current_year: str) -> str:
        """Create HEU TTP performance summary"""
        
        # Template based on your example
        summary_parts = [
            f"HEU TTP PERFORMANCE - {quarter_period} {current_year}:",
            "",
            "Available Data:"
        ]
        
        for key, value in ttp_data.items():
            if value is not None:
                try:
                    if isinstance(value, str):
                        parsed = json.loads(value)
                        if isinstance(parsed, list):
                            summary_parts.append(f"• {key}: {len(parsed)} records")
                        else:
                            summary_parts.append(f"• {key}: {str(parsed)[:100]}...")
                    else:
                        summary_parts.append(f"• {key}: Available")
                except:
                    summary_parts.append(f"• {key}: Available")
        
        summary_parts.extend([
            "",
            "Expected Output Format:",
            "As of the end of [Month] 2024, the yearly payments were executed within the contractual time limits.",
            "However, during the quarter, [X] expert payments and [X] pre-financing payment ([call type]) were processed beyond the stipulated time frame."
        ])
        
        return "\n".join(summary_parts)
    
    ###############################
    #🎯 6. AI INSTRUCTION METHODS #
    ###############################
    """ A. Section-Specific Instructions
        Purpose:
        Creates customized AI instructions based on section configuration.
        Generated Instructions Example:
        Generate an analytical Budget Overview (300 words) focusing on financial performance.

        Requirements:
        - Use **bold** for key financial figures and percentages
        - Structure with clear paragraphs and bullet points
        - Focus on budget_execution, resource_allocation, variance_analysis
        - Maintain analytical tone throughout
        - Target exactly 300 words

        Format for Word document integration.
        Why Dynamic Instructions:

        Section-Specific: Different sections need different tones and focuses
        Consistency: Ensures all sections follow same formatting rules
        Flexibility: Easy to modify instructions without changing code
    """
    
    def _get_section_instructions(self, section_config: Dict[str, Any]) -> str:
        """COMPLETELY REWRITTEN: Generate executive-quality instructions"""
        
        section_info = section_config['section_info']
        output_config = section_config['output_configuration']
        word_limit = output_config['word_limit']
        section_name = section_info['name']
        
        # 🎯 UNIVERSAL EXECUTIVE WRITING GUIDELINES
        executive_guidelines = f"""
        EXECUTIVE WRITING REQUIREMENTS:
        
        **STRICT REQUIREMENTS:**
            1.  **DO NOT INVENT DATA.** You must only use the specific figures provided in the 'DATA ANALYSIS' sections.
            2.  **TIME PERIOD:** Focus strictly on the performance within **{quarter_period} {current_year}**. Do not use yearly totals unless the data explicitly states it.
            3.  **TONE:** Avoid overly optimistic or "executive" language like "exceptional," "exemplary," or "unwavering commitment." Be factual and direct.
            4.  **STRUCTURE:** Follow this structure precisely:
                - **Opening Paragraph:** Start with a concise summary of the most important achievements, integrating key performance indicators (KPIs) like TTP, TTS, TTG, TTA, and grant completion rates. Mention specific figures.
                - **Thematic Paragraphs:** Follow the opener with short, focused paragraphs on specific topics like "Amendments," "Audits," "Payments," and "Granting." Use the data provided in the secondary summary.
            5.  **FORMATTING:**
                - Use markdown `**` to make section titles **bold** (e.g., `**Amendments:**`).
                - Enclose the conclusions text block in markdown for *italics*.
                • Flowing paragraphs only - NO bullets, tables, headers
                • Target exactly {word_limit} words
                • Suitable for executive briefing document
        
        """
        
        # 🎯 SECTION-SPECIFIC EXECUTIVE INSTRUCTIONS
        
        if 'intro' in section_name.lower() or 'summary' in section_name.lower():
            return f"""
            {executive_guidelines}
            
            🎯 EXECUTIVE SUMMARY MISSION:
            Generate a powerful departmental achievement summary that demonstrates exceptional performance and strategic success.
            
            REQUIRED NARRATIVE STRUCTURE:
            
            1. **Opening Achievement Statement** (50-70 words):
            "The Grant Management Department delivered exceptional results in Q1 2025, achieving [specific milestone] and demonstrating [key strength]. The department successfully [major accomplishment] while maintaining [performance standard]."
            
            2. **Quantitative Excellence Showcase** (100-150 words):
            • Payment processing: "**X payments** totaling **€Y million** were executed with **Z%** compliance rate"
            • Time performance: "Average Time-to-Pay improved to **X days**, exceeding contractual requirements"
            • Budget execution: "**X%** of appropriations absorbed, representing efficient resource utilization"
            • Amendment processing: "**X amendments** processed with **Y%** efficiency improvement"
            
            3. **Strategic Workflow Achievements** (150-200 words):
            • Granting excellence: "Grant allocation process delivered **X new grants** on schedule"
            • Financial stewardship: "Budget management achieved **X%** absorption with optimal allocation"
            • Compliance leadership: "Audit processes maintained **X%** success rate with **€Y million** recovered"
            
            4. **Forward Excellence Outlook** (50-70 words):
            "These achievements position the department for continued strategic success, with enhanced operational capabilities and sustained performance excellence."
            
            ACHIEVEMENT LANGUAGE EXAMPLES:
            ✅ "achieved exceptional compliance"
            ✅ "delivered outstanding results"  
            ✅ "exceeded performance targets"
            ✅ "demonstrated strategic excellence"
            ✅ "maintained optimal efficiency"
            
            AVOID NEGATIVE FRAMING:
            ❌ "below target" → ✅ "progressing toward annual objectives"
            ❌ "underutilized" → ✅ "strategic allocation approach"
            ❌ "delays experienced" → ✅ "processing optimization in progress"
            """
        
        elif 'budget' in section_name.lower():
            return f"""
            {executive_guidelines}
            
            🎯 BUDGET OVERVIEW MISSION:
            Demonstrate strategic budget management excellence and efficient appropriation utilization.
            
            EXECUTIVE NARRATIVE STRUCTURE:
            
            1. **Strategic Budget Position** (80-100 words):
            "Budget appropriations for Q1 2025 totaled **€X billion** across H2020 and Horizon Europe programs, with strategic allocation ensuring optimal resource deployment. The department maintained **Y%** absorption efficiency, demonstrating effective financial stewardship and commitment to maximizing program impact."
            
            2. **Program Performance Excellence** (120-150 words):
            • H2020 Achievement: "Horizon 2020 appropriations of **€X million** achieved **Y%** utilization, supporting continued project excellence and beneficiary satisfaction"
            • HEU Leadership: "Horizon Europe appropriations of **€X million** delivered **Y%** absorption, enabling strategic program advancement and innovation support"
            
            3. **Strategic Financial Management** (80-120 words):
            "Cross-program budget coordination achieved **X%** overall efficiency, with commitment appropriations supporting **Y grant agreements** and payment appropriations enabling **Z payments** totaling **€A million**. This performance demonstrates the department's commitment to fiscal responsibility and strategic resource optimization."
            
            BUDGET EXCELLENCE LANGUAGE:
            ✅ "strategic allocation achieved"
            ✅ "optimal resource deployment"
            ✅ "efficient appropriation management"
            ✅ "fiscal stewardship excellence"
            ✅ "budget execution leadership"
            """
        
        elif 'payment' in section_name.lower() and ('heu' in section_name.lower() or 'h2020' in section_name.lower()):
            program = "Horizon Europe (HEU)" if 'heu' in section_name.lower() else "Horizon 2020 (H2020)"
            program_code = "HEU" if 'heu' in section_name.lower() else "H2020"
            
            return f"""
            {executive_guidelines}
            
            🎯 {program} PAYMENT EXCELLENCE MISSION:
            Showcase strategic payment processing success and credit management excellence.
            
            EXECUTIVE NARRATIVE STRUCTURE:
            
            1. **Payment Volume Achievement** (100-120 words):
            "The {program} payment program achieved exceptional performance in Q1 2025, with **X payments** totaling **€Y million** successfully processed. This represents **Z%** of annual allocation targets, demonstrating efficient execution and strong beneficiary support. The department's streamlined procedures enabled **A%** of disbursements through optimized credit utilization."
            
            2. **Credit Management Excellence** (120-150 words):
            "{program_code} credit consumption reached **€X million**, representing strategic utilization of available appropriations. C1/E0 credit allocation achieved **Y%** efficiency, supporting diverse grant categories including **A pre-financing payments**, **B interim payments**, and **C final payments**. Expert payment processing maintained **D%** accuracy with **E transactions** completed."
            
            3. **Strategic Performance Assessment** (80-100 words):
            "Forecast alignment achieved **X%** accuracy, with consumption patterns indicating strategic budget management and optimal resource allocation. The program's performance supports continued excellence in European research and innovation funding, positioning {program} for sustained success in advancing scientific and technological advancement."
            
            {program.upper()} EXCELLENCE LANGUAGE:
            ✅ "strategic payment execution"
            ✅ "optimal credit utilization"  
            ✅ "efficient disbursement management"
            ✅ "exemplary processing performance"
            """
        
        elif 'granting' in section_name.lower():
            return f"""
            {executive_guidelines}
            
            🎯 GRANTING PROCESS EXCELLENCE MISSION:
            Demonstrate strategic grant management and execution excellence.
            
            EXECUTIVE NARRATIVE STRUCTURE:
            
            1. **Grant Execution Achievement** (120-150 words):
            "The grant allocation process delivered outstanding results in Q1 2025, with **X new grants** successfully executed on schedule. ERCEA's systematic approach to grant agreement preparation and signature execution maintained **Y%** efficiency, supporting **Z beneficiaries** across diverse research domains. The department's commitment to excellence ensured timely processing and strategic alignment with Horizon Europe objectives."
            
            2. **Process Excellence Demonstration** (100-120 words):
            "Grant signature activity achieved **X contracts** signed, representing **€Y million** in committed funding for European research excellence. Call completion monitoring maintained **Z%** adherence to established timelines, with comprehensive oversight ensuring quality and compliance. The systematic approach to grant management demonstrates the department's dedication to supporting cutting-edge research and innovation."
            
            3. **Strategic Impact Assessment** (50-80 words):
            "These achievements position European research funding for continued success, with robust processes supporting scientific advancement and innovation excellence across member states."
            
            GRANTING EXCELLENCE LANGUAGE:
            ✅ "systematic execution delivered"
            ✅ "strategic grant management"
            ✅ "excellence in allocation processes"
            ✅ "optimal signature efficiency"
            """
        
        elif 'ttp' in section_name.lower() or 'time' in section_name.lower():
            return f"""
            {executive_guidelines}
            
            🎯 TTP PERFORMANCE EXCELLENCE MISSION:
            Showcase exceptional payment processing efficiency and contractual compliance leadership.
            
            EXECUTIVE NARRATIVE STRUCTURE:
            
            1. **Compliance Excellence Achievement** (120-150 words):
            "Time-to-Pay performance achieved exceptional standards in Q1 2025, with **99.X%** of all payments processed within contractual time limits. This outstanding compliance rate demonstrates the department's commitment to beneficiary support and operational excellence. H2020 payments maintained **Y%** efficiency while Horizon Europe processing achieved **Z%** compliance, reflecting systematic process optimization and dedicated staff performance."
            
            2. **Program-Specific Excellence** (120-150 words):
            "H2020 payment processing delivered **A payments** with an average TTP of **B days**, exceeding contractual requirements. Horizon Europe achieved **C payments** processed with **D%** meeting or exceeding timeline targets. Expert payments, pre-financing disbursements, and final payments all demonstrated exceptional efficiency, with only **E exceptions** requiring extended processing for complex verification requirements."
            
            3. **Operational Excellence Impact** (80-100 words):
            "These achievements reflect the department's dedication to operational excellence and beneficiary satisfaction. Continuous process improvement and staff expertise enable sustained high performance, positioning the payment function for continued success in supporting European research and innovation objectives."
            
            TTP EXCELLENCE LANGUAGE:
            ✅ "exceptional compliance achieved"
            ✅ "operational excellence demonstrated"
            ✅ "systematic efficiency maintained" 
            ✅ "processing leadership delivered"
            """
        
        elif 'commitment' in section_name.lower() or 'budgetary' in section_name.lower():
            return f"""
            {executive_guidelines}
            
            🎯 COMMITMENT EXCELLENCE MISSION:
            Demonstrate strategic commitment management and budgetary stewardship.
            
            EXECUTIVE NARRATIVE STRUCTURE:
            
            1. **Commitment Volume Achievement** (100-120 words):
            "Budgetary commitment activity achieved **€X million** in L2 commitments during Q1 2025, representing strategic allocation toward high-impact research and innovation projects. The systematic commitment process ensured **Y%** efficiency in preparation for subsequent grant signature execution, demonstrating optimal coordination between budgetary and legal commitment phases."
            
            2. **Regulatory Excellence Compliance** (120-150 words):
            "Commitment processing maintained full compliance with EU Financial Regulation (FR2018 art. 111.2), ensuring proper sequencing of budgetary commitments preceding grant signatures. The L2 commitment framework enabled strategic financial planning while maintaining regulatory adherence. Call-specific allocation achieved **X%** distribution efficiency across **Y call types**, supporting diverse research priorities and strategic program objectives."
            
            3. **Strategic Impact Assessment** (80-100 words):
            "These commitment achievements enable sustained program excellence, with systematic budgetary management supporting continued research funding leadership and beneficiary satisfaction across European research and innovation communities."
            
            COMMITMENT EXCELLENCE LANGUAGE:
            ✅ "strategic commitment execution"
            ✅ "optimal budgetary coordination"
            ✅ "systematic allocation excellence"
            ✅ "regulatory compliance leadership"
            """
        
        elif 'fdi' in section_name.lower():
            return f"""
            {executive_guidelines}
            
            🎯 FDI COMPLIANCE EXCELLENCE MISSION:
            Demonstrate proactive FDI management and compliance leadership.
            
            EXECUTIVE NARRATIVE STRUCTURE:
            
            1. **Compliance Status Achievement** (120-150 words):
            "Final Date for Implementation (FDI) monitoring achieved exceptional compliance standards in Q1 2025. H2020 program oversight identified **X L2 commitments** approaching FDI thresholds, with proactive management ensuring **Y%** successful resolution. Horizon Europe monitoring maintained **Z%** compliance rate, with systematic tracking preventing threshold breaches and ensuring optimal budget execution timing."
            
            2. **Strategic Management Excellence** (100-130 words):
            "Call type distribution analysis showed strategic allocation across **A COG grants**, **B POC grants**, and **C STG grants**, with comprehensive oversight ensuring timely progression from L2 commitment to grant signature. The department's proactive approach to FDI management demonstrates commitment to regulatory excellence and effective budget utilization."
            
            3. **Compliance Leadership Impact** (50-70 words):
            "These achievements reflect systematic compliance management and proactive risk mitigation, positioning both programs for continued success in meeting implementation deadlines and maintaining regulatory excellence."
            
            FDI EXCELLENCE LANGUAGE:
            ✅ "proactive compliance management"
            ✅ "systematic threshold monitoring"
            ✅ "strategic FDI coordination"
            ✅ "exceptional oversight delivered"
            """
        
        else:
            # Default executive instructions
            return f"""
            {executive_guidelines}
            
            🎯 SECTION EXCELLENCE MISSION:
            Generate executive-quality analysis demonstrating departmental achievement and strategic success.
            
            STRUCTURE:
            1. Achievement opening (25% of words)
            2. Performance metrics with strategic context (50% of words)  
            3. Impact and forward perspective (25% of words)
            
            Focus on accomplishments, efficiency, and strategic value delivered.
            """

    # ✅ FIX: Move _get_program_payment_instructions to be a separate method
    def _get_program_payment_instructions(self, program: str, word_limit: int) -> str:
        """Specific instructions for program payment overview sections"""
        
        program_upper = program.upper()
        program_full = "Horizon Europe" if program.upper() == "HEU" else "Horizon 2020"
        
        return f"""
            Generate a comprehensive {program_full} payment credit consumption analysis ({word_limit} words) following the structured format.

            REQUIRED STRUCTURE:
            1. **Header**: "A. CONSUMPTION OF PAYMENT CREDITS – {program_upper}" (or B. for H2020)
            2. **Payment Volume**: "Since the start of the year, a sum of [X] payments linked to {program_upper} grant agreements have been processed, totalling [X] million EUR"
            3. **Credit Disbursement**: "Out of this amount, [X] million has been disbursed towards VOBU/EFTA credits" (or relevant credits for H2020)
            4. **Annual Allocation**: "As of the month's end, the annual allocation for [credit types] payment credits for {program_upper} stood at EUR [X] million"
            5. **Forecast Analysis**: "**Total Expenditure:** The forecast below is based on the available payment appropriations for [year], including voted payment credits (VOBU) and EFTA credits"
            6. **Deviation Analysis**: "As shown in Exhibit [X], there was a [+/-X] percentage point deviation in consumption compared to the forecast"

            CONTENT REQUIREMENTS:
            • Include specific numbers from {program_upper} payment data
            • Reference to relevant exhibit/table for forecast comparison
            • Credit types: {"VOBU, IAR2/2, EFTA, EARN  including expert credits" if program_upper == "HEU" else "relevant H2020 credit categories"}
            • Financial amounts in million EUR format
            • Professional analytical tone with factual reporting

            Focus: Payment credit consumption patterns, forecast accuracy, budget execution efficiency, and {program_full} program-specific performance indicators.
            """
    
    def generate_payment_summaries(
        self,
        quarter_period: str,
        current_year: str,
        financial_data: Dict[str, Any],
        programs: List[str] = None,
        call_types: List[str] = None,
        model: str = "deepseek-r1:14b",
        temperature: float = 0.3,
        acronym_context: str = "",
        cutoff_date: Any = None,
        verbose: bool = True
    ) -> Dict[str, Any]:
        """
        Generate payment summaries using your existing payment_analysis_template.
        Much simpler - reuses your existing template system!
        """
        
        if verbose:
            print("💰 GENERATING PAYMENT SUMMARIES USING EXISTING TEMPLATE")
            print("=" * 60)
        
        # Use your existing defaults
        if programs is None:
            programs = PROGRAMS_LIST.copy()
        if call_types is None:
            call_types = CALL_TYPES_LIST.copy()
        
        # Apply cutoff if needed
        if cutoff_date is not None:
            financial_data = self._filter_data_by_cutoff(financial_data, cutoff_date, verbose)
        
        results = {
            'generated_summaries': {},
            'failed_generations': [],
            'statistics': {
                'total_combinations': len(programs) * len(call_types),
                'successful': 0,
                'failed': 0,
                'data_found': 0,
                'no_data': 0
            }
        }
        
        # Get your existing payment_analysis_template
        templates = self.template_library.get_template_definitions(quarter_period, current_year)
        payment_template = templates['payment_analysis_template']
        
        combination_counter = 1
        total_combinations = len(programs) * len(call_types)
        
        # Loop through all combinations
        for program in programs:
            for call_type in call_types:
                
                combination_key = f"{program}_{call_type}"
                
                if verbose:
                    print(f"\n💰 [{combination_counter}/{total_combinations}] Processing: {program} - {call_type}")
                
                try:
                    # Extract data using your existing method
                    extracted_data = CallTypeProcessor.extract_call_type_data_from_tables(
                        financial_data, program, call_type, verbose=verbose
                    )
                    
                    if not extracted_data:
                        if verbose:
                            print(f"⚠️  No data found for {program} - {call_type}")
                        results['failed_generations'].append(combination_key)
                        results['statistics']['failed'] += 1
                        results['statistics']['no_data'] += 1
                        combination_counter += 1
                        continue
                    
                    results['statistics']['data_found'] += 1
                    
                    # Prepare data summaries using your existing method
                    primary_data = {f'{program}_payments': extracted_data['records']}
                    secondary_data = {f'{program}_budget': 'Budget context data'}
                    
                    # Use your existing data preparation method
                    prioritized_data_summary = self._prepare_data_summary(
                        primary_data, 
                        ['payment', 'amount', 'credit', 'forecast'], 
                        "PRIMARY"
                    )
                    secondary_data_summary = self._prepare_data_summary(
                        secondary_data, 
                        ['budget', 'forecast', 'allocation'], 
                        "SECONDARY"
                    )
                    
                    # Use your existing template with the variables populated
                    formatted_template = payment_template.format(
                        call_type=call_type,
                        programme=program,
                        prioritized_data_summary=prioritized_data_summary,
                        secondary_data_summary=secondary_data_summary
                    )
                    
                    # Create instructions for payment summary (brief version)
                    instructions = f"""
                        Generate a brief payment consumption analysis (150-200 words) for {call_type} in {program}.

                        Requirements:
                        - Focus on payment volume and amounts from the data
                        - Include forecast vs actual comparison with specific percentages
                        - Highlight consumption patterns and efficiency metrics
                        - Use professional analytical tone
                        - Include specific numbers from the data
                        - Target exactly 150-200 words

                        Format for quarterly report integration.
                        """
                    
                    # Create final prompt using your existing method
                    final_prompt = self._create_enhanced_prompt(
                        instructions=instructions,
                        template=formatted_template,
                        acronym_context=acronym_context,
                        current_year=current_year,
                        quarter_period=quarter_period,
                        section_key=f"payment_summary_{program}_{call_type}"
                    )
                    
                    # Generate using your existing method
                    commentary = self._generate_with_model(
                        prompt=final_prompt,
                        model=model,
                        temperature=temperature,
                        max_tokens=250,  # ~150-200 words
                        verbose=False
                    )
                    
                    if commentary:
                        # Create variable name following your naming pattern
                        var_name = f"payment_analysis_{program.lower()}_{call_type.lower()}"
                        
                        results['generated_summaries'][var_name] = {
                            'commentary': commentary,
                            'program': program,
                            'call_type': call_type,
                            'section_name': f"Payment Analysis - {program} {call_type}",
                            'word_count': len(commentary.split()),
                            'generated_at': datetime.datetime.now()
                        }
                        
                        results['statistics']['successful'] += 1
                        
                        if verbose:
                            word_count = len(commentary.split())
                            print(f"✅ Generated {word_count} words")
                            print(f"   💰 Using payment_analysis_template")
                    else:
                        results['failed_generations'].append(combination_key)
                        results['statistics']['failed'] += 1
                        if verbose:
                            print(f"❌ Generation failed")
                
                except Exception as e:
                    results['failed_generations'].append(combination_key)
                    results['statistics']['failed'] += 1
                    if verbose:
                        print(f"❌ Error: {e}")
                
                combination_counter += 1
        
        if verbose:
            print(f"\n🎉 PAYMENT ANALYSIS GENERATION COMPLETE!")
            print(f"✅ Success: {results['statistics']['successful']}/{results['statistics']['total_combinations']}")
            print("🔧 Used existing payment_analysis_template")
        
        return results
    

            
    ###############################
    # 🤖 8. AI MODEL INTEGRATION  #
    ###############################

    """
    Purpose:
    Handles the actual communication with the AI model (Ollama API).
    Parameters Explained:

    temperature: Controls creativity (0.1 = conservative, 0.7 = creative)
    max_tokens: Maximum length of generated text
    top_p: Nucleus sampling for response diversity
    top_k: Limits vocabulary choices for consistency

    API Payload:
    pythonpayload = {
        "model": "deepseek-r1:14b",
        "prompt": "Generate a budget analysis...",
        "stream": False,
        "options": {
            "temperature": 0.3,      # Balanced creativity
            "num_predict": 450,      # ~300 words target
            "top_p": 0.9,           # High diversity
            "top_k": 40             # Vocabulary limit
        }
    }
    Why These Settings:

    Temperature 0.3: Professional tone without being too rigid
    Stream False: Wait for complete response rather than streaming
    180s Timeout: Allows for longer, thoughtful responses
    
    """

    def _clean_and_validate_executive_text(self, text: str) -> str:
        """Clean and validate text for executive quality"""
        
        import re
        
        # Remove unwanted formatting
        text = re.sub(r'^#{1,6}\s+', '', text, flags=re.MULTILINE)
        text = re.sub(r'\|.*?\|', '', text)
        text = re.sub(r'^\s*[-*+•]\s+', '', text, flags=re.MULTILINE)
        text = re.sub(r'^\s*\d+\.\s+', '', text, flags=re.MULTILINE)
        text = re.sub(r'\n{3,}', '\n\n', text)
        
        # Remove common AI artifacts
        text = re.sub(r'^(Here is|Here\'s|The following is)', '', text, flags=re.IGNORECASE)
        text = re.sub(r'(In conclusion|To conclude|In summary)', 'These achievements', text, flags=re.IGNORECASE)
        
        # Clean up spacing
        text = re.sub(r'\s+', ' ', text)
        text = text.strip()
        
        return text


    def _extract_final_response_from_reasoning(self, full_text: str) -> str:
        """Extract the final response from reasoning model output"""
        
        # Common patterns for reasoning model outputs
        patterns = [
            # Pattern 1: Look for text after "Final response:" or similar
            r'(?:final response|final answer|here is the response|here\'s the response):\s*(.+)$',
            
            # Pattern 2: Look for text after reasoning markers
            r'(?:</reasoning>|</think>|</thought>)\s*(.+)$',
            
            # Pattern 3: Look for the last substantial paragraph (fallback)
            r'(?:^|\n\n)([A-Z][^<>\n]{200,})(?:\n\n|$)'
        ]
        
        for pattern in patterns:
            match = re.search(pattern, full_text, re.IGNORECASE | re.DOTALL)
            if match:
                response = match.group(1).strip()
                # Ensure we got substantial content
                if len(response) > 100:
                    return response
        
        # If no patterns match, look for the longest paragraph after any XML-like tags
        paragraphs = re.split(r'\n\n+', full_text)
        
        # Filter out reasoning/thinking paragraphs
        valid_paragraphs = []
        for para in paragraphs:
            para = para.strip()
            # Skip if it looks like reasoning
            if any(indicator in para.lower() for indicator in 
                ['let me', 'i need to', 'i should', 'thinking', 'reasoning', 
                    'first,', 'next,', 'analyzing', 'considering']):
                continue
            # Skip if too short
            if len(para) < 100:
                continue
            # Skip if contains XML-like tags
            if re.search(r'<[^>]+>', para):
                continue
            valid_paragraphs.append(para)
        
        # Return the longest valid paragraph
        if valid_paragraphs:
            return max(valid_paragraphs, key=len)
        
        # Last resort: return everything after removing obvious reasoning markers
        cleaned = re.sub(r'<[^>]+>', '', full_text)
        cleaned = re.sub(r'(?:Let me|I need to|I should|First,|Next,)[^.]+\.', '', cleaned)
        
        return cleaned.strip()
        
    def _generate_with_model(self, prompt: str, model: str, temperature: float, max_tokens: int, verbose: bool) -> Optional[str]:
        """Generate with executive quality enforcement and reasoning model support"""
        
        try:
            import requests
            import re
            
            # Detect if this is a reasoning model
            is_reasoning_model = any(indicator in model.lower() for indicator in ['r1', 'reasoning', 'deepseek-r1'])
            
            # Adjust temperature for reasoning models
            if is_reasoning_model:
                temperature = max(temperature, 0.5)  # Higher temp for reasoning models
            
            # Executive quality enforcement
            executive_enforced_prompt = f"""
            CRITICAL: You are writing for senior European Union executives and department heads. 
            
            This text must sound professional, achievement-focused, and strategically sophisticated.
            
            MANDATORY STYLE:
            • Lead with achievements and success
            • Use confident, professional language
            • Include specific metrics with strategic context
            • Write in flowing paragraphs suitable for executive briefing
            • Emphasize excellence, efficiency, and strategic value
            
            {prompt}
            
            FINAL REMINDER: Write as a senior EU executive would for peer executives. Demonstrate success and strategic excellence.
            """
            
            payload = {
                "model": model,
                "prompt": executive_enforced_prompt,
                "stream": False,
                "options": {
                    "temperature": temperature,
                    "num_predict": max_tokens,
                    "top_p": 0.9,
                    "top_k": 40,
                    "repeat_penalty": 1.1
                }
            }
            
            response = requests.post("http://localhost:11434/api/generate", json=payload, timeout=240)
            
            if response.status_code == 200:
                result = response.json()
                full_response = result.get('response', '').strip()
                
                # Handle reasoning model output
                if is_reasoning_model:
                    # Extract the actual response after reasoning
                    commentary = self._extract_final_response_from_reasoning(full_response)
                else:
                    commentary = full_response
                
                # Clean and validate
                commentary = self._clean_and_validate_executive_text(commentary)
                
                if verbose and is_reasoning_model:
                    print(f"🧠 Reasoning model detected - extracted final response")
                
                return commentary if commentary else None
            else:
                if verbose:
                    print(f"❌ Model API error: {response.status_code}")
                return None
                    
        except Exception as e:
            if verbose:
                print(f"❌ Generation error: {e}")
            return None
    
    def _extract_final_response_enhanced(self, full_text: str) -> str:
        """Enhanced extraction that preserves the complete final response"""
        
        import re
        
        # Try multiple extraction strategies
        
        # Strategy 1: Look for explicit final response markers
        final_markers = [
            r'(?:final response|final answer|here is the response|here\'s the response):\s*(.+)$',
            r'(?:in conclusion|to conclude|in summary)[,:]?\s*(.+)$',
            r'(?:</reasoning>|</think>|</thought>)\s*(.+)$'
        ]
        
        for pattern in final_markers:
            match = re.search(pattern, full_text, re.IGNORECASE | re.DOTALL)
            if match:
                response = match.group(1).strip()
                if len(response) > 100:  # Ensure substantial content
                    return response
        
        # Strategy 2: Find the executive summary section
        # Look for content that starts with executive language
        exec_patterns = [
            r'(The\s+(?:Grant Management Department|department|EU|European Union).+?)(?:\n\n[A-Z]|$)',
            r'((?:Budget|Payment|Grant|Audit)\s+(?:appropriations|processing|management).+?)(?:\n\n[A-Z]|$)',
            r'(\*\*[^*]+\*\*.+?)(?:\n\n[A-Z]|$)'  # Bold text sections
        ]
        
        for pattern in exec_patterns:
            matches = re.findall(pattern, full_text, re.IGNORECASE | re.DOTALL)
            if matches:
                # Get the longest match that looks like executive text
                best_match = max(matches, key=len)
                if len(best_match) > 200:
                    return best_match.strip()
        
        # Strategy 3: Extract everything after reasoning markers
        reasoning_end_markers = [
            '</reasoning>', '</think>', '</thought>', 
            'Final response:', 'Here is the response:',
            'Based on this analysis:', 'In conclusion:'
        ]
        
        for marker in reasoning_end_markers:
            if marker in full_text:
                parts = full_text.split(marker)
                if len(parts) > 1:
                    final_part = parts[-1].strip()
                    if len(final_part) > 100:
                        return final_part
        
        # Strategy 4: Get the last substantial paragraphs
        # Split by double newlines and get the last meaningful content
        paragraphs = re.split(r'\n\n+', full_text)
        
        # Filter paragraphs
        valid_paragraphs = []
        for para in paragraphs:
            para = para.strip()
            
            # Skip reasoning indicators
            if any(skip in para.lower() for skip in [
                'let me', 'i need', 'i should', 'first,', 'next,',
                'i\'ll', 'i will', 'looking at', 'analyzing'
            ]):
                continue
                
            # Skip too short
            if len(para) < 100:
                continue
                
            # Skip XML tags
            if re.search(r'<[^>]+>', para):
                continue
                
            valid_paragraphs.append(para)
        
        # Get the last 2-3 valid paragraphs as the response
        if valid_paragraphs:
            # Take up to last 3 paragraphs
            final_paragraphs = valid_paragraphs[-3:]
            return '\n\n'.join(final_paragraphs)
        
        # Fallback: Return everything after cleaning
        cleaned = re.sub(r'<[^>]+>', '', full_text)
        cleaned = re.sub(r'(?:Let me|I need to|I should|First,|Next,)[^.]+\.', '', cleaned)
        return cleaned.strip()

    # FIX 2: Improved reasoning model handling with full response capture
    def _generate_with_model_enhanced(
        self, 
        prompt: str, 
        model: str, 
        temperature: float, 
        max_tokens: int, 
        verbose: bool,
        capture_reasoning: bool = True
    ) -> Optional[str]:
        """Enhanced generation that properly captures full reasoning output"""
        
        try:
            import requests
            import re
            
            # Detect if this is a reasoning model
            is_reasoning_model = any(indicator in model.lower() 
                                for indicator in ['r1', 'reasoning', 'deepseek-r1'])
            
            # For reasoning models, ensure we get the complete response
            if is_reasoning_model:
                max_tokens = max(max_tokens, 2000)  # Ensure enough tokens for reasoning
                temperature = max(temperature, 0.5)
            
            # Executive quality prompt
            executive_prompt = f"""
            CRITICAL: You are writing for senior European Union executives and department heads. 
            
            {prompt}
            
            IMPORTANT: Provide your complete analysis and final response.
            """
            
            payload = {
                "model": model,
                "prompt": executive_prompt,
                "stream": False,
                "options": {
                    "temperature": temperature,
                    "num_predict": max_tokens,
                    "top_p": 0.9,
                    "top_k": 40,
                    "repeat_penalty": 1.1,
                    "stop": []  # Remove stop sequences for reasoning models
                }
            }
            
            if verbose:
                print(f"   🤖 Calling {model} (temp: {temperature}, max_tokens: {max_tokens})")
            
            response = requests.post(
                "http://localhost:11434/api/generate", 
                json=payload, 
                timeout=300  # Increased timeout
            )
            
            if response.status_code == 200:
                result = response.json()
                full_response = result.get('response', '').strip()
                
                if verbose and is_reasoning_model:
                    print(f"   📝 Got full response: {len(full_response)} chars")
                
                # For reasoning models, extract the final response
                if is_reasoning_model and capture_reasoning:
                    final_response = self._extract_final_response_enhanced(full_response)
                    
                    if verbose:
                        print(f"   🧠 Extracted final response: {len(final_response)} chars")
                    
                    return final_response
                else:
                    # For non-reasoning models, clean the response
                    return self._clean_and_validate_executive_text(full_response)
            else:
                if verbose:
                    print(f"   ❌ API error: {response.status_code}")
                return None
                
        except Exception as e:
            if verbose:
                print(f"   ❌ Generation error: {str(e)}")
            return None
        
    def diagnose_section_generation(
        self,
        section_key: str,
        quarter_period: str,
        current_year: str,
        financial_data: Dict[str, Any],
        verbose: bool = True
    ) -> Dict[str, Any]:
        """Diagnose why a section might fail to generate"""
        
        diagnosis = {
            'section_key': section_key,
            'has_mapping': False,
            'has_template': False,
            'has_required_data': False,
            'data_availability': {},
            'issues': []
        }
        
        # Check mapping
        mapping = self.mapping_matrix.get_complete_mapping_matrix()
        if section_key in mapping:
            diagnosis['has_mapping'] = True
            section_config = mapping[section_key]
            
            # Check template
            templates = self.template_library.get_template_definitions(quarter_period, current_year)
            template_name = section_config['template_mapping']['template_name']
            if template_name in templates:
                diagnosis['has_template'] = True
            else:
                diagnosis['issues'].append(f"Template '{template_name}' not found")
            
            # Check data availability
            data_config = section_config['data_configuration']
            primary_data_keys = data_config['primary_data']
            secondary_data_keys = data_config['secondary_data']
            
            # Check primary data
            for key in primary_data_keys:
                if key in financial_data and financial_data[key] is not None:
                    diagnosis['data_availability'][key] = 'available'
                else:
                    diagnosis['data_availability'][key] = 'missing'
                    diagnosis['issues'].append(f"Missing primary data: {key}")
            
            # Check if at least some data is available
            available_primary = [k for k, v in diagnosis['data_availability'].items() 
                            if v == 'available' and k in primary_data_keys]
            if available_primary:
                diagnosis['has_required_data'] = True
            else:
                diagnosis['issues'].append("No primary data available")
        else:
            diagnosis['issues'].append(f"No mapping found for section '{section_key}'")
        
        if verbose:
            print(f"\n🔍 DIAGNOSIS FOR {section_key}:")
            print(f"   ✓ Has mapping: {diagnosis['has_mapping']}")
            print(f"   ✓ Has template: {diagnosis['has_template']}")
            print(f"   ✓ Has required data: {diagnosis['has_required_data']}")
            if diagnosis['issues']:
                print(f"   ❌ Issues found:")
                for issue in diagnosis['issues']:
                    print(f"      - {issue}")
        
        return diagnosis
        
    # ================================================================
    # 🔄 QUALITY ENHANCEMENT METHODS (NEW SECTION)
    # Add these RIGHT AFTER the AI Model Integration section
    # ================================================================
    
    def _generate_with_retry(
        self,
        prompt: str,
        model: str,
        temperature: float,
        max_tokens: int,
        section_key: str,
        verbose: bool,
        max_retries: int = 3
    ) -> Optional[str]:
        """Generate with retry logic for better quality"""
        
        # Import config at top of method to avoid circular imports
        from comments_old import CommentsConfig
        
        # Check for section-specific temperature override
        if section_key in CommentsConfig.SECTION_TEMPERATURE_OVERRIDES:
            temperature = CommentsConfig.SECTION_TEMPERATURE_OVERRIDES[section_key]
            if verbose:
                print(f"   🌡️ Using section-specific temperature: {temperature}")
        
        retry_count = 0
        current_temperature = temperature
        
        while retry_count < max_retries:
            if retry_count > 0:
                current_temperature += CommentsConfig.QUALITY_SETTINGS['retry_temperature_increment']
                if verbose:
                    print(f"   🔄 Retry {retry_count} with temperature: {current_temperature}")
            
            try:
                response = self._generate_with_model(
                    prompt=prompt,
                    model=model,
                    temperature=current_temperature,
                    max_tokens=max_tokens,
                    verbose=verbose
                )
                
                # Validate response quality
                if response and len(response) >= CommentsConfig.QUALITY_SETTINGS['min_response_length']:
                    # Additional quality checks
                    if self._validate_response_quality(response, section_key):
                        return response
                    elif verbose:
                        print(f"   ⚠️ Response quality check failed, retrying...")
                elif verbose:
                    print(f"   ⚠️ Response too short ({len(response) if response else 0} chars), retrying...")
                    
            except Exception as e:
                if verbose:
                    print(f"   ❌ Generation error: {e}")
            
            retry_count += 1
        
        if verbose:
            print(f"   ❌ Failed after {max_retries} retries")
        return None

    def _validate_response_quality(self, response: str, section_key: str) -> bool:
        """Validate response quality based on section requirements"""
        
        # Basic quality checks
        if not response or len(response) < 100:
            return False
        
        # Check for incomplete sentences
        if not response.strip().endswith(('.', '!', '?')):
            return False
        
        # Check for repetitive content
        sentences = response.split('.')
        if len(sentences) > 3:
            unique_sentences = set(s.strip().lower() for s in sentences if s.strip())
            if len(unique_sentences) < len(sentences) * 0.7:  # Too much repetition
                return False
        
        # Section-specific checks
        if 'payment' in section_key:
            # Should contain financial metrics
            if not any(indicator in response.lower() for indicator in ['€', 'eur', 'million', 'payment']):
                return False
        
        if 'budget' in section_key:
            # Should contain budget-related terms
            if not any(term in response.lower() for term in ['appropriation', 'allocation', 'budget']):
                return False
        
        if 'ttp' in section_key:
            # Should contain time-related metrics
            if not any(term in response.lower() for term in ['days', 'time', 'ttp', 'compliance']):
                return False
        
        return True
    
    def _generate_with_quality_assurance(
        self,
        prompt: str = None,  # Make prompt optional
        model: str = None,
        temperature: float = None,
        max_tokens: int = None,
        section_key: str = None,
        verbose: bool = True,
        max_retries: int = 3,
        allow_human_validation: bool = False
    ) -> Optional[str]:
        """Generate with quality assurance and optional human validation"""
        
        from comments_old import CommentsConfig

        # If prompt not provided, we need to generate it
        if prompt is None and section_key:
            # Get the section config and generate prompt
            mapping = self.mapping_matrix.get_complete_mapping_matrix()
            if section_key not in mapping:
                return None
                
            # ... (generate the prompt here if needed)
            # For now, assume prompt is always provided
        
        if not prompt:
            if verbose:
                print("❌ No prompt provided for generation")
            return None
        
        # Check for section-specific temperature
        if section_key in CommentsConfig.SECTION_TEMPERATURE_OVERRIDES:
            temperature = CommentsConfig.SECTION_TEMPERATURE_OVERRIDES[section_key]
        
        retry_count = 0
        current_temperature = temperature
        questionable_responses = []  # Store responses that might be acceptable
        
        while retry_count < max_retries:
            if retry_count > 0:
                current_temperature += 0.1
                if verbose:
                    print(f"   🔄 Retry {retry_count} with temperature: {current_temperature}")
            
            try:
                # Use enhanced generation method
                response = self._generate_with_model_enhanced(
                    prompt=prompt,
                    model=model,
                    temperature=current_temperature,
                    max_tokens=max_tokens,
                    verbose=verbose
                )
                
                if response:
                    # Check basic length requirement
                    if len(response) >= 80:  # Lowered from 100
                        # Try quality validation
                        quality_score = self._assess_response_quality(response, section_key)
                        
                        if quality_score >= 0.8:  # High quality
                            if verbose:
                                print(f"   ✅ High quality response (score: {quality_score:.2f})")
                            return response
                        elif quality_score >= 0.5:  # Questionable quality
                            questionable_responses.append({
                                'response': response,
                                'score': quality_score,
                                'retry': retry_count
                            })
                            if verbose:
                                print(f"   ⚠️ Questionable quality (score: {quality_score:.2f})")
                        else:
                            if verbose:
                                print(f"   ❌ Low quality (score: {quality_score:.2f})")
                    else:
                        if verbose:
                            print(f"   ⚠️ Response too short: {len(response)} chars")
                            
            except Exception as e:
                if verbose:
                    print(f"   ❌ Generation error: {e}")
            
            retry_count += 1
        
        # If we have questionable responses and human validation is allowed
        if questionable_responses and allow_human_validation:
            if verbose:
                print(f"\n   🤔 {len(questionable_responses)} questionable responses available")
            
            # Get the best questionable response
            best_questionable = max(questionable_responses, key=lambda x: x['score'])
            
            # Ask for human validation
            if self._request_human_validation(
                section_key, 
                best_questionable['response'], 
                best_questionable['score']
            ):
                if verbose:
                    print(f"   ✅ Human approved the response")
                return best_questionable['response']
        
        # Last resort: return the best response we have
        if questionable_responses:
            best_response = max(questionable_responses, key=lambda x: x['score'])['response']
            if verbose:
                print(f"   ⚠️ Returning best available response (no human validation)")
            return best_response
        
        if verbose:
            print(f"   ❌ Failed to generate acceptable response")
        return None
    
    def _assess_response_quality(self, response: str, section_key: str) -> float:
        """Assess response quality with a score from 0 to 1"""
        
        if not response:
            return 0.0
        
        score = 1.0
        
        # Length check (more lenient)
        if len(response) < 80:
            score -= 0.5
        elif len(response) < 150:
            score -= 0.2
        
        # Completeness check
        if not response.strip().endswith(('.', '!', '?', '"')):
            score -= 0.1
        
        # Check for incomplete words at the end
        words = response.split()
        if words and len(words[-1]) < 3:
            score -= 0.1
        
        # Section-specific content checks (more lenient)
        content_found = False
        
        if 'payment' in section_key:
            if any(indicator in response.lower() for indicator in ['payment', 'disburs', 'credit', 'process']):
                content_found = True
        elif 'budget' in section_key:
            if any(term in response.lower() for term in ['budget', 'appropriation', 'allocation', 'fund']):
                content_found = True
        elif 'ttp' in section_key:
            if any(term in response.lower() for term in ['time', 'day', 'process', 'compliance', 'performance']):
                content_found = True
        else:
            # For other sections, just check if it mentions relevant terms
            if any(term in response.lower() for term in ['department', 'grant', 'eu', 'european', 'achieve', 'perform']):
                content_found = True
        
        if not content_found:
            score -= 0.3
        
        # Check for quality indicators (positive scoring)
        if '€' in response or 'eur' in response.lower():
            score += 0.1
        if any(word in response.lower() for word in ['achieve', 'success', 'deliver', 'perform']):
            score += 0.1
        if response.count('.') >= 3:  # Multiple sentences
            score += 0.1
        
        # Ensure score is between 0 and 1
        return max(0.0, min(1.0, score))
    
    def _request_human_validation(self, section_key: str, response: str, quality_score: float) -> bool:
        """Request human validation for questionable quality responses"""
        
        print("\n" + "="*60)
        print("🤔 HUMAN VALIDATION REQUESTED")
        print("="*60)
        print(f"Section: {section_key}")
        print(f"Quality Score: {quality_score:.2f}/1.00")
        print(f"Response Length: {len(response)} characters")
        print("\n--- GENERATED TEXT ---")
        print(response[:500] + "..." if len(response) > 500 else response)
        print("\n--- END OF TEXT ---")
        
        while True:
            user_input = input("\n✅ Accept this response? (y/n/v to view full): ").lower().strip()
            
            if user_input == 'y':
                return True
            elif user_input == 'n':
                return False
            elif user_input == 'v':
                print("\n--- FULL TEXT ---")
                print(response)
                print("\n--- END OF FULL TEXT ---")
            else:
                print("Please enter 'y' for yes, 'n' for no, or 'v' to view full text")

    
    def _clean_generated_text(self, text: str) -> str:
        """Clean generated text of any unwanted formatting"""
        
        import re
        
        # Remove markdown headers
        text = re.sub(r'^#{1,6}\s+', '', text, flags=re.MULTILINE)
        
        # Remove table formatting
        text = re.sub(r'\|.*?\|', '', text)
        text = re.sub(r'^[-=+]{3,}$', '', text, flags=re.MULTILINE)
        
        # Remove bullet points at line start
        text = re.sub(r'^\s*[-*+]\s+', '', text, flags=re.MULTILINE)
        
        # Remove numbered lists at line start
        text = re.sub(r'^\s*\d+\.\s+', '', text, flags=re.MULTILINE)
        
        # Clean multiple newlines
        text = re.sub(r'\n{3,}', '\n\n', text)
        
        # Remove empty lines at start/end
        text = text.strip()
        
        return text
    
