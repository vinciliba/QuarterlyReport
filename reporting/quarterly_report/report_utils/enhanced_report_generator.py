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
            """
            ,

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
                    'word_limit': 800,                   # ✅ Increased for comprehensive coverage
                    'formatting_level': 'executive'
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
                        'TTG',                             # Time-to-Grant performance
                        'completion_previous_year_calls'    # Call completion status
                    ],
                    
                    # 🔄 SECONDARY_DATA: Supporting process context
                    'secondary_data': [
                        'grants_commitment_activity',       # Grant commitment activity
                        'current_year_global_commitment_activity',  # Current year activity
                        'commitments'                      # Overall commitment context
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

            'commitments_workflow': {
                'section_info': {
                    'name': 'Commitments Workflow Summary',
                    'category': 'workflow',
                    'priority': 4,
                    'description': 'Commitment allocation and utilization efficiency'
                },
                'template_mapping': {
                    'template_name': 'commitments_workflow_template',
                    'template_category': 'workflow_analysis',
                    'supports_variables': ['prioritized_data_summary', 'secondary_data_summary']
                },
                'data_configuration': {
                    'primary_data': ['commitments'],
                    'secondary_data': ['summary_budget'],
                    'focus_metrics': ['commitment_consumption', 'allocation_efficiency', 'portfolio_performance']
                },
                'output_configuration': {
                    'module': 'CommitmentsModule',
                    'variable_name': 'commitments_workflow_summary',
                    'word_limit': 250,
                    'formatting_level': 'operational'
                },
                'instruction_mapping': {
                    'instruction_key': 'commitments_workflow_instructions',
                    'tone': 'operational',
                    'focus': 'allocation_efficiency'
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
        """Get all payment-related data keys for a program"""
        info = ProgramProcessor.get_program_info(program)
        if info:
            return [info['data_key']] + info.get('payment_fields', []) + info.get('analysis_tables', [])
        return []
    
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
        Extract call type specific data from financial tables using enhanced program mapping.
        This function automatically derives payment types from the actual data.
        """
        
        # Get program information using enhanced mapping
        program_info = ProgramProcessor.get_program_info(program)
        if not program_info:
            if verbose:
                print(f"⚠️  Unknown program: {program}")
            return None
        
        # Get all possible data keys for this program
        data_keys = ProgramProcessor.get_all_data_keys(program)
        
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
                print(f"⚠️  No valid data found for program: {program}")
            return None
        
        # Normalize the call type for matching
        normalized_call_type = CallTypeProcessor.normalize_call_type(call_type)
        
        # Find records that match this call type (flexible matching)
        matching_records = []
        derived_descriptions = set()
        
        for record in found_data:
            if isinstance(record, dict):
                # Check all field values for call type matches
                record_str = json.dumps(record, default=str).upper()
                
                # Check for various call type representations
                call_type_variants = CALL_TYPE_NORMALIZATION.get(normalized_call_type, [call_type])
                
                for variant in call_type_variants:
                    if variant.upper() in record_str:
                        matching_records.append(record)
                        
                        # Try to derive payment description from the record
                        for key, value in record.items():
                            if isinstance(value, str) and len(value) > 10 and any(word in value.lower() for word in ['payment', 'financing', 'grant', 'interim', 'final']):
                                derived_descriptions.add(value.strip())
                        break
        
        if matching_records:
            # Get the best description from derived descriptions
            best_description = CallTypeProcessor._select_best_description(
                list(derived_descriptions), normalized_call_type
            )
            
            return {
                'program': program,
                'program_info': program_info,
                'call_type': call_type,
                'normalized_call_type': normalized_call_type,
                'records': matching_records,
                'total_records': len(matching_records),
                'derived_description': best_description,
                'all_descriptions': list(derived_descriptions),
                'data_source': data_source
            }
        else:
            if verbose:
                print(f"⚠️  No matching records found for {call_type} in {program}")
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

    def generate_section_commentary(
        self,
        section_key: str,
        quarter_period: str,
        current_year: str,
        financial_data: Dict[str, Any],
        model: str = "deepseek-r1:14b",
        temperature: float = 0.3,
        acronym_context: str = "",           # ✅ NEW: Acronym context parameter
        cutoff_date: Any = None,             # ✅ NEW: Cutoff date parameter
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

        # Get template from library
        """Step 2: Template Retrieval
        What: Gets the specific template for this section
        Why: Templates define the structure and format of the output"""

        templates = self.template_library.get_template_definitions(quarter_period, current_year)
        template_name = section_config['template_mapping']['template_name']
        template = templates[template_name]
        
        # ✅ FIXED: Apply cutoff date filtering to financial data
        if cutoff_date is not None:
            financial_data = self._filter_data_by_cutoff(financial_data, cutoff_date, verbose)
    
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
        instructions = self._get_section_instructions(section_config)
        
         # ✅ FIXED: Create final prompt with acronym context
        final_prompt = self._create_enhanced_prompt(
            instructions=instructions,
            template=formatted_template,
            acronym_context=acronym_context,
            section_key=section_key
        )
        
        # Generate commentary
        commentary = self._generate_with_model(
            prompt=final_prompt,
            model=model,
            temperature=temperature,
            max_tokens=int(section_config['output_configuration']['word_limit'] * 1.5),
            verbose=verbose
        )
        
        return commentary
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
        call_types: List[str] = None,
        model: str = "deepseek-r1:14b",
        temperature: float = 0.3,
        acronym_context: str = "",           # ✅ NEW: Acronym context parameter
        cutoff_date: Any = None,             # ✅ NEW: Cutoff date parameter
        verbose: bool = True
    ) -> Dict[str, Any]:
        """
        Generate call type payment details using predefined programs and call types lists.
        Payment types are automatically derived from the data tables.
        """

        """1.Process Flow:
           Setup Phase:"""
        
        # Use predefined lists if none provided
        if programs is None:
            programs = PROGRAMS_LIST.copy()
        if call_types is None:
            call_types = CALL_TYPES_LIST.copy()
        
        if verbose:
            print("🔄 PREDEFINED CALL TYPE LOOPS GENERATION")
            print("=" * 60)
            print(f"📋 Programs: {programs}")
            print(f"📋 Call Types: {call_types}")
            print("📊 Payment types will be derived from data tables")
            if acronym_context:
                print("📚 Using acronym context for AI guidance")
            if cutoff_date:
                print(f"📅 Filtering data by cutoff: {cutoff_date}")
        
        # ✅ FIXED: Apply cutoff date filtering
        if cutoff_date is not None:
            financial_data = self._filter_data_by_cutoff(financial_data, cutoff_date, verbose)
        
        results = {
            'generated_details': {},
            'failed_generations': [],
            'data_summary': {},
            'statistics': {
                'total_combinations': len(programs) * len(call_types),
                'successful': 0,
                'failed': 0,
                'data_found': 0,
                'no_data': 0
            }
        }
        
        # Get configuration
        mapping = self.mapping_matrix.get_complete_mapping_matrix()
        config = mapping['auto_call_type_detail']
        
        # Get template
        templates = self.template_library.get_template_definitions(quarter_period, current_year)
        template = templates[config['template_mapping']['template_name']]
        
        combination_counter = 1
        total_combinations = len(programs) * len(call_types)
        
        # Loop through all combinations
        """2. Loop Processing:"""

        for program in programs:
            for call_type in call_types:
                
                combination_key = f"{program}_{call_type}"
                
                if verbose:
                    print(f"\n📝 [{combination_counter}/{total_combinations}] Processing: {program} - {call_type}")
                
                try:
                    # Extract data for this combination using the enhanced utility function
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
                    
                    # Store data summary for reference
                    results['data_summary'][combination_key] = {
                        'program': program,
                        'call_type': call_type,
                        'normalized_call_type': extracted_data['normalized_call_type'],
                        'records_found': extracted_data['total_records'],
                        'derived_description': extracted_data['derived_description'],
                        'data_source': extracted_data['data_source']
                    }
                    
                    # Calculate payment statistics
                    payment_stats = CallTypeProcessor.calculate_payment_statistics(extracted_data['records'])
                    
                    # Generate payment analysis text
                    payment_analysis_text = self._create_payment_analysis_text(
                        payment_stats, quarter_period, current_year
                    )
                    
                    # Generate variance statement
                    variance_statement = self._create_variance_statement(payment_stats)
                    
                    # Format the template
                    formatted_template = template.format(
                        call_type_abbreviation=extracted_data['normalized_call_type'],
                        derived_payment_description=extracted_data['derived_description'],
                        payment_analysis_text=payment_analysis_text,
                        variance_statement=variance_statement
                    )
                    
                    # Generate instructions
                    instructions = self._get_auto_call_type_instructions(
                        config, extracted_data, payment_stats
                    )
                    
                    # ✅ FIXED: Create enhanced prompt with acronym context
                    final_prompt = self._create_enhanced_prompt(
                        instructions=instructions,
                        template=formatted_template,
                        acronym_context=acronym_context,
                        section_key=f"auto_call_type_{program}_{call_type}"
                    )
                    
                    # Generate commentary
                    commentary = self._generate_with_model(
                        prompt=final_prompt,
                        model=model,
                        temperature=temperature,
                        max_tokens=int(config['output_configuration']['word_limit'] * 1.5),
                        verbose=False
                    )
                    
                    if commentary:
                        # Create variable name
                        var_name = config['output_configuration']['variable_name'].format(
                            programme=program.lower(),
                            call_type=call_type.lower()
                        )
                        """3. Results Tracking:"""

                        results['generated_details'][var_name] = {
                            'commentary': commentary,
                            'program': program,
                            'call_type': call_type,
                            'normalized_call_type': extracted_data['normalized_call_type'],
                            'derived_description': extracted_data['derived_description'],
                            'section_name': f"Auto Call Type - {program} - {call_type}",
                            'word_count': len(commentary.split()),
                            'target_words': config['output_configuration']['word_limit'],
                            'payment_stats': payment_stats,
                            'generated_at': datetime.datetime.now()
                        }
                        
                        results['statistics']['successful'] += 1
                        
                        if verbose:
                            word_count = len(commentary.split())
                            target = config['output_configuration']['word_limit']
                            print(f"✅ Generated {word_count} words (target: {target})")
                            print(f"   📊 Found {payment_stats['total_payments']} payments")
                            print(f"   📝 Description: {extracted_data['derived_description'][:50]}...")
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
            print(f"\n🎉 PREDEFINED LOOPS GENERATION COMPLETE!")
            print(f"✅ Success: {results['statistics']['successful']}/{results['statistics']['total_combinations']}")
            print(f"📊 Data found for: {results['statistics']['data_found']} combinations")
            print(f"⚠️  No data for: {results['statistics']['no_data']} combinations")
            if results['failed_generations']:
                print(f"❌ Failed: {', '.join(results['failed_generations'])}")
        
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
        section_key: str
    ) -> str:
        """Create enhanced prompt with acronym context"""
        
        prompt_parts = []
        
        # Add acronym context if available
        if acronym_context and acronym_context.strip():
            prompt_parts.append(acronym_context)
            prompt_parts.append("\n" + "="*60 + "\n")
        
        # Add main instructions
        prompt_parts.append(instructions)
        
        # Add template
        prompt_parts.append("\n" + template)
        
        # Add final instructions if acronym context was provided
        if acronym_context and acronym_context.strip():
            prompt_parts.append(f"""

        🔍 IMPORTANT: Use the acronym definitions provided above to ensure accurate terminology.
        Always explain acronyms on first use: "Horizon Europe (HEU)" then use "HEU" thereafter.
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
        """Generate section-specific instructions - ENHANCED for executive summary"""
        
        section_info = section_config['section_info']
        output_config = section_config['output_configuration']
        instruction_config = section_config['instruction_mapping']
        
        word_limit = output_config['word_limit']
        tone = instruction_config['tone']
        focus = instruction_config['focus']
        section_name = section_info['name']
        
        # ✅ SPECIAL HANDLING FOR EXECUTIVE SUMMARY
        if section_info.get('name') == 'Introductory Summary' or 'intro_summary' in str(section_config):
            return f"""
            Generate a comprehensive Grant Management Department executive summary ({word_limit} words) highlighting major achievements and performance milestones.

            STRUCTURE REQUIREMENTS:
            1. **Opening Achievement Statement**: Department accomplishments and milestone highlights
            2. **Quantitative Performance Metrics**: Include specific numbers, percentages, and time metrics from the data
            3. **Workflow Breakdown Sections**: 
            - **Payments:** Volume statistics, TTP averages, appropriation utilization
            - **Granting:** TTG performance, call completion rates, funding progress
            - **Amendments:** TTA efficiency, amendment volumes, processing rates
            - **Audits:** Recovery amounts, audit completion, financial integrity results
            - **Other Activities:** Budget compliance, exceptional cases, strategic notes

            CONTENT REQUIREMENTS:
            • Use executive achievement tone: professional, accomplishment-focused, quantitative
            • Include exact metrics from data: TTP days, payment volumes, amendment counts, recovery amounts
            • Highlight contractual compliance: "90-day limits", "45-day targets", "100% on-time rates"
            • Structure with clear section headers using **bold** formatting
            • Emphasize successful execution and exceptional performance
            • Include forward-looking performance assessment in conclusion

            FORMATTING:
            • Use **bold** for section headers: **Payments:**, **Amendments:**, **Audits:**
            • Bold key department names: **The Grant Management Department**
            • Include specific numerical achievements: "486 payments", "22.1 days", "EUR 6.73 million"
            • Maintain continuous narrative flow with clear workflow transitions
            • Target exactly {word_limit} words for comprehensive executive coverage

            Focus: Demonstrating exceptional departmental performance through quantitative achievements, successful milestone completion, and exemplary execution of grant management workflows.
            """
        # ✅ SPECIAL HANDLING FOR EXECUTIVE SUMMARY
        elif section_info.get('name') == 'Budget Overview' or 'budget_overview' in str(section_config):
            return f"""
                    Generate a comprehensive budget appropriations and absorption analysis ({word_limit} words) covering H2020 and Horizon Europe programs.

                    STRUCTURE REQUIREMENTS:
                    1. **Budget Appropriations Overview**: Available commitment and payment appropriations
                    2. **Absorption Performance**: Utilization rates and consumption patterns  
                    3. **Program Comparison**: H2020 vs Horizon Europe performance analysis
                    4. **Strategic Analysis**: Efficiency insights and optimization opportunities

                    CONTENT REQUIREMENTS:
                    • **Commitment Appropriations**: Available amounts, allocation patterns, absorption rates
                    • **Payment Appropriations**: H2020 and HEU credit consumption, efficiency metrics
                    • **Absorption Levels**: Specific utilization percentages and execution rates
                    • **Performance Metrics**: Include variance analysis and trend assessment
                    • **Cross-Program Analysis**: Compare H2020 and HEU performance and patterns

                    FORMATTING:
                    • Use **bold** for key financial figures and appropriation amounts
                    • Include specific percentages for absorption and utilization rates  
                    • Structure with clear sections for commitment vs payment appropriations
                    • Highlight performance indicators and efficiency metrics
                    • Maintain analytical tone with quantitative focus

                    ANALYTICAL FOCUS:
                    • Budget availability and strategic allocation effectiveness
                    • Absorption efficiency and utilization optimization
                    • Performance benchmarking between H2020 and Horizon Europe
                    • Resource management insights and planning implications

                    Target exactly {word_limit} words for comprehensive budget appropriations coverage.
                    """
        elif ' granting_process_overview' in section_name.lower() or 'granting process overview' in section_name.lower():
            return f"""
                    Generate a descriptive granting process overview ({word_limit} words) covering grant execution and call completion.

                    STRUCTURE REQUIREMENTS:
                    1. **Grant Execution Summary**: Number of new grants executed during the period
                    2. **Process Status**: Grant allocation process schedule adherence  
                    3. **Table Reference**: Reference to grant agreements overview table
                    4. **Call Completion**: Brief description of call completion status

                    CONTENT REQUIREMENTS:
                    • Include specific numbers: "In total, X.X new grants were executed during {period}"
                    • Process status: "The grant allocation process proceeded as scheduled"
                    • Table reference: "Table 3a presents an overview of grant agreements (GA) under preparation and signed contracts by ERCEA"
                    • Call completion rates and progress where applicable

                    FORMATTING:
                    • Maintain descriptive, factual tone
                    • Include specific grant execution numbers from data
                    • Reference relevant tables for detailed breakdown
                    • Target exactly {word_limit} words for concise overview

                    Focus: Grant signature activity, process efficiency, and scheduled milestone achievement.
                    """
        elif 'commitment_budgetary' in section_name.lower() or 'budgetary consequence' in section_name.lower():
            return f"""
                    Generate budgetary consequence analysis ({word_limit} words) covering commitment financial impact and regulatory context.

                    STRUCTURE REQUIREMENTS:
                    1. **Financial Impact**: Total money committed during the period
                    2. **Breakdown Reference**: Reference to commitment activity breakdown table
                    3. **Regulatory Boilerplate**: EU Financial Regulation explanation
                    4. **L2 vs Legal Commitment**: Timing difference explanation

                    CONTENT REQUIREMENTS:
                    • Financial amounts: Include total commitment amounts for the period
                    • Table reference: "A breakdown of the total commitment activity for [year] by period and call is detailed in Table 3c"
                    • Regulatory text: "As per the EU Financial Regulation (FR2018 art. 111.2), the budgetary commitment, also known as the L2 commitment, precedes the grant signature, which constitutes the legal commitment"
                    • Timing explanation: "Therefore, it is possible that at the end of a month's cut-off, a budgetary commitment for a specific grant has been established while the grant itself has not yet been formally signed"

                    FORMATTING:
                    • Include specific financial amounts from data
                    • Maintain analytical tone with regulatory context
                    • Reference Table 3c for detailed breakdown
                    • Target exactly {word_limit} words including boilerplate

                    Focus: Financial commitment impact, regulatory compliance, and commitment process timing.
                    """
    
        elif 'fdi_status' in section_name.lower() or 'final date for implementation' in section_name.lower():
            return f"""
                    Generate FDI status analysis ({word_limit} words) covering commitments approaching or exceeding FDI thresholds.

                    STRUCTURE REQUIREMENTS:
                    1. **H2020 FDI Status**: Commitments exceeding FDI threshold with call type breakdown
                    2. **HEU FDI Status**: Horizon Europe FDI threshold compliance status
                    3. **Distribution Details**: Call type distribution (COG, POC, STG, etc.)
                    4. **Factual Reporting**: Specific numbers and observed status

                    CONTENT REQUIREMENTS:
                    • H2020 format: "At the end of [Period], [X] L2 commitments exceeding the FDI (H2020) threshold were observed, distributed as follows: COG (X), POC (X), STG (X)"
                    • HEU format: "At the end of [Period], [no/X] commitments exceeding the FDI (HEU) threshold were observed"
                    • Include specific numbers from grants_exceeding_fdi data
                    • Call type breakdown with exact counts

                    FORMATTING:
                    • Use factual, compliance-focused tone
                    • Include specific numbers and call type distributions
                    • Separate H2020 and HEU status clearly
                    • Target exactly {word_limit} words for brief status report

                    Focus: FDI compliance monitoring, threshold status, and call type impact distribution.
                    """
    
        # ✅ DEFAULT HANDLING FOR OTHER SECTIONS (your existing logic)
        else:
            return f"""
                Generate a {tone} {section_name.lower()} ({word_limit} words) focusing on {focus.replace('_', ' ')}.

                Requirements:
                - Use **bold** for key financial figures and percentages
                - Structure with clear paragraphs and bullet points
                - Focus on {', '.join(section_config['data_configuration']['focus_metrics'])}
                - Maintain {tone} tone throughout
                - Target exactly {word_limit} words

                Format for Word document integration.
                """
    
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
    
    def _generate_with_model(self, prompt: str, model: str, temperature: float, max_tokens: int, verbose: bool) -> Optional[str]:
        """Generate text using the specified model"""
        
        try:
            import requests
            
            payload = {
                "model": model,
                "prompt": prompt,
                "stream": False,
                "options": {
                    "temperature": temperature,
                    "num_predict": max_tokens,
                    "top_p": 0.9,
                    "top_k": 40
                }
            }
            
            response = requests.post("http://localhost:11434/api/generate", json=payload, timeout=180)
            
            if response.status_code == 200:
                result = response.json()
                commentary = result.get('response', '').strip()
                return commentary if commentary else None
            else:
                if verbose:
                    print(f"❌ Model API error: {response.status_code}")
                return None
                
        except Exception as e:
            if verbose:
                print(f"❌ Generation error: {e}")
            return None
