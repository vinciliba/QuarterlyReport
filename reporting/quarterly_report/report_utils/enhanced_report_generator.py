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
        # ✅ NEW AND IMPROVED FDI STATUS TEMPLATE
        # ============================================================
        'fdi_status_template': f"""
            FINAL DATE FOR IMPLEMENTATION (FDI) - COMPLIANCE AND RISK ANALYSIS
            Reporting Period: {quarter_period} {current_year}
            Scope: Monitoring of L2 commitments approaching or exceeding FDI thresholds

            FDI COMPLIANCE AND RISK OVERVIEW:
            {{prioritized_data_summary}}

            DETAILED PROGRAM AND FINANCIAL IMPACT ANALYSIS:
            {{secondary_data_summary}}

            **FDI Monitoring Framework:**
            The Final Date for Implementation (FDI) is the deadline for legally committing funds through grant agreements. Proactive monitoring of commitments relative to their FDI thresholds is a critical component of sound financial management, ensuring compliance and preventing the de-commitment of funds.

            **Analysis Areas:**
            • **Compliance Status:** Assessment of commitments exceeding FDI and those approaching the deadline.
            • **Financial Impact:** Analysis of the financial value of at-risk commitments.
            • **Program Breakdown:** Identification of trends within H2020 and Horizon Europe.
            • **Operational Response:** Overview of actions taken to mitigate risks and ensure timely grant signatures.

            Focus: Strategic FDI risk management, financial impact assessment, compliance assurance, and operational efficiency in the commitment lifecycle.

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
        ✅ FIXED: This method now returns a valid Python dictionary.
        The string literals between entries have been converted to comments,
        which resolves the "matrix configuration absent" error.
        """

        return {
            # ============================================================
            # 1. EXECUTIVE LEVEL SECTIONS
            # ============================================================

            # Target Text Analysis for Executive Summary:
            # ✅ Covers: TTP metrics, TTA metrics, payment volumes, amendment statistics, audit results, grant progress
            # ✅ Tone: Executive, achievement-focused, specific numbers
            # ✅ Length: ~600 words
            # ✅ Structure: Department achievements → Detailed breakdowns by workflow
            'intro_summary': {
                'section_info': {
                    'name': 'Introductory Summary',
                    'category': 'executive',
                    'priority': 1,
                    'description': 'Comprehensive executive overview covering all department workflows and achievements'
                },
                'template_mapping': {
                    'template_name': 'executive_summary_template',
                    'template_category': 'executive_overview',
                    'supports_variables': ['prioritized_data_summary', 'secondary_data_summary']
                },
                'data_configuration': {
                    'primary_data': [
                        'TTP_Overview', 'pay_credits_H2020', 'pay_credits_HEU',
                        'amendment_activity_H2020', 'amendment_activity_HEU',
                        'amendment_TTA_H2020', 'amendment_TTA_HEU',
                        'auri_overview', 'recovery_activity', 'TTG'
                    ],
                    'secondary_data': [
                        'summary_budget', 'H2020_TTP_FP', 'H2020_TTP_IP',
                        'HEU_TTP_FP', 'HEU_TTP_IP', 'grants_signature_activity',
                        'completion_previous_year_calls', 'grants_exceeding_fdi',
                        'amendment_cases_H2020', 'amendment_cases_HEU', 'external_audits_activity'
                    ],
                    'focus_metrics': [
                        'time', 'days', 'average', 'ttp', 'tta', 'ttg',
                        'payments', 'amendments', 'audits', 'grants', 'calls', 'total', 'count',
                        'amount', 'million', 'eur', 'recovery', 'appropriations',
                        'rate', 'percentage', 'completion', 'efficiency', 'targets', 'milestones'
                    ]
                },
                'output_configuration': {
                    'module': 'CommentsModule',
                    'variable_name': 'intro_summary_text',
                    'word_limit': 800,
                    'formatting_level': 'executive'
                },
                'instruction_mapping': {
                    'instruction_key': 'executive_summary_instructions',
                    'tone': 'executive',
                    'focus': 'comprehensive_achievements'
                }
            },

            # Target Text Analysis for Budget Overview:
            # ✅ Budget available for commitment and payments (H2020 + HEU)
            # ✅ Level of absorption analysis
            # ✅ Commitment and payment appropriations overview
            # ✅ Cross-program performance comparison
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
                    'primary_data': [
                        'summary_budget', 'pay_credits_H2020',
                        'pay_credits_HEU', 'commitments'
                    ],
                    'secondary_data': [
                        'grants_commitment_activity', 'current_year_global_commitment_activity',
                        'completion_previous_year_calls', 'TTP_Overview',
                        'H2020_All_Payments', 'HEU_All_Payments',
                        'H2020_payments_analysis_ALL', 'HEU_payments_analysis_ALL'
                    ],
                    'focus_metrics': [
                        'budget', 'appropriation', 'allocation', 'available', 'remaining',
                        'absorption', 'utilization', 'consumption', 'execution', 'spent',
                        'efficiency', 'variance', 'performance', 'rate', 'percentage',
                        'amount', 'total', 'credit', 'million', 'eur',
                        'h2020', 'heu', 'horizon', 'programme', 'program'
                    ]
                },
                'output_configuration': {
                    'module': 'CommentsModule',
                    'variable_name': 'budget_overview_text',
                    'word_limit': 500,
                    'formatting_level': 'detailed'
                },
                'instruction_mapping': {
                    'instruction_key': 'budget_overview_instructions',
                    'tone': 'analytical',
                    'focus': 'appropriations_and_absorption'
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
                    'primary_data': [
                        'grants_signature_activity', 'TTG', 'completion_previous_year_calls'
                    ],
                    'secondary_data': [
                        'grants_commitment_activity', 'current_year_global_commitment_activity', 'commitments'
                    ],
                    'focus_metrics': [
                        'grants', 'signature', 'signed', 'executed', 'agreement',
                        'calls', 'completion', 'progress', 'status', 'scheduled',
                        'preparation', 'allocation', 'process', 'timeline'
                    ]
                },
                'output_configuration': {
                    'module': 'CommentsModule',
                    'variable_name': 'granting_process_text',
                    'word_limit': 200,
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
                    'primary_data': [
                        'commitments', 'current_year_global_commitment_activity', 'grants_commitment_activity'
                    ],
                    'secondary_data': [
                        'grants_signature_activity', 'completion_previous_year_calls', 'summary_budget'
                    ],
                    'focus_metrics': [
                        'commitment', 'committed', 'amount', 'total', 'million', 'eur',
                        'budgetary', 'financial', 'l2', 'period', 'activity',
                        'breakdown', 'call', 'allocation'
                    ]
                },
                'output_configuration': {
                    'module': 'CommentsModule',
                    'variable_name': 'commitment_budgetary_text',
                    'word_limit': 250,
                    'formatting_level': 'detailed'
                },
                'instruction_mapping': {
                    'instruction_key': 'commitment_budgetary_instructions',
                    'tone': 'analytical',
                    'focus': 'financial_impact'
                }
            },

            # ============================================================
            # ✅ 5: OVERHAULED FDI Status Analysis
            # ============================================================
            'fdi_status_analysis': {
                'section_info': {
                    'name': 'Final Date for Implementation Status',
                    'category': 'compliance',
                    'priority': 5,
                    'description': 'FDI threshold monitoring and compliance analysis for L2 commitments'
                },
                'template_mapping': {
                    'template_name': 'fdi_status_template', # Uses the new, more detailed template
                    'template_category': 'fdi_compliance',
                    'supports_variables': ['prioritized_data_summary', 'secondary_data_summary']
                },
                'data_configuration': {
                    'primary_data': [
                        'grants_exceeding_fdi' # Core table with at-risk grants
                    ],
                    'secondary_data': [
                        'commitments', # Provides total commitment context
                        'summary_budget'   # Provides overall budget context
                    ],
                    'focus_metrics': [
                        'fdi', 'threshold', 'exceeding', 'risk', 'compliance',
                        'h2020', 'heu', 'commitment', 'l2', 'financial_value',
                        'cog', 'poc', 'stg', 'syg', 'adg', 'call', 'mitigation'
                    ]
                },
                'output_configuration': {
                    'module': 'CommentsModule',
                    'variable_name': 'fdi_status_text',
                    'word_limit': 400, # Increased for a more analytical text
                    'formatting_level': 'analytical'
                },
                'instruction_mapping': {
                    'instruction_key': 'fdi_status_instructions', # Links to new, detailed instructions
                    'tone': 'analytical',
                    'focus': 'risk_and_impact' # New focus for deeper analysis
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
                    'primary_data': [
                        'pay_credits_HEU', 'HEU_All_Payments', 'HEU_payments_analysis_ALL'
                    ],
                    'secondary_data': [
                        'HEU_TTP_FP', 'HEU_TTP_IP', 'HEU_TTP_PF', 'HEU_TTP_EXPERTS',
                        'HEU_payments_final_payments', 'HEU_payments_pre_financing_payments',
                        'HEU_payments_EXPERTS', 'summary_budget'
                    ],
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
                    'word_limit': 350,
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
                    'primary_data': [
                        'pay_credits_H2020', 'H2020_All_Payments', 'H2020_payments_analysis_ALL'
                    ],
                    'secondary_data': [
                        'H2020_TTP_FP', 'H2020_TTP_IP',
                        'H2020_payments_final_payments', 'H2020_payments_interim_payments',
                        'summary_budget'
                    ],
                    'focus_metrics': [
                        'h2020', 'horizon', 'payments', 'processed', 'total', 'amount',
                        'million', 'eur', 'credits', 'disbursed', 'allocation', 'annual',
                        'forecast', 'consumption', 'deviation', 'percentage', 'budget'
                    ]
                },
                'output_configuration': {
                    'module': 'CommentsModule',
                    'variable_name': 'h2020_payment_overview_text',
                    'word_limit': 350,
                    'formatting_level': 'detailed'
                },
                'instruction_mapping': {
                    'instruction_key': 'h2020_payment_overview_instructions',
                    'tone': 'analytical',
                    'focus': 'h2020_payment_consumption'
                }
            },

            # ============================================================
            # 🕐 TTP Performance Analysis Section
            # ============================================================
            'ttp_performance': {
                'section_info': {
                    'name': 'Time-to-Pay Performance Analysis',
                    'category': 'operational',
                    'priority': 8,
                    'description': 'TTP compliance analysis covering H2020 and HEU payment processing efficiency'
                },
                'template_mapping': {
                    'template_name': 'ttp_performance_template',
                    'template_category': 'operational_performance',
                    'supports_variables': [
                        'quarter_period', 'current_year', 'h2020_ttp_summary',
                        'heu_ttp_summary', 'prioritized_data_summary', 'secondary_data_summary'
                    ]
                },
                'data_configuration': {
                    'primary_data': [
                        'TTP_Overview', 'H2020_TTP_FP', 'H2020_TTP_IP',
                        'HEU_TTP_FP', 'HEU_TTP_IP', 'HEU_TTP_PF', 'HEU_TTP_EXPERTS'
                    ],
                    'secondary_data': [],
                    'focus_metrics': [
                        'ttp', 'time', 'days', 'delay', 'processing', 'timeline', 'duration',
                        'compliance', 'contractual', 'limits', 'percentage', 'rate', 'within',
                        'expert', 'pre-financing', 'interim', 'final', 'adg',
                        'executed', 'processed', 'efficiency', 'performance', 'quarterly', 'yearly',
                        'average', 'median', 'maximum', 'minimum', 'target', 'threshold'
                    ]
                },
                'output_configuration': {
                    'module': 'CommentsModule',
                    'variable_name': 'ttp_performance_text',
                    'word_limit': 400,
                    'formatting_level': 'operational'
                },
                'instruction_mapping': {
                    'instruction_key': 'ttp_performance_instructions',
                    'tone': 'operational',
                    'focus': 'compliance_and_efficiency'
                }
            },
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
        """
        Generate commentary for a specific section using the enhanced matrix system.
        This method is the primary entry point for generating any section, handling both
        single text outputs and complex outputs that require looping (like payment overviews).
        """
        # Step 1: Configuration Lookup
        mapping = self.mapping_matrix.get_complete_mapping_matrix()
        if section_key not in mapping:
            if verbose:
                print(f"❌ Section key '{section_key}' not found in mapping matrix")
            return None
        section_config = mapping[section_key]

        if verbose:
            print(f"📝 Generating: {section_config['section_info']['name']}")
            print(f"   Template: {section_config['template_mapping']['template_name']}")

        # ✅ FIX: Centralized logic to handle special "looping" sections like payment overviews.
        # This replaces the hardcoded logic that was previously in comments.py.
        if section_key in ['heu_payment_overview', 'h2020_payment_overview']:
            # This section requires generating multiple commentaries (one for each call type).
            # The method returns a single string summarizing the results, which the calling module can handle.
            return self._generate_payment_overview_combinations(
                section_key=section_key,
                quarter_period=quarter_period,
                current_year=current_year,
                financial_data=financial_data,
                model=model,
                temperature=temperature,
                acronym_context=acronym_context,
                cutoff_date=cutoff_date,
                verbose=verbose
            )

        # For all other "normal" sections, proceed with single commentary generation.
        return self._generate_single_section_commentary(
            section_key=section_key,
            section_config=section_config,
            quarter_period=quarter_period,
            current_year=current_year,
            financial_data=financial_data,
            model=model,
            temperature=temperature,
            acronym_context=acronym_context,
            cutoff_date=cutoff_date, # Not used here, but passed for consistency
            verbose=verbose
        )


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
        """Handles the generation of a standard, single-text section."""
        # Step 2: Template Retrieval
        templates = self.template_library.get_template_definitions(quarter_period, current_year)
        template_name = section_config['template_mapping']['template_name']
        if template_name not in templates:
            if verbose:
                print(f"❌ Template '{template_name}' not found in template library")
            return None
        template = templates[template_name]

        # Step 3: Data Preparation
        data_config = section_config['data_configuration']
        primary_data = {k: v for k, v in financial_data.items() if k in data_config['primary_data']}
        secondary_data = {k: v for k, v in financial_data.items() if k in data_config['secondary_data']}

        # Check if essential primary data is missing
        if not primary_data:
            if verbose:
                print(f"⚠️  No primary data found for section '{section_key}'. Required: {data_config['primary_data']}")
            return None

        # Step 4: Data Formatting
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

        # Step 5: Template Population
        # A dictionary to hold all possible variables for the template
        template_vars = {
            'prioritized_data_summary': prioritized_data_summary,
            'secondary_data_summary': secondary_data_summary,
            'h2020_ttp_summary': '', # Default empty values
            'heu_ttp_summary': '',
        }

        # For the TTP performance template, we need to prepare special summaries
        if section_key == 'ttp_performance':
            ttp_summaries = self._prepare_ttp_data_summary(financial_data, quarter_period, current_year)
            template_vars.update(ttp_summaries)

        # Use a relaxed format method that ignores unused keys
        formatted_template = self._format_template_safely(template, template_vars)


        # Step 6: Get AI Instructions
        instructions = self._get_section_instructions(section_config)

        # Step 7: Create Final Prompt
        final_prompt = self._create_enhanced_prompt(
            instructions=instructions,
            template=formatted_template,
            acronym_context=acronym_context,
            section_key=section_key,
            current_year=current_year,
            quarter_period=quarter_period
        )

        # Step 8: AI Generation with Retry Logic
        commentary = self._generate_with_retry(
            prompt=final_prompt,
            model=model,
            temperature=temperature,
            max_tokens=int(section_config['output_configuration']['word_limit'] * 1.8), # Increased multiplier
            section_key=section_key,
            verbose=verbose
        )

        return commentary

    def _format_template_safely(self, template: str, data: Dict[str, Any]) -> str:
        """Formats a string template, ignoring any keys not present in the data dictionary."""
        # This custom formatter will not raise an error if a key is missing.
        class SafeDict(dict):
            def __missing__(self, key):
                return f'{{{{{key}}}}}' # Return the placeholder itself if key is missing

        return template.format_map(SafeDict(**data))

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
    ) -> str: # ✅ FIX: This method should return a string, not a dictionary, to be compatible with the caller.
        """
        Generate payment overview for each call type combination and return a summary string.
        This centralized logic is now called from generate_section_commentary.
        """
        program = 'HEU' if 'heu' in section_key.lower() else 'H2020'
        call_types = ['STG', 'ADG', 'COG', 'SYG', 'POC', 'EXPERTS'] if program == 'HEU' else ['STG', 'ADG', 'COG', 'SYG']

        generated_texts = {}
        if verbose:
            print(f"🔄 Generating {program} payment overviews for {len(call_types)} call types")

        for call_type in call_types:
            if verbose:
                print(f"   📝 Generating {program}-{call_type} overview...")
            try:
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

        # Return a summary string that includes all generated texts.
        # This allows the main module to save each piece individually.
        if generated_texts:
            # We'll use a unique separator to make splitting easy in the calling module.
            summary_parts = []
            for var_name, text in generated_texts.items():
                summary_parts.append(f"VAR_NAME:{var_name}\n{text}")
            return "\n[---END_OF_SECTION---]\n".join(summary_parts)
        else:
            return None


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

        period_str = f"{quarter_period} {current_year}"

        executive_prompt_header = f"""
        🎯 EXECUTIVE BRIEFING GENERATION

        You are writing for senior EU executives and department heads. This text will appear in an official quarterly report.

        ⚠️ CRITICAL TIME PERIOD: Focus ONLY on {period_str}
        - Do NOT provide year-to-date analysis unless specifically asked.
        - Do NOT reference other quarters (e.g., Q2, Q3) unless comparing.
        - Focus ONLY on what happened during {period_str}.

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
        ❌ Technical data dumps or simple lists of numbers
        ❌ Negative language ("below target", "underperformed") - reframe positively
        ❌ Excessive detail without context
        ❌ Passive voice constructions
        ❌ Bullet points or markdown formatting in the final output unless requested
        """

        prompt_parts = [executive_prompt_header]

        if acronym_context and acronym_context.strip():
            prompt_parts.append(f"\n📚 REFERENCE INFORMATION (Acronyms):\n{acronym_context}")

        prompt_parts.append(f"\n🎯 SPECIFIC INSTRUCTIONS:\n{instructions}")
        prompt_parts.append(f"\n📄 CONTENT FRAMEWORK AND DATA:\n{template}")

        prompt_parts.append(f"""
        🎯 FINAL QUALITY CHECK:
        Before responding, ensure your text:
        • Sounds like it was written by a senior EU executive.
        • Emphasizes achievements and strategic success.
        • Uses specific metrics from the data with context.
        • Flows as sophisticated, well-structured paragraphs.
        • Demonstrates departmental excellence and command of the subject matter.

        Generate the executive briefing text now:
        """)

        return "\n".join(prompt_parts)

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
                # Sanitize key for display
                display_key = key.replace('_', ' ').upper()

                # Handle string values that might be JSON
                if isinstance(value, str):
                    try:
                        parsed = json.loads(value)
                        # Now treat parsed data as the new value
                        value = parsed
                    except json.JSONDecodeError:
                        # It's just a regular string, display it
                        summary_parts.append(f"\n{display_key}: {str(value)[:300]}{'...' if len(str(value)) > 300 else ''}")
                        continue # continue to next item in data_dict

                # Handle lists (likely from JSON or already parsed)
                if isinstance(value, list) and len(value) > 0:
                    summary_parts.append(f"\n{display_key} ({len(value)} records):")
                    for i, row in enumerate(value[:3], 1): # Show first 3 records
                        if isinstance(row, dict):
                            highlighted_items = []
                            other_items = []
                            for k, v in row.items():
                                item_str = f"{k}: {v}"
                                if any(metric.lower() in k.lower() for metric in focus_metrics):
                                    highlighted_items.append(f"**{item_str}**")
                                else:
                                    other_items.append(item_str)
                            # Show highlighted items first
                            row_summary = ", ".join(highlighted_items + other_items)
                            summary_parts.append(f"  Record {i}: {row_summary[:250]}{'...' if len(row_summary) > 250 else ''}")
                    if len(value) > 3:
                        summary_parts.append(f"  ... and {len(value) - 3} more records")

                # Handle dictionaries
                elif isinstance(value, dict):
                    summary_parts.append(f"\n{display_key}: {json.dumps(value, indent=2)}")

                # Handle other data types
                else:
                    summary_parts.append(f"\n{display_key}: {value}")

            except Exception:
                summary_parts.append(f"\n{display_key}: [Data processing error]")

        return "\n".join(summary_parts)

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
        h2020_ttp_data = {k: financial_data[k] for k in ['H2020_TTP_FP', 'H2020_TTP_IP', 'TTP_Overview'] if k in financial_data and financial_data[k] is not None}
        # Extract HEU TTP data
        heu_ttp_data = {k: financial_data[k] for k in ['HEU_TTP_FP', 'HEU_TTP_IP', 'HEU_TTP_PF', 'HEU_TTP_EXPERTS', 'TTP_Overview'] if k in financial_data and financial_data[k] is not None}

        # Create program-specific summaries
        h2020_summary = self._create_ttp_summary_for_ai("H2020", h2020_ttp_data, quarter_period, current_year)
        heu_summary = self._create_ttp_summary_for_ai("HEU", heu_ttp_data, quarter_period, current_year)

        return {
            'h2020_ttp_summary': h2020_summary,
            'heu_ttp_summary': heu_summary
        }


    def _create_ttp_summary_for_ai(self, program: str, ttp_data: Dict[str, Any], quarter_period: str, current_year: str) -> str:
        """Creates a concise TTP summary formatted for the AI to understand."""
        summary_parts = [
            f"DATA FOR {program} TTP PERFORMANCE - {quarter_period} {current_year}:",
        ]

        if not ttp_data:
            return f"DATA FOR {program} TTP: No data available."

        for key, value in ttp_data.items():
            summary_line = f"• {key}: "
            try:
                if isinstance(value, str):
                    parsed = json.loads(value)
                    if isinstance(parsed, list):
                        summary_line += f"{len(parsed)} records available."
                    else:
                        summary_line += "Data available."
                elif isinstance(value, list):
                     summary_line += f"{len(value)} records available."
                else:
                    summary_line += "Data available."
            except (json.JSONDecodeError, TypeError):
                summary_line += "Data available (non-JSON format)."
            summary_parts.append(summary_line)

        # Add an expected output format hint for the AI
        summary_parts.append("\nAI should analyze this data to report on compliance rates, processing times, and any delays noted during the quarter.")
        return "\n".join(summary_parts)


    def _get_section_instructions(self, section_config: Dict[str, Any]) -> str:
        """✅ REWRITTEN: Generate executive-quality instructions based on section."""
        section_info = section_config['section_info']
        output_config = section_config['output_configuration']
        word_limit = output_config['word_limit']
        section_name = section_info['name']

        # 🎯 UNIVERSAL EXECUTIVE WRITING GUIDELINES
        executive_guidelines = f"""
        EXECUTIVE WRITING REQUIREMENTS:
        - TONE & STYLE: Write for senior EU executives. Use a confident, achievement-focused, and strategic tone.
        - CONTENT: Lead with achievements. Use specific metrics from the data to support a strategic narrative. **Bold** key figures.
        - LANGUAGE: Use active voice (e.g., "The department achieved..."). Use professional terms like "executed," "implemented," "delivered."
        - FORMAT: Write in flowing paragraphs. Target approximately {word_limit} words. NO bullet points or markdown headers in the final text.
        """

        # 🎯 SECTION-SPECIFIC EXECUTIVE INSTRUCTIONS
        if 'intro' in section_name.lower() or 'summary' in section_name.lower():
            return f"""
            {executive_guidelines}
            MISSION: Generate a powerful departmental achievement summary demonstrating exceptional performance and strategic success.
            NARRATIVE STRUCTURE:
            1. **Opening Achievement Statement**: Start with a strong sentence summarizing the key success of the quarter.
            2. **Quantitative Excellence Showcase**: Detail key metrics for payments, time performance (TTP, TTG), and budget execution.
            3. **Strategic Workflow Achievements**: Discuss successes in granting, financial stewardship, and compliance.
            4. **Forward Excellence Outlook**: Conclude with a confident statement about future performance.
            """

        elif 'budget' in section_name.lower():
            return f"""
            {executive_guidelines}
            MISSION: Demonstrate strategic budget management and efficient appropriation utilization.
            NARRATIVE STRUCTURE:
            1. **Strategic Budget Position**: State the total budget and overall absorption rate, emphasizing financial stewardship.
            2. **Program Performance (H2020 vs HEU)**: Compare the utilization and performance of both main programs.
            3. **Strategic Financial Management**: Connect commitment and payment appropriations to concrete outcomes (e.g., grants signed, payments made).
            """

        elif 'payment' in section_name.lower() and ('heu' in section_name.lower() or 'h2020' in section_name.lower()):
            program_name = "Horizon Europe (HEU)" if 'heu' in section_name.lower() else "Horizon 2020 (H2020)"
            return f"""
            {executive_guidelines}
            MISSION: Showcase strategic payment processing and credit management excellence for the {program_name} program.
            NARRATIVE STRUCTURE:
            1. **Payment Volume Achievement**: State the total number and value of payments processed, linking it to annual targets.
            2. **Credit Management Excellence**: Detail the consumption of different credit types and their allocation across grant categories.
            3. **Strategic Performance Assessment**: Analyze forecast alignment and what consumption patterns indicate about budget management.
            """

        elif 'ttp' in section_name.lower():
            return f"""
            {executive_guidelines}
            MISSION: Showcase exceptional payment processing efficiency and leadership in contractual compliance (Time-to-Pay).
            NARRATIVE STRUCTURE:
            1. **Overall Compliance Achievement**: State the headline compliance rate for all payments within contractual limits.
            2. **Program-Specific Performance**: Detail and compare the TTP performance for both H2020 and HEU.
            3. **Exception Analysis & Impact**: Discuss any payments that were delayed, framing it in the context of complex verifications, and emphasize the overall operational excellence.
            """

        elif 'fdi' in section_name.lower(): # ✅ NEW INSTRUCTIONS FOR FDI
            return f"""
            {executive_guidelines}
            MISSION: Provide a strategic analysis of the Final Date for Implementation (FDI) status, demonstrating proactive financial risk management.
            NARRATIVE STRUCTURE:
            1. **Compliance & Risk Overview**: Start with a clear statement on the overall compliance with FDI thresholds and identify the total financial value of any at-risk commitments.
            2. **Program-Specific Analysis**: Breakdown the FDI status for H2020 and HEU, highlighting any specific trends or challenges in each program.
            3. **Financial Impact Assessment**: Analyze the significance of the commitments exceeding or approaching FDI, putting the financial figures into strategic context.
            4. **Proactive Mitigation Strategy**: Describe the department's actions to ensure at-risk commitments are legally finalized, demonstrating control and effective management.
            """


        else: # Default executive instructions for other sections
            return f"""
            {executive_guidelines}
            MISSION: Generate an executive-quality analysis for '{section_name}' demonstrating departmental achievement.
            STRUCTURE:
            1. Opening statement summarizing the key achievement. (25%)
            2. Analysis of performance metrics with strategic context, using data provided. (50%)
            3. Conclusion on impact and forward-looking perspective. (25%)
            Focus on accomplishments, efficiency, and the strategic value delivered.
            """

    def _generate_with_retry(
        self,
        prompt: str,
        model: str,
        temperature: float,
        max_tokens: int,
        section_key: str,
        verbose: bool,
        max_retries: int = 2
    ) -> Optional[str]:
        """Generate with retry logic for better quality"""
        from reporting.quarterly_report.modules.comments import CommentsConfig

        retry_count = 0
        current_temperature = temperature
        min_length = CommentsConfig.QUALITY_SETTINGS['min_response_length']

        while retry_count <= max_retries:
            if retry_count > 0:
                current_temperature += CommentsConfig.QUALITY_SETTINGS['retry_temperature_increment']
                if verbose:
                    print(f"   🔄 Retry {retry_count} with increased temperature: {current_temperature:.2f}")

            response = self._generate_with_model(
                prompt=prompt,
                model=model,
                temperature=current_temperature,
                max_tokens=max_tokens,
                verbose=verbose
            )

            if response and len(response) >= min_length and self._validate_response_quality(response, section_key):
                if verbose:
                    print(f"   ✅ Quality check passed on attempt {retry_count + 1}.")
                return response
            elif verbose:
                print(f"   ⚠️ Quality check failed for attempt {retry_count + 1}. Response length: {len(response) if response else 0} chars.")

            retry_count += 1

        if verbose:
            print(f"   ❌ Failed to generate a quality response after {max_retries + 1} attempts.")
        return None


    def _validate_response_quality(self, response: str, section_key: str) -> bool:
        """Validate response quality based on section requirements"""
        # Basic quality checks
        if not response or len(response) < 50: return False
        if not response.strip().endswith(('.', '!', '?', '"', ')')): return False # Check for incomplete sentence

        # Check for repetitive content
        sentences = response.split('.')
        if len(sentences) > 3:
            # Normalize sentences for comparison
            unique_sentences = {s.strip().lower() for s in sentences if len(s.strip()) > 10}
            if len(unique_sentences) < len(sentences) * 0.6:  # Over 40% repetition
                return False

        # Section-specific keyword checks
        if 'payment' in section_key or 'ttp' in section_key:
            if not any(kw in response.lower() for kw in ['€', 'eur', 'million', 'payment', 'ttp', 'compliance']):
                return False
        if 'budget' in section_key:
            if not any(kw in response.lower() for kw in ['appropriation', 'allocation', 'budget', 'consumption']):
                return False

        return True


    def _generate_with_model(self, prompt: str, model: str, temperature: float, max_tokens: int, verbose: bool) -> Optional[str]:
        """Generate with executive quality enforcement and reasoning model support"""
        import requests
        try:
            payload = {
                "model": model,
                "prompt": prompt,
                "stream": False,
                "options": {
                    "temperature": temperature,
                    "num_predict": max_tokens,
                    "top_p": 0.9,
                    "top_k": 40,
                    "repeat_penalty": 1.15 # Slightly increased to reduce repetition
                }
            }
            if verbose:
                print(f"   🤖 Calling model {model} (Temp: {temperature:.2f}, Max Tokens: {max_tokens})...")

            response = requests.post("http://localhost:11434/api/generate", json=payload, timeout=240)

            if response.status_code == 200:
                result = response.json()
                commentary = result.get('response', '').strip()
                # Clean the response from markdown and other artifacts
                return self._clean_generated_text(commentary)
            else:
                if verbose:
                    print(f"   ❌ Model API error: {response.status_code} - {response.text}")
                return None

        except requests.exceptions.RequestException as e:
            if verbose:
                print(f"   ❌ Generation request error: {e}")
            return None
        except Exception as e:
            if verbose:
                print(f"   ❌ An unexpected error occurred during generation: {e}")
            return None

    def _clean_generated_text(self, text: str) -> str:
        """Clean generated text of any unwanted formatting like markdown headers or AI conversational fillers."""
        # Remove markdown headers
        text = re.sub(r'^#+\s+', '', text, flags=re.MULTILINE)
        # Remove conversational openings
        text = re.sub(r'^(Here is the|Here\'s a|Certainly, here is the).+?\n\n', '', text, flags=re.IGNORECASE)
        # Remove bullet points
        text = re.sub(r'^\s*[-*•]\s+', '', text, flags=re.MULTILINE)
        # Consolidate multiple newlines
        text = re.sub(r'\n{3,}', '\n\n', text)
        return text.strip()

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