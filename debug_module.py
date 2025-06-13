import json
import sys
from pathlib import Path
from ingestion.db_utils import fetch_vars_for_report

# Add your project path if needed
sys.path.append(str(Path(__file__).parent.parent.parent))



def diagnose_data_structure(financial_data):
    """Analyze actual data structure to understand why call type matching fails"""
    
    print("🔍 DATA STRUCTURE DIAGNOSTIC")
    print("=" * 50)
    
    payment_tables = {k: v for k, v in financial_data.items() if 'payment' in k.lower() and v is not None}
    
    print(f"📊 Found {len(payment_tables)} payment-related tables")
    
    for table_name, data in list(payment_tables.items())[:3]:  # Check first 3
        print(f"\n📋 TABLE: {table_name}")
        
        try:
            if isinstance(data, str):
                parsed = json.loads(data)
            else:
                parsed = data
            
            if isinstance(parsed, list) and len(parsed) > 0:
                sample_record = parsed[0]
                print(f"   Records: {len(parsed)}")
                print(f"   Sample fields: {list(sample_record.keys())}")
                
                # Check for Budget Address Type values
                budget_types = set()
                call_type_indicators = set()
                
                for record in parsed[:5]:  # Check first 5 records
                    if isinstance(record, dict):
                        # Check Budget Address Type
                        budget_type = record.get('Budget Address Type', '')
                        if budget_type:
                            budget_types.add(budget_type)
                        
                        # Look for any field that might contain call types
                        for key, value in record.items():
                            if any(ct in str(value).upper() for ct in ['STG', 'ADG', 'COG', 'POC', 'SYG']):
                                call_type_indicators.add(f"{key}: {value}")
                
                print(f"   Budget Address Types: {list(budget_types)}")
                if call_type_indicators:
                    print(f"   Call type indicators found: {list(call_type_indicators)}")
                else:
                    print(f"   ❌ No call type indicators (STG, ADG, etc.) found in data")
                
        except Exception as e:
            print(f"   ❌ Error parsing table: {e}")
    
    return payment_tables

def test_mapping_strategy(financial_data, payment_tables):
    """Test mapping strategy with corrected data"""
    
    print("\n🎯 CORRECTED CALL TYPE MAPPING TEST")
    print("=" * 50)
    
    # Collect all payment records from corrected tables
    all_payment_records = []
    
    for table_name, data in payment_tables.items():
        try:
            if isinstance(data, str):
                parsed = json.loads(data)
            else:
                parsed = data
            
            if isinstance(parsed, list):
                all_payment_records.extend(parsed)
                print(f"✅ Added {len(parsed)} records from {table_name}")
        except:
            continue
    
    print(f"\n📊 Total payment records for mapping test: {len(all_payment_records)}")
    
    if len(all_payment_records) > 0:
        
        # Test Budget Address Type mapping
        call_type_mapping = {
            'STG': ['Starting', 'Early Career', 'Main Calls'],
            'ADG': ['Advanced', 'Established', 'Main Calls'], 
            'COG': ['Consolidator', 'Mid Career', 'Main Calls'],
            'POC': ['Proof of Concept', 'Commercialization', 'Main Calls'],
            'SYG': ['Synergy', 'Collaborative', 'Main Calls'],
            'EXPERTS': ['Experts', 'Expert', 'Support'],
        }
        
        # Check what Budget Address Types are available
        available_budget_types = set()
        for record in all_payment_records:
            if isinstance(record, dict):
                budget_type = record.get('Budget Address Type', '')
                if budget_type:
                    available_budget_types.add(budget_type)
        
        print(f"📋 Available Budget Address Types: {list(available_budget_types)}")
        
        # Test mapping
        for call_type, mapping_options in call_type_mapping.items():
            matches = 0
            for record in all_payment_records:
                if isinstance(record, dict):
                    budget_type = record.get('Budget Address Type', '')
                    if any(option.lower() in budget_type.lower() for option in mapping_options):
                        matches += 1
            
            print(f"   {call_type}: {matches} potential matches")
        
        print(f"\n🎯 EXPECTED IMPROVEMENT: With {len(all_payment_records)} records, call type generation should work!")
    else:
        print("❌ No payment records found even with corrected names")

def test_fallback_strategy(financial_data):
    """Test fallback to program-level summaries"""
    
    print("\n🔄 FALLBACK STRATEGY TEST")
    print("=" * 50)
    
    programs = ['HEU', 'H2020']
    
    for program in programs:
        program_key = f"pay_credits_{program}"
        
        if program_key in financial_data and financial_data[program_key] is not None:
            try:
                if isinstance(financial_data[program_key], str):
                    data = json.loads(financial_data[program_key])
                else:
                    data = financial_data[program_key]
                
                if isinstance(data, list):
                    print(f"✅ {program} program data available: {len(data)} records")
                    
                    # Calculate summary stats
                    total_available = sum(float(r.get('Available_Payment_Appropriations', 0) or 0) for r in data)
                    total_paid = sum(float(r.get('Paid_Amount', 0) or 0) for r in data)
                    
                    print(f"   Available: €{total_available/1000000:.1f}M, Paid: €{total_paid/1000000:.1f}M")
                    print(f"   Consumption rate: {(total_paid/total_available*100):.1f}%" if total_available > 0 else "   Consumption rate: N/A")
                else:
                    print(f"❌ {program} data not in expected format")
            except Exception as e:
                print(f"❌ {program} data parsing error: {e}")
        else:
            print(f"❌ {program} program data not found")

def validate_text_quality_inputs(financial_data):
    """Validate that data is rich enough for quality text generation"""
    
    print("\n📝 TEXT QUALITY INPUT VALIDATION")
    print("=" * 50)
    
    # Check for narrative-friendly data
    narrative_elements = {
        'financial_amounts': 0,
        'percentages': 0, 
        'time_references': 0,
        'categorical_data': 0,
        'descriptive_fields': 0
    }
    
    sample_count = 0
    for key, data in financial_data.items():
        if data is not None and sample_count < 5:  # Check first 5 tables
            try:
                if isinstance(data, str):
                    parsed = json.loads(data)
                else:
                    parsed = data
                
                if isinstance(parsed, list) and len(parsed) > 0:
                    sample_count += 1
                    sample_record = parsed[0]
                    
                    if isinstance(sample_record, dict):
                        for field_name, value in sample_record.items():
                            # Check for financial amounts
                            if any(word in field_name.lower() for word in ['amount', 'payment', 'appropriation', 'budget']):
                                if isinstance(value, (int, float)) and value > 0:
                                    narrative_elements['financial_amounts'] += 1
                            
                            # Check for percentage/ratio fields
                            if any(word in field_name.lower() for word in ['ratio', 'rate', 'percent']):
                                narrative_elements['percentages'] += 1
                            
                            # Check for categorical data
                            if any(word in field_name.lower() for word in ['type', 'category', 'source', 'status']):
                                narrative_elements['categorical_data'] += 1
                            
                            # Check for descriptive fields
                            if isinstance(value, str) and len(value) > 10:
                                narrative_elements['descriptive_fields'] += 1
            except:
                continue
    
    print("📊 NARRATIVE ELEMENT ASSESSMENT:")
    for element, count in narrative_elements.items():
        status = "✅ Good" if count >= 3 else "⚠️ Limited" if count >= 1 else "❌ Missing"
        print(f"   {element.replace('_', ' ').title()}: {count} fields - {status}")
    
    total_score = sum(narrative_elements.values())
    if total_score >= 10:
        print(f"\n✅ OVERALL: Good data richness for quality text generation ({total_score} narrative elements)")
    elif total_score >= 5:
        print(f"\n⚠️ OVERALL: Moderate data richness, may need enhanced prompting ({total_score} narrative elements)")
    else:
        print(f"\n❌ OVERALL: Limited data richness, significant prompt improvement needed ({total_score} narrative elements)")

def run_complete_diagnosis(financial_data):
    """Run complete diagnostic suite"""
    
    print("🔧 COMMENTS MODULE COMPLETE DIAGNOSIS")
    print("=" * 60)
    print("Run this BEFORE applying fixes to understand current issues")
    print("=" * 60)
    
    # Run all diagnostics
    payment_tables = diagnose_data_structure(financial_data)
    test_mapping_strategy(financial_data)
    test_fallback_strategy(financial_data)
    validate_text_quality_inputs(financial_data)
    
    print("\n" + "=" * 60)
    print("📋 DIAGNOSTIC SUMMARY AND RECOMMENDATIONS")
    print("=" * 60)
    
    recommendations = []
    
    if len(payment_tables) > 0:
        recommendations.append("✅ Payment data available - implement improved extraction logic")
    else:
        recommendations.append("❌ No payment data found - check data mapping")
    
    recommendations.append("🔧 REQUIRED: Implement Budget Address Type mapping strategy")
    recommendations.append("🔄 RECOMMENDED: Add fallback to program-level summaries")
    recommendations.append("📝 RECOMMENDED: Enhance AI prompting for better text quality")
    
    for i, rec in enumerate(recommendations, 1):
        print(f"{i}. {rec}")
    
    print(f"\n🎯 EXPECTED IMPROVEMENT: 0/10 → 6-8/10 loop success rate")
    print(f"📈 EXPECTED QUALITY: Significant improvement in text professionalism")

def debug_section_mapping(self):
    """Debug which sections are being skipped and why"""
    
    from reporting.quarterly_report.modules.comments import CommentsConfig
    
    SINGLE_SECTIONS = CommentsConfig.SINGLE_SECTIONS
    mapping = self.mapping_matrix.get_complete_mapping_matrix()
    
    print("🔍 SECTION MAPPING DIAGNOSTIC")
    print("=" * 50)
    
    for i, section in enumerate(SINGLE_SECTIONS, 1):
        print(f"\n{i}. {section}")
        
        if section in mapping:
            template_name = mapping[section]['template_mapping']['template_name']
            var_name = mapping[section]['output_configuration']['variable_name']
            print(f"   ✅ Mapping exists")
            print(f"   📄 Template: {template_name}")
            print(f"   💾 Variable: {var_name}")
        else:
            print(f"   ❌ NO MAPPING FOUND")
            
            # Suggest similar keys
            similar = [k for k in mapping.keys() if section.replace('_', '') in k.replace('_', '')]
            if similar:
                print(f"   💡 Similar keys: {similar}")
    
    print(f"\n📊 Available mapping keys:")
    for key in sorted(mapping.keys()):
        print(f"   • {key}")

# ================================================================
# 5. QUICK TEST METHOD
# ================================================================

def test_single_section_generation(self, section_name: str = "budget_overview"):
    """Test generation of a single section with enhanced diagnostics"""
    
    print(f"🧪 TESTING SINGLE SECTION: {section_name}")
    print("=" * 50)
    
    # Test data
    financial_data = {'test_data': [{'amount': 1000000, 'type': 'payment'}]}
    
    try:
        result = self.generate_section_commentary(
            section_key=section_name,
            quarter_period="Q1",
            current_year="2025", 
            financial_data=financial_data,
            model="qwen2.5:14b",
            temperature=0.3,
            acronym_context="",
            verbose=True
        )
        
        if result:
            print(f"✅ SUCCESS!")
            print(f"📝 Length: {len(result.split())} words")
            print(f"🔍 Preview: {result[:200]}...")
            
            # Check for formatting issues
            has_markdown = any(marker in result for marker in ['###', '####', '|', '```'])
            if has_markdown:
                print(f"⚠️  WARNING: Contains markdown formatting")
            else:
                print(f"✅ Clean text format")
                
        else:
            print(f"❌ FAILED: No result generated")
            
    except Exception as e:
        print(f"❌ ERROR: {e}")
        import traceback
        print(f"📋 Traceback: {traceback.format_exc()}")


# [PASTE THE ENTIRE DIAGNOSTIC CODE HERE]

def main():
    """Main diagnostic runner"""
    
    # Configuration
    report_name = "Quarterly_Report"
    db_path = "database/reporting.db"  # Adjust path as needed
    
    print("🔧 LOADING DATA FOR DIAGNOSIS...")
    
    try:
        # Load actual report data
        report_vars = fetch_vars_for_report(report_name, db_path)
        
        # Map to financial data structure (same as in CommentsModule)
        financial_data = {
            'commitments': report_vars.get('table_1a'),
            'pay_credits_H2020': report_vars.get('table_2a_H2020'),
            'pay_credits_HEU': report_vars.get('table_2a_HE'),
            'summary_budget': report_vars.get('overview_budget_table'),
            'HEU_payments_all': report_vars.get('HEU_All_Payments'),
            'H2020_payments_all': report_vars.get('H2020_All_Payments'),
            # Add other mappings as needed
        }
        
        # Filter out None values
        financial_data = {k: v for k, v in financial_data.items() if v is not None}
        
        print(f"✅ Loaded {len(financial_data)} data tables")
        
        # Run complete diagnosis
        run_complete_diagnosis(financial_data)
        
    except Exception as e:
        print(f"❌ Error loading data: {e}")
        print("💡 Make sure database path and report name are correct")

if __name__ == "__main__":
    main()