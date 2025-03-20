import apache_beam as beam
from apache_beam.options.pipeline_options import PipelineOptions, StandardOptions
from apache_beam import Map, Filter, CombinePerKey, FlatMap
import csv
import logging
import os
import json
import datetime

# Configure logging
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

# Set the environment variable for Google Application Credentials
os.environ['GOOGLE_APPLICATION_CREDENTIALS'] = os.path.join(os.path.dirname(os.path.abspath(__file__)), 'e2e-project-etl-cc9bf99bbb88.json')

# Define pipeline options for local runner
options = PipelineOptions()
options.view_as(StandardOptions).runner = 'DirectRunner'

# Start the pipeline
with beam.Pipeline(options=options) as p:
    # Step 1: Read CSV from local file
    lines = p | 'ReadFromLocal' >> beam.io.ReadFromText('loans.csv')

    # Step 2: Parse CSV lines with error handling
    def parse_csv(line):
        try:
            return next(csv.reader([line]))
        except Exception as e:
            logger.error(f"Error parsing line: {line}, Error: {e}")
            return None

    parsed = lines | 'ParseCSV' >> Map(parse_csv)

    # Step 3: Filter out parsing errors
    parsed = parsed | 'FilterParsingErrors' >> Filter(lambda x: x is not None)

    # Step 4: Remove header row
    data = parsed | 'FilterHeader' >> Filter(lambda row: row[0] != 'loan_id')

    # Step 5: Filter invalid loans (e.g., negative amounts)
    valid_data = data | 'FilterInvalidLoans' >> Filter(lambda row: float(row[2]) > 0)

    # Step 6: Calculate repayment (simple interest formula)
    def calculate_repayment(row):
        try:
            loan_id = row[0]
            customer_id = row[1]
            loan_amount = float(row[2])
            interest_rate = float(row[3])
            loan_term = int(row[4])  # months
            issue_date = row[5]
            
            # Calculate total repayment using simple interest formula
            total_repayment = loan_amount * (1 + (interest_rate / 100) * (loan_term / 12))
            
            # Calculate monthly payment
            monthly_payment = total_repayment / loan_term
            
            # Calculate debt-to-income ratio (simplified version)
            # Assuming higher loan amounts relative to repayment capacity (monthly payment) indicate higher risk
            dti_ratio = monthly_payment / (loan_amount / 100)  # Simplified DTI calculation
            
            # Calculate credit risk score (simplified model)
            # Lower score = higher risk, scale 300-850
            base_score = 600
            term_factor = min(50, loan_term / 2)  # Longer terms slightly higher risk
            amount_factor = min(100, loan_amount / 1000)  # Higher amounts slightly higher risk
            rate_factor = min(100, interest_rate * 10)  # Higher rates indicate higher risk borrowers
            
            credit_risk_score = int(base_score + term_factor - amount_factor - rate_factor)
            credit_risk_score = max(300, min(850, credit_risk_score))  # Ensure within FICO range
            
            # Determine loan status category
            if credit_risk_score >= 750:
                risk_category = "Low Risk"
            elif credit_risk_score >= 650:
                risk_category = "Medium Risk"
            else:
                risk_category = "High Risk"
                
            return (customer_id, (loan_amount, total_repayment, monthly_payment, credit_risk_score, risk_category))
        except Exception as e:
            logger.error(f"Error calculating repayment for row: {row}, Error: {e}")
            return None

    repayments = valid_data | 'CalculateRepayment' >> Map(calculate_repayment)

    # Step 7: Filter out calculation errors
    repayments = repayments | 'FilterCalculationErrors' >> Filter(lambda x: x is not None)

    # Step 8: Aggregate totals per customer
    def sum_loans_and_repayments(values):
        total_loans = sum(loan for loan, *_ in values)
        total_repayments = sum(repayment for _, repayment, *_ in values)
        total_monthly_payment = sum(monthly for _, _, monthly, *_ in values)
        
        # Calculate average risk score weighted by loan amount
        weighted_risk_score_sum = 0
        for value in values:
            loan = value[0]  # loan_amount is the first item
            risk_score = value[3] if len(value) > 3 else 0  # risk_score is the fourth item if it exists
            weighted_risk_score_sum += loan * risk_score
            
        avg_risk_score = int(weighted_risk_score_sum / total_loans) if total_loans > 0 else 0
        
        # Determine overall risk category based on average score
        if avg_risk_score >= 750:
            overall_risk = "Low Risk"
        elif avg_risk_score >= 650:
            overall_risk = "Medium Risk"
        else:
            overall_risk = "High Risk"
            
        # Count loans by risk category
        risk_counts = {}
        for value in values:
            if len(value) > 4:  # Make sure we have a category
                category = value[4]  # category is the fifth item
                risk_counts[category] = risk_counts.get(category, 0) + 1
            
        low_risk_count = risk_counts.get("Low Risk", 0)
        medium_risk_count = risk_counts.get("Medium Risk", 0)
        high_risk_count = risk_counts.get("High Risk", 0)
        
        return (total_loans, total_repayments, total_monthly_payment, 
                avg_risk_score, overall_risk, low_risk_count, medium_risk_count, high_risk_count)

    aggregated = repayments | 'SumPerCustomer' >> CombinePerKey(sum_loans_and_repayments)

    # Step 9: Format for output
    def format_output(element):
        customer_id, (total_loans, total_repayments, total_monthly_payment, 
                     avg_risk_score, overall_risk, low_risk_count, 
                     medium_risk_count, high_risk_count) = element
        
        # Calculate debt service ratio (monthly payment to total loan ratio)
        debt_service_ratio = (total_monthly_payment * 12) / total_loans if total_loans > 0 else 0
        
        # Calculate loan-to-value ratio (simplified for this example)
        ltv_ratio = total_loans / (total_loans * 1.25)  # Assuming collateral value is 125% of loan amount
        
        # Current timestamp for the record
        processing_timestamp = datetime.datetime.now().strftime("%Y-%m-%d %H:%M:%S")
        
        return {
            'customer_id': customer_id,
            'total_loans': total_loans,
            'total_repayments': total_repayments,
            'total_monthly_payment': total_monthly_payment,
            'avg_risk_score': avg_risk_score,
            'overall_risk_category': overall_risk,
            'low_risk_loans': low_risk_count,
            'medium_risk_loans': medium_risk_count,
            'high_risk_loans': high_risk_count,
            'debt_service_ratio': debt_service_ratio,
            'loan_to_value_ratio': ltv_ratio,
            'processing_timestamp': processing_timestamp
        }

    output = aggregated | 'FormatOutput' >> Map(format_output)

    # Step 10: Write to local file
    output | 'WriteToJSON' >> beam.io.WriteToText('local_results.json')

print("Pipeline completed. Results written to local_results.json")
