import apache_beam as beam
from apache_beam.options.pipeline_options import PipelineOptions, GoogleCloudOptions, StandardOptions, SetupOptions
from apache_beam import Map, Filter, CombinePerKey, FlatMap
from apache_beam.io import ReadFromText, WriteToBigQuery
from apache_beam.io.gcp.bigquery import BigQueryDisposition
import csv
import logging
import os
import datetime
import argparse

# Configure logging for error tracking
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

# Set the environment variable for Google Application Credentials
os.environ['GOOGLE_APPLICATION_CREDENTIALS'] = os.path.join(os.path.dirname(os.path.abspath(__file__)), 'e2e-project-etl-cc9bf99bbb88.json')

def run(argv=None):
    """Main entry point for the loan data pipeline."""
    # Parse command line arguments
    parser = argparse.ArgumentParser(description='Loan Data Processing Pipeline')
    parser.add_argument('--input', dest='input', default='gs://loan-input-data-e2e-project-etl/loans.csv',
                        help='Input file to process')
    parser.add_argument('--output', dest='output', default='e2e-project-etl:loan_data.customer_loans',
                        help='Output BigQuery table')
    
    # Parse known args and pass the rest to the pipeline options
    known_args, pipeline_args = parser.parse_known_args(argv)
    
    # Set up the pipeline options
    options = PipelineOptions(pipeline_args)
    
    # Set GoogleCloudOptions
    google_cloud_options = options.view_as(GoogleCloudOptions)
    if not google_cloud_options.project:
        google_cloud_options.project = 'e2e-project-etl'
    
    if not google_cloud_options.job_name:
        google_cloud_options.job_name = 'loan-pipeline-job'
    
    # Ensure temp and staging locations are set
    if not google_cloud_options.staging_location:
        google_cloud_options.staging_location = 'gs://dataflow-temp-e2e-project-etl/staging'
    
    if not google_cloud_options.temp_location:
        google_cloud_options.temp_location = 'gs://dataflow-temp-e2e-project-etl/temp'
    
    # Set the runner (DataflowRunner for cloud, DirectRunner for local)
    if not options.view_as(StandardOptions).runner:
        options.view_as(StandardOptions).runner = 'DataflowRunner'
    
    # Enable package installation
    options.view_as(SetupOptions).save_main_session = True
    
    # Start the pipeline
    with beam.Pipeline(options=options) as p:
        # Step 1: Read CSV from Cloud Storage
        lines = p | 'ReadFromGCS' >> ReadFromText(known_args.input)

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
        # total_repayment = loan_amount * (1 + (interest_rate / 100) * (loan_term / 12))
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
                base_score = 700  # Increased base score to allow for more medium and low risk loans
                
                # Adjust factors to create more balanced distribution
                term_factor = min(30, loan_term / 3)  # Reduced impact of term
                amount_factor = min(50, loan_amount / 2000)  # Reduced impact of amount
                rate_factor = min(150, interest_rate * 12)  # Increased impact of interest rate
                
                # Interest rate is now the primary driver of risk score
                if interest_rate <= 6.0:
                    base_score = 780  # Low interest rates get higher base score
                elif interest_rate <= 10.0:
                    base_score = 700  # Medium interest rates get medium base score
                else:
                    base_score = 600  # High interest rates get lower base score
                
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

        # Step 9: Format for BigQuery
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

        # Step 10: Write to BigQuery
        output | 'WriteToBigQuery' >> WriteToBigQuery(
            known_args.output,  # Output table from command line args
            schema='''
            customer_id:STRING,
            total_loans:FLOAT,
            total_repayments:FLOAT,
            total_monthly_payment:FLOAT,
            avg_risk_score:INTEGER,
            overall_risk_category:STRING,
            low_risk_loans:INTEGER,
            medium_risk_loans:INTEGER,
            high_risk_loans:INTEGER,
            debt_service_ratio:FLOAT,
            loan_to_value_ratio:FLOAT,
            processing_timestamp:TIMESTAMP
            ''',
            write_disposition=BigQueryDisposition.WRITE_TRUNCATE,
            create_disposition=BigQueryDisposition.CREATE_IF_NEEDED
        )

if __name__ == '__main__':
    logging.getLogger().setLevel(logging.INFO)
    run()
