#!/usr/bin/env python3
import csv
import random
import datetime

# Define the number of records to generate
NUM_RECORDS = 500

# Define the range of customer IDs
MIN_CUSTOMER_ID = 1001
MAX_CUSTOMER_ID = 1200

# Define different risk profiles
RISK_PROFILES = {
    'Low Risk': {
        'min_loan': 1000.0,
        'max_loan': 20000.0,
        'min_interest': 3.0,
        'max_interest': 6.0,
        'probability': 0.3  # 30% of customers
    },
    'Medium Risk': {
        'min_loan': 5000.0,
        'max_loan': 35000.0,
        'min_interest': 5.5,
        'max_interest': 10.0,
        'probability': 0.4  # 40% of customers
    },
    'High Risk': {
        'min_loan': 10000.0,
        'max_loan': 50000.0,
        'min_interest': 9.0,
        'max_interest': 15.0,
        'probability': 0.3  # 30% of customers
    }
}

# Define the range of loan terms (in months)
LOAN_TERMS = [12, 24, 36, 48, 60, 72, 84, 96, 120]

# Define the date range for issue dates
START_DATE = datetime.date(2022, 1, 1)
END_DATE = datetime.date(2025, 3, 1)

def random_date(start_date, end_date):
    """Generate a random date between start_date and end_date."""
    days_between = (end_date - start_date).days
    random_days = random.randint(0, days_between)
    return start_date + datetime.timedelta(days=random_days)

def generate_loan_data(num_records):
    """Generate random loan data with different risk profiles."""
    loans = []
    
    # Add header
    header = ["loan_id", "customer_id", "loan_amount", "interest_rate", "loan_term", "issue_date"]
    loans.append(header)
    
    # Assign risk profiles to customer IDs
    customer_profiles = {}
    for cust_id in range(MIN_CUSTOMER_ID, MAX_CUSTOMER_ID + 1):
        # Randomly assign a risk profile based on probabilities
        rand_val = random.random()
        cumulative_prob = 0
        for profile, details in RISK_PROFILES.items():
            cumulative_prob += details['probability']
            if rand_val <= cumulative_prob:
                customer_profiles[cust_id] = profile
                break
    
    # Generate random loan records
    for i in range(1, num_records + 1):
        loan_id = i
        customer_id = random.randint(MIN_CUSTOMER_ID, MAX_CUSTOMER_ID)
        profile = customer_profiles[customer_id]
        profile_details = RISK_PROFILES[profile]
        
        # Occasionally introduce negative loan amounts (bad data) for testing error handling
        if random.random() < 0.02:  # 2% chance of negative value
            loan_amount = -random.uniform(profile_details['min_loan'], profile_details['max_loan'])
        else:
            loan_amount = random.uniform(profile_details['min_loan'], profile_details['max_loan'])
        
        loan_amount = round(loan_amount, 2)
        interest_rate = round(random.uniform(profile_details['min_interest'], profile_details['max_interest']), 2)
        loan_term = random.choice(LOAN_TERMS)
        issue_date = random_date(START_DATE, END_DATE).isoformat()
        
        loan_record = [loan_id, customer_id, loan_amount, interest_rate, loan_term, issue_date]
        loans.append(loan_record)
    
    return loans

def write_to_csv(data, filename):
    """Write data to a CSV file."""
    with open(filename, 'w', newline='', encoding='utf-8') as csvfile:
        writer = csv.writer(csvfile)
        writer.writerows(data)
    print(f"Generated {len(data) - 1} loan records and saved to {filename}")

if __name__ == "__main__":
    loan_data = generate_loan_data(NUM_RECORDS)
    write_to_csv(loan_data, "loans.csv")
