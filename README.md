# GCP Loan Data Processing Pipeline

![Apache Beam](https://img.shields.io/badge/Apache%20Beam-2.46.0-blue)
![Google Cloud](https://img.shields.io/badge/Google%20Cloud-Dataflow-4285F4)
![Python](https://img.shields.io/badge/Python-3.9%2B-yellow)

A scalable data processing pipeline for loan data using Google Cloud Platform services. This pipeline generates synthetic loan data, processes it using Apache Beam and Google Cloud Dataflow, and stores the results in BigQuery for analysis.

## 📋 Features

- Generate synthetic loan data with different risk profiles (Low, Medium, High)
- Process loan data using Apache Beam and Google Cloud Dataflow
- Calculate loan repayment amounts and risk scores
- Aggregate data by customer
- Categorize loans into risk categories
- Store results in BigQuery for analysis

## 🏗️ Architecture

![Architecture Diagram](docs/images/architecture.svg)

The architecture shows how data flows through the pipeline, from synthetic data generation to final analysis in BigQuery. The pipeline processes loan data, calculates repayments, categorizes loans by risk, and stores the results in BigQuery for analysis.

## 🗂️ Project Structure

```
gcp-loan-data-pipeline/
├── README.md                   # Project overview and instructions
├── requirements.txt           # Python dependencies
├── LICENSE                    # MIT License
├── src/
│   ├── generate_loan_data.py  # Script to generate synthetic loan data
│   ├── loan_pipeline.py       # Apache Beam pipeline for processing loan data
│   └── run_local.py           # Script to run the pipeline locally
├── scripts/
│   ├── fix_permissions.sh     # Script to set up GCP permissions
│   ├── run_dataflow.sh        # Script to deploy to Google Cloud Dataflow
│   ├── setup_gcp.sh           # Script to set up GCP resources
│   └── setup_and_run.sh       # Script to set up and run the pipeline
├── data/
│   └── loans.csv              # Sample generated loan data
└── docs/
    ├── PROJECT_DOCUMENTATION.md  # Detailed project documentation
    └── images/                # Diagrams and screenshots
        └── architecture.svg   # Architecture diagram
```

## 🚀 Getting Started

### Prerequisites

- Google Cloud Platform account
- Python 3.9 or higher
- Google Cloud SDK installed and configured

### Setup

1. Clone this repository:
   ```bash
   git clone https://github.com/Mamidi7/gcp-loan-data-pipeline.git
   cd gcp-loan-data-pipeline
   ```

   The repository structure follows the organization shown above.

2. Install dependencies:
   ```bash
   pip install -r requirements.txt
   ```

3. Set up GCP resources:
   ```bash
   ./scripts/setup_gcp.sh
   ```

4. Set up permissions for your service account:
   ```bash
   ./scripts/fix_permissions.sh
   ```

### Generate Loan Data

Generate synthetic loan data with different risk profiles:

```bash
python src/generate_loan_data.py
```

This will create a `loans.csv` file with 500 loan records.

### Run the Pipeline

#### Option 1: Run on Google Cloud Dataflow

```bash
./scripts/run_dataflow.sh
```

#### Option 2: Run Locally

```bash
python src/run_local.py
```

### Verify Results

Query the results in BigQuery:

```sql
SELECT overall_risk_category, COUNT(*) as count, AVG(total_loans) as avg_loan_amount 
FROM `e2e-project-etl.loan_data.customer_loans` 
GROUP BY overall_risk_category 
ORDER BY count DESC
```

## 📊 Sample Results

```
+-----------------------+-------+--------------------+
| overall_risk_category | count |  avg_loan_amount   |
+-----------------------+-------+--------------------+
| High Risk             |   103 |  71562.56097087379 |
| Medium Risk           |    65 |  29759.84676923077 |
| Low Risk              |    14 | 21508.394285714287 |
+-----------------------+-------+--------------------+
```

## 📝 Detailed Documentation

For a detailed explanation of the project, including the development process, challenges, and solutions, see [docs/PROJECT_DOCUMENTATION.md](docs/PROJECT_DOCUMENTATION.md).

## 📄 License

This project is licensed under the MIT License - see the LICENSE file for details.
