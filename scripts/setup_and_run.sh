#!/bin/bash

# Setup and run the loan data processing pipeline
echo "Setting up loan data processing pipeline..."

# Set the service account credentials
export GOOGLE_APPLICATION_CREDENTIALS="$(pwd)/e2e-project-etl-cc9bf99bbb88.json"
echo "Service account credentials set."

# Install dependencies
echo "Installing dependencies..."
pip3 install -r requirements.txt

# Create GCS buckets if they don't exist
echo "Creating GCS buckets..."
gsutil mb -p e2e-project-etl -l us-central1 gs://loan-input-data || echo "Bucket already exists or error creating bucket"
gsutil mb -p e2e-project-etl -l us-central1 gs://dataflow-temp || echo "Bucket already exists or error creating bucket"

# Create BigQuery dataset if it doesn't exist
echo "Creating BigQuery dataset..."
bq --location=US mk --dataset e2e-project-etl:loan_data || echo "Dataset already exists or error creating dataset"

# Upload sample data to GCS
echo "Uploading sample data to GCS..."
gsutil cp loans.csv gs://loan-input-data/

# Run local test first
echo "Running local test pipeline..."
python3 run_local.py

# Display the results
echo ""
echo "Local test results:"
cat local_results.json-00000-of-00001

# Ask if user wants to run on Dataflow
echo ""
echo "Do you want to run the pipeline on Dataflow? (y/n)"
read -r answer

if [ "$answer" = "y" ] || [ "$answer" = "Y" ]; then
    echo "Running pipeline on Dataflow..."
    python3 loan_pipeline.py
    echo "Pipeline job submitted to Dataflow. Check the GCP Console for job status."
else
    echo "Skipping Dataflow execution. You can run it later with: python3 loan_pipeline.py"
fi

echo "Setup complete!"
