#!/bin/bash

# GCP Environment Setup Script for Loan Data Pipeline
echo "Setting up GCP environment for Loan Data Pipeline..."

# Set the service account credentials
export GOOGLE_APPLICATION_CREDENTIALS="$(pwd)/e2e-project-etl-cc9bf99bbb88.json"
echo "Service account credentials set to: $GOOGLE_APPLICATION_CREDENTIALS"

# Project ID from the service account
PROJECT_ID="e2e-project-etl"
echo "Using project ID: $PROJECT_ID"

# Create GCS buckets if they don't exist
echo "Creating GCS buckets..."
gsutil mb -p $PROJECT_ID -l us-central1 gs://loan-input-data-$PROJECT_ID || echo "Bucket already exists or error creating bucket"
gsutil mb -p $PROJECT_ID -l us-central1 gs://dataflow-temp-$PROJECT_ID || echo "Bucket already exists or error creating bucket"

# Create BigQuery dataset if it doesn't exist
echo "Creating BigQuery dataset..."
bq --location=US mk --dataset $PROJECT_ID:loan_data || echo "Dataset already exists or error creating dataset"

# Upload sample data to GCS
echo "Uploading sample data to GCS..."
gsutil cp loans.csv gs://loan-input-data-$PROJECT_ID/

echo "GCP environment setup complete!"
echo ""
echo "You can now run the pipeline with:"
echo "python3 loan_pipeline.py"
