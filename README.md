# End-to-End Event-Driven Data Analytics Pipeline on AWS

A serverless data engineering pipeline built on AWS that automates CSV ingestion, ETL processing, data cleaning, feature engineering, and analytics-ready dataset generation for dashboarding tools like Power BI.


---

# Project Highlights

- Built a fully automated event-driven ETL pipeline using AWS services
- Processed and transformed ~10,000 retail transaction records using PySpark
- Reduced noisy and inconsistent data through statistical preprocessing techniques
- Engineered new analytical features for downstream business insights
- Implemented real-time job tracking and monitoring
- Created analytics-ready datasets for BI dashboards and reporting

---

# Architecture

<p align="center">
  <img width="1036" height="477" alt="Architecture Diagram" src="https://github.com/user-attachments/assets/1be1b1e7-e076-45f7-9c53-be431b24c405" />
</p>

---

# End-to-End Pipeline Flow

```text
User Uploads CSV
        ↓
Frontend Upload Portal (S3 Hosted)
        ↓
AWS Lambda
    • Generates pre-signed upload URL
    • Starts Glue ETL job
    • Tracks processing status
        ↓
Amazon S3 (Raw Data Bucket)
        ↓
AWS Glue PySpark ETL Pipeline
    • Schema cleaning
    • Duplicate removal
    • Null handling
    • Data type conversion
    • Statistical outlier detection using IQR
    • Feature engineering
        ↓
Amazon S3 (Processed Data Bucket)
        ↓
Power BI Dashboard / Analytics Layer
```

---

# Tech Stack

| Technology | Usage |
|---|---|
| Amazon S3 | Data lake storage and frontend hosting |
| AWS Lambda | Serverless orchestration and API handling |
| AWS Glue | Distributed ETL processing using PySpark |
| PySpark | Large-scale data transformation |
| IAM | Secure role-based access control |
| CloudWatch | Logging and monitoring |
| HTML / CSS / JavaScript | Frontend upload interface |
| Power BI | Dashboarding and business analytics |

---

# ETL Processing Performed

## Data Cleaning
- Standardized column names
- Removed duplicate records
- Handled missing/null values
- Corrected inconsistent datatypes

## Statistical Processing
- Implemented IQR-based outlier detection and removal
- Reduced noisy transaction data for better analytical accuracy

## Feature Engineering
Generated business-focused analytical features such as:
- Delivery Days
- Profit Margin %
- Revenue Band
- Order Processing Metrics
- Sales Categorization Features

---

# Dataset Information

## Dataset Used
**Sample Superstore Retail Dataset**

| Metric | Value |
|---|---|
| Original Rows | 9,994 |
| Original Columns | 21 |
| Final Cleaned Rows | ~7,000 |
| New Features Added | 8 |

---

# Key Features

- Fully automated ETL workflow
- Event-driven architecture
- Serverless cloud-native deployment
- Real-time Glue job monitoring
- Scalable processing pipeline
- Analytics-ready cleaned dataset generation
- Production-style AWS workflow implementation

---

# Project Structure

```bash
.
├── index.html          # Frontend upload portal
├── index.mjs           # Lambda backend logic
├── glue_etl.py         # PySpark ETL pipeline
└── README.md
```

---

# AWS Services Used

## Amazon S3
Used for:
- Raw dataset ingestion
- Processed dataset storage
- Static website hosting

## AWS Lambda
Responsible for:
- Generating secure pre-signed URLs
- Triggering Glue jobs
- Monitoring ETL execution status

## AWS Glue
Handles:
- Distributed ETL processing
- Data transformation
- Statistical preprocessing
- Feature engineering

## CloudWatch
Provides:
- Pipeline monitoring
- Error logging
- Execution tracking

---

# Setup Guide

## 1. Create S3 Buckets

Create the following buckets:

- `csv-raw-data-cc`
- `csv-processed-data-cc`
- `csv-final-data-cc`

Enable:
- CORS for upload bucket
- Public read access for final dataset bucket

---

## 2. Configure AWS Lambda

### Runtime
- Node.js 22.x

### Required IAM Policies
- `AmazonS3FullAccess`
- `AWSGlueServiceRole`

### Additional Setup
- Enable Lambda Function URL
- Set authentication to `NONE`
- Update `GLUE_JOB_NAME` inside `index.mjs`

---

## 3. Configure AWS Glue ETL Job

### Runtime
- AWS Glue 5.1 (PySpark)

### Input Path

```text
s3://csv-raw-data-cc/uploads/
```

### Output Path

```text
s3://csv-final-data-cc/
```

Upload:
- `glue_etl.py` as the ETL script

---

## 4. Deploy Frontend

- Update `LAMBDA_URL` inside `index.html`
- Upload frontend files to S3
- Enable static website hosting

---

# Power BI Dashboard

The cleaned dataset is connected to Power BI for interactive business analytics and visualization.

<p align="center">
  <img width="1394" height="799" alt="Power BI Dashboard" src="https://github.com/user-attachments/assets/03ac50a8-c6fa-4a24-8126-3649b00898a0" />
</p>

---

# Live Demo

## Frontend Upload Portal

```text
http://csv-upload-portal.s3-website.ap-south-1.amazonaws.com
```

## Processed Dataset

```text
https://csv-final-data-cc.s3.ap-south-1.amazonaws.com/final_dashboard_cleaned.csv
```

---

# Business Value Delivered

- Automated manual data preprocessing workflows
- Improved analytical data quality using statistical methods
- Enabled faster BI reporting and visualization
- Demonstrated scalable cloud-native data engineering practices

---

# Future Improvements

- Athena integration for SQL-based querying
- AWS QuickSight dashboard integration
- Data validation layer before ETL
- CI/CD deployment pipeline
- Infrastructure as Code using Terraform
- Real-time streaming ingestion using Kafka/Kinesis





---


