# SuperFaktura ETL Pipeline

This repository contains an ETL (Extract, Transform, Load) pipeline for processing data from SuperFaktura and uploading it to Google BigQuery. It is designed for periodic data synchronization and analysis.

## 🔧 Features

- Extraction of invoice, client, and expense data from SuperFaktura API.
- Transformation and preprocessing of data in the landing zone.
- Incremental and full data loading support.
- Uploading processed data to BigQuery.
- Configurable and modular structure for easy maintenance and scalability.

## 🗂 Project Structure

```
etl/
├── keys/
│   └── serviceAccount/
│       └── serviceAccount.json       # Google Cloud service account credentials
├── superfaktura/
│   ├── app.py                        # Main app initialization
│   ├── config.py                     # Configuration settings (API keys, project IDs, etc.)
│   ├── key.json                      # SuperFaktura API credentials
│   ├── main.py                       # Entry point of the ETL process
│   ├── utils.py                      # Helper functions
│   └── etl/
│       ├── bigquery_uploader.py      # Uploading transformed data to BigQuery
│       ├── landing_preprocess_all.py# Full load preprocessing
│       ├── landing_preprocess_inc.py# Incremental load preprocessing
│       ├── present.py                # Presentation layer logic
```

## 🚀 Getting Started

### Prerequisites

- Python 3.9+
- Google Cloud project with BigQuery enabled
- SuperFaktura account with API access

### Installation

1. Clone this repository:

```bash
git clone https://github.com/yourusername/superfaktura-etl.git
cd superfaktura-etl
```

2. Create a virtual environment and activate it:

```bash
python -m venv venv
source venv/bin/activate
```

3. Install dependencies:

```bash
pip install -r requirements.txt
```

4. Set up configuration:

- Add your SuperFaktura API credentials to `key.json`.
- Add your GCP service account JSON to `keys/serviceAccount/serviceAccount.json`.

### Running the Pipeline

Simply run:

```bash
python superfaktura/main.py
```

This will trigger the full ETL process including data extraction, transformation, and upload to BigQuery.

## 🧠 Notes

- Make sure your GCP service account has permission to access and write to BigQuery datasets.
- This pipeline can be scheduled via cron jobs, Airflow, or any orchestration tool of choice.
