"""
Flask web application for Superfaktura ETL pipeline management and monitoring.
"""
import os
import logging
import subprocess
from datetime import datetime
import threading
import json

from flask import Flask, render_template, request, jsonify, redirect, url_for

import config
from utils import get_timestamp
import main

# Configure logging
logging.basicConfig(
    level=getattr(logging, config.LOG_LEVEL),
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger(__name__)

# Create Flask app
app = Flask(__name__)
app.secret_key = os.environ.get("SESSION_SECRET", "superfaktura-etl-secret")

# Store pipeline status globally
pipeline_status = {
    "running": False,
    "last_run": None,
    "status": "not_started",
    "log": []
}

def log_message(message, level="INFO"):
    """Add a message to the pipeline log"""
    timestamp = get_timestamp()
    log_entry = {"timestamp": timestamp, "message": message, "level": level}
    pipeline_status["log"].append(log_entry)
    
    if level == "INFO":
        logger.info(message)
    elif level == "ERROR":
        logger.error(message)
    elif level == "WARNING":
        logger.warning(message)

def run_pipeline_thread(mode, project_id, dataset_id, no_bigquery):
    """Run the ETL pipeline in a separate thread"""
    try:
        pipeline_status["running"] = True
        pipeline_status["status"] = "running"
        log_message(f"Starting pipeline in {mode} mode")
        
        # Update configuration based on parameters
        if mode == "full":
            config.EXTRACT_FULL = True
            config.EXTRACT_INCREMENTAL = False
        elif mode == "incremental":
            config.EXTRACT_FULL = False
            config.EXTRACT_INCREMENTAL = True
        else:  # both
            config.EXTRACT_FULL = True
            config.EXTRACT_INCREMENTAL = True
        
        config.PROJECT_ID = project_id
        config.DATASET_ID = dataset_id
        
        # Run the pipeline
        if no_bigquery:
            log_message("Running pipeline without BigQuery upload")
            run_date = config.RUN_DATE
            data_path = config.BASE_DATA_PATH
            
            if config.EXTRACT_FULL:
                log_message("Running full extraction")
                main.run_full_extraction(run_date, data_path)
            
            if config.EXTRACT_INCREMENTAL:
                log_message("Running incremental extraction")
                main.run_incremental_extraction(run_date, data_path)
            
            log_message("Running presentation layer processing")
            main.run_presentation_layer(data_path)
        else:
            log_message("Running full pipeline including BigQuery upload")
            main.run_full_pipeline()
        
        pipeline_status["status"] = "completed"
        log_message("Pipeline completed successfully")
    except Exception as e:
        pipeline_status["status"] = "failed"
        log_message(f"Pipeline failed: {str(e)}", "ERROR")
    finally:
        pipeline_status["running"] = False
        pipeline_status["last_run"] = get_timestamp()

@app.route('/')
def index():
    """Render the home page"""
    return render_template('index.html', 
                          status=pipeline_status,
                          config=config)

@app.route('/run', methods=['POST'])
def run_pipeline():
    """Start the ETL pipeline"""
    if pipeline_status["running"]:
        return jsonify({"error": "Pipeline is already running"}), 400
    
    # Get parameters from the form
    mode = request.form.get('mode', 'both')
    project_id = request.form.get('project_id', config.PROJECT_ID)
    dataset_id = request.form.get('dataset_id', config.DATASET_ID)
    no_bigquery = 'no_bigquery' in request.form
    
    # Clear the log
    pipeline_status["log"] = []
    
    # Start the pipeline in a separate thread
    thread = threading.Thread(
        target=run_pipeline_thread,
        args=(mode, project_id, dataset_id, no_bigquery)
    )
    thread.daemon = True
    thread.start()
    
    return redirect(url_for('index'))

@app.route('/status')
def get_status():
    """Get the current pipeline status"""
    return jsonify(pipeline_status)

@app.route('/data')
def view_data():
    """View data directory structure"""
    data_path = config.BASE_DATA_PATH
    
    try:
        result = {"directories": [], "files": []}
        
        # List top-level directories
        if os.path.exists(data_path):
            for item in os.listdir(data_path):
                full_path = os.path.join(data_path, item)
                if os.path.isdir(full_path):
                    result["directories"].append({
                        "name": item,
                        "path": full_path,
                        "subdirs": [subdir for subdir in os.listdir(full_path) 
                                    if os.path.isdir(os.path.join(full_path, subdir))][:10]  # Limit to 10 subdirs
                    })
                elif os.path.isfile(full_path):
                    result["files"].append({
                        "name": item,
                        "path": full_path,
                        "size": os.path.getsize(full_path)
                    })
        
        return render_template('data.html', data=result, base_path=data_path)
    except Exception as e:
        return render_template('error.html', error=str(e))

if __name__ == '__main__':
    app.run(host='0.0.0.0', port=5000, debug=True)