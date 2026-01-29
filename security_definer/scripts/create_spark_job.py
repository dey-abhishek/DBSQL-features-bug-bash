"""
Create and run SQL SECURITY DEFINER tests via Jobs API using Spark Python Task
This approach uploads the test scripts directly without requiring notebooks
"""

from framework.jobs_api import DatabricksJobsAPI
from framework.serverless_config import WORKSPACE_URL, PAT_TOKEN, SERVERLESS_CLUSTER_ID
import json
import sys


def create_spark_python_job(test_suite="all"):
    """
    Create a job that runs Python tests using spark_python_task
    
    This uploads run_tests.py to DBFS and executes it
    """
    client = DatabricksJobsAPI(WORKSPACE_URL, PAT_TOKEN)
    
    job_name = f"SQL_DEFINER_Tests_{test_suite}"
    
    # Create job with spark_python_task
    payload = {
        "name": job_name,
        "tasks": [
            {
                "task_key": "run_sql_definer_tests",
                "existing_cluster_id": SERVERLESS_CLUSTER_ID,
                "spark_python_task": {
                    "python_file": "dbfs:/FileStore/sql_definer_tests/run_tests.py",
                    "parameters": [f"--suite={test_suite}"]
                },
                "libraries": [
                    {"pypi": {"package": "databricks-sql-connector"}},
                    {"pypi": {"package": "databricks-sdk"}}
                ],
                "timeout_seconds": 3600
            }
        ],
        "timeout_seconds": 3600,
        "max_concurrent_runs": 1
    }
    
    endpoint = f"{client.api_base}/jobs/create"
    response = client._make_request("POST", endpoint, json=payload)
    
    return response


def main():
    print("=" * 80)
    print("Creating Spark Python Job for SQL SECURITY DEFINER Tests")
    print("=" * 80)
    print()
    
    # Note: This requires uploading files to DBFS first
    print("⚠️  NOTE: This approach requires files to be uploaded to DBFS")
    print("   The notebook approach or direct SQL Warehouse testing is recommended")
    print()
    print("For now, please use:")
    print("  1. Local testing: PYTHONPATH=. python run_tests.py")
    print("  2. Upload run_tests_notebook.py to Databricks workspace as a notebook")
    print("  3. Create job pointing to that notebook")
    print()


if __name__ == "__main__":
    main()
