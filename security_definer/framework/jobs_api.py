"""
Databricks Jobs API Integration
Module for creating and running tests via Databricks Jobs API
"""

import requests
import json
import time
from typing import Dict, List, Optional, Any
from datetime import datetime


class DatabricksJobsAPI:
    """
    Interface to Databricks Jobs API for automated test execution
    """
    
    def __init__(self, workspace_url: str, token: str):
        """
        Initialize Jobs API client
        
        Parameters:
        -----------
        workspace_url : str
            Databricks workspace URL (e.g., https://xxx.cloud.databricks.com)
        token : str
            Personal Access Token or Service Principal token
        """
        self.workspace_url = workspace_url.rstrip('/')
        self.token = token
        self.api_base = f"{self.workspace_url}/api/2.1"
        self.headers = {
            "Authorization": f"Bearer {token}",
            "Content-Type": "application/json"
        }
    
    def create_job(self, 
                   job_name: str,
                   notebook_path: str,
                   cluster_id: str,
                   timeout_seconds: int = 3600,
                   max_retries: int = 1,
                   parameters: Optional[Dict[str, str]] = None) -> Dict[str, Any]:
        """
        Create a Databricks job for running tests
        
        Parameters:
        -----------
        job_name : str
            Name of the job
        notebook_path : str
            Path to notebook in workspace
        cluster_id : str
            Existing cluster ID to use
        timeout_seconds : int
            Job timeout in seconds
        max_retries : int
            Number of retries on failure
        parameters : dict
            Notebook parameters
            
        Returns:
        --------
        dict : Job creation response with job_id
        """
        endpoint = f"{self.api_base}/jobs/create"
        
        payload = {
            "name": job_name,
            "tasks": [
                {
                    "task_key": "run_tests",
                    "existing_cluster_id": cluster_id,
                    "notebook_task": {
                        "notebook_path": notebook_path,
                        "base_parameters": parameters or {}
                    },
                    "timeout_seconds": timeout_seconds,
                    "max_retries": max_retries
                }
            ],
            "timeout_seconds": timeout_seconds,
            "max_concurrent_runs": 1
        }
        
        response = requests.post(endpoint, headers=self.headers, json=payload)
        response.raise_for_status()
        
        return response.json()
    
    def create_python_task_job(self,
                                job_name: str,
                                python_file: str,
                                cluster_id: str,
                                timeout_seconds: int = 3600,
                                parameters: Optional[List[str]] = None) -> Dict[str, Any]:
        """
        Create a job that runs a Python script
        
        Parameters:
        -----------
        job_name : str
            Name of the job
        python_file : str
            Path to Python file in workspace or DBFS
        cluster_id : str
            Existing cluster ID
        timeout_seconds : int
            Timeout in seconds
        parameters : list
            Command-line parameters for Python script
            
        Returns:
        --------
        dict : Job creation response
        """
        endpoint = f"{self.api_base}/jobs/create"
        
        payload = {
            "name": job_name,
            "tasks": [
                {
                    "task_key": "run_python_tests",
                    "existing_cluster_id": cluster_id,
                    "python_wheel_task": {
                        "entry_point": python_file,
                        "parameters": parameters or []
                    },
                    "timeout_seconds": timeout_seconds
                }
            ]
        }
        
        response = requests.post(endpoint, headers=self.headers, json=payload)
        response.raise_for_status()
        
        return response.json()
    
    def run_now(self, job_id: int, notebook_params: Optional[Dict[str, str]] = None) -> Dict[str, Any]:
        """
        Trigger a job run
        
        Parameters:
        -----------
        job_id : int
            Job ID to run
        notebook_params : dict
            Parameters to pass to notebook
            
        Returns:
        --------
        dict : Run response with run_id
        """
        endpoint = f"{self.api_base}/jobs/run-now"
        
        payload = {
            "job_id": job_id
        }
        
        if notebook_params:
            payload["notebook_params"] = notebook_params
        
        response = requests.post(endpoint, headers=self.headers, json=payload)
        response.raise_for_status()
        
        return response.json()
    
    def get_run_status(self, run_id: int) -> Dict[str, Any]:
        """
        Get status of a job run
        
        Parameters:
        -----------
        run_id : int
            Run ID to check
            
        Returns:
        --------
        dict : Run status information
        """
        endpoint = f"{self.api_base}/jobs/runs/get"
        params = {"run_id": run_id}
        
        response = requests.get(endpoint, headers=self.headers, params=params)
        response.raise_for_status()
        
        return response.json()
    
    def wait_for_run_completion(self, 
                                 run_id: int,
                                 poll_interval: int = 10,
                                 timeout: int = 3600) -> Dict[str, Any]:
        """
        Wait for a job run to complete
        
        Parameters:
        -----------
        run_id : int
            Run ID to monitor
        poll_interval : int
            Seconds between status checks
        timeout : int
            Maximum seconds to wait
            
        Returns:
        --------
        dict : Final run status
        """
        start_time = time.time()
        
        while True:
            if time.time() - start_time > timeout:
                raise TimeoutError(f"Job run {run_id} exceeded timeout of {timeout}s")
            
            status = self.get_run_status(run_id)
            state = status.get("state", {})
            life_cycle_state = state.get("life_cycle_state")
            
            print(f"Run {run_id} status: {life_cycle_state}")
            
            if life_cycle_state in ["TERMINATED", "SKIPPED", "INTERNAL_ERROR"]:
                result_state = state.get("result_state")
                print(f"Run completed with result: {result_state}")
                return status
            
            time.sleep(poll_interval)
    
    def get_run_output(self, run_id: int) -> Dict[str, Any]:
        """
        Get output from a completed job run
        
        Parameters:
        -----------
        run_id : int
            Completed run ID
            
        Returns:
        --------
        dict : Run output
        """
        endpoint = f"{self.api_base}/jobs/runs/get-output"
        params = {"run_id": run_id}
        
        response = requests.get(endpoint, headers=self.headers, params=params)
        response.raise_for_status()
        
        return response.json()
    
    def delete_job(self, job_id: int) -> None:
        """
        Delete a job
        
        Parameters:
        -----------
        job_id : int
            Job ID to delete
        """
        endpoint = f"{self.api_base}/jobs/delete"
        payload = {"job_id": job_id}
        
        response = requests.post(endpoint, headers=self.headers, json=payload)
        response.raise_for_status()
    
    def list_jobs(self, limit: int = 25) -> Dict[str, Any]:
        """
        List all jobs
        
        Parameters:
        -----------
        limit : int
            Maximum number of jobs to return
            
        Returns:
        --------
        dict : List of jobs
        """
        endpoint = f"{self.api_base}/jobs/list"
        params = {"limit": limit}
        
        response = requests.get(endpoint, headers=self.headers, params=params)
        response.raise_for_status()
        
        return response.json()


def create_test_job(workspace_url: str,
                   token: str,
                   cluster_id: str,
                   test_suite: str = "all",
                   job_name: Optional[str] = None) -> Dict[str, Any]:
    """
    Helper function to create a test job
    
    Parameters:
    -----------
    workspace_url : str
        Databricks workspace URL
    token : str
        Authentication token
    cluster_id : str
        Serverless cluster ID
    test_suite : str
        Which test suite to run (all, core, advanced, etc.)
    job_name : str
        Custom job name
        
    Returns:
    --------
    dict : Job creation response
    """
    client = DatabricksJobsAPI(workspace_url, token)
    
    if not job_name:
        job_name = f"SQL_SECURITY_DEFINER_Tests_{test_suite}_{datetime.now().strftime('%Y%m%d_%H%M%S')}"
    
    # For now, we'll create a notebook-based job
    # Later we can switch to Python wheel tasks
    job_response = client.create_job(
        job_name=job_name,
        notebook_path="/Workspace/Users/abhishek.dey@databricks.com/DBSQL-features-bug-bash/run_tests",
        cluster_id=cluster_id,
        timeout_seconds=3600,
        parameters={"test_suite": test_suite}
    )
    
    return job_response


def run_tests_via_jobs_api(workspace_url: str,
                           token: str,
                           cluster_id: str,
                           test_suite: str = "all",
                           wait_for_completion: bool = True) -> Dict[str, Any]:
    """
    End-to-end function to run tests via Jobs API
    
    Parameters:
    -----------
    workspace_url : str
        Databricks workspace URL
    token : str
        Authentication token
    cluster_id : str
        Serverless cluster ID
    test_suite : str
        Test suite to run
    wait_for_completion : bool
        Whether to wait for job to complete
        
    Returns:
    --------
    dict : Job run results
    """
    print("=" * 80)
    print("🚀 Running Tests via Databricks Jobs API")
    print("=" * 80)
    print()
    
    client = DatabricksJobsAPI(workspace_url, token)
    
    # Create job
    print(f"📦 Creating job for test suite: {test_suite}")
    job_response = create_test_job(workspace_url, token, cluster_id, test_suite)
    job_id = job_response.get("job_id")
    print(f"✅ Job created: ID = {job_id}")
    print()
    
    # Run job
    print(f"▶️  Triggering job run...")
    run_response = client.run_now(job_id)
    run_id = run_response.get("run_id")
    print(f"✅ Job run started: Run ID = {run_id}")
    print(f"   View at: {workspace_url}/#job/{job_id}/run/{run_id}")
    print()
    
    if wait_for_completion:
        print("⏳ Waiting for job completion...")
        print()
        
        try:
            final_status = client.wait_for_run_completion(run_id, poll_interval=15)
            
            result_state = final_status.get("state", {}).get("result_state")
            
            if result_state == "SUCCESS":
                print("✅ Job completed successfully!")
            else:
                print(f"❌ Job failed with state: {result_state}")
            
            print()
            print("📊 Getting job output...")
            output = client.get_run_output(run_id)
            
            return {
                "job_id": job_id,
                "run_id": run_id,
                "status": final_status,
                "output": output,
                "result_state": result_state
            }
            
        except Exception as e:
            print(f"❌ Error during job execution: {e}")
            return {
                "job_id": job_id,
                "run_id": run_id,
                "error": str(e)
            }
    else:
        return {
            "job_id": job_id,
            "run_id": run_id,
            "message": "Job started, not waiting for completion"
        }


if __name__ == "__main__":
    # Example usage
    from framework.serverless_config import (
        WORKSPACE_URL, PAT_TOKEN, SERVERLESS_CLUSTER_ID
    )
    
    print("Testing Jobs API Integration...")
    print()
    
    # Test connection
    client = DatabricksJobsAPI(WORKSPACE_URL, PAT_TOKEN)
    
    print("📋 Listing existing jobs...")
    try:
        jobs = client.list_jobs(limit=10)
        job_count = len(jobs.get("jobs", []))
        print(f"✅ Found {job_count} existing jobs")
        print()
    except Exception as e:
        print(f"❌ Error: {e}")
        print()
    
    print("To run tests via Jobs API:")
    print(f"  python framework/jobs_api.py")
    print()
