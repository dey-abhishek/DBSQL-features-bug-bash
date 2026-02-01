#!/usr/bin/env python3
"""
Upload and run SP Bidirectional Context Switching tests as a Databricks Job

This script:
1. Uploads the sp_bidirectional_test_notebook.py to Databricks workspace
2. Creates a new Databricks Job (or uses existing)
3. Triggers the job to run on Serverless General Compute
4. Displays the run URL for monitoring

Usage:
    export DATABRICKS_PAT_TOKEN="your_pat_token"
    python3 run_sp_bidirectional_job.py
"""

import os
import sys
import json
import requests
from datetime import datetime
from typing import Dict, Any, Optional

# Configuration
WORKSPACE_URL = "https://e2-dogfood-unity-catalog-us-east-1.staging.cloud.databricks.com"
SERVERLESS_CLUSTER_ID = "0127-210051-t8p9ys5k"
NOTEBOOK_LOCAL_PATH = "../tests/notebooks/sp_bidirectional_test_notebook.py"
NOTEBOOK_WORKSPACE_PATH = "/Users/abhishek.dey@databricks.com/security_definer/sp_bidirectional_tests"

def get_pat_token() -> str:
    """Get PAT token from environment"""
    token = os.getenv("DATABRICKS_PAT_TOKEN")
    if not token:
        print("❌ Error: DATABRICKS_PAT_TOKEN environment variable not set")
        print("   Export it with: export DATABRICKS_PAT_TOKEN='your_token'")
        sys.exit(1)
    return token

def get_sp_credentials() -> tuple:
    """Get Service Principal credentials from environment"""
    client_id = os.getenv("DATABRICKS_SP_CLIENT_ID")
    client_secret = os.getenv("DATABRICKS_SP_CLIENT_SECRET")
    
    if not client_id or not client_secret:
        print("❌ Error: Service Principal credentials not found in environment")
        print("   Please set DATABRICKS_SP_CLIENT_ID and DATABRICKS_SP_CLIENT_SECRET")
        sys.exit(1)
    
    return client_id, client_secret

def upload_notebook(token: str) -> bool:
    """Upload notebook to Databricks workspace"""
    print(f"\n📤 Uploading notebook to workspace...")
    print(f"   Local: {NOTEBOOK_LOCAL_PATH}")
    print(f"   Remote: {NOTEBOOK_WORKSPACE_PATH}")
    
    script_dir = os.path.dirname(os.path.abspath(__file__))
    local_path = os.path.join(script_dir, NOTEBOOK_LOCAL_PATH)
    
    if not os.path.exists(local_path):
        print(f"❌ Error: Local notebook not found at {local_path}")
        return False
    
    with open(local_path, 'r') as f:
        content = f.read()
    
    import base64
    content_b64 = base64.b64encode(content.encode()).decode()
    
    url = f"{WORKSPACE_URL}/api/2.0/workspace/import"
    headers = {
        "Authorization": f"Bearer {token}",
        "Content-Type": "application/json"
    }
    payload = {
        "path": NOTEBOOK_WORKSPACE_PATH,
        "content": content_b64,
        "language": "PYTHON",
        "overwrite": True,
        "format": "SOURCE"
    }
    
    response = requests.post(url, headers=headers, json=payload)
    
    if response.status_code == 200:
        print(f"✅ Notebook uploaded successfully")
        return True
    else:
        print(f"❌ Upload failed: {response.status_code}")
        print(f"   {response.text}")
        return False

def create_job(token: str, sp_client_id: str, sp_client_secret: str) -> Optional[str]:
    """Create a new Databricks Job for SP bidirectional tests - RUN AS SERVICE PRINCIPAL"""
    print(f"\n🔧 Creating Databricks Job...")
    print(f"   🤖 Run as: Service Principal ({sp_client_id[:8]}...)")
    
    timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
    job_name = f"SP_Bidirectional_Tests_RunAsSP_{timestamp}"
    
    url = f"{WORKSPACE_URL}/api/2.1/jobs/create"
    headers = {
        "Authorization": f"Bearer {token}",
        "Content-Type": "application/json"
    }
    
    payload = {
        "name": job_name,
        "tasks": [
            {
                "task_key": "sp_bidirectional_tests",
                "description": "Service Principal bidirectional context switching tests (Run as SP)",
                "notebook_task": {
                    "notebook_path": NOTEBOOK_WORKSPACE_PATH,
                    "source": "WORKSPACE"
                },
                "existing_cluster_id": SERVERLESS_CLUSTER_ID,
                "timeout_seconds": 3600,
                "max_retries": 0
            }
        ],
        "timeout_seconds": 3600,
        "max_concurrent_runs": 1,
        "format": "MULTI_TASK",
        # Configure job to run as Service Principal
        "run_as": {
            "service_principal_name": sp_client_id
        }
    }
    
    response = requests.post(url, headers=headers, json=payload)
    
    if response.status_code == 200:
        job_id = response.json().get("job_id")
        print(f"✅ Job created successfully")
        print(f"   Job ID: {job_id}")
        print(f"   Job Name: {job_name}")
        print(f"   🤖 Run As: Service Principal")
        return str(job_id)
    else:
        print(f"❌ Job creation failed: {response.status_code}")
        print(f"   {response.text}")
        return None

def run_job(token: str, job_id: str) -> Optional[Dict[str, Any]]:
    """Trigger the job to run"""
    print(f"\n🚀 Triggering job run...")
    
    url = f"{WORKSPACE_URL}/api/2.1/jobs/run-now"
    headers = {
        "Authorization": f"Bearer {token}",
        "Content-Type": "application/json"
    }
    payload = {
        "job_id": int(job_id)
    }
    
    response = requests.post(url, headers=headers, json=payload)
    
    if response.status_code == 200:
        run_data = response.json()
        run_id = run_data.get("run_id")
        print(f"✅ Job run started successfully")
        print(f"   Run ID: {run_id}")
        return run_data
    else:
        print(f"❌ Job run failed: {response.status_code}")
        print(f"   {response.text}")
        return None

def main():
    """Main execution"""
    print("="*80)
    print("🚀 SP BIDIRECTIONAL CONTEXT SWITCHING - RUN AS SERVICE PRINCIPAL")
    print("="*80)
    
    # Get PAT token for API calls
    token = get_pat_token()
    
    # Get SP credentials
    sp_client_id, sp_client_secret = get_sp_credentials()
    print(f"\n🤖 Service Principal: {sp_client_id[:8]}...")
    
    # Upload notebook
    if not upload_notebook(token):
        sys.exit(1)
    
    # Create job (configured to run as SP)
    job_id = create_job(token, sp_client_id, sp_client_secret)
    if not job_id:
        sys.exit(1)
    
    # Run job
    run_data = run_job(token, job_id)
    if not run_data:
        sys.exit(1)
    
    # Display results
    run_id = run_data.get("run_id")
    print("\n" + "="*80)
    print("✅ JOB IS NOW RUNNING AS SERVICE PRINCIPAL!")
    print("="*80)
    print(f"\n📊 Run Details:")
    print(f"   Job ID: {job_id}")
    print(f"   Run ID: {run_id}")
    print(f"   Run Number: {run_data.get('number_in_job', 'N/A')}")
    print(f"   🤖 Run As: Service Principal ({sp_client_id[:8]}...)")
    print(f"\n🔗 Monitor in Jobs UI:")
    print(f"   {WORKSPACE_URL}/#job/{job_id}/run/{run_id}")
    print(f"\n📋 Test Coverage:")
    print(f"   ✅ TC-SP-01: User creates DEFINER procedure, SP executes")
    print(f"   ✅ TC-SP-02: SP creates DEFINER procedure and executes")
    print(f"   ✅ TC-SP-03: Nested DEFINER procedures")
    print(f"   ✅ TC-SP-04: Parameterized DEFINER procedure")
    print(f"   ✅ TC-SP-05: DEFINER controlled access")
    print(f"   ✅ TC-SP-06: Chained DEFINER procedures")
    print(f"   ✅ TC-SP-07: Parameterized filtering")
    print(f"   ✅ TC-SP-08: DEFINER privilege validation")
    print(f"   ─────────────────────────────────────")
    print(f"   📊 Total: 8 tests (all run as SP)")
    print(f"\n⏱️  Estimated Time: ~2-3 minutes")
    print(f"\n🔍 What's Different:")
    print(f"   • Job runs with SP identity (not user)")
    print(f"   • CURRENT_USER() will show SP client ID")
    print(f"   • Tests SP's ability to create/execute procedures")
    print(f"   • Validates SP permissions and grants")
    print("\n" + "="*80)
    print("✅ Track progress in the Databricks Jobs UI!")
    print("="*80)
    
    return 0

if __name__ == "__main__":
    sys.exit(main())
