#!/usr/bin/env python3
"""
Upload notebook to Databricks workspace and create a job to run it
"""

import requests
import json
import base64
import sys
import os

# Configuration - Use environment variables
WORKSPACE_URL = os.getenv("DATABRICKS_WORKSPACE_URL", "https://e2-dogfood-unity-catalog-us-east-1.staging.cloud.databricks.com")
PAT_TOKEN = os.getenv("DATABRICKS_PAT_TOKEN")
NOTEBOOK_PATH = "/Users/abhishek.dey@databricks.com/serverless_test_notebook"
CLUSTER_ID = os.getenv("DATABRICKS_SERVERLESS_CLUSTER_ID", "0127-210051-t8p9ys5k")
LOCAL_NOTEBOOK = "tests/notebooks/serverless_test_notebook.py"

# Validate required environment variables
if not PAT_TOKEN:
    print("=" * 80, file=sys.stderr)
    print("❌ ERROR: DATABRICKS_PAT_TOKEN environment variable not set", file=sys.stderr)
    print("=" * 80, file=sys.stderr)
    print("Set it with:", file=sys.stderr)
    print("  export DATABRICKS_PAT_TOKEN='your_token_here'", file=sys.stderr)
    print("=" * 80, file=sys.stderr)
    sys.exit(1)

def upload_notebook():
    """Upload notebook to workspace"""
    print("="*80)
    print("🚀 Uploading Notebook to Databricks Workspace")
    print("="*80)
    print()
    
    # Read notebook content
    with open(LOCAL_NOTEBOOK, 'r') as f:
        content = f.read()
    
    # Encode as base64
    content_b64 = base64.b64encode(content.encode('utf-8')).decode('utf-8')
    
    # Upload via API
    url = f"{WORKSPACE_URL}/api/2.0/workspace/import"
    headers = {
        "Authorization": f"Bearer {PAT_TOKEN}",
        "Content-Type": "application/json"
    }
    
    payload = {
        "path": NOTEBOOK_PATH,
        "format": "SOURCE",
        "language": "PYTHON",
        "content": content_b64,
        "overwrite": True
    }
    
    print(f"📤 Uploading to: {NOTEBOOK_PATH}")
    response = requests.post(url, headers=headers, json=payload)
    
    if response.status_code == 200:
        print(f"✅ Notebook uploaded successfully!")
        return True
    else:
        print(f"❌ Upload failed: {response.status_code}")
        print(f"   Response: {response.text}")
        return False

def create_job():
    """Create a Databricks job to run the notebook"""
    print()
    print("="*80)
    print("📝 Creating Databricks Job")
    print("="*80)
    print()
    
    url = f"{WORKSPACE_URL}/api/2.1/jobs/create"
    headers = {
        "Authorization": f"Bearer {PAT_TOKEN}",
        "Content-Type": "application/json"
    }
    
    from datetime import datetime
    job_name = f"SQL_DEFINER_Serverless_Tests_{datetime.now().strftime('%Y%m%d_%H%M%S')}"
    
    payload = {
        "name": job_name,
        "tasks": [
            {
                "task_key": "run_definer_tests",
                "notebook_task": {
                    "notebook_path": NOTEBOOK_PATH,
                    "base_parameters": {}
                },
                "existing_cluster_id": CLUSTER_ID,
                "timeout_seconds": 3600
            }
        ],
        "timeout_seconds": 3600,
        "max_concurrent_runs": 1
    }
    
    print(f"📋 Job name: {job_name}")
    print(f"📓 Notebook: {NOTEBOOK_PATH}")
    print(f"🖥️  Cluster: {CLUSTER_ID}")
    print()
    
    response = requests.post(url, headers=headers, json=payload)
    
    if response.status_code == 200:
        job_id = response.json()['job_id']
        print(f"✅ Job created successfully!")
        print(f"   Job ID: {job_id}")
        print()
        print(f"🌐 View job: {WORKSPACE_URL}/#job/{job_id}")
        return job_id
    else:
        print(f"❌ Job creation failed: {response.status_code}")
        print(f"   Response: {response.text}")
        return None

def run_job(job_id):
    """Trigger a job run"""
    print()
    print("="*80)
    print("▶️  Triggering Job Run")
    print("="*80)
    print()
    
    url = f"{WORKSPACE_URL}/api/2.1/jobs/run-now"
    headers = {
        "Authorization": f"Bearer {PAT_TOKEN}",
        "Content-Type": "application/json"
    }
    
    payload = {"job_id": job_id}
    
    response = requests.post(url, headers=headers, json=payload)
    
    if response.status_code == 200:
        run_id = response.json()['run_id']
        print(f"✅ Job run started!")
        print(f"   Run ID: {run_id}")
        print()
        print(f"🌐 Monitor run: {WORKSPACE_URL}/#job/{job_id}/run/{run_id}")
        return run_id
    else:
        print(f"❌ Job run failed: {response.status_code}")
        print(f"   Response: {response.text}")
        return None

def main():
    print()
    print("="*80)
    print("🚀 SQL SECURITY DEFINER - Serverless Test Setup")
    print("="*80)
    print()
    
    # Step 1: Upload notebook
    if not upload_notebook():
        print("\n❌ Setup failed at upload step")
        return 1
    
    # Step 2: Create job
    job_id = create_job()
    if not job_id:
        print("\n⚠️  Notebook uploaded but job creation failed")
        print("   You can still run the notebook manually in the workspace")
        return 0
    
    # Step 3: Prompt to run
    print()
    print("="*80)
    print("✅ Setup Complete!")
    print("="*80)
    print()
    print("📋 Next steps:")
    print()
    print("Option 1 - Run via Jobs API (Automated):")
    print(f"   Run ID will be displayed above")
    print()
    print("Option 2 - Run in Workspace UI (Interactive):")
    print(f"   1. Open: {WORKSPACE_URL}")
    print(f"   2. Navigate to: Workspace → Users → abhishek.dey@databricks.com")
    print(f"   3. Open: serverless_test_notebook")
    print(f"   4. Click 'Run All'")
    print()
    
    # Ask if user wants to run now
    response = input("🚀 Run the job now? (y/n): ").strip().lower()
    if response == 'y':
        run_id = run_job(job_id)
        if run_id:
            print()
            print("✅ Job is running! Check the workspace UI for results.")
        else:
            print()
            print("⚠️  Could not start job run. Try manually from the UI.")
    else:
        print()
        print(f"✅ Job created but not run. Start it manually:")
        print(f"   {WORKSPACE_URL}/#job/{job_id}")
    
    return 0

if __name__ == "__main__":
    try:
        sys.exit(main())
    except KeyboardInterrupt:
        print("\n\n⚠️  Setup interrupted by user")
        sys.exit(1)
    except Exception as e:
        print(f"\n\n❌ Unexpected error: {e}")
        import traceback
        traceback.print_exc()
        sys.exit(1)
