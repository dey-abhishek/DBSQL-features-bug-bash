#!/usr/bin/env python3
"""
Upload and run Impersonation Test Suite as a Databricks Job

This script tests SQL SECURITY DEFINER impersonation behavior:
- Identity context switching
- Permission elevation
- Cross-principal chains
- Impersonation boundaries
- Context isolation
"""

import os
import sys
import json
import requests
import base64
from datetime import datetime

# Configuration
WORKSPACE_URL = "https://e2-dogfood-unity-catalog-us-east-1.staging.cloud.databricks.com"
SERVERLESS_CLUSTER_ID = "0127-210051-t8p9ys5k"
NOTEBOOK_LOCAL_PATH = "../tests/notebooks/impersonation_test_notebook.py"
NOTEBOOK_WORKSPACE_PATH = "/Users/abhishek.dey@databricks.com/security_definer/impersonation_tests"

def get_token():
    token = os.getenv("DATABRICKS_PAT_TOKEN")
    if not token:
        print("❌ Error: DATABRICKS_PAT_TOKEN not set")
        sys.exit(1)
    return token

def upload_notebook(token):
    print(f"\n📤 Uploading impersonation test notebook...")
    
    script_dir = os.path.dirname(os.path.abspath(__file__))
    local_path = os.path.join(script_dir, NOTEBOOK_LOCAL_PATH)
    
    with open(local_path, 'r') as f:
        content = f.read()
    
    content_b64 = base64.b64encode(content.encode()).decode()
    
    url = f"{WORKSPACE_URL}/api/2.0/workspace/import"
    response = requests.post(url, 
        headers={"Authorization": f"Bearer {token}", "Content-Type": "application/json"},
        json={
            "path": NOTEBOOK_WORKSPACE_PATH,
            "content": content_b64,
            "language": "PYTHON",
            "overwrite": True,
            "format": "SOURCE"
        }
    )
    
    if response.status_code == 200:
        print(f"✅ Notebook uploaded")
        return True
    else:
        print(f"❌ Upload failed: {response.status_code} - {response.text}")
        return False

def create_job(token, run_as_sp=False):
    print(f"\n🔧 Creating Databricks Job...")
    
    timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
    run_as_text = "RunAsSP" if run_as_sp else "RunAsUser"
    job_name = f"Impersonation_Tests_{run_as_text}_{timestamp}"
    
    payload = {
        "name": job_name,
        "tasks": [{
            "task_key": "impersonation_tests",
            "description": "SQL SECURITY DEFINER Impersonation Test Suite",
            "notebook_task": {
                "notebook_path": NOTEBOOK_WORKSPACE_PATH,
                "source": "WORKSPACE"
            },
            "existing_cluster_id": SERVERLESS_CLUSTER_ID,
            "timeout_seconds": 3600,
            "max_retries": 0
        }],
        "timeout_seconds": 3600,
        "max_concurrent_runs": 1,
        "format": "MULTI_TASK"
    }
    
    if run_as_sp:
        payload["run_as"] = {"service_principal_name": "9c819e4d-1280-4ffa-85a0-e50b41222f52"}
        print(f"   🤖 Run as: Service Principal")
    else:
        print(f"   👤 Run as: Current User")
    
    url = f"{WORKSPACE_URL}/api/2.1/jobs/create"
    response = requests.post(url,
        headers={"Authorization": f"Bearer {token}", "Content-Type": "application/json"},
        json=payload
    )
    
    if response.status_code == 200:
        job_id = response.json().get("job_id")
        print(f"✅ Job created: {job_id}")
        return str(job_id)
    else:
        print(f"❌ Job creation failed: {response.status_code} - {response.text}")
        return None

def grant_sp_permission(token):
    """Grant Service Principal READ permission on notebook"""
    print(f"\n🔐 Granting SP permission on notebook...")
    
    sp_client_id = "9c819e4d-1280-4ffa-85a0-e50b41222f52"
    
    # Get object ID
    url_status = f"{WORKSPACE_URL}/api/2.0/workspace/get-status"
    response = requests.get(url_status,
        headers={"Authorization": f"Bearer {token}"},
        params={"path": NOTEBOOK_WORKSPACE_PATH}
    )
    
    if response.status_code == 200:
        obj_id = response.json().get("object_id")
        
        # Grant permission
        url_perm = f"{WORKSPACE_URL}/api/2.0/permissions/notebooks/{obj_id}"
        resp_perm = requests.patch(url_perm,
            headers={"Authorization": f"Bearer {token}", "Content-Type": "application/json"},
            json={
                "access_control_list": [{
                    "service_principal_name": sp_client_id,
                    "permission_level": "CAN_READ"
                }]
            }
        )
        
        if resp_perm.status_code == 200:
            print(f"✅ Permission granted")
        else:
            print(f"⚠️  Permission grant: {resp_perm.status_code}")

def run_job(token, job_id):
    print(f"\n🚀 Triggering job run...")
    
    url = f"{WORKSPACE_URL}/api/2.1/jobs/run-now"
    response = requests.post(url,
        headers={"Authorization": f"Bearer {token}", "Content-Type": "application/json"},
        json={"job_id": int(job_id)}
    )
    
    if response.status_code == 200:
        return response.json()
    else:
        print(f"❌ Run failed: {response.status_code} - {response.text}")
        return None

def main():
    import argparse
    parser = argparse.ArgumentParser(description="Run Impersonation Test Suite")
    parser.add_argument("--run-as-sp", action="store_true", help="Run job as Service Principal")
    args = parser.parse_args()
    
    print("="*80)
    print("🔐 SQL SECURITY DEFINER - IMPERSONATION TEST SUITE")
    print("="*80)
    
    token = get_token()
    
    if not upload_notebook(token):
        sys.exit(1)
    
    if args.run_as_sp:
        grant_sp_permission(token)
    
    job_id = create_job(token, args.run_as_sp)
    if not job_id:
        sys.exit(1)
    
    run_data = run_job(token, job_id)
    if not run_data:
        sys.exit(1)
    
    run_id = run_data.get("run_id")
    
    print("\n" + "="*80)
    print("✅ IMPERSONATION TEST SUITE RUNNING!")
    print("="*80)
    print(f"\n📊 Job Details:")
    print(f"   Job ID: {job_id}")
    print(f"   Run ID: {run_id}")
    print(f"   Run as: {'Service Principal' if args.run_as_sp else 'Current User'}")
    print(f"\n🔗 Monitor:")
    print(f"   {WORKSPACE_URL}/#job/{job_id}/run/{run_id}")
    print(f"\n🧪 8 Impersonation Tests:")
    print(f"   ✅ TC-IMP-01: Identity context verification")
    print(f"   ✅ TC-IMP-02: Permission elevation via impersonation")
    print(f"   ✅ TC-IMP-03: Cross-principal impersonation chain")
    print(f"   ✅ TC-IMP-04: Impersonation privilege boundaries")
    print(f"   ✅ TC-IMP-05: GRANT propagation via impersonation")
    print(f"   ✅ TC-IMP-06: Impersonation with parameterized SQL")
    print(f"   ✅ TC-IMP-07: Impersonation nesting depth limit")
    print(f"   ✅ TC-IMP-08: Impersonation context isolation")
    print(f"\n⏱️  Estimated time: ~2-3 minutes")
    print("="*80)
    
    return 0

if __name__ == "__main__":
    sys.exit(main())
