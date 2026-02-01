#!/usr/bin/env python3
"""
Upload and run the complete 78-test DEFINER impersonation suite on Databricks
"""

import os
import sys
import json
import time
import requests

# Configuration from environment
SERVER_HOSTNAME = os.getenv("DATABRICKS_SERVER_HOSTNAME", "e2-dogfood-unity-catalog-us-east-1.staging.cloud.databricks.com")
PAT_TOKEN = os.getenv("DATABRICKS_PAT_TOKEN")
CLUSTER_ID = os.getenv("SERVERLESS_CLUSTER_ID", "0127-210051-t8p9ys5k")
SP_CLIENT_ID = os.getenv("SERVICE_PRINCIPAL_CLIENT_ID", "9c819e4d-1280-4ffa-85a0-e50b41222f52")
USER_EMAIL = os.getenv("USER_A", "abhishek.dey@databricks.com")

if not PAT_TOKEN:
    print("❌ ERROR: DATABRICKS_PAT_TOKEN environment variable not set")
    print("   Please set it before running this script")
    sys.exit(1)

BASE_URL = f"https://{SERVER_HOSTNAME}"
HEADERS = {
    "Authorization": f"Bearer {PAT_TOKEN}",
    "Content-Type": "application/json"
}

NOTEBOOK_PATH = f"/Users/{USER_EMAIL}/security_definer/complete_definer_tests"
LOCAL_NOTEBOOK = "security_definer/tests/notebooks/complete_definer_tests.py"


def create_folder():
    """Create workspace folder if it doesn't exist"""
    folder_path = f"/Users/{USER_EMAIL}/security_definer"
    url = f"{BASE_URL}/api/2.0/workspace/mkdirs"
    
    try:
        response = requests.post(url, headers=HEADERS, json={"path": folder_path})
        if response.status_code in [200, 409]:  # 409 = already exists
            print(f"✅ Workspace folder ready: {folder_path}")
            return True
        else:
            print(f"⚠️  Warning: Folder creation returned {response.status_code}")
            return True  # Continue anyway
    except Exception as e:
        print(f"⚠️  Warning: Could not create folder: {e}")
        return True  # Continue anyway


def upload_notebook():
    """Upload the notebook to Databricks workspace"""
    print(f"\n📤 Uploading notebook: {LOCAL_NOTEBOOK} → {NOTEBOOK_PATH}")
    
    if not os.path.exists(LOCAL_NOTEBOOK):
        print(f"❌ ERROR: Local notebook not found: {LOCAL_NOTEBOOK}")
        return False
    
    with open(LOCAL_NOTEBOOK, 'r') as f:
        content = f.read()
    
    import base64
    encoded_content = base64.b64encode(content.encode()).decode()
    
    url = f"{BASE_URL}/api/2.0/workspace/import"
    payload = {
        "path": NOTEBOOK_PATH,
        "format": "SOURCE",
        "language": "PYTHON",
        "content": encoded_content,
        "overwrite": True
    }
    
    try:
        response = requests.post(url, headers=HEADERS, json=payload)
        if response.status_code == 200:
            print(f"✅ Upload successful!")
            return True
        else:
            print(f"❌ Upload failed: {response.status_code}")
            print(f"   Response: {response.text}")
            return False
    except Exception as e:
        print(f"❌ Upload error: {e}")
        return False


def grant_sp_permission():
    """Grant CAN_READ permission to Service Principal on the notebook"""
    print(f"\n🔑 Granting CAN_READ permission to SP: {SP_CLIENT_ID}")
    
    # First, get the notebook object ID
    try:
        status_url = f"{BASE_URL}/api/2.0/workspace/get-status"
        status_response = requests.get(
            status_url,
            headers=HEADERS,
            params={"path": NOTEBOOK_PATH}
        )
        
        if status_response.status_code != 200:
            print(f"⚠️  Could not get notebook ID: {status_response.text}")
            return True  # Continue anyway
        
        object_id = status_response.json().get("object_id")
        if not object_id:
            print(f"⚠️  Could not find object_id in response")
            return True  # Continue anyway
        
        print(f"   Notebook Object ID: {object_id}")
        
    except Exception as e:
        print(f"⚠️  Warning: Could not get notebook ID: {e}")
        return True  # Continue anyway
    
    # Now grant permission using the object ID
    url = f"{BASE_URL}/api/2.0/permissions/notebooks/{object_id}"
    payload = {
        "access_control_list": [
            {
                "service_principal_name": SP_CLIENT_ID,
                "permission_level": "CAN_READ"
            }
        ]
    }
    
    try:
        response = requests.patch(url, headers=HEADERS, json=payload)
        if response.status_code == 200:
            print(f"✅ Permission granted!")
            return True
        else:
            print(f"⚠️  Warning: Permission grant returned {response.status_code}")
            print(f"   Response: {response.text}")
            return True  # Continue anyway
    except Exception as e:
        print(f"⚠️  Warning: Could not grant permission: {e}")
        return True  # Continue anyway


def create_job(run_as_user):
    """Create a Databricks job to run the notebook"""
    is_sp = run_as_user == SP_CLIENT_ID
    job_name = f"Complete DEFINER Tests - Run as {'SP' if is_sp else 'User'}"
    
    print(f"\n📝 Creating job: {job_name}")
    
    url = f"{BASE_URL}/api/2.1/jobs/create"
    payload = {
        "name": job_name,
        "tasks": [
            {
                "task_key": "complete_definer_tests",
                "notebook_task": {
                    "notebook_path": NOTEBOOK_PATH,
                    "source": "WORKSPACE"
                },
                "existing_cluster_id": CLUSTER_ID,
                "timeout_seconds": 7200,  # 2 hours
                "libraries": []
            }
        ],
        "timeout_seconds": 7200,
        "max_concurrent_runs": 1
    }
    
    # Add run_as if running as SP
    if is_sp:
        payload["run_as"] = {
            "service_principal_name": run_as_user
        }
    
    try:
        response = requests.post(url, headers=HEADERS, json=payload)
        if response.status_code == 200:
            job_id = response.json()["job_id"]
            print(f"✅ Job created: {job_id}")
            return job_id
        else:
            print(f"❌ Job creation failed: {response.status_code}")
            print(f"   Response: {response.text}")
            return None
    except Exception as e:
        print(f"❌ Job creation error: {e}")
        return None


def run_job(job_id):
    """Trigger a job run"""
    print(f"\n▶️  Triggering job: {job_id}")
    
    url = f"{BASE_URL}/api/2.1/jobs/run-now"
    payload = {"job_id": job_id}
    
    try:
        response = requests.post(url, headers=HEADERS, json=payload)
        if response.status_code == 200:
            run_id = response.json()["run_id"]
            print(f"✅ Job started: Run ID {run_id}")
            
            job_url = f"https://{SERVER_HOSTNAME}/#job/{job_id}/run/{run_id}"
            print(f"\n🔗 Monitor run:")
            print(f"   {job_url}")
            
            return run_id
        else:
            print(f"❌ Job run failed: {response.status_code}")
            print(f"   Response: {response.text}")
            return None
    except Exception as e:
        print(f"❌ Job run error: {e}")
        return None


def get_run_status(run_id):
    """Get the status of a job run"""
    url = f"{BASE_URL}/api/2.1/jobs/runs/get"
    params = {"run_id": run_id}
    
    try:
        response = requests.get(url, headers=HEADERS, params=params)
        if response.status_code == 200:
            data = response.json()
            return data.get("state", {}).get("life_cycle_state"), data.get("state", {}).get("result_state")
        else:
            return None, None
    except Exception as e:
        print(f"⚠️  Status check error: {e}")
        return None, None


def main():
    print("=" * 80)
    print("🧪 COMPLETE DEFINER IMPERSONATION TEST SUITE (94 Tests)")
    print("=" * 80)
    print("")
    print("This will upload and run the complete test suite that validates:")
    print("  • DEFINER procedures execute with OWNER'S permissions")
    print("  • NOT with INVOKER'S permissions")
    print("")
    print("Test Categories:")
    print("  • Core Impersonation (10 tests)")
    print("  • Error Handling (2 tests)")
    print("  • Unity Catalog (4 tests)")
    print("  • Negative Cases (3 tests)")
    print("  • Compliance (1 test)")
    print("  • Known Issues (5 tests)")
    print("  • Advanced Scenarios (53 tests)")
    print("    - Nested context switching (10)")
    print("    - Permission patterns (10)")
    print("    - Parameterized SQL (8)")
    print("    - Unity Catalog integration (10)")
    print("    - Error boundaries (10)")
    print("    - Concurrency & Compliance (5)")
    print("  • ✨ Bidirectional Cross-Principal (8 tests)")
    print("  • ✨ Deep Impersonation (8 tests)")
    print("")
    print("Bidirectional Validation:")
    print("  • Job 1 (User): User creates → SP executes (8 tests)")
    print("  • Job 2 (SP): SP creates → User executes (8 tests)")
    print("  • Result: Both directions validated automatically!")
    print("")
    print("=" * 80)
    
    # Step 1: Create folder
    if not create_folder():
        print("\n❌ Failed to create workspace folder")
        return 1
    
    # Step 2: Upload notebook
    if not upload_notebook():
        print("\n❌ Failed to upload notebook")
        return 1
    
    # Step 3: Grant SP permission
    if not grant_sp_permission():
        print("\n⚠️  Could not grant SP permission, but continuing...")
    
    # Step 4: Create jobs (User and SP)
    print("\n" + "=" * 80)
    print("📋 Creating Jobs")
    print("=" * 80)
    
    job_id_user = create_job(USER_EMAIL)
    job_id_sp = create_job(SP_CLIENT_ID)
    
    if not job_id_user or not job_id_sp:
        print("\n❌ Failed to create one or both jobs")
        return 1
    
    # Step 5: Run both jobs
    print("\n" + "=" * 80)
    print("▶️  Running Jobs")
    print("=" * 80)
    
    run_id_user = run_job(job_id_user)
    time.sleep(2)  # Small delay between launches
    run_id_sp = run_job(job_id_sp)
    
    if not run_id_user or not run_id_sp:
        print("\n❌ Failed to start one or both jobs")
        return 1
    
    # Step 6: Display summary
    print("\n" + "=" * 80)
    print("✅ COMPLETE DEFINER TEST SUITE - JOBS LAUNCHED")
    print("=" * 80)
    print("")
    print("📊 Job 1: Run as User")
    print(f"   Job ID:  {job_id_user}")
    print(f"   Run ID:  {run_id_user}")
    print(f"   Status:  🟢 RUNNING")
    print(f"   Monitor: https://{SERVER_HOSTNAME}/#job/{job_id_user}/run/{run_id_user}")
    print("")
    print("📊 Job 2: Run as Service Principal")
    print(f"   Job ID:  {job_id_sp}")
    print(f"   Run ID:  {run_id_sp}")
    print(f"   Status:  🟢 RUNNING")
    print(f"   Monitor: https://{SERVER_HOSTNAME}/#job/{job_id_sp}/run/{run_id_sp}")
    print("")
    print("=" * 80)
    print("⏱️  Expected Duration: ~20-25 minutes for all 94 tests")
    print("=" * 80)
    print("")
    
    return 0


if __name__ == "__main__":
    sys.exit(main())
