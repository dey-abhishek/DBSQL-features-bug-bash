#!/usr/bin/env python3
"""
Upload the fixed complete_definer_tests.py notebook to update existing jobs
"""

import os
import sys
import base64
import requests

# Configuration from environment
SERVER_HOSTNAME = os.getenv("DATABRICKS_SERVER_HOSTNAME", "e2-dogfood-unity-catalog-us-east-1.staging.cloud.databricks.com")
PAT_TOKEN = os.getenv("DATABRICKS_PAT_TOKEN")
USER_EMAIL = os.getenv("USER_A", "abhishek.dey@databricks.com")

if not PAT_TOKEN:
    print("❌ ERROR: DATABRICKS_PAT_TOKEN environment variable not set")
    sys.exit(1)

BASE_URL = f"https://{SERVER_HOSTNAME}"
HEADERS = {
    "Authorization": f"Bearer {PAT_TOKEN}",
    "Content-Type": "application/json"
}

NOTEBOOK_PATH = f"/Users/{USER_EMAIL}/security_definer/complete_definer_tests"
LOCAL_NOTEBOOK = "security_definer/tests/notebooks/complete_definer_tests.py"


def upload_notebook():
    """Upload the notebook to Databricks workspace"""
    print(f"\n📤 Uploading fixed notebook: {LOCAL_NOTEBOOK} → {NOTEBOOK_PATH}")
    
    if not os.path.exists(LOCAL_NOTEBOOK):
        print(f"❌ ERROR: Local notebook not found: {LOCAL_NOTEBOOK}")
        return False
    
    with open(LOCAL_NOTEBOOK, 'r') as f:
        content = f.read()
    
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
            print(f"\n🔄 The notebook has been updated for both jobs:")
            print(f"   • Job 1 (User): Will use fixed notebook on next run")
            print(f"   • Job 2 (SP): Will use fixed notebook on next run")
            return True
        else:
            print(f"❌ Upload failed: {response.status_code}")
            print(f"   Response: {response.text}")
            return False
    except Exception as e:
        print(f"❌ Upload error: {e}")
        return False


def main():
    print("=" * 80)
    print("🔧 UPLOADING FIXED NOTEBOOK")
    print("=" * 80)
    print("")
    print("Fixes applied:")
    print("  • TC-06: Changed CTAS to CREATE + INSERT (FQN not allowed in CTAS)")
    print("  • TC-19: Removed DECLARE/SELECT INTO (use subquery instead)")
    print("  • TC-27: Changed CTAS to CREATE + INSERT (nested DDL)")
    print("")
    print("=" * 80)
    
    if upload_notebook():
        print("")
        print("=" * 80)
        print("✅ NOTEBOOK UPDATED")
        print("=" * 80)
        print("")
        print("The existing jobs will use the fixed notebook.")
        print("You can re-run the jobs from the Databricks UI or cancel current runs.")
        print("")
        return 0
    else:
        print("")
        print("❌ Upload failed")
        return 1


if __name__ == "__main__":
    sys.exit(main())
