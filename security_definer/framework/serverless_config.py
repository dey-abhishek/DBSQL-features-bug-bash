"""
Serverless Compute Configuration
Settings for running tests on Serverless General Compute instead of SQL Warehouse
SECURE VERSION - Uses environment variables
"""

import os
import sys

# Serverless Compute Configuration (non-sensitive)
SERVERLESS_CLUSTER_ID = os.getenv("DATABRICKS_SERVERLESS_CLUSTER_ID", "0127-210051-t8p9ys5k")
SERVERLESS_CLUSTER_NAME = "18.0 - definer, serverless-like"

# Jobs API Configuration (non-sensitive)
WORKSPACE_URL = os.getenv("DATABRICKS_WORKSPACE_URL", "https://e2-dogfood-unity-catalog-us-east-1.staging.cloud.databricks.com")
WORKSPACE_ID = os.getenv("DATABRICKS_WORKSPACE_ID", "653212377970039")

# Database Configuration (non-sensitive)
CATALOG = os.getenv("DATABRICKS_CATALOG", "ad_bugbash")
SCHEMA = os.getenv("DATABRICKS_SCHEMA", "ad_bugbash_schema")

# User Configuration (non-sensitive)
USER_A = os.getenv("DATABRICKS_USER", "abhishek.dey@databricks.com")
SERVICE_PRINCIPAL_B_ID = os.getenv("DATABRICKS_SP_ID", "9c819e4d-1280-4ffa-85a0-e50b41222f52")

# Authentication (SENSITIVE - MUST use environment variables)
PAT_TOKEN = os.getenv("DATABRICKS_PAT_TOKEN")
SERVICE_PRINCIPAL_CLIENT_ID = os.getenv("DATABRICKS_SP_CLIENT_ID", SERVICE_PRINCIPAL_B_ID)
SERVICE_PRINCIPAL_CLIENT_SECRET = os.getenv("DATABRICKS_SP_CLIENT_SECRET")

# Validate required secrets
_missing = []
if not PAT_TOKEN:
    _missing.append("DATABRICKS_PAT_TOKEN")
if not SERVICE_PRINCIPAL_CLIENT_SECRET:
    _missing.append("DATABRICKS_SP_CLIENT_SECRET")

if _missing:
    print("=" * 80, file=sys.stderr)
    print("❌ ERROR: Missing required environment variables", file=sys.stderr)
    print("=" * 80, file=sys.stderr)
    print(f"Missing: {', '.join(_missing)}", file=sys.stderr)
    print(file=sys.stderr)
    print("Set these before importing serverless_config:", file=sys.stderr)
    print("  export DATABRICKS_PAT_TOKEN='your_token'", file=sys.stderr)
    print("  export DATABRICKS_SP_CLIENT_SECRET='your_secret'", file=sys.stderr)
    print("=" * 80, file=sys.stderr)
    raise EnvironmentError(f"Missing required environment variables: {', '.join(_missing)}")

# Job Configuration
JOB_TIMEOUT_SECONDS = 3600  # 1 hour
JOB_MAX_RETRIES = 1
