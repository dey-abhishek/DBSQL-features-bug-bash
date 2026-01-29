"""
Framework configuration for SQL SECURITY DEFINER tests
Uses environment variables for sensitive values with fallback to defaults (for backward compatibility)
"""

import os

# Workspace configuration (non-sensitive)
WORKSPACE_URL = os.getenv("DATABRICKS_WORKSPACE_URL", "https://e2-dogfood-unity-catalog-us-east-1.staging.cloud.databricks.com")
SERVER_HOSTNAME = os.getenv("DATABRICKS_SERVER_HOSTNAME", "e2-dogfood-unity-catalog-us-east-1.staging.cloud.databricks.com")

# SQL Warehouse configuration
WAREHOUSE_ID = os.getenv("DATABRICKS_WAREHOUSE_ID", "4bafae112e5b5f6e")
HTTP_PATH = os.getenv("DATABRICKS_HTTP_PATH", f"/sql/1.0/warehouses/{WAREHOUSE_ID}")

# Catalog and Schema (non-sensitive)
CATALOG = os.getenv("DATABRICKS_CATALOG", "ad_bugbash")
SCHEMA = os.getenv("DATABRICKS_SCHEMA", "ad_bugbash_schema")

# User identity (non-sensitive)
USER_A = os.getenv("DATABRICKS_USER", "abhishek.dey@databricks.com")

# Service Principal (non-sensitive UUID, sensitive credentials)
SERVICE_PRINCIPAL_B = "bugbash_ad_sp"
SERVICE_PRINCIPAL_B_ID = os.getenv("DATABRICKS_SP_ID", "9c819e4d-1280-4ffa-85a0-e50b41222f52")

# Sensitive credentials - MUST USE ENVIRONMENT VARIABLES
# For local development: export DATABRICKS_PAT_TOKEN="your_token"
# For notebooks: Use dbutils.secrets.get()
PAT_TOKEN = os.getenv("DATABRICKS_PAT_TOKEN")
SERVICE_PRINCIPAL_CLIENT_ID = os.getenv("DATABRICKS_SP_CLIENT_ID", SERVICE_PRINCIPAL_B_ID)
SERVICE_PRINCIPAL_CLIENT_SECRET = os.getenv("DATABRICKS_SP_CLIENT_SECRET")

# Validate that required secrets are set
_missing_vars = []
if not PAT_TOKEN:
    _missing_vars.append("DATABRICKS_PAT_TOKEN")
if not SERVICE_PRINCIPAL_CLIENT_SECRET:
    _missing_vars.append("DATABRICKS_SP_CLIENT_SECRET")

if _missing_vars:
    import sys
    print("=" * 80, file=sys.stderr)
    print("❌ ERROR: Missing required environment variables", file=sys.stderr)
    print("=" * 80, file=sys.stderr)
    print(f"Missing: {', '.join(_missing_vars)}", file=sys.stderr)
    print(file=sys.stderr)
    print("📋 Required setup:", file=sys.stderr)
    print(file=sys.stderr)
    print("   For local testing, set environment variables:", file=sys.stderr)
    print("   export DATABRICKS_PAT_TOKEN='your_token'", file=sys.stderr)
    print("   export DATABRICKS_SP_CLIENT_SECRET='your_secret'", file=sys.stderr)
    print(file=sys.stderr)
    print("   For notebooks, use Databricks Secrets:", file=sys.stderr)
    print("   Run: ./security_definer/bin/python setup_secrets.py", file=sys.stderr)
    print("=" * 80, file=sys.stderr)
    raise EnvironmentError(f"Missing required environment variables: {', '.join(_missing_vars)}")

# Serverless configuration
SERVERLESS_WAREHOUSE_ID = os.getenv("DATABRICKS_SERVERLESS_WAREHOUSE_ID", "4bafae112e5b5f6e")
SERVERLESS_HTTP_PATH = f"/sql/1.0/warehouses/{SERVERLESS_WAREHOUSE_ID}"
SERVERLESS_CLUSTER_ID = os.getenv("DATABRICKS_SERVERLESS_CLUSTER_ID", "0127-210051-t8p9ys5k")

# Print configuration (sanitized) when imported
def print_config():
    """Print current configuration with sensitive values masked"""
    print("="*80)
    print("🔧 Framework Configuration")
    print("="*80)
    print(f"Workspace: {WORKSPACE_URL}")
    print(f"Catalog:   {CATALOG}")
    print(f"Schema:    {SCHEMA}")
    print(f"User:      {USER_A}")
    print(f"SP UUID:   {SERVICE_PRINCIPAL_B_ID}")
    if PAT_TOKEN:
        print(f"PAT Token: {'*' * 20} (✅ loaded from env)")
    else:
        print(f"PAT Token: ❌ NOT SET")
    if SERVICE_PRINCIPAL_CLIENT_SECRET:
        print(f"SP Secret: {'*' * 20} (✅ loaded from env)")
    else:
        print(f"SP Secret: ❌ NOT SET")
    print("="*80)

# Only print if directly imported (not from other modules)
if __name__ == "__main__":
    print_config()
