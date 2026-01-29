"""
Configuration for SQL SECURITY DEFINER testing environment
SECURE VERSION - All sensitive values MUST come from environment variables
"""

import os
import sys

# Workspace Configuration (non-sensitive)
WORKSPACE_URL = os.getenv("DATABRICKS_WORKSPACE_URL", "https://e2-dogfood-unity-catalog-us-east-1.staging.cloud.databricks.com")
WORKSPACE_ID = os.getenv("DATABRICKS_WORKSPACE_ID", "653212377970039")

# Compute Configuration (non-sensitive)
WAREHOUSE_ID = os.getenv("DATABRICKS_WAREHOUSE_ID", "4bafae112e5b5f6e")
SERVERLESS_CLUSTER_ID = os.getenv("DATABRICKS_SERVERLESS_CLUSTER_ID", "0127-210051-t8p9ys5k")

# Database Configuration (non-sensitive)
CATALOG = os.getenv("DATABRICKS_CATALOG", "ad_bugbash")
SCHEMA = os.getenv("DATABRICKS_SCHEMA", "ad_bugbash_schema")

# User Configuration (non-sensitive)
USER_A = os.getenv("DATABRICKS_USER", "abhishek.dey@databricks.com")  # Owner/Definer
SERVICE_PRINCIPAL_B = "bugbash_ad_sp"   # Invoker (name)
SERVICE_PRINCIPAL_B_ID = os.getenv("DATABRICKS_SP_ID", "9c819e4d-1280-4ffa-85a0-e50b41222f52")  # Invoker (UUID)

# Sensitive credentials - MUST be provided via environment variables
# NO DEFAULTS - Fail fast if not provided
PAT_TOKEN = os.getenv("DATABRICKS_PAT_TOKEN")
SERVICE_PRINCIPAL_CLIENT_ID = os.getenv("DATABRICKS_SP_CLIENT_ID", SERVICE_PRINCIPAL_B_ID)  # Can default to UUID
SERVICE_PRINCIPAL_CLIENT_SECRET = os.getenv("DATABRICKS_SP_CLIENT_SECRET")
SERVICE_PRINCIPAL_PAT = os.getenv("SERVICE_PRINCIPAL_PAT")  # Optional alternative to OAuth

# Validate that required secrets are set
_missing_vars = []
if not PAT_TOKEN:
    _missing_vars.append("DATABRICKS_PAT_TOKEN")
if not SERVICE_PRINCIPAL_CLIENT_SECRET and not SERVICE_PRINCIPAL_PAT:
    _missing_vars.append("DATABRICKS_SP_CLIENT_SECRET or SERVICE_PRINCIPAL_PAT")

if _missing_vars:
    print("=" * 80, file=sys.stderr)
    print("❌ ERROR: Missing required environment variables", file=sys.stderr)
    print("=" * 80, file=sys.stderr)
    print(f"Missing: {', '.join(_missing_vars)}", file=sys.stderr)
    print(file=sys.stderr)
    print("📋 Required setup for local testing:", file=sys.stderr)
    print(file=sys.stderr)
    print("   # Set environment variables:", file=sys.stderr)
    print("   export DATABRICKS_PAT_TOKEN='your_pat_token_here'", file=sys.stderr)
    print("   export DATABRICKS_SP_CLIENT_SECRET='your_sp_secret_here'", file=sys.stderr)
    print(file=sys.stderr)
    print("   # Or use a .env file (add to .gitignore!):", file=sys.stderr)
    print("   echo 'DATABRICKS_PAT_TOKEN=your_token' > .env", file=sys.stderr)
    print("   echo 'DATABRICKS_SP_CLIENT_SECRET=your_secret' >> .env", file=sys.stderr)
    print(file=sys.stderr)
    print("📋 For notebooks, use Databricks Secrets:", file=sys.stderr)
    print("   Run: ./security_definer/bin/python setup_secrets.py", file=sys.stderr)
    print("=" * 80, file=sys.stderr)
    raise EnvironmentError(
        f"Missing required environment variables: {', '.join(_missing_vars)}. "
        "Set these variables before running tests."
    )

# Derived Configuration
SERVER_HOSTNAME = WORKSPACE_URL.replace("https://", "")
HTTP_PATH = f"/sql/1.0/warehouses/{WAREHOUSE_ID}"

# Print configuration summary (with masked secrets)
def print_config_summary():
    """Print configuration summary with sensitive values masked"""
    print("🔧 Configuration Loaded:")
    print(f"   Workspace: {WORKSPACE_URL}")
    print(f"   Catalog:   {CATALOG}.{SCHEMA}")
    print(f"   User:      {USER_A}")
    print(f"   SP UUID:   {SERVICE_PRINCIPAL_B_ID}")
    print(f"   PAT Token: {'✅ Set (' + '*' * 10 + ')' if PAT_TOKEN else '❌ Not set'}")
    print(f"   SP Secret: {'✅ Set (' + '*' * 10 + ')' if SERVICE_PRINCIPAL_CLIENT_SECRET else '❌ Not set'}")
