#!/bin/bash
# Upload test notebook to Databricks workspace and create job

set -e

echo "=================================="
echo "🚀 Serverless Test Setup"
echo "=================================="
echo ""

# Configuration
WORKSPACE_URL="https://e2-dogfood-unity-catalog-us-east-1.staging.cloud.databricks.com"
NOTEBOOK_PATH="/Users/abhishek.dey@databricks.com/serverless_test_notebook"
CLUSTER_ID="0127-210051-t8p9ys5k"
LOCAL_NOTEBOOK="tests/notebooks/serverless_test_notebook.py"

echo "📋 Configuration:"
echo "   Workspace: $WORKSPACE_URL"
echo "   Notebook path: $NOTEBOOK_PATH"
echo "   Cluster: $CLUSTER_ID"
echo ""

# Check if databricks CLI is installed
if ! command -v databricks &> /dev/null; then
    echo "❌ Databricks CLI not found. Installing..."
    pip install databricks-cli
    echo "✅ Databricks CLI installed"
fi

# Check if CLI is configured
if [ ! -f ~/.databrickscfg ]; then
    echo "⚙️  Configuring Databricks CLI..."
    databricks configure --token <<EOF
$WORKSPACE_URL
dapi***REDACTED***
EOF
    echo "✅ CLI configured"
else
    echo "✅ CLI already configured"
fi

# Upload notebook
echo ""
echo "📤 Uploading notebook to workspace..."
databricks workspace import \
  "$NOTEBOOK_PATH" \
  --file "$LOCAL_NOTEBOOK" \
  --language PYTHON \
  --format SOURCE \
  --overwrite

if [ $? -eq 0 ]; then
    echo "✅ Notebook uploaded successfully"
else
    echo "❌ Failed to upload notebook"
    exit 1
fi

# Create job (optional)
echo ""
echo "📝 Creating Databricks job..."

JOB_CONFIG=$(cat <<EOF
{
  "name": "SQL_DEFINER_Serverless_Tests_$(date +%Y%m%d_%H%M%S)",
  "tasks": [
    {
      "task_key": "run_definer_tests",
      "notebook_task": {
        "notebook_path": "$NOTEBOOK_PATH",
        "base_parameters": {}
      },
      "existing_cluster_id": "$CLUSTER_ID",
      "timeout_seconds": 3600,
      "max_retries": 0
    }
  ],
  "timeout_seconds": 3600,
  "max_concurrent_runs": 1
}
EOF
)

# Save job config to temp file
echo "$JOB_CONFIG" > /tmp/job_config.json

# Create job using databricks CLI
JOB_RESPONSE=$(databricks jobs create --json-file /tmp/job_config.json)
JOB_ID=$(echo "$JOB_RESPONSE" | python3 -c "import sys, json; print(json.load(sys.stdin)['job_id'])" 2>/dev/null || echo "")

if [ -n "$JOB_ID" ]; then
    echo "✅ Job created with ID: $JOB_ID"
    echo ""
    echo "🚀 To run the job:"
    echo "   databricks jobs run-now --job-id $JOB_ID"
    echo ""
    echo "🌐 View job in UI:"
    echo "   $WORKSPACE_URL/#job/$JOB_ID"
else
    echo "⚠️  Job creation skipped or failed (notebook can still be run manually)"
fi

# Clean up
rm -f /tmp/job_config.json

echo ""
echo "=================================="
echo "✅ Setup Complete!"
echo "=================================="
echo ""
echo "📋 Next steps:"
echo ""
echo "Option 1 - Run in Workspace UI (Fastest):"
echo "   1. Open: $WORKSPACE_URL"
echo "   2. Navigate to: Workspace → Users → abhishek.dey@databricks.com"
echo "   3. Open: serverless_test_notebook"
echo "   4. Attach to cluster: $CLUSTER_ID"
echo "   5. Click 'Run All'"
echo ""

if [ -n "$JOB_ID" ]; then
echo "Option 2 - Run via Jobs API:"
echo "   databricks jobs run-now --job-id $JOB_ID"
echo ""
fi

echo "Option 3 - Run via Python:"
echo "   cd $(pwd)"
echo "   PYTHONPATH=. ./security_definer/bin/python -c \\"
echo "     \"from framework.jobs_api import DatabricksJobsClient; \\"
echo "      from framework.config import WORKSPACE_URL, PAT_TOKEN; \\"
echo "      client = DatabricksJobsClient(WORKSPACE_URL, PAT_TOKEN); \\"
echo "      print(client.list_jobs())\""
echo ""
echo "🎉 Ready to test on serverless compute!"
