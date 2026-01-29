# Databricks notebook source
# MAGIC %md
# MAGIC # SQL SECURITY DEFINER Test Runner
# MAGIC 
# MAGIC This notebook runs the SQL SECURITY DEFINER test suite on Serverless Compute.

# COMMAND ----------

# MAGIC %md
# MAGIC ## Setup

# COMMAND ----------

# Install dependencies if needed
import sys
import os

# Add project to path
project_path = "/Workspace/Repos/abhishek.dey@databricks.com/DBSQL-features-bug-bash"
if os.path.exists(project_path):
    sys.path.insert(0, project_path)

# COMMAND ----------

# MAGIC %md
# MAGIC ## Configuration

# COMMAND ----------

from framework.config import (
    SERVER_HOSTNAME, HTTP_PATH, PAT_TOKEN, CATALOG, SCHEMA,
    SERVICE_PRINCIPAL_B_ID, SERVICE_PRINCIPAL_CLIENT_ID, SERVICE_PRINCIPAL_CLIENT_SECRET
)

# Get test suite parameter (default to "all")
dbutils.widgets.text("test_suite", "all", "Test Suite")
test_suite = dbutils.widgets.get("test_suite")

print(f"Running test suite: {test_suite}")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Run Tests

# COMMAND ----------

from framework.test_framework import DatabricksConnection, TestExecutor, TestReporter
import json
from datetime import datetime

# Connect to SQL Warehouse
print("Connecting to SQL Warehouse...")
conn = DatabricksConnection(SERVER_HOSTNAME, HTTP_PATH, PAT_TOKEN, CATALOG, SCHEMA)

# Import test modules based on suite
test_functions = []

if test_suite in ["all", "core"]:
    from tests.core.test_core_impersonation import get_tests as get_core_tests
    test_functions.append(("Core Impersonation", get_core_tests))

if test_suite in ["all", "access"]:
    from tests.access.test_object_access import get_tests as get_access_tests
    test_functions.append(("Object Access", get_access_tests))

if test_suite in ["all", "nested"]:
    from tests.nested.test_nested_procedures import get_tests as get_nested_tests
    test_functions.append(("Nested Procedures", get_nested_tests))

if test_suite in ["all", "security"]:
    from tests.security.test_injection_safety import get_tests as get_security_tests
    test_functions.append(("Injection Safety", get_security_tests))

if test_suite in ["all", "observability"]:
    from tests.observability.test_error_handling import get_tests as get_observability_tests
    test_functions.append(("Error Handling", get_observability_tests))

if test_suite in ["all", "unity"]:
    from tests.unity.test_unity_catalog import get_tests as get_unity_tests
    test_functions.append(("Unity Catalog", get_unity_tests))

if test_suite in ["all", "negative"]:
    from tests.negative.test_abuse_cases import get_tests as get_negative_tests
    test_functions.append(("Negative Cases", get_negative_tests))

if test_suite in ["all", "compliance"]:
    from tests.compliance.test_regression import get_tests as get_compliance_tests
    test_functions.append(("Compliance", get_compliance_tests))

if test_suite in ["all", "known"]:
    from tests.known_issues.test_known_issues import get_tests as get_known_tests
    test_functions.append(("Known Issues", get_known_tests))

# COMMAND ----------

# MAGIC %md
# MAGIC ## Execute Tests

# COMMAND ----------

all_results = []
executor = TestExecutor(conn)

for suite_name, get_tests_func in test_functions:
    print("=" * 80)
    print(f"📦 Running {suite_name}")
    print("=" * 80)
    
    test_cases = get_tests_func()
    results = [executor.run_test(tc) for tc in test_cases]
    all_results.extend(results)
    
    # Print quick summary
    passed = sum(1 for r in results if r.status == "PASS")
    failed = sum(1 for r in results if r.status == "FAIL")
    print(f"✅ Passed: {passed}, ❌ Failed: {failed}")
    print()

conn.close()

# COMMAND ----------

# MAGIC %md
# MAGIC ## Results Summary

# COMMAND ----------

reporter = TestReporter(all_results)
reporter.print_summary()

# Save results
results_dict = {
    "timestamp": datetime.now().isoformat(),
    "test_suite": test_suite,
    "summary": {
        "total": len(all_results),
        "passed": sum(1 for r in all_results if r.status == "PASS"),
        "failed": sum(1 for r in all_results if r.status == "FAIL"),
        "skipped": sum(1 for r in all_results if r.status == "SKIP"),
        "errors": sum(1 for r in all_results if r.status == "ERROR")
    },
    "tests": [
        {
            "test_id": r.test_id,
            "description": r.description,
            "status": r.status,
            "execution_time": r.execution_time,
            "error_message": r.error_message,
            "timestamp": r.timestamp.isoformat() if r.timestamp else None
        }
        for r in all_results
    ]
}

# Display results
print(json.dumps(results_dict["summary"], indent=2))

# COMMAND ----------

# MAGIC %md
# MAGIC ## Exit Status

# COMMAND ----------

# Exit with appropriate code
failed_count = results_dict["summary"]["failed"]
error_count = results_dict["summary"]["errors"]

if failed_count > 0 or error_count > 0:
    print(f"❌ Tests completed with {failed_count} failures and {error_count} errors")
    dbutils.notebook.exit(f"FAILED: {failed_count} failures, {error_count} errors")
else:
    print("✅ All tests passed!")
    dbutils.notebook.exit("SUCCESS")
