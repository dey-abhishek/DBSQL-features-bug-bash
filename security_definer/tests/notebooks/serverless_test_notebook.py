# Databricks notebook source
# MAGIC %md
# MAGIC # SQL SECURITY DEFINER - Serverless Test Execution
# MAGIC 
# MAGIC This notebook runs all DEFINER mode tests on Serverless General Compute
# MAGIC 
# MAGIC **Cluster**: 0127-210051-t8p9ys5k (18.0 - definer, serverless-like)

# COMMAND ----------

# MAGIC %md
# MAGIC ## 1. Install Dependencies

# COMMAND ----------

# Install required packages
%pip install databricks-sql-connector databricks-sdk --quiet
dbutils.library.restartPython()

# COMMAND ----------

# MAGIC %md
# MAGIC ## 2. Configuration (Using Secrets)

# COMMAND ----------

# Configuration - Using Databricks Secrets for sensitive values
# Non-sensitive configuration
SERVER_HOSTNAME = "e2-dogfood-unity-catalog-us-east-1.staging.cloud.databricks.com"
HTTP_PATH = "/sql/1.0/warehouses/4bafae112e5b5f6e"
CATALOG = "ad_bugbash"
SCHEMA = "ad_bugbash_schema"
USER_A = "abhishek.dey@databricks.com"
SERVICE_PRINCIPAL_B_ID = "9c819e4d-1280-4ffa-85a0-e50b41222f52"

# Sensitive values from Databricks Secrets
# Secret scope: "definer_tests" (must be created first via setup_secrets.py)
try:
    PAT_TOKEN = dbutils.secrets.get(scope="definer_tests", key="pat_token")
    SERVICE_PRINCIPAL_CLIENT_ID = dbutils.secrets.get(scope="definer_tests", key="sp_client_id")
    SERVICE_PRINCIPAL_CLIENT_SECRET = dbutils.secrets.get(scope="definer_tests", key="sp_client_secret")
    print("✅ Secrets loaded successfully")
    print(f"   PAT Token: {'*' * 20} (hidden)")
    print(f"   SP Client Secret: {'*' * 20} (hidden)")
except Exception as e:
    print(f"❌ ERROR: Could not load secrets from scope 'definer_tests'")
    print(f"   Error: {e}")
    print()
    print("📋 Setup required:")
    print("   1. Run: ./security_definer/bin/python setup_secrets.py")
    print("   2. Or manually create secrets:")
    print("      databricks secrets create-scope definer_tests")
    print("      databricks secrets put --scope definer_tests --key pat_token")
    print("      databricks secrets put --scope definer_tests --key sp_client_id")
    print("      databricks secrets put --scope definer_tests --key sp_client_secret")
    print()
    raise RuntimeError("Secrets not configured. Cannot proceed without credentials.")

print(f"✅ Configuration loaded")
print(f"   Workspace: {SERVER_HOSTNAME}")
print(f"   Catalog: {CATALOG}")
print(f"   Schema: {SCHEMA}")

# COMMAND ----------

# MAGIC %md
# MAGIC ## 3. Import Test Framework

# COMMAND ----------

# Import test framework components
from databricks import sql
from dataclasses import dataclass
from typing import List, Optional, Tuple
from datetime import datetime
import json

@dataclass
class TestResult:
    test_id: str
    description: str
    status: str  # PASS, FAIL, SKIP, ERROR
    execution_time: float
    error_message: Optional[str] = None
    timestamp: Optional[datetime] = None

@dataclass
class DefinerTestCase:
    test_id: str
    description: str
    setup_sql: List[str]
    test_sql: str
    teardown_sql: List[str]
    should_fail: bool = False
    skip_reason: Optional[str] = None

class DatabricksConnection:
    def __init__(self, server_hostname: str, http_path: str, access_token: str, catalog: str, schema: str):
        self.connection = sql.connect(
            server_hostname=server_hostname,
            http_path=http_path,
            access_token=access_token
        )
        self.catalog = catalog
        self.schema = schema
        cursor = self.connection.cursor()
        cursor.execute(f"USE CATALOG {catalog}")
        cursor.execute(f"USE SCHEMA {schema}")
        cursor.close()
    
    def execute(self, query: str) -> Tuple[Optional[List], Optional[str]]:
        try:
            cursor = self.connection.cursor()
            cursor.execute(query)
            if query.strip().upper().startswith(('SELECT', 'SHOW', 'DESCRIBE', 'CALL')):
                results = cursor.fetchall()
            else:
                results = None
            cursor.close()
            return results, None
        except Exception as e:
            return None, str(e)
    
    def close(self):
        if self.connection:
            self.connection.close()

class TestExecutor:
    def __init__(self, conn: DatabricksConnection):
        self.conn = conn
    
    def run_test(self, test_case: DefinerTestCase) -> TestResult:
        start_time = datetime.now()
        
        if test_case.skip_reason:
            return TestResult(
                test_id=test_case.test_id,
                description=test_case.description,
                status="SKIP",
                execution_time=0,
                error_message=test_case.skip_reason,
                timestamp=start_time
            )
        
        try:
            # Setup
            for sql in test_case.setup_sql:
                _, error = self.conn.execute(sql)
                if error:
                    raise Exception(f"Setup failed: {error}")
            
            # Execute test
            result, error = self.conn.execute(test_case.test_sql)
            
            # Check result
            if test_case.should_fail:
                if error:
                    status = "PASS"
                    error_msg = None
                else:
                    status = "FAIL"
                    error_msg = "Expected failure but query succeeded"
            else:
                if error:
                    status = "FAIL"
                    error_msg = error
                else:
                    status = "PASS"
                    error_msg = None
            
            # Teardown
            for sql in test_case.teardown_sql:
                self.conn.execute(sql)
            
            end_time = datetime.now()
            execution_time = (end_time - start_time).total_seconds()
            
            return TestResult(
                test_id=test_case.test_id,
                description=test_case.description,
                status=status,
                execution_time=execution_time,
                error_message=error_msg,
                timestamp=start_time
            )
        
        except Exception as e:
            end_time = datetime.now()
            execution_time = (end_time - start_time).total_seconds()
            return TestResult(
                test_id=test_case.test_id,
                description=test_case.description,
                status="ERROR",
                execution_time=execution_time,
                error_message=str(e),
                timestamp=start_time
            )

print("✅ Test framework loaded")

# COMMAND ----------

# MAGIC %md
# MAGIC ## 4. Define Core Tests (TC-01 to TC-03)

# COMMAND ----------

def get_core_tests():
    tests = []
    
    # TC-01: Basic DEFINER identity
    tc01 = DefinerTestCase(
        test_id="TC-01",
        description="DEFINER Identity - Procedure uses owner's identity",
        setup_sql=[
            f"DROP PROCEDURE IF EXISTS {CATALOG}.{SCHEMA}.tc01_definer_test",
            f"""
            CREATE PROCEDURE {CATALOG}.{SCHEMA}.tc01_definer_test()
            LANGUAGE SQL
            AS BEGIN
                SELECT CURRENT_USER() as user, 'DEFINER' as mode;
            END
            """
        ],
        test_sql=f"CALL {CATALOG}.{SCHEMA}.tc01_definer_test()",
        teardown_sql=[f"DROP PROCEDURE IF EXISTS {CATALOG}.{SCHEMA}.tc01_definer_test"],
        should_fail=False
    )
    tests.append(tc01)
    
    # TC-02: Permission elevation
    tc02 = DefinerTestCase(
        test_id="TC-02",
        description="Permission Elevation - DEFINER grants access through procedure",
        setup_sql=[
            f"DROP TABLE IF EXISTS {CATALOG}.{SCHEMA}.tc02_restricted_table",
            f"CREATE TABLE {CATALOG}.{SCHEMA}.tc02_restricted_table (id INT, data STRING)",
            f"INSERT INTO {CATALOG}.{SCHEMA}.tc02_restricted_table VALUES (1, 'sensitive')",
            f"DROP PROCEDURE IF EXISTS {CATALOG}.{SCHEMA}.tc02_gateway_proc",
            f"""
            CREATE PROCEDURE {CATALOG}.{SCHEMA}.tc02_gateway_proc()
            LANGUAGE SQL
            AS BEGIN
                SELECT COUNT(*) as row_count FROM {CATALOG}.{SCHEMA}.tc02_restricted_table;
            END
            """,
            f"GRANT EXECUTE ON PROCEDURE {CATALOG}.{SCHEMA}.tc02_gateway_proc TO `{SERVICE_PRINCIPAL_B_ID}`"
        ],
        test_sql=f"CALL {CATALOG}.{SCHEMA}.tc02_gateway_proc()",
        teardown_sql=[
            f"DROP PROCEDURE IF EXISTS {CATALOG}.{SCHEMA}.tc02_gateway_proc",
            f"DROP TABLE IF EXISTS {CATALOG}.{SCHEMA}.tc02_restricted_table"
        ],
        should_fail=False
    )
    tests.append(tc02)
    
    # TC-03: DEFINER mode validation
    tc03 = DefinerTestCase(
        test_id="TC-03",
        description="DEFINER Mode - Validate procedure runs with owner's identity",
        setup_sql=[
            f"DROP PROCEDURE IF EXISTS {CATALOG}.{SCHEMA}.tc03_definer_identity",
            f"""
            CREATE PROCEDURE {CATALOG}.{SCHEMA}.tc03_definer_identity()
            LANGUAGE SQL
            AS BEGIN
                SELECT 'definer_mode' as mode, CURRENT_USER() as user;
            END
            """
        ],
        test_sql=f"CALL {CATALOG}.{SCHEMA}.tc03_definer_identity()",
        teardown_sql=[f"DROP PROCEDURE IF EXISTS {CATALOG}.{SCHEMA}.tc03_definer_identity"],
        should_fail=False
    )
    tests.append(tc03)
    
    return tests

print("✅ Core tests defined")

# COMMAND ----------

# MAGIC %md
# MAGIC ## 5. Run Tests

# COMMAND ----------

print("="*80)
print("🚀 Starting SQL SECURITY DEFINER Tests on Serverless Compute")
print("="*80)
print()

# Connect
print("🔗 Connecting to SQL Warehouse...")
conn = DatabricksConnection(SERVER_HOSTNAME, HTTP_PATH, PAT_TOKEN, CATALOG, SCHEMA)
print("✅ Connected successfully")
print()

# Create executor
executor = TestExecutor(conn)

# Run core tests
print("="*80)
print("📦 Running Core Impersonation Tests (TC-01 to TC-03)")
print("="*80)
print()

test_cases = get_core_tests()
results = []

for test in test_cases:
    print(f"🧪 {test.test_id}: {test.description}")
    result = executor.run_test(test)
    results.append(result)
    
    status_icon = "✅" if result.status == "PASS" else "❌"
    print(f"   {status_icon} Status: {result.status} ({result.execution_time:.2f}s)")
    if result.error_message:
        print(f"   ⚠️  Error: {result.error_message}")
    print()

# Close connection
conn.close()

# COMMAND ----------

# MAGIC %md
# MAGIC ## 6. Results Summary

# COMMAND ----------

print("="*80)
print("📊 TEST RESULTS SUMMARY")
print("="*80)
print()

total = len(results)
passed = sum(1 for r in results if r.status == "PASS")
failed = sum(1 for r in results if r.status == "FAIL")
errors = sum(1 for r in results if r.status == "ERROR")
skipped = sum(1 for r in results if r.status == "SKIP")

print(f"Total Tests:   {total}")
print(f"✅ Passed:     {passed} ({100*passed/total:.1f}%)")
print(f"❌ Failed:     {failed}")
print(f"💥 Errors:     {errors}")
print(f"⏭️  Skipped:    {skipped}")
print()

if failed > 0 or errors > 0:
    print("❌ Failed/Error Tests:")
    for r in results:
        if r.status in ["FAIL", "ERROR"]:
            print(f"  • {r.test_id}: {r.description}")
            print(f"    Error: {r.error_message}")
else:
    print("🎉 All tests passed!")

print()
print("="*80)
print(f"✅ Serverless execution complete!")
print("="*80)

# COMMAND ----------

# MAGIC %md
# MAGIC ## 7. Export Results

# COMMAND ----------

# Create JSON report
report = {
    "execution_environment": "Serverless General Compute",
    "cluster_id": "0127-210051-t8p9ys5k",
    "timestamp": datetime.now().isoformat(),
    "catalog": CATALOG,
    "schema": SCHEMA,
    "summary": {
        "total": total,
        "passed": passed,
        "failed": failed,
        "errors": errors,
        "skipped": skipped
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
        for r in results
    ]
}

# Display report
print("📄 JSON Report:")
print(json.dumps(report, indent=2))

# Save to DBFS (optional)
# dbutils.fs.put("/tmp/serverless_test_results.json", json.dumps(report, indent=2), overwrite=True)
# print("\n✅ Results saved to: dbfs:/tmp/serverless_test_results.json")
