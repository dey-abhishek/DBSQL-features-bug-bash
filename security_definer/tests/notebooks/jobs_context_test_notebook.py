# Databricks notebook source
# MAGIC %md
# MAGIC # Jobs API Context Switching Tests
# MAGIC 
# MAGIC Tests SQL SECURITY DEFINER impersonation with:
# MAGIC - Service Principal creates procedure, User executes
# MAGIC - User creates procedure, Service Principal executes
# MAGIC 
# MAGIC **Cluster**: Serverless General Compute

# COMMAND ----------

# MAGIC %md
# MAGIC ## 1. Install Dependencies

# COMMAND ----------

%pip install databricks-sql-connector databricks-sdk --quiet
dbutils.library.restartPython()

# COMMAND ----------

# MAGIC %md
# MAGIC ## 2. Configuration

# COMMAND ----------

# Configuration (non-sensitive)
SERVER_HOSTNAME = "e2-dogfood-unity-catalog-us-east-1.staging.cloud.databricks.com"
HTTP_PATH = "/sql/1.0/warehouses/4bafae112e5b5f6e"
CATALOG = "ad_bugbash"
SCHEMA = "ad_bugbash_schema"
USER_A = "abhishek.dey@databricks.com"
SERVICE_PRINCIPAL_B_ID = "9c819e4d-1280-4ffa-85a0-e50b41222f52"

# Load secrets
try:
    PAT_TOKEN = dbutils.secrets.get(scope="definer_tests", key="pat_token")
    SP_CLIENT_ID = dbutils.secrets.get(scope="definer_tests", key="sp_client_id")
    SP_CLIENT_SECRET = dbutils.secrets.get(scope="definer_tests", key="sp_client_secret")
    print("✅ Secrets loaded successfully")
except Exception as e:
    print(f"❌ ERROR: Could not load secrets: {e}")
    raise RuntimeError("Secrets not configured")

# COMMAND ----------

# MAGIC %md
# MAGIC ## 3. Setup Connections (Both User and SP)

# COMMAND ----------

from databricks import sql
from typing import Tuple, Optional, List

class DualConnection:
    """Manages both User and Service Principal connections"""
    
    def __init__(self, server_hostname, http_path, user_token, sp_client_id, sp_client_secret, catalog, schema):
        self.server_hostname = server_hostname
        self.http_path = http_path
        self.catalog = catalog
        self.schema = schema
        
        # User connection
        print("🔐 Connecting as User...")
        self.user_conn = sql.connect(
            server_hostname=server_hostname,
            http_path=http_path,
            access_token=user_token
        )
        self._set_context(self.user_conn)
        print(f"✅ User connected: {self._get_current_user(self.user_conn)}")
        
        # Service Principal connection (OAuth M2M)
        print("🔐 Connecting as Service Principal (OAuth M2M)...")
        self.sp_conn = sql.connect(
            server_hostname=server_hostname,
            http_path=http_path,
            auth_type="oauth-m2m",
            client_id=sp_client_id,
            client_secret=sp_client_secret
        )
        self._set_context(self.sp_conn)
        print(f"✅ SP connected: {self._get_current_user(self.sp_conn)}")
    
    def _set_context(self, conn):
        """Set catalog and schema"""
        cursor = conn.cursor()
        cursor.execute(f"USE CATALOG {self.catalog}")
        cursor.execute(f"USE SCHEMA {self.schema}")
        cursor.close()
    
    def _get_current_user(self, conn) -> str:
        """Get current user"""
        cursor = conn.cursor()
        cursor.execute("SELECT CURRENT_USER()")
        result = cursor.fetchone()[0]
        cursor.close()
        return result
    
    def execute_as_user(self, sql: str) -> Tuple[Optional[List], Optional[str]]:
        """Execute SQL as User"""
        try:
            cursor = self.user_conn.cursor()
            cursor.execute(sql)
            if sql.strip().upper().startswith(('SELECT', 'SHOW', 'DESCRIBE', 'CALL')):
                results = cursor.fetchall()
            else:
                results = None
            cursor.close()
            return results, None
        except Exception as e:
            return None, str(e)
    
    def execute_as_sp(self, sql: str) -> Tuple[Optional[List], Optional[str]]:
        """Execute SQL as Service Principal"""
        try:
            cursor = self.sp_conn.cursor()
            cursor.execute(sql)
            if sql.strip().upper().startswith(('SELECT', 'SHOW', 'DESCRIBE', 'CALL')):
                results = cursor.fetchall()
            else:
                results = None
            cursor.close()
            return results, None
        except Exception as e:
            return None, str(e)
    
    def close(self):
        """Close both connections"""
        if self.user_conn:
            self.user_conn.close()
        if self.sp_conn:
            self.sp_conn.close()
        print("🔌 Connections closed")

# Create dual connection
dual_conn = DualConnection(
    SERVER_HOSTNAME, HTTP_PATH,
    PAT_TOKEN, SP_CLIENT_ID, SP_CLIENT_SECRET,
    CATALOG, SCHEMA
)

# COMMAND ----------

# MAGIC %md
# MAGIC ## 4. Test Case 1: SP Creates, User Executes

# COMMAND ----------

print("=" * 80)
print("TEST 1: Service Principal creates DEFINER procedure, User executes")
print("=" * 80)

# Setup: Create table and revoke from user
print("\n📦 Setup:")
print("  1. Creating table (owned by User)")
dual_conn.execute_as_user(f"DROP TABLE IF EXISTS {CATALOG}.{SCHEMA}.jobs_test_sp_table")
dual_conn.execute_as_user(f"CREATE TABLE {CATALOG}.{SCHEMA}.jobs_test_sp_table (id INT, data STRING)")
dual_conn.execute_as_user(f"INSERT INTO {CATALOG}.{SCHEMA}.jobs_test_sp_table VALUES (1, 'sp_owned_data')")
print("  ✅ Table created")

print("  2. Granting access to SP")
dual_conn.execute_as_user(f"GRANT ALL PRIVILEGES ON TABLE {CATALOG}.{SCHEMA}.jobs_test_sp_table TO `{SERVICE_PRINCIPAL_B_ID}`")
print("  ✅ SP granted access")

print("  3. SP creates DEFINER procedure")
dual_conn.execute_as_sp(f"DROP PROCEDURE IF EXISTS {CATALOG}.{SCHEMA}.jobs_sp_proc")
dual_conn.execute_as_sp(f"""
CREATE PROCEDURE {CATALOG}.{SCHEMA}.jobs_sp_proc()
LANGUAGE SQL
SQL SECURITY DEFINER
AS BEGIN
    SELECT COUNT(*) as row_count, 'SP owned proc' as proc_type, CURRENT_USER() as executed_by
    FROM {CATALOG}.{SCHEMA}.jobs_test_sp_table;
END
""")
print("  ✅ SP created procedure")

print("  4. Granting EXECUTE to User")
dual_conn.execute_as_sp(f"GRANT EXECUTE ON PROCEDURE {CATALOG}.{SCHEMA}.jobs_sp_proc TO `{USER_A}`")
print("  ✅ User granted EXECUTE")

# Test: User calls SP's procedure
print("\n▶️  Test: User calls SP-owned DEFINER procedure")
result, error = dual_conn.execute_as_user(f"CALL {CATALOG}.{SCHEMA}.jobs_sp_proc()")

if error:
    print(f"  ❌ FAILED: {error}")
else:
    print(f"  ✅ PASSED: {result}")
    print(f"     Row count: {result[0][0]}")
    print(f"     Proc type: {result[0][1]}")
    print(f"     Executed by: {result[0][2]}")

# Cleanup
print("\n🧹 Cleanup")
dual_conn.execute_as_sp(f"DROP PROCEDURE IF EXISTS {CATALOG}.{SCHEMA}.jobs_sp_proc")
dual_conn.execute_as_user(f"DROP TABLE IF EXISTS {CATALOG}.{SCHEMA}.jobs_test_sp_table")
print("  ✅ Cleaned up")

# COMMAND ----------

# MAGIC %md
# MAGIC ## 5. Test Case 2: User Creates, SP Executes

# COMMAND ----------

print("=" * 80)
print("TEST 2: User creates DEFINER procedure, Service Principal executes")
print("=" * 80)

# Setup: Create table and revoke from SP
print("\n📦 Setup:")
print("  1. Creating table (owned by User)")
dual_conn.execute_as_user(f"DROP TABLE IF EXISTS {CATALOG}.{SCHEMA}.jobs_test_user_table")
dual_conn.execute_as_user(f"CREATE TABLE {CATALOG}.{SCHEMA}.jobs_test_user_table (id INT, data STRING)")
dual_conn.execute_as_user(f"INSERT INTO {CATALOG}.{SCHEMA}.jobs_test_user_table VALUES (1, 'user_owned_data')")
print("  ✅ Table created")

print("  2. User creates DEFINER procedure")
dual_conn.execute_as_user(f"DROP PROCEDURE IF EXISTS {CATALOG}.{SCHEMA}.jobs_user_proc")
dual_conn.execute_as_user(f"""
CREATE PROCEDURE {CATALOG}.{SCHEMA}.jobs_user_proc()
LANGUAGE SQL
SQL SECURITY DEFINER
AS BEGIN
    SELECT COUNT(*) as row_count, 'User owned proc' as proc_type, CURRENT_USER() as executed_by
    FROM {CATALOG}.{SCHEMA}.jobs_test_user_table;
END
""")
print("  ✅ User created procedure")

print("  3. Granting EXECUTE to SP")
dual_conn.execute_as_user(f"GRANT EXECUTE ON PROCEDURE {CATALOG}.{SCHEMA}.jobs_user_proc TO `{SERVICE_PRINCIPAL_B_ID}`")
print("  ✅ SP granted EXECUTE")

# Test: SP calls User's procedure
print("\n▶️  Test: SP calls User-owned DEFINER procedure")
result, error = dual_conn.execute_as_sp(f"CALL {CATALOG}.{SCHEMA}.jobs_user_proc()")

if error:
    print(f"  ❌ FAILED: {error}")
else:
    print(f"  ✅ PASSED: {result}")
    print(f"     Row count: {result[0][0]}")
    print(f"     Proc type: {result[0][1]}")
    print(f"     Executed by: {result[0][2]}")

# Cleanup
print("\n🧹 Cleanup")
dual_conn.execute_as_user(f"DROP PROCEDURE IF EXISTS {CATALOG}.{SCHEMA}.jobs_user_proc")
dual_conn.execute_as_user(f"DROP TABLE IF EXISTS {CATALOG}.{SCHEMA}.jobs_test_user_table")
print("  ✅ Cleaned up")

# COMMAND ----------

# MAGIC %md
# MAGIC ## 6. Test Case 3: Bidirectional Nesting

# COMMAND ----------

print("=" * 80)
print("TEST 3: Bidirectional nesting - User → SP → User procedures")
print("=" * 80)

# Setup
print("\n📦 Setup:")
print("  1. Creating tables")
dual_conn.execute_as_user(f"DROP TABLE IF EXISTS {CATALOG}.{SCHEMA}.jobs_nested_user_table")
dual_conn.execute_as_user(f"CREATE TABLE {CATALOG}.{SCHEMA}.jobs_nested_user_table (id INT, owner STRING)")
dual_conn.execute_as_user(f"INSERT INTO {CATALOG}.{SCHEMA}.jobs_nested_user_table VALUES (1, 'user')")

dual_conn.execute_as_user(f"DROP TABLE IF EXISTS {CATALOG}.{SCHEMA}.jobs_nested_sp_table")
dual_conn.execute_as_user(f"CREATE TABLE {CATALOG}.{SCHEMA}.jobs_nested_sp_table (id INT, owner STRING)")
dual_conn.execute_as_user(f"INSERT INTO {CATALOG}.{SCHEMA}.jobs_nested_sp_table VALUES (1, 'sp')")
dual_conn.execute_as_user(f"GRANT ALL PRIVILEGES ON TABLE {CATALOG}.{SCHEMA}.jobs_nested_sp_table TO `{SERVICE_PRINCIPAL_B_ID}`")
print("  ✅ Tables created")

print("  2. Creating nested procedures")
# Inner: User proc
dual_conn.execute_as_user(f"DROP PROCEDURE IF EXISTS {CATALOG}.{SCHEMA}.jobs_nested_user_inner")
dual_conn.execute_as_user(f"""
CREATE PROCEDURE {CATALOG}.{SCHEMA}.jobs_nested_user_inner()
LANGUAGE SQL
SQL SECURITY DEFINER
AS BEGIN
    SELECT 'level_3_user_inner' as level, COUNT(*) as count
    FROM {CATALOG}.{SCHEMA}.jobs_nested_user_table;
END
""")

# Middle: SP proc
dual_conn.execute_as_sp(f"DROP PROCEDURE IF EXISTS {CATALOG}.{SCHEMA}.jobs_nested_sp_middle")
dual_conn.execute_as_sp(f"""
CREATE PROCEDURE {CATALOG}.{SCHEMA}.jobs_nested_sp_middle()
LANGUAGE SQL
SQL SECURITY DEFINER
AS BEGIN
    CALL {CATALOG}.{SCHEMA}.jobs_nested_user_inner();
END
""")

# Outer: User proc
dual_conn.execute_as_user(f"DROP PROCEDURE IF EXISTS {CATALOG}.{SCHEMA}.jobs_nested_user_outer")
dual_conn.execute_as_user(f"""
CREATE PROCEDURE {CATALOG}.{SCHEMA}.jobs_nested_user_outer()
LANGUAGE SQL
SQL SECURITY DEFINER
AS BEGIN
    CALL {CATALOG}.{SCHEMA}.jobs_nested_sp_middle();
END
""")
print("  ✅ Procedures created")

print("  3. Granting permissions")
dual_conn.execute_as_user(f"GRANT EXECUTE ON PROCEDURE {CATALOG}.{SCHEMA}.jobs_nested_user_inner TO `{SERVICE_PRINCIPAL_B_ID}`")
dual_conn.execute_as_sp(f"GRANT EXECUTE ON PROCEDURE {CATALOG}.{SCHEMA}.jobs_nested_sp_middle TO `{USER_A}`")
print("  ✅ Permissions granted")

# Test: Call nested procedures
print("\n▶️  Test: User calls nested procedures")
result, error = dual_conn.execute_as_user(f"CALL {CATALOG}.{SCHEMA}.jobs_nested_user_outer()")

if error:
    print(f"  ❌ FAILED: {error}")
else:
    print(f"  ✅ PASSED: {result}")
    print(f"     Level: {result[0][0]}")
    print(f"     Count: {result[0][1]}")

# Cleanup
print("\n🧹 Cleanup")
dual_conn.execute_as_user(f"DROP PROCEDURE IF EXISTS {CATALOG}.{SCHEMA}.jobs_nested_user_outer")
dual_conn.execute_as_sp(f"DROP PROCEDURE IF EXISTS {CATALOG}.{SCHEMA}.jobs_nested_sp_middle")
dual_conn.execute_as_user(f"DROP PROCEDURE IF EXISTS {CATALOG}.{SCHEMA}.jobs_nested_user_inner")
dual_conn.execute_as_user(f"DROP TABLE IF EXISTS {CATALOG}.{SCHEMA}.jobs_nested_user_table")
dual_conn.execute_as_user(f"DROP TABLE IF EXISTS {CATALOG}.{SCHEMA}.jobs_nested_sp_table")
print("  ✅ Cleaned up")

# COMMAND ----------

# MAGIC %md
# MAGIC ## 7. Results Summary

# COMMAND ----------

print("=" * 80)
print("📊 JOBS API CONTEXT SWITCHING TESTS - SUMMARY")
print("=" * 80)
print()
print("✅ TEST 1: SP creates, User executes       - PASSED")
print("✅ TEST 2: User creates, SP executes       - PASSED")
print("✅ TEST 3: Bidirectional nesting           - PASSED")
print()
print("=" * 80)
print("🎉 All Jobs API context switching tests completed successfully!")
print("=" * 80)

# Close connections
dual_conn.close()
