"""
TC-04 to TC-06: Object Access Boundaries Tests
Tests that validate read/write access control through procedures.
"""

from framework.test_framework import DefinerTestCase, TestExecutor
from framework.config import SERVICE_PRINCIPAL_B_ID, CATALOG, SCHEMA

def get_tests():
    tests = []
    
    # TC-04: Read Access via Procedure Only
    tc04 = DefinerTestCase(
        test_id="TC-04",
        description="Read Access via Procedure Only - Procedure acts as controlled gateway",
        setup_sql=[
            f"DROP TABLE IF EXISTS {CATALOG}.{SCHEMA}.tc04_sensitive_table",
            f"CREATE TABLE {CATALOG}.{SCHEMA}.tc04_sensitive_table (id INT, salary DECIMAL(10,2), name STRING)",
            f"INSERT INTO {CATALOG}.{SCHEMA}.tc04_sensitive_table VALUES (1, 50000.00, 'Alice'), (2, 75000.00, 'Bob'), (3, 60000.00, 'Charlie')",
            
            # Revoke direct table access from service principal
            f"REVOKE ALL PRIVILEGES ON TABLE {CATALOG}.{SCHEMA}.tc04_sensitive_table FROM `{SERVICE_PRINCIPAL_B_ID}`",
            
            # Create DEFINER procedure that provides aggregated view
            f"DROP PROCEDURE IF EXISTS {CATALOG}.{SCHEMA}.tc04_get_avg_salary",
            f"""
            CREATE PROCEDURE {CATALOG}.{SCHEMA}.tc04_get_avg_salary()
            LANGUAGE SQL
            AS BEGIN
                SELECT AVG(salary) as avg_salary, COUNT(*) as employee_count 
                FROM {CATALOG}.{SCHEMA}.tc04_sensitive_table;
            END
            """,
            
            # Grant execute permission to service principal
            f"GRANT EXECUTE ON PROCEDURE {CATALOG}.{SCHEMA}.tc04_get_avg_salary TO `{SERVICE_PRINCIPAL_B_ID}`"
        ],
        test_sql=f"""
            -- Service principal can access aggregated data via procedure
            CALL {CATALOG}.{SCHEMA}.tc04_get_avg_salary()
        """,
        teardown_sql=[
            f"DROP PROCEDURE IF EXISTS {CATALOG}.{SCHEMA}.tc04_get_avg_salary",
            f"DROP TABLE IF EXISTS {CATALOG}.{SCHEMA}.tc04_sensitive_table"
        ]
    )
    tests.append(tc04)
    
    # TC-05: Write Operations Under Impersonation
    tc05 = DefinerTestCase(
        test_id="TC-05",
        description="Write Operations - DEFINER mode allows writes with owner's permissions",
        setup_sql=[
            f"DROP TABLE IF EXISTS {CATALOG}.{SCHEMA}.tc05_audit_log",
            f"CREATE TABLE {CATALOG}.{SCHEMA}.tc05_audit_log (timestamp TIMESTAMP, user_id STRING, action STRING)",
            
            # Revoke write access from service principal
            f"REVOKE INSERT ON TABLE {CATALOG}.{SCHEMA}.tc05_audit_log FROM `{SERVICE_PRINCIPAL_B_ID}`",
            f"REVOKE UPDATE ON TABLE {CATALOG}.{SCHEMA}.tc05_audit_log FROM `{SERVICE_PRINCIPAL_B_ID}`",
            f"REVOKE DELETE ON TABLE {CATALOG}.{SCHEMA}.tc05_audit_log FROM `{SERVICE_PRINCIPAL_B_ID}`",
            
            # Create DEFINER procedure that performs INSERT
            f"DROP PROCEDURE IF EXISTS {CATALOG}.{SCHEMA}.tc05_log_action",
            f"""
            CREATE PROCEDURE {CATALOG}.{SCHEMA}.tc05_log_action(action_name STRING)
            LANGUAGE SQL
            AS BEGIN
                INSERT INTO {CATALOG}.{SCHEMA}.tc05_audit_log 
                VALUES (CURRENT_TIMESTAMP(), CURRENT_USER(), action_name);
                
                SELECT COUNT(*) as log_count FROM {CATALOG}.{SCHEMA}.tc05_audit_log;
            END
            """,
            
            # Grant execute permission
            f"GRANT EXECUTE ON PROCEDURE {CATALOG}.{SCHEMA}.tc05_log_action TO `{SERVICE_PRINCIPAL_B_ID}`"
        ],
        test_sql=f"CALL {CATALOG}.{SCHEMA}.tc05_log_action('test_insert')",
        teardown_sql=[
            f"DROP PROCEDURE IF EXISTS {CATALOG}.{SCHEMA}.tc05_log_action",
            f"DROP TABLE IF EXISTS {CATALOG}.{SCHEMA}.tc05_audit_log"
        ]
    )
    tests.append(tc05)
    
    # TC-06: DDL Execution Restrictions
    tc06 = DefinerTestCase(
        test_id="TC-06",
        description="DDL Execution - Validate DDL operations in procedures",
        setup_sql=[
            # Create DEFINER procedure that attempts DDL
            f"DROP PROCEDURE IF EXISTS {CATALOG}.{SCHEMA}.tc06_create_table_proc",
            f"""
            CREATE PROCEDURE {CATALOG}.{SCHEMA}.tc06_create_table_proc()
            LANGUAGE SQL
            AS BEGIN
                -- Try to create a table inside procedure
                CREATE TABLE IF NOT EXISTS {CATALOG}.{SCHEMA}.tc06_dynamic_table (id INT, data STRING);
                INSERT INTO {CATALOG}.{SCHEMA}.tc06_dynamic_table VALUES (1, 'test');
                SELECT COUNT(*) as row_count FROM {CATALOG}.{SCHEMA}.tc06_dynamic_table;
            END
            """,
            
            f"GRANT EXECUTE ON PROCEDURE {CATALOG}.{SCHEMA}.tc06_create_table_proc TO `{SERVICE_PRINCIPAL_B_ID}`"
        ],
        test_sql=f"CALL {CATALOG}.{SCHEMA}.tc06_create_table_proc()",
        teardown_sql=[
            f"DROP PROCEDURE IF EXISTS {CATALOG}.{SCHEMA}.tc06_create_table_proc",
            f"DROP TABLE IF EXISTS {CATALOG}.{SCHEMA}.tc06_dynamic_table"
        ]
    )
    tests.append(tc06)
    
    return tests


if __name__ == "__main__":
    from framework.test_framework import DatabricksConnection, TestReporter
    from framework.config import SERVER_HOSTNAME, HTTP_PATH, PAT_TOKEN, CATALOG, SCHEMA
    
    print("=" * 80)
    print("🧪 Object Access Boundaries Test Suite (TC-04 to TC-06)")
    print("=" * 80)
    print()
    
    conn = DatabricksConnection(SERVER_HOSTNAME, HTTP_PATH, PAT_TOKEN, CATALOG, SCHEMA)
    executor = TestExecutor(conn)
    
    test_cases = get_tests()
    results = [executor.run_test(tc) for tc in test_cases]
    
    reporter = TestReporter(results)
    reporter.print_summary()
    
    conn.close()
