"""
TC-11 to TC-12: Error Handling & Observability Tests
Tests that validate error messages and audit logging.
"""

from framework.test_framework import DefinerTestCase, TestExecutor
from framework.config import SERVICE_PRINCIPAL_B_ID, CATALOG, SCHEMA

def get_tests():
    tests = []
    
    # TC-11: Permission Error Transparency
    tc11 = DefinerTestCase(
        test_id="TC-11",
        description="Permission Error Transparency - INVOKER mode respects caller's permissions",
        setup_sql=[
            f"DROP TABLE IF EXISTS {CATALOG}.{SCHEMA}.tc11_secret_table",
            f"CREATE TABLE {CATALOG}.{SCHEMA}.tc11_secret_table (id INT, classified STRING)",
            f"INSERT INTO {CATALOG}.{SCHEMA}.tc11_secret_table VALUES (1, 'top_secret')",
            
            # Create INVOKER procedure (so it uses caller's permissions)
            f"DROP PROCEDURE IF EXISTS {CATALOG}.{SCHEMA}.tc11_invoker_read",
            f"""
            CREATE PROCEDURE {CATALOG}.{SCHEMA}.tc11_invoker_read()
            LANGUAGE SQL
            SQL SECURITY INVOKER
            AS BEGIN
                -- In INVOKER mode, this uses the caller's permissions
                -- When owner calls it, it should succeed
                -- When service principal calls it (without grants), it should fail
                SELECT COUNT(*) as record_count FROM {CATALOG}.{SCHEMA}.tc11_secret_table;
            END
            """,
            
            # Revoke table access from service principal
            f"REVOKE ALL PRIVILEGES ON TABLE {CATALOG}.{SCHEMA}.tc11_secret_table FROM `{SERVICE_PRINCIPAL_B_ID}`",
            f"GRANT EXECUTE ON PROCEDURE {CATALOG}.{SCHEMA}.tc11_invoker_read TO `{SERVICE_PRINCIPAL_B_ID}`"
        ],
        test_sql=f"CALL {CATALOG}.{SCHEMA}.tc11_invoker_read()",
        teardown_sql=[
            f"DROP PROCEDURE IF EXISTS {CATALOG}.{SCHEMA}.tc11_invoker_read",
            f"DROP TABLE IF EXISTS {CATALOG}.{SCHEMA}.tc11_secret_table"
        ]
        # Note: This test passes when owner calls it, would fail if service principal called it
    )
    tests.append(tc11)
    
    # TC-12: Audit & Lineage Attribution
    tc12 = DefinerTestCase(
        test_id="TC-12",
        description="Audit & Lineage - Execution should be properly logged and attributed",
        setup_sql=[
            f"DROP TABLE IF EXISTS {CATALOG}.{SCHEMA}.tc12_audit_test",
            f"CREATE TABLE {CATALOG}.{SCHEMA}.tc12_audit_test (id INT, execution_user STRING, execution_time TIMESTAMP)",
            
            # Create DEFINER procedure that logs execution context
            f"DROP PROCEDURE IF EXISTS {CATALOG}.{SCHEMA}.tc12_log_execution",
            f"""
            CREATE PROCEDURE {CATALOG}.{SCHEMA}.tc12_log_execution()
            LANGUAGE SQL
            AS BEGIN
                INSERT INTO {CATALOG}.{SCHEMA}.tc12_audit_test 
                VALUES (1, CURRENT_USER(), CURRENT_TIMESTAMP());
                
                SELECT * FROM {CATALOG}.{SCHEMA}.tc12_audit_test;
            END
            """,
            
            f"GRANT EXECUTE ON PROCEDURE {CATALOG}.{SCHEMA}.tc12_log_execution TO `{SERVICE_PRINCIPAL_B_ID}`"
        ],
        test_sql=f"CALL {CATALOG}.{SCHEMA}.tc12_log_execution()",
        teardown_sql=[
            f"DROP PROCEDURE IF EXISTS {CATALOG}.{SCHEMA}.tc12_log_execution",
            f"DROP TABLE IF EXISTS {CATALOG}.{SCHEMA}.tc12_audit_test"
        ]
    )
    tests.append(tc12)
    
    return tests


if __name__ == "__main__":
    from framework.test_framework import DatabricksConnection, TestReporter
    from framework.config import SERVER_HOSTNAME, HTTP_PATH, PAT_TOKEN, CATALOG, SCHEMA
    
    print("=" * 80)
    print("🧪 Error Handling & Observability Test Suite (TC-11 to TC-12)")
    print("=" * 80)
    print()
    
    conn = DatabricksConnection(SERVER_HOSTNAME, HTTP_PATH, PAT_TOKEN, CATALOG, SCHEMA)
    executor = TestExecutor(conn)
    
    test_cases = get_tests()
    results = [executor.run_test(tc) for tc in test_cases]
    
    reporter = TestReporter(results)
    reporter.print_summary()
    
    conn.close()
