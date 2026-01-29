"""
TC-07 to TC-08: Nested & Chained Procedures Tests
Tests that validate identity propagation and privilege handling in nested procedure calls.
"""

from framework.test_framework import DefinerTestCase, TestExecutor
from framework.config import SERVICE_PRINCIPAL_B_ID, CATALOG, SCHEMA

def get_tests():
    tests = []
    
    # TC-07: Nested Procedure Identity Propagation
    tc07 = DefinerTestCase(
        test_id="TC-07",
        description="Nested Procedure Identity - Verify identity across procedure calls",
        setup_sql=[
            # Create inner DEFINER procedure
            f"DROP PROCEDURE IF EXISTS {CATALOG}.{SCHEMA}.tc07_inner_proc",
            f"""
            CREATE PROCEDURE {CATALOG}.{SCHEMA}.tc07_inner_proc()
            LANGUAGE SQL
            AS BEGIN
                SELECT CURRENT_USER() as inner_user;
            END
            """,
            
            # Create outer INVOKER procedure that calls inner
            f"DROP PROCEDURE IF EXISTS {CATALOG}.{SCHEMA}.tc07_outer_proc",
            f"""
            CREATE PROCEDURE {CATALOG}.{SCHEMA}.tc07_outer_proc()
            LANGUAGE SQL
            SQL SECURITY INVOKER
            AS BEGIN
                CALL {CATALOG}.{SCHEMA}.tc07_inner_proc();
            END
            """,
            
            # Grant execute permissions
            f"GRANT EXECUTE ON PROCEDURE {CATALOG}.{SCHEMA}.tc07_inner_proc TO `{SERVICE_PRINCIPAL_B_ID}`",
            f"GRANT EXECUTE ON PROCEDURE {CATALOG}.{SCHEMA}.tc07_outer_proc TO `{SERVICE_PRINCIPAL_B_ID}`"
        ],
        test_sql=f"CALL {CATALOG}.{SCHEMA}.tc07_outer_proc()",
        teardown_sql=[
            f"DROP PROCEDURE IF EXISTS {CATALOG}.{SCHEMA}.tc07_outer_proc",
            f"DROP PROCEDURE IF EXISTS {CATALOG}.{SCHEMA}.tc07_inner_proc"
        ]
    )
    tests.append(tc07)
    
    # TC-08: Mixed Security Modes
    tc08 = DefinerTestCase(
        test_id="TC-08",
        description="Mixed Security Modes - Prevent privilege escalation",
        setup_sql=[
            # Create restricted table
            f"DROP TABLE IF EXISTS {CATALOG}.{SCHEMA}.tc08_restricted_table",
            f"CREATE TABLE {CATALOG}.{SCHEMA}.tc08_restricted_table (id INT, secret STRING)",
            f"INSERT INTO {CATALOG}.{SCHEMA}.tc08_restricted_table VALUES (1, 'confidential_data')",
            
            # Revoke access from service principal
            f"REVOKE ALL PRIVILEGES ON TABLE {CATALOG}.{SCHEMA}.tc08_restricted_table FROM `{SERVICE_PRINCIPAL_B_ID}`",
            
            # Create DEFINER procedure that accesses restricted table
            f"DROP PROCEDURE IF EXISTS {CATALOG}.{SCHEMA}.tc08_definer_read",
            f"""
            CREATE PROCEDURE {CATALOG}.{SCHEMA}.tc08_definer_read()
            LANGUAGE SQL
            AS BEGIN
                SELECT COUNT(*) as secret_count FROM {CATALOG}.{SCHEMA}.tc08_restricted_table;
            END
            """,
            
            # Create INVOKER procedure that tries to call DEFINER procedure
            f"DROP PROCEDURE IF EXISTS {CATALOG}.{SCHEMA}.tc08_invoker_caller",
            f"""
            CREATE PROCEDURE {CATALOG}.{SCHEMA}.tc08_invoker_caller()
            LANGUAGE SQL
            SQL SECURITY INVOKER
            AS BEGIN
                CALL {CATALOG}.{SCHEMA}.tc08_definer_read();
            END
            """,
            
            # Grant execute permissions
            f"GRANT EXECUTE ON PROCEDURE {CATALOG}.{SCHEMA}.tc08_definer_read TO `{SERVICE_PRINCIPAL_B_ID}`",
            f"GRANT EXECUTE ON PROCEDURE {CATALOG}.{SCHEMA}.tc08_invoker_caller TO `{SERVICE_PRINCIPAL_B_ID}`"
        ],
        test_sql=f"CALL {CATALOG}.{SCHEMA}.tc08_invoker_caller()",
        teardown_sql=[
            f"DROP PROCEDURE IF EXISTS {CATALOG}.{SCHEMA}.tc08_invoker_caller",
            f"DROP PROCEDURE IF EXISTS {CATALOG}.{SCHEMA}.tc08_definer_read",
            f"DROP TABLE IF EXISTS {CATALOG}.{SCHEMA}.tc08_restricted_table"
        ]
    )
    tests.append(tc08)
    
    return tests


if __name__ == "__main__":
    from framework.test_framework import DatabricksConnection, TestReporter
    from framework.config import SERVER_HOSTNAME, HTTP_PATH, PAT_TOKEN, CATALOG, SCHEMA
    
    print("=" * 80)
    print("🧪 Nested & Chained Procedures Test Suite (TC-07 to TC-08)")
    print("=" * 80)
    print()
    
    conn = DatabricksConnection(SERVER_HOSTNAME, HTTP_PATH, PAT_TOKEN, CATALOG, SCHEMA)
    executor = TestExecutor(conn)
    
    test_cases = get_tests()
    results = [executor.run_test(tc) for tc in test_cases]
    
    reporter = TestReporter(results)
    reporter.print_summary()
    
    conn.close()
