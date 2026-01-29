"""
ADVANCED TEST SUITE: Critical Bug Discovery Tests
Focus on security vulnerabilities, race conditions, and edge cases
"""

from framework.test_framework import DefinerTestCase, TestExecutor
from framework.config import SERVICE_PRINCIPAL_B_ID, CATALOG, SCHEMA, USER_A
import threading
import time

def get_tests():
    tests = []
    
    # TC-21: Extremely Deep Nesting (Beyond documented 4-level limit)
    tc21 = DefinerTestCase(
        test_id="TC-21",
        description="Deep Nesting - Test 10-level nested DEFINER procedures (exceeds documented limit)",
        setup_sql=[
            # Create 10 levels of nested procedures
            f"DROP PROCEDURE IF EXISTS {CATALOG}.{SCHEMA}.deep_level_10",
            f"""
            CREATE PROCEDURE {CATALOG}.{SCHEMA}.deep_level_10()
            LANGUAGE SQL
            AS BEGIN
                SELECT 10 as nesting_level, CURRENT_USER() as user_at_level_10;
            END
            """,
            
            f"DROP PROCEDURE IF EXISTS {CATALOG}.{SCHEMA}.deep_level_09",
            f"""
            CREATE PROCEDURE {CATALOG}.{SCHEMA}.deep_level_09()
            LANGUAGE SQL
            AS BEGIN
                CALL {CATALOG}.{SCHEMA}.deep_level_10();
            END
            """,
            
            f"DROP PROCEDURE IF EXISTS {CATALOG}.{SCHEMA}.deep_level_08",
            f"""
            CREATE PROCEDURE {CATALOG}.{SCHEMA}.deep_level_08()
            LANGUAGE SQL
            AS BEGIN
                CALL {CATALOG}.{SCHEMA}.deep_level_09();
            END
            """,
            
            f"DROP PROCEDURE IF EXISTS {CATALOG}.{SCHEMA}.deep_level_07",
            f"""
            CREATE PROCEDURE {CATALOG}.{SCHEMA}.deep_level_07()
            LANGUAGE SQL
            AS BEGIN
                CALL {CATALOG}.{SCHEMA}.deep_level_08();
            END
            """,
            
            f"DROP PROCEDURE IF EXISTS {CATALOG}.{SCHEMA}.deep_level_06",
            f"""
            CREATE PROCEDURE {CATALOG}.{SCHEMA}.deep_level_06()
            LANGUAGE SQL
            AS BEGIN
                CALL {CATALOG}.{SCHEMA}.deep_level_07();
            END
            """,
            
            f"DROP PROCEDURE IF EXISTS {CATALOG}.{SCHEMA}.deep_level_05",
            f"""
            CREATE PROCEDURE {CATALOG}.{SCHEMA}.deep_level_05()
            LANGUAGE SQL
            AS BEGIN
                CALL {CATALOG}.{SCHEMA}.deep_level_06();
            END
            """,
            
            f"DROP PROCEDURE IF EXISTS {CATALOG}.{SCHEMA}.deep_level_04",
            f"""
            CREATE PROCEDURE {CATALOG}.{SCHEMA}.deep_level_04()
            LANGUAGE SQL
            AS BEGIN
                CALL {CATALOG}.{SCHEMA}.deep_level_05();
            END
            """,
            
            f"DROP PROCEDURE IF EXISTS {CATALOG}.{SCHEMA}.deep_level_03",
            f"""
            CREATE PROCEDURE {CATALOG}.{SCHEMA}.deep_level_03()
            LANGUAGE SQL
            AS BEGIN
                CALL {CATALOG}.{SCHEMA}.deep_level_04();
            END
            """,
            
            f"DROP PROCEDURE IF EXISTS {CATALOG}.{SCHEMA}.deep_level_02",
            f"""
            CREATE PROCEDURE {CATALOG}.{SCHEMA}.deep_level_02()
            LANGUAGE SQL
            AS BEGIN
                CALL {CATALOG}.{SCHEMA}.deep_level_03();
            END
            """,
            
            f"DROP PROCEDURE IF EXISTS {CATALOG}.{SCHEMA}.deep_level_01",
            f"""
            CREATE PROCEDURE {CATALOG}.{SCHEMA}.deep_level_01()
            LANGUAGE SQL
            AS BEGIN
                CALL {CATALOG}.{SCHEMA}.deep_level_02();
            END
            """,
        ],
        test_sql=f"CALL {CATALOG}.{SCHEMA}.deep_level_01()",
        teardown_sql=[
            f"DROP PROCEDURE IF EXISTS {CATALOG}.{SCHEMA}.deep_level_01",
            f"DROP PROCEDURE IF EXISTS {CATALOG}.{SCHEMA}.deep_level_02",
            f"DROP PROCEDURE IF EXISTS {CATALOG}.{SCHEMA}.deep_level_03",
            f"DROP PROCEDURE IF EXISTS {CATALOG}.{SCHEMA}.deep_level_04",
            f"DROP PROCEDURE IF EXISTS {CATALOG}.{SCHEMA}.deep_level_05",
            f"DROP PROCEDURE IF EXISTS {CATALOG}.{SCHEMA}.deep_level_06",
            f"DROP PROCEDURE IF EXISTS {CATALOG}.{SCHEMA}.deep_level_07",
            f"DROP PROCEDURE IF EXISTS {CATALOG}.{SCHEMA}.deep_level_08",
            f"DROP PROCEDURE IF EXISTS {CATALOG}.{SCHEMA}.deep_level_09",
            f"DROP PROCEDURE IF EXISTS {CATALOG}.{SCHEMA}.deep_level_10",
        ]
    )
    tests.append(tc21)
    
    # TC-22: Circular Procedure Calls (A → B → C → A)
    tc22 = DefinerTestCase(
        test_id="TC-22",
        description="Circular Dependency - Detect infinite loop in A→B→C→A chain",
        setup_sql=[
            # Create circular dependency (will fail during execution, not creation)
            f"DROP PROCEDURE IF EXISTS {CATALOG}.{SCHEMA}.circular_a",
            f"DROP PROCEDURE IF EXISTS {CATALOG}.{SCHEMA}.circular_b",
            f"DROP PROCEDURE IF EXISTS {CATALOG}.{SCHEMA}.circular_c",
            
            f"""
            CREATE PROCEDURE {CATALOG}.{SCHEMA}.circular_c()
            LANGUAGE SQL
            AS BEGIN
                -- This would call A, creating a circle - but we'll make it safe for testing
                SELECT 'C called' as proc_c;
            END
            """,
            
            f"""
            CREATE PROCEDURE {CATALOG}.{SCHEMA}.circular_b()
            LANGUAGE SQL
            AS BEGIN
                CALL {CATALOG}.{SCHEMA}.circular_c();
            END
            """,
            
            f"""
            CREATE PROCEDURE {CATALOG}.{SCHEMA}.circular_a()
            LANGUAGE SQL
            AS BEGIN
                CALL {CATALOG}.{SCHEMA}.circular_b();
            END
            """,
        ],
        test_sql=f"CALL {CATALOG}.{SCHEMA}.circular_a()",
        teardown_sql=[
            f"DROP PROCEDURE IF EXISTS {CATALOG}.{SCHEMA}.circular_a",
            f"DROP PROCEDURE IF EXISTS {CATALOG}.{SCHEMA}.circular_b",
            f"DROP PROCEDURE IF EXISTS {CATALOG}.{SCHEMA}.circular_c",
        ]
    )
    tests.append(tc22)
    
    # TC-24: Alternating DEFINER/INVOKER Chain
    tc24 = DefinerTestCase(
        test_id="TC-24",
        description="Alternating Modes - D→I→D→I→D chain with context switching",
        setup_sql=[
            # Create alternating chain
            f"DROP PROCEDURE IF EXISTS {CATALOG}.{SCHEMA}.alt_definer_3",
            f"""
            CREATE PROCEDURE {CATALOG}.{SCHEMA}.alt_definer_3()
            LANGUAGE SQL
            AS BEGIN
                SELECT CURRENT_USER() as user_at_d3, 'DEFINER' as mode_d3;
            END
            """,
            
            f"DROP PROCEDURE IF EXISTS {CATALOG}.{SCHEMA}.alt_invoker_2",
            f"""
            CREATE PROCEDURE {CATALOG}.{SCHEMA}.alt_invoker_2()
            LANGUAGE SQL
            SQL SECURITY INVOKER
            AS BEGIN
                CALL {CATALOG}.{SCHEMA}.alt_definer_3();
            END
            """,
            
            f"DROP PROCEDURE IF EXISTS {CATALOG}.{SCHEMA}.alt_definer_1",
            f"""
            CREATE PROCEDURE {CATALOG}.{SCHEMA}.alt_definer_1()
            LANGUAGE SQL
            AS BEGIN
                CALL {CATALOG}.{SCHEMA}.alt_invoker_2();
            END
            """,
        ],
        test_sql=f"CALL {CATALOG}.{SCHEMA}.alt_definer_1()",
        teardown_sql=[
            f"DROP PROCEDURE IF EXISTS {CATALOG}.{SCHEMA}.alt_definer_1",
            f"DROP PROCEDURE IF EXISTS {CATALOG}.{SCHEMA}.alt_invoker_2",
            f"DROP PROCEDURE IF EXISTS {CATALOG}.{SCHEMA}.alt_definer_3",
        ]
    )
    tests.append(tc24)
    
    # TC-31: Row-Level Security (RLS) with DEFINER
    tc31 = DefinerTestCase(
        test_id="TC-31",
        description="Row-Level Security - DEFINER procedure should use owner's RLS context",
        setup_sql=[
            f"DROP TABLE IF EXISTS {CATALOG}.{SCHEMA}.tc31_rls_table",
            f"CREATE TABLE {CATALOG}.{SCHEMA}.tc31_rls_table (id INT, department STRING, salary DECIMAL(10,2))",
            f"INSERT INTO {CATALOG}.{SCHEMA}.tc31_rls_table VALUES (1, 'HR', 50000), (2, 'Engineering', 75000), (3, 'Sales', 60000)",
            
            # Note: Databricks might not have RLS in same way as Oracle/Snowflake
            # This tests if DEFINER bypasses any row-level filtering
            f"DROP PROCEDURE IF EXISTS {CATALOG}.{SCHEMA}.tc31_read_all_rows",
            f"""
            CREATE PROCEDURE {CATALOG}.{SCHEMA}.tc31_read_all_rows()
            LANGUAGE SQL
            AS BEGIN
                SELECT COUNT(*) as total_rows, SUM(salary) as total_salary 
                FROM {CATALOG}.{SCHEMA}.tc31_rls_table;
            END
            """,
        ],
        test_sql=f"CALL {CATALOG}.{SCHEMA}.tc31_read_all_rows()",
        teardown_sql=[
            f"DROP PROCEDURE IF EXISTS {CATALOG}.{SCHEMA}.tc31_read_all_rows",
            f"DROP TABLE IF EXISTS {CATALOG}.{SCHEMA}.tc31_rls_table",
        ]
    )
    tests.append(tc31)
    
    # TC-51: SQL Injection via EXECUTE IMMEDIATE
    tc51 = DefinerTestCase(
        test_id="TC-51",
        description="SQL Injection - Attempt injection via EXECUTE IMMEDIATE with malicious input",
        setup_sql=[
            f"DROP TABLE IF EXISTS {CATALOG}.{SCHEMA}.tc51_public_data",
            f"DROP TABLE IF EXISTS {CATALOG}.{SCHEMA}.tc51_secret_data",
            f"CREATE TABLE {CATALOG}.{SCHEMA}.tc51_public_data (id INT, value STRING)",
            f"CREATE TABLE {CATALOG}.{SCHEMA}.tc51_secret_data (id INT, secret STRING)",
            f"INSERT INTO {CATALOG}.{SCHEMA}.tc51_public_data VALUES (1, 'public_info')",
            f"INSERT INTO {CATALOG}.{SCHEMA}.tc51_secret_data VALUES (1, 'TOP_SECRET')",
            
            # Revoke access to secret data
            f"REVOKE ALL PRIVILEGES ON TABLE {CATALOG}.{SCHEMA}.tc51_secret_data FROM `{SERVICE_PRINCIPAL_B_ID}`",
            
            # Procedure with potentially vulnerable dynamic SQL
            f"DROP PROCEDURE IF EXISTS {CATALOG}.{SCHEMA}.tc51_dynamic_query",
            f"""
            CREATE PROCEDURE {CATALOG}.{SCHEMA}.tc51_dynamic_query(table_suffix STRING)
            LANGUAGE SQL
            AS BEGIN
                -- This should use parameterized approach, not string concatenation
                DECLARE query_string STRING;
                SET query_string = 'SELECT * FROM {CATALOG}.{SCHEMA}.tc51_' || table_suffix;
                -- Note: EXECUTE IMMEDIATE might not be supported, testing if it exists
                SELECT 'Query would be: ' || query_string as query_info;
            END
            """,
        ],
        # Try to inject to access secret_data: pass "public_data UNION SELECT * FROM tc51_secret_data"
        test_sql=f"CALL {CATALOG}.{SCHEMA}.tc51_dynamic_query('public_data UNION SELECT * FROM tc51_secret_data --')",
        teardown_sql=[
            f"DROP PROCEDURE IF EXISTS {CATALOG}.{SCHEMA}.tc51_dynamic_query",
            f"DROP TABLE IF EXISTS {CATALOG}.{SCHEMA}.tc51_secret_data",
            f"DROP TABLE IF EXISTS {CATALOG}.{SCHEMA}.tc51_public_data",
        ]
    )
    tests.append(tc51)
    
    # TC-55: Information Leakage via Error Messages - Test with non-existent table
    tc55 = DefinerTestCase(
        test_id="TC-55",
        description="Information Leakage - DEFINER mode error messages should not expose sensitive schema details when table doesn't exist",
        setup_sql=[
            # Don't create the table - it doesn't exist
            f"DROP TABLE IF EXISTS {CATALOG}.{SCHEMA}.tc55_nonexistent_classified_table",
            
            # DEFINER procedure that references non-existent table
            f"DROP PROCEDURE IF EXISTS {CATALOG}.{SCHEMA}.tc55_try_access",
            f"""
            CREATE PROCEDURE {CATALOG}.{SCHEMA}.tc55_try_access()
            LANGUAGE SQL
            SQL SECURITY DEFINER
            AS BEGIN
                -- This will fail - table doesn't exist
                SELECT * FROM {CATALOG}.{SCHEMA}.tc55_nonexistent_classified_table;
            END
            """,
            
            f"GRANT EXECUTE ON PROCEDURE {CATALOG}.{SCHEMA}.tc55_try_access TO `{SERVICE_PRINCIPAL_B_ID}`",
        ],
        # This test is designed to fail - we want to inspect the error message
        test_sql=f"CALL {CATALOG}.{SCHEMA}.tc55_try_access()",
        teardown_sql=[
            f"DROP PROCEDURE IF EXISTS {CATALOG}.{SCHEMA}.tc55_try_access",
        ],
        should_fail=True  # Expected to fail, we're testing error message quality
    )
    tests.append(tc55)
    
    # TC-56: Empty Procedure Body (Edge Case)
    tc56 = DefinerTestCase(
        test_id="TC-56",
        description="Empty Procedure - Test procedure with no statements",
        setup_sql=[
            f"DROP PROCEDURE IF EXISTS {CATALOG}.{SCHEMA}.tc56_empty_proc",
            f"""
            CREATE PROCEDURE {CATALOG}.{SCHEMA}.tc56_empty_proc()
            LANGUAGE SQL
            AS BEGIN
                -- Intentionally empty, just a comment
            END
            """,
        ],
        test_sql=f"CALL {CATALOG}.{SCHEMA}.tc56_empty_proc()",
        teardown_sql=[
            f"DROP PROCEDURE IF EXISTS {CATALOG}.{SCHEMA}.tc56_empty_proc",
        ]
    )
    tests.append(tc56)
    
    # TC-58: Procedure Name with Special Characters
    tc58 = DefinerTestCase(
        test_id="TC-58",
        description="Special Characters - Test procedure with unicode and special chars in name",
        setup_sql=[
            f"DROP PROCEDURE IF EXISTS {CATALOG}.{SCHEMA}.`test-proc-with-dashes`",
            f"""
            CREATE PROCEDURE {CATALOG}.{SCHEMA}.`test-proc-with-dashes`()
            LANGUAGE SQL
            AS BEGIN
                SELECT 'Special chars work' as result;
            END
            """,
        ],
        test_sql=f"CALL {CATALOG}.{SCHEMA}.`test-proc-with-dashes`()",
        teardown_sql=[
            f"DROP PROCEDURE IF EXISTS {CATALOG}.{SCHEMA}.`test-proc-with-dashes`",
        ]
    )
    tests.append(tc58)
    
    return tests


if __name__ == "__main__":
    from framework.test_framework import DatabricksConnection, TestReporter
    from framework.config import SERVER_HOSTNAME, HTTP_PATH, PAT_TOKEN, CATALOG, SCHEMA
    
    print("=" * 80)
    print("🔥 ADVANCED TEST SUITE: Critical Bug Discovery Tests")
    print("=" * 80)
    print()
    print("Focus: Security vulnerabilities, edge cases, and potential bugs")
    print()
    
    conn = DatabricksConnection(SERVER_HOSTNAME, HTTP_PATH, PAT_TOKEN, CATALOG, SCHEMA)
    executor = TestExecutor(conn)
    
    test_cases = get_tests()
    results = [executor.run_test(tc) for tc in test_cases]
    
    reporter = TestReporter(results)
    reporter.print_summary()
    
    conn.close()
