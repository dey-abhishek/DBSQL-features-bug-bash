"""
TC-20: Compliance & Regression Tests
Tests that validate consistent behavior across versions and upgrades.
"""

from framework.test_framework import DefinerTestCase, TestExecutor
from framework.config import SERVICE_PRINCIPAL_B_ID, CATALOG, SCHEMA

def get_tests():
    tests = []
    
    # TC-20: Upgrade Regression
    tc20 = DefinerTestCase(
        test_id="TC-20",
        description="Upgrade Regression - Ensure consistent impersonation semantics across versions",
        setup_sql=[
            f"DROP TABLE IF EXISTS {CATALOG}.{SCHEMA}.tc20_regression_table",
            f"CREATE TABLE {CATALOG}.{SCHEMA}.tc20_regression_table (id INT, test_data STRING, version_info STRING)",
            f"INSERT INTO {CATALOG}.{SCHEMA}.tc20_regression_table VALUES (1, 'baseline_data', 'v1.0')",
            
            # Create DEFINER procedure
            f"DROP PROCEDURE IF EXISTS {CATALOG}.{SCHEMA}.tc20_definer_regression",
            f"""
            CREATE PROCEDURE {CATALOG}.{SCHEMA}.tc20_definer_regression()
            LANGUAGE SQL
            AS BEGIN
                -- Test basic DEFINER behavior
                SELECT 
                    COUNT(*) as row_count,
                    CURRENT_USER() as execution_user,
                    CURRENT_CATALOG() as catalog,
                    CURRENT_SCHEMA() as schema
                FROM {CATALOG}.{SCHEMA}.tc20_regression_table;
            END
            """,
            
            # Create INVOKER procedure
            f"DROP PROCEDURE IF EXISTS {CATALOG}.{SCHEMA}.tc20_invoker_regression",
            f"""
            CREATE PROCEDURE {CATALOG}.{SCHEMA}.tc20_invoker_regression()
            LANGUAGE SQL
            SQL SECURITY INVOKER
            AS BEGIN
                -- Test basic INVOKER behavior
                SELECT 
                    COUNT(*) as row_count,
                    CURRENT_USER() as execution_user
                FROM {CATALOG}.{SCHEMA}.tc20_regression_table;
            END
            """,
            
            f"GRANT EXECUTE ON PROCEDURE {CATALOG}.{SCHEMA}.tc20_definer_regression TO `{SERVICE_PRINCIPAL_B_ID}`",
            f"GRANT EXECUTE ON PROCEDURE {CATALOG}.{SCHEMA}.tc20_invoker_regression TO `{SERVICE_PRINCIPAL_B_ID}`",
            f"GRANT SELECT ON TABLE {CATALOG}.{SCHEMA}.tc20_regression_table TO `{SERVICE_PRINCIPAL_B_ID}`"
        ],
        test_sql=f"""
            CALL {CATALOG}.{SCHEMA}.tc20_definer_regression()
        """,
        teardown_sql=[
            f"DROP PROCEDURE IF EXISTS {CATALOG}.{SCHEMA}.tc20_invoker_regression",
            f"DROP PROCEDURE IF EXISTS {CATALOG}.{SCHEMA}.tc20_definer_regression",
            f"DROP TABLE IF EXISTS {CATALOG}.{SCHEMA}.tc20_regression_table"
        ]
    )
    tests.append(tc20)
    
    return tests


if __name__ == "__main__":
    from framework.test_framework import DatabricksConnection, TestReporter
    from framework.config import SERVER_HOSTNAME, HTTP_PATH, PAT_TOKEN, CATALOG, SCHEMA
    
    print("=" * 80)
    print("🧪 Compliance & Regression Test Suite (TC-20)")
    print("=" * 80)
    print()
    
    conn = DatabricksConnection(SERVER_HOSTNAME, HTTP_PATH, PAT_TOKEN, CATALOG, SCHEMA)
    executor = TestExecutor(conn)
    
    test_cases = get_tests()
    results = [executor.run_test(tc) for tc in test_cases]
    
    reporter = TestReporter(results)
    reporter.print_summary()
    
    conn.close()
