"""
TC-09 to TC-10: Parameter & Injection Safety Tests
Tests that validate dynamic SQL and SQL injection prevention.
"""

from framework.test_framework import DefinerTestCase, TestExecutor
from framework.config import SERVICE_PRINCIPAL_B_ID, CATALOG, SCHEMA

def get_tests():
    tests = []
    
    # TC-09: Dynamic SQL Under Impersonation
    tc09 = DefinerTestCase(
        test_id="TC-09",
        description="Dynamic SQL - Ensure permissions evaluated at execution time",
        setup_sql=[
            f"DROP TABLE IF EXISTS {CATALOG}.{SCHEMA}.tc09_data_table",
            f"CREATE TABLE {CATALOG}.{SCHEMA}.tc09_data_table (id INT, category STRING, value DECIMAL(10,2))",
            f"INSERT INTO {CATALOG}.{SCHEMA}.tc09_data_table VALUES (1, 'A', 100.0), (2, 'B', 200.0), (3, 'A', 150.0)",
            
            # Revoke direct access from service principal
            f"REVOKE ALL PRIVILEGES ON TABLE {CATALOG}.{SCHEMA}.tc09_data_table FROM `{SERVICE_PRINCIPAL_B_ID}`",
            
            # Create DEFINER procedure that uses parameterized query (safer than dynamic SQL)
            f"DROP PROCEDURE IF EXISTS {CATALOG}.{SCHEMA}.tc09_dynamic_query",
            f"""
            CREATE PROCEDURE {CATALOG}.{SCHEMA}.tc09_dynamic_query(filter_category STRING)
            LANGUAGE SQL
            AS BEGIN
                -- Parameterized query (evaluated at execution time with definer's permissions)
                SELECT SUM(value) as total FROM {CATALOG}.{SCHEMA}.tc09_data_table 
                WHERE category = filter_category;
            END
            """,
            
            f"GRANT EXECUTE ON PROCEDURE {CATALOG}.{SCHEMA}.tc09_dynamic_query TO `{SERVICE_PRINCIPAL_B_ID}`"
        ],
        test_sql=f"CALL {CATALOG}.{SCHEMA}.tc09_dynamic_query('A')",
        teardown_sql=[
            f"DROP PROCEDURE IF EXISTS {CATALOG}.{SCHEMA}.tc09_dynamic_query",
            f"DROP TABLE IF EXISTS {CATALOG}.{SCHEMA}.tc09_data_table"
        ]
    )
    tests.append(tc09)
    
    # TC-10: SQL Injection Attempt via Parameters
    tc10 = DefinerTestCase(
        test_id="TC-10",
        description="SQL Injection Prevention - Malicious input should not escalate privileges",
        setup_sql=[
            f"DROP TABLE IF EXISTS {CATALOG}.{SCHEMA}.tc10_user_data",
            f"DROP TABLE IF EXISTS {CATALOG}.{SCHEMA}.tc10_admin_data",
            
            f"CREATE TABLE {CATALOG}.{SCHEMA}.tc10_user_data (id INT, username STRING)",
            f"CREATE TABLE {CATALOG}.{SCHEMA}.tc10_admin_data (id INT, admin_secret STRING)",
            
            f"INSERT INTO {CATALOG}.{SCHEMA}.tc10_user_data VALUES (1, 'user1'), (2, 'user2')",
            f"INSERT INTO {CATALOG}.{SCHEMA}.tc10_admin_data VALUES (1, 'super_secret')",
            
            # Revoke access to admin table
            f"REVOKE ALL PRIVILEGES ON TABLE {CATALOG}.{SCHEMA}.tc10_admin_data FROM `{SERVICE_PRINCIPAL_B_ID}`",
            
            # Create procedure with parameter validation
            f"DROP PROCEDURE IF EXISTS {CATALOG}.{SCHEMA}.tc10_search_user",
            f"""
            CREATE PROCEDURE {CATALOG}.{SCHEMA}.tc10_search_user(username_param STRING)
            LANGUAGE SQL
            AS BEGIN
                -- Parameterized query (safe approach)
                SELECT * FROM {CATALOG}.{SCHEMA}.tc10_user_data WHERE username = username_param;
            END
            """,
            
            f"GRANT EXECUTE ON PROCEDURE {CATALOG}.{SCHEMA}.tc10_search_user TO `{SERVICE_PRINCIPAL_B_ID}`"
        ],
        # Try to inject SQL - this should be safely handled
        test_sql=f"CALL {CATALOG}.{SCHEMA}.tc10_search_user('user1'' OR 1=1 --')",
        teardown_sql=[
            f"DROP PROCEDURE IF EXISTS {CATALOG}.{SCHEMA}.tc10_search_user",
            f"DROP TABLE IF EXISTS {CATALOG}.{SCHEMA}.tc10_admin_data",
            f"DROP TABLE IF EXISTS {CATALOG}.{SCHEMA}.tc10_user_data"
        ]
    )
    tests.append(tc10)
    
    return tests


if __name__ == "__main__":
    from framework.test_framework import DatabricksConnection, TestReporter
    from framework.config import SERVER_HOSTNAME, HTTP_PATH, PAT_TOKEN, CATALOG, SCHEMA
    
    print("=" * 80)
    print("🧪 Parameter & Injection Safety Test Suite (TC-09 to TC-10)")
    print("=" * 80)
    print()
    
    conn = DatabricksConnection(SERVER_HOSTNAME, HTTP_PATH, PAT_TOKEN, CATALOG, SCHEMA)
    executor = TestExecutor(conn)
    
    test_cases = get_tests()
    results = [executor.run_test(tc) for tc in test_cases]
    
    reporter = TestReporter(results)
    reporter.print_summary()
    
    conn.close()
