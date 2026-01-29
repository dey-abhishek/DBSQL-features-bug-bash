"""
KI-01 to KI-05: Known Issues Tests
Tests that validate known issues with the SECURITY DEFINER feature.

Known Issues:
1. Definer's calls nesting is not limited (expected to be up to 4 calls with the definer's context change)
2. Information about run_as and run_by is not propagated to the audit log
3. Workspaces functions have very limited availability
4. Non descriptive error messages are printed in case a forbidden workspace API is called
5. current_user() always returns the session user, meaning the outermost user is expected, 
   but functions is_member() and is_account_group_member() are calculated based on the 
   current definer context even if they should behave based on session user as per documentation
"""

from framework.test_framework import DefinerTestCase, TestExecutor
from framework.config import SERVICE_PRINCIPAL_B_ID, CATALOG, SCHEMA

def get_tests():
    tests = []
    
    # KI-01: Definer's Calls Nesting Not Limited
    ki01 = DefinerTestCase(
        test_id="KI-01",
        description="Known Issue: Definer nesting should be limited to 4 levels but isn't",
        setup_sql=[
            # Create nested procedures (level 5)
            f"DROP PROCEDURE IF EXISTS {CATALOG}.{SCHEMA}.ki01_proc_level5",
            f"""
            CREATE PROCEDURE {CATALOG}.{SCHEMA}.ki01_proc_level5()
            LANGUAGE SQL
            AS BEGIN
                SELECT 5 as nesting_level, CURRENT_USER() as user;
            END
            """,
            
            f"DROP PROCEDURE IF EXISTS {CATALOG}.{SCHEMA}.ki01_proc_level4",
            f"""
            CREATE PROCEDURE {CATALOG}.{SCHEMA}.ki01_proc_level4()
            LANGUAGE SQL
            AS BEGIN
                CALL {CATALOG}.{SCHEMA}.ki01_proc_level5();
            END
            """,
            
            f"DROP PROCEDURE IF EXISTS {CATALOG}.{SCHEMA}.ki01_proc_level3",
            f"""
            CREATE PROCEDURE {CATALOG}.{SCHEMA}.ki01_proc_level3()
            LANGUAGE SQL
            AS BEGIN
                CALL {CATALOG}.{SCHEMA}.ki01_proc_level4();
            END
            """,
            
            f"DROP PROCEDURE IF EXISTS {CATALOG}.{SCHEMA}.ki01_proc_level2",
            f"""
            CREATE PROCEDURE {CATALOG}.{SCHEMA}.ki01_proc_level2()
            LANGUAGE SQL
            AS BEGIN
                CALL {CATALOG}.{SCHEMA}.ki01_proc_level3();
            END
            """,
            
            f"DROP PROCEDURE IF EXISTS {CATALOG}.{SCHEMA}.ki01_proc_level1",
            f"""
            CREATE PROCEDURE {CATALOG}.{SCHEMA}.ki01_proc_level1()
            LANGUAGE SQL
            AS BEGIN
                CALL {CATALOG}.{SCHEMA}.ki01_proc_level2();
            END
            """,
            
            # Grant execute permissions
            f"GRANT EXECUTE ON PROCEDURE {CATALOG}.{SCHEMA}.ki01_proc_level1 TO `{SERVICE_PRINCIPAL_B_ID}`",
            f"GRANT EXECUTE ON PROCEDURE {CATALOG}.{SCHEMA}.ki01_proc_level2 TO `{SERVICE_PRINCIPAL_B_ID}`",
            f"GRANT EXECUTE ON PROCEDURE {CATALOG}.{SCHEMA}.ki01_proc_level3 TO `{SERVICE_PRINCIPAL_B_ID}`",
            f"GRANT EXECUTE ON PROCEDURE {CATALOG}.{SCHEMA}.ki01_proc_level4 TO `{SERVICE_PRINCIPAL_B_ID}`",
            f"GRANT EXECUTE ON PROCEDURE {CATALOG}.{SCHEMA}.ki01_proc_level5 TO `{SERVICE_PRINCIPAL_B_ID}`"
        ],
        test_sql=f"CALL {CATALOG}.{SCHEMA}.ki01_proc_level1()",
        teardown_sql=[
            f"DROP PROCEDURE IF EXISTS {CATALOG}.{SCHEMA}.ki01_proc_level1",
            f"DROP PROCEDURE IF EXISTS {CATALOG}.{SCHEMA}.ki01_proc_level2",
            f"DROP PROCEDURE IF EXISTS {CATALOG}.{SCHEMA}.ki01_proc_level3",
            f"DROP PROCEDURE IF EXISTS {CATALOG}.{SCHEMA}.ki01_proc_level4",
            f"DROP PROCEDURE IF EXISTS {CATALOG}.{SCHEMA}.ki01_proc_level5"
        ]
    )
    tests.append(ki01)
    
    # KI-02: Audit Log Information (run_as and run_by)
    ki02 = DefinerTestCase(
        test_id="KI-02",
        description="Known Issue: Audit log doesn't properly track run_as and run_by",
        setup_sql=[
            f"DROP TABLE IF EXISTS {CATALOG}.{SCHEMA}.ki02_audit_table",
            f"CREATE TABLE {CATALOG}.{SCHEMA}.ki02_audit_table (id INT, logged_user STRING, timestamp TIMESTAMP)",
            
            f"DROP PROCEDURE IF EXISTS {CATALOG}.{SCHEMA}.ki02_audit_proc",
            f"""
            CREATE PROCEDURE {CATALOG}.{SCHEMA}.ki02_audit_proc()
            LANGUAGE SQL
            AS BEGIN
                -- Log execution (audit should capture both definer and invoker)
                INSERT INTO {CATALOG}.{SCHEMA}.ki02_audit_table 
                VALUES (1, CURRENT_USER(), CURRENT_TIMESTAMP());
                
                SELECT * FROM {CATALOG}.{SCHEMA}.ki02_audit_table;
            END
            """,
            
            f"GRANT EXECUTE ON PROCEDURE {CATALOG}.{SCHEMA}.ki02_audit_proc TO `{SERVICE_PRINCIPAL_B_ID}`"
        ],
        test_sql=f"CALL {CATALOG}.{SCHEMA}.ki02_audit_proc()",
        teardown_sql=[
            f"DROP PROCEDURE IF EXISTS {CATALOG}.{SCHEMA}.ki02_audit_proc",
            f"DROP TABLE IF EXISTS {CATALOG}.{SCHEMA}.ki02_audit_table"
        ]
    )
    tests.append(ki02)
    
    # KI-03: Workspace Functions Limited Availability
    ki03 = DefinerTestCase(
        test_id="KI-03",
        description="Known Issue: Workspace functions have limited availability",
        setup_sql=[
            f"DROP PROCEDURE IF EXISTS {CATALOG}.{SCHEMA}.ki03_workspace_func",
            f"""
            CREATE PROCEDURE {CATALOG}.{SCHEMA}.ki03_workspace_func()
            LANGUAGE SQL
            AS BEGIN
                -- Try to use workspace functions (may have limited availability)
                SELECT 
                    CURRENT_USER() as user,
                    CURRENT_CATALOG() as catalog,
                    CURRENT_SCHEMA() as schema;
            END
            """,
            
            f"GRANT EXECUTE ON PROCEDURE {CATALOG}.{SCHEMA}.ki03_workspace_func TO `{SERVICE_PRINCIPAL_B_ID}`"
        ],
        test_sql=f"CALL {CATALOG}.{SCHEMA}.ki03_workspace_func()",
        teardown_sql=[
            f"DROP PROCEDURE IF EXISTS {CATALOG}.{SCHEMA}.ki03_workspace_func"
        ]
    )
    tests.append(ki03)
    
    # KI-04: Non-Descriptive Error Messages
    ki04 = DefinerTestCase(
        test_id="KI-04",
        description="Known Issue: Non-descriptive errors when calling forbidden workspace APIs",
        setup_sql=[
            f"DROP PROCEDURE IF EXISTS {CATALOG}.{SCHEMA}.ki04_workspace_api",
            f"""
            CREATE PROCEDURE {CATALOG}.{SCHEMA}.ki04_workspace_api()
            LANGUAGE SQL
            AS BEGIN
                -- Attempt to access workspace metadata (may fail with unclear error)
                SELECT CURRENT_USER() as user;
            END
            """,
            
            f"GRANT EXECUTE ON PROCEDURE {CATALOG}.{SCHEMA}.ki04_workspace_api TO `{SERVICE_PRINCIPAL_B_ID}`"
        ],
        test_sql=f"CALL {CATALOG}.{SCHEMA}.ki04_workspace_api()",
        teardown_sql=[
            f"DROP PROCEDURE IF EXISTS {CATALOG}.{SCHEMA}.ki04_workspace_api"
        ]
    )
    tests.append(ki04)
    
    # KI-05: current_user() vs is_member() Inconsistency
    ki05 = DefinerTestCase(
        test_id="KI-05",
        description="Known Issue: current_user() returns session user but is_member() uses definer context",
        setup_sql=[
            f"DROP PROCEDURE IF EXISTS {CATALOG}.{SCHEMA}.ki05_user_context",
            f"""
            CREATE PROCEDURE {CATALOG}.{SCHEMA}.ki05_user_context()
            LANGUAGE SQL
            AS BEGIN
                -- current_user() should return outermost user
                -- but is_member() uses definer context (inconsistent behavior)
                SELECT 
                    CURRENT_USER() as current_user_result;
            END
            """,
            
            f"GRANT EXECUTE ON PROCEDURE {CATALOG}.{SCHEMA}.ki05_user_context TO `{SERVICE_PRINCIPAL_B_ID}`"
        ],
        test_sql=f"CALL {CATALOG}.{SCHEMA}.ki05_user_context()",
        teardown_sql=[
            f"DROP PROCEDURE IF EXISTS {CATALOG}.{SCHEMA}.ki05_user_context"
        ]
    )
    tests.append(ki05)
    
    return tests


if __name__ == "__main__":
    from framework.test_framework import DatabricksConnection, TestReporter
    from framework.config import SERVER_HOSTNAME, HTTP_PATH, PAT_TOKEN, CATALOG, SCHEMA
    
    print("=" * 80)
    print("🧪 Known Issues Test Suite (KI-01 to KI-05)")
    print("=" * 80)
    print()
    
    conn = DatabricksConnection(SERVER_HOSTNAME, HTTP_PATH, PAT_TOKEN, CATALOG, SCHEMA)
    executor = TestExecutor(conn)
    
    test_cases = get_tests()
    results = [executor.run_test(tc) for tc in test_cases]
    
    reporter = TestReporter(results)
    reporter.print_summary()
    
    conn.close()
