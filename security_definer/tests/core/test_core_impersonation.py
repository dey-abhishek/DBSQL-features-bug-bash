"""
TC-01 to TC-03: Core Impersonation Tests
Tests identity resolution, role inheritance, and role switching
"""

import sys
import os
sys.path.insert(0, os.path.join(os.path.dirname(__file__), '..'))

from framework.test_framework import DefinerTestCase
from framework.config import SERVICE_PRINCIPAL_B, SERVICE_PRINCIPAL_B_ID, CATALOG, SCHEMA


def get_tests():
    """Return list of core impersonation test cases"""
    
    tests = []
    
    # TC-01: Invoker vs Definer Identity Resolution
    tc01 = DefinerTestCase(
        test_id="TC-01",
        description="Invoker vs Definer Identity Resolution",
        setup_sql=[
            f"DROP PROCEDURE IF EXISTS {CATALOG}.{SCHEMA}.tc01_proc_definer",
            f"DROP PROCEDURE IF EXISTS {CATALOG}.{SCHEMA}.tc01_proc_invoker",
            
            # Load SQL from file (already has fully qualified names)
            open('sql/tc01_definer_procedure.sql').read(),
            open('sql/tc01_invoker_procedure.sql').read(),
            
            # Grant using UUID instead of name
            f"GRANT EXECUTE ON PROCEDURE {CATALOG}.{SCHEMA}.tc01_proc_definer TO `{SERVICE_PRINCIPAL_B_ID}`",
            f"GRANT EXECUTE ON PROCEDURE {CATALOG}.{SCHEMA}.tc01_proc_invoker TO `{SERVICE_PRINCIPAL_B_ID}`"
        ],
        test_sql=f"CALL {CATALOG}.{SCHEMA}.tc01_proc_definer()",
        teardown_sql=[
            f"DROP PROCEDURE IF EXISTS {CATALOG}.{SCHEMA}.tc01_proc_definer",
            f"DROP PROCEDURE IF EXISTS {CATALOG}.{SCHEMA}.tc01_proc_invoker"
        ]
    )
    tests.append(tc01)
    
    # TC-02: Role Inheritance During Execution
    tc02 = DefinerTestCase(
        test_id="TC-02",
        description="Role Inheritance - DEFINER mode should succeed with owner's permissions",
        setup_sql=[
            f"DROP TABLE IF EXISTS {CATALOG}.{SCHEMA}.tc02_protected_table",
            f"CREATE TABLE {CATALOG}.{SCHEMA}.tc02_protected_table (id INT, data STRING)",
            f"INSERT INTO {CATALOG}.{SCHEMA}.tc02_protected_table VALUES (1, 'sensitive'), (2, 'data')",
            
            # Revoke from service principal using UUID
            f"REVOKE ALL PRIVILEGES ON TABLE {CATALOG}.{SCHEMA}.tc02_protected_table FROM `{SERVICE_PRINCIPAL_B_ID}`",
            
            f"DROP PROCEDURE IF EXISTS {CATALOG}.{SCHEMA}.tc02_proc_definer",
            f"""
            CREATE PROCEDURE {CATALOG}.{SCHEMA}.tc02_proc_definer()
            LANGUAGE SQL
            AS BEGIN
                SELECT COUNT(*) as row_count FROM {CATALOG}.{SCHEMA}.tc02_protected_table;
            END
            """,
            
            # Grant execute using UUID
            f"GRANT EXECUTE ON PROCEDURE {CATALOG}.{SCHEMA}.tc02_proc_definer TO `{SERVICE_PRINCIPAL_B_ID}`"
        ],
        test_sql=f"CALL {CATALOG}.{SCHEMA}.tc02_proc_definer();",
        teardown_sql=[
            f"DROP PROCEDURE IF EXISTS {CATALOG}.{SCHEMA}.tc02_proc_definer",
            f"DROP TABLE IF EXISTS {CATALOG}.{SCHEMA}.tc02_protected_table"
        ],
        expected_result={"value": 2}
    )
    tests.append(tc02)
    
    # TC-03: Explicit Role Switching
    tc03 = DefinerTestCase(
        test_id="TC-03",
        description="Role Switching - Validate SET ROLE behavior",
        setup_sql=[
            f"DROP PROCEDURE IF EXISTS {CATALOG}.{SCHEMA}.tc03_role_switch",
            f"""
            CREATE PROCEDURE {CATALOG}.{SCHEMA}.tc03_role_switch()
            LANGUAGE SQL
            AS BEGIN
                SELECT 
                    current_user() as user,
                    'Role switch test' as status;
            END
            """
        ],
        test_sql=f"CALL {CATALOG}.{SCHEMA}.tc03_role_switch();",
        teardown_sql=[f"DROP PROCEDURE IF EXISTS {CATALOG}.{SCHEMA}.tc03_role_switch"]
    )
    tests.append(tc03)
    
    return tests


if __name__ == "__main__":
    # Can be run standalone for testing this module
    from framework.test_framework import DatabricksConnection, TestExecutor, TestReporter
    from framework.config import SERVER_HOSTNAME, HTTP_PATH, PAT_TOKEN, CATALOG, SCHEMA
    
    conn = DatabricksConnection(SERVER_HOSTNAME, HTTP_PATH, PAT_TOKEN, CATALOG, SCHEMA)
    executor = TestExecutor(conn)
    
    tests = get_tests()
    results = [executor.run_test(tc) for tc in tests]
    
    reporter = TestReporter(results)
    reporter.print_summary()
    
    conn.close()
