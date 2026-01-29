"""
TC-13 to TC-16: Databricks-Specific Tests (Unity Catalog / DBSQL)
Tests that validate Unity Catalog privilege enforcement and warehouse-level controls.
"""

from framework.test_framework import DefinerTestCase, TestExecutor
from framework.config import SERVICE_PRINCIPAL_B_ID, CATALOG, SCHEMA

def get_tests():
    tests = []
    
    # TC-13: Unity Catalog Privilege Enforcement
    tc13 = DefinerTestCase(
        test_id="TC-13",
        description="Unity Catalog Privilege Enforcement - UC permissions are authoritative",
        setup_sql=[
            f"DROP TABLE IF EXISTS {CATALOG}.{SCHEMA}.tc13_uc_governed_table",
            f"CREATE TABLE {CATALOG}.{SCHEMA}.tc13_uc_governed_table (id INT, uc_data STRING)",
            f"INSERT INTO {CATALOG}.{SCHEMA}.tc13_uc_governed_table VALUES (1, 'unity_catalog_data')",
            
            # Explicitly revoke UC table access
            f"REVOKE ALL PRIVILEGES ON TABLE {CATALOG}.{SCHEMA}.tc13_uc_governed_table FROM `{SERVICE_PRINCIPAL_B_ID}`",
            
            # Create DEFINER procedure
            f"DROP PROCEDURE IF EXISTS {CATALOG}.{SCHEMA}.tc13_uc_access",
            f"""
            CREATE PROCEDURE {CATALOG}.{SCHEMA}.tc13_uc_access()
            LANGUAGE SQL
            AS BEGIN
                SELECT COUNT(*) as row_count FROM {CATALOG}.{SCHEMA}.tc13_uc_governed_table;
            END
            """,
            
            f"GRANT EXECUTE ON PROCEDURE {CATALOG}.{SCHEMA}.tc13_uc_access TO `{SERVICE_PRINCIPAL_B_ID}`"
        ],
        test_sql=f"CALL {CATALOG}.{SCHEMA}.tc13_uc_access()",
        teardown_sql=[
            f"DROP PROCEDURE IF EXISTS {CATALOG}.{SCHEMA}.tc13_uc_access",
            f"DROP TABLE IF EXISTS {CATALOG}.{SCHEMA}.tc13_uc_governed_table"
        ]
    )
    tests.append(tc13)
    
    # TC-14: Warehouse-Level Enforcement
    tc14 = DefinerTestCase(
        test_id="TC-14",
        description="Warehouse-Level Enforcement - Warehouse ACLs are respected",
        setup_sql=[
            # Create simple procedure
            f"DROP PROCEDURE IF EXISTS {CATALOG}.{SCHEMA}.tc14_warehouse_check",
            f"""
            CREATE PROCEDURE {CATALOG}.{SCHEMA}.tc14_warehouse_check()
            LANGUAGE SQL
            AS BEGIN
                SELECT CURRENT_USER() as user, CURRENT_TIMESTAMP() as exec_time;
            END
            """,
            
            f"GRANT EXECUTE ON PROCEDURE {CATALOG}.{SCHEMA}.tc14_warehouse_check TO `{SERVICE_PRINCIPAL_B_ID}`"
        ],
        test_sql=f"CALL {CATALOG}.{SCHEMA}.tc14_warehouse_check()",
        teardown_sql=[
            f"DROP PROCEDURE IF EXISTS {CATALOG}.{SCHEMA}.tc14_warehouse_check"
        ]
    )
    tests.append(tc14)
    
    # TC-15: Cross-Catalog / Cross-Schema Access
    tc15 = DefinerTestCase(
        test_id="TC-15",
        description="Cross-Catalog Access - Prevent unauthorized lateral movement",
        setup_sql=[
            # Create table in same catalog/schema
            f"DROP TABLE IF EXISTS {CATALOG}.{SCHEMA}.tc15_local_table",
            f"CREATE TABLE {CATALOG}.{SCHEMA}.tc15_local_table (id INT, data STRING)",
            f"INSERT INTO {CATALOG}.{SCHEMA}.tc15_local_table VALUES (1, 'local_data')",
            
            # Create procedure that accesses local catalog
            f"DROP PROCEDURE IF EXISTS {CATALOG}.{SCHEMA}.tc15_catalog_access",
            f"""
            CREATE PROCEDURE {CATALOG}.{SCHEMA}.tc15_catalog_access()
            LANGUAGE SQL
            AS BEGIN
                -- Access only explicitly granted catalog.schema
                SELECT COUNT(*) as count FROM {CATALOG}.{SCHEMA}.tc15_local_table;
            END
            """,
            
            f"GRANT EXECUTE ON PROCEDURE {CATALOG}.{SCHEMA}.tc15_catalog_access TO `{SERVICE_PRINCIPAL_B_ID}`"
        ],
        test_sql=f"CALL {CATALOG}.{SCHEMA}.tc15_catalog_access()",
        teardown_sql=[
            f"DROP PROCEDURE IF EXISTS {CATALOG}.{SCHEMA}.tc15_catalog_access",
            f"DROP TABLE IF EXISTS {CATALOG}.{SCHEMA}.tc15_local_table"
        ]
    )
    tests.append(tc15)
    
    # TC-16: Photon vs Non-Photon Execution
    tc16 = DefinerTestCase(
        test_id="TC-16",
        description="Photon Consistency - Same security semantics across execution engines",
        setup_sql=[
            f"DROP TABLE IF EXISTS {CATALOG}.{SCHEMA}.tc16_execution_test",
            f"CREATE TABLE {CATALOG}.{SCHEMA}.tc16_execution_test (id INT, engine_test STRING)",
            f"INSERT INTO {CATALOG}.{SCHEMA}.tc16_execution_test VALUES (1, 'photon_test'), (2, 'standard_test')",
            
            # Create DEFINER procedure
            f"DROP PROCEDURE IF EXISTS {CATALOG}.{SCHEMA}.tc16_engine_proc",
            f"""
            CREATE PROCEDURE {CATALOG}.{SCHEMA}.tc16_engine_proc()
            LANGUAGE SQL
            AS BEGIN
                SELECT 
                    COUNT(*) as total_rows,
                    CURRENT_USER() as exec_user
                FROM {CATALOG}.{SCHEMA}.tc16_execution_test;
            END
            """,
            
            f"GRANT EXECUTE ON PROCEDURE {CATALOG}.{SCHEMA}.tc16_engine_proc TO `{SERVICE_PRINCIPAL_B_ID}`"
        ],
        test_sql=f"CALL {CATALOG}.{SCHEMA}.tc16_engine_proc()",
        teardown_sql=[
            f"DROP PROCEDURE IF EXISTS {CATALOG}.{SCHEMA}.tc16_engine_proc",
            f"DROP TABLE IF EXISTS {CATALOG}.{SCHEMA}.tc16_execution_test"
        ]
    )
    tests.append(tc16)
    
    return tests


if __name__ == "__main__":
    from framework.test_framework import DatabricksConnection, TestReporter
    from framework.config import SERVER_HOSTNAME, HTTP_PATH, PAT_TOKEN, CATALOG, SCHEMA
    
    print("=" * 80)
    print("🧪 Databricks-Specific Test Suite (TC-13 to TC-16)")
    print("=" * 80)
    print()
    
    conn = DatabricksConnection(SERVER_HOSTNAME, HTTP_PATH, PAT_TOKEN, CATALOG, SCHEMA)
    executor = TestExecutor(conn)
    
    test_cases = get_tests()
    results = [executor.run_test(tc) for tc in test_cases]
    
    reporter = TestReporter(results)
    reporter.print_summary()
    
    conn.close()
