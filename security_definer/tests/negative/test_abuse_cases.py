"""
TC-17 to TC-19: Negative / Abuse Cases Tests
Critical tests that validate security boundaries and prevent abuse.
"""

from framework.test_framework import DefinerTestCase, TestExecutor
from framework.config import SERVICE_PRINCIPAL_B_ID, CATALOG, SCHEMA

def get_tests():
    tests = []
    
    # TC-17: Attempt to Call System / Admin Procedures
    tc17 = DefinerTestCase(
        test_id="TC-17",
        description="System Procedure Access - Prevent unauthorized system/admin procedure calls",
        setup_sql=[
            # Create a procedure that tries to access system functions
            f"DROP PROCEDURE IF EXISTS {CATALOG}.{SCHEMA}.tc17_system_access",
            f"""
            CREATE PROCEDURE {CATALOG}.{SCHEMA}.tc17_system_access()
            LANGUAGE SQL
            AS BEGIN
                -- Try to access system information
                SELECT CURRENT_USER() as user, CURRENT_CATALOG() as catalog, CURRENT_SCHEMA() as schema;
            END
            """,
            
            f"GRANT EXECUTE ON PROCEDURE {CATALOG}.{SCHEMA}.tc17_system_access TO `{SERVICE_PRINCIPAL_B_ID}`"
        ],
        test_sql=f"CALL {CATALOG}.{SCHEMA}.tc17_system_access()",
        teardown_sql=[
            f"DROP PROCEDURE IF EXISTS {CATALOG}.{SCHEMA}.tc17_system_access"
        ]
    )
    tests.append(tc17)
    
    # TC-18: Metadata Enumeration
    tc18 = DefinerTestCase(
        test_id="TC-18",
        description="Metadata Enumeration - Prevent information leakage via information_schema",
        setup_sql=[
            # Create procedure that queries information_schema
            f"DROP PROCEDURE IF EXISTS {CATALOG}.{SCHEMA}.tc18_enum_metadata",
            f"""
            CREATE PROCEDURE {CATALOG}.{SCHEMA}.tc18_enum_metadata()
            LANGUAGE SQL
            SQL SECURITY INVOKER
            AS BEGIN
                -- Try to enumerate tables (should only see what caller can see)
                SELECT COUNT(*) as visible_tables 
                FROM information_schema.tables 
                WHERE table_catalog = '{CATALOG}' 
                AND table_schema = '{SCHEMA}';
            END
            """,
            
            f"GRANT EXECUTE ON PROCEDURE {CATALOG}.{SCHEMA}.tc18_enum_metadata TO `{SERVICE_PRINCIPAL_B_ID}`"
        ],
        test_sql=f"CALL {CATALOG}.{SCHEMA}.tc18_enum_metadata()",
        teardown_sql=[
            f"DROP PROCEDURE IF EXISTS {CATALOG}.{SCHEMA}.tc18_enum_metadata"
        ]
    )
    tests.append(tc18)
    
    # TC-19: Long-Running Procedure + Role Revocation
    tc19 = DefinerTestCase(
        test_id="TC-19",
        description="Time-of-Check vs Time-of-Use - Validate behavior when permissions change during execution",
        setup_sql=[
            f"DROP TABLE IF EXISTS {CATALOG}.{SCHEMA}.tc19_test_table",
            f"CREATE TABLE {CATALOG}.{SCHEMA}.tc19_test_table (id INT, step STRING, timestamp TIMESTAMP)",
            
            # Create procedure that performs multiple operations
            f"DROP PROCEDURE IF EXISTS {CATALOG}.{SCHEMA}.tc19_multi_step",
            f"""
            CREATE PROCEDURE {CATALOG}.{SCHEMA}.tc19_multi_step()
            LANGUAGE SQL
            AS BEGIN
                -- Step 1: Insert initial record
                INSERT INTO {CATALOG}.{SCHEMA}.tc19_test_table 
                VALUES (1, 'step1', CURRENT_TIMESTAMP());
                
                -- Step 2: Query the record
                SELECT COUNT(*) as count_after_insert FROM {CATALOG}.{SCHEMA}.tc19_test_table;
                
                -- Step 3: Insert another record
                INSERT INTO {CATALOG}.{SCHEMA}.tc19_test_table 
                VALUES (2, 'step2', CURRENT_TIMESTAMP());
                
                -- Step 4: Final count
                SELECT COUNT(*) as final_count FROM {CATALOG}.{SCHEMA}.tc19_test_table;
            END
            """,
            
            f"GRANT EXECUTE ON PROCEDURE {CATALOG}.{SCHEMA}.tc19_multi_step TO `{SERVICE_PRINCIPAL_B_ID}`"
        ],
        test_sql=f"CALL {CATALOG}.{SCHEMA}.tc19_multi_step()",
        teardown_sql=[
            f"DROP PROCEDURE IF EXISTS {CATALOG}.{SCHEMA}.tc19_multi_step",
            f"DROP TABLE IF EXISTS {CATALOG}.{SCHEMA}.tc19_test_table"
        ]
    )
    tests.append(tc19)
    
    return tests


if __name__ == "__main__":
    from framework.test_framework import DatabricksConnection, TestReporter
    from framework.config import SERVER_HOSTNAME, HTTP_PATH, PAT_TOKEN, CATALOG, SCHEMA
    
    print("=" * 80)
    print("🧪 Negative / Abuse Cases Test Suite (TC-17 to TC-19)")
    print("=" * 80)
    print()
    
    conn = DatabricksConnection(SERVER_HOSTNAME, HTTP_PATH, PAT_TOKEN, CATALOG, SCHEMA)
    executor = TestExecutor(conn)
    
    test_cases = get_tests()
    results = [executor.run_test(tc) for tc in test_cases]
    
    reporter = TestReporter(results)
    reporter.print_summary()
    
    conn.close()
