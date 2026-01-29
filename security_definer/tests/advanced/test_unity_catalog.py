"""
ADVANCED TEST SUITE: Unity Catalog Specific Tests
Focus on UC features, external locations, and cross-catalog scenarios
"""

from framework.test_framework import DefinerTestCase, TestExecutor
from framework.config import SERVICE_PRINCIPAL_B_ID, CATALOG, SCHEMA

def get_tests():
    tests = []
    
    # TC-93: Cross-Catalog Access Attempt
    tc93 = DefinerTestCase(
        test_id="TC-93",
        description="Cross-Catalog Access - DEFINER procedure should not bypass catalog boundaries",
        setup_sql=[
            f"DROP TABLE IF EXISTS {CATALOG}.{SCHEMA}.tc93_local_table",
            f"CREATE TABLE {CATALOG}.{SCHEMA}.tc93_local_table (id INT, data STRING)",
            f"INSERT INTO {CATALOG}.{SCHEMA}.tc93_local_table VALUES (1, 'local_data')",
            
            # Procedure that tries to access current catalog only
            f"DROP PROCEDURE IF EXISTS {CATALOG}.{SCHEMA}.tc93_catalog_check",
            f"""
            CREATE PROCEDURE {CATALOG}.{SCHEMA}.tc93_catalog_check()
            LANGUAGE SQL
            AS BEGIN
                SELECT 
                    CURRENT_CATALOG() as current_cat,
                    CURRENT_SCHEMA() as current_sch,
                    COUNT(*) as table_count
                FROM {CATALOG}.{SCHEMA}.tc93_local_table;
            END
            """,
        ],
        test_sql=f"CALL {CATALOG}.{SCHEMA}.tc93_catalog_check()",
        teardown_sql=[
            f"DROP PROCEDURE IF EXISTS {CATALOG}.{SCHEMA}.tc93_catalog_check",
            f"DROP TABLE IF EXISTS {CATALOG}.{SCHEMA}.tc93_local_table",
        ]
    )
    tests.append(tc93)
    
    # TC-94: Schema Context Switching
    tc94 = DefinerTestCase(
        test_id="TC-94",
        description="Schema Switching - Test USE SCHEMA inside procedure",
        setup_sql=[
            f"DROP TABLE IF EXISTS {CATALOG}.{SCHEMA}.tc94_original_schema_table",
            f"CREATE TABLE {CATALOG}.{SCHEMA}.tc94_original_schema_table (id INT, location STRING)",
            f"INSERT INTO {CATALOG}.{SCHEMA}.tc94_original_schema_table VALUES (1, 'original_schema')",
            
            # Procedure that checks current schema
            f"DROP PROCEDURE IF EXISTS {CATALOG}.{SCHEMA}.tc94_schema_context",
            f"""
            CREATE PROCEDURE {CATALOG}.{SCHEMA}.tc94_schema_context()
            LANGUAGE SQL
            AS BEGIN
                SELECT 
                    CURRENT_CATALOG() as catalog,
                    CURRENT_SCHEMA() as schema,
                    'Before any USE statement' as context;
                
                -- Check if we can switch schema (might not be allowed)
                -- USE SCHEMA information_schema;
                
                SELECT 
                    CURRENT_CATALOG() as catalog,
                    CURRENT_SCHEMA() as schema,
                    'After context' as context;
            END
            """,
        ],
        test_sql=f"CALL {CATALOG}.{SCHEMA}.tc94_schema_context()",
        teardown_sql=[
            f"DROP PROCEDURE IF EXISTS {CATALOG}.{SCHEMA}.tc94_schema_context",
            f"DROP TABLE IF EXISTS {CATALOG}.{SCHEMA}.tc94_original_schema_table",
        ]
    )
    tests.append(tc94)
    
    # TC-95: Information Schema Access
    tc95 = DefinerTestCase(
        test_id="TC-95",
        description="Information Schema - Test metadata enumeration via DEFINER procedure",
        setup_sql=[
            # Create procedure that queries information_schema
            f"DROP PROCEDURE IF EXISTS {CATALOG}.{SCHEMA}.tc95_enum_tables",
            f"""
            CREATE PROCEDURE {CATALOG}.{SCHEMA}.tc95_enum_tables()
            LANGUAGE SQL
            AS BEGIN
                -- Query information schema (should only see permitted tables)
                SELECT COUNT(*) as table_count
                FROM information_schema.tables
                WHERE table_catalog = '{CATALOG}'
                  AND table_schema = '{SCHEMA}'
                  AND table_name LIKE 'tc95%';
            END
            """,
            
            f"GRANT EXECUTE ON PROCEDURE {CATALOG}.{SCHEMA}.tc95_enum_tables TO `{SERVICE_PRINCIPAL_B_ID}`",
        ],
        test_sql=f"CALL {CATALOG}.{SCHEMA}.tc95_enum_tables()",
        teardown_sql=[
            f"DROP PROCEDURE IF EXISTS {CATALOG}.{SCHEMA}.tc95_enum_tables",
        ]
    )
    tests.append(tc95)
    
    # TC-96: Column-Level Security (if supported)
    tc96 = DefinerTestCase(
        test_id="TC-96",
        description="Column Masking - Test if DEFINER procedure respects column masks",
        setup_sql=[
            f"DROP TABLE IF EXISTS {CATALOG}.{SCHEMA}.tc96_masked_data",
            f"CREATE TABLE {CATALOG}.{SCHEMA}.tc96_masked_data (id INT, ssn STRING, salary DECIMAL(10,2))",
            f"INSERT INTO {CATALOG}.{SCHEMA}.tc96_masked_data VALUES (1, '123-45-6789', 100000), (2, '987-65-4321', 150000)",
            
            # Note: Column masking might not be available in all UC versions
            # This procedure accesses potentially masked columns
            f"DROP PROCEDURE IF EXISTS {CATALOG}.{SCHEMA}.tc96_read_sensitive",
            f"""
            CREATE PROCEDURE {CATALOG}.{SCHEMA}.tc96_read_sensitive()
            LANGUAGE SQL
            AS BEGIN
                -- DEFINER should see unmasked data (owner's view)
                SELECT id, ssn, salary FROM {CATALOG}.{SCHEMA}.tc96_masked_data;
            END
            """,
        ],
        test_sql=f"CALL {CATALOG}.{SCHEMA}.tc96_read_sensitive()",
        teardown_sql=[
            f"DROP PROCEDURE IF EXISTS {CATALOG}.{SCHEMA}.tc96_read_sensitive",
            f"DROP TABLE IF EXISTS {CATALOG}.{SCHEMA}.tc96_masked_data",
        ]
    )
    tests.append(tc96)
    
    # TC-97: View Access with Different Modes
    tc97 = DefinerTestCase(
        test_id="TC-97",
        description="View Interaction - DEFINER procedure accessing views",
        setup_sql=[
            f"DROP VIEW IF EXISTS {CATALOG}.{SCHEMA}.tc97_filtered_view",
            f"DROP TABLE IF EXISTS {CATALOG}.{SCHEMA}.tc97_base_table",
            f"CREATE TABLE {CATALOG}.{SCHEMA}.tc97_base_table (id INT, category STRING, amount DECIMAL(10,2))",
            f"INSERT INTO {CATALOG}.{SCHEMA}.tc97_base_table VALUES (1, 'public', 100), (2, 'private', 500), (3, 'public', 200)",
            
            # Create view with filter
            f"CREATE VIEW {CATALOG}.{SCHEMA}.tc97_filtered_view AS SELECT * FROM {CATALOG}.{SCHEMA}.tc97_base_table WHERE category = 'public'",
            
            # Procedure accesses view
            f"DROP PROCEDURE IF EXISTS {CATALOG}.{SCHEMA}.tc97_read_via_view",
            f"""
            CREATE PROCEDURE {CATALOG}.{SCHEMA}.tc97_read_via_view()
            LANGUAGE SQL
            AS BEGIN
                SELECT COUNT(*) as visible_rows, SUM(amount) as total_amount
                FROM {CATALOG}.{SCHEMA}.tc97_filtered_view;
            END
            """,
        ],
        test_sql=f"CALL {CATALOG}.{SCHEMA}.tc97_read_via_view()",
        teardown_sql=[
            f"DROP PROCEDURE IF EXISTS {CATALOG}.{SCHEMA}.tc97_read_via_view",
            f"DROP VIEW IF EXISTS {CATALOG}.{SCHEMA}.tc97_filtered_view",
            f"DROP TABLE IF EXISTS {CATALOG}.{SCHEMA}.tc97_base_table",
        ]
    )
    tests.append(tc97)
    
    # TC-98: Managed Table vs External Table
    tc98 = DefinerTestCase(
        test_id="TC-98",
        description="Table Types - Access patterns for managed vs external tables",
        setup_sql=[
            f"DROP TABLE IF EXISTS {CATALOG}.{SCHEMA}.tc98_managed_table",
            f"CREATE TABLE {CATALOG}.{SCHEMA}.tc98_managed_table (id INT, data STRING)",
            f"INSERT INTO {CATALOG}.{SCHEMA}.tc98_managed_table VALUES (1, 'managed_data')",
            
            # Note: External table creation requires external location
            # We'll just test managed table access
            f"DROP PROCEDURE IF EXISTS {CATALOG}.{SCHEMA}.tc98_table_access",
            f"""
            CREATE PROCEDURE {CATALOG}.{SCHEMA}.tc98_table_access()
            LANGUAGE SQL
            AS BEGIN
                SELECT 
                    id,
                    data,
                    'MANAGED' as table_type
                FROM {CATALOG}.{SCHEMA}.tc98_managed_table;
            END
            """,
        ],
        test_sql=f"CALL {CATALOG}.{SCHEMA}.tc98_table_access()",
        teardown_sql=[
            f"DROP PROCEDURE IF EXISTS {CATALOG}.{SCHEMA}.tc98_table_access",
            f"DROP TABLE IF EXISTS {CATALOG}.{SCHEMA}.tc98_managed_table",
        ]
    )
    tests.append(tc98)
    
    # TC-99: Grant Cascade Behavior
    tc99 = DefinerTestCase(
        test_id="TC-99",
        description="Grant Propagation - Test if EXECUTE grant is sufficient for nested access",
        setup_sql=[
            f"DROP TABLE IF EXISTS {CATALOG}.{SCHEMA}.tc99_restricted_table",
            f"CREATE TABLE {CATALOG}.{SCHEMA}.tc99_restricted_table (id INT, value INT)",
            f"INSERT INTO {CATALOG}.{SCHEMA}.tc99_restricted_table VALUES (1, 100), (2, 200)",
            
            # Revoke table access
            f"REVOKE ALL PRIVILEGES ON TABLE {CATALOG}.{SCHEMA}.tc99_restricted_table FROM `{SERVICE_PRINCIPAL_B_ID}`",
            
            # DEFINER procedure (owner has access to table)
            f"DROP PROCEDURE IF EXISTS {CATALOG}.{SCHEMA}.tc99_definer_gateway",
            f"""
            CREATE PROCEDURE {CATALOG}.{SCHEMA}.tc99_definer_gateway()
            LANGUAGE SQL
            AS BEGIN
                -- Service principal can execute this procedure
                -- But table access is via definer's privileges
                SELECT SUM(value) as total FROM {CATALOG}.{SCHEMA}.tc99_restricted_table;
            END
            """,
            
            # Grant only EXECUTE permission (not SELECT on table)
            f"GRANT EXECUTE ON PROCEDURE {CATALOG}.{SCHEMA}.tc99_definer_gateway TO `{SERVICE_PRINCIPAL_B_ID}`",
        ],
        test_sql=f"CALL {CATALOG}.{SCHEMA}.tc99_definer_gateway()",
        teardown_sql=[
            f"DROP PROCEDURE IF EXISTS {CATALOG}.{SCHEMA}.tc99_definer_gateway",
            f"DROP TABLE IF EXISTS {CATALOG}.{SCHEMA}.tc99_restricted_table",
        ]
    )
    tests.append(tc99)
    
    return tests


if __name__ == "__main__":
    from framework.test_framework import DatabricksConnection, TestReporter
    from framework.config import SERVER_HOSTNAME, HTTP_PATH, PAT_TOKEN, CATALOG, SCHEMA
    
    print("=" * 80)
    print("🔥 UNITY CATALOG ADVANCED TEST SUITE")
    print("=" * 80)
    print()
    print("Testing: Cross-catalog access, schema switching, information_schema,")
    print("         column masking, view interaction, and grant propagation")
    print()
    
    conn = DatabricksConnection(SERVER_HOSTNAME, HTTP_PATH, PAT_TOKEN, CATALOG, SCHEMA)
    executor = TestExecutor(conn)
    
    test_cases = get_tests()
    results = [executor.run_test(tc) for tc in test_cases]
    
    reporter = TestReporter(results)
    reporter.print_summary()
    
    conn.close()
    
    # Analyze UC-specific findings
    print()
    print("=" * 80)
    print("🎯 UNITY CATALOG FINDINGS")
    print("=" * 80)
    passed = [r for r in results if r.status == "PASS"]
    failed = [r for r in results if r.status == "FAIL"]
    
    print(f"✅ UC features working correctly: {len(passed)}")
    if failed:
        print(f"⚠️  UC features with issues: {len(failed)}")
        for r in failed:
            print(f"   - {r.test_id}: {r.description}")
