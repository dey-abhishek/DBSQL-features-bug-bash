"""
ADVANCED TEST SUITE: Privilege Escalation & Security Bypass Tests
Focus on privilege escalation attempts and security boundary violations
"""

from framework.test_framework import DefinerTestCase, TestExecutor
from framework.config import SERVICE_PRINCIPAL_B_ID, CATALOG, SCHEMA

def get_tests():
    tests = []
    
    # TC-81: Procedure Ownership Transfer (if supported)
    tc81 = DefinerTestCase(
        test_id="TC-81",
        description="Ownership Transfer - Test if procedure uses new owner's privileges after transfer",
        setup_sql=[
            f"DROP TABLE IF EXISTS {CATALOG}.{SCHEMA}.tc81_owner_private_data",
            f"CREATE TABLE {CATALOG}.{SCHEMA}.tc81_owner_private_data (id INT, data STRING)",
            f"INSERT INTO {CATALOG}.{SCHEMA}.tc81_owner_private_data VALUES (1, 'owner_secret')",
            
            # Create DEFINER procedure
            f"DROP PROCEDURE IF EXISTS {CATALOG}.{SCHEMA}.tc81_access_private",
            f"""
            CREATE PROCEDURE {CATALOG}.{SCHEMA}.tc81_access_private()
            LANGUAGE SQL
            AS BEGIN
                -- This uses definer's (owner's) privileges
                SELECT COUNT(*) as secret_count FROM {CATALOG}.{SCHEMA}.tc81_owner_private_data;
            END
            """,
            
            # Note: Ownership transfer might not be supported
            # This tests if the procedure works with current owner
            f"GRANT EXECUTE ON PROCEDURE {CATALOG}.{SCHEMA}.tc81_access_private TO `{SERVICE_PRINCIPAL_B_ID}`",
        ],
        test_sql=f"CALL {CATALOG}.{SCHEMA}.tc81_access_private()",
        teardown_sql=[
            f"DROP PROCEDURE IF EXISTS {CATALOG}.{SCHEMA}.tc81_access_private",
            f"DROP TABLE IF EXISTS {CATALOG}.{SCHEMA}.tc81_owner_private_data",
        ]
    )
    tests.append(tc81)
    
    # TC-83: Confused Deputy Attack
    tc83 = DefinerTestCase(
        test_id="TC-83",
        description="Confused Deputy - Use high-privilege procedure to access forbidden data",
        setup_sql=[
            f"DROP TABLE IF EXISTS {CATALOG}.{SCHEMA}.tc83_confidential_hr",
            f"DROP TABLE IF EXISTS {CATALOG}.{SCHEMA}.tc83_public_directory",
            f"CREATE TABLE {CATALOG}.{SCHEMA}.tc83_confidential_hr (emp_id INT, salary DECIMAL(10,2), ssn STRING)",
            f"CREATE TABLE {CATALOG}.{SCHEMA}.tc83_public_directory (emp_id INT, name STRING, email STRING)",
            
            f"INSERT INTO {CATALOG}.{SCHEMA}.tc83_confidential_hr VALUES (1, 150000, '123-45-6789')",
            f"INSERT INTO {CATALOG}.{SCHEMA}.tc83_public_directory VALUES (1, 'Alice', 'alice@company.com')",
            
            # Revoke HR access from service principal
            f"REVOKE ALL PRIVILEGES ON TABLE {CATALOG}.{SCHEMA}.tc83_confidential_hr FROM `{SERVICE_PRINCIPAL_B_ID}`",
            
            # High-privilege procedure that joins both tables (dangerous design)
            f"DROP PROCEDURE IF EXISTS {CATALOG}.{SCHEMA}.tc83_get_employee_info",
            f"""
            CREATE PROCEDURE {CATALOG}.{SCHEMA}.tc83_get_employee_info(emp_id_param INT)
            LANGUAGE SQL
            AS BEGIN
                -- This DEFINER procedure has access to HR data
                -- But should not expose it based on user input
                SELECT 
                    d.emp_id,
                    d.name,
                    d.email,
                    -- Don't expose salary/SSN even though we have access
                    'REDACTED' as salary_info
                FROM {CATALOG}.{SCHEMA}.tc83_public_directory d
                WHERE d.emp_id = emp_id_param;
            END
            """,
            
            f"GRANT EXECUTE ON PROCEDURE {CATALOG}.{SCHEMA}.tc83_get_employee_info TO `{SERVICE_PRINCIPAL_B_ID}`",
        ],
        test_sql=f"CALL {CATALOG}.{SCHEMA}.tc83_get_employee_info(1)",
        teardown_sql=[
            f"DROP PROCEDURE IF EXISTS {CATALOG}.{SCHEMA}.tc83_get_employee_info",
            f"DROP TABLE IF EXISTS {CATALOG}.{SCHEMA}.tc83_public_directory",
            f"DROP TABLE IF EXISTS {CATALOG}.{SCHEMA}.tc83_confidential_hr",
        ]
    )
    tests.append(tc83)
    
    # TC-84: Nested Privilege Amplification
    tc84 = DefinerTestCase(
        test_id="TC-84",
        description="Privilege Amplification - Multi-level nesting with increasing privileges",
        setup_sql=[
            f"DROP TABLE IF EXISTS {CATALOG}.{SCHEMA}.tc84_level3_data",
            f"CREATE TABLE {CATALOG}.{SCHEMA}.tc84_level3_data (id INT, top_secret STRING)",
            f"INSERT INTO {CATALOG}.{SCHEMA}.tc84_level3_data VALUES (1, 'ULTRA_SECRET')",
            
            # Revoke access from service principal
            f"REVOKE ALL PRIVILEGES ON TABLE {CATALOG}.{SCHEMA}.tc84_level3_data FROM `{SERVICE_PRINCIPAL_B_ID}`",
            
            # Level 3: High privilege procedure (accesses top secret)
            f"DROP PROCEDURE IF EXISTS {CATALOG}.{SCHEMA}.tc84_high_priv_proc",
            f"""
            CREATE PROCEDURE {CATALOG}.{SCHEMA}.tc84_high_priv_proc()
            LANGUAGE SQL
            AS BEGIN
                SELECT COUNT(*) as secret_count FROM {CATALOG}.{SCHEMA}.tc84_level3_data;
            END
            """,
            
            # Level 2: Medium privilege procedure (calls Level 3)
            f"DROP PROCEDURE IF EXISTS {CATALOG}.{SCHEMA}.tc84_medium_priv_proc",
            f"""
            CREATE PROCEDURE {CATALOG}.{SCHEMA}.tc84_medium_priv_proc()
            LANGUAGE SQL
            AS BEGIN
                CALL {CATALOG}.{SCHEMA}.tc84_high_priv_proc();
            END
            """,
            
            # Level 1: Low privilege procedure (calls Level 2)
            f"DROP PROCEDURE IF EXISTS {CATALOG}.{SCHEMA}.tc84_low_priv_proc",
            f"""
            CREATE PROCEDURE {CATALOG}.{SCHEMA}.tc84_low_priv_proc()
            LANGUAGE SQL
            AS BEGIN
                CALL {CATALOG}.{SCHEMA}.tc84_medium_priv_proc();
            END
            """,
            
            # Grant execute at each level
            f"GRANT EXECUTE ON PROCEDURE {CATALOG}.{SCHEMA}.tc84_high_priv_proc TO `{SERVICE_PRINCIPAL_B_ID}`",
            f"GRANT EXECUTE ON PROCEDURE {CATALOG}.{SCHEMA}.tc84_medium_priv_proc TO `{SERVICE_PRINCIPAL_B_ID}`",
            f"GRANT EXECUTE ON PROCEDURE {CATALOG}.{SCHEMA}.tc84_low_priv_proc TO `{SERVICE_PRINCIPAL_B_ID}`",
        ],
        test_sql=f"CALL {CATALOG}.{SCHEMA}.tc84_low_priv_proc()",
        teardown_sql=[
            f"DROP PROCEDURE IF EXISTS {CATALOG}.{SCHEMA}.tc84_low_priv_proc",
            f"DROP PROCEDURE IF EXISTS {CATALOG}.{SCHEMA}.tc84_medium_priv_proc",
            f"DROP PROCEDURE IF EXISTS {CATALOG}.{SCHEMA}.tc84_high_priv_proc",
            f"DROP TABLE IF EXISTS {CATALOG}.{SCHEMA}.tc84_level3_data",
        ]
    )
    tests.append(tc84)
    
    # TC-85: External Location Access Pattern
    tc85 = DefinerTestCase(
        test_id="TC-85",
        description="External Location - Test if DEFINER procedure exposes external location info",
        setup_sql=[
            # Create a procedure that would access external location (simulated)
            f"DROP PROCEDURE IF EXISTS {CATALOG}.{SCHEMA}.tc85_external_access",
            f"""
            CREATE PROCEDURE {CATALOG}.{SCHEMA}.tc85_external_access(location_path STRING)
            LANGUAGE SQL
            AS BEGIN
                -- Simulate external location access
                -- In real scenario, this would use COPY INTO or similar
                SELECT 
                    location_path as requested_path,
                    CURRENT_USER() as accessing_user,
                    'Would access external storage' as action;
            END
            """,
        ],
        test_sql=f"CALL {CATALOG}.{SCHEMA}.tc85_external_access('s3://bucket/path')",
        teardown_sql=[
            f"DROP PROCEDURE IF EXISTS {CATALOG}.{SCHEMA}.tc85_external_access",
        ]
    )
    tests.append(tc85)
    
    # TC-86: Deep Nesting Stress Test (20 levels)
    tc86 = DefinerTestCase(
        test_id="TC-86",
        description="Extreme Nesting - Test 20-level deep procedure nesting",
        setup_sql=[
            # Create 20 levels of nested procedures
            f"DROP PROCEDURE IF EXISTS {CATALOG}.{SCHEMA}.nest_level_20",
            f"""CREATE PROCEDURE {CATALOG}.{SCHEMA}.nest_level_20() LANGUAGE SQL AS BEGIN
                SELECT 20 as level;
            END""",
        ] + [
            f"""DROP PROCEDURE IF EXISTS {CATALOG}.{SCHEMA}.nest_level_{i:02d}"""
            for i in range(19, 0, -1)
        ] + [
            f"""CREATE PROCEDURE {CATALOG}.{SCHEMA}.nest_level_{i:02d}() LANGUAGE SQL AS BEGIN
                CALL {CATALOG}.{SCHEMA}.nest_level_{i+1:02d}();
            END"""
            for i in range(19, 0, -1)
        ],
        test_sql=f"CALL {CATALOG}.{SCHEMA}.nest_level_01()",
        teardown_sql=[
            f"DROP PROCEDURE IF EXISTS {CATALOG}.{SCHEMA}.nest_level_{i:02d}"
            for i in range(1, 21)
        ]
    )
    tests.append(tc86)
    
    return tests


if __name__ == "__main__":
    from framework.test_framework import DatabricksConnection, TestReporter
    from framework.config import SERVER_HOSTNAME, HTTP_PATH, PAT_TOKEN, CATALOG, SCHEMA
    
    print("=" * 80)
    print("🔥 PRIVILEGE ESCALATION TEST SUITE")
    print("=" * 80)
    print()
    print("Testing: Ownership transfer, confused deputy, privilege amplification,")
    print("         external access patterns, and extreme nesting (20 levels)")
    print()
    
    conn = DatabricksConnection(SERVER_HOSTNAME, HTTP_PATH, PAT_TOKEN, CATALOG, SCHEMA)
    executor = TestExecutor(conn)
    
    test_cases = get_tests()
    results = [executor.run_test(tc) for tc in test_cases]
    
    reporter = TestReporter(results)
    reporter.print_summary()
    
    conn.close()
    
    # Analyze specific results
    tc86_result = [r for r in results if r.test_id == "TC-86"]
    if tc86_result and tc86_result[0].status == "PASS":
        print()
        print("🎯 KEY FINDING: 20-level nesting works!")
        print("   This significantly exceeds the documented 4-level limit (KI-01)")
        print("   Actual limit may be much higher or unlimited")
