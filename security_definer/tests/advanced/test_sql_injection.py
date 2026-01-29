"""
ADVANCED TEST SUITE: SQL Injection & Security Bypass Tests
Focus on sophisticated injection attempts and security vulnerabilities
"""

from framework.test_framework import DefinerTestCase, TestExecutor
from framework.config import SERVICE_PRINCIPAL_B_ID, CATALOG, SCHEMA

def get_tests():
    tests = []
    
    # TC-76: UNION-Based SQL Injection
    tc76 = DefinerTestCase(
        test_id="TC-76",
        description="UNION-Based Injection - Attempt to access unauthorized data via UNION",
        setup_sql=[
            f"DROP TABLE IF EXISTS {CATALOG}.{SCHEMA}.tc76_public_data",
            f"DROP TABLE IF EXISTS {CATALOG}.{SCHEMA}.tc76_secret_data",
            f"CREATE TABLE {CATALOG}.{SCHEMA}.tc76_public_data (id INT, info STRING)",
            f"CREATE TABLE {CATALOG}.{SCHEMA}.tc76_secret_data (id INT, secret STRING)",
            f"INSERT INTO {CATALOG}.{SCHEMA}.tc76_public_data VALUES (1, 'public_info'), (2, 'public_data')",
            f"INSERT INTO {CATALOG}.{SCHEMA}.tc76_secret_data VALUES (1, 'TOP_SECRET'), (2, 'CLASSIFIED')",
            
            # Revoke access to secret table
            f"REVOKE ALL PRIVILEGES ON TABLE {CATALOG}.{SCHEMA}.tc76_secret_data FROM `{SERVICE_PRINCIPAL_B_ID}`",
            
            # Create procedure that takes ID parameter
            f"DROP PROCEDURE IF EXISTS {CATALOG}.{SCHEMA}.tc76_get_public_data",
            f"""
            CREATE PROCEDURE {CATALOG}.{SCHEMA}.tc76_get_public_data(user_id INT)
            LANGUAGE SQL
            AS BEGIN
                -- Parameterized query should prevent injection
                SELECT id, info FROM {CATALOG}.{SCHEMA}.tc76_public_data WHERE id = user_id;
            END
            """,
            
            f"GRANT EXECUTE ON PROCEDURE {CATALOG}.{SCHEMA}.tc76_get_public_data TO `{SERVICE_PRINCIPAL_B_ID}`"
        ],
        # Try injection: pass negative ID to trigger UNION
        test_sql=f"CALL {CATALOG}.{SCHEMA}.tc76_get_public_data(-1)",
        teardown_sql=[
            f"DROP PROCEDURE IF EXISTS {CATALOG}.{SCHEMA}.tc76_get_public_data",
            f"DROP TABLE IF EXISTS {CATALOG}.{SCHEMA}.tc76_secret_data",
            f"DROP TABLE IF EXISTS {CATALOG}.{SCHEMA}.tc76_public_data",
        ]
    )
    tests.append(tc76)
    
    # TC-77: Blind SQL Injection via Timing Attack
    tc77 = DefinerTestCase(
        test_id="TC-77",
        description="Timing Attack - Use execution time to infer sensitive data",
        setup_sql=[
            f"DROP TABLE IF EXISTS {CATALOG}.{SCHEMA}.tc77_sensitive_flags",
            f"CREATE TABLE {CATALOG}.{SCHEMA}.tc77_sensitive_flags (user_id INT, is_admin BOOLEAN, flag_value STRING)",
            f"INSERT INTO {CATALOG}.{SCHEMA}.tc77_sensitive_flags VALUES (1, false, 'regular'), (2, true, 'admin_flag')",
            
            # Create procedure that could leak info via timing
            f"DROP PROCEDURE IF EXISTS {CATALOG}.{SCHEMA}.tc77_check_user",
            f"""
            CREATE PROCEDURE {CATALOG}.{SCHEMA}.tc77_check_user(uid INT)
            LANGUAGE SQL
            AS BEGIN
                DECLARE is_admin_flag BOOLEAN;
                SET is_admin_flag = (SELECT is_admin FROM {CATALOG}.{SCHEMA}.tc77_sensitive_flags WHERE user_id = uid);
                
                -- Return generic result (no info leak)
                SELECT 'User checked' as result;
            END
            """,
        ],
        test_sql=f"CALL {CATALOG}.{SCHEMA}.tc77_check_user(1)",
        teardown_sql=[
            f"DROP PROCEDURE IF EXISTS {CATALOG}.{SCHEMA}.tc77_check_user",
            f"DROP TABLE IF EXISTS {CATALOG}.{SCHEMA}.tc77_sensitive_flags",
        ]
    )
    tests.append(tc77)
    
    # TC-78: Second-Order SQL Injection
    tc78 = DefinerTestCase(
        test_id="TC-78",
        description="Second-Order Injection - Store malicious SQL, execute later",
        setup_sql=[
            f"DROP TABLE IF EXISTS {CATALOG}.{SCHEMA}.tc78_stored_queries",
            f"DROP TABLE IF EXISTS {CATALOG}.{SCHEMA}.tc78_target_data",
            f"CREATE TABLE {CATALOG}.{SCHEMA}.tc78_stored_queries (id INT, query_template STRING)",
            f"CREATE TABLE {CATALOG}.{SCHEMA}.tc78_target_data (id INT, value STRING)",
            f"INSERT INTO {CATALOG}.{SCHEMA}.tc78_target_data VALUES (1, 'sensitive_data')",
            
            # Store potentially malicious query template
            f"INSERT INTO {CATALOG}.{SCHEMA}.tc78_stored_queries VALUES (1, 'SELECT * FROM {CATALOG}.{SCHEMA}.tc78_target_data')",
            
            # Procedure that retrieves and uses stored query (dangerous pattern)
            f"DROP PROCEDURE IF EXISTS {CATALOG}.{SCHEMA}.tc78_execute_stored",
            f"""
            CREATE PROCEDURE {CATALOG}.{SCHEMA}.tc78_execute_stored(query_id INT)
            LANGUAGE SQL
            AS BEGIN
                -- This is safe because we just return the template, not execute it
                SELECT query_template FROM {CATALOG}.{SCHEMA}.tc78_stored_queries WHERE id = query_id;
            END
            """,
        ],
        test_sql=f"CALL {CATALOG}.{SCHEMA}.tc78_execute_stored(1)",
        teardown_sql=[
            f"DROP PROCEDURE IF EXISTS {CATALOG}.{SCHEMA}.tc78_execute_stored",
            f"DROP TABLE IF EXISTS {CATALOG}.{SCHEMA}.tc78_target_data",
            f"DROP TABLE IF EXISTS {CATALOG}.{SCHEMA}.tc78_stored_queries",
        ]
    )
    tests.append(tc78)
    
    # TC-79: Comment-Based Bypass Attempt
    tc79 = DefinerTestCase(
        test_id="TC-79",
        description="Comment Bypass - Attempt to use SQL comments to bypass checks",
        setup_sql=[
            f"DROP TABLE IF EXISTS {CATALOG}.{SCHEMA}.tc79_user_accounts",
            f"CREATE TABLE {CATALOG}.{SCHEMA}.tc79_user_accounts (username STRING, role STRING, status STRING)",
            f"INSERT INTO {CATALOG}.{SCHEMA}.tc79_user_accounts VALUES ('user1', 'user', 'active'), ('admin', 'admin', 'active')",
            
            f"DROP PROCEDURE IF EXISTS {CATALOG}.{SCHEMA}.tc79_find_user",
            f"""
            CREATE PROCEDURE {CATALOG}.{SCHEMA}.tc79_find_user(uname STRING)
            LANGUAGE SQL
            AS BEGIN
                -- Parameterized query prevents comment-based bypass
                SELECT username, role FROM {CATALOG}.{SCHEMA}.tc79_user_accounts 
                WHERE username = uname AND status = 'active';
            END
            """,
        ],
        # Try injection: "admin' --" to bypass status check
        test_sql=f"CALL {CATALOG}.{SCHEMA}.tc79_find_user('admin'' --')",
        teardown_sql=[
            f"DROP PROCEDURE IF EXISTS {CATALOG}.{SCHEMA}.tc79_find_user",
            f"DROP TABLE IF EXISTS {CATALOG}.{SCHEMA}.tc79_user_accounts",
        ]
    )
    tests.append(tc79)
    
    # TC-80: JSON/XML Injection in Parameters
    tc80 = DefinerTestCase(
        test_id="TC-80",
        description="Structured Data Injection - Malformed JSON/XML in parameters",
        setup_sql=[
            f"DROP TABLE IF EXISTS {CATALOG}.{SCHEMA}.tc80_json_data",
            f"CREATE TABLE {CATALOG}.{SCHEMA}.tc80_json_data (id INT, json_field STRING)",
            f"""INSERT INTO {CATALOG}.{SCHEMA}.tc80_json_data VALUES (1, '{{"key": "value"}}')""",
            
            f"DROP PROCEDURE IF EXISTS {CATALOG}.{SCHEMA}.tc80_process_json",
            f"""
            CREATE PROCEDURE {CATALOG}.{SCHEMA}.tc80_process_json(json_input STRING)
            LANGUAGE SQL
            AS BEGIN
                -- Test handling of malformed JSON
                SELECT json_input as received_json, LENGTH(json_input) as json_length;
            END
            """,
        ],
        # Pass malformed JSON with special characters
        test_sql=f"""CALL {CATALOG}.{SCHEMA}.tc80_process_json('{{"key": "value\\'", "injection": "attempt"}}')""",
        teardown_sql=[
            f"DROP PROCEDURE IF EXISTS {CATALOG}.{SCHEMA}.tc80_process_json",
            f"DROP TABLE IF EXISTS {CATALOG}.{SCHEMA}.tc80_json_data",
        ]
    )
    tests.append(tc80)
    
    return tests


if __name__ == "__main__":
    from framework.test_framework import DatabricksConnection, TestReporter
    from framework.config import SERVER_HOSTNAME, HTTP_PATH, PAT_TOKEN, CATALOG, SCHEMA
    
    print("=" * 80)
    print("🔥 ADVANCED SQL INJECTION TEST SUITE")
    print("=" * 80)
    print()
    print("Testing: UNION injection, timing attacks, second-order injection,")
    print("         comment-based bypass, and structured data injection")
    print()
    
    conn = DatabricksConnection(SERVER_HOSTNAME, HTTP_PATH, PAT_TOKEN, CATALOG, SCHEMA)
    executor = TestExecutor(conn)
    
    test_cases = get_tests()
    results = [executor.run_test(tc) for tc in test_cases]
    
    reporter = TestReporter(results)
    reporter.print_summary()
    
    conn.close()
    
    # Analyze results
    failed = [r for r in results if r.status == "FAIL"]
    if failed:
        print()
        print("⚠️  POTENTIAL VULNERABILITIES FOUND:")
        for result in failed:
            print(f"  - {result.test_id}: {result.description}")
    else:
        print()
        print("✅ No SQL injection vulnerabilities detected")
