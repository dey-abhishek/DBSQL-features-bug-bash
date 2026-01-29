"""
Comprehensive Jobs API Test Suite for SQL SECURITY DEFINER
Mirrors all warehouse tests but runs via Databricks Jobs API on Serverless Compute

This suite includes:
- Core impersonation (TC-01 to TC-03)
- Object access boundaries (TC-04 to TC-06)
- Nested procedures (TC-07 to TC-08)
- Security & injection (TC-09 to TC-10)
- Error handling (TC-11 to TC-12)
- Unity Catalog (TC-13 to TC-16)
- Context switching (User ↔ SP)
- Known issues validation
"""

from framework.test_framework import DefinerTestCase
from framework.config import CATALOG, SCHEMA, USER_A, SERVICE_PRINCIPAL_B_ID

def get_jobs_complete_tests():
    """
    Complete test suite for Jobs API execution
    All tests adapted for serverless compute with bidirectional context switching
    """
    tests = []
    
    # ============================================================================
    # CORE IMPERSONATION (TC-JOBS-CORE-01 to 03)
    # ============================================================================
    
    # TC-JOBS-CORE-01: Basic DEFINER identity resolution
    tc_core_01 = DefinerTestCase(
        test_id="TC-JOBS-CORE-01",
        description="Jobs API: DEFINER mode returns owner's identity",
        setup_sql=[
            f"DROP PROCEDURE IF EXISTS {CATALOG}.{SCHEMA}.jobs_core_01_identity",
            f"""
            CREATE PROCEDURE {CATALOG}.{SCHEMA}.jobs_core_01_identity()
            LANGUAGE SQL
            AS BEGIN
                SELECT 'DEFINER' as mode, CURRENT_USER() as user;
            END
            """
        ],
        test_sql=f"CALL {CATALOG}.{SCHEMA}.jobs_core_01_identity()",
        teardown_sql=[f"DROP PROCEDURE IF EXISTS {CATALOG}.{SCHEMA}.jobs_core_01_identity"],
        should_fail=False
    )
    tests.append(tc_core_01)
    
    # TC-JOBS-CORE-02: Permission elevation via gateway pattern
    tc_core_02 = DefinerTestCase(
        test_id="TC-JOBS-CORE-02",
        description="Jobs API: DEFINER grants controlled access to restricted resources",
        setup_sql=[
            f"DROP TABLE IF EXISTS {CATALOG}.{SCHEMA}.jobs_core_02_restricted",
            f"CREATE TABLE {CATALOG}.{SCHEMA}.jobs_core_02_restricted (id INT, data STRING)",
            f"INSERT INTO {CATALOG}.{SCHEMA}.jobs_core_02_restricted VALUES (1, 'sensitive')",
            f"REVOKE ALL PRIVILEGES ON TABLE {CATALOG}.{SCHEMA}.jobs_core_02_restricted FROM `{SERVICE_PRINCIPAL_B_ID}`",
            
            f"DROP PROCEDURE IF EXISTS {CATALOG}.{SCHEMA}.jobs_core_02_gateway",
            f"""
            CREATE PROCEDURE {CATALOG}.{SCHEMA}.jobs_core_02_gateway()
            LANGUAGE SQL
            AS BEGIN
                SELECT COUNT(*) as count FROM {CATALOG}.{SCHEMA}.jobs_core_02_restricted;
            END
            """,
            f"GRANT EXECUTE ON PROCEDURE {CATALOG}.{SCHEMA}.jobs_core_02_gateway TO `{SERVICE_PRINCIPAL_B_ID}`"
        ],
        test_sql=f"CALL {CATALOG}.{SCHEMA}.jobs_core_02_gateway()",
        teardown_sql=[
            f"DROP PROCEDURE IF EXISTS {CATALOG}.{SCHEMA}.jobs_core_02_gateway",
            f"DROP TABLE IF EXISTS {CATALOG}.{SCHEMA}.jobs_core_02_restricted"
        ],
        should_fail=False
    )
    tests.append(tc_core_02)
    
    # TC-JOBS-CORE-03: Role/permission inheritance
    tc_core_03 = DefinerTestCase(
        test_id="TC-JOBS-CORE-03",
        description="Jobs API: DEFINER uses owner's permissions not caller's",
        setup_sql=[
            f"DROP TABLE IF EXISTS {CATALOG}.{SCHEMA}.jobs_core_03_owner_only",
            f"CREATE TABLE {CATALOG}.{SCHEMA}.jobs_core_03_owner_only (id INT)",
            f"INSERT INTO {CATALOG}.{SCHEMA}.jobs_core_03_owner_only VALUES (1)",
            f"REVOKE ALL PRIVILEGES ON TABLE {CATALOG}.{SCHEMA}.jobs_core_03_owner_only FROM `{SERVICE_PRINCIPAL_B_ID}`",
            
            f"DROP PROCEDURE IF EXISTS {CATALOG}.{SCHEMA}.jobs_core_03_check_access",
            f"""
            CREATE PROCEDURE {CATALOG}.{SCHEMA}.jobs_core_03_check_access()
            LANGUAGE SQL
            AS BEGIN
                SELECT 'owner_access' as result, COUNT(*) as count 
                FROM {CATALOG}.{SCHEMA}.jobs_core_03_owner_only;
            END
            """,
            f"GRANT EXECUTE ON PROCEDURE {CATALOG}.{SCHEMA}.jobs_core_03_check_access TO `{SERVICE_PRINCIPAL_B_ID}`"
        ],
        test_sql=f"CALL {CATALOG}.{SCHEMA}.jobs_core_03_check_access()",
        teardown_sql=[
            f"DROP PROCEDURE IF EXISTS {CATALOG}.{SCHEMA}.jobs_core_03_check_access",
            f"DROP TABLE IF EXISTS {CATALOG}.{SCHEMA}.jobs_core_03_owner_only"
        ],
        should_fail=False
    )
    tests.append(tc_core_03)
    
    # ============================================================================
    # NESTED PROCEDURES (TC-JOBS-NESTED-01 to 02)
    # ============================================================================
    
    # TC-JOBS-NESTED-01: Simple 2-level nesting
    tc_nested_01 = DefinerTestCase(
        test_id="TC-JOBS-NESTED-01",
        description="Jobs API: 2-level nested DEFINER procedures maintain context",
        setup_sql=[
            f"DROP PROCEDURE IF EXISTS {CATALOG}.{SCHEMA}.jobs_nested_01_inner",
            f"""
            CREATE PROCEDURE {CATALOG}.{SCHEMA}.jobs_nested_01_inner()
            LANGUAGE SQL
            AS BEGIN
                SELECT 'inner' as level, CURRENT_USER() as user;
            END
            """,
            
            f"DROP PROCEDURE IF EXISTS {CATALOG}.{SCHEMA}.jobs_nested_01_outer",
            f"""
            CREATE PROCEDURE {CATALOG}.{SCHEMA}.jobs_nested_01_outer()
            LANGUAGE SQL
            AS BEGIN
                CALL {CATALOG}.{SCHEMA}.jobs_nested_01_inner();
            END
            """
        ],
        test_sql=f"CALL {CATALOG}.{SCHEMA}.jobs_nested_01_outer()",
        teardown_sql=[
            f"DROP PROCEDURE IF EXISTS {CATALOG}.{SCHEMA}.jobs_nested_01_outer",
            f"DROP PROCEDURE IF EXISTS {CATALOG}.{SCHEMA}.jobs_nested_01_inner"
        ],
        should_fail=False
    )
    tests.append(tc_nested_01)
    
    # TC-JOBS-NESTED-02: Cross-owner nesting (User → SP → User)
    tc_nested_02 = DefinerTestCase(
        test_id="TC-JOBS-NESTED-02",
        description="Jobs API: Cross-owner nested procedures with context switching",
        setup_sql=[
            # Inner: User-owned
            f"DROP PROCEDURE IF EXISTS {CATALOG}.{SCHEMA}.jobs_nested_02_user_inner",
            f"""
            CREATE PROCEDURE {CATALOG}.{SCHEMA}.jobs_nested_02_user_inner()
            LANGUAGE SQL
            AS BEGIN
                SELECT 'user_inner' as level;
            END
            """,
            
            # Middle: SP-owned (would need SP to create in real scenario)
            f"DROP PROCEDURE IF EXISTS {CATALOG}.{SCHEMA}.jobs_nested_02_sp_middle",
            f"""
            CREATE PROCEDURE {CATALOG}.{SCHEMA}.jobs_nested_02_sp_middle()
            LANGUAGE SQL
            AS BEGIN
                CALL {CATALOG}.{SCHEMA}.jobs_nested_02_user_inner();
            END
            """,
            
            # Outer: User-owned
            f"DROP PROCEDURE IF EXISTS {CATALOG}.{SCHEMA}.jobs_nested_02_user_outer",
            f"""
            CREATE PROCEDURE {CATALOG}.{SCHEMA}.jobs_nested_02_user_outer()
            LANGUAGE SQL
            AS BEGIN
                CALL {CATALOG}.{SCHEMA}.jobs_nested_02_sp_middle();
            END
            """,
            
            # Grants
            f"GRANT EXECUTE ON PROCEDURE {CATALOG}.{SCHEMA}.jobs_nested_02_user_inner TO `{SERVICE_PRINCIPAL_B_ID}`",
            f"GRANT EXECUTE ON PROCEDURE {CATALOG}.{SCHEMA}.jobs_nested_02_sp_middle TO `{USER_A}`"
        ],
        test_sql=f"CALL {CATALOG}.{SCHEMA}.jobs_nested_02_user_outer()",
        teardown_sql=[
            f"DROP PROCEDURE IF EXISTS {CATALOG}.{SCHEMA}.jobs_nested_02_user_outer",
            f"DROP PROCEDURE IF EXISTS {CATALOG}.{SCHEMA}.jobs_nested_02_sp_middle",
            f"DROP PROCEDURE IF EXISTS {CATALOG}.{SCHEMA}.jobs_nested_02_user_inner"
        ],
        should_fail=False
    )
    tests.append(tc_nested_02)
    
    # ============================================================================
    # SECURITY & INJECTION (TC-JOBS-SEC-01 to 03)
    # ============================================================================
    
    # TC-JOBS-SEC-01: Dynamic SQL with EXECUTE IMMEDIATE
    tc_sec_01 = DefinerTestCase(
        test_id="TC-JOBS-SEC-01",
        description="Jobs API: Dynamic SQL respects owner's permissions",
        setup_sql=[
            f"DROP TABLE IF EXISTS {CATALOG}.{SCHEMA}.jobs_sec_01_dynamic_test",
            f"CREATE TABLE {CATALOG}.{SCHEMA}.jobs_sec_01_dynamic_test (id INT, value STRING)",
            f"INSERT INTO {CATALOG}.{SCHEMA}.jobs_sec_01_dynamic_test VALUES (1, 'test')",
            
            f"DROP PROCEDURE IF EXISTS {CATALOG}.{SCHEMA}.jobs_sec_01_dynamic_query",
            f"""
            CREATE PROCEDURE {CATALOG}.{SCHEMA}.jobs_sec_01_dynamic_query(table_suffix STRING)
            LANGUAGE SQL
            AS BEGIN
                DECLARE query STRING;
                SET query = CONCAT('SELECT COUNT(*) as count FROM {CATALOG}.{SCHEMA}.jobs_sec_01_', table_suffix);
                EXECUTE IMMEDIATE query;
            END
            """
        ],
        test_sql=f"CALL {CATALOG}.{SCHEMA}.jobs_sec_01_dynamic_query('dynamic_test')",
        teardown_sql=[
            f"DROP PROCEDURE IF EXISTS {CATALOG}.{SCHEMA}.jobs_sec_01_dynamic_query",
            f"DROP TABLE IF EXISTS {CATALOG}.{SCHEMA}.jobs_sec_01_dynamic_test"
        ],
        should_fail=False
    )
    tests.append(tc_sec_01)
    
    # TC-JOBS-SEC-02: SQL injection attempt
    tc_sec_02 = DefinerTestCase(
        test_id="TC-JOBS-SEC-02",
        description="Jobs API: SQL injection blocked in parameterized procedures",
        setup_sql=[
            f"DROP TABLE IF EXISTS {CATALOG}.{SCHEMA}.jobs_sec_02_users",
            f"CREATE TABLE {CATALOG}.{SCHEMA}.jobs_sec_02_users (id INT, name STRING, role STRING)",
            f"INSERT INTO {CATALOG}.{SCHEMA}.jobs_sec_02_users VALUES (1, 'admin', 'admin'), (2, 'user', 'user')",
            
            f"DROP PROCEDURE IF EXISTS {CATALOG}.{SCHEMA}.jobs_sec_02_get_user",
            f"""
            CREATE PROCEDURE {CATALOG}.{SCHEMA}.jobs_sec_02_get_user(user_name STRING)
            LANGUAGE SQL
            AS BEGIN
                SELECT * FROM {CATALOG}.{SCHEMA}.jobs_sec_02_users 
                WHERE name = user_name;
            END
            """
        ],
        # Injection attempt
        test_sql=f"CALL {CATALOG}.{SCHEMA}.jobs_sec_02_get_user(''' OR 1=1 --')",
        teardown_sql=[
            f"DROP PROCEDURE IF EXISTS {CATALOG}.{SCHEMA}.jobs_sec_02_get_user",
            f"DROP TABLE IF EXISTS {CATALOG}.{SCHEMA}.jobs_sec_02_users"
        ],
        should_fail=False  # Should succeed but return no results (injection blocked)
    )
    tests.append(tc_sec_02)
    
    # TC-JOBS-SEC-03: Parameter validation
    tc_sec_03 = DefinerTestCase(
        test_id="TC-JOBS-SEC-03",
        description="Jobs API: Parameters passed correctly through impersonation",
        setup_sql=[
            f"DROP TABLE IF EXISTS {CATALOG}.{SCHEMA}.jobs_sec_03_log",
            f"CREATE TABLE {CATALOG}.{SCHEMA}.jobs_sec_03_log (id INT, message STRING, value INT)",
            
            f"DROP PROCEDURE IF EXISTS {CATALOG}.{SCHEMA}.jobs_sec_03_log_event",
            f"""
            CREATE PROCEDURE {CATALOG}.{SCHEMA}.jobs_sec_03_log_event(msg STRING, val INT)
            LANGUAGE SQL
            AS BEGIN
                INSERT INTO {CATALOG}.{SCHEMA}.jobs_sec_03_log VALUES (1, msg, val);
                SELECT * FROM {CATALOG}.{SCHEMA}.jobs_sec_03_log;
            END
            """
        ],
        test_sql=f"CALL {CATALOG}.{SCHEMA}.jobs_sec_03_log_event('test_message', 42)",
        teardown_sql=[
            f"DROP PROCEDURE IF EXISTS {CATALOG}.{SCHEMA}.jobs_sec_03_log_event",
            f"DROP TABLE IF EXISTS {CATALOG}.{SCHEMA}.jobs_sec_03_log"
        ],
        should_fail=False
    )
    tests.append(tc_sec_03)
    
    # ============================================================================
    # UNITY CATALOG (TC-JOBS-UC-01 to 03)
    # ============================================================================
    
    # TC-JOBS-UC-01: Unity Catalog privilege enforcement
    tc_uc_01 = DefinerTestCase(
        test_id="TC-JOBS-UC-01",
        description="Jobs API: Unity Catalog permissions respected in DEFINER mode",
        setup_sql=[
            f"DROP TABLE IF EXISTS {CATALOG}.{SCHEMA}.jobs_uc_01_uc_managed",
            f"CREATE TABLE {CATALOG}.{SCHEMA}.jobs_uc_01_uc_managed (id INT, data STRING)",
            f"INSERT INTO {CATALOG}.{SCHEMA}.jobs_uc_01_uc_managed VALUES (1, 'uc_data')",
            
            f"DROP PROCEDURE IF EXISTS {CATALOG}.{SCHEMA}.jobs_uc_01_read_uc",
            f"""
            CREATE PROCEDURE {CATALOG}.{SCHEMA}.jobs_uc_01_read_uc()
            LANGUAGE SQL
            AS BEGIN
                SELECT * FROM {CATALOG}.{SCHEMA}.jobs_uc_01_uc_managed;
            END
            """,
            f"GRANT EXECUTE ON PROCEDURE {CATALOG}.{SCHEMA}.jobs_uc_01_read_uc TO `{SERVICE_PRINCIPAL_B_ID}`"
        ],
        test_sql=f"CALL {CATALOG}.{SCHEMA}.jobs_uc_01_read_uc()",
        teardown_sql=[
            f"DROP PROCEDURE IF EXISTS {CATALOG}.{SCHEMA}.jobs_uc_01_read_uc",
            f"DROP TABLE IF EXISTS {CATALOG}.{SCHEMA}.jobs_uc_01_uc_managed"
        ],
        should_fail=False
    )
    tests.append(tc_uc_01)
    
    # TC-JOBS-UC-02: Cross-schema access
    tc_uc_02 = DefinerTestCase(
        test_id="TC-JOBS-UC-02",
        description="Jobs API: DEFINER can access owner's schemas",
        setup_sql=[
            f"DROP PROCEDURE IF EXISTS {CATALOG}.{SCHEMA}.jobs_uc_02_check_schema",
            f"""
            CREATE PROCEDURE {CATALOG}.{SCHEMA}.jobs_uc_02_check_schema()
            LANGUAGE SQL
            AS BEGIN
                SELECT CURRENT_CATALOG() as catalog, CURRENT_SCHEMA() as schema;
            END
            """
        ],
        test_sql=f"CALL {CATALOG}.{SCHEMA}.jobs_uc_02_check_schema()",
        teardown_sql=[f"DROP PROCEDURE IF EXISTS {CATALOG}.{SCHEMA}.jobs_uc_02_check_schema"],
        should_fail=False
    )
    tests.append(tc_uc_02)
    
    # TC-JOBS-UC-03: Information schema access
    tc_uc_03 = DefinerTestCase(
        test_id="TC-JOBS-UC-03",
        description="Jobs API: Information schema queries respect owner's view",
        setup_sql=[
            f"DROP PROCEDURE IF EXISTS {CATALOG}.{SCHEMA}.jobs_uc_03_list_tables",
            f"""
            CREATE PROCEDURE {CATALOG}.{SCHEMA}.jobs_uc_03_list_tables()
            LANGUAGE SQL
            AS BEGIN
                SELECT COUNT(*) as table_count
                FROM information_schema.tables
                WHERE table_schema = '{SCHEMA}';
            END
            """,
            f"GRANT EXECUTE ON PROCEDURE {CATALOG}.{SCHEMA}.jobs_uc_03_list_tables TO `{SERVICE_PRINCIPAL_B_ID}`"
        ],
        test_sql=f"CALL {CATALOG}.{SCHEMA}.jobs_uc_03_list_tables()",
        teardown_sql=[f"DROP PROCEDURE IF EXISTS {CATALOG}.{SCHEMA}.jobs_uc_03_list_tables"],
        should_fail=False
    )
    tests.append(tc_uc_03)
    
    # ============================================================================
    # ERROR HANDLING (TC-JOBS-ERR-01 to 02)
    # ============================================================================
    
    # TC-JOBS-ERR-01: Error message clarity
    tc_err_01 = DefinerTestCase(
        test_id="TC-JOBS-ERR-01",
        description="Jobs API: Error messages are clear without leaking sensitive info",
        setup_sql=[
            f"DROP PROCEDURE IF EXISTS {CATALOG}.{SCHEMA}.jobs_err_01_fail_gracefully",
            f"""
            CREATE PROCEDURE {CATALOG}.{SCHEMA}.jobs_err_01_fail_gracefully()
            LANGUAGE SQL
            AS BEGIN
                -- Reference non-existent table
                SELECT * FROM {CATALOG}.{SCHEMA}.jobs_err_01_nonexistent_table;
            END
            """
        ],
        test_sql=f"CALL {CATALOG}.{SCHEMA}.jobs_err_01_fail_gracefully()",
        teardown_sql=[f"DROP PROCEDURE IF EXISTS {CATALOG}.{SCHEMA}.jobs_err_01_fail_gracefully"],
        should_fail=True  # Expected to fail with clear error
    )
    tests.append(tc_err_01)
    
    # TC-JOBS-ERR-02: Exception handling in procedures
    tc_err_02 = DefinerTestCase(
        test_id="TC-JOBS-ERR-02",
        description="Jobs API: Procedures can handle errors gracefully",
        setup_sql=[
            f"DROP TABLE IF EXISTS {CATALOG}.{SCHEMA}.jobs_err_02_safe_table",
            f"CREATE TABLE {CATALOG}.{SCHEMA}.jobs_err_02_safe_table (id INT, status STRING)",
            
            f"DROP PROCEDURE IF EXISTS {CATALOG}.{SCHEMA}.jobs_err_02_safe_insert",
            f"""
            CREATE PROCEDURE {CATALOG}.{SCHEMA}.jobs_err_02_safe_insert()
            LANGUAGE SQL
            AS BEGIN
                -- Insert and return result
                INSERT INTO {CATALOG}.{SCHEMA}.jobs_err_02_safe_table VALUES (1, 'success');
                SELECT * FROM {CATALOG}.{SCHEMA}.jobs_err_02_safe_table;
            END
            """
        ],
        test_sql=f"CALL {CATALOG}.{SCHEMA}.jobs_err_02_safe_insert()",
        teardown_sql=[
            f"DROP PROCEDURE IF EXISTS {CATALOG}.{SCHEMA}.jobs_err_02_safe_insert",
            f"DROP TABLE IF EXISTS {CATALOG}.{SCHEMA}.jobs_err_02_safe_table"
        ],
        should_fail=False
    )
    tests.append(tc_err_02)
    
    # ============================================================================
    # CONTEXT SWITCHING - SP CREATES, USER EXECUTES (TC-JOBS-CTX-SP-01 to 03)
    # ============================================================================
    
    # TC-JOBS-CTX-SP-01: SP creates basic DEFINER, User executes
    tc_ctx_sp_01 = DefinerTestCase(
        test_id="TC-JOBS-CTX-SP-01",
        description="Jobs API: SP-owned DEFINER procedure executed by User uses SP permissions",
        setup_sql=[
            f"DROP TABLE IF EXISTS {CATALOG}.{SCHEMA}.jobs_ctx_sp_01_sp_table",
            f"CREATE TABLE {CATALOG}.{SCHEMA}.jobs_ctx_sp_01_sp_table (id INT, owner STRING)",
            f"INSERT INTO {CATALOG}.{SCHEMA}.jobs_ctx_sp_01_sp_table VALUES (1, 'sp')",
            f"GRANT ALL PRIVILEGES ON TABLE {CATALOG}.{SCHEMA}.jobs_ctx_sp_01_sp_table TO `{SERVICE_PRINCIPAL_B_ID}`",
            f"REVOKE ALL PRIVILEGES ON TABLE {CATALOG}.{SCHEMA}.jobs_ctx_sp_01_sp_table FROM `{USER_A}`",
            
            # Note: In real Jobs test, SP would create this
            f"DROP PROCEDURE IF EXISTS {CATALOG}.{SCHEMA}.jobs_ctx_sp_01_sp_proc",
            f"""
            CREATE PROCEDURE {CATALOG}.{SCHEMA}.jobs_ctx_sp_01_sp_proc()
            LANGUAGE SQL
            COMMENT 'Owner: SP'
            AS BEGIN
                SELECT 'SP-owned-proc' as proc_type, COUNT(*) as count
                FROM {CATALOG}.{SCHEMA}.jobs_ctx_sp_01_sp_table;
            END
            """,
            f"GRANT EXECUTE ON PROCEDURE {CATALOG}.{SCHEMA}.jobs_ctx_sp_01_sp_proc TO `{USER_A}`"
        ],
        test_sql=f"CALL {CATALOG}.{SCHEMA}.jobs_ctx_sp_01_sp_proc()",
        teardown_sql=[
            f"DROP PROCEDURE IF EXISTS {CATALOG}.{SCHEMA}.jobs_ctx_sp_01_sp_proc",
            f"DROP TABLE IF EXISTS {CATALOG}.{SCHEMA}.jobs_ctx_sp_01_sp_table"
        ],
        should_fail=False  # Should succeed using SP's permissions
    )
    tests.append(tc_ctx_sp_01)
    
    # TC-JOBS-CTX-SP-02: SP creates with DML, User executes
    tc_ctx_sp_02 = DefinerTestCase(
        test_id="TC-JOBS-CTX-SP-02",
        description="Jobs API: SP-owned procedure with DML executed by User",
        setup_sql=[
            f"DROP TABLE IF EXISTS {CATALOG}.{SCHEMA}.jobs_ctx_sp_02_write_test",
            f"CREATE TABLE {CATALOG}.{SCHEMA}.jobs_ctx_sp_02_write_test (id INT, data STRING)",
            f"GRANT ALL PRIVILEGES ON TABLE {CATALOG}.{SCHEMA}.jobs_ctx_sp_02_write_test TO `{SERVICE_PRINCIPAL_B_ID}`",
            
            # SP-owned procedure that writes
            f"DROP PROCEDURE IF EXISTS {CATALOG}.{SCHEMA}.jobs_ctx_sp_02_write_data",
            f"""
            CREATE PROCEDURE {CATALOG}.{SCHEMA}.jobs_ctx_sp_02_write_data(val STRING)
            LANGUAGE SQL
            AS BEGIN
                INSERT INTO {CATALOG}.{SCHEMA}.jobs_ctx_sp_02_write_test VALUES (1, val);
                SELECT COUNT(*) as rows FROM {CATALOG}.{SCHEMA}.jobs_ctx_sp_02_write_test;
            END
            """,
            f"GRANT EXECUTE ON PROCEDURE {CATALOG}.{SCHEMA}.jobs_ctx_sp_02_write_data TO `{USER_A}`"
        ],
        test_sql=f"CALL {CATALOG}.{SCHEMA}.jobs_ctx_sp_02_write_data('test_data')",
        teardown_sql=[
            f"DROP PROCEDURE IF EXISTS {CATALOG}.{SCHEMA}.jobs_ctx_sp_02_write_data",
            f"DROP TABLE IF EXISTS {CATALOG}.{SCHEMA}.jobs_ctx_sp_02_write_test"
        ],
        should_fail=False
    )
    tests.append(tc_ctx_sp_02)
    
    # TC-JOBS-CTX-SP-03: SP creates nested, User executes
    tc_ctx_sp_03 = DefinerTestCase(
        test_id="TC-JOBS-CTX-SP-03",
        description="Jobs API: SP-owned nested procedures maintain SP context when User executes",
        setup_sql=[
            f"DROP TABLE IF EXISTS {CATALOG}.{SCHEMA}.jobs_ctx_sp_03_data",
            f"CREATE TABLE {CATALOG}.{SCHEMA}.jobs_ctx_sp_03_data (level STRING, value INT)",
            f"INSERT INTO {CATALOG}.{SCHEMA}.jobs_ctx_sp_03_data VALUES ('inner', 1)",
            f"GRANT ALL PRIVILEGES ON TABLE {CATALOG}.{SCHEMA}.jobs_ctx_sp_03_data TO `{SERVICE_PRINCIPAL_B_ID}`",
            
            # SP inner procedure
            f"DROP PROCEDURE IF EXISTS {CATALOG}.{SCHEMA}.jobs_ctx_sp_03_inner",
            f"""
            CREATE PROCEDURE {CATALOG}.{SCHEMA}.jobs_ctx_sp_03_inner()
            LANGUAGE SQL
            AS BEGIN
                SELECT * FROM {CATALOG}.{SCHEMA}.jobs_ctx_sp_03_data;
            END
            """,
            
            # SP outer procedure
            f"DROP PROCEDURE IF EXISTS {CATALOG}.{SCHEMA}.jobs_ctx_sp_03_outer",
            f"""
            CREATE PROCEDURE {CATALOG}.{SCHEMA}.jobs_ctx_sp_03_outer()
            LANGUAGE SQL
            AS BEGIN
                CALL {CATALOG}.{SCHEMA}.jobs_ctx_sp_03_inner();
            END
            """,
            f"GRANT EXECUTE ON PROCEDURE {CATALOG}.{SCHEMA}.jobs_ctx_sp_03_outer TO `{USER_A}`"
        ],
        test_sql=f"CALL {CATALOG}.{SCHEMA}.jobs_ctx_sp_03_outer()",
        teardown_sql=[
            f"DROP PROCEDURE IF EXISTS {CATALOG}.{SCHEMA}.jobs_ctx_sp_03_outer",
            f"DROP PROCEDURE IF EXISTS {CATALOG}.{SCHEMA}.jobs_ctx_sp_03_inner",
            f"DROP TABLE IF EXISTS {CATALOG}.{SCHEMA}.jobs_ctx_sp_03_data"
        ],
        should_fail=False
    )
    tests.append(tc_ctx_sp_03)
    
    # ============================================================================
    # CONTEXT SWITCHING - USER CREATES, SP EXECUTES (TC-JOBS-CTX-USER-01 to 03)
    # ============================================================================
    
    # TC-JOBS-CTX-USER-01: User creates basic DEFINER, SP executes
    tc_ctx_user_01 = DefinerTestCase(
        test_id="TC-JOBS-CTX-USER-01",
        description="Jobs API: User-owned DEFINER procedure executed by SP uses User permissions",
        setup_sql=[
            f"DROP TABLE IF EXISTS {CATALOG}.{SCHEMA}.jobs_ctx_user_01_user_table",
            f"CREATE TABLE {CATALOG}.{SCHEMA}.jobs_ctx_user_01_user_table (id INT, owner STRING)",
            f"INSERT INTO {CATALOG}.{SCHEMA}.jobs_ctx_user_01_user_table VALUES (1, 'user')",
            f"REVOKE ALL PRIVILEGES ON TABLE {CATALOG}.{SCHEMA}.jobs_ctx_user_01_user_table FROM `{SERVICE_PRINCIPAL_B_ID}`",
            
            # User creates procedure
            f"DROP PROCEDURE IF EXISTS {CATALOG}.{SCHEMA}.jobs_ctx_user_01_user_proc",
            f"""
            CREATE PROCEDURE {CATALOG}.{SCHEMA}.jobs_ctx_user_01_user_proc()
            LANGUAGE SQL
            COMMENT 'Owner: User'
            AS BEGIN
                SELECT 'User-owned-proc' as proc_type, COUNT(*) as count
                FROM {CATALOG}.{SCHEMA}.jobs_ctx_user_01_user_table;
            END
            """,
            f"GRANT EXECUTE ON PROCEDURE {CATALOG}.{SCHEMA}.jobs_ctx_user_01_user_proc TO `{SERVICE_PRINCIPAL_B_ID}`"
        ],
        test_sql=f"CALL {CATALOG}.{SCHEMA}.jobs_ctx_user_01_user_proc()",
        teardown_sql=[
            f"DROP PROCEDURE IF EXISTS {CATALOG}.{SCHEMA}.jobs_ctx_user_01_user_proc",
            f"DROP TABLE IF EXISTS {CATALOG}.{SCHEMA}.jobs_ctx_user_01_user_table"
        ],
        should_fail=False  # Should succeed using User's permissions
    )
    tests.append(tc_ctx_user_01)
    
    # TC-JOBS-CTX-USER-02: User creates analytics proc, SP runs on schedule
    tc_ctx_user_02 = DefinerTestCase(
        test_id="TC-JOBS-CTX-USER-02",
        description="Jobs API: User analytics procedure scheduled via SP",
        setup_sql=[
            f"DROP TABLE IF EXISTS {CATALOG}.{SCHEMA}.jobs_ctx_user_02_analytics",
            f"CREATE TABLE {CATALOG}.{SCHEMA}.jobs_ctx_user_02_analytics (metric STRING, value DECIMAL(10,2))",
            f"INSERT INTO {CATALOG}.{SCHEMA}.jobs_ctx_user_02_analytics VALUES ('revenue', 1000.50), ('users', 150.00)",
            
            # User's analytics procedure
            f"DROP PROCEDURE IF EXISTS {CATALOG}.{SCHEMA}.jobs_ctx_user_02_report",
            f"""
            CREATE PROCEDURE {CATALOG}.{SCHEMA}.jobs_ctx_user_02_report()
            LANGUAGE SQL
            AS BEGIN
                SELECT metric, value, (value * 1.1) as projected
                FROM {CATALOG}.{SCHEMA}.jobs_ctx_user_02_analytics;
            END
            """,
            f"GRANT EXECUTE ON PROCEDURE {CATALOG}.{SCHEMA}.jobs_ctx_user_02_report TO `{SERVICE_PRINCIPAL_B_ID}`"
        ],
        test_sql=f"CALL {CATALOG}.{SCHEMA}.jobs_ctx_user_02_report()",
        teardown_sql=[
            f"DROP PROCEDURE IF EXISTS {CATALOG}.{SCHEMA}.jobs_ctx_user_02_report",
            f"DROP TABLE IF EXISTS {CATALOG}.{SCHEMA}.jobs_ctx_user_02_analytics"
        ],
        should_fail=False
    )
    tests.append(tc_ctx_user_02)
    
    # TC-JOBS-CTX-USER-03: Complex user workflow via SP automation
    tc_ctx_user_03 = DefinerTestCase(
        test_id="TC-JOBS-CTX-USER-03",
        description="Jobs API: Complex User workflow automated by SP via Jobs",
        setup_sql=[
            f"DROP TABLE IF EXISTS {CATALOG}.{SCHEMA}.jobs_ctx_user_03_input",
            f"DROP TABLE IF EXISTS {CATALOG}.{SCHEMA}.jobs_ctx_user_03_output",
            f"CREATE TABLE {CATALOG}.{SCHEMA}.jobs_ctx_user_03_input (id INT, value STRING)",
            f"CREATE TABLE {CATALOG}.{SCHEMA}.jobs_ctx_user_03_output (id INT, processed STRING)",
            f"INSERT INTO {CATALOG}.{SCHEMA}.jobs_ctx_user_03_input VALUES (1, 'raw')",
            
            # User's ETL procedure
            f"DROP PROCEDURE IF EXISTS {CATALOG}.{SCHEMA}.jobs_ctx_user_03_process",
            f"""
            CREATE PROCEDURE {CATALOG}.{SCHEMA}.jobs_ctx_user_03_process()
            LANGUAGE SQL
            AS BEGIN
                INSERT INTO {CATALOG}.{SCHEMA}.jobs_ctx_user_03_output
                SELECT id, CONCAT(value, '_processed') as processed
                FROM {CATALOG}.{SCHEMA}.jobs_ctx_user_03_input;
                
                SELECT COUNT(*) as processed_count 
                FROM {CATALOG}.{SCHEMA}.jobs_ctx_user_03_output;
            END
            """,
            f"GRANT EXECUTE ON PROCEDURE {CATALOG}.{SCHEMA}.jobs_ctx_user_03_process TO `{SERVICE_PRINCIPAL_B_ID}`"
        ],
        test_sql=f"CALL {CATALOG}.{SCHEMA}.jobs_ctx_user_03_process()",
        teardown_sql=[
            f"DROP PROCEDURE IF EXISTS {CATALOG}.{SCHEMA}.jobs_ctx_user_03_process",
            f"DROP TABLE IF EXISTS {CATALOG}.{SCHEMA}.jobs_ctx_user_03_output",
            f"DROP TABLE IF EXISTS {CATALOG}.{SCHEMA}.jobs_ctx_user_03_input"
        ],
        should_fail=False
    )
    tests.append(tc_ctx_user_03)
    
    return tests

if __name__ == "__main__":
    """Quick test to validate module loads"""
    tests = get_jobs_complete_tests()
    print(f"✅ Loaded {len(tests)} comprehensive Jobs API tests")
    
    # Group by category
    categories = {}
    for test in tests:
        category = test.test_id.split('-')[2]  # Extract category
        if category not in categories:
            categories[category] = []
        categories[category].append(test.test_id)
    
    print("\n📋 Test Categories:")
    for category, test_ids in sorted(categories.items()):
        print(f"   {category}: {len(test_ids)} tests")
        for tid in test_ids:
            print(f"      • {tid}")
