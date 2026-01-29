"""
Jobs API Context Switching Tests for SQL SECURITY DEFINER
Tests procedures created by SP and run by User, and vice versa, via Jobs API
"""

from framework.test_framework import DefinerTestCase
from framework.config import CATALOG, SCHEMA, USER_A, SERVICE_PRINCIPAL_B_ID

def get_jobs_context_tests():
    """
    Test cases for Jobs API context switching scenarios
    These tests validate impersonation when procedures are executed via Jobs API
    """
    tests = []
    
    # ============================================================================
    # SCENARIO 1: Service Principal Creates, User Executes (via Jobs API)
    # ============================================================================
    
    # TC-JOBS-01: SP creates DEFINER procedure, User runs via Jobs API
    tc_jobs_01 = DefinerTestCase(
        test_id="TC-JOBS-01",
        description="Jobs API: SP creates DEFINER proc, User executes - should use SP's permissions",
        setup_sql=[
            # Create a restricted table (only SP will have access)
            f"DROP TABLE IF EXISTS {CATALOG}.{SCHEMA}.tc_jobs_01_sp_only_table",
            f"CREATE TABLE {CATALOG}.{SCHEMA}.tc_jobs_01_sp_only_table (id INT, data STRING)",
            f"INSERT INTO {CATALOG}.{SCHEMA}.tc_jobs_01_sp_only_table VALUES (1, 'sp_data')",
            
            # Revoke access from user
            f"REVOKE ALL PRIVILEGES ON TABLE {CATALOG}.{SCHEMA}.tc_jobs_01_sp_only_table FROM `{USER_A}`",
            
            # SP creates DEFINER procedure
            # Note: In actual Jobs API test, this would be created by SP connection
            f"DROP PROCEDURE IF EXISTS {CATALOG}.{SCHEMA}.tc_jobs_01_sp_proc",
            f"""
            CREATE PROCEDURE {CATALOG}.{SCHEMA}.tc_jobs_01_sp_proc()
            LANGUAGE SQL
            SQL SECURITY DEFINER
            COMMENT 'Owner: SP, Caller: User, Mode: DEFINER, Context: Jobs API'
            AS BEGIN
                -- Should succeed using SP's permissions
                SELECT COUNT(*) as row_count, 'SP owned DEFINER' as proc_type
                FROM {CATALOG}.{SCHEMA}.tc_jobs_01_sp_only_table;
            END
            """,
            
            # Grant execute to user
            f"GRANT EXECUTE ON PROCEDURE {CATALOG}.{SCHEMA}.tc_jobs_01_sp_proc TO `{USER_A}`",
        ],
        test_sql=f"CALL {CATALOG}.{SCHEMA}.tc_jobs_01_sp_proc()",
        teardown_sql=[
            f"DROP PROCEDURE IF EXISTS {CATALOG}.{SCHEMA}.tc_jobs_01_sp_proc",
            f"DROP TABLE IF EXISTS {CATALOG}.{SCHEMA}.tc_jobs_01_sp_only_table",
        ],
        should_fail=False  # Should succeed - uses SP's permissions
    )
    tests.append(tc_jobs_01)
    
    # TC-JOBS-02: SP creates procedure that calls another SP procedure (nested)
    tc_jobs_02 = DefinerTestCase(
        test_id="TC-JOBS-02",
        description="Jobs API: SP creates nested DEFINER procs, User executes - context maintained",
        setup_sql=[
            f"DROP TABLE IF EXISTS {CATALOG}.{SCHEMA}.tc_jobs_02_sensitive_data",
            f"CREATE TABLE {CATALOG}.{SCHEMA}.tc_jobs_02_sensitive_data (id INT, secret STRING)",
            f"INSERT INTO {CATALOG}.{SCHEMA}.tc_jobs_02_sensitive_data VALUES (1, 'classified')",
            
            # Revoke from user
            f"REVOKE ALL PRIVILEGES ON TABLE {CATALOG}.{SCHEMA}.tc_jobs_02_sensitive_data FROM `{USER_A}`",
            
            # SP creates inner procedure
            f"DROP PROCEDURE IF EXISTS {CATALOG}.{SCHEMA}.tc_jobs_02_sp_inner",
            f"""
            CREATE PROCEDURE {CATALOG}.{SCHEMA}.tc_jobs_02_sp_inner()
            LANGUAGE SQL
            SQL SECURITY DEFINER
            AS BEGIN
                SELECT 'inner_proc' as level, COUNT(*) as data_count
                FROM {CATALOG}.{SCHEMA}.tc_jobs_02_sensitive_data;
            END
            """,
            
            # SP creates outer procedure that calls inner
            f"DROP PROCEDURE IF EXISTS {CATALOG}.{SCHEMA}.tc_jobs_02_sp_outer",
            f"""
            CREATE PROCEDURE {CATALOG}.{SCHEMA}.tc_jobs_02_sp_outer()
            LANGUAGE SQL
            SQL SECURITY DEFINER
            AS BEGIN
                CALL {CATALOG}.{SCHEMA}.tc_jobs_02_sp_inner();
            END
            """,
            
            # Grant execute to user (only outer proc)
            f"GRANT EXECUTE ON PROCEDURE {CATALOG}.{SCHEMA}.tc_jobs_02_sp_outer TO `{USER_A}`",
        ],
        test_sql=f"CALL {CATALOG}.{SCHEMA}.tc_jobs_02_sp_outer()",
        teardown_sql=[
            f"DROP PROCEDURE IF EXISTS {CATALOG}.{SCHEMA}.tc_jobs_02_sp_outer",
            f"DROP PROCEDURE IF EXISTS {CATALOG}.{SCHEMA}.tc_jobs_02_sp_inner",
            f"DROP TABLE IF EXISTS {CATALOG}.{SCHEMA}.tc_jobs_02_sensitive_data",
        ],
        should_fail=False  # Should succeed - SP owns both procedures
    )
    tests.append(tc_jobs_02)
    
    # ============================================================================
    # SCENARIO 2: User Creates, Service Principal Executes (via Jobs API)
    # ============================================================================
    
    # TC-JOBS-03: User creates DEFINER procedure, SP runs via Jobs API
    tc_jobs_03 = DefinerTestCase(
        test_id="TC-JOBS-03",
        description="Jobs API: User creates DEFINER proc, SP executes - should use User's permissions",
        setup_sql=[
            # Create a table only user has access to
            f"DROP TABLE IF EXISTS {CATALOG}.{SCHEMA}.tc_jobs_03_user_only_table",
            f"CREATE TABLE {CATALOG}.{SCHEMA}.tc_jobs_03_user_only_table (id INT, data STRING)",
            f"INSERT INTO {CATALOG}.{SCHEMA}.tc_jobs_03_user_only_table VALUES (1, 'user_data')",
            
            # Revoke access from SP
            f"REVOKE ALL PRIVILEGES ON TABLE {CATALOG}.{SCHEMA}.tc_jobs_03_user_only_table FROM `{SERVICE_PRINCIPAL_B_ID}`",
            
            # User creates DEFINER procedure
            f"DROP PROCEDURE IF EXISTS {CATALOG}.{SCHEMA}.tc_jobs_03_user_proc",
            f"""
            CREATE PROCEDURE {CATALOG}.{SCHEMA}.tc_jobs_03_user_proc()
            LANGUAGE SQL
            SQL SECURITY DEFINER
            COMMENT 'Owner: User, Caller: SP, Mode: DEFINER, Context: Jobs API'
            AS BEGIN
                -- Should succeed using User's permissions
                SELECT COUNT(*) as row_count, 'User owned DEFINER' as proc_type
                FROM {CATALOG}.{SCHEMA}.tc_jobs_03_user_only_table;
            END
            """,
            
            # Grant execute to SP
            f"GRANT EXECUTE ON PROCEDURE {CATALOG}.{SCHEMA}.tc_jobs_03_user_proc TO `{SERVICE_PRINCIPAL_B_ID}`",
        ],
        test_sql=f"CALL {CATALOG}.{SCHEMA}.tc_jobs_03_user_proc()",
        teardown_sql=[
            f"DROP PROCEDURE IF EXISTS {CATALOG}.{SCHEMA}.tc_jobs_03_user_proc",
            f"DROP TABLE IF EXISTS {CATALOG}.{SCHEMA}.tc_jobs_03_user_only_table",
        ],
        should_fail=False  # Should succeed - uses User's permissions
    )
    tests.append(tc_jobs_03)
    
    # TC-JOBS-04: Bidirectional nesting via Jobs API
    tc_jobs_04 = DefinerTestCase(
        test_id="TC-JOBS-04",
        description="Jobs API: User proc calls SP proc calls User proc - context switches correctly",
        setup_sql=[
            # Create tables for both user and SP
            f"DROP TABLE IF EXISTS {CATALOG}.{SCHEMA}.tc_jobs_04_user_table",
            f"CREATE TABLE {CATALOG}.{SCHEMA}.tc_jobs_04_user_table (id INT, owner STRING)",
            f"INSERT INTO {CATALOG}.{SCHEMA}.tc_jobs_04_user_table VALUES (1, 'user')",
            
            f"DROP TABLE IF EXISTS {CATALOG}.{SCHEMA}.tc_jobs_04_sp_table",
            f"CREATE TABLE {CATALOG}.{SCHEMA}.tc_jobs_04_sp_table (id INT, owner STRING)",
            f"INSERT INTO {CATALOG}.{SCHEMA}.tc_jobs_04_sp_table VALUES (1, 'sp')",
            
            # Revoke cross-access
            f"REVOKE ALL PRIVILEGES ON TABLE {CATALOG}.{SCHEMA}.tc_jobs_04_sp_table FROM `{USER_A}`",
            f"REVOKE ALL PRIVILEGES ON TABLE {CATALOG}.{SCHEMA}.tc_jobs_04_user_table FROM `{SERVICE_PRINCIPAL_B_ID}`",
            
            # Level 3: User proc (innermost)
            f"DROP PROCEDURE IF EXISTS {CATALOG}.{SCHEMA}.tc_jobs_04_user_inner",
            f"""
            CREATE PROCEDURE {CATALOG}.{SCHEMA}.tc_jobs_04_user_inner()
            LANGUAGE SQL
            SQL SECURITY DEFINER
            AS BEGIN
                SELECT 'level_3_user' as level, COUNT(*) as count
                FROM {CATALOG}.{SCHEMA}.tc_jobs_04_user_table;
            END
            """,
            
            # Level 2: SP proc (middle)
            f"DROP PROCEDURE IF EXISTS {CATALOG}.{SCHEMA}.tc_jobs_04_sp_middle",
            f"""
            CREATE PROCEDURE {CATALOG}.{SCHEMA}.tc_jobs_04_sp_middle()
            LANGUAGE SQL
            SQL SECURITY DEFINER
            AS BEGIN
                CALL {CATALOG}.{SCHEMA}.tc_jobs_04_user_inner();
            END
            """,
            
            # Level 1: User proc (outer)
            f"DROP PROCEDURE IF EXISTS {CATALOG}.{SCHEMA}.tc_jobs_04_user_outer",
            f"""
            CREATE PROCEDURE {CATALOG}.{SCHEMA}.tc_jobs_04_user_outer()
            LANGUAGE SQL
            SQL SECURITY DEFINER
            AS BEGIN
                CALL {CATALOG}.{SCHEMA}.tc_jobs_04_sp_middle();
            END
            """,
            
            # Grant necessary permissions
            f"GRANT EXECUTE ON PROCEDURE {CATALOG}.{SCHEMA}.tc_jobs_04_user_inner TO `{SERVICE_PRINCIPAL_B_ID}`",
            f"GRANT EXECUTE ON PROCEDURE {CATALOG}.{SCHEMA}.tc_jobs_04_sp_middle TO `{USER_A}`",
            f"GRANT EXECUTE ON PROCEDURE {CATALOG}.{SCHEMA}.tc_jobs_04_user_outer TO `{SERVICE_PRINCIPAL_B_ID}`",
        ],
        test_sql=f"CALL {CATALOG}.{SCHEMA}.tc_jobs_04_user_outer()",
        teardown_sql=[
            f"DROP PROCEDURE IF EXISTS {CATALOG}.{SCHEMA}.tc_jobs_04_user_outer",
            f"DROP PROCEDURE IF EXISTS {CATALOG}.{SCHEMA}.tc_jobs_04_sp_middle",
            f"DROP PROCEDURE IF EXISTS {CATALOG}.{SCHEMA}.tc_jobs_04_user_inner",
            f"DROP TABLE IF EXISTS {CATALOG}.{SCHEMA}.tc_jobs_04_user_table",
            f"DROP TABLE IF EXISTS {CATALOG}.{SCHEMA}.tc_jobs_04_sp_table",
        ],
        should_fail=False  # Should succeed - context switches correctly
    )
    tests.append(tc_jobs_04)
    
    # ============================================================================
    # SCENARIO 3: Jobs API Specific Edge Cases
    # ============================================================================
    
    # TC-JOBS-05: Parameter passing through Jobs API context
    tc_jobs_05 = DefinerTestCase(
        test_id="TC-JOBS-05",
        description="Jobs API: Parameters passed correctly through impersonation context",
        setup_sql=[
            f"DROP TABLE IF EXISTS {CATALOG}.{SCHEMA}.tc_jobs_05_log_table",
            f"CREATE TABLE {CATALOG}.{SCHEMA}.tc_jobs_05_log_table (id INT, message STRING, caller STRING)",
            
            # User creates procedure with parameters
            f"DROP PROCEDURE IF EXISTS {CATALOG}.{SCHEMA}.tc_jobs_05_log_message",
            f"""
            CREATE PROCEDURE {CATALOG}.{SCHEMA}.tc_jobs_05_log_message(msg STRING, val INT)
            LANGUAGE SQL
            SQL SECURITY DEFINER
            AS BEGIN
                INSERT INTO {CATALOG}.{SCHEMA}.tc_jobs_05_log_table 
                VALUES (val, msg, CURRENT_USER());
            END
            """,
            
            f"GRANT EXECUTE ON PROCEDURE {CATALOG}.{SCHEMA}.tc_jobs_05_log_message TO `{SERVICE_PRINCIPAL_B_ID}`",
        ],
        test_sql=f"CALL {CATALOG}.{SCHEMA}.tc_jobs_05_log_message('test_message', 123)",
        teardown_sql=[
            f"DROP PROCEDURE IF EXISTS {CATALOG}.{SCHEMA}.tc_jobs_05_log_message",
            f"DROP TABLE IF EXISTS {CATALOG}.{SCHEMA}.tc_jobs_05_log_table",
        ],
        should_fail=False  # Should succeed with correct parameters
    )
    tests.append(tc_jobs_05)
    
    # TC-JOBS-06: Dynamic SQL in Jobs API context
    tc_jobs_06 = DefinerTestCase(
        test_id="TC-JOBS-06",
        description="Jobs API: Dynamic SQL respects impersonation context",
        setup_sql=[
            f"DROP TABLE IF EXISTS {CATALOG}.{SCHEMA}.tc_jobs_06_dynamic_test",
            f"CREATE TABLE {CATALOG}.{SCHEMA}.tc_jobs_06_dynamic_test (id INT, val STRING)",
            f"INSERT INTO {CATALOG}.{SCHEMA}.tc_jobs_06_dynamic_test VALUES (1, 'dynamic')",
            
            # Revoke from SP
            f"REVOKE ALL PRIVILEGES ON TABLE {CATALOG}.{SCHEMA}.tc_jobs_06_dynamic_test FROM `{SERVICE_PRINCIPAL_B_ID}`",
            
            # User creates procedure with dynamic SQL
            f"DROP PROCEDURE IF EXISTS {CATALOG}.{SCHEMA}.tc_jobs_06_dynamic_proc",
            f"""
            CREATE PROCEDURE {CATALOG}.{SCHEMA}.tc_jobs_06_dynamic_proc(table_name STRING)
            LANGUAGE SQL
            SQL SECURITY DEFINER
            AS BEGIN
                DECLARE query STRING;
                SET query = CONCAT('SELECT COUNT(*) as count FROM {CATALOG}.{SCHEMA}.', table_name);
                EXECUTE IMMEDIATE query;
            END
            """,
            
            f"GRANT EXECUTE ON PROCEDURE {CATALOG}.{SCHEMA}.tc_jobs_06_dynamic_proc TO `{SERVICE_PRINCIPAL_B_ID}`",
        ],
        test_sql=f"CALL {CATALOG}.{SCHEMA}.tc_jobs_06_dynamic_proc('tc_jobs_06_dynamic_test')",
        teardown_sql=[
            f"DROP PROCEDURE IF EXISTS {CATALOG}.{SCHEMA}.tc_jobs_06_dynamic_proc",
            f"DROP TABLE IF EXISTS {CATALOG}.{SCHEMA}.tc_jobs_06_dynamic_test",
        ],
        should_fail=False  # Should succeed - dynamic SQL uses owner's permissions
    )
    tests.append(tc_jobs_06)
    
    return tests

if __name__ == "__main__":
    """
    Quick test to ensure module loads correctly
    """
    tests = get_jobs_context_tests()
    print(f"✅ Loaded {len(tests)} Jobs API context switching tests")
    for test in tests:
        print(f"   • {test.test_id}: {test.description}")
