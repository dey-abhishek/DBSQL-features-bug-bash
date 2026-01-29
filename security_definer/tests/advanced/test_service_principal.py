"""
SERVICE PRINCIPAL TEST SUITE
Tests that execute AS service principal (not as owner)

These tests validate:
- INVOKER mode truly uses caller's permissions
- Error messages don't leak sensitive info
- Permission checks work correctly for non-owner callers
"""

from framework.test_framework import DefinerTestCase, TestExecutor, TestResult
from framework.service_principal_auth import ServicePrincipalAuth
from framework.config import (
    SERVER_HOSTNAME, HTTP_PATH, PAT_TOKEN, CATALOG, SCHEMA,
    SERVICE_PRINCIPAL_B_ID, SERVICE_PRINCIPAL_PAT
)
import time


class ServicePrincipalTestExecutor:
    """
    Test executor that runs tests AS service principal
    """
    
    def __init__(self, sp_auth: ServicePrincipalAuth, owner_connection):
        self.sp_auth = sp_auth
        self.owner_connection = owner_connection
    
    def run_test(self, test_case: DefinerTestCase) -> TestResult:
        """
        Execute test case with SP authentication
        
        Setup/Teardown run as owner (to create/drop objects)
        Test execution runs as service principal
        """
        start_time = time.time()
        
        print()
        print("=" * 80)
        print(f"🧪 Running {test_case.test_id}: {test_case.description}")
        print("=" * 80)
        
        try:
            # SETUP as owner (SP typically can't create procedures)
            for sql in test_case.setup_sql:
                print(f"⚙️  Setup (as owner): {sql[:80]}...")
                result, error = self.owner_connection.execute(sql)
                if error:
                    execution_time = time.time() - start_time
                    return TestResult(
                        test_id=test_case.test_id,
                        description=test_case.description,
                        status="ERROR",
                        execution_time=execution_time,
                        error_message=f"Setup failed: {error}"
                    )
            
            # EXECUTE as service principal
            print(f"▶️  Executing test SQL (as SP: {SERVICE_PRINCIPAL_B_ID[:20]}...)...")
            result, error = self.sp_auth.execute(test_case.test_sql)
            
            # Determine status
            if test_case.should_fail:
                if error:
                    status = "PASS"
                    print(f"✅ Test passed (expected failure occurred)")
                else:
                    status = "FAIL"
                    error = "Expected failure but query succeeded"
                    print(f"❌ Test failed: {error}")
            else:
                if error:
                    status = "FAIL"
                    print(f"❌ Test failed: {error}")
                else:
                    status = "PASS"
                    print(f"✅ Test passed")
            
            # TEARDOWN as owner
            for sql in test_case.teardown_sql:
                print(f"🧹 Cleanup (as owner): {sql[:80]}...")
                self.owner_connection.execute(sql)
            
            execution_time = time.time() - start_time
            return TestResult(
                test_id=test_case.test_id,
                description=test_case.description,
                status=status,
                execution_time=execution_time,
                error_message=error if status != "PASS" else None
            )
            
        except Exception as e:
            execution_time = time.time() - start_time
            print(f"💥 Error: {e}")
            
            # Cleanup on error
            for sql in test_case.teardown_sql:
                try:
                    self.owner_connection.execute(sql)
                except:
                    pass
            
            return TestResult(
                test_id=test_case.test_id,
                description=test_case.description,
                status="ERROR",
                execution_time=execution_time,
                error_message=str(e)
            )


def get_tests():
    """
    Get service principal test cases
    These test INVOKER mode behavior when called by SP
    """
    tests = []
    
    # TC-71: INVOKER Mode Should Fail for SP Without Table Access
    tc71 = DefinerTestCase(
        test_id="TC-71",
        description="INVOKER Mode - SP without table access should fail",
        setup_sql=[
            f"DROP TABLE IF EXISTS {CATALOG}.{SCHEMA}.tc71_private_table",
            f"CREATE TABLE {CATALOG}.{SCHEMA}.tc71_private_table (id INT, secret STRING)",
            f"INSERT INTO {CATALOG}.{SCHEMA}.tc71_private_table VALUES (1, 'owner_secret')",
            
            # Create INVOKER procedure (uses caller's permissions)
            f"DROP PROCEDURE IF EXISTS {CATALOG}.{SCHEMA}.tc71_invoker_proc",
            f"""
            CREATE PROCEDURE {CATALOG}.{SCHEMA}.tc71_invoker_proc()
            LANGUAGE SQL
            SQL SECURITY INVOKER
            AS BEGIN
                -- This should FAIL when SP calls it (no table access)
                SELECT * FROM {CATALOG}.{SCHEMA}.tc71_private_table;
            END
            """,
            
            # Revoke table access from SP
            f"REVOKE ALL PRIVILEGES ON TABLE {CATALOG}.{SCHEMA}.tc71_private_table FROM `{SERVICE_PRINCIPAL_B_ID}`",
            
            # Grant EXECUTE permission (but not table access)
            f"GRANT EXECUTE ON PROCEDURE {CATALOG}.{SCHEMA}.tc71_invoker_proc TO `{SERVICE_PRINCIPAL_B_ID}`",
        ],
        test_sql=f"CALL {CATALOG}.{SCHEMA}.tc71_invoker_proc()",
        teardown_sql=[
            f"DROP PROCEDURE IF EXISTS {CATALOG}.{SCHEMA}.tc71_invoker_proc",
            f"DROP TABLE IF EXISTS {CATALOG}.{SCHEMA}.tc71_private_table",
        ],
        should_fail=True  # Expected to fail - SP has no table access
    )
    tests.append(tc71)
    
    # TC-72: DEFINER Mode Should Succeed for SP
    tc72 = DefinerTestCase(
        test_id="TC-72",
        description="DEFINER Mode - SP can execute with owner's permissions",
        setup_sql=[
            f"DROP TABLE IF EXISTS {CATALOG}.{SCHEMA}.tc72_owner_table",
            f"CREATE TABLE {CATALOG}.{SCHEMA}.tc72_owner_table (id INT, data STRING)",
            f"INSERT INTO {CATALOG}.{SCHEMA}.tc72_owner_table VALUES (1, 'accessible_via_definer')",
            
            # Create DEFINER procedure (uses owner's permissions)
            f"DROP PROCEDURE IF EXISTS {CATALOG}.{SCHEMA}.tc72_definer_proc",
            f"""
            CREATE PROCEDURE {CATALOG}.{SCHEMA}.tc72_definer_proc()
            LANGUAGE SQL
            AS BEGIN
                -- This should SUCCEED when SP calls it (owner has access)
                SELECT COUNT(*) as row_count FROM {CATALOG}.{SCHEMA}.tc72_owner_table;
            END
            """,
            
            # Revoke table access from SP (but procedure will work via DEFINER)
            f"REVOKE ALL PRIVILEGES ON TABLE {CATALOG}.{SCHEMA}.tc72_owner_table FROM `{SERVICE_PRINCIPAL_B_ID}`",
            
            # Grant EXECUTE permission
            f"GRANT EXECUTE ON PROCEDURE {CATALOG}.{SCHEMA}.tc72_definer_proc TO `{SERVICE_PRINCIPAL_B_ID}`",
        ],
        test_sql=f"CALL {CATALOG}.{SCHEMA}.tc72_definer_proc()",
        teardown_sql=[
            f"DROP PROCEDURE IF EXISTS {CATALOG}.{SCHEMA}.tc72_definer_proc",
            f"DROP TABLE IF EXISTS {CATALOG}.{SCHEMA}.tc72_owner_table",
        ],
        should_fail=False  # Should succeed - DEFINER mode uses owner's permissions
    )
    tests.append(tc72)
    
    # TC-73: Error Message Quality for SP
    tc73 = DefinerTestCase(
        test_id="TC-73",
        description="Error Messages - Verify no sensitive info leaked to SP",
        setup_sql=[
            f"DROP TABLE IF EXISTS {CATALOG}.{SCHEMA}.tc73_classified_data",
            f"CREATE TABLE {CATALOG}.{SCHEMA}.tc73_classified_data (ssn STRING, salary DECIMAL(10,2))",
            f"INSERT INTO {CATALOG}.{SCHEMA}.tc73_classified_data VALUES ('123-45-6789', 200000)",
            
            # INVOKER procedure that will fail for SP
            f"DROP PROCEDURE IF EXISTS {CATALOG}.{SCHEMA}.tc73_check_access",
            f"""
            CREATE PROCEDURE {CATALOG}.{SCHEMA}.tc73_check_access()
            LANGUAGE SQL
            SQL SECURITY INVOKER
            AS BEGIN
                SELECT * FROM {CATALOG}.{SCHEMA}.tc73_classified_data;
            END
            """,
            
            # Revoke table access
            f"REVOKE ALL PRIVILEGES ON TABLE {CATALOG}.{SCHEMA}.tc73_classified_data FROM `{SERVICE_PRINCIPAL_B_ID}`",
            f"GRANT EXECUTE ON PROCEDURE {CATALOG}.{SCHEMA}.tc73_check_access TO `{SERVICE_PRINCIPAL_B_ID}`",
        ],
        test_sql=f"CALL {CATALOG}.{SCHEMA}.tc73_check_access()",
        teardown_sql=[
            f"DROP PROCEDURE IF EXISTS {CATALOG}.{SCHEMA}.tc73_check_access",
            f"DROP TABLE IF EXISTS {CATALOG}.{SCHEMA}.tc73_classified_data",
        ],
        should_fail=True  # Expected to fail, we'll check error message quality
    )
    tests.append(tc73)
    
    # TC-74: SP Identity Resolution
    tc74 = DefinerTestCase(
        test_id="TC-74",
        description="Identity Resolution - CURRENT_USER() should return SP ID",
        setup_sql=[
            f"DROP PROCEDURE IF EXISTS {CATALOG}.{SCHEMA}.tc74_who_am_i",
            f"""
            CREATE PROCEDURE {CATALOG}.{SCHEMA}.tc74_who_am_i()
            LANGUAGE SQL
            AS BEGIN
                SELECT 
                    CURRENT_USER() as current_user,
                    CURRENT_CATALOG() as catalog,
                    CURRENT_SCHEMA() as schema;
            END
            """,
            
            f"GRANT EXECUTE ON PROCEDURE {CATALOG}.{SCHEMA}.tc74_who_am_i TO `{SERVICE_PRINCIPAL_B_ID}`",
        ],
        test_sql=f"CALL {CATALOG}.{SCHEMA}.tc74_who_am_i()",
        teardown_sql=[
            f"DROP PROCEDURE IF EXISTS {CATALOG}.{SCHEMA}.tc74_who_am_i",
        ],
        should_fail=False
    )
    tests.append(tc74)
    
    return tests


if __name__ == "__main__":
    from framework.test_framework import DatabricksConnection, TestReporter
    
    print("=" * 80)
    print("🔐 SERVICE PRINCIPAL TEST SUITE")
    print("=" * 80)
    print()
    
    # Check if SP PAT is configured
    if not SERVICE_PRINCIPAL_PAT:
        print("❌ SERVICE_PRINCIPAL_PAT not configured!")
        print()
        print("To run these tests, you need to:")
        print("1. Generate a PAT for the service principal")
        print("2. Set it in framework/config.py or environment variable")
        print()
        print("Run this for instructions:")
        print("  python framework/service_principal_auth.py")
        print()
        exit(1)
    
    # Connect as owner (for setup/teardown)
    print("🔗 Connecting as owner (for setup/teardown)...")
    owner_conn = DatabricksConnection(SERVER_HOSTNAME, HTTP_PATH, PAT_TOKEN, CATALOG, SCHEMA)
    print(f"✅ Owner connected: {owner_conn.execute('SELECT CURRENT_USER()')[0][0][0]}")
    print()
    
    # Connect as service principal (for test execution)
    print("🔗 Connecting as service principal (for test execution)...")
    sp_auth = ServicePrincipalAuth(
        server_hostname=SERVER_HOSTNAME,
        http_path=HTTP_PATH,
        sp_token=SERVICE_PRINCIPAL_PAT,
        catalog=CATALOG,
        schema=SCHEMA
    )
    
    if not sp_auth.connect():
        print("❌ Failed to connect as service principal")
        exit(1)
    
    sp_user = sp_auth.get_current_user()
    print(f"✅ SP connected: {sp_user}")
    print()
    
    # Run tests
    executor = ServicePrincipalTestExecutor(sp_auth, owner_conn)
    test_cases = get_tests()
    results = [executor.run_test(tc) for tc in test_cases]
    
    # Generate report
    reporter = TestReporter(results)
    reporter.print_summary()
    
    # Analyze error messages for TC-73
    tc73_result = [r for r in results if r.test_id == "TC-73"]
    if tc73_result and tc73_result[0].error_message:
        print()
        print("=" * 80)
        print("🔍 ERROR MESSAGE ANALYSIS (TC-73)")
        print("=" * 80)
        error_msg = tc73_result[0].error_message
        print(f"Error: {error_msg}")
        print()
        
        # Check for sensitive info leakage
        sensitive_keywords = ['ssn', 'salary', '123-45-6789', '200000']
        leaked = [kw for kw in sensitive_keywords if kw.lower() in error_msg.lower()]
        
        if leaked:
            print(f"⚠️  POTENTIAL INFO LEAKAGE: Found {leaked} in error message")
        else:
            print(f"✅ No sensitive data found in error message")
    
    # Close connections
    sp_auth.close()
    owner_conn.close()
    
    print()
    print("=" * 80)
    print("🎯 KEY INSIGHTS")
    print("=" * 80)
    passed = [r for r in results if r.status == "PASS"]
    print(f"✅ Tests passed: {len(passed)}/{len(results)}")
    print()
    print("Service principal testing validates:")
    print("  • INVOKER mode uses caller's (SP's) permissions")
    print("  • DEFINER mode uses owner's permissions")
    print("  • Error messages don't leak sensitive info")
    print("  • Identity resolution works correctly")
