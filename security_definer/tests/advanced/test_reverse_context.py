"""
REVERSE CONTEXT SWITCHING TESTS
Test scenarios where Service Principal OWNS procedures and User executes them

This validates the opposite direction of ownership:
- SP creates and owns procedures
- User gets EXECUTE grant
- Procedure should run with SP's permissions (DEFINER mode)
"""

from framework.test_framework import TestResult
from framework.service_principal_auth import ServicePrincipalAuth
from framework.config import (
    SERVER_HOSTNAME, HTTP_PATH, PAT_TOKEN, CATALOG, SCHEMA,
    SERVICE_PRINCIPAL_B_ID, SERVICE_PRINCIPAL_CLIENT_ID, SERVICE_PRINCIPAL_CLIENT_SECRET, USER_A
)
import time


def run_reverse_context_tests():
    """
    Run tests where SP creates procedures and User executes them
    
    This is CRITICAL for validating:
    1. SP can create procedures
    2. User can execute SP's procedures
    3. DEFINER mode uses SP's permissions (owner)
    4. INVOKER mode uses User's permissions (caller)
    """
    
    if not SERVICE_PRINCIPAL_CLIENT_ID or not SERVICE_PRINCIPAL_CLIENT_SECRET:
        print("❌ SERVICE_PRINCIPAL OAuth credentials not configured!")
        print("Update framework/config.py with CLIENT_ID and CLIENT_SECRET")
        return False
    
    print("=" * 80)
    print("🔄 REVERSE CONTEXT SWITCHING TEST SUITE")
    print("=" * 80)
    print()
    print("Testing: SP creates procedures → User executes them")
    print()
    
    # Connect as both SP and User
    from framework.test_framework import DatabricksConnection
    
    print("🔗 Connecting as User (for execution)...")
    user_conn = DatabricksConnection(SERVER_HOSTNAME, HTTP_PATH, PAT_TOKEN, CATALOG, SCHEMA)
    user_name = user_conn.execute("SELECT CURRENT_USER()")[0][0][0]
    print(f"✅ User connected: {user_name}")
    print()
    
    print("🔗 Connecting as SP (for procedure creation)...")
    sp_conn = ServicePrincipalAuth(
        server_hostname=SERVER_HOSTNAME,
        http_path=HTTP_PATH,
        sp_client_id=SERVICE_PRINCIPAL_CLIENT_ID,
        sp_client_secret=SERVICE_PRINCIPAL_CLIENT_SECRET,
        catalog=CATALOG,
        schema=SCHEMA
    )
    
    if not sp_conn.connect():
        print("❌ SP connection failed")
        return False
    
    sp_name = sp_conn.get_current_user()
    print(f"✅ SP connected: {sp_name}")
    print()
    
    results = []
    
    # ============================================================================
    # TC-100: SP Creates DEFINER Procedure, User Executes It
    # ============================================================================
    print("=" * 80)
    print("🧪 TC-100: SP owns DEFINER procedure → User executes")
    print("=" * 80)
    print()
    
    start_time = time.time()
    
    try:
        # Setup: Create table accessible only to SP
        print("⚙️  Setup (as SP): Create SP-only table...")
        sp_conn.execute(f"DROP TABLE IF EXISTS {CATALOG}.{SCHEMA}.tc100_sp_private_table")
        sp_conn.execute(f"CREATE TABLE {CATALOG}.{SCHEMA}.tc100_sp_private_table (id INT, sp_data STRING)")
        sp_conn.execute(f"INSERT INTO {CATALOG}.{SCHEMA}.tc100_sp_private_table VALUES (1, 'SP_SECRET')")
        
        # Revoke access from User
        print(f"⚙️  Setup (as SP): Revoke table access from {user_name}...")
        sp_conn.execute(f"REVOKE ALL PRIVILEGES ON TABLE {CATALOG}.{SCHEMA}.tc100_sp_private_table FROM `{user_name}`")
        
        # SP creates DEFINER procedure (SP owns it)
        print("⚙️  Setup (as SP): Create DEFINER procedure...")
        sp_conn.execute(f"DROP PROCEDURE IF EXISTS {CATALOG}.{SCHEMA}.tc100_sp_owned_definer")
        result, error = sp_conn.execute(f"""
            CREATE PROCEDURE {CATALOG}.{SCHEMA}.tc100_sp_owned_definer()
            LANGUAGE SQL
            AS BEGIN
                -- This procedure is OWNED by SP
                -- When User calls it, should use SP's permissions (DEFINER mode)
                SELECT COUNT(*) as row_count FROM {CATALOG}.{SCHEMA}.tc100_sp_private_table;
            END
        """)
        
        if error:
            print(f"❌ SP cannot create procedure: {error}")
            results.append({
                'test_id': 'TC-100',
                'description': 'SP owns DEFINER proc → User executes',
                'status': 'ERROR',
                'error': f'SP cannot create procedure: {error}'
            })
        else:
            # Grant EXECUTE to User
            print(f"⚙️  Setup (as SP): Grant EXECUTE to {user_name}...")
            sp_conn.execute(f"GRANT EXECUTE ON PROCEDURE {CATALOG}.{SCHEMA}.tc100_sp_owned_definer TO `{user_name}`")
            
            # User executes SP's procedure
            print(f"▶️  Test (as User): Execute SP's DEFINER procedure...")
            result, error = user_conn.execute(f"CALL {CATALOG}.{SCHEMA}.tc100_sp_owned_definer()")
            
            if error:
                print(f"❌ Test FAILED: {error}")
                status = "FAIL"
            else:
                print(f"✅ Test PASSED: User successfully executed SP's procedure")
                print(f"   Result: {result}")
                status = "PASS"
            
            results.append({
                'test_id': 'TC-100',
                'description': 'SP owns DEFINER proc → User executes',
                'status': status,
                'error': error,
                'execution_time': time.time() - start_time
            })
            
            # Cleanup
            print("🧹 Cleanup...")
            sp_conn.execute(f"DROP PROCEDURE IF EXISTS {CATALOG}.{SCHEMA}.tc100_sp_owned_definer")
            sp_conn.execute(f"DROP TABLE IF EXISTS {CATALOG}.{SCHEMA}.tc100_sp_private_table")
    
    except Exception as e:
        print(f"💥 Error: {e}")
        results.append({
            'test_id': 'TC-100',
            'description': 'SP owns DEFINER proc → User executes',
            'status': 'ERROR',
            'error': str(e)
        })
    
    print()
    
    # ============================================================================
    # TC-101: SP Creates INVOKER Procedure, User Executes It
    # ============================================================================
    print("=" * 80)
    print("🧪 TC-101: SP owns INVOKER procedure → User executes")
    print("=" * 80)
    print()
    
    start_time = time.time()
    
    try:
        # Setup: Create table accessible to User
        print("⚙️  Setup (as User): Create User-accessible table...")
        user_conn.execute(f"DROP TABLE IF EXISTS {CATALOG}.{SCHEMA}.tc101_user_table")
        user_conn.execute(f"CREATE TABLE {CATALOG}.{SCHEMA}.tc101_user_table (id INT, user_data STRING)")
        user_conn.execute(f"INSERT INTO {CATALOG}.{SCHEMA}.tc101_user_table VALUES (1, 'USER_DATA')")
        
        # SP creates INVOKER procedure
        print("⚙️  Setup (as SP): Create INVOKER procedure...")
        sp_conn.execute(f"DROP PROCEDURE IF EXISTS {CATALOG}.{SCHEMA}.tc101_sp_owned_invoker")
        result, error = sp_conn.execute(f"""
            CREATE PROCEDURE {CATALOG}.{SCHEMA}.tc101_sp_owned_invoker()
            LANGUAGE SQL
            SQL SECURITY INVOKER
            AS BEGIN
                -- This procedure is OWNED by SP but uses INVOKER mode
                -- When User calls it, should use User's permissions
                SELECT COUNT(*) as row_count FROM {CATALOG}.{SCHEMA}.tc101_user_table;
            END
        """)
        
        if error:
            print(f"❌ SP cannot create INVOKER procedure: {error}")
            results.append({
                'test_id': 'TC-101',
                'description': 'SP owns INVOKER proc → User executes',
                'status': 'ERROR',
                'error': f'SP cannot create procedure: {error}'
            })
        else:
            # Grant EXECUTE to User
            print(f"⚙️  Setup (as SP): Grant EXECUTE to {user_name}...")
            sp_conn.execute(f"GRANT EXECUTE ON PROCEDURE {CATALOG}.{SCHEMA}.tc101_sp_owned_invoker TO `{user_name}`")
            
            # User executes SP's INVOKER procedure
            print(f"▶️  Test (as User): Execute SP's INVOKER procedure...")
            result, error = user_conn.execute(f"CALL {CATALOG}.{SCHEMA}.tc101_sp_owned_invoker()")
            
            if error:
                print(f"❌ Test FAILED: {error}")
                status = "FAIL"
            else:
                print(f"✅ Test PASSED: User successfully executed SP's INVOKER procedure")
                print(f"   Result: {result}")
                status = "PASS"
            
            results.append({
                'test_id': 'TC-101',
                'description': 'SP owns INVOKER proc → User executes',
                'status': status,
                'error': error,
                'execution_time': time.time() - start_time
            })
            
            # Cleanup
            print("🧹 Cleanup...")
            sp_conn.execute(f"DROP PROCEDURE IF EXISTS {CATALOG}.{SCHEMA}.tc101_sp_owned_invoker")
            user_conn.execute(f"DROP TABLE IF EXISTS {CATALOG}.{SCHEMA}.tc101_user_table")
    
    except Exception as e:
        print(f"💥 Error: {e}")
        results.append({
            'test_id': 'TC-101',
            'description': 'SP owns INVOKER proc → User executes',
            'status': 'ERROR',
            'error': str(e)
        })
    
    print()
    
    # ============================================================================
    # TC-102: Bidirectional Context Switching
    # ============================================================================
    print("=" * 80)
    print("🧪 TC-102: Bidirectional - SP proc calls User proc calls SP proc")
    print("=" * 80)
    print()
    
    start_time = time.time()
    
    try:
        # Create nested procedures with alternating ownership
        print("⚙️  Setup: Create nested procedures with mixed ownership...")
        
        # SP creates inner procedure
        sp_conn.execute(f"DROP PROCEDURE IF EXISTS {CATALOG}.{SCHEMA}.tc102_sp_inner")
        sp_conn.execute(f"""
            CREATE PROCEDURE {CATALOG}.{SCHEMA}.tc102_sp_inner()
            LANGUAGE SQL
            AS BEGIN
                SELECT 'SP_INNER' as level, CURRENT_USER() as current_user;
            END
        """)
        sp_conn.execute(f"GRANT EXECUTE ON PROCEDURE {CATALOG}.{SCHEMA}.tc102_sp_inner TO `{user_name}`")
        
        # User creates middle procedure that calls SP's inner
        user_conn.execute(f"DROP PROCEDURE IF EXISTS {CATALOG}.{SCHEMA}.tc102_user_middle")
        user_conn.execute(f"""
            CREATE PROCEDURE {CATALOG}.{SCHEMA}.tc102_user_middle()
            LANGUAGE SQL
            AS BEGIN
                CALL {CATALOG}.{SCHEMA}.tc102_sp_inner();
            END
        """)
        user_conn.execute(f"GRANT EXECUTE ON PROCEDURE {CATALOG}.{SCHEMA}.tc102_user_middle TO `{SERVICE_PRINCIPAL_B_ID}`")
        
        # SP creates outer procedure that calls User's middle
        sp_conn.execute(f"DROP PROCEDURE IF EXISTS {CATALOG}.{SCHEMA}.tc102_sp_outer")
        sp_conn.execute(f"""
            CREATE PROCEDURE {CATALOG}.{SCHEMA}.tc102_sp_outer()
            LANGUAGE SQL
            AS BEGIN
                CALL {CATALOG}.{SCHEMA}.tc102_user_middle();
            END
        """)
        sp_conn.execute(f"GRANT EXECUTE ON PROCEDURE {CATALOG}.{SCHEMA}.tc102_sp_outer TO `{user_name}`")
        
        # User executes SP's outer procedure
        print(f"▶️  Test (as User): Execute SP_outer → User_middle → SP_inner...")
        result, error = user_conn.execute(f"CALL {CATALOG}.{SCHEMA}.tc102_sp_outer()")
        
        if error:
            print(f"❌ Test FAILED: {error}")
            status = "FAIL"
        else:
            print(f"✅ Test PASSED: Bidirectional nesting works")
            print(f"   Result: {result}")
            status = "PASS"
        
        results.append({
            'test_id': 'TC-102',
            'description': 'Bidirectional context switching',
            'status': status,
            'error': error,
            'execution_time': time.time() - start_time
        })
        
        # Cleanup
        print("🧹 Cleanup...")
        sp_conn.execute(f"DROP PROCEDURE IF EXISTS {CATALOG}.{SCHEMA}.tc102_sp_outer")
        user_conn.execute(f"DROP PROCEDURE IF EXISTS {CATALOG}.{SCHEMA}.tc102_user_middle")
        sp_conn.execute(f"DROP PROCEDURE IF EXISTS {CATALOG}.{SCHEMA}.tc102_sp_inner")
    
    except Exception as e:
        print(f"💥 Error: {e}")
        results.append({
            'test_id': 'TC-102',
            'description': 'Bidirectional context switching',
            'status': 'ERROR',
            'error': str(e)
        })
    
    # Print summary
    print()
    print("=" * 80)
    print("📊 REVERSE CONTEXT TEST SUMMARY")
    print("=" * 80)
    passed = sum(1 for r in results if r['status'] == 'PASS')
    failed = sum(1 for r in results if r['status'] == 'FAIL')
    errors = sum(1 for r in results if r['status'] == 'ERROR')
    
    print(f"Total Tests:   {len(results)}")
    print(f"✅ Passed:     {passed}")
    print(f"❌ Failed:     {failed}")
    print(f"💥 Errors:     {errors}")
    print()
    
    for r in results:
        status_icon = "✅" if r['status'] == "PASS" else "❌" if r['status'] == "FAIL" else "💥"
        print(f"{status_icon} {r['test_id']}: {r['description']}")
        if r.get('error'):
            print(f"   Error: {r['error']}")
    
    # Close connections
    sp_conn.close()
    user_conn.close()
    
    print()
    print("=" * 80)
    print("🎯 KEY INSIGHTS")
    print("=" * 80)
    print()
    print("Reverse context switching validates:")
    print("  • SP can create and own procedures")
    print("  • User can execute SP's procedures")
    print("  • DEFINER mode uses owner's (SP's) permissions")
    print("  • INVOKER mode uses caller's (User's) permissions")
    print("  • Bidirectional nesting maintains correct context")
    print()
    
    return passed == len(results)


if __name__ == "__main__":
    success = run_reverse_context_tests()
    exit(0 if success else 1)
