"""
SIMPLIFIED MULTI-LEVEL CONTEXT SWITCHING TESTS
Testing deep nesting with simpler approach for easier debugging
"""

from framework.test_framework import TestResult
from framework.service_principal_auth import ServicePrincipalAuth
from framework.config import (
    SERVER_HOSTNAME, HTTP_PATH, PAT_TOKEN, CATALOG, SCHEMA,
    SERVICE_PRINCIPAL_B_ID, SERVICE_PRINCIPAL_CLIENT_ID, SERVICE_PRINCIPAL_CLIENT_SECRET, USER_A
)
import time


def run_simplified_multilevel_tests():
    """
    Simplified 3-level context switching tests
    """
    
    if not SERVICE_PRINCIPAL_CLIENT_ID or not SERVICE_PRINCIPAL_CLIENT_SECRET:
        print("❌ SERVICE_PRINCIPAL OAuth credentials not configured!")
        return False
    
    print("=" * 80)
    print("🔄 SIMPLIFIED MULTI-LEVEL CONTEXT TESTS")
    print("=" * 80)
    print()
    
    from framework.test_framework import DatabricksConnection
    
    # Connect as both
    user_conn = DatabricksConnection(SERVER_HOSTNAME, HTTP_PATH, PAT_TOKEN, CATALOG, SCHEMA)
    sp_conn = ServicePrincipalAuth(
        server_hostname=SERVER_HOSTNAME,
        http_path=HTTP_PATH,
        sp_client_id=SERVICE_PRINCIPAL_CLIENT_ID,
        sp_client_secret=SERVICE_PRINCIPAL_CLIENT_SECRET,
        catalog=CATALOG,
        schema=SCHEMA
    )
    sp_conn.connect()
    
    user_name = user_conn.execute("SELECT CURRENT_USER()")[0][0][0]
    
    print(f"👤 User: {user_name}")
    print(f"🤖 SP ID: {SERVICE_PRINCIPAL_B_ID}")
    print()
    
    results = []
    
    # ============================================================================
    # TC-120: 3-Level Simple Nesting (User→SP→User)
    # ============================================================================
    print("=" * 80)
    print("🧪 TC-120: 3-Level Simple Nesting (User→SP→User)")
    print("=" * 80)
    print()
    
    start_time = time.time()
    
    try:
        # Level 3 (innermost) - User owns
        print("⚙️  Creating Level 3 (User-owned)...")
        user_conn.execute(f"DROP PROCEDURE IF EXISTS {CATALOG}.{SCHEMA}.tc120_level3_user")
        user_conn.execute(f"""
            CREATE PROCEDURE {CATALOG}.{SCHEMA}.tc120_level3_user()
            LANGUAGE SQL
            AS BEGIN
                SELECT 3 as level, 'User' as owner;
            END
        """)
        
        # Level 2 (middle) - SP owns, calls Level 3
        print("⚙️  Creating Level 2 (SP-owned, calls Level 3)...")
        sp_conn.execute(f"DROP PROCEDURE IF EXISTS {CATALOG}.{SCHEMA}.tc120_level2_sp")
        sp_conn.execute(f"""
            CREATE PROCEDURE {CATALOG}.{SCHEMA}.tc120_level2_sp()
            LANGUAGE SQL
            AS BEGIN
                CALL {CATALOG}.{SCHEMA}.tc120_level3_user();
            END
        """)
        
        # Level 1 (outermost) - User owns, calls Level 2
        print("⚙️  Creating Level 1 (User-owned, calls Level 2)...")
        user_conn.execute(f"DROP PROCEDURE IF EXISTS {CATALOG}.{SCHEMA}.tc120_level1_user")
        user_conn.execute(f"""
            CREATE PROCEDURE {CATALOG}.{SCHEMA}.tc120_level1_user()
            LANGUAGE SQL
            AS BEGIN
                CALL {CATALOG}.{SCHEMA}.tc120_level2_sp();
            END
        """)
        
        # Grant EXECUTE permissions
        print("⚙️  Granting EXECUTE permissions...")
        # SP needs to call User's Level 3
        user_conn.execute(f"GRANT EXECUTE ON PROCEDURE {CATALOG}.{SCHEMA}.tc120_level3_user TO `{SERVICE_PRINCIPAL_B_ID}`")
        # User needs to call SP's Level 2
        sp_conn.execute(f"GRANT EXECUTE ON PROCEDURE {CATALOG}.{SCHEMA}.tc120_level2_sp TO `{user_name}`")
        
        # Execute the chain
        print("▶️  Executing 3-level chain...")
        result, error = user_conn.execute(f"CALL {CATALOG}.{SCHEMA}.tc120_level1_user()")
        
        if error:
            print(f"❌ Test FAILED: {error[:200]}")
            status = "FAIL"
        else:
            print(f"✅ Test PASSED: 3-level nesting successful")
            print(f"   Result: {result}")
            status = "PASS"
        
        results.append({
            'test_id': 'TC-120',
            'description': '3-level simple nesting (User→SP→User)',
            'status': status,
            'error': error,
            'execution_time': time.time() - start_time
        })
        
        # Cleanup
        print("🧹 Cleanup...")
        user_conn.execute(f"DROP PROCEDURE IF EXISTS {CATALOG}.{SCHEMA}.tc120_level1_user")
        sp_conn.execute(f"DROP PROCEDURE IF EXISTS {CATALOG}.{SCHEMA}.tc120_level2_sp")
        user_conn.execute(f"DROP PROCEDURE IF EXISTS {CATALOG}.{SCHEMA}.tc120_level3_user")
    
    except Exception as e:
        print(f"💥 Error: {e}")
        import traceback
        traceback.print_exc()
        results.append({
            'test_id': 'TC-120',
            'description': '3-level simple nesting',
            'status': 'ERROR',
            'error': str(e)
        })
    
    print()
    
    # ============================================================================
    # TC-121: 3-Level with DEFINER/INVOKER Mix
    # ============================================================================
    print("=" * 80)
    print("🧪 TC-121: 3-Level with Mixed DEFINER/INVOKER")
    print("=" * 80)
    print()
    
    start_time = time.time()
    
    try:
        # Create shared table
        print("⚙️  Creating shared table...")
        user_conn.execute(f"DROP TABLE IF EXISTS {CATALOG}.{SCHEMA}.tc121_shared")
        user_conn.execute(f"CREATE TABLE {CATALOG}.{SCHEMA}.tc121_shared (level INT, mode STRING)")
        user_conn.execute(f"INSERT INTO {CATALOG}.{SCHEMA}.tc121_shared VALUES (1, 'DEFINER'), (2, 'INVOKER'), (3, 'DEFINER')")
        sp_conn.execute(f"GRANT SELECT ON TABLE {CATALOG}.{SCHEMA}.tc121_shared TO `{SERVICE_PRINCIPAL_B_ID}`")
        
        # Level 3 - DEFINER
        print("⚙️  Creating Level 3 (DEFINER)...")
        user_conn.execute(f"DROP PROCEDURE IF EXISTS {CATALOG}.{SCHEMA}.tc121_l3_definer")
        user_conn.execute(f"""
            CREATE PROCEDURE {CATALOG}.{SCHEMA}.tc121_l3_definer()
            LANGUAGE SQL
            AS BEGIN
                SELECT COUNT(*) as l3_count FROM {CATALOG}.{SCHEMA}.tc121_shared WHERE level = 3;
            END
        """)
        
        # Level 2 - INVOKER
        print("⚙️  Creating Level 2 (INVOKER)...")
        sp_conn.execute(f"DROP PROCEDURE IF EXISTS {CATALOG}.{SCHEMA}.tc121_l2_invoker")
        sp_conn.execute(f"""
            CREATE PROCEDURE {CATALOG}.{SCHEMA}.tc121_l2_invoker()
            LANGUAGE SQL
            SQL SECURITY INVOKER
            AS BEGIN
                CALL {CATALOG}.{SCHEMA}.tc121_l3_definer();
            END
        """)
        
        # Level 1 - DEFINER
        print("⚙️  Creating Level 1 (DEFINER)...")
        user_conn.execute(f"DROP PROCEDURE IF EXISTS {CATALOG}.{SCHEMA}.tc121_l1_definer")
        user_conn.execute(f"""
            CREATE PROCEDURE {CATALOG}.{SCHEMA}.tc121_l1_definer()
            LANGUAGE SQL
            AS BEGIN
                CALL {CATALOG}.{SCHEMA}.tc121_l2_invoker();
            END
        """)
        
        # Grant permissions
        print("⚙️  Granting permissions...")
        user_conn.execute(f"GRANT EXECUTE ON PROCEDURE {CATALOG}.{SCHEMA}.tc121_l3_definer TO `{SERVICE_PRINCIPAL_B_ID}`")
        sp_conn.execute(f"GRANT EXECUTE ON PROCEDURE {CATALOG}.{SCHEMA}.tc121_l2_invoker TO `{user_name}`")
        
        # Execute
        print("▶️  Executing mixed mode chain...")
        result, error = user_conn.execute(f"CALL {CATALOG}.{SCHEMA}.tc121_l1_definer()")
        
        if error:
            print(f"❌ Test FAILED: {error[:200]}")
            status = "FAIL"
        else:
            print(f"✅ Test PASSED: Mixed mode nesting successful")
            print(f"   Result: {result}")
            status = "PASS"
        
        results.append({
            'test_id': 'TC-121',
            'description': '3-level mixed DEFINER/INVOKER modes',
            'status': status,
            'error': error,
            'execution_time': time.time() - start_time
        })
        
        # Cleanup
        print("🧹 Cleanup...")
        user_conn.execute(f"DROP PROCEDURE IF EXISTS {CATALOG}.{SCHEMA}.tc121_l1_definer")
        sp_conn.execute(f"DROP PROCEDURE IF EXISTS {CATALOG}.{SCHEMA}.tc121_l2_invoker")
        user_conn.execute(f"DROP PROCEDURE IF EXISTS {CATALOG}.{SCHEMA}.tc121_l3_definer")
        user_conn.execute(f"DROP TABLE IF EXISTS {CATALOG}.{SCHEMA}.tc121_shared")
    
    except Exception as e:
        print(f"💥 Error: {e}")
        import traceback
        traceback.print_exc()
        results.append({
            'test_id': 'TC-121',
            'description': '3-level mixed modes',
            'status': 'ERROR',
            'error': str(e)
        })
    
    print()
    
    # Print summary
    print("=" * 80)
    print("📊 SUMMARY")
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
        if r['status'] == 'PASS':
            print(f"✅ {r['test_id']}: {r['description']}")
        elif r['status'] == 'FAIL':
            print(f"❌ {r['test_id']}: {r['description']}")
        else:
            print(f"💥 {r['test_id']}: {r['description']}")
    
    # Close connections
    sp_conn.close()
    user_conn.close()
    
    return failed == 0 and errors == 0


if __name__ == "__main__":
    success = run_simplified_multilevel_tests()
    exit(0 if success else 1)
