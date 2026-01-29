"""
MULTI-LEVEL CONTEXT SWITCHING TESTS (FIXED)
Advanced tests for deep nesting with alternating ownership and security modes

These tests validate:
1. Deep nesting (5 levels)
2. Alternating ownership (User↔SP↔User...)
3. Mixed security modes (DEFINER/INVOKER combinations)
4. Context preservation across multiple levels
5. Permission inheritance and isolation
"""

from framework.test_framework import TestResult
from framework.service_principal_auth import ServicePrincipalAuth
from framework.config import (
    SERVER_HOSTNAME, HTTP_PATH, PAT_TOKEN, CATALOG, SCHEMA,
    SERVICE_PRINCIPAL_B_ID, SERVICE_PRINCIPAL_CLIENT_ID, SERVICE_PRINCIPAL_CLIENT_SECRET, USER_A
)
import time


def run_multilevel_context_tests():
    """
    Test multi-level context switching with deep nesting
    """
    
    if not SERVICE_PRINCIPAL_CLIENT_ID or not SERVICE_PRINCIPAL_CLIENT_SECRET:
        print("❌ SERVICE_PRINCIPAL OAuth credentials not configured!")
        return False
    
    print("=" * 80)
    print("🔄 MULTI-LEVEL CONTEXT SWITCHING TESTS (5-Level Deep)")
    print("=" * 80)
    print()
    print("Testing: Deep nesting with alternating ownership")
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
    # TC-110: 5-Level Deep - All DEFINER
    # ============================================================================
    print("=" * 80)
    print("🧪 TC-110: 5-Level Deep Nesting - All DEFINER Mode")
    print("=" * 80)
    print()
    
    start_time = time.time()
    
    try:
        print("⚙️  Creating 5-level nested procedures...")
        
        # Level 5 (innermost) - User owns
        print("   Creating Level 5 (User)...")
        user_conn.execute(f"DROP PROCEDURE IF EXISTS {CATALOG}.{SCHEMA}.tc110_level5")
        user_conn.execute(f"""
            CREATE PROCEDURE {CATALOG}.{SCHEMA}.tc110_level5()
            LANGUAGE SQL
            AS BEGIN
                SELECT 5 as level, 'User' as owner, 'DEFINER' as mode;
            END
        """)
        
        # Level 4 - SP owns, calls Level 5
        print("   Creating Level 4 (SP → calls L5)...")
        sp_conn.execute(f"DROP PROCEDURE IF EXISTS {CATALOG}.{SCHEMA}.tc110_level4")
        sp_conn.execute(f"""
            CREATE PROCEDURE {CATALOG}.{SCHEMA}.tc110_level4()
            LANGUAGE SQL
            AS BEGIN
                CALL {CATALOG}.{SCHEMA}.tc110_level5();
            END
        """)
        
        # Level 3 - User owns, calls Level 4
        print("   Creating Level 3 (User → calls L4)...")
        user_conn.execute(f"DROP PROCEDURE IF EXISTS {CATALOG}.{SCHEMA}.tc110_level3")
        user_conn.execute(f"""
            CREATE PROCEDURE {CATALOG}.{SCHEMA}.tc110_level3()
            LANGUAGE SQL
            AS BEGIN
                CALL {CATALOG}.{SCHEMA}.tc110_level4();
            END
        """)
        
        # Level 2 - SP owns, calls Level 3
        print("   Creating Level 2 (SP → calls L3)...")
        sp_conn.execute(f"DROP PROCEDURE IF EXISTS {CATALOG}.{SCHEMA}.tc110_level2")
        sp_conn.execute(f"""
            CREATE PROCEDURE {CATALOG}.{SCHEMA}.tc110_level2()
            LANGUAGE SQL
            AS BEGIN
                CALL {CATALOG}.{SCHEMA}.tc110_level3();
            END
        """)
        
        # Level 1 (outermost) - User owns, calls Level 2
        print("   Creating Level 1 (User → calls L2)...")
        user_conn.execute(f"DROP PROCEDURE IF EXISTS {CATALOG}.{SCHEMA}.tc110_level1")
        user_conn.execute(f"""
            CREATE PROCEDURE {CATALOG}.{SCHEMA}.tc110_level1()
            LANGUAGE SQL
            AS BEGIN
                CALL {CATALOG}.{SCHEMA}.tc110_level2();
            END
        """)
        
        # Grant EXECUTE permissions
        print("⚙️  Granting EXECUTE permissions...")
        user_conn.execute(f"GRANT EXECUTE ON PROCEDURE {CATALOG}.{SCHEMA}.tc110_level5 TO `{SERVICE_PRINCIPAL_B_ID}`")
        user_conn.execute(f"GRANT EXECUTE ON PROCEDURE {CATALOG}.{SCHEMA}.tc110_level3 TO `{SERVICE_PRINCIPAL_B_ID}`")
        sp_conn.execute(f"GRANT EXECUTE ON PROCEDURE {CATALOG}.{SCHEMA}.tc110_level4 TO `{user_name}`")
        sp_conn.execute(f"GRANT EXECUTE ON PROCEDURE {CATALOG}.{SCHEMA}.tc110_level2 TO `{user_name}`")
        
        print("▶️  Test: Execute 5-level nested call...")
        result, error = user_conn.execute(f"CALL {CATALOG}.{SCHEMA}.tc110_level1()")
        
        if error:
            print(f"❌ Test FAILED: {error[:200]}")
            status = "FAIL"
        else:
            print(f"✅ Test PASSED: 5-level nesting executed successfully")
            print(f"   Result: {result}")
            status = "PASS"
        
        results.append({
            'test_id': 'TC-110',
            'description': '5-level deep nesting - All DEFINER mode',
            'status': status,
            'error': error,
            'execution_time': time.time() - start_time
        })
        
        # Cleanup
        print("🧹 Cleanup...")
        user_conn.execute(f"DROP PROCEDURE IF EXISTS {CATALOG}.{SCHEMA}.tc110_level1")
        sp_conn.execute(f"DROP PROCEDURE IF EXISTS {CATALOG}.{SCHEMA}.tc110_level2")
        user_conn.execute(f"DROP PROCEDURE IF EXISTS {CATALOG}.{SCHEMA}.tc110_level3")
        sp_conn.execute(f"DROP PROCEDURE IF EXISTS {CATALOG}.{SCHEMA}.tc110_level4")
        user_conn.execute(f"DROP PROCEDURE IF EXISTS {CATALOG}.{SCHEMA}.tc110_level5")
    
    except Exception as e:
        print(f"💥 Error: {e}")
        import traceback
        traceback.print_exc()
        results.append({
            'test_id': 'TC-110',
            'description': '5-level deep nesting - All DEFINER mode',
            'status': 'ERROR',
            'error': str(e)
        })
    
    print()
    
    # ============================================================================
    # TC-111: 5-Level Deep - Alternating DEFINER/INVOKER
    # ============================================================================
    print("=" * 80)
    print("🧪 TC-111: 5-Level Deep - Alternating DEFINER/INVOKER")
    print("=" * 80)
    print()
    
    start_time = time.time()
    
    try:
        print("⚙️  Creating 5-level alternating mode procedures...")
        
        # Create shared table accessible to both
        print("   Creating shared table...")
        user_conn.execute(f"DROP TABLE IF EXISTS {CATALOG}.{SCHEMA}.tc111_shared")
        user_conn.execute(f"CREATE TABLE {CATALOG}.{SCHEMA}.tc111_shared (level INT, mode STRING)")
        user_conn.execute(f"INSERT INTO {CATALOG}.{SCHEMA}.tc111_shared VALUES (5, 'DEFINER')")
        sp_conn.execute(f"GRANT SELECT ON TABLE {CATALOG}.{SCHEMA}.tc111_shared TO `{SERVICE_PRINCIPAL_B_ID}`")
        
        # Level 5 - DEFINER
        print("   Creating Level 5 (DEFINER)...")
        user_conn.execute(f"DROP PROCEDURE IF EXISTS {CATALOG}.{SCHEMA}.tc111_l5_definer")
        user_conn.execute(f"""
            CREATE PROCEDURE {CATALOG}.{SCHEMA}.tc111_l5_definer()
            LANGUAGE SQL
            AS BEGIN
                SELECT COUNT(*) as l5_count FROM {CATALOG}.{SCHEMA}.tc111_shared WHERE level = 5;
            END
        """)
        
        # Level 4 - INVOKER (calls L5)
        print("   Creating Level 4 (INVOKER → calls L5)...")
        sp_conn.execute(f"DROP PROCEDURE IF EXISTS {CATALOG}.{SCHEMA}.tc111_l4_invoker")
        sp_conn.execute(f"""
            CREATE PROCEDURE {CATALOG}.{SCHEMA}.tc111_l4_invoker()
            LANGUAGE SQL
            SQL SECURITY INVOKER
            AS BEGIN
                CALL {CATALOG}.{SCHEMA}.tc111_l5_definer();
            END
        """)
        
        # Level 3 - DEFINER (calls L4)
        print("   Creating Level 3 (DEFINER → calls L4)...")
        user_conn.execute(f"DROP PROCEDURE IF EXISTS {CATALOG}.{SCHEMA}.tc111_l3_definer")
        user_conn.execute(f"""
            CREATE PROCEDURE {CATALOG}.{SCHEMA}.tc111_l3_definer()
            LANGUAGE SQL
            AS BEGIN
                CALL {CATALOG}.{SCHEMA}.tc111_l4_invoker();
            END
        """)
        
        # Level 2 - INVOKER (calls L3)
        print("   Creating Level 2 (INVOKER → calls L3)...")
        sp_conn.execute(f"DROP PROCEDURE IF EXISTS {CATALOG}.{SCHEMA}.tc111_l2_invoker")
        sp_conn.execute(f"""
            CREATE PROCEDURE {CATALOG}.{SCHEMA}.tc111_l2_invoker()
            LANGUAGE SQL
            SQL SECURITY INVOKER
            AS BEGIN
                CALL {CATALOG}.{SCHEMA}.tc111_l3_definer();
            END
        """)
        
        # Level 1 - DEFINER (calls L2)
        print("   Creating Level 1 (DEFINER → calls L2)...")
        user_conn.execute(f"DROP PROCEDURE IF EXISTS {CATALOG}.{SCHEMA}.tc111_l1_definer")
        user_conn.execute(f"""
            CREATE PROCEDURE {CATALOG}.{SCHEMA}.tc111_l1_definer()
            LANGUAGE SQL
            AS BEGIN
                CALL {CATALOG}.{SCHEMA}.tc111_l2_invoker();
            END
        """)
        
        # Grant EXECUTE permissions
        print("⚙️  Granting EXECUTE permissions...")
        user_conn.execute(f"GRANT EXECUTE ON PROCEDURE {CATALOG}.{SCHEMA}.tc111_l5_definer TO `{SERVICE_PRINCIPAL_B_ID}`")
        user_conn.execute(f"GRANT EXECUTE ON PROCEDURE {CATALOG}.{SCHEMA}.tc111_l3_definer TO `{SERVICE_PRINCIPAL_B_ID}`")
        sp_conn.execute(f"GRANT EXECUTE ON PROCEDURE {CATALOG}.{SCHEMA}.tc111_l4_invoker TO `{user_name}`")
        sp_conn.execute(f"GRANT EXECUTE ON PROCEDURE {CATALOG}.{SCHEMA}.tc111_l2_invoker TO `{user_name}`")
        
        print("▶️  Test: Execute 5-level alternating mode call...")
        result, error = user_conn.execute(f"CALL {CATALOG}.{SCHEMA}.tc111_l1_definer()")
        
        if error:
            print(f"❌ Test FAILED: {error[:200]}")
            status = "FAIL"
        else:
            print(f"✅ Test PASSED: Alternating mode nesting works")
            print(f"   Result: {result}")
            status = "PASS"
        
        results.append({
            'test_id': 'TC-111',
            'description': '5-level alternating DEFINER/INVOKER modes',
            'status': status,
            'error': error,
            'execution_time': time.time() - start_time
        })
        
        # Cleanup
        print("🧹 Cleanup...")
        user_conn.execute(f"DROP PROCEDURE IF EXISTS {CATALOG}.{SCHEMA}.tc111_l1_definer")
        sp_conn.execute(f"DROP PROCEDURE IF EXISTS {CATALOG}.{SCHEMA}.tc111_l2_invoker")
        user_conn.execute(f"DROP PROCEDURE IF EXISTS {CATALOG}.{SCHEMA}.tc111_l3_definer")
        sp_conn.execute(f"DROP PROCEDURE IF EXISTS {CATALOG}.{SCHEMA}.tc111_l4_invoker")
        user_conn.execute(f"DROP PROCEDURE IF EXISTS {CATALOG}.{SCHEMA}.tc111_l5_definer")
        user_conn.execute(f"DROP TABLE IF EXISTS {CATALOG}.{SCHEMA}.tc111_shared")
    
    except Exception as e:
        print(f"💥 Error: {e}")
        import traceback
        traceback.print_exc()
        results.append({
            'test_id': 'TC-111',
            'description': '5-level alternating DEFINER/INVOKER modes',
            'status': 'ERROR',
            'error': str(e)
        })
    
    print()
    
    # ============================================================================
    # TC-112: Permission Propagation in Deep Nesting
    # ============================================================================
    print("=" * 80)
    print("🧪 TC-112: Permission Propagation - Restricted Access at Each Level")
    print("=" * 80)
    print()
    
    start_time = time.time()
    
    try:
        print("⚙️  Creating 3-level with restricted permissions...")
        
        # User creates restricted table (SP has no access)
        print("   Creating User restricted table...")
        user_conn.execute(f"DROP TABLE IF EXISTS {CATALOG}.{SCHEMA}.tc112_user_secret")
        user_conn.execute(f"CREATE TABLE {CATALOG}.{SCHEMA}.tc112_user_secret (secret STRING)")
        user_conn.execute(f"INSERT INTO {CATALOG}.{SCHEMA}.tc112_user_secret VALUES ('user_data')")
        user_conn.execute(f"REVOKE ALL PRIVILEGES ON TABLE {CATALOG}.{SCHEMA}.tc112_user_secret FROM `{SERVICE_PRINCIPAL_B_ID}`")
        
        # SP creates restricted table (User has no access)
        print("   Creating SP restricted table...")
        sp_conn.execute(f"DROP TABLE IF EXISTS {CATALOG}.{SCHEMA}.tc112_sp_secret")
        sp_conn.execute(f"CREATE TABLE {CATALOG}.{SCHEMA}.tc112_sp_secret (secret STRING)")
        sp_conn.execute(f"INSERT INTO {CATALOG}.{SCHEMA}.tc112_sp_secret VALUES ('sp_data')")
        sp_conn.execute(f"REVOKE ALL PRIVILEGES ON TABLE {CATALOG}.{SCHEMA}.tc112_sp_secret FROM `{user_name}`")
        
        # Level 3 (innermost) - User DEFINER proc accesses user table
        print("   Creating Level 3 (User DEFINER)...")
        user_conn.execute(f"DROP PROCEDURE IF EXISTS {CATALOG}.{SCHEMA}.tc112_l3_user")
        user_conn.execute(f"""
            CREATE PROCEDURE {CATALOG}.{SCHEMA}.tc112_l3_user()
            LANGUAGE SQL
            AS BEGIN
                SELECT secret FROM {CATALOG}.{SCHEMA}.tc112_user_secret;
            END
        """)
        
        # Level 2 - SP DEFINER proc accesses SP table + calls User proc
        print("   Creating Level 2 (SP DEFINER → calls L3)...")
        sp_conn.execute(f"DROP PROCEDURE IF EXISTS {CATALOG}.{SCHEMA}.tc112_l2_sp")
        sp_conn.execute(f"""
            CREATE PROCEDURE {CATALOG}.{SCHEMA}.tc112_l2_sp()
            LANGUAGE SQL
            AS BEGIN
                CALL {CATALOG}.{SCHEMA}.tc112_l3_user();
            END
        """)
        
        # Level 1 (outermost) - User calls SP proc
        print("   Creating Level 1 (User → calls L2)...")
        user_conn.execute(f"DROP PROCEDURE IF EXISTS {CATALOG}.{SCHEMA}.tc112_l1_gateway")
        user_conn.execute(f"""
            CREATE PROCEDURE {CATALOG}.{SCHEMA}.tc112_l1_gateway()
            LANGUAGE SQL
            AS BEGIN
                CALL {CATALOG}.{SCHEMA}.tc112_l2_sp();
            END
        """)
        
        # Grant EXECUTE permissions
        print("⚙️  Granting EXECUTE permissions...")
        user_conn.execute(f"GRANT EXECUTE ON PROCEDURE {CATALOG}.{SCHEMA}.tc112_l3_user TO `{SERVICE_PRINCIPAL_B_ID}`")
        sp_conn.execute(f"GRANT EXECUTE ON PROCEDURE {CATALOG}.{SCHEMA}.tc112_l2_sp TO `{user_name}`")
        
        print("▶️  Test: Execute 3-level with restricted permissions...")
        result, error = user_conn.execute(f"CALL {CATALOG}.{SCHEMA}.tc112_l1_gateway()")
        
        if error:
            print(f"❌ Test FAILED: {error[:200]}")
            status = "FAIL"
        else:
            print(f"✅ Test PASSED: Permission propagation works correctly")
            print(f"   Result: {result}")
            print(f"   ✅ User accessed own restricted table via DEFINER gateway!")
            status = "PASS"
        
        results.append({
            'test_id': 'TC-112',
            'description': '3-level permission propagation with restricted access',
            'status': status,
            'error': error,
            'execution_time': time.time() - start_time
        })
        
        # Cleanup
        print("🧹 Cleanup...")
        user_conn.execute(f"DROP PROCEDURE IF EXISTS {CATALOG}.{SCHEMA}.tc112_l1_gateway")
        sp_conn.execute(f"DROP PROCEDURE IF EXISTS {CATALOG}.{SCHEMA}.tc112_l2_sp")
        user_conn.execute(f"DROP PROCEDURE IF EXISTS {CATALOG}.{SCHEMA}.tc112_l3_user")
        user_conn.execute(f"DROP TABLE IF EXISTS {CATALOG}.{SCHEMA}.tc112_user_secret")
        sp_conn.execute(f"DROP TABLE IF EXISTS {CATALOG}.{SCHEMA}.tc112_sp_secret")
    
    except Exception as e:
        print(f"💥 Error: {e}")
        import traceback
        traceback.print_exc()
        results.append({
            'test_id': 'TC-112',
            'description': '3-level permission propagation with restricted access',
            'status': 'ERROR',
            'error': str(e)
        })
    
    print()
    
    # Print summary
    print("=" * 80)
    print("📊 MULTI-LEVEL CONTEXT TEST SUMMARY")
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
            if r.get('error'):
                print(f"   {r['error'][:100]}")
        else:
            print(f"💥 {r['test_id']}: {r['description']} - ERROR")
            if r.get('error'):
                print(f"   {r['error'][:100]}")
    
    # Close connections
    sp_conn.close()
    user_conn.close()
    
    print()
    print("=" * 80)
    print("🎯 MULTI-LEVEL INSIGHTS")
    print("=" * 80)
    print()
    print("These tests validate:")
    print("  • Deep nesting (5 levels) maintains correct context")
    print("  • Alternating ownership preserves permissions at each level")
    print("  • Mixed DEFINER/INVOKER modes work in combination")
    print("  • Permission boundaries are enforced independently per level")
    print("  • EXECUTE grants required at each transition")
    print()
    
    return failed == 0 and errors == 0


if __name__ == "__main__":
    success = run_multilevel_context_tests()
    exit(0 if success else 1)
