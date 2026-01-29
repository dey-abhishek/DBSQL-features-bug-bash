#!/usr/bin/env python3
"""
Test OAuth M2M Authentication for Service Principal
"""

from framework.service_principal_auth import ServicePrincipalAuth
from framework.config import (
    SERVER_HOSTNAME, HTTP_PATH, CATALOG, SCHEMA,
    SERVICE_PRINCIPAL_CLIENT_ID, SERVICE_PRINCIPAL_CLIENT_SECRET
)

def test_oauth_connection():
    print("=" * 80)
    print("🔐 Testing OAuth M2M Authentication")
    print("=" * 80)
    print()
    
    if not SERVICE_PRINCIPAL_CLIENT_ID or not SERVICE_PRINCIPAL_CLIENT_SECRET:
        print("❌ OAuth credentials not configured!")
        print()
        print("Update framework/config.py with:")
        print("  SERVICE_PRINCIPAL_CLIENT_ID = '...'")
        print("  SERVICE_PRINCIPAL_CLIENT_SECRET = '...'")
        return False
    
    print(f"📋 Configuration:")
    print(f"  Server: {SERVER_HOSTNAME}")
    print(f"  Warehouse Path: {HTTP_PATH}")
    print(f"  Client ID: {SERVICE_PRINCIPAL_CLIENT_ID}")
    print(f"  Client Secret: {SERVICE_PRINCIPAL_CLIENT_SECRET[:10]}... (masked)")
    print(f"  Catalog: {CATALOG}")
    print(f"  Schema: {SCHEMA}")
    print()
    
    try:
        print("🔗 Connecting as Service Principal...")
        sp_auth = ServicePrincipalAuth(
            server_hostname=SERVER_HOSTNAME,
            http_path=HTTP_PATH,
            sp_client_id=SERVICE_PRINCIPAL_CLIENT_ID,
            sp_client_secret=SERVICE_PRINCIPAL_CLIENT_SECRET,
            catalog=CATALOG,
            schema=SCHEMA
        )
        
        if not sp_auth.connect():
            print("❌ Connection failed")
            return False
        
        print()
        print("=" * 80)
        print("✅ Connection Successful!")
        print("=" * 80)
        print()
        
        # Test queries
        print("🧪 Running test queries...")
        print()
        
        # Test 1: Get current user
        print("1️⃣  SELECT CURRENT_USER()")
        user = sp_auth.get_current_user()
        print(f"   Result: {user}")
        print()
        
        # Test 2: Get catalog and schema
        print("2️⃣  SELECT CURRENT_CATALOG(), CURRENT_SCHEMA()")
        result, error = sp_auth.execute("SELECT CURRENT_CATALOG(), CURRENT_SCHEMA()")
        if error:
            print(f"   ❌ Error: {error}")
        else:
            print(f"   Catalog: {result[0][0]}")
            print(f"   Schema: {result[0][1]}")
        print()
        
        # Test 3: List tables
        print("3️⃣  SHOW TABLES")
        result, error = sp_auth.execute(f"SHOW TABLES IN {CATALOG}.{SCHEMA}")
        if error:
            print(f"   ⚠️  Error: {error}")
        else:
            table_count = len(result) if result else 0
            print(f"   Found {table_count} tables")
        print()
        
        sp_auth.close()
        
        print("=" * 80)
        print("✅ OAuth M2M Authentication Working!")
        print("=" * 80)
        print()
        print("You can now run:")
        print("  • python tests/advanced/test_reverse_context.py")
        print("  • python tests/advanced/test_negative_context.py")
        print()
        
        return True
        
    except Exception as e:
        print(f"❌ Error: {e}")
        import traceback
        traceback.print_exc()
        return False


if __name__ == "__main__":
    success = test_oauth_connection()
    exit(0 if success else 1)
