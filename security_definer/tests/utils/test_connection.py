#!/usr/bin/env python3
"""
Quick connectivity test for SQL SECURITY DEFINER Bug Bash
Run this from your local machine to verify connection before running full test suite
SECURE VERSION - Uses environment variables
"""

import sys
import os

def test_databricks_connection():
    """Test connection to Databricks"""
    
    print("="*60)
    print("🔍 SQL SECURITY DEFINER - Connectivity Test")
    print("="*60)
    
    # Step 1: Check if databricks-sql-connector is installed
    print("\n1️⃣ Checking dependencies...")
    try:
        import databricks.sql as dbsql
        print("   ✅ databricks-sql-connector installed")
    except ImportError:
        print("   ❌ databricks-sql-connector NOT installed")
        print("\n   Install it with:")
        print("   pip install databricks-sql-connector")
        return False
    
    # Step 2: Load configuration from environment
    print("\n2️⃣ Loading configuration...")
    
    server_hostname = os.getenv("DATABRICKS_SERVER_HOSTNAME", "e2-dogfood-unity-catalog-us-east-1.staging.cloud.databricks.com")
    http_path = os.getenv("DATABRICKS_HTTP_PATH", "/sql/1.0/warehouses/4bafae112e5b5f6e")
    access_token = os.getenv("DATABRICKS_PAT_TOKEN")
    catalog = os.getenv("DATABRICKS_CATALOG", "ad_bugbash")
    schema = os.getenv("DATABRICKS_SCHEMA", "ad_bugbash_schema")
    
    if not access_token:
        print("   ❌ DATABRICKS_PAT_TOKEN environment variable not set")
        print("\n   Set it with:")
        print("   export DATABRICKS_PAT_TOKEN='your_token_here'")
        print("\n   Or create a .env file:")
        print("   cp env.template .env")
        print("   # Edit .env with your token")
        return False
    
    print(f"   ✅ Configuration loaded")
    print(f"   Server: {server_hostname}")
    print(f"   Token: {'*' * 20} (hidden)")
    
    # Step 3: Test connection
    print("\n3️⃣ Testing Databricks connection...")
    
    try:
        print(f"   Connecting to: {server_hostname}")
        connection = dbsql.connect(
            server_hostname=server_hostname,
            http_path=http_path,
            access_token=access_token
        )
        print("   ✅ Connection established")
        
        # Step 4: Test query execution
        print("\n4️⃣ Testing query execution...")
        cursor = connection.cursor()
        
        # Set catalog and schema
        cursor.execute(f"USE CATALOG {catalog}")
        cursor.execute(f"USE SCHEMA {schema}")
        print(f"   ✅ Using catalog: {catalog}")
        print(f"   ✅ Using schema: {schema}")
        
        # Query current context
        cursor.execute("""
            SELECT 
                current_user() as user,
                current_catalog() as catalog,
                current_schema() as schema
        """)
        result = cursor.fetchone()
        
        print("\n5️⃣ Connection details:")
        print(f"   User:      {result[0]}")
        print(f"   Catalog:   {result[1]}")
        print(f"   Schema:    {result[2]}")
        
        # Step 5: Test stored procedure capability
        print("\n6️⃣ Testing stored procedure support...")
        try:
            cursor.execute(f"""
                SELECT COUNT(*) as proc_count 
                FROM information_schema.routines 
                WHERE routine_schema = '{schema}'
            """)
            proc_result = cursor.fetchone()
            print(f"   ✅ Can query stored procedures")
            print(f"   Found {proc_result[0]} existing procedures in schema")
        except Exception as e:
            print(f"   ⚠️  Stored procedure query: {e}")
        
        # Cleanup
        cursor.close()
        connection.close()
        
        print("\n" + "="*60)
        print("✅ SUCCESS! All connectivity tests passed")
        print("="*60)
        print("\n📝 Next steps:")
        print("   Run tests with:")
        print("   PYTHONPATH=. python run_tests.py")
        print("\n   Or run specific test suite:")
        print("   PYTHONPATH=. python -m pytest tests/core/")
        print("="*60)
        
        return True
        
    except Exception as e:
        print(f"\n❌ Connection failed!")
        print(f"   Error: {e}")
        print("\n🔧 Troubleshooting:")
        print("   • Check DATABRICKS_PAT_TOKEN is set correctly")
        print("   • Verify the PAT token is valid (not expired)")
        print("   • Ensure the warehouse is running")
        print("   • Check if you need VPN access")
        print("   • Verify your internet connection")
        return False

if __name__ == "__main__":
    success = test_databricks_connection()
    sys.exit(0 if success else 1)
