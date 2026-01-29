#!/usr/bin/env python3
"""
Verify OAuth Identity
Check what user/SP the OAuth credentials actually authenticate as
"""

from framework.config import (
    SERVER_HOSTNAME, SERVICE_PRINCIPAL_CLIENT_ID, SERVICE_PRINCIPAL_CLIENT_SECRET
)

print("=" * 80)
print("🔍 OAuth Identity Verification")
print("=" * 80)
print()

try:
    from databricks.sdk import WorkspaceClient
    
    print(f"📋 Testing OAuth credentials:")
    print(f"  Client ID: {SERVICE_PRINCIPAL_CLIENT_ID}")
    print(f"  Client Secret: {SERVICE_PRINCIPAL_CLIENT_SECRET[:10]}... (masked)")
    print()
    
    print("🔗 Authenticating with Databricks SDK...")
    w = WorkspaceClient(
        host=f"https://{SERVER_HOSTNAME}",
        client_id=SERVICE_PRINCIPAL_CLIENT_ID,
        client_secret=SERVICE_PRINCIPAL_CLIENT_SECRET
    )
    
    print("✅ Authentication successful!")
    print()
    
    print("=" * 80)
    print("🎭 Identity Information")
    print("=" * 80)
    
    # Get current user info
    current_user = w.current_user.me()
    
    print(f"User Name:      {current_user.user_name}")
    print(f"Display Name:   {current_user.display_name}")
    print(f"Active:         {current_user.active}")
    
    if current_user.emails:
        print(f"Emails:         {[e.value for e in current_user.emails]}")
    
    print()
    print("=" * 80)
    print("🔍 Analysis")
    print("=" * 80)
    print()
    
    if current_user.user_name and '@' in current_user.user_name:
        print("❌ ISSUE: Authenticated as a USER, not a SERVICE PRINCIPAL!")
        print(f"   Current: {current_user.user_name}")
        print(f"   Expected: bugbash_ad_sp or {SERVICE_PRINCIPAL_CLIENT_ID}")
        print()
        print("📋 This means:")
        print("  • The OAuth credentials belong to a user account")
        print("  • Service Principal testing will not work correctly")
        print("  • All tests will run as the same user")
        print()
        print("🔧 Solutions:")
        print("  1. Generate OAuth credentials for the SERVICE PRINCIPAL (not user)")
        print("  2. OR use Service Principal PAT instead")
        print()
        exit(1)
    else:
        print("✅ Authenticated as SERVICE PRINCIPAL!")
        print(f"   SP Name: {current_user.user_name}")
        print(f"   SP ID: {SERVICE_PRINCIPAL_CLIENT_ID}")
        print()
        print("This is correct! Tests should work properly.")
        print()
        exit(0)

except ImportError:
    print("⚠️  databricks-sdk not installed")
    print("   Run: pip install databricks-sdk")
    exit(1)
    
except Exception as e:
    print(f"❌ Error: {e}")
    import traceback
    traceback.print_exc()
    exit(1)
