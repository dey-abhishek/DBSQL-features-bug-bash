#!/bin/bash
#
# Generate Service Principal PAT Helper Script
# This script guides you through generating a PAT for the service principal
#

set -e

echo "=============================================================================="
echo "🔐 Service Principal PAT Generation Guide"
echo "=============================================================================="
echo ""
echo "To run the new context switching and negative tests, you need a PAT for:"
echo "  Service Principal: bugbash_ad_sp"
echo "  UUID: ***REDACTED-SP-UUID***"
echo ""
echo "=============================================================================="
echo "📋 Step-by-Step Instructions"
echo "=============================================================================="
echo ""
echo "1. Navigate to your Databricks workspace:"
echo "   https://e2-dogfood-unity-catalog-us-east-1.staging.cloud.databricks.com/?o=653212377970039"
echo ""
echo "2. Go to: Settings → Admin Console → Service Principals"
echo ""
echo "3. Find and click: bugbash_ad_sp"
echo ""
echo "4. Go to the 'Access tokens' tab"
echo ""
echo "5. Click 'Generate new token'"
echo "   - Comment: 'Bug bash testing - SQL SECURITY DEFINER'"
echo "   - Lifetime: 90 days (or as needed)"
echo ""
echo "6. Click 'Generate' and COPY the token (starts with 'dapi...')"
echo ""
echo "7. Paste it below when prompted"
echo ""
echo "=============================================================================="
echo ""

# Check if PAT is already configured
if grep -q "SERVICE_PRINCIPAL_PAT = \"dapi" framework/config.py 2>/dev/null; then
    echo "✅ SERVICE_PRINCIPAL_PAT already configured in framework/config.py"
    echo ""
    read -p "Do you want to update it? (y/N): " update
    if [[ ! "$update" =~ ^[Yy]$ ]]; then
        echo "Keeping existing PAT. Exiting."
        exit 0
    fi
fi

echo "Enter the Service Principal PAT (or press Ctrl+C to cancel):"
read -r sp_pat

# Validate PAT format
if [[ ! "$sp_pat" =~ ^dapi[a-zA-Z0-9]+ ]]; then
    echo "❌ Error: PAT should start with 'dapi' followed by alphanumeric characters"
    exit 1
fi

echo ""
echo "✅ PAT received (length: ${#sp_pat} characters)"
echo ""

# Update config.py
echo "📝 Updating framework/config.py..."

# Use sed to replace the line
sed -i.bak "s|SERVICE_PRINCIPAL_PAT = None.*|SERVICE_PRINCIPAL_PAT = \"$sp_pat\"  # Generated $(date '+%Y-%m-%d')|" framework/config.py

if [ $? -eq 0 ]; then
    echo "✅ Configuration updated successfully!"
    echo ""
    echo "Backup saved as: framework/config.py.bak"
    echo ""
    echo "=============================================================================="
    echo "🧪 Testing Connection"
    echo "=============================================================================="
    echo ""
    
    # Test the connection
    PYTHONPATH=. ./security_definer/bin/python framework/service_principal_auth.py
    
    if [ $? -eq 0 ]; then
        echo ""
        echo "=============================================================================="
        echo "🎉 SUCCESS! Service Principal authentication is working!"
        echo "=============================================================================="
        echo ""
        echo "You can now run the new test suites:"
        echo ""
        echo "  # Reverse context tests (SP owns, User executes)"
        echo "  python tests/advanced/test_reverse_context.py"
        echo ""
        echo "  # Negative security tests (validate blocks)"
        echo "  python tests/advanced/test_negative_context.py"
        echo ""
        echo "  # Run all tests"
        echo "  python run_tests.py"
        echo ""
    else
        echo ""
        echo "⚠️  Connection test failed. Please check the PAT and try again."
        echo ""
        echo "To retry, run: ./generate_sp_pat.sh"
        exit 1
    fi
else
    echo "❌ Error updating config.py"
    exit 1
fi
