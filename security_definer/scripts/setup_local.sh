#!/bin/bash
# Quick setup script for running tests locally

echo "=============================================="
echo "SQL SECURITY DEFINER - Local Setup"
echo "=============================================="
echo ""

# Check Python installation
echo "1️⃣  Checking Python..."
if command -v python3 &> /dev/null; then
    PYTHON_VERSION=$(python3 --version)
    echo "   ✅ $PYTHON_VERSION found"
else
    echo "   ❌ Python 3 not found"
    echo "   Please install Python 3.8 or higher"
    exit 1
fi

# Check if virtual environment exists
if [ -d "venv" ]; then
    echo ""
    echo "2️⃣  Virtual environment already exists"
else
    echo ""
    echo "2️⃣  Creating virtual environment..."
    python3 -m venv venv
    echo "   ✅ Virtual environment created"
fi

# Activate virtual environment
echo ""
echo "3️⃣  Activating virtual environment..."
source venv/bin/activate
echo "   ✅ Virtual environment activated"

# Install dependencies
echo ""
echo "4️⃣  Installing dependencies..."
pip install -q databricks-sql-connector pandas jupyter ipykernel
echo "   ✅ Dependencies installed"

# Test connectivity
echo ""
echo "5️⃣  Testing Databricks connectivity..."
python3 tests/utils/test_connection.py

# Check exit code
if [ $? -eq 0 ]; then
    echo ""
    echo "=============================================="
    echo "✅ Setup Complete!"
    echo "=============================================="
    echo ""
    echo "🚀 To run the tests:"
    echo ""
    echo "   Option 1: Jupyter Notebook (Recommended)"
    echo "   -----------------------------------------"
    echo "   source venv/bin/activate"
    echo "   jupyter notebook SP_Security_Definer_BB.ipynb"
    echo "   (Skip Cell 0 and run all other cells)"
    echo ""
    echo "   Option 2: VS Code"
    echo "   -----------------"
    echo "   Open SP_Security_Definer_BB.ipynb in VS Code"
    echo "   Select Python kernel: venv/bin/python"
    echo "   Skip Cell 0 and run all cells"
    echo ""
   echo "   Option 3: Command Line"
   echo "   ----------------------"
   echo "   source venv/bin/activate"
   echo "   python3 tests/utils/test_connection.py  # Quick test"
   echo ""
else
    echo ""
    echo "=============================================="
    echo "⚠️  Setup completed but connectivity test failed"
    echo "=============================================="
    echo ""
    echo "Please check:"
    echo "  • Internet connection"
    echo "  • PAT token validity"
    echo "  • Warehouse is running"
    echo "  • VPN if required"
fi
