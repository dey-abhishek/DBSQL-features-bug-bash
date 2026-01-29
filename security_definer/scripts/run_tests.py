#!/usr/bin/env python3
"""
Main test runner for SQL SECURITY DEFINER Bug Bash
Orchestrates all test suites and generates comprehensive reports
"""

import sys
import os

# Add parent directory to path
sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))

from framework.test_framework import DatabricksConnection, TestExecutor, TestReporter
from framework.config import SERVER_HOSTNAME, HTTP_PATH, PAT_TOKEN, CATALOG, SCHEMA

# Import test modules
from tests.core.test_core_impersonation import get_tests as get_core_tests
from tests.access.test_object_access import get_tests as get_access_tests
from tests.nested.test_nested_procedures import get_tests as get_nested_tests
from tests.security.test_injection_safety import get_tests as get_injection_tests
from tests.observability.test_error_handling import get_tests as get_observability_tests
from tests.unity.test_unity_catalog import get_tests as get_unity_tests
from tests.negative.test_abuse_cases import get_tests as get_negative_tests
from tests.compliance.test_regression import get_tests as get_compliance_tests
from tests.known_issues.test_known_issues import get_tests as get_known_issues_tests
# Advanced test suites
from tests.advanced.test_bug_discovery import get_tests as get_bug_discovery_tests
from tests.advanced.test_concurrency import get_tests as get_concurrency_tests
from tests.advanced.test_sql_injection import get_tests as get_sql_injection_tests
from tests.advanced.test_privilege_escalation import get_tests as get_privilege_escalation_tests
from tests.advanced.test_unity_catalog import get_tests as get_uc_advanced_tests
from tests.advanced.test_jobs_context import get_jobs_context_tests
from tests.advanced.test_jobs_complete import get_jobs_complete_tests


def main():
    """Main test execution"""
    
    print("="*80)
    print("🔐 SQL SECURITY DEFINER - Bug Bash Test Suite")
    print("="*80)
    print()
    
    # Initialize connection
    print("🔗 Connecting to Databricks...")
    conn = DatabricksConnection(SERVER_HOSTNAME, HTTP_PATH, PAT_TOKEN, CATALOG, SCHEMA)
    
    # Test connectivity
    result, error = conn.execute("SELECT current_user(), current_catalog(), current_schema()")
    if error:
        print(f"❌ Connection failed: {error}")
        return 1
    
    print(f"✅ Connected as: {result[0][0]}")
    print(f"   Catalog: {result[0][1]}")
    print(f"   Schema: {result[0][2]}")
    print()
    
    # Initialize executor
    executor = TestExecutor(conn)
    all_results = []
    
    # Run test suites
    test_suites = [
        ("Core Impersonation (TC-01 to TC-03)", get_core_tests()),
        ("Object Access Boundaries (TC-04 to TC-06)", get_access_tests()),
        ("Nested & Chained Procedures (TC-07 to TC-08)", get_nested_tests()),
        ("Parameter & Injection Safety (TC-09 to TC-10)", get_injection_tests()),
        ("Error Handling & Observability (TC-11 to TC-12)", get_observability_tests()),
        ("Databricks-Specific / Unity Catalog (TC-13 to TC-16)", get_unity_tests()),
        ("Negative / Abuse Cases (TC-17 to TC-19)", get_negative_tests()),
        ("Compliance & Regression (TC-20)", get_compliance_tests()),
        ("Known Issues (KI-01 to KI-05)", get_known_issues_tests()),
        # Advanced test suites
        ("🔥 Advanced: Bug Discovery (TC-21, TC-22, TC-24, TC-31, TC-51, TC-55, TC-56, TC-58)", get_bug_discovery_tests()),
        ("🔥 Advanced: Concurrency & TOCTOU (TC-26, TC-28, TC-29)", get_concurrency_tests()),
        ("🔥 Advanced: SQL Injection (TC-76 to TC-80)", get_sql_injection_tests()),
        ("🔥 Advanced: Privilege Escalation (TC-81 to TC-86)", get_privilege_escalation_tests()),
        ("🔥 Advanced: Unity Catalog Deep Dive (TC-93 to TC-99)", get_uc_advanced_tests()),
        ("🚀 Jobs API: Context Switching (TC-JOBS-01 to 06)", get_jobs_context_tests()),
        ("🚀 Jobs API: Complete Suite (TC-JOBS-CORE/NESTED/SEC/UC/CTX)", get_jobs_complete_tests()),
    ]
    
    for suite_name, tests in test_suites:
        print(f"\n{'='*80}")
        print(f"📦 Running {suite_name}")
        print(f"{'='*80}")
        
        results = executor.run_test_suite(tests)
        all_results.extend(results)
        
        # Print suite summary
        passed = sum(1 for r in results if r.status == "PASS")
        total = len(results)
        print(f"\n✅ Suite completed: {passed}/{total} tests passed")
    
    # Generate final report
    print("\n" + "="*80)
    print("📊 FINAL TEST SUMMARY")
    print("="*80)
    
    reporter = TestReporter(all_results)
    reporter.print_summary()
    
    # Generate JSON report
    report_path = "test_results.json"
    reporter.generate_json_report(report_path)
    
    print("\n" + "="*80)
    print("✅ Test execution complete!")
    print("="*80)
    
    # Close connection
    conn.close()
    
    # Return exit code based on results
    failed = sum(1 for r in all_results if r.status in ["FAIL", "ERROR"])
    return 1 if failed > 0 else 0


if __name__ == "__main__":
    sys.exit(main())
