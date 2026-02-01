#!/usr/bin/env python3
"""
Parallel Test Runner for SQL SECURITY DEFINER Tests
Runs multiple tests concurrently to speed up execution
"""

import sys
import os
from concurrent.futures import ThreadPoolExecutor, as_completed
from datetime import datetime
import time

# Add parent directory to path
sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from framework.test_framework import DatabricksConnection, TestExecutor, TestReporter
from framework.service_principal_auth import ServicePrincipalAuth
from framework.config import *

# Import all test suites
from tests.core.test_core_impersonation import get_tests as get_core_tests
from tests.access.test_object_access import get_tests as get_access_tests
from tests.nested.test_nested_procedures import get_tests as get_nested_tests
from tests.security.test_injection_safety import get_tests as get_security_tests
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


def run_test_with_connection(test_case):
    """Run a single test case with its own connection"""
    try:
        # Create a new connection for this test
        conn = DatabricksConnection(SERVER_HOSTNAME, HTTP_PATH, PAT_TOKEN, CATALOG, SCHEMA)
        if not conn.connect():
            return None
        
        # Run the test
        executor = TestExecutor(conn)
        result = executor.run_test(test_case)
        
        # Close connection
        conn.close()
        
        return result
    except Exception as e:
        print(f"❌ Error running {test_case.test_id}: {e}")
        return None


def main():
    print("=" * 80)
    print("🚀 PARALLEL SQL SECURITY DEFINER TEST SUITE")
    print("=" * 80)
    print(f"⚡ Running tests with up to 10 parallel workers")
    print()
    
    start_time = time.time()
    
    # Collect all tests
    all_test_suites = [
        ("Core Impersonation (TC-01 to TC-03)", get_core_tests()),
        ("Object Access (TC-04 to TC-06)", get_access_tests()),
        ("Nested Procedures (TC-07 to TC-08)", get_nested_tests()),
        ("Security & Injection (TC-09 to TC-10)", get_security_tests()),
        ("Error Handling (TC-11 to TC-12)", get_observability_tests()),
        ("Unity Catalog (TC-13 to TC-16)", get_unity_tests()),
        ("Negative Cases (TC-17 to TC-19)", get_negative_tests()),
        ("Compliance (TC-20)", get_compliance_tests()),
        ("Known Issues (KI-01 to KI-05)", get_known_issues_tests()),
        ("Advanced: Bug Discovery", get_bug_discovery_tests()),
        ("Advanced: Concurrency", get_concurrency_tests()),
        ("Advanced: SQL Injection", get_sql_injection_tests()),
        ("Advanced: Privilege Escalation", get_privilege_escalation_tests()),
        ("Advanced: Unity Catalog Deep Dive", get_uc_advanced_tests()),
        ("Jobs API: Context Switching", get_jobs_context_tests()),
        ("Jobs API: Complete Suite", get_jobs_complete_tests()),
    ]
    
    # Flatten all tests
    all_tests = []
    for suite_name, tests in all_test_suites:
        all_tests.extend(tests)
    
    print(f"📊 Total tests to run: {len(all_tests)}")
    print()
    
    # Run tests in parallel with ThreadPoolExecutor
    results = []
    completed = 0
    
    with ThreadPoolExecutor(max_workers=10) as executor:
        # Submit all tests
        future_to_test = {executor.submit(run_test_with_connection, test): test for test in all_tests}
        
        # Process completed tests as they finish
        for future in as_completed(future_to_test):
            test = future_to_test[future]
            completed += 1
            
            try:
                result = future.result()
                if result:
                    results.append(result)
                    status_icon = "✅" if result.status == "PASS" else "❌" if result.status == "FAIL" else "💥"
                    print(f"[{completed}/{len(all_tests)}] {status_icon} {test.test_id}: {result.status}")
            except Exception as e:
                print(f"[{completed}/{len(all_tests)}] 💥 {test.test_id}: ERROR - {e}")
    
    end_time = time.time()
    duration = end_time - start_time
    
    # Generate report
    print()
    print("=" * 80)
    print("📊 FINAL TEST SUMMARY")
    print("=" * 80)
    
    reporter = TestReporter(results)
    reporter.print_summary()
    
    # Show timing comparison
    print()
    print("⚡ Performance:")
    print(f"   Parallel execution time: {duration:.2f}s")
    print(f"   Estimated sequential time: ~1200s (20 minutes)")
    print(f"   Time saved: ~{1200 - duration:.0f}s ({((1200 - duration) / 1200 * 100):.0f}% faster)")
    
    # Generate JSON report
    report_path = "logs/test_results_parallel.json"
    reporter.generate_json_report(report_path)
    print()
    print(f"📄 JSON report generated: {report_path}")
    
    print()
    print("=" * 80)
    print("✅ Parallel test execution complete!")
    print("=" * 80)


if __name__ == "__main__":
    main()
