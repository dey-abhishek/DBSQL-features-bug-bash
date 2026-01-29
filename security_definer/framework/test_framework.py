"""
Core framework for SQL SECURITY DEFINER testing
Provides base classes for test execution, reporting, and database connectivity
"""

import databricks.sql as dbsql
from typing import List, Dict, Any, Optional, Tuple
from dataclasses import dataclass, field
from datetime import datetime
import traceback
import time


class DatabricksConnection:
    """Manages Databricks SQL connections"""
    
    def __init__(self, server_hostname: str, http_path: str, access_token: str, 
                 catalog: str, schema: str):
        self.server_hostname = server_hostname
        self.http_path = http_path
        self.access_token = access_token
        self.catalog = catalog
        self.schema = schema
        self._connection = None
    
    def connect(self):
        """Establish connection to Databricks"""
        if self._connection is None:
            self._connection = dbsql.connect(
                server_hostname=self.server_hostname,
                http_path=self.http_path,
                access_token=self.access_token
            )
            with self._connection.cursor() as cursor:
                cursor.execute(f"USE CATALOG {self.catalog}")
                cursor.execute(f"USE SCHEMA {self.schema}")
        return self._connection
    
    def execute(self, sql: str, fetch: bool = True) -> Tuple[Optional[List[Any]], Optional[str]]:
        """Execute SQL and return results or error"""
        try:
            conn = self.connect()
            with conn.cursor() as cursor:
                cursor.execute(sql)
                if fetch:
                    try:
                        results = cursor.fetchall()
                        return results, None
                    except Exception:
                        return None, None
                return None, None
        except Exception as e:
            return None, str(e)
    
    def close(self):
        """Close the connection"""
        if self._connection:
            self._connection.close()
            self._connection = None


@dataclass
class TestResult:
    """Represents the result of a single test execution"""
    test_id: str
    description: str
    status: str  # 'PASS', 'FAIL', 'SKIP', 'ERROR'
    execution_time: float
    expected: Optional[str] = None
    actual: Optional[str] = None
    error_message: Optional[str] = None
    details: Dict[str, Any] = field(default_factory=dict)
    timestamp: str = field(default_factory=lambda: datetime.now().isoformat())


@dataclass
class DefinerTestCase:
    """Base class for test cases with setup/teardown"""
    test_id: str
    description: str
    setup_sql: List[str] = field(default_factory=list)
    test_sql: str = ""
    teardown_sql: List[str] = field(default_factory=list)
    expected_result: Optional[Dict[str, Any]] = None
    execute_as: str = "owner"
    should_fail: bool = False
    skip_reason: Optional[str] = None
    
    def __post_init__(self):
        if not self.test_id or not self.description:
            raise ValueError("test_id and description are required")


class TestExecutor:
    """Handles test execution with different user contexts"""
    
    def __init__(self, connection: DatabricksConnection):
        self.connection = connection
        self.results: List[TestResult] = []
    
    def execute_sql(self, sql: str, as_user: str = "owner") -> Tuple[Optional[List[Any]], Optional[str]]:
        """Execute SQL as specified user"""
        return self.connection.execute(sql)
    
    def run_test(self, test_case: DefinerTestCase) -> TestResult:
        """Execute a single test case"""
        start_time = time.time()
        
        if test_case.skip_reason:
            return TestResult(
                test_id=test_case.test_id,
                description=test_case.description,
                status="SKIP",
                execution_time=0.0,
                details={"skip_reason": test_case.skip_reason}
            )
        
        try:
            print(f"\n{'='*80}")
            print(f"🧪 Running {test_case.test_id}: {test_case.description}")
            print(f"{'='*80}")
            
            # Setup phase
            for sql in test_case.setup_sql:
                print(f"⚙️  Setup: {sql[:100]}...")
                result, error = self.execute_sql(sql)
                if error:
                    raise Exception(f"Setup failed: {error}")
            
            # Execution phase
            print(f"▶️  Executing test SQL...")
            result, error = self.execute_sql(test_case.test_sql, as_user=test_case.execute_as)
            
            execution_time = time.time() - start_time
            
            # Evaluate result
            if test_case.should_fail:
                if error:
                    print(f"✅ Test passed (expected failure occurred)")
                    return TestResult(
                        test_id=test_case.test_id,
                        description=test_case.description,
                        status="PASS",
                        execution_time=execution_time,
                        expected="Error expected",
                        actual=f"Error: {error}",
                        details={"error": error}
                    )
                else:
                    print(f"❌ Test failed (expected failure but succeeded)")
                    return TestResult(
                        test_id=test_case.test_id,
                        description=test_case.description,
                        status="FAIL",
                        execution_time=execution_time,
                        expected="Error expected",
                        actual=f"Success: {result}",
                        error_message="Expected failure but query succeeded"
                    )
            else:
                if error:
                    print(f"❌ Test failed: {error}")
                    return TestResult(
                        test_id=test_case.test_id,
                        description=test_case.description,
                        status="FAIL",
                        execution_time=execution_time,
                        expected="Success",
                        actual="Error",
                        error_message=error
                    )
                else:
                    if test_case.expected_result:
                        actual_result = self._format_result(result)
                        if self._compare_results(actual_result, test_case.expected_result):
                            print(f"✅ Test passed")
                            return TestResult(
                                test_id=test_case.test_id,
                                description=test_case.description,
                                status="PASS",
                                execution_time=execution_time,
                                expected=str(test_case.expected_result),
                                actual=str(actual_result)
                            )
                        else:
                            print(f"❌ Test failed (result mismatch)")
                            return TestResult(
                                test_id=test_case.test_id,
                                description=test_case.description,
                                status="FAIL",
                                execution_time=execution_time,
                                expected=str(test_case.expected_result),
                                actual=str(actual_result),
                                error_message="Result does not match expected"
                            )
                    else:
                        print(f"✅ Test passed")
                        return TestResult(
                            test_id=test_case.test_id,
                            description=test_case.description,
                            status="PASS",
                            execution_time=execution_time,
                            actual=str(result)[:200] if result else "No results"
                        )
        
        except Exception as e:
            execution_time = time.time() - start_time
            print(f"❌ Test error: {str(e)}")
            return TestResult(
                test_id=test_case.test_id,
                description=test_case.description,
                status="ERROR",
                execution_time=execution_time,
                error_message=str(e),
                details={"traceback": traceback.format_exc()}
            )
        
        finally:
            # Teardown phase (always runs)
            for sql in test_case.teardown_sql:
                try:
                    print(f"🧹 Cleanup: {sql[:100]}...")
                    self.execute_sql(sql)
                except Exception as e:
                    print(f"⚠️  Cleanup warning: {str(e)}")
    
    def _format_result(self, result: Optional[List[Any]]) -> Any:
        """Format query result for comparison"""
        if result is None:
            return None
        if len(result) == 1 and len(result[0]) == 1:
            return result[0][0]
        return result
    
    def _compare_results(self, actual: Any, expected: Dict[str, Any]) -> bool:
        """Compare actual result with expected"""
        if "value" in expected:
            return actual == expected["value"]
        if "contains" in expected:
            return expected["contains"] in str(actual)
        if "pattern" in expected:
            import re
            return bool(re.search(expected["pattern"], str(actual)))
        return True
    
    def run_test_suite(self, test_cases: List[DefinerTestCase]) -> List[TestResult]:
        """Run multiple test cases"""
        self.results = []
        for test_case in test_cases:
            result = self.run_test(test_case)
            self.results.append(result)
        return self.results


class TestReporter:
    """Generates test reports"""
    
    def __init__(self, results: List[TestResult]):
        self.results = results
    
    def print_summary(self):
        """Print test summary to console"""
        total = len(self.results)
        passed = sum(1 for r in self.results if r.status == "PASS")
        failed = sum(1 for r in self.results if r.status == "FAIL")
        skipped = sum(1 for r in self.results if r.status == "SKIP")
        errors = sum(1 for r in self.results if r.status == "ERROR")
        
        print(f"\n{'='*80}")
        print(f"📊 TEST SUMMARY")
        print(f"{'='*80}")
        print(f"Total Tests:   {total}")
        if total > 0:
            print(f"✅ Passed:     {passed} ({passed/total*100:.1f}%)")
            print(f"❌ Failed:     {failed} ({failed/total*100:.1f}%)")
        else:
            print(f"✅ Passed:     0")
            print(f"❌ Failed:     0")
        print(f"⏭️  Skipped:    {skipped}")
        print(f"💥 Errors:     {errors}")
        
        total_time = sum(r.execution_time for r in self.results)
        print(f"\nTotal Execution Time: {total_time:.2f}s")
        print(f"{'='*80}\n")
        
        if failed > 0 or errors > 0:
            print("❌ Failed/Error Tests:")
            for result in self.results:
                if result.status in ["FAIL", "ERROR"]:
                    print(f"  • {result.test_id}: {result.description}")
                    if result.error_message:
                        print(f"    Error: {result.error_message[:200]}")
    
    def generate_json_report(self, output_path: str):
        """Generate JSON report"""
        import json
        
        report_data = {
            "timestamp": datetime.now().isoformat(),
            "summary": {
                "total": len(self.results),
                "passed": sum(1 for r in self.results if r.status == "PASS"),
                "failed": sum(1 for r in self.results if r.status == "FAIL"),
                "skipped": sum(1 for r in self.results if r.status == "SKIP"),
                "errors": sum(1 for r in self.results if r.status == "ERROR")
            },
            "tests": [
                {
                    "test_id": r.test_id,
                    "description": r.description,
                    "status": r.status,
                    "execution_time": r.execution_time,
                    "error_message": r.error_message,
                    "timestamp": r.timestamp
                }
                for r in self.results
            ]
        }
        
        with open(output_path, 'w') as f:
            json.dump(report_data, f, indent=2)
        
        print(f"📄 JSON report generated: {output_path}")
        return output_path
