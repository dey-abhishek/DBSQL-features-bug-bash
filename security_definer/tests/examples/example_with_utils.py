"""
Example: Using utility functions for cleaner test code
This shows how to use framework/utils.py for fully qualified names
"""

import sys
import os
sys.path.insert(0, os.path.join(os.path.dirname(__file__), '../..'))

from framework.test_framework import DefinerTestCase
from framework.config import SERVICE_PRINCIPAL_B
from framework.utils import fqn, drop_if_exists, grant_execute, call_procedure


def get_example_tests_with_utils():
    """Example tests using utility functions"""
    
    tests = []
    
    # Example 1: Simple procedure test with utils
    tc_example1 = DefinerTestCase(
        test_id="EXAMPLE-01",
        description="Example using utility functions",
        setup_sql=[
            # Clean way to drop objects
            drop_if_exists("example_proc", "PROCEDURE"),
            
            # Use fqn() for fully qualified names
            f"""
            CREATE PROCEDURE {fqn('example_proc')}()
            LANGUAGE SQL
            AS BEGIN
                SELECT 
                    current_catalog() as catalog,
                    current_schema() as schema,
                    'Example test' as description;
            END
            """,
            
            # Grant with utility
            grant_execute("example_proc", SERVICE_PRINCIPAL_B)
        ],
        # Call with utility
        test_sql=call_procedure("example_proc"),
        teardown_sql=[
            drop_if_exists("example_proc", "PROCEDURE")
        ]
    )
    tests.append(tc_example1)
    
    # Example 2: Table and procedure with utils
    tc_example2 = DefinerTestCase(
        test_id="EXAMPLE-02",
        description="Example with table and procedure using utils",
        setup_sql=[
            # Drop objects
            drop_if_exists("example_table", "TABLE"),
            drop_if_exists("example_read_proc", "PROCEDURE"),
            
            # Create table with fqn
            f"CREATE TABLE {fqn('example_table')} (id INT, name STRING)",
            f"INSERT INTO {fqn('example_table')} VALUES (1, 'test'), (2, 'data')",
            
            # Create procedure referencing table with fqn
            f"""
            CREATE PROCEDURE {fqn('example_read_proc')}()
            LANGUAGE SQL
            AS BEGIN
                SELECT COUNT(*) as row_count 
                FROM {fqn('example_table')};
            END
            """,
            
            # Grant execute
            grant_execute("example_read_proc", SERVICE_PRINCIPAL_B)
        ],
        test_sql=call_procedure("example_read_proc"),
        teardown_sql=[
            drop_if_exists("example_read_proc", "PROCEDURE"),
            drop_if_exists("example_table", "TABLE")
        ],
        expected_result={"value": 2}
    )
    tests.append(tc_example2)
    
    return tests


if __name__ == "__main__":
    from framework.test_framework import DatabricksConnection, TestExecutor, TestReporter
    from framework.config import SERVER_HOSTNAME, HTTP_PATH, PAT_TOKEN, CATALOG, SCHEMA
    
    print("="*80)
    print("Example: Using Utility Functions")
    print("="*80)
    print()
    print("This demonstrates how to use framework/utils.py for:")
    print("  • fqn() - Generate fully qualified names")
    print("  • drop_if_exists() - Clean DROP statements")
    print("  • grant_execute() - GRANT statements")
    print("  • call_procedure() - CALL statements")
    print("  • revoke_all() - REVOKE statements")
    print()
    
    conn = DatabricksConnection(SERVER_HOSTNAME, HTTP_PATH, PAT_TOKEN, CATALOG, SCHEMA)
    executor = TestExecutor(conn)
    
    tests = get_example_tests_with_utils()
    results = executor.run_test_suite(tests)
    
    reporter = TestReporter(results)
    reporter.print_summary()
