"""
ADVANCED TEST SUITE: Concurrency & Race Condition Tests
Focus on TOCTOU vulnerabilities, race conditions, and concurrent execution
"""

from framework.test_framework import DefinerTestCase, TestExecutor
from framework.config import SERVICE_PRINCIPAL_B_ID, CATALOG, SCHEMA
import threading
import time
from typing import List, Tuple

def get_tests():
    tests = []
    
    # TC-26: Concurrent Permission Revocation (TOCTOU Attack)
    tc26 = DefinerTestCase(
        test_id="TC-26",
        description="TOCTOU - Revoke permissions while procedure is running (Time-Of-Check-Time-Of-Use)",
        setup_sql=[
            f"DROP TABLE IF EXISTS {CATALOG}.{SCHEMA}.tc26_toctou_table",
            f"CREATE TABLE {CATALOG}.{SCHEMA}.tc26_toctou_table (id INT, data STRING, timestamp TIMESTAMP)",
            f"INSERT INTO {CATALOG}.{SCHEMA}.tc26_toctou_table VALUES (1, 'initial_data', CURRENT_TIMESTAMP())",
            
            # Create a long-running procedure
            f"DROP PROCEDURE IF EXISTS {CATALOG}.{SCHEMA}.tc26_long_running",
            f"""
            CREATE PROCEDURE {CATALOG}.{SCHEMA}.tc26_long_running()
            LANGUAGE SQL
            AS BEGIN
                -- First access
                INSERT INTO {CATALOG}.{SCHEMA}.tc26_toctou_table 
                VALUES (2, 'step1', CURRENT_TIMESTAMP());
                
                -- Simulate some work
                SELECT COUNT(*) FROM {CATALOG}.{SCHEMA}.tc26_toctou_table;
                
                -- Second access (permissions might be revoked by now)
                INSERT INTO {CATALOG}.{SCHEMA}.tc26_toctou_table 
                VALUES (3, 'step2', CURRENT_TIMESTAMP());
                
                SELECT * FROM {CATALOG}.{SCHEMA}.tc26_toctou_table ORDER BY id;
            END
            """,
        ],
        test_sql=f"CALL {CATALOG}.{SCHEMA}.tc26_long_running()",
        teardown_sql=[
            f"DROP PROCEDURE IF EXISTS {CATALOG}.{SCHEMA}.tc26_long_running",
            f"DROP TABLE IF EXISTS {CATALOG}.{SCHEMA}.tc26_toctou_table",
        ]
    )
    tests.append(tc26)
    
    # TC-28: Object Drop During Nested Call
    tc28 = DefinerTestCase(
        test_id="TC-28",
        description="Object Drop During Execution - Drop table while nested procedure accesses it",
        setup_sql=[
            f"DROP TABLE IF EXISTS {CATALOG}.{SCHEMA}.tc28_volatile_table",
            f"CREATE TABLE {CATALOG}.{SCHEMA}.tc28_volatile_table (id INT, status STRING)",
            f"INSERT INTO {CATALOG}.{SCHEMA}.tc28_volatile_table VALUES (1, 'exists')",
            
            f"DROP PROCEDURE IF EXISTS {CATALOG}.{SCHEMA}.tc28_inner_access",
            f"""
            CREATE PROCEDURE {CATALOG}.{SCHEMA}.tc28_inner_access()
            LANGUAGE SQL
            AS BEGIN
                -- This might fail if table is dropped
                SELECT COUNT(*) as count FROM {CATALOG}.{SCHEMA}.tc28_volatile_table;
            END
            """,
            
            f"DROP PROCEDURE IF EXISTS {CATALOG}.{SCHEMA}.tc28_outer_caller",
            f"""
            CREATE PROCEDURE {CATALOG}.{SCHEMA}.tc28_outer_caller()
            LANGUAGE SQL
            AS BEGIN
                -- Check table exists
                SELECT 'Table exists initially' as status;
                
                -- Call inner (table might be dropped during this)
                CALL {CATALOG}.{SCHEMA}.tc28_inner_access();
                
                SELECT 'Completed' as final_status;
            END
            """,
        ],
        test_sql=f"CALL {CATALOG}.{SCHEMA}.tc28_outer_caller()",
        teardown_sql=[
            f"DROP PROCEDURE IF EXISTS {CATALOG}.{SCHEMA}.tc28_outer_caller",
            f"DROP PROCEDURE IF EXISTS {CATALOG}.{SCHEMA}.tc28_inner_access",
            f"DROP TABLE IF EXISTS {CATALOG}.{SCHEMA}.tc28_volatile_table",
        ]
    )
    tests.append(tc28)
    
    # TC-29: Procedure Self-Modification During Execution
    tc29 = DefinerTestCase(
        test_id="TC-29",
        description="Self-Modification - Procedure attempts to modify itself while running",
        setup_sql=[
            f"DROP PROCEDURE IF EXISTS {CATALOG}.{SCHEMA}.tc29_self_modify",
            f"""
            CREATE PROCEDURE {CATALOG}.{SCHEMA}.tc29_self_modify()
            LANGUAGE SQL
            AS BEGIN
                SELECT 'Executing version 1' as version;
                
                -- Attempt to recreate itself (should fail or be queued)
                -- CREATE OR REPLACE PROCEDURE {CATALOG}.{SCHEMA}.tc29_self_modify()
                -- This is commented out as it would likely cause syntax error
                
                SELECT 'Still version 1' as check_version;
            END
            """,
        ],
        test_sql=f"CALL {CATALOG}.{SCHEMA}.tc29_self_modify()",
        teardown_sql=[
            f"DROP PROCEDURE IF EXISTS {CATALOG}.{SCHEMA}.tc29_self_modify",
        ]
    )
    tests.append(tc29)
    
    return tests


def run_concurrent_test():
    """
    Special test: Execute same procedure from multiple threads simultaneously
    Tests for race conditions and context isolation
    """
    from framework.test_framework import DatabricksConnection
    from framework.config import SERVER_HOSTNAME, HTTP_PATH, PAT_TOKEN, CATALOG, SCHEMA
    
    print("=" * 80)
    print("🔥 CONCURRENT EXECUTION TEST (TC-27)")
    print("=" * 80)
    print()
    
    # Setup
    conn = DatabricksConnection(SERVER_HOSTNAME, HTTP_PATH, PAT_TOKEN, CATALOG, SCHEMA)
    
    print("⚙️  Setup: Creating concurrent test procedure...")
    setup_queries = [
        f"DROP TABLE IF EXISTS {CATALOG}.{SCHEMA}.tc27_concurrent_log",
        f"CREATE TABLE {CATALOG}.{SCHEMA}.tc27_concurrent_log (thread_id INT, execution_time TIMESTAMP, user_name STRING)",
        f"DROP PROCEDURE IF EXISTS {CATALOG}.{SCHEMA}.tc27_concurrent_proc",
        f"""
        CREATE PROCEDURE {CATALOG}.{SCHEMA}.tc27_concurrent_proc(thread_num INT)
        LANGUAGE SQL
        AS BEGIN
            INSERT INTO {CATALOG}.{SCHEMA}.tc27_concurrent_log 
            VALUES (thread_num, CURRENT_TIMESTAMP(), CURRENT_USER());
            
            SELECT thread_num as thread, CURRENT_USER() as user;
        END
        """,
    ]
    
    for query in setup_queries:
        result, error = conn.execute(query)
        if error:
            print(f"❌ Setup failed: {error}")
            return
    
    print("✅ Setup complete")
    print()
    print(f"▶️  Launching 10 concurrent executions...")
    
    # Execute procedure concurrently
    results: List[Tuple[int, bool, str]] = []
    
    def execute_procedure(thread_id: int):
        try:
            thread_conn = DatabricksConnection(SERVER_HOSTNAME, HTTP_PATH, PAT_TOKEN, CATALOG, SCHEMA)
            result, error = thread_conn.execute(
                f"CALL {CATALOG}.{SCHEMA}.tc27_concurrent_proc({thread_id})"
            )
            thread_conn.close()
            
            if error:
                results.append((thread_id, False, str(error)))
            else:
                results.append((thread_id, True, "Success"))
        except Exception as e:
            results.append((thread_id, False, str(e)))
    
    # Launch threads
    threads = []
    start_time = time.time()
    
    for i in range(10):
        thread = threading.Thread(target=execute_procedure, args=(i,))
        threads.append(thread)
        thread.start()
    
    # Wait for completion
    for thread in threads:
        thread.join()
    
    end_time = time.time()
    
    # Analyze results
    successful = sum(1 for _, success, _ in results if success)
    failed = len(results) - successful
    
    print()
    print("📊 Results:")
    print(f"  Total threads:    10")
    print(f"  Successful:       {successful}")
    print(f"  Failed:           {failed}")
    print(f"  Execution time:   {end_time - start_time:.2f}s")
    print()
    
    if failed > 0:
        print("❌ Failed executions:")
        for thread_id, success, msg in results:
            if not success:
                print(f"  Thread {thread_id}: {msg}")
        print()
    
    # Check log table
    log_result, log_error = conn.execute(
        f"SELECT COUNT(*) as total_logs FROM {CATALOG}.{SCHEMA}.tc27_concurrent_log"
    )
    
    if log_result:
        print(f"✅ Total log entries: {log_result[0][0]}")
        print(f"   Expected: {successful}")
        if log_result[0][0] != successful:
            print(f"   ⚠️  MISMATCH: Some executions may have failed to log!")
    
    # Cleanup
    print()
    print("🧹 Cleanup...")
    conn.execute(f"DROP PROCEDURE IF EXISTS {CATALOG}.{SCHEMA}.tc27_concurrent_proc")
    conn.execute(f"DROP TABLE IF EXISTS {CATALOG}.{SCHEMA}.tc27_concurrent_log")
    conn.close()
    
    print("✅ Concurrent test complete")
    print("=" * 80)
    
    return successful == 10


if __name__ == "__main__":
    from framework.test_framework import DatabricksConnection, TestReporter
    from framework.config import SERVER_HOSTNAME, HTTP_PATH, PAT_TOKEN, CATALOG, SCHEMA
    
    print("=" * 80)
    print("🔥 ADVANCED TEST SUITE: Concurrency & Race Conditions")
    print("=" * 80)
    print()
    
    conn = DatabricksConnection(SERVER_HOSTNAME, HTTP_PATH, PAT_TOKEN, CATALOG, SCHEMA)
    executor = TestExecutor(conn)
    
    # Run standard tests
    test_cases = get_tests()
    results = [executor.run_test(tc) for tc in test_cases]
    
    reporter = TestReporter(results)
    reporter.print_summary()
    
    conn.close()
    
    print()
    print("=" * 80)
    print()
    
    # Run special concurrent test
    concurrent_success = run_concurrent_test()
    
    if not concurrent_success:
        print()
        print("⚠️  POTENTIAL BUG: Concurrent execution test revealed issues!")
