#!/usr/bin/env python3
"""
Run Tests on Serverless Compute via Jobs API
Main script for executing SQL SECURITY DEFINER tests on serverless compute
"""

import sys
import argparse
import json
from datetime import datetime

from framework.serverless_config import (
    WORKSPACE_URL, PAT_TOKEN, SERVERLESS_CLUSTER_ID,
    SERVICE_PRINCIPAL_CLIENT_ID, SERVICE_PRINCIPAL_CLIENT_SECRET
)
from framework.jobs_api import DatabricksJobsAPI, run_tests_via_jobs_api


def parse_arguments():
    """Parse command-line arguments"""
    parser = argparse.ArgumentParser(
        description="Run SQL SECURITY DEFINER tests on Serverless Compute via Jobs API"
    )
    
    parser.add_argument(
        "--test-suite",
        choices=["all", "core", "advanced", "context", "negative"],
        default="all",
        help="Which test suite to run"
    )
    
    parser.add_argument(
        "--cluster-id",
        default=SERVERLESS_CLUSTER_ID,
        help="Serverless cluster ID to use"
    )
    
    parser.add_argument(
        "--no-wait",
        action="store_true",
        help="Don't wait for job completion"
    )
    
    parser.add_argument(
        "--list-jobs",
        action="store_true",
        help="List existing jobs and exit"
    )
    
    parser.add_argument(
        "--delete-job",
        type=int,
        metavar="JOB_ID",
        help="Delete a job by ID"
    )
    
    parser.add_argument(
        "--output",
        default="serverless_test_results.json",
        help="Output file for test results"
    )
    
    return parser.parse_args()


def list_existing_jobs():
    """List all existing test jobs"""
    print("=" * 80)
    print("📋 Existing Databricks Jobs")
    print("=" * 80)
    print()
    
    client = DatabricksJobsAPI(WORKSPACE_URL, PAT_TOKEN)
    
    try:
        response = client.list_jobs(limit=50)
        jobs = response.get("jobs", [])
        
        if not jobs:
            print("No jobs found.")
            return
        
        # Filter for our test jobs
        test_jobs = [j for j in jobs if "SQL_SECURITY_DEFINER" in j.get("settings", {}).get("name", "")]
        
        if test_jobs:
            print(f"Found {len(test_jobs)} SQL SECURITY DEFINER test jobs:")
            print()
            
            for job in test_jobs:
                settings = job.get("settings", {})
                job_id = job.get("job_id")
                name = settings.get("name", "Unknown")
                created_time = job.get("created_time", 0)
                created_date = datetime.fromtimestamp(created_time / 1000).strftime("%Y-%m-%d %H:%M:%S")
                
                print(f"  Job ID: {job_id}")
                print(f"  Name: {name}")
                print(f"  Created: {created_date}")
                print(f"  View: {WORKSPACE_URL}/#job/{job_id}")
                print()
        else:
            print(f"No SQL SECURITY DEFINER test jobs found (out of {len(jobs)} total jobs)")
    
    except Exception as e:
        print(f"❌ Error listing jobs: {e}")
        sys.exit(1)


def delete_job(job_id: int):
    """Delete a job by ID"""
    print(f"🗑️  Deleting job {job_id}...")
    
    client = DatabricksJobsAPI(WORKSPACE_URL, PAT_TOKEN)
    
    try:
        client.delete_job(job_id)
        print(f"✅ Job {job_id} deleted successfully")
    except Exception as e:
        print(f"❌ Error deleting job: {e}")
        sys.exit(1)


def main():
    """Main execution function"""
    args = parse_arguments()
    
    # Handle list-jobs command
    if args.list_jobs:
        list_existing_jobs()
        return
    
    # Handle delete-job command
    if args.delete_job:
        delete_job(args.delete_job)
        return
    
    # Run tests via Jobs API
    print("=" * 80)
    print("🚀 SQL SECURITY DEFINER Tests - Serverless Compute")
    print("=" * 80)
    print()
    print(f"📦 Test Suite: {args.test_suite}")
    print(f"🖥️  Cluster: {args.cluster_id}")
    print(f"🌐 Workspace: {WORKSPACE_URL}")
    print()
    
    try:
        results = run_tests_via_jobs_api(
            workspace_url=WORKSPACE_URL,
            token=PAT_TOKEN,
            cluster_id=args.cluster_id,
            test_suite=args.test_suite,
            wait_for_completion=not args.no_wait
        )
        
        # Save results
        print(f"💾 Saving results to {args.output}...")
        with open(args.output, 'w') as f:
            json.dump(results, f, indent=2)
        print(f"✅ Results saved")
        print()
        
        # Print summary
        if "result_state" in results:
            result_state = results["result_state"]
            
            print("=" * 80)
            print("📊 Test Summary")
            print("=" * 80)
            print(f"Job ID: {results.get('job_id')}")
            print(f"Run ID: {results.get('run_id')}")
            print(f"Result: {result_state}")
            print()
            
            if result_state == "SUCCESS":
                print("✅ All tests completed successfully!")
                sys.exit(0)
            else:
                print("❌ Some tests failed or job encountered errors")
                sys.exit(1)
        else:
            print("Job started successfully.")
            print(f"Check status at: {WORKSPACE_URL}/#job/{results.get('job_id')}")
            sys.exit(0)
    
    except Exception as e:
        print(f"❌ Error running tests: {e}")
        import traceback
        traceback.print_exc()
        sys.exit(1)


if __name__ == "__main__":
    main()
