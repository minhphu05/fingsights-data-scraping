#!/usr/bin/env python3
"""Submit scraping jobs to Dagster Webserver via GraphQL API.

Triggers transient K3s pods via Dagster's K8sRunLauncher.
Can be executed locally or from GitHub Actions pipelines.
"""

from __future__ import annotations

import argparse
import json
import os
import sys
import time
import urllib.error
import urllib.request
from typing import Any, Dict, List, Optional, Tuple

SOURCE_CONFIGS: Dict[str, Dict[str, Any]] = {
    "cafef": {
        "job_name": "cafef_news_job",
        "op_name": "collect_cafef_news",
        "config_builder": lambda start, end, mode: {"start_date": start, "end_date": end, "storage_mode": mode},
    },
    "vietstock": {
        "job_name": "vietstock_market_job",
        "op_name": "collect_vietstock_market",
        "config_builder": lambda start, end, mode: {"start_date": start, "end_date": end},
    },
    "thanhnien": {
        "job_name": "thanhnien_news_job",
        "op_name": "collect_configured_news",
        "config_builder": lambda start, end, mode: {"source_key": "thanhnien", "start_date": start, "end_date": end, "storage_mode": mode},
    },
    "tuoitre": {
        "job_name": "tuoitre_news_job",
        "op_name": "collect_configured_news",
        "config_builder": lambda start, end, mode: {"source_key": "tuoitre", "start_date": start, "end_date": end, "storage_mode": mode},
    },
    "baomoi": {
        "job_name": "baomoi_news_job",
        "op_name": "collect_configured_news",
        "config_builder": lambda start, end, mode: {"source_key": "baomoi", "start_date": start, "end_date": end, "storage_mode": mode},
    },
    "vnexpress": {
        "job_name": "vnexpress_news_job",
        "op_name": "collect_configured_news",
        "config_builder": lambda start, end, mode: {"source_key": "vnexpress", "start_date": start, "end_date": end, "storage_mode": mode},
    },
}

ALL_SOURCES = list(SOURCE_CONFIGS.keys())


def execute_graphql(endpoint_url: str, query: str, variables: Optional[Dict[str, Any]] = None) -> Dict[str, Any]:
    """Send GraphQL query/mutation to Dagster webserver."""
    payload = json.dumps({"query": query, "variables": variables or {}}).encode("utf-8")
    req = urllib.request.Request(
        url=endpoint_url.rstrip("/") + "/graphql",
        data=payload,
        headers={"Content-Type": "application/json", "User-Agent": "Finsights-Job-Submitter/1.0"},
        method="POST",
    )
    try:
        with urllib.request.urlopen(req, timeout=30) as resp:
            data = json.loads(resp.read().decode("utf-8"))
            if "errors" in data and data["errors"]:
                raise RuntimeError(f"GraphQL errors: {data['errors']}")
            return data.get("data", {})
    except urllib.error.URLError as exc:
        raise ConnectionError(f"Failed to connect to Dagster at {endpoint_url}: {exc}") from exc


def discover_job_selector(endpoint_url: str, job_name: str) -> Tuple[str, str]:
    """Find repositoryLocationName and repositoryName for a target job."""
    query = """
    query RepositoriesQuery {
      repositoriesOrError {
        __typename
        ... on RepositoryConnection {
          nodes {
            name
            location {
              name
            }
            pipelines {
              name
            }
          }
        }
        ... on PythonError {
          message
          stack
        }
      }
    }
    """
    data = execute_graphql(endpoint_url, query)
    repos_data = data.get("repositoriesOrError", {})
    typename = repos_data.get("__typename")

    if typename == "PythonError":
        raise RuntimeError(f"Dagster returned PythonError: {repos_data.get('message')}")

    for repo in repos_data.get("nodes", []):
        location_name = repo.get("location", {}).get("name")
        repo_name = repo.get("name")
        pipelines = [p.get("name") for p in repo.get("pipelines", [])]
        if job_name in pipelines:
            return location_name, repo_name

    # Fallback default if not explicitly found in query
    return "finsights-scraping-pipelines", "__repository__"


def launch_dagster_job(
    endpoint_url: str,
    source: str,
    start_date: str,
    end_date: str,
    storage_mode: str = "link_only",
) -> str:
    """Submit a single scraping job to Dagster."""
    if source not in SOURCE_CONFIGS:
        raise ValueError(f"Unknown source '{source}'. Available: {ALL_SOURCES}")

    source_info = SOURCE_CONFIGS[source]
    job_name = source_info["job_name"]
    op_name = source_info["op_name"]
    op_config = source_info["config_builder"](start_date, end_date, storage_mode)

    location_name, repo_name = discover_job_selector(endpoint_url, job_name)

    run_config = {
        "ops": {
            op_name: {
                "config": op_config,
            }
        }
    }

    mutation = """
    mutation LaunchExecution($executionParams: ExecutionParams!) {
      launchExecution(executionParams: $executionParams) {
        __typename
        ... on LaunchRunSuccess {
          run {
            runId
            status
          }
        }
        ... on PipelineNotFoundError {
          message
        }
        ... on RunConfigValidationInvalid {
          pipelineName
          errors {
            message
            reason
          }
        }
        ... on PythonError {
          message
          stack
        }
      }
    }
    """

    variables = {
        "executionParams": {
            "selector": {
                "repositoryLocationName": location_name,
                "repositoryName": repo_name,
                "jobName": job_name,
            },
            "runConfigData": run_config,
            "mode": "default",
        }
    }

    print(f"\n🚀 Submitting job '{job_name}' for source '{source}' [{start_date} -> {end_date}]...")
    resp = execute_graphql(endpoint_url, mutation, variables)
    result = resp.get("launchExecution", {})
    typename = result.get("__typename")

    if typename == "LaunchRunSuccess":
        run_info = result["run"]
        run_id = run_info["runId"]
        status = run_info["status"]
        print(f"✅ Job submitted successfully! Run ID: {run_id} | Initial Status: {status}")
        print(f"🔗 View in Dagster UI: {endpoint_url.rstrip('/')}/runs/{run_id}")
        return run_id
    elif typename == "RunConfigValidationInvalid":
        errors = result.get("errors", [])
        raise RuntimeError(f"Config validation error for job {job_name}: {json.dumps(errors, indent=2)}")
    elif typename == "PipelineNotFoundError":
        raise RuntimeError(f"Job '{job_name}' not found in Dagster location '{location_name}'. {result.get('message')}")
    else:
        raise RuntimeError(f"Failed to launch job: {json.dumps(result, indent=2)}")


def wait_for_run(endpoint_url: str, run_id: str, poll_interval: int = 5, timeout: int = 600) -> bool:
    """Poll run status until completion."""
    query = """
    query RunStatus($runId: ID!) {
      runOrError(runId: $runId) {
        __typename
        ... on Run {
          runId
          status
        }
      }
    }
    """
    terminal_statuses = {"SUCCESS", "FAILURE", "CANCELED"}
    start_time = time.time()
    last_status = None

    print(f"⏳ Waiting for Pod run {run_id} to finish (poll interval: {poll_interval}s, timeout: {timeout}s)...")
    while time.time() - start_time < timeout:
        data = execute_graphql(endpoint_url, query, {"runId": run_id})
        run_data = data.get("runOrError", {})
        status = run_data.get("status", "UNKNOWN")

        if status != last_status:
            elapsed = int(time.time() - start_time)
            print(f"   [{elapsed}s] Run {run_id} status: {status}")
            last_status = status

        if status in terminal_statuses:
            if status == "SUCCESS":
                print(f"🎉 Run {run_id} COMPLETED SUCCESSFULLY!")
                return True
            else:
                print(f"❌ Run {run_id} TERMINATED WITH STATUS: {status}")
                return False

        time.sleep(poll_interval)

    print(f"⚠️ Run {run_id} timed out after {timeout} seconds.")
    return False


def main() -> None:
    parser = argparse.ArgumentParser(description="Submit Finsights scraping jobs to Dagster K3s")
    parser.add_argument(
        "--dagster-url",
        default=os.getenv("DAGSTER_URL", "http://localhost:31882"),
        help="Dagster Webserver URL (default: http://localhost:31882 or DAGSTER_URL env)",
    )
    parser.add_argument(
        "--source",
        choices=["all", *ALL_SOURCES],
        default="cafef",
        help="Source to scrape, or 'all' to scrape all 6 sources",
    )
    parser.add_argument(
        "--start-date",
        required=True,
        help="Start date YYYY-MM-DD (Asia/Ho_Chi_Minh)",
    )
    parser.add_argument(
        "--end-date",
        required=True,
        help="End date YYYY-MM-DD (Asia/Ho_Chi_Minh)",
    )
    parser.add_argument(
        "--storage-mode",
        choices=["link_only", "full_media"],
        default="link_only",
        help="Storage mode: 'link_only' (no OCI upload) or 'full_media' (default: link_only)",
    )
    parser.add_argument(
        "--wait",
        action="store_true",
        help="Wait for run execution pod to complete on K3s",
    )
    parser.add_argument(
        "--poll-interval",
        type=int,
        default=5,
        help="Poll interval in seconds when --wait is enabled (default: 5)",
    )
    parser.add_argument(
        "--timeout",
        type=int,
        default=900,
        help="Timeout in seconds when --wait is enabled (default: 900s / 15m)",
    )

    args = parser.parse_args()

    sources_to_run = ALL_SOURCES if args.source == "all" else [args.source]
    failed_runs = []

    print("=" * 65)
    print("FINSIGHTS DATA SCRAPING - DAGSTER K3S JOB SUBMISSION")
    print(f"Endpoint:   {args.dagster_url}")
    print(f"Sources:    {', '.join(sources_to_run)}")
    print(f"Window:     {args.start_date} -> {args.end_date}")
    print(f"Wait mode:  {'Enabled' if args.wait else 'Disabled'}")
    print("=" * 65)

    for src in sources_to_run:
        try:
            run_id = launch_dagster_job(
                endpoint_url=args.dagster_url,
                source=src,
                start_date=args.start_date,
                end_date=args.end_date,
                storage_mode=args.storage_mode,
            )
            if args.wait:
                success = wait_for_run(
                    endpoint_url=args.dagster_url,
                    run_id=run_id,
                    poll_interval=args.poll_interval,
                    timeout=args.timeout,
                )
                if not success:
                    failed_runs.append((src, run_id))
        except Exception as exc:
            print(f"❌ Error submitting job for {src}: {exc}")
            failed_runs.append((src, "SUBMIT_FAILED"))

    print("\n" + "=" * 65)
    if failed_runs:
        print(f"❌ Completed with {len(failed_runs)} failed run(s): {failed_runs}")
        sys.exit(1)
    else:
        print("✅ All requested scraping jobs have been successfully triggered!")
        sys.exit(0)


if __name__ == "__main__":
    main()

