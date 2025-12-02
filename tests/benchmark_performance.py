#!/usr/bin/env python
"""Performance benchmark for DAG generation with large manifests."""
import json
import os
import tempfile
import time
from pathlib import Path


def generate_manifest(num_models: int, output_path: str) -> None:
    """Generate a dbt manifest.json with the specified number of models."""
    nodes = {}

    # Create linear dependency chain for realistic graph
    for i in range(num_models):
        model_name = f"model_{i:04d}"
        node_id = f"model.benchmark_project.{model_name}"

        # Each model depends on the previous one (except first)
        depends_on = []
        if i > 0:
            prev_model = f"model.benchmark_project.model_{i-1:04d}"
            depends_on = [prev_model]

        nodes[node_id] = {
            "name": model_name,
            "unique_id": node_id,
            "resource_type": "model",
            "depends_on": {"nodes": depends_on},
            "config": {"materialized": "view"},
            "tags": [],
            "path": f"models/{model_name}.sql",
            "original_file_path": f"models/{model_name}.sql",
            "package_name": "benchmark_project",
            "fqn": ["benchmark_project", model_name],
        }

    manifest = {
        "metadata": {"dbt_version": "1.7.0", "project_name": "benchmark_project"},
        "nodes": nodes,
        "sources": {},
        "docs": {},
        "macros": {},
        "exposures": {},
    }

    with open(output_path, "w") as f:
        json.dump(manifest, f)


def benchmark_dag_creation(manifest_path: str, num_models: int) -> float:
    """Benchmark DAG creation time."""
    from dbt_airflow_factory.cosmos.project_config_builder import build_project_config

    dbt_config = {
        "project_dir_path": "/dbt",
        "target": "dev",
        "dbt_project_name": "benchmark_project",
    }

    start = time.perf_counter()

    # Build project config (includes manifest parsing)
    result = build_project_config(
        dbt_config=dbt_config,
        manifest_path=manifest_path,
        dag_id="benchmark_project",
    )

    elapsed = time.perf_counter() - start
    return elapsed


def run_benchmarks():
    """Run performance benchmarks."""
    print("=" * 60)
    print("dbt-airflow-factory Performance Benchmark")
    print("=" * 60)

    with tempfile.TemporaryDirectory() as tmpdir:
        for num_models in [10, 50, 100, 200, 500]:
            manifest_path = os.path.join(tmpdir, f"manifest_{num_models}.json")

            # Generate manifest
            generate_manifest(num_models, manifest_path)
            manifest_size = os.path.getsize(manifest_path) / 1024

            # Benchmark
            elapsed = benchmark_dag_creation(manifest_path, num_models)

            status = "PASS" if (num_models <= 100 and elapsed < 5.0) or (num_models <= 500 and elapsed < 15.0) else "SLOW"

            print(f"{num_models:4d} models | {elapsed:6.3f}s | {manifest_size:6.1f}KB | [{status}]")

    print("=" * 60)
    print("Target: 100 models < 5s, 500 models < 15s")


if __name__ == "__main__":
    run_benchmarks()
