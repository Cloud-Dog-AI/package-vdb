# Copyright 2026 Cloud-Dog, Viewdeck Engineering Limited
# Licensed under the Apache License, Version 2.0
"""
Adoption test for cloud_dog_vdb (PS-60).
Run check_adoption(project_root) to verify a project fully uses cloud_dog_vdb.
"""

import pathlib
import subprocess


def _grep(pattern: str, search_path: pathlib.Path, extra_flags: str = "") -> list[str]:
    cmd = f"grep -rn {extra_flags} '{pattern}' {search_path} --include='*.py' | grep -v __pycache__ | grep -v '/archive/' | grep -v '/working/'"
    result = subprocess.run(cmd, shell=True, capture_output=True, text=True)
    return [line for line in result.stdout.strip().split("\n") if line]


def _find_source_dirs(project_root: pathlib.Path) -> list[pathlib.Path]:
    dirs = []
    src = project_root / "src"
    if src.is_dir():
        dirs.append(src)
    crawler = project_root / "crawler"
    if crawler.is_dir():
        dirs.append(crawler)
    return dirs


def check_adoption(project_root: pathlib.Path) -> dict:
    results = {"pass": [], "fail": []}
    source_dirs = _find_source_dirs(project_root)

    if not source_dirs:
        results["fail"].append(f"No source directory found in {project_root}")
        return results

    # 1. cloud_dog_vdb imported
    all_imports = []
    for sd in source_dirs:
        all_imports.extend(_grep("cloud_dog_vdb", sd))
    for f in project_root.glob("start_*.py"):
        cmd = f"grep -n 'cloud_dog_vdb' {f}"
        r = subprocess.run(cmd, shell=True, capture_output=True, text=True)
        if r.stdout.strip():
            all_imports.extend(r.stdout.strip().split("\n"))
    if len(all_imports) > 0:
        results["pass"].append(f"cloud_dog_vdb imported ({len(all_imports)} references)")
    else:
        results["fail"].append("cloud_dog_vdb NOT imported anywhere in source")

    # 2. No direct chromadb/qdrant_client/weaviate imports outside cloud_dog_vdb
    direct_vdb = []
    for sd in source_dirs:
        hits = _grep(
            "import chromadb\\|from chromadb\\|import qdrant_client\\|from qdrant_client\\|import weaviate\\|from weaviate",
            sd,
        )
        for h in hits:
            if "cloud_dog_vdb" not in h and "/test" not in h.lower():
                direct_vdb.append(h)
    if len(direct_vdb) == 0:
        results["pass"].append("No direct vector DB client imports outside cloud_dog_vdb")
    else:
        results["fail"].append(f"Direct vector DB imports found (should use cloud_dog_vdb): {direct_vdb}")

    # 3. Vector operations via cloud_dog_vdb abstraction
    vdb_api_refs = []
    for sd in source_dirs:
        vdb_api_refs.extend(
            _grep("VDBClient\\|get_vdb_client\\|SearchRequest\\|ingest_document\\|IngestionPipeline", sd)
        )
    if len(vdb_api_refs) > 0:
        results["pass"].append(f"cloud_dog_vdb API used ({len(vdb_api_refs)} references)")
    else:
        results["fail"].append("No cloud_dog_vdb API usage (VDBClient/get_vdb_client/SearchRequest)")

    return results
