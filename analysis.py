#!/usr/bin/env python3
import argparse
import re
import time
from concurrent.futures import ThreadPoolExecutor, as_completed
from typing import Dict, List, Optional, Set, Tuple

from google.cloud import storage

from stats import stats_block
from pagerank import iterative_pagerank

HREF_RE = re.compile(r'HREF\s*=\s*"(.*?)"', re.IGNORECASE)


def parse_outgoing_links(text: str, nodes_set: Set[str]) -> List[str]:
    out: List[str] = []
    for line in text.splitlines():
        line = line.strip()
        if not line:
            continue

        m = HREF_RE.search(line)
        if m:
            target = m.group(1).strip()
            if target in nodes_set:
                out.append(target)
            continue

        if line in nodes_set:
            out.append(line)

    return out


def list_nodes_from_bucket(client: storage.Client, bucket_name: str) -> List[str]:
    nodes: List[str] = []
    for blob in client.list_blobs(bucket_name):
        nodes.append(blob.name)
    nodes.sort()
    return nodes


def _download_and_parse_one(
    bucket_name: str,
    blob_name: str,
    nodes_set: Set[str],
) -> Tuple[str, List[str]]:
    """
    Runs inside a worker thread:
      - creates its own client (safe)
      - downloads blob text
      - parses outgoing links
    Returns: (node, outs)
    """
    client = storage.Client()
    bucket = client.bucket(bucket_name)
    text = bucket.blob(blob_name).download_as_text()
    outs = parse_outgoing_links(text, nodes_set)
    return blob_name, outs


def compute_in_out_counts(
    bucket_name: str,
    limit_files: Optional[int] = None,
    workers: int = 32,
) -> Tuple[List[str], Dict[str, int], Dict[str, int], Dict[str, List[str]]]:

    client = storage.Client()
    nodes = list_nodes_from_bucket(client, bucket_name)

    if not nodes:
        raise RuntimeError("No files found in the bucket.")

    if limit_files is not None:
        nodes = nodes[:limit_files]

    nodes_set = set(nodes)

    # We fill these AFTER parallel parsing
    out_degree: Dict[str, int] = {u: 0 for u in nodes}
    in_counts: Dict[str, int] = {u: 0 for u in nodes}
    in_links: Dict[str, List[str]] = {u: [] for u in nodes}

    # Parallel download + parse
    futures = []
    with ThreadPoolExecutor(max_workers=workers) as ex:
        for u in nodes:
            futures.append(ex.submit(_download_and_parse_one, bucket_name, u, nodes_set))

        for fut in as_completed(futures):
            u, outs = fut.result()
            out_degree[u] = len(outs)
            for v in outs:
                in_counts[v] += 1
                in_links[v].append(u)

    return nodes, out_degree, in_counts, in_links


def main() -> None:
    parser = argparse.ArgumentParser()
    parser.add_argument("--bucket", required=True)
    parser.add_argument(
        "--limit_files",
        type=int,
        default=1000,
        help="Read only the first N files (for testing)",
    )
    parser.add_argument(
        "--workers",
        type=int,
        default=32,
        help="Number of threads for parallel GCS downloads/parsing",
    )
    args = parser.parse_args()

    print(f"\n=== Analyzing {args.limit_files} files (workers={args.workers}) ===")

    t0 = time.time()
    nodes, out_degree, in_counts, in_links = compute_in_out_counts(
        args.bucket, args.limit_files, args.workers
    )
    t1 = time.time()

    print("\n=== Outgoing Links Stats ===")
    print(stats_block(list(out_degree.values())))

    print("\n=== Incoming Links Stats ===")
    print(stats_block(list(in_counts.values())))

    pr, iters = iterative_pagerank(
        nodes=nodes,
        in_links=in_links,
        out_degree=out_degree,
        damping=0.85,
        base=0.15,
        tol_ratio=0.005,
        max_iters=200,
    )
    t2 = time.time()

    print("\n=== PageRank Top 5 ===")
    for i, (page, score) in enumerate(
        sorted(pr.items(), key=lambda x: x[1], reverse=True)[:5], start=1
    ):
        print(f"{i}. {page}  PR={score:.10f}")

    print("\n=== Timing ===")
    print(f"Read+parse time: {t1 - t0:.2f} sec")
    print(f"PageRank time:    {t2 - t1:.2f} sec (iters={iters})")
    print(f"Total time:       {t2 - t0:.2f} sec")


if __name__ == "__main__":
    main()
