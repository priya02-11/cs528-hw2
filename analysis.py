#!/usr/bin/env python3
import argparse
import logging
import re
import time
from concurrent.futures import ThreadPoolExecutor, as_completed
from typing import Dict, List, Optional, Set, Tuple

from google.cloud import storage

from stats import stats_block
from pagerank import iterative_pagerank

HREF_RE = re.compile(r'HREF\s*=\s*"(.*?)"', re.IGNORECASE)

def setup_logging() -> logging.Logger:
    logging.basicConfig(
        level=logging.INFO,
        format="%(asctime)s | %(levelname)s | %(message)s",
    )
    return logging.getLogger("analysis")

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

def list_nodes_from_bucket(client: storage.Client, bucket_name: str, log: logging.Logger) -> List[str]:
    nodes: List[str] = []
    t0 = time.time()

    for i, blob in enumerate(client.list_blobs(bucket_name), start=1):
        nodes.append(blob.name)
        if i % 5000 == 0:
            log.info(f"Listed {i} objects so far...")

    nodes.sort()
    log.info(f"Finished listing. Found {len(nodes)} objects in {time.time() - t0:.2f}s")
    return nodes

def _download_one(bucket_name: str, blob_name: str) -> Tuple[str, str]:
    """
    Runs inside a download worker thread:
      - creates its own client (safe)
      - downloads blob text
    Returns: (blob_name, text)
    """
    client = storage.Client()
    bucket = client.bucket(bucket_name)
    text = bucket.blob(blob_name).download_as_text()
    return blob_name, text

def _parse_one(blob_name: str, text: str, nodes_set: Set[str]) -> Tuple[str, List[str]]:
    """
    Runs inside a parse worker thread:
      - parses outgoing links from text
    Returns: (blob_name, outs)
    """
    outs = parse_outgoing_links(text, nodes_set)
    return blob_name, outs

def compute_in_out_counts(
    bucket_name: str,
    limit_files: Optional[int] = None,
    download_workers: int = 8,
    parse_workers: int = 8,
    log_every: int = 100,
    log: Optional[logging.Logger] = None,
) -> Tuple[List[str], Dict[str, int], Dict[str, int], Dict[str, List[str]]]:

    if log is None:
        log = logging.getLogger("analysis")

    client = storage.Client()
    nodes = list_nodes_from_bucket(client, bucket_name, log)

    if not nodes:
        raise RuntimeError("No files found in the bucket.")

    if limit_files is not None:
        nodes = nodes[:limit_files]

    nodes_set = set(nodes)

    log.info(
        f"Will analyze {len(nodes)} files from bucket '{bucket_name}' "
        f"using download_workers={download_workers}, parse_workers={parse_workers}"
    )

    # Final outputs
    out_degree: Dict[str, int] = {u: 0 for u in nodes}
    in_counts: Dict[str, int] = {u: 0 for u in nodes}
    in_links: Dict[str, List[str]] = {u: [] for u in nodes}

    t0 = time.time()

    # Stage 1: download futures
    download_futures = []
    downloaded = 0

    with ThreadPoolExecutor(max_workers=download_workers) as dl_ex:
        for u in nodes:
            download_futures.append(dl_ex.submit(_download_one, bucket_name, u))
        log.info(f"Queued {len(download_futures)} downloads. Starting downloads...")

        # Stage 2: parse futures created as downloads complete
        parse_futures = []
        parsed = 0

        with ThreadPoolExecutor(max_workers=parse_workers) as parse_ex:
            for df in as_completed(download_futures):
                u, text = df.result()
                downloaded += 1

                if log_every > 0 and downloaded % log_every == 0:
                    elapsed = time.time() - t0
                    rate = downloaded / elapsed if elapsed > 0 else 0.0
                    log.info(f"Downloaded {downloaded}/{len(nodes)} files ({rate:.1f} files/sec)")

                parse_futures.append(parse_ex.submit(_parse_one, u, text, nodes_set))

            log.info("All downloads completed. Waiting for parsing to finish...")

            for pf in as_completed(parse_futures):
                u, outs = pf.result()
                parsed += 1

                out_degree[u] = len(outs)
                for v in outs:
                    in_counts[v] += 1
                    in_links[v].append(u)

                if log_every > 0 and parsed % log_every == 0:
                    elapsed = time.time() - t0
                    rate = parsed / elapsed if elapsed > 0 else 0.0
                    log.info(f"Parsed {parsed}/{len(nodes)} files ({rate:.1f} files/sec)")

    log.info(f"Finished download+parse of {len(nodes)} files in {time.time() - t0:.2f}s")
    return nodes, out_degree, in_counts, in_links

def main() -> None:
    log = setup_logging()

    parser = argparse.ArgumentParser()
    parser.add_argument("--bucket", required=True)
    parser.add_argument(
        "--limit_files",
        type=int,
        default=1000,
        help="Read only the first N files (for testing)",
    )
    parser.add_argument(
        "--download_workers",
        type=int,
        default=8,
        help="Number of threads for GCS downloads (network bound)",
    )
    parser.add_argument(
        "--parse_workers",
        type=int,
        default=8,
        help="Number of threads for parsing (CPU bound-ish)",
    )
    parser.add_argument(
        "--log_every",
        type=int,
        default=100,
        help="Log progress after every N files (0 disables)",
    )
    args = parser.parse_args()

    print(
        f"\n=== Analyzing {args.limit_files} files "
        f"(download_workers={args.download_workers}, parse_workers={args.parse_workers}) ==="
    )
    log.info(
        f"Args: bucket={args.bucket}, limit_files={args.limit_files}, "
        f"download_workers={args.download_workers}, parse_workers={args.parse_workers}, "
        f"log_every={args.log_every}"
    )

    t0 = time.time()
    nodes, out_degree, in_counts, in_links = compute_in_out_counts(
        bucket_name=args.bucket,
        limit_files=args.limit_files,
        download_workers=args.download_workers,
        parse_workers=args.parse_workers,
        log_every=args.log_every,
        log=log,
    )
    t1 = time.time()

    print("\n=== Outgoing Links Stats ===")
    print(stats_block(list(out_degree.values())))

    print("\n=== Incoming Links Stats ===")
    print(stats_block(list(in_counts.values())))

    log.info("Starting PageRank iterations...")
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
    log.info(f"Finished PageRank in {t2 - t1:.2f}s (iters={iters})")

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


