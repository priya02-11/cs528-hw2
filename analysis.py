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


def list_nodes_from_bucket(
    client: storage.Client, 
    bucket_name: str, 
    prefix: str,
    log: logging.Logger
) -> List[str]:
    """List nodes with prefix support and progress updates."""
    nodes: List[str] = []
    t0 = time.time()

    for i, blob in enumerate(client.list_blobs(bucket_name, prefix=prefix), start=1):
        nodes.append(blob.name)
        if i % 1000 == 0:
            log.info(f"Listed {i} objects so far...")

    nodes.sort()
    log.info(f"Finished listing. Found {len(nodes)} objects in {time.time() - t0:.2f}s")
    return nodes


# FIX #1: Share a single client via closure instead of creating new ones
def make_download_worker(bucket_name: str):
    """
    Create a download worker that reuses a single client.
    This closure captures the client once.
    """
    client = storage.Client()
    bucket = client.bucket(bucket_name)
    
    def _download_one(blob_name: str) -> Tuple[str, str]:
        """Download using the shared client."""
        text = bucket.blob(blob_name).download_as_text()
        return blob_name, text
    
    return _download_one


def _parse_one(blob_name: str, text: str, nodes_set: Set[str]) -> Tuple[str, List[str]]:
    """
    Parse outgoing links from text.
    Returns: (blob_name, outs)
    """
    outs = parse_outgoing_links(text, nodes_set)
    return blob_name, outs


# FIX #2: Stream processing - download and parse together
def compute_in_out_counts_streaming(
    bucket_name: str,
    prefix: str = "",
    limit_files: Optional[int] = None,
    download_workers: int = 16,
    parse_workers: int = 8,
    log_every: int = 500,
    log: Optional[logging.Logger] = None,
) -> Tuple[List[str], Dict[str, int], Dict[str, int], Dict[str, List[str]]]:
    """
    Streaming approach: Download and parse simultaneously without storing all in memory.
    """
    if log is None:
        log = logging.getLogger("analysis")

    client = storage.Client()
    nodes = list_nodes_from_bucket(client, bucket_name, prefix, log)

    if not nodes:
        raise RuntimeError("No files found in the bucket.")

    if limit_files is not None:
        nodes = nodes[:limit_files]

    nodes_set = set(nodes)

    # Final outputs
    out_degree: Dict[str, int] = {u: 0 for u in nodes}
    in_counts: Dict[str, int] = {u: 0 for u in nodes}
    in_links: Dict[str, List[str]] = {u: [] for u in nodes}

    t0 = time.time()
    processed = 0

    # Create download worker with shared client
    download_worker = make_download_worker(bucket_name)

    # Download and parse in pipeline
    with ThreadPoolExecutor(max_workers=download_workers) as dl_ex:
        # Submit downloads
        download_futures = {dl_ex.submit(download_worker, u): u for u in nodes}
        
        with ThreadPoolExecutor(max_workers=parse_workers) as parse_ex:
            parse_futures = []
            
            # As downloads complete, immediately submit for parsing
            for dl_future in as_completed(download_futures):
                u, text = dl_future.result()
                
                # Submit to parse pool immediately (no storage in memory)
                parse_future = parse_ex.submit(_parse_one, u, text, nodes_set)
                parse_futures.append((u, parse_future))
            
            # Collect parse results
            for u, parse_future in parse_futures:
                _, outs = parse_future.result()
                processed += 1
                
                out_degree[u] = len(outs)
                for v in outs:
                    in_counts[v] += 1
                    in_links[v].append(u)
                
                if log_every > 0 and processed % log_every == 0:
                    elapsed = time.time() - t0
                    rate = processed / elapsed if elapsed > 0 else 0.0
                    log.info(f"Processed {processed}/{len(nodes)} files ({rate:.1f} files/sec)")

    log.info(f"Finished processing {len(nodes)} files in {time.time() - t0:.2f}s")
    return nodes, out_degree, in_counts, in_links


# Keep the old function for comparison
def compute_in_out_counts_download_then_parse(
    bucket_name: str,
    prefix: str = "",
    limit_files: Optional[int] = None,
    download_workers: int = 16,
    parse_workers: int = 8,
    log_every: int = 500,
    log: Optional[logging.Logger] = None,
) -> Tuple[List[str], Dict[str, int], Dict[str, int], Dict[str, List[str]]]:
    """
    Two-phase approach: Download ALL then parse ALL.
    OPTIMIZED VERSION with shared client.
    """
    if log is None:
        log = logging.getLogger("analysis")

    client = storage.Client()
    nodes = list_nodes_from_bucket(client, bucket_name, prefix, log)

    if not nodes:
        raise RuntimeError("No files found in the bucket.")

    if limit_files is not None:
        nodes = nodes[:limit_files]

    nodes_set = set(nodes)

    # Final outputs
    out_degree: Dict[str, int] = {u: 0 for u in nodes}
    in_counts: Dict[str, int] = {u: 0 for u in nodes}
    in_links: Dict[str, List[str]] = {u: [] for u in nodes}

    t0 = time.time()

    # FIX: Use shared client via closure
    download_worker = make_download_worker(bucket_name)

    # Phase 1: DOWNLOAD ALL
    downloaded_text: Dict[str, str] = {}
    downloaded = 0

    with ThreadPoolExecutor(max_workers=download_workers) as dl_ex:
        futures = [dl_ex.submit(download_worker, u) for u in nodes]
        log.info(f"Queued {len(futures)} downloads. Starting downloads...")

        for f in as_completed(futures):
            u, text = f.result()
            downloaded_text[u] = text
            downloaded += 1

            if log_every > 0 and downloaded % log_every == 0:
                elapsed = time.time() - t0
                rate = downloaded / elapsed if elapsed > 0 else 0.0
                log.info(f"Downloaded {downloaded}/{len(nodes)} files ({rate:.1f} files/sec)")

    log.info("All downloads completed. Starting parsing phase...")

    # Phase 2: PARSE ALL
    parsed = 0

    with ThreadPoolExecutor(max_workers=parse_workers) as parse_ex:
        parse_futures = [parse_ex.submit(_parse_one, u, downloaded_text[u], nodes_set) for u in nodes]

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

    log.info(f"Finished download THEN parse of {len(nodes)} files in {time.time() - t0:.2f}s")
    return nodes, out_degree, in_counts, in_links


def main() -> None:
    log = setup_logging()

    parser = argparse.ArgumentParser()
    parser.add_argument("--bucket", required=True)
    parser.add_argument("--prefix", default="", help="Prefix for bucket listing (e.g., 'generated_files_v2/')")
    parser.add_argument(
        "--limit_files",
        type=int,
        default=0,
        help="Read only the first N files (for testing). Use 0 for all files.",
    )
    parser.add_argument(
        "--download_workers",
        type=int,
        default=16,
        help="Number of threads for GCS downloads (network bound).",
    )
    parser.add_argument(
        "--parse_workers",
        type=int,
        default=8,
        help="Number of threads for parsing (CPU bound-ish).",
    )
    parser.add_argument(
        "--log_every",
        type=int,
        default=500,
        help="Log progress after every N files (0 disables).",
    )
    parser.add_argument(
        "--streaming",
        action="store_true",
        help="Use streaming approach (download+parse together) instead of two-phase",
    )
    args = parser.parse_args()

    limit_files = args.limit_files if args.limit_files > 0 else None

    print(
        f"\n=== Analyzing {'ALL' if limit_files is None else limit_files} files "
        f"(download_workers={args.download_workers}, parse_workers={args.parse_workers}) ==="
    )
    log.info(
        f"Args: bucket={args.bucket}, prefix={args.prefix}, limit_files={limit_files}, "
        f"download_workers={args.download_workers}, parse_workers={args.parse_workers}, "
        f"log_every={args.log_every}, streaming={args.streaming}"
    )

    t0 = time.time()
    
    if args.streaming:
        log.info("Using STREAMING approach (download+parse pipeline)")
        nodes, out_degree, in_counts, in_links = compute_in_out_counts_streaming(
            bucket_name=args.bucket,
            prefix=args.prefix,
            limit_files=limit_files,
            download_workers=args.download_workers,
            parse_workers=args.parse_workers,
            log_every=args.log_every,
            log=log,
        )
    else:
        log.info("Using TWO-PHASE approach (download all, then parse all)")
        nodes, out_degree, in_counts, in_links = compute_in_out_counts_download_then_parse(
            bucket_name=args.bucket,
            prefix=args.prefix,
            limit_files=limit_files,
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