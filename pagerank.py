from typing import Dict, List, Tuple
import time


def iterative_pagerank(
    nodes: List[str],
    in_links: Dict[str, List[str]],
    out_degree: Dict[str, int],
    damping: float = 0.85,
    base: float = 0.15,
    tol_ratio: float = 0.005,
    max_iters: int = 200,
    log_every_iters: int = 10,   # NEW: print progress every N iterations (0 disables)
) -> Tuple[Dict[str, float], int]:
    """
    PageRank (iterative):

      PR(A) = base/n + damping * ( sum_{T in In(A)} PR(T) / C(T)  + dangling_mass/n )

    Convergence (your assignment style):
      delta = sum_u |PR_new(u) - PR_old(u)|
      stop when delta < tol_ratio
    """

    n = len(nodes)
    if n == 0:
        return {}, 0

    sum_tol = 1e-3

    # Initialize PR uniformly
    pr: Dict[str, float] = {u: 1.0 / n for u in nodes}
    teleport = base / n

    t0 = time.time()

    for it in range(1, max_iters + 1):
        new_pr: Dict[str, float] = {}

        # 1) Dangling mass = PR from nodes with out_degree == 0
        dangling_mass = 0.0
        for u in nodes:
            if out_degree.get(u, 0) == 0:
                dangling_mass += pr[u]
        dangling_share = dangling_mass / n

        # 2) Compute PageRank update
        for a in nodes:
            s = 0.0
            for t in in_links.get(a, []):
                c = out_degree.get(t, 0)
                if c > 0:
                    s += pr[t] / c
                # if c == 0, it contributes via dangling_share instead

            new_pr[a] = teleport + damping * (s + dangling_share)

        # 3) Convergence check: sum of absolute differences
        delta = 0.0
        for u in nodes:
            delta += abs(new_pr[u] - pr[u])

        pr = new_pr

        # 4) Optional progress logs
        if log_every_iters and (it == 1 or it % log_every_iters == 0):
            elapsed = time.time() - t0
            sum_pr = sum(pr.values())
            print(f"[pagerank] iter={it:3d} delta={delta:.6f} sumPR={sum_pr:.6f} elapsed={elapsed:.1f}s")

        # 5) Stop condition
        if delta < tol_ratio:
            if abs(sum(pr.values()) - 1.0) > sum_tol:
                print("Warning: sum(PR) is not ~1.0; check convergence logic or dangling handling.")
            return pr, it

    return pr, max_iters
