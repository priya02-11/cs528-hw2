from typing import Dict, List, Tuple


def iterative_pagerank(
    nodes: List[str],
    in_links: Dict[str, List[str]],
    out_degree: Dict[str, int],
    damping: float = 0.85,
    base: float = 0.15,
    tol_ratio: float = 0.005,
    max_iters: int = 200,
) -> Tuple[Dict[str, float], int]:
    """
    PageRank (iterative):

      PR(A) = base/n + damping * sum_{T in In(A)} PR(T) / C(T)

    Convergence:
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

    for it in range(1, max_iters + 1):
        new_pr: Dict[str, float] = {}

        # Compute PageRank update
        for a in nodes:
            s = 0.0
            for t in in_links.get(a, []):
                c = out_degree[t]  # guaranteed > 0
                s += pr[t] / c

            new_pr[a] = teleport + damping * s

        # Convergence check: sum of absolute differences
        delta = 0.0
        for u in nodes:
            delta += abs(new_pr[u] - pr[u])

        pr = new_pr

        # Stop condition
        if delta < tol_ratio:
            if abs(sum(pr.values()) - 1.0) > sum_tol:
                print('Warning: There is an issue in the convergence logic.')
            return pr, it

    return pr, max_iters
