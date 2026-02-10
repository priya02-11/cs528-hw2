from pagerank import iterative_pagerank


def test_pagerank_small_graph_basic_properties():
    nodes = ["A", "B", "C", "D"]

    # Optional but recommended: test dangling node handling
    out_degree = {"A": 2, "B": 1, "C": 1, "D": 0}

    in_links = {
        "A": ["C"],
        "B": ["A"],
        "C": ["A", "B", "D"],
        "D": [],
    }

    pr, iters = iterative_pagerank(
        nodes, in_links, out_degree,
        tol_ratio=0.005, max_iters=200,
        log_every_iters=0
    )

    # 1) Non-negative
    assert all(v >= 0 for v in pr.values())

    # 2) Sum should be ~1
    s = sum(pr.values())
    assert abs(s - 1.0) < 1e-3

    # 3) C should be top
    top = sorted(pr.items(), key=lambda x: x[1], reverse=True)
    assert top[0][0] == "C"

    assert iters >= 1
