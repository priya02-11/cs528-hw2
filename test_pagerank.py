from pagerank import iterative_pagerank


def test_pagerank_small_graph_basic_properties():
    # Small fixed graph (independent of your 20k random graph)
    # A -> B, C
    # B -> C
    # C -> A
    # D -> C
    nodes = ["A", "B", "C", "D"]
    out_degree = {"A": 2, "B": 1, "C": 1, "D": 1}

    in_links = {
        "A": ["C"],
        "B": ["A"],
        "C": ["A", "B", "D"],
        "D": [],
    }

    pr, iters = iterative_pagerank(nodes, in_links, out_degree, tol_ratio=0.005, max_iters=200)

    # 1) Non-negative
    assert all(v >= 0 for v in pr.values())

    # 2) Sum should be positive and stable-ish
    s = sum(pr.values())
    assert s > 0

    # 3) C should be among the highest because it has most incoming links
    top = sorted(pr.items(), key=lambda x: x[1], reverse=True)
    assert top[0][0] in ("C",)  # expect C as top
    assert iters >= 1
