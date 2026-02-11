from pagerank import iterative_pagerank


def test_pagerank_small_graph_basic_properties():
    print("\n==============================")
    print("TEST: PageRank on small graph")
    print("==============================")

    # Setup
    nodes = ["A", "B", "C", "D"]
    out_degree = {"A": 2, "B": 1, "C": 1, "D": 0}  # D is dangling
    in_links = {
        "A": ["C"],
        "B": ["A"],
        "C": ["A", "B", "D"],
        "D": [],
    }

    print("Graph:")
    print("  Nodes:", nodes)
    print("  out_degree:", out_degree)
    print("  in_links:", in_links)

    # Run PageRank
    pr, iters = iterative_pagerank(
        nodes, in_links, out_degree,
        tol_ratio=0.005, max_iters=200,
        log_every_iters=0
    )

    s = sum(pr.values())
    top = sorted(pr.items(), key=lambda x: x[1], reverse=True)

    print("\n--- Results ---")
    print(f"Iterations: {iters}")
    print(f"Sum(PR): {s:.10f}")
    print("Sorted PR:")
    for k, v in top:
        print(f"  {k}: {v:.10f}")

    #  Clear “test case” logs
    print("\n--- Checks ---")

    print("1) Non-negative PR values ... ", end="")
    assert all(v >= 0 for v in pr.values())
    print("PASS")

    print("2) Sum(PR) approximately 1.0 ... ", end="")
    assert abs(s - 1.0) < 1e-3
    print("PASS")

    print("3) Top ranked node is C ... ", end="")
    assert top[0][0] == "C"
    print("PASS")

    print("4) Iterations >= 1 ... ", end="")
    assert iters >= 1
    print("PASS")

    print("\n ALL TESTS PASSED")
