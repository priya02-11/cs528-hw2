import numpy as np
from typing import Dict, List, Union


def stats_block(vals: List[int]) -> Dict[str, Union[float, int, List[float]]]:
    if not vals:
        raise ValueError("vals is empty")

    arr = np.array(vals, dtype=float)

    return {
        "average": float(arr.mean()),
        "median": float(np.median(arr)),
        "min": int(arr.min()),
        "max": int(arr.max()),
        "quintiles": [
            float(np.percentile(arr, 20)),
            float(np.percentile(arr, 40)),
            float(np.percentile(arr, 60)),
            float(np.percentile(arr, 80)),
        ],
    }
