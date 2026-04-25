from __future__ import annotations

import argparse
import sys
from pathlib import Path

ROOT = Path(__file__).resolve().parents[2]
SRC = ROOT / "src"
if str(SRC) not in sys.path:
    sys.path.insert(0, str(SRC))

from stock_analyst.stock_reference import CONCEPT_CACHE, SPOT_CACHE, get_concept_index, get_spot_df


def main() -> None:
    parser = argparse.ArgumentParser(description="预构建 AkShare 股票名称与概念板块离线缓存")
    parser.add_argument("--skip-spot", action="store_true", help="跳过股票名称缓存")
    parser.add_argument("--skip-concept", action="store_true", help="跳过概念板块缓存")
    args = parser.parse_args()

    if not args.skip_spot:
        spot_df = get_spot_df(force_refresh=True)
        print(f"[akshare-cache] spot_rows={len(spot_df)} path={SPOT_CACHE.as_posix()}")

    if not args.skip_concept:
        concept_index = get_concept_index(force_refresh=True)
        stock_hits = sum(len(v) > 0 for v in concept_index.values())
        print(
            "[akshare-cache] "
            f"concept_stock_count={stock_hits} unique_codes={len(concept_index)} path={CONCEPT_CACHE.as_posix()}"
        )


if __name__ == "__main__":
    main()
