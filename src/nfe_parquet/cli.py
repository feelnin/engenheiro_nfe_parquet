from __future__ import annotations

import argparse
from pathlib import Path

from .config.loader import load_config
from .orchestrator.pipeline_mt import run_once_mt

def main() -> None:
    parser = argparse.ArgumentParser(description="ETL NF-e -> Parquet")
    parser.add_argument("config", type=Path, help="Caminho para config.yaml")
    args = parser.parse_args()
    
    cfg = load_config(args.config)
    run_once_mt(cfg)

if __name__ == "__main__":
    main()