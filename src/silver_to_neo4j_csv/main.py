"""
Silver → Neo4j CSV 파이프라인 진입점

실행:
    cd Iceberg_pipeline
    python src/silver_to_neo4j_csv/main.py
"""

import sys
import os

_PROJECT_ROOT = os.path.dirname(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
if _PROJECT_ROOT not in sys.path:
    sys.path.insert(0, _PROJECT_ROOT)

import logging

from src.silver_to_neo4j_csv.pipeline import run_pipeline


if __name__ == "__main__":
    logging.basicConfig(
        level=logging.INFO,
        format="%(asctime)s [%(levelname)s] %(name)s: %(message)s",
    )
    run_pipeline()
