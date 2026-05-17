"""
Silver → Gold 파이프라인 진입점

실행:
    cd Iceberg_pipeline
    python src/silver_to_gold/main.py
"""

import sys
import os
import logging

_PROJECT_ROOT = os.path.dirname(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
if _PROJECT_ROOT not in sys.path:
    sys.path.insert(0, _PROJECT_ROOT)

from oliveyoung_common.batch import build_run_id
from oliveyoung_common.logging import job_unit, setup_logging
from src.silver_to_gold.pipeline import run_pipeline

setup_logging("iceberg-silver-to-gold")

logger = logging.getLogger(__name__)


if __name__ == "__main__":
    with job_unit(
        logger,
        job="iceberg_silver_to_gold",
        run_id=build_run_id("iceberg_silver_to_gold"),
    ):
        run_pipeline()
