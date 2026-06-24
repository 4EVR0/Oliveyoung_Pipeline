#!/bin/bash
set -e

case "$1" in
  create_reference_tables)
    python reference_pipeline/create_reference_tables.py "${@:2}"
    ;;
  sync_reference)
    python reference_pipeline/sync_reference_data.py
    ;;
  bronze_to_silver)
    python src/bronze_to_silver/main.py
    ;;
  silver_to_gold)
    python src/silver_to_gold/main.py
    ;;
  silver_to_neo4j_csv)
    python src/silver_to_neo4j_csv/main.py
    ;;
  create_gold_product_ingredients)
    python gold_pipeline/create_gold_product_ingredients.py
    ;;
  neo4j_incremental)
    python neo4j_incremental.py
    ;;
  *)
    echo "Usage: $0 {create_reference_tables|sync_reference|bronze_to_silver|silver_to_gold|silver_to_neo4j_csv|create_gold_product_ingredients|neo4j_incremental}"
    exit 1
    ;;
esac
