"""Remove only the isolated outputs named by a comparison manifest.

Defaults to a dry run. Source Bronze files and production tables are never used
as deletion targets. Run after downloading comparison.json and retaining logs.
"""

import argparse
from collections import Counter
import json
from pathlib import Path
import re
import sys

sys.path.insert(0, str(Path(__file__).resolve().parents[1]))

import boto3

from config.settings import OliveyoungIceberg, S3


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--manifest", required=True)
    parser.add_argument("--apply", action="store_true")
    args = parser.parse_args()
    manifest = json.loads(Path(args.manifest).read_text())
    run_id = manifest["id"]
    if not re.fullmatch(r"\d{8}_\d{6}", run_id):
        raise ValueError("Invalid benchmark ID")
    prefix = f"benchmark/pipeline_comparison/{run_id}/"
    if manifest["s3_prefix"] + "/" != prefix:
        raise ValueError("Manifest prefix does not match benchmark ID")
    catalog = OliveyoungIceberg.get_catalog()
    tables = []
    for identifier in catalog.list_tables(OliveyoungIceberg.DATABASE):
        name = identifier[-1]
        if not name.startswith(f"bench_{run_id}_"):
            continue
        if not re.fullmatch(rf"bench_{run_id}_(before|after_materialized|after_cached)_\d+_(current|history|error)", name):
            raise ValueError(f"Unexpected benchmark table name: {name}")
        table = catalog.load_table(identifier)
        if not table.location().startswith(f"s3://{S3.BUCKET}/{prefix}"):
            raise ValueError(f"Refusing to remove table outside benchmark prefix: {name}")
        tables.append(identifier)
    client = boto3.client("s3", region_name=S3.REGION)
    objects = [obj for page in client.get_paginator("list_objects_v2").paginate(Bucket=S3.BUCKET, Prefix=prefix)
               for obj in page.get("Contents", [])]
    result = {"id": run_id, "prefix": prefix, "tables": len(tables),
              "objects": len(objects), "bytes": sum(obj["Size"] for obj in objects),
              "applied": args.apply}
    if args.apply:
        for identifier in tables:
            catalog.drop_table(identifier)
        for offset in range(0, len(objects), 1000):
            response = client.delete_objects(Bucket=S3.BUCKET, Delete={
                "Objects": [{"Key": obj["Key"]} for obj in objects[offset:offset + 1000]], "Quiet": True,
            })
            if response.get("Errors"):
                result["delete_error_codes"] = dict(Counter(error["Code"] for error in response["Errors"]))
                result["tables_dropped"] = len(tables)
                Path(args.manifest).with_name("cleanup.json").write_text(json.dumps(result, indent=2) + "\n")
                raise RuntimeError(f"S3 cleanup incomplete: {result['delete_error_codes']}; see cleanup.json")
        # Versioned buckets may retain previous versions according to bucket policy.
        result["remaining_current_objects"] = client.list_objects_v2(Bucket=S3.BUCKET, Prefix=prefix, MaxKeys=1)["KeyCount"]
        Path(args.manifest).with_name("cleanup.json").write_text(json.dumps(result, indent=2) + "\n")
    print(json.dumps(result, indent=2))


if __name__ == "__main__":
    main()
