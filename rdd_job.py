"""
PySpark RDD job to compute top X detected items per geographical location.

Usage:
    spark-submit top_items_rdd_job.py \
        --input_a /path/to/parquet_a \
        --input_b /path/to/parquet_b \
        --output /path/to/output \
        --top_x 10
"""

import argparse
from pyspark.sql import SparkSession


def main(input_a: str, input_b: str, output: str, top_x: int) -> None:
    """Main job using RDD transformations."""
    spark = SparkSession.builder.appName("TopItemsRDDJob").getOrCreate()

    # --- Step 1: Read parquet files into DataFrames ---
    df_a = spark.read.parquet(input_a)
    df_b = spark.read.parquet(input_b)

    # --- Step 2: Convert to RDDs ---
    rdd_a = df_a.rdd
    rdd_b = df_b.rdd

    # --- Step 3: Deduplicate by detection_oid ---
    # Map detection_oid as key, keep first occurrence
    rdd_a_kv = rdd_a.map(lambda row: (row["detection_oid"], row))
    rdd_a_dedup = rdd_a_kv.reduceByKey(lambda x, _: x).map(lambda x: x[1])

    # --- Step 4: Count items per location ---
    # Key: (geographical_location_oid, item_name), Value: 1
    rdd_counts = (
        rdd_a_dedup
        .map(lambda row: ((row["geographical_location_oid"], row["item_name"]), 1))
        .reduceByKey(lambda a, b: a + b)
    )

    # --- Step 5: Group by location and sort items by count ---
    # Transform to (location_oid, (item_name, count))
    rdd_grouped = rdd_counts.map(
        lambda x: (x[0][0], (x[0][1], x[1]))
    ).groupByKey()

    # For each location, sort items by count desc and take top X
    rdd_ranked = rdd_grouped.flatMap(
        lambda kv: [
            (kv[0], rank + 1, item_name)
            for rank, (item_name, _) in enumerate(
                sorted(kv[1], key=lambda x: -x[1])[:top_x]
            )
        ]
    )

    # --- Step 6: Join with location metadata ---
    # Convert location metadata to RDD keyed by oid
    rdd_b_kv = rdd_b.map(
        lambda row: (row["geographical_location_oid"], row["geographical_location"])
    )

    # Join
    rdd_joined = rdd_ranked.map(
        lambda x: (x[0], (x[1], x[2]))
    ).join(rdd_b_kv)

    # --- Step 7: Convert back to DataFrame for output ---
    df_result = spark.createDataFrame(
        rdd_joined.map(
            lambda x: (
                x[0],              # geographical_location_oid
                x[1][0][0],        # item_rank
                x[1][0][1],        # item_name
                x[1][1],           # geographical_location
            )
        ),
        schema=[
            "geographical_location_oid",
            "item_rank",
            "item_name",
            "geographical_location",
        ],
    )

    # --- Step 8: Write output parquet ---
    df_result.write.mode("overwrite").parquet(output)

    spark.stop()


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="Top X Items RDD Job")
    parser.add_argument("--input_a", required=True, help="Path to Dataset A")
    parser.add_argument("--input_b", required=True, help="Path to Dataset B")
    parser.add_argument("--output", required=True, help="Output path")
    parser.add_argument(
        "--top_x", type=int, default=5, help="Top X items per location"
    )
    args = parser.parse_args()

    main(args.input_a, args.input_b, args.output, args.top_x)
