"""
PySpark job to compute top X detected items per geographical location.

Usage:
    spark-submit top_items_job.py \
        --input_a /path/to/parquet_a \
        --input_b /path/to/parquet_b \
        --output /path/to/output \
        --top_x 10
"""

import argparse
from pyspark.sql import SparkSession
from pyspark.sql import Window
from pyspark.sql.functions import col, row_number


def main(input_a: str, input_b: str, output: str, top_x: int) -> None:
    """Main job to compute top X items per location."""
    spark = SparkSession.builder.appName("TopItemsJob").getOrCreate()

    # Load datasets
    df_a = spark.read.parquet(input_a)
    df_b = spark.read.parquet(input_b)

    # Deduplicate detection_oid
    df_a_dedup = df_a.dropDuplicates(["detection_oid"])

    # Count items per location
    item_counts = (
        df_a_dedup.groupBy("geographical_location_oid", "item_name")
        .count()
    )

    # Rank items per location
    window_spec = Window.partitionBy("geographical_location_oid").orderBy(
        col("count").desc()
    )
    ranked_items = item_counts.withColumn(
        "item_rank", row_number().over(window_spec)
    )

    # Filter top X
    top_items = ranked_items.filter(col("item_rank") <= top_x)

    # Join with location metadata (optional, if you want location name too)
    result = top_items.join(
        df_b, on="geographical_location_oid", how="left"
    ).select(
        "geographical_location_oid",
        "item_rank",
        "item_name",
        "geographical_location",
    )

    # Write output
    result.write.mode("overwrite").parquet(output)

    spark.stop()


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="Top X Items Job")
    parser.add_argument("--input_a", required=True, help="Path to Dataset A")
    parser.add_argument("--input_b", required=True, help="Path to Dataset B")
    parser.add_argument("--output", required=True, help="Output path")
    parser.add_argument(
        "--top_x", type=int, default=5, help="Top X items per location"
    )
    args = parser.parse_args()

    main(args.input_a, args.input_b, args.output, args.top_x)
