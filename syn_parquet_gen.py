"""
Synthetic parquet generator for Dataset A and Dataset B.
Creates tiny sample datasets for local testing.

Usage:
    spark-submit generate_synthetic_parquet.py \
        --output_a /tmp/dataset_a \
        --output_b /tmp/dataset_b
"""

import argparse
from pyspark.sql import SparkSession


def main(output_a: str, output_b: str) -> None:
    spark = SparkSession.builder.appName("SyntheticParquetGenerator").getOrCreate()

    # --- Dataset A: detections ---
    data_a = [
        # geographical_location_oid, video_camera_oid, detection_oid, item_name, timestamp_detected
        (1, 101, 1001, "Car", 1700000000),
        (1, 101, 1002, "Person", 1700000001),
        (1, 101, 1003, "Car", 1700000002),
        (1, 102, 1004, "Dog", 1700000003),
        (2, 201, 2001, "Car", 1700000004),
        (2, 201, 2002, "Bicycle", 1700000005),
        (2, 202, 2003, "Car", 1700000006),
        (2, 202, 2003, "Car", 1700000006),  # duplicate detection_oid
        (3, 301, 3001, "Person", 1700000007),
        (3, 301, 3002, "Dog", 1700000008),
    ]

    df_a = spark.createDataFrame(
        data_a,
        schema=[
            "geographical_location_oid",
            "video_camera_oid",
            "detection_oid",
            "item_name",
            "timestamp_detected",
        ],
    )
    df_a.write.mode("overwrite").parquet(output_a)

    # --- Dataset B: location metadata ---
    data_b = [
        (1, "Central Park"),
        (2, "Main Street"),
        (3, "Town Square"),
    ]

    df_b = spark.createDataFrame(
        data_b,
        schema=["geographical_location_oid", "geographical_location"],
    )
    df_b.write.mode("overwrite").parquet(output_b)

    spark.stop()


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description
