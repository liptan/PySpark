from pyspark.sql import SparkSession


def main():
    """Compute top X items detected by video cameras in Utopia."""

    spark = SparkSession.builder.appName("TopItemsRDD").getOrCreate()

    # Load parquet into DataFrame
    df = spark.read.parquet("path/to/parquet_file1")

    # Convert to RDD
    rdd = df.rdd

    # Deduplicate by detection_oid
    dedup_rdd = (
        rdd.keyBy(lambda row: row["detection_oid"])
        .reduceByKey(lambda a, b: a)
        .map(lambda x: x[1])
    )

    # Map to (item_name, 1)
    item_counts_rdd = dedup_rdd.map(lambda row: (row["item_name"], 1))

    # Aggregate counts
    item_totals_rdd = item_counts_rdd.reduceByKey(lambda a, b: a + b)

    # Sort and take top X items
    top_x_items = item_totals_rdd.sortBy(
        lambda x: x[1], ascending=False
    ).take(10)

    for item, count in top_x_items:
        print(item, count)

    # Optional: breakdown by location
    location_item_counts_rdd = (
        dedup_rdd.map(
            lambda row: ((row["geographical_location_oid"], row["item_name"]), 1)
        )
        .reduceByKey(lambda a, b: a + b)
    )

    top_items_per_location = (
        location_item_counts_rdd.map(
            lambda x: (x[0][0], (x[0][1], x[1]))
        )
        .groupByKey()
        .mapValues(
            lambda items: sorted(items, key=lambda y: y[1], reverse=True)[:5]
        )
    )

    for loc, items in top_items_per_location.collect():
        print(f"Location {loc}: {items}")


if __name__ == "__main__":
    main()

