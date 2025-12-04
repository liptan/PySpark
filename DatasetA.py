from pyspark.sql import SparkSession

spark = SparkSession.builder.appName("TopItemsRDD").getOrCreate()

# Load parquet into DataFrame
df = spark.read.parquet("path/to/parquet_file1")

# Convert to RDD
rdd = df.rdd

# Key by detection_oid
dedup_rdd = rdd.keyBy(lambda row: row["detection_oid"]) \
               .reduceByKey(lambda a, b: a) \
               .map(lambda x: x[1])   # keep the row object
# Count items
item_counts_rdd = dedup_rdd.map(lambda row: (row["item_name"], 1))

# To sum toptal counts per item
item_totals_rdd = item_counts_rdd.reduceByKey(lambda a, b: a + b)

# Sort by count descending and take top X (e.g. 10)
top_x_items = item_totals_rdd.sortBy(lambda x: x[1], ascending=False).take(10)

for item, count in top_x_items:
    print(item, count)

# Breakdown by Location
location_item_counts_rdd = dedup_rdd.map(
    lambda row: ((row["geographical_location_oid"], row["item_name"]), 1)
).reduceByKey(lambda a, b: a + b)

# Example: top 5 items per location
top_items_per_location = location_item_counts_rdd \
    .map(lambda x: (x[0][0], (x[0][1], x[1]))) \
    .groupByKey() \
    .mapValues(lambda items: sorted(items, key=lambda y: y[1], reverse=True)[:5])

for loc, items in top_items_per_location.collect():
    print(f"Location {loc}: {items}")


