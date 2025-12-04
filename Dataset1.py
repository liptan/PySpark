# read from parquet file
df = spark.read.parquet("path/to/parquet_file")
# drop duplicate entries 
df_dedup = df.dropDuplicates(["detection_oid"])
item_counts = (
    df_dedup.groupBy("geographical_location_oid", "item_name")
    .count()
)
# creating the dataset view 
from pyspark.sql import Window
from pyspark.sql.functions import col, row_number

windowSpec = Window.partitionBy("geographical_location_oid").orderBy(col("count").desc())

ranked_items = item_counts.withColumn("rank", row_number().over(windowSpec))

X = 5  # for example, top 5 items
top_items = ranked_items.filter(col("rank") <= X)

# Show the items
top_items.show(truncate=False)


