from pyspark.sql import Window
from pyspark.sql.functions import col, row_number

def test_ranking(spark):
    data = [
        (1, "Car", 5),
        (1, "Person", 3),
        (1, "Dog", 2),
    ]
    df = spark.createDataFrame(data, ["geographical_location_oid", "item_name", "count"])

    window_spec = Window.partitionBy("geographical_location_oid").orderBy(col("count").desc())
    ranked = df.withColumn("item_rank", row_number().over(window_spec)).collect()

    ranks = {row["item_name"]: row["item_rank"] for row in ranked}
    assert ranks["Car"] == 1
    assert ranks["Person"] == 2
    assert ranks["Dog"] == 3
