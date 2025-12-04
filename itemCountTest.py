def test_item_count(spark):
    data = [
        (1, "Car"),
        (1, "Car"),
        (1, "Person"),
    ]
    df = spark.createDataFrame(data, ["geographical_location_oid", "item_name"])
    counts = df.groupBy("geographical_location_oid", "item_name").count().collect()

    result = {row["item_name"]: row["count"] for row in counts}
    assert result["Car"] == 2
    assert result["Person"] == 1
