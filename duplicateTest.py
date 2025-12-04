def test_deduplication(spark):
    data = [
        (1, 101, 1001, "Car", 123456),
        (1, 101, 1001, "Car", 123456),  # duplicate detection_oid
        (1, 101, 1002, "Person", 123457),
    ]
    df = spark.createDataFrame(data, [
        "geographical_location_oid",
        "video_camera_oid",
        "detection_oid",
        "item_name",
        "timestamp_detected"
    ])

    dedup = df.dropDuplicates(["detection_oid"])
    assert dedup.count() == 2
