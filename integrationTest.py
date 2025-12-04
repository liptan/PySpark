import os
import tempfile
from top_items_job import main

def test_integration(spark):
    # Create temporary input/output paths
    tmpdir = tempfile.mkdtemp()
    input_a = os.path.join(tmpdir, "dataset_a")
    input_b = os.path.join(tmpdir, "dataset_b")
    output = os.path.join(tmpdir, "output")

    # Dataset A (detections)
    data_a = [
        (1, 101, 1001, "Car", 123456),
        (1, 101, 1002, "Person", 123457),
        (1, 101, 1003, "Car", 123458),
        (2, 201, 2001, "Dog", 123459),
    ]
    df_a = spark.createDataFrame(data_a, [
        "geographical_location_oid",
        "video_camera_oid",
        "detection_oid",
        "item_name",
        "timestamp_detected"
    ])
    df_a.write.mode("overwrite").parquet(input_a)

    # Dataset B (locations)
    data_b = [
        (1, "Central Park"),
        (2, "Main Street"),
    ]
    df_b = spark.createDataFrame(data_b, [
        "geographical_location_oid",
        "geographical_location"
    ])
    df_b.write.mode("overwrite").parquet(input_b)

    # Run job
    main(input_a, input_b, output, top_x=2)

    # Validate output
    result = spark.read.parquet(output).collect()
    assert any(row.item_name == "Car" and row.item_rank == 1 for row in result)
    assert any(row.item_name == "Person" and row.item_rank == 2 for row in result)
    assert any(row.geographical_location == "Main Street" for row in result)
