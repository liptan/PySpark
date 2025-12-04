import random

# --- Re-implemented: Count items per location with salting ---

# Add a random salt to skewed keys (e.g., location_oid=1)
def add_salt(row):
    location_oid = row["geographical_location_oid"]
    item_name = row["item_name"]
    # If this location is skewed, add salt
    if location_oid == 1:  # example skewed location
        salt = random.randint(0, 9)  # distribute into 10 buckets
        return ((location_oid, item_name, salt), 1)
    else:
        return ((location_oid, item_name, 0), 1)

# a: Map with salt
rdd_salted = rdd_a_dedup.map(add_salt)

# b: Reduce by salted key
rdd_partial_counts = rdd_salted.reduceByKey(lambda a, b: a + b)

# c: Remove salt and re-aggregate
rdd_counts = (
    rdd_partial_counts
    .map(lambda x: ((x[0][0], x[0][1]), x[1]))  # drop salt
    .reduceByKey(lambda a, b: a + b)
)
