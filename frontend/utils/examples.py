"""Example PySpark code snippets for the code editor."""

EXAMPLES = {
    "Simple ETL Pipeline": '''df = spark.read.parquet("data/sales.parquet")
filtered = df.filter(col("amount") > 100)
result = filtered.groupBy("category").agg(sum("amount"))
result.write.parquet("output/summary")''',
    "Join with Shuffle": '''orders = spark.read.parquet("orders.parquet")
customers = spark.read.parquet("customers.parquet")
joined = orders.join(customers, "customer_id")
joined.show()''',
    "Nested Expressions": '''df1 = spark.read.parquet("orders.parquet")
df2 = spark.read.parquet("customers.parquet")
result = df1.join(df2.filter(col("active") == True), on="customer_id")
result.show()''',
    "Variable Aliasing": '''df = spark.read.parquet("data.parquet")
backup = df
other = backup
result = other.filter(col("x") > 10)
result.show()''',
    "Multiple Actions (Anti-pattern)": '''df = spark.read.parquet("data.parquet")
df.count()
df.show()
df.write.parquet("output")''',
    "Cartesian Join (Anti-pattern)": '''df1 = spark.read.parquet("big.parquet")
df2 = spark.read.parquet("other.parquet")
cartesian = df1.crossJoin(df2)
cartesian.show()''',
    "Missing Unpersist (Anti-pattern)": '''df = spark.read.parquet("data.parquet")
cached = df.cache()
result1 = cached.filter(col("x") > 10).count()
result2 = cached.groupBy("y").count().show()''',
    "Chained Transformations": '''df = spark.read.csv("input.csv", header=True)
result = (df
    .filter(col("status") == "active")
    .select("id", "name", "value")
    .withColumn("doubled", col("value") * 2)
    .orderBy("name"))
result.show()''',
    "Window Functions": '''from pyspark.sql.window import Window

df = spark.read.parquet("transactions.parquet")
window = Window.partitionBy("customer_id").orderBy("date")
ranked = df.withColumn("rank", row_number().over(window))
ranked.filter(col("rank") == 1).show()''',
    "Aggregation Pipeline": '''sales = spark.read.parquet("sales.parquet")
summary = (sales
    .groupBy("region", "product")
    .agg(
        sum("revenue").alias("total_revenue"),
        count("*").alias("num_transactions"),
        avg("quantity").alias("avg_quantity")
    )
    .filter(col("total_revenue") > 10000))
summary.write.parquet("summary_output")''',
}

# Default example to show on startup
DEFAULT_EXAMPLE = "Simple ETL Pipeline"
