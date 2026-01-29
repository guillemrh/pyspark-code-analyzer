import { CodeExample } from './types';

export const CODE_EXAMPLES: CodeExample[] = [
  {
    name: 'ETL Pipeline',
    description: 'Basic ETL with joins and aggregations',
    code: `from pyspark.sql import SparkSession
from pyspark.sql.functions import col, sum, avg

spark = SparkSession.builder.appName("ETL").getOrCreate()

# Read data sources
orders = spark.read.parquet("s3://data/orders/")
customers = spark.read.parquet("s3://data/customers/")

# Filter and transform
active_orders = orders.filter(col("status") == "active")
filtered_customers = customers.filter(col("country") == "US")

# Join datasets
joined = active_orders.join(
    filtered_customers,
    on="customer_id",
    how="inner"
)

# Aggregate results
summary = joined.groupBy("region") \\
    .agg(
        sum("amount").alias("total_amount"),
        avg("amount").alias("avg_amount")
    )

# Write output
summary.write.mode("overwrite").parquet("s3://output/summary/")`,
  },
  {
    name: 'Window Functions',
    description: 'Ranking and running totals',
    code: `from pyspark.sql import SparkSession
from pyspark.sql.functions import col, row_number, sum
from pyspark.sql.window import Window

spark = SparkSession.builder.appName("WindowDemo").getOrCreate()

sales = spark.read.parquet("s3://data/sales/")

# Define window specifications
region_window = Window.partitionBy("region").orderBy(col("amount").desc())
running_window = Window.partitionBy("region").orderBy("date").rowsBetween(Window.unboundedPreceding, 0)

# Apply window functions
ranked = sales \\
    .withColumn("rank", row_number().over(region_window)) \\
    .withColumn("running_total", sum("amount").over(running_window))

# Get top 10 per region
top_sales = ranked.filter(col("rank") <= 10)

top_sales.write.mode("overwrite").parquet("s3://output/top_sales/")`,
  },
  {
    name: 'Anti-Pattern Demo',
    description: 'Code with common performance issues',
    code: `from pyspark.sql import SparkSession
from pyspark.sql.functions import col, udf
from pyspark.sql.types import StringType

spark = SparkSession.builder.appName("AntiPatterns").getOrCreate()

# Read data
df = spark.read.parquet("s3://data/large_dataset/")

# Anti-pattern: UDF instead of native function
@udf(StringType())
def uppercase_name(name):
    return name.upper() if name else None

df_with_upper = df.withColumn("upper_name", uppercase_name(col("name")))

# Anti-pattern: Multiple actions on same DataFrame
count1 = df_with_upper.count()
count2 = df_with_upper.filter(col("active") == True).count()

# Anti-pattern: collect() on potentially large data
all_data = df_with_upper.collect()

# Anti-pattern: Early shuffle before filter
shuffled = df.repartition(100)
filtered = shuffled.filter(col("status") == "active")

filtered.show()`,
  },
  {
    name: 'Multi-Source Join',
    description: 'Complex joins with multiple tables',
    code: `from pyspark.sql import SparkSession
from pyspark.sql.functions import col, broadcast

spark = SparkSession.builder.appName("MultiJoin").getOrCreate()

# Read multiple data sources
users = spark.read.parquet("s3://data/users/")
orders = spark.read.parquet("s3://data/orders/")
products = spark.read.parquet("s3://data/products/")
categories = spark.read.table("catalog.categories")

# Filter before joins
active_users = users.filter(col("is_active") == True)
recent_orders = orders.filter(col("order_date") >= "2024-01-01")

# Join with broadcast hint for small table
enriched_orders = recent_orders \\
    .join(active_users, on="user_id", how="inner") \\
    .join(products, on="product_id", how="left") \\
    .join(broadcast(categories), on="category_id", how="left")

# Select relevant columns
result = enriched_orders.select(
    "order_id",
    "user_name",
    "product_name",
    "category_name",
    "amount"
)

result.write.mode("overwrite").parquet("s3://output/enriched_orders/")`,
  },
  {
    name: 'Streaming Simulation',
    description: 'Batch code simulating streaming patterns',
    code: `from pyspark.sql import SparkSession
from pyspark.sql.functions import col, window, count, avg, max

spark = SparkSession.builder.appName("StreamSim").getOrCreate()

# Read event data
events = spark.read.json("s3://data/events/")

# Parse timestamp and filter
parsed = events \\
    .withColumn("event_time", col("timestamp").cast("timestamp")) \\
    .filter(col("event_type").isin(["click", "view", "purchase"]))

# Window aggregation (simulating streaming windows)
windowed = parsed \\
    .groupBy(
        window("event_time", "5 minutes"),
        "user_id",
        "event_type"
    ) \\
    .agg(
        count("*").alias("event_count"),
        avg("value").alias("avg_value"),
        max("value").alias("max_value")
    )

# Flatten window column
flattened = windowed \\
    .withColumn("window_start", col("window.start")) \\
    .withColumn("window_end", col("window.end")) \\
    .drop("window")

flattened.write.mode("append").parquet("s3://output/windowed_events/")`,
  },
];
