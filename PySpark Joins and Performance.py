

from pyspark.sql import SparkSession
from pyspark.sql.functions import col, sum as spark_sum, broadcast

spark = SparkSession.builder \
    .appName("CustomerOrdersAnalysis") \
    .master("local[*]") \
    .getOrCreate()

spark.sparkContext.setLogLevel("ERROR")

customers_df = spark.read.csv(
    "customers.csv",
    header=True,
    inferSchema=True
)

orders_df = spark.read.csv(
    "orders.csv",
    header=True,
    inferSchema=True
)


print("\n CUSTOMERS DATA")
customers_df.show(truncate=False)


print("\n ORDERS DATA ")
orders_df.show(truncate=False)


print("\n ORDERS WITH CUSTOMER DETAILS ")

joined_df = customers_df.join(
    orders_df,
    on="CustomerID",
    how="inner"
)

joined_df.show(truncate=False)


print("\n CUSTOMERS WITHOUT ORDERS ")

customers_without_orders = customers_df.join(
    orders_df,
    on="CustomerID",
    how="left_anti"
)

customers_without_orders.show(truncate=False)

print("\n TOTAL ORDER VALUE BY CITY ")

city_revenue_df = joined_df.groupBy("City").agg(
    spark_sum("OrderAmount").alias("TotalOrderValue")
)

city_revenue_df = city_revenue_df.orderBy(
    col("TotalOrderValue").desc()
)

city_revenue_df.show(truncate=False)


print("\n BROADCAST JOIN EXAMPLE ")

broadcast_join_df = customers_df.join(
    broadcast(orders_df),
    on="CustomerID",
    how="inner"
)

broadcast_join_df.show(truncate=False)


large_orders_df = orders_df.filter(
    col("OrderAmount") > 1000
)


print("\n ORDERS ABOVE 1000 ")
large_orders_df.show(truncate=False)

spark.stop()

