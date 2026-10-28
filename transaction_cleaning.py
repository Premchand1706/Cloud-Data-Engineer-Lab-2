
from pyspark.sql import SparkSession
from pyspark.sql.functions import col, try_to_timestamp, sum as spark_sum, to_date, lit

spark = SparkSession.builder \
    .appName("TransactionCleaning") \
    .master("local[*]") \
    .getOrCreate()

spark.sparkContext.setLogLevel("ERROR")

df = spark.read.csv(
    "transactions.csv",
    header=True,
    inferSchema=True
)

print("\n ORIGINAL DATA ")
df.show(truncate=False)

print("\n DATA TYPES ")
df.printSchema()

df = df.dropDuplicates(["TransactionID"])

df = df.dropna(
    subset=[
        "TransactionID",
        "CustomerID",
        "TransactionDate",
        "Product",
        "Quantity",
        "UnitPrice"
    ]
)

df = df.withColumn(
    "TransactionDate",
    to_date(
        try_to_timestamp(
            col("TransactionDate"),
            lit("yyyy-MM-dd")
        )
    )
)

df = df.filter(col("TransactionDate").isNotNull())

df = df.filter(
    (col("Quantity") > 0) &
    (col("UnitPrice") >= 0)
)

df = df.withColumn(
    "Revenue",
    col("Quantity") * col("UnitPrice")
)

print("\n CLEANED DATA ")
df.show(truncate=False)

print("\n TOTAL REVENUE BY PRODUCT ")

revenue_df = df.groupBy("Product").agg(
    spark_sum("Revenue").alias("TotalRevenue")
)

revenue_df = revenue_df.orderBy(
    col("TotalRevenue").desc()
)

revenue_df.show(truncate=False)

print("\n TOTAL REVENUE ")

df.select(
    spark_sum("Revenue").alias("TotalRevenue")
).show()

print("\n TOTAL VALID TRANSACTIONS ")
print("Total Transactions:", df.count())

spark.stop()
