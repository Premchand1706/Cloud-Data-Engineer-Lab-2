
from pyspark.sql import SparkSession
from pyspark.sql.functions import col, avg, count

spark = SparkSession.builder \
    .appName("EmployeeData") \
    .master("local[*]") \
    .getOrCreate()

data = [
    (1, "Prem", "IT", 75000, "2024-01-10"),
    (2, "Rahul", "HR", 45000, "2023-05-12"),
    (3, "Priya", "IT", 85000, "2022-03-15"),
    (4, "Anil", "Sales", 55000, "2024-07-20"),
    (5, "Sneha", "HR", 62000, "2021-09-11"),
    (6, "Arjun", "IT", 95000, "2020-02-18"),
    (7, "Kiran", "Sales", 70000, "2023-11-25"),
    (8, "Divya", "Finance", 65000, "2022-06-30"),
    (9, "Manoj", "IT", 58000, "2024-04-05"),
    (10, "Neha", "HR", 72000, "2020-12-01"),
    (11, "Vijay", "Finance", 80000, "2021-08-16"),
    (12, "Pooja", "Sales", 48000, "2023-02-14"),
    (13, "Ravi", "IT", 68000, "2022-10-10"),
    (14, "Meena", "Finance", 90000, "2020-07-22"),
    (15, "Ajay", "Sales", 61000, "2024-05-19")
]

columns = ["EmployeeID", "EmployeeName", "Department",
           "Salary", "JoiningDate"]

df = spark.createDataFrame(data, columns)
df = df.withColumn("JoiningDate", col("JoiningDate").cast("date"))

print("\n1. SCHEMA")
df.printSchema()

print("\n2. FIRST FIVE EMPLOYEES")
df.show(5)

print("\n3. SALARY GREATER THAN 60000")
df.filter(col("Salary") > 60000).show()

print("\n4. ANNUAL SALARY")
df = df.withColumn("AnnualSalary", col("Salary") * 12)
df.show()

print("\n5. DEPARTMENT-WISE AVERAGE SALARY AND COUNT")
df.groupBy("Department").agg(
    avg("Salary").alias("AverageSalary"),
    count("*").alias("EmployeeCount")
).show()

print("\n6. TOP FIVE HIGHEST-PAID EMPLOYEES")
df.orderBy(col("Salary").desc()).show(5)

spark.stop()
