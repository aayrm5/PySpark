# Perform operations using Aggregate functions
# Displaying multiple ways of calling columns

from ast import alias
from pyspark import SparkConf
from pyspark.sql import SparkSession
from sys import stdin
from pyspark.sql.types import BooleanType
from pyspark.sql import functions as F

my_conf = (
    SparkConf()
    .set("spark.app.name", "UDF-2")
    .set("spark.master", "local[*]")
)

spark = (
    SparkSession
    .builder
    .appName("UDF-2")
    .config(conf=my_conf)
    .getOrCreate()
)

invoice_df = (
    spark
    .read
    .option("inferSchema", True)
    .option("header", True)
    .csv("../week 9 - Spark1/order_data-201025-223502.csv")
)

# Column Object Expression
(invoice_df
    .select(
        F.count("*").alias("row_count"),
        F.sum("Quantity").alias("total_quantity"),
        F.avg("UnitPrice").alias("avg_price"),
        F.count_distinct("invoiceNo").alias("count_distinct_invoiceNo")
    )
).show(20, truncate=False)


# Column String Expression
(invoice_df
    .selectExpr(
        "count(*) as row_count",
        "sum(Quantity) as total_quantity",
        "avg(UnitPrice) as avg_price",
        "count(Distinct(invoiceNo)) as count_distinct"
    )
).show(20, truncate=False)

#Spark-SQL
(invoice_df
    .createOrReplaceTempView("sales")
)

spark.sql("""
    SELECT 
        COUNT(*) as count_recs,
        SUM(Quantity) as sum_quantity,
        AVG(UnitPrice) as avg_price,
        COUNT(DISTINCT(invoiceNo)) as count_distinct_invoice
    FROM sales
""").show(20, truncate=False)

#holds the job so that we can inspect the SparkUI at localhost:4040
print("HOLD THE DOOR!!,..!.. Hodor!!")
stdin.readline()

spark.stop()
