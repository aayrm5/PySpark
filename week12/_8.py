# Displaying multiple ways of calling columns and performing Window Functions

from pyspark import SparkConf
from pyspark.sql import SparkSession, Window
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

col_names = ["country", "week_num", "quantity", "unit_price", "invoice_value"]

invoice_df = (
    spark
    .read
    .option("inferSchema", True)
    .option("header", True)
    .csv("../week 9 - Spark1/windowdata-201025-223502.csv")
).toDF(*col_names)

# Defining window to use in the over clause
my_window = (
    Window
    .partitionBy("country")
    .orderBy("week_num")
    .rowsBetween(Window.unboundedPreceding, Window.currentRow)
)

df_new = (
    invoice_df
    .withColumn("running_total", F.sum("invoice_value").over(my_window))
)

df_new.show(20, truncate=False)

# Column String Expression
(
    df_new
    .selectExpr(
        "count(*) as RowCount",
        "sum(quantity) as TotalQuantity",
        "avg(unit_price) as AvgPrice",
        "count(Distinct(invoice_value)) as CountDistinct"
    )

).show(20, truncate=False)

#holds the job so that we can inspect the SparkUI at localhost:4040
print("HOLD THE DOOR!!,..!.. Hodor!!")
stdin.readline()

spark.stop()
