#SPARK SQL

from pyspark import SparkConf
from pyspark.sql import SparkSession
from sys import stdin
from pyspark.sql.types import StructType, StructField, StringType, IntegerType, TimestampType

my_conf = SparkConf()
my_conf.set("spark.app.name", "SPARK SQL")
my_conf.set("spark.master", "local[*]")

spark = (SparkSession
    .builder
    .appName("SPARK SQL")
    .config(conf=my_conf)
    .getOrCreate()
)

order_schema = StructType([
    StructField("order_id", IntegerType()),
    StructField("order_date", TimestampType()),
    StructField("order_customer", IntegerType()),
    StructField("order_status", StringType())
])

order_df = (spark.read
    .option("header", True)
    .csv("../week 9 - Spark1/orders-201019-002101.csv")
)

order_df.createOrReplaceTempView("orders")

result_df = spark.sql("""
SELECT order_status, count(*) as total_orders
FROM orders
GROUP BY order_status
""")

result_df.show(25, truncate=False)

#holds the job so that we can inspect the SparkUI at localhost:4040
print("HOLD THE DOOR!!,..!.. Hodor!!")
stdin.readline()

spark.stop()