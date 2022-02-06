# DataFrames operations

from sys import stdin
from pyspark.sql import SparkSession
from pyspark import SparkConf
from pyspark.sql import functions as F

my_conf = SparkConf()
my_conf.set("spark.app.name", "DataFrames")
my_conf.set("spark.master", "local[*]")

spark = (SparkSession
    .builder
    .appName("DataFrames")
    .config(conf=my_conf)
    .getOrCreate()
)

order_df = (spark.read
    .option("header", True)
    .option("inferSchema", True)
    .csv("../week 9 - Spark1/orders-201019-002101.csv")
)

print(order_df.printSchema())

grouped_order_df = (
    order_df
    .repartition(4)
    .where("order_customer_id > 10000")
    .select("order_id", "order_customer_id")
    .groupBy("order_customer_id")
    .agg(
        F.count("*").alias("count_order_customer_id")
    )
)

grouped_order_df.show()

spark.sparkContext.setLogLevel("ERROR")

#holds the job so that we can inspect the SparkUI at localhost:4040
stdin.readline()

spark.stop()