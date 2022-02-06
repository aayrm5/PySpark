#In this file, we'll be using regex to decode a multi-delimiter `orders_new-201019-002101.csv` file

from pyspark import SparkConf
from pyspark.sql import SparkSession
from pyspark.sql import functions as F
from sys import stdin
from pyspark.sql.types import StringType, IntegerType, DateType

my_conf = SparkConf()
my_conf.set("spark.app.name", "MultiDelimiter-RegexpParser")
my_conf.set("spark.master", "local[*]")

spark = (SparkSession
    .builder
    .appName("MultiDelimiter-RegexpParser")
    .config(conf=my_conf)
    .getOrCreate()
)

orders_new = (
    spark.read
    .text("../week 9 - Spark1/orders_new-201019-002101.csv")
)

my_regexp = r'(\S+) (\S+)\t(\S+)\,(\S+)'

orders_new.printSchema()
orders_new.show(20, truncate=False)

parsed_df = (
    orders_new
    .select(F.regexp_extract('value', my_regexp, 1).cast(IntegerType()).alias("order_id"),
            F.regexp_extract('value', my_regexp, 2).cast(DateType()).alias("order_date"),
            F.regexp_extract("value", my_regexp, 3).cast(IntegerType()).alias('customer_id'),
            F.regexp_extract("value", my_regexp, 4).cast(StringType()).alias('order_status')
    )
)

parsed_df.printSchema()
parsed_df.show(20, truncate=False)

(parsed_df.groupBy("order_id").agg(
    F.count("*").alias("order_count")
)
    .orderBy("order_count", ascending=False)
    .show(20, truncate=False)
)

#holds the job so that we can inspect the SparkUI at localhost:4040
print("HOLD THE DOOR!!,..!.. Hodor!!")
stdin.readline()

spark.stop()