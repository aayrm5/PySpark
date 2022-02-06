# Create DataFrame from a list of tuples.

from pyspark import SparkConf
from pyspark.sql import SparkSession
from pyspark.sql import functions as F
from sys import stdin
from pyspark.sql.types import DateType

my_conf = SparkConf()
my_conf.set("spark.app.name", "List_To_DataFrame")
my_conf.set("spark.master", "local[*]")

spark = (SparkSession
    .builder
    .appName("List_To_DataFrame")
    .config(conf=my_conf)
    .getOrCreate()
)

my_list = [
    (1,"2013-07-25",11599,"CLOSED"),
    (2,"2014-07-25",256,"PENDING_PAYMENT"),
    (3,"2013-07-25",11599,"COMPLETE"),
    (4,"2019-07-25",8827,"CLOSED")
]

col_names = ["order_id", "order_date", "customer_id", "status"]

list_df = (spark.createDataFrame(my_list)
    .toDF(*col_names)
)

new_df = (
    list_df
    .withColumn("order_date", F.col("order_date").cast(DateType()))
    .withColumn("new_id", F.monotonically_increasing_id())
    .drop_duplicates(["order_date", "customer_id"])
    .drop("order_id")
    .orderBy("order_date", ascending=False)
)

new_df.show(20, truncate=False)

#holds the job so that we can inspect the SparkUI at localhost:4040
print("HOLD THE DOOR!!,..!.. Hodor!!")
stdin.readline()

spark.stop()