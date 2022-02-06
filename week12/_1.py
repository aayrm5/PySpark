from pyspark import SparkConf
from pyspark.sql import SparkSession
from sys import stdin
from pyspark.sql import functions as F
from pyspark.sql.types import StructType, StructField, IntegerType, TimestampType, StringType

my_conf = SparkConf()
my_conf.set("spark.app.name", "DataFramesWriterAPI")
my_conf.set("spark.master", "local[*]")

spark = (SparkSession
    .builder
    .appName("DataFramesWriterAPI")
    .config(conf=my_conf)
    .getOrCreate()
)

# order_schema = StructType([
#     StructField("order_id", IntegerType()),
#     StructField("order_date", TimestampType()),
#     StructField("order_customer", IntegerType()),
#     StructField("order_status", StringType())
# ])

# order_df = (
#     spark.read
#     .option("header", True)
#     .schema(order_schema)
#     .csv("../week 9 - Spark1/orders-201019-002101.csv")
# )

# print(f"number of partitions:= {order_df.rdd.getNumPartitions()}")

# order_df = order_df.withColumn("order_OnlyDate", F.to_date(F.col("order_date"), 'yyyy-MM-dd'))

# order_rep = order_df.repartition(4)

# (order_rep.write
#     .mode("overwrite")
#     .parquet("../outputFolder/orders_parquet")
# )


order_parq_df = (
    spark.read
    .option("header", True)
    .parquet("../outputFolder/orders_parquet/")
)

print(order_parq_df.printSchema())

(order_parq_df
    .drop_duplicates(subset=["order_status","order_date"])
    .orderBy("order_date", ascending=False)
    .show(19, truncate=False)
    )

print(f"Number of records in the parquet file:= {order_parq_df.count()}")


#holds the job so that we can inspect the SparkUI at localhost:4040
print("HOLD THE DOOR!!,..!.. Hodor!!")
stdin.readline()

spark.stop()