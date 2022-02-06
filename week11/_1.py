#first file on spark dataframes


from pyspark import SparkConf
from pyspark.sql import SparkSession

my_conf = SparkConf()
my_conf.set("spark.app.name", "my_first_application")
my_conf.set("spark.master", "local[*]")

spark = (SparkSession
    .builder
    .config(conf=my_conf)
    .getOrCreate()
)

spark.sparkContext.setLogLevel("ERROR")

order_df = (spark.read
    .option("header", True)
    .option("inferSchema", True)
    .csv("/home/riz/Desktop/TrendyTech/week 9 - Spark1/orders-201025-223502.csv")
)

print(order_df.printSchema())

order_df.show(truncate=False)

#holds the job so that we can inspect the SparkUI at localhost:4040
stdin.readline()

spark.stop()