#Creating User Defined Functions using `udf` built-in function in PySpark

from pyspark.sql import SparkSession
from pyspark import SparkConf
from sys import stdin
from pyspark.sql.functions import udf
from pyspark.sql.types import BooleanType

my_conf = (
    SparkConf()
    .set("spark.app.name", "Spark-UDF")
    .set("spark.master","local[*]")
)

spark = (
    SparkSession
    .builder
    .appName("Spark-UDF")
    .config(conf=my_conf)
    .getOrCreate()
)

df = (
    spark
    .read
    .option("inferSchema", True)
    .csv("../week 9 - Spark1/-201125-161348.dataset1")
    .toDF("name", "age", "city")
)

def adult_check(age):
    if age > 18:
        return True
    else:
        return False

# Creating a UDF called `parse_adult_func(arg:age->Int):->Boolean`
parse_adult_func = udf(adult_check, BooleanType())

df_new = df.withColumn("adult", parse_adult_func("age"))

df_new.printSchema()

df_new.show(20,truncate=False) 

#holds the job so that we can inspect the SparkUI at localhost:4040
print("HOLD THE DOOR!!,..!.. Hodor!!")
stdin.readline()

spark.stop()