# Creating UDF with `spark.udf.register()` functionality rather than in-built udf function.


from pyspark import SparkConf
from pyspark.sql import SparkSession
from sys import stdin
from pyspark.sql.types import BooleanType
from pyspark.sql.functions import expr

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

df = (
    spark
    .read
    .option("inferSchema", True)
    .csv("../week 9 - Spark1/-201125-161348.dataset1")
    .toDF("name", "age", "city")
)

def age_check(age):
    if age > 18:
        return True
    else:
        return False

#Registering the above created function as an UDF.
spark.udf.register("parse_age_func", age_check, BooleanType())

for x in spark.catalog.listFunctions():
    print(x)

df_new = df.withColumn("adult", expr("parse_age_func(age)"))

df_new.printSchema()

df_new.show(20,truncate=False) 

#holds the job so that we can inspect the SparkUI at localhost:4040
print("HOLD THE DOOR!!,..!.. Hodor!!")
stdin.readline()

spark.stop()
