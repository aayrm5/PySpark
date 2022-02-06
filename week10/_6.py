# - This script executes reduceByKey()  on an rdd 
# - see how it is different from groupByKey() 
# - Which one is better?

from pyspark import SparkContext
from sys import stdin

sc = SparkContext("local[*]", "LogLevelCount")
sc.setLogLevel("INFO")

base_rdd = sc.textFile("/home/riz/Desktop/TrendyTech/week 9 - Spark1/bigLog.txt")
mapped_rdd = base_rdd.map(lambda x: (x.split(":")[0], 1))
reduced_rdd = mapped_rdd.reduceByKey(lambda x,y: x+y)
result = reduced_rdd.collect()

for x in result:
    print(x)

#holds the job so that we can inspect the SparkUI at localhost:4040
stdin.readline()