# - This script executes groupByKey()  on an rdd 
# - see how it is different from reduceByKey() 
# - Which one is better?

from pyspark import SparkContext
from sys import stdin

sc = SparkContext("local[*]", "LogLevelCount")

# Set the log level to only print errorssc = SparkContext("local[*]", "LogLevelCount")
sc.setLogLevel("ERROR")

# Create a SparkContext using every core of the local machine
base_rdd = sc.textFile("/home/riz/Desktop/TrendyTech/week 9 - Spark1/bigLog.txt")

mapped_rdd = base_rdd.map(lambda x: (x.split(":")[0], x.split(":")[1]))

grouped_rdd = mapped_rdd.groupByKey()

final_rdd = grouped_rdd.map(lambda x: (x[0], len(x[1])))

for x in final_rdd.take(10):
    print(x)

#holds the job so that we can inspect the SparkUI at localhost:4040
stdin.readline()