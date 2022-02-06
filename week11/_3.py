#cache & persist operations on rdd/DataFrame

from sys import stdin
from pyspark import SparkContext, StorageLevel

sc = SparkContext("local[*]", "W12_cache&persist")
sc.setLogLevel("ERROR")

base_rdd = sc.textFile("../week 9 - Spark1/customerorders-201008-180523.csv")

map_rdd = (base_rdd
    .map(lambda x: (x.split(",")[0],float(x.split(",")[2])))
    .reduceByKey(lambda x,y: (x+y))
    .filter(lambda x: x[1] > 5000)
    .map(lambda x: (x[0], x[1]*2))
    .sortBy(lambda x: x[1], ascending=False)
    .persist(StorageLevel.MEMORY_ONLY)
)

for i in map_rdd.take(10):
    print(i)

print(map_rdd.count())

#holds the job so that we can inspect the SparkUI at localhost:4040
stdin.readline()
