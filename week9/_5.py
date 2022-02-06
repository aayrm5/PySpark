from pyspark import SparkContext
sc = SparkContext("local[*]","movie-data")
sc.setLogLevel("ERROR")
lines = sc.textFile("/home/riz/Desktop/TrendyTech/week 9 - Spark1/moviedata-201008-180523.data")
ratings = lines.map(lambda x: (x.split("\t")[2],1))
result = ratings.reduceByKey(lambda x,y: x+y).collect()
for a in result:
    print(a)

#holds the job so that we can inspect the SparkUI at localhost:4040
# stdin.readline()