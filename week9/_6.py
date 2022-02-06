from pyspark import SparkContext


def parseLines(line):
    fields = line.split("::")
    age = int(fields[2])
    numFriends = int(fields[3])

    return(age,numFriends)

sc = SparkContext("local[*]", "FriendsByAge")

lines = sc.textFile("/home/riz/Desktop/TrendyTech/week 9 - Spark1/friendsdata-201008-180523.csv")

map_rdd = (lines
    .map(parseLines)
    .mapValues(lambda x: (x,1))
    .reduceByKey(lambda x,y: (x[0]+y[0], x[1]+y[1]))
    .mapValues(lambda x: x[0]/x[1])
    .sortBy(lambda x: x[1], ascending=False)
)

for i in map_rdd.take(10):
    k,v = i
    print(f"{k}:{v}")

#holds the job so that we can inspect the SparkUI at localhost:4040
# stdin.readline()