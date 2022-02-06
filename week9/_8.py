from pyspark import SparkContext

sc = SparkContext("local[*]", "AverageNumFriends")

sc.setLogLevel("ERROR")

lines = sc.textFile("/home/riz/Desktop/TrendyTech/week 9 - Spark1/friendsdata-201008-180523.csv")

def parse_lines(line):
    fields = line.split("::")
    age = int(fields[2])
    num_friends = int(fields[3])
    return (age, num_friends)

map_rdd = (lines.map(parse_lines)
    .mapValues(lambda x: (x,1))
    .reduceByKey(lambda x,y: (x[0]+x[1], y[0]+y[1]))
    .mapValues(lambda x: x[0]/x[1])
    .sortBy(lambda x: x[1], ascending=False)
)

for i in map_rdd.take(10):
    print(i)

#holds the job so that we can inspect the SparkUI at localhost:4040
# stdin.readline()