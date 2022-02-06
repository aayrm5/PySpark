from pyspark import SparkContext

sc = SparkContext("local[*]", "AverageNumFriends")

sc.setLogLevel("ERROR")

lines = sc.textFile("/home/riz/Desktop/TrendyTech/week 9 - Spark1/friendsdata-201008-180523.csv")

def parseLines(line):
    fields = line.split("::")
    age = int(fields[2])
    num_friends = int(fields[3])
    return (age, num_friends)

map_lines = (lines
.map(parseLines)                                    # (33,385)
.mapValues(lambda x: (x,1))                         # (33,(385,1))
.reduceByKey(lambda x,y: (x[0]+y[0],x[1]+y[1]))     # (33,(3000,5))
)

sort_avg_by_age = (map_lines
.mapValues(lambda x: x[0]/x[1])
.sortBy(lambda x: x[1], False)

)


for i in sort_avg_by_age.take(10):
    print(i)


#holds the job so that we can inspect the SparkUI at localhost:4040
# stdin.readline()