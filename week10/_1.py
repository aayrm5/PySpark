from threading import local
from pyspark import SparkContext

sc = SparkContext("local[*]", "KeywordAmount")
sc.setLogLevel("ERROR")

lines = sc.textFile("/home/riz/Desktop/TrendyTech/week 9 - Spark1/bigdatacampaigndata-201014-183159.csv")

def parse_lines(line):
    fields = line.split(",")
    amt = float(fields[10])
    word = fields[0]
    return (amt, word)

map_rdd = (lines.map(parse_lines)
    .flatMapValues(lambda x: x.split(" "))
    .map(lambda x: (x[1].lower(),x[0]))
    .reduceByKey(lambda x,y: (x+y))
    .sortBy(lambda x: x[1], ascending=False)
)

for i in map_rdd.take(10):
    print(i)

#holds the job so that we can inspect the SparkUI at localhost:4040
# stdin.readline()