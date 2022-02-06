#This program aims to remove boring words (through broadcasting) from the bigData-campaigndata

from pyspark import SparkContext

sc = SparkContext("local[*]", "RemoveBoringWords")
sc.setLogLevel("ERROR")

lines = sc.textFile("/home/riz/Desktop/TrendyTech/week 9 - Spark1/bigdatacampaigndata-201014-183159.csv")

def load_boring_words():
    return set(line.strip() for line in open("/home/riz/Desktop/TrendyTech/week 9 - Spark1/boringwords.txt"))

boring_words = sc.broadcast(load_boring_words())

map_rdd = (lines
    .map(lambda x: (float(x.split(",")[10]), x.split(",")[0]))      #(big data content, 55.23)
    .flatMapValues(lambda x: x.split(" "))                          #(55.23, BIG \n 55.23, Data \n 55.23, ConTeNt)
    .map(lambda x: (x[1].lower(), x[0]))                            #(big, 55.23 \n data, 55.23 \n content, 55.23)
    .filter(lambda x: x[0] not in boring_words.value)
    .reduceByKey(lambda x,y: (x+y))
    .sortBy(lambda x: x[1], ascending=False)
)

for i in map_rdd.take(10):
    print(i)

#holds the job so that we can inspect the SparkUI at localhost:4040
# stdin.readline()