from pyspark import SparkContext
sc = SparkContext("local[*]", "logLevelCount")
sc.setLogLevel("ERROR")

if __name__ == "__main__":
    my_list = ["WARN: Tuesday 4 September 0405",
    "ERROR: Tuesday 4 September 0408",
    "ERROR: Tuesday 4 September 0408",
    "ERROR: Tuesday 4 September 0408",
    "ERROR: Tuesday 4 September 0408",
    "ERROR: Tuesday 4 September 0408"]
    original_logs_rdd = sc.parallelize(my_list)
else:
    original_logs_rdd = sc.textFile("/home/riz/Desktop/TrendyTech/week 9 - Spark1/logsample.txt")
    print("inside the else part")

new_pair_rdd = original_logs_rdd.map(lambda x:(x.split(":")[0],1))
resultant_rdd = new_pair_rdd.reduceByKey(lambda x,y: x+y)
result = resultant_rdd.collect()

for x in result:
    print(x)

#holds the job so that we can inspect the SparkUI at localhost:4040
# stdin.readline()