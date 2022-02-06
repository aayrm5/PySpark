#using flatMap(), reduceByKey(), & sortBy() rather than sortByValues() in this file

from pyspark import SparkContext
from sys import stdin

if __name__=="__main__":

    #sc import & logging levels
    sc = SparkContext("local[*]", "wordcount_sortBy")
    sc.setLogLevel("ERROR")

    #reading the file
    input = sc.textFile("/home/riz/Desktop/TrendyTech/week 9 - Spark1/search_data.txt")

    #one input row will give multiple output rows
    words = input.flatMap(lambda x: x.split(" "))

    #assigning a digit one to each word created above
    assign_count = words.map(lambda x: (x,1))

    #combing the count of all the words using reduceByKey
    final_count = assign_count.reduceByKey(lambda x,y: (x+y))

    #sorting the count(val) (in a key-val pair) using sortBy
    sorted_count = final_count.sortBy(lambda x: x[1], False)

    for i in sorted_count.take(10):
        k,v = i
        print(f"{k} -:- {v}")

#holds the job so that we can inspect the SparkUI at localhost:4040
# stdin.readline()

