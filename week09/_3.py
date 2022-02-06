#used map & countByValue and used sorted() in this file.

from pyspark import SparkContext
from sys import stdin

if __name__ == "__main__":
    
    # common lines
    sc = SparkContext("local[*]", "wordcount")
    sc.setLogLevel("ERROR")
    
    input = sc.textFile("/home/riz/Desktop/TrendyTech/week 9 - Spark1/search_data.txt")
    
    # one input row will give multiple output rows
    words = input.flatMap(lambda x: x.split(" "))
    
    # one input row will be giving one output row
    word_counts= words.map(lambda x: (x.lower()))

    #Using `countByValue()` rather than `reduceByKey() to derive the count of each word in the file`
    final_count = word_counts.countByValue()
    

    # `final_count` is of type collections.default_dict;
    # using sorted on its items will return a list of tuples as k-v pairs
    for i in sorted(final_count.items(), key=lambda x: x[1], reverse=True)[:10]:
        print(i)

else:
    print("Not executed directly")

#holds the job so that we can inspect the SparkUI at localhost:4040
# stdin.readline()