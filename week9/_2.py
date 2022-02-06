#used reduceByKey & SortByKey in this file, included the code in if __name__==__main__


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
    word_counts= words.map(lambda x: (x, 1))
    
    final_count_rev = (word_counts
    .reduceByKey(lambda x, y: x + y)
    .map(lambda x: (x[1],x[0]))
    )
    
    result = (final_count_rev
    .sortByKey(False)
    .map(lambda x: (x[1],x[0]))
    )
    
    for a in result.take(10):
        print(a)
else:
    print("Not executed directly")

#holds the job so that we can inspect the SparkUI at localhost:4040
# stdin.readline()