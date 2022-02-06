#used reduceByKey & SortByKey in this file and removed empty space count.

from sys import stdin
from pyspark import SparkContext

# common lines
sc = SparkContext("local[*]", "wordcount")
input = sc.textFile("/home/riz/Desktop/TrendyTech/week 9 - Spark1/samplefile.txt")

# one input row will give multiple output rows
words = input.flatMap(lambda x: x.split(" "))

# one input row will be giving one output row
word_counts= words.map(lambda x: (x, 1))

#reducing the word_counts and reversing the order of key-value 
# (value is first, key is last now => (val,key))
final_count = (word_counts
.reduceByKey(lambda x, y: x + y)
.map(lambda x: (x[1], x[0]))
)

#Sorted the value and reversing the order back to key-value pair
sort_final_count = (final_count
.sortByKey(False)
.map(lambda x: (x[1],x[0]))
)

#filtering out empty space count
filter_sort_final_count = sort_final_count.filter(lambda x: x[0] != '')

result = filter_sort_final_count.collect()
for a in result:
    print(a)

#holds the job so that we can inspect the SparkUI at localhost:4040
stdin.readline()