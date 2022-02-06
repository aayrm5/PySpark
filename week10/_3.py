#Accumulator example

from pyspark import SparkContext

sc = SparkContext("local[*]", "Accumulator Example")
sc.setLogLevel("ERROR")

lines_rdd = sc.textFile("/home/riz/Desktop/TrendyTech/week 9 - Spark1/samplefile.txt")

def blank_line_checker(line):
    
    if len(line) == 0:
        my_accum.add(1)
    return my_accum
    
my_accum = sc.accumulator(0.0)

lines_rdd.foreach(blank_line_checker)

print(f"Accumulator Value = {my_accum}")

#`foreach` could be called on an rdd but not on a collection, In Scala foreach is also called on a collection.

#holds the job so that we can inspect the SparkUI at localhost:4040
# stdin.readline()