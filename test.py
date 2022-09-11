from sys import stdin
from pyspark import SparkContext

# common lines
sc = SparkContext("local[*]", "wordcount")

lst = [
    ("101, Azar, Finance"),
    ("102, Riz, IT"),
    ("103, Yas, Engg")
]

base_rdd = sc.parallelize(lst)

map_rdd = base_rdd.map(lambda x: x.split(","))

flat_rdd = base_rdd.flatMap(lambda x: x.split(","))

map_res = map_rdd.count()
flat_res = flat_rdd.count()
print(map_res)
print("\n")
print(flat_res)
