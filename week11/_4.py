#finding top movies
# dataset ratings-201014-183159.dat

from pyspark import SparkConf
from pyspark.sql import SparkSession
from sys import stdin
from pyspark.sql import functions as F

my_conf = SparkConf()
my_conf.set("spark.app.name", "TopMovies")
my_conf.set("spark.master", "local[*]")

spark = (SparkSession
    .builder
    .appName("TopMovies")
    .config(my_conf)
    .getOrCreate()
)

ratings_df = (spark.SparkContext
    .textFile("../week 9 - Spark1/ratings-201014-183159.dat")
)

