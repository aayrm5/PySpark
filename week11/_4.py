#finding top movies
# dataset ratings-201014-183159.dat

from pyspark import SparkContext
from sys import stdin
from pyspark.sql import functions as F

sc = SparkContext("local[*]", "DataFrame-Top Movies")
sc.setLogLevel("ERROR")

ratings_df = sc.textFile("../week 9 - Spark1/ratings-201014-183159.dat")


map_rdd = (ratings_df
    .map(lambda x: (x.split("::")[1],x.split("::")[2]))         #(movie_id, ratings)
    .mapValues(lambda x: (float(x[0]), 1.0))                    #(movie_id, (ratings, 1.0))
    .reduceByKey(lambda x,y: (x[0]+y[0], x[1]+y[1]))            #(movie_id, (sum_ratings, count_ratings))
    .filter(lambda x: x[1][1] > 100)                            # The movie should have atleast 100 ratings
    .mapValues(lambda x: x[0]/x[1])
    .filter(lambda x: x[1] > 4.5)
)

movies_rdd = sc.textFile("../week 9 - Spark1/movies-201019-002101.dat")
movies_map_rdd = (
    movies_rdd
    .map(lambda x: (x.split("::")[0],x.split("::")[1],x.split("::")[2]))
)

joined_rdd = (movies_map_rdd.join(map_rdd)
    .map(lambda x: x[1])
)


for i in joined_rdd.take(10):
    k,v=i
    print(f"{k} := {v}")
    # print(i)

#holds the job so that we can inspect the SparkUI at localhost:4040
stdin.readline()