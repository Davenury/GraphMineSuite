from pyspark.sql import Row
from pyspark.sql import SparkSession
from VectorSetRDD import VectorSetRDD
from VectorSetRoaring import VectorSetRoaring
from typing import List, Callable, Tuple
from k_clique import degeneracy_order, k_clique, k_clique_parallel, add_class
from time import time
from graph_loading import read_graph_from_path

spark = SparkSession.builder.getOrCreate()
sc = spark.sparkContext


# 0  -  1
# |  \
# 3  -  2
neighbours = [[3, 1, 2],
              [0],
              [1, 3, 0],
              [2, 0]]


graph_rdd_sets = []
graph_roaring_sets = []
for (idx, neighbours) in enumerate(neighbours):
    graph_rdd_sets.append((idx, VectorSetRDD.from_array(neighbours)))
    graph_roaring_sets.append((idx, VectorSetRoaring.from_array(neighbours)))


add_class(VectorSetRDD)
add_class(VectorSetRoaring)


def measure_time(function: Callable[[], int], class_name: str, function_name: str):
    start_time = time()
    result = function()
    end_time = time()

    print(f"{class_name} {function_name} \n result: {result} time: {end_time-start_time}")
    return (result, end_time-start_time)


def run_small_graph():
    measure_time(
        lambda: k_clique(graph_rdd_sets, 3, VectorSetRDD), "VectorSetRDD", "k_clique")
    measure_time(
        lambda: k_clique(graph_roaring_sets, 3, VectorSetRoaring), "VectorSetRoaring", "k_clique")
    measure_time(
        lambda: k_clique_parallel(graph_roaring_sets, 3, VectorSetRoaring), "VectorSetRoaring", "k_clique_parallel")


def run_twitter_graph():
    graph = read_graph_from_path("./twitter", VectorSetRDD)
    measure_time(
        lambda: k_clique(graph, 3, VectorSetRDD), "VectorSetRDD", "k_clique")
    graph = read_graph_from_path("./twitter", VectorSetRoaring)
    measure_time(
        lambda: k_clique(graph, 3, VectorSetRoaring), "VectorSetRoaring", "k_clique")
    measure_time(
        lambda: k_clique_parallel(graph, 3, VectorSetRoaring), "VectorSetRoaring", "k_clique_parallel")


run_twitter_graph()
