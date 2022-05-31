from pyspark import SparkContext
from Set import Set
from VectorSetRDD import VectorSetRDD
from VectorSetRoaring import VectorSetRoaring
from typing import List, Callable, Tuple, Type
import k_clique_module
from k_clique_module import degeneracy_order, k_clique, k_clique_parallel
from time import time
from graph_loading import read_graph_from_path
from inspect import getfile
import sys
import boto3


# spark = SparkSession.builder.getOrCreate()
sc = SparkContext(appName="GraphProcessing")

s3 = boto3.resource('s3')


def load_spark_context():

    def add_class(set_class: Type[Set]):
        sc.addFile(getfile(set_class))

    add_class(Set)
    add_class(VectorSetRDD)
    add_class(VectorSetRoaring)
    sc.addFile(k_clique_module.__file__)
    VectorSetRDD.set_spark_context(sc)


load_spark_context()
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


def measure_time(function: Callable[[], int], class_name: str, function_name: str):
    start_time = time()
    result = function()
    end_time = time()

    object = s3.Object(
        'gms-us-east-1', f'results/{class_name}/{function_name}/result.txt')

    result = f"{class_name} {function_name} \n result: {result} time: {end_time-start_time}".encode(
        "ascii")
    object.put(Body=result)


def run_small_graph():
    measure_time(
        lambda: k_clique(graph_rdd_sets, 3, VectorSetRDD), "VectorSetRDD", "k_clique")
    measure_time(
        lambda: k_clique(graph_roaring_sets, 3, VectorSetRoaring), "VectorSetRoaring", "k_clique")
    measure_time(
        lambda: k_clique_parallel(graph_roaring_sets, 3, VectorSetRoaring, sc), "VectorSetRoaring", "k_clique_parallel")


def run_twitter_graph():
    graph = read_graph_from_path("./twitter", VectorSetRDD)
    measure_time(
        lambda: k_clique(graph, 3, VectorSetRDD), "VectorSetRDD", "k_clique")
    graph = read_graph_from_path("./twitter", VectorSetRoaring)
    measure_time(
        lambda: k_clique(graph, 3, VectorSetRoaring), "VectorSetRoaring", "k_clique")
    measure_time(
        lambda: k_clique_parallel(graph, 3, VectorSetRoaring, sc), "VectorSetRoaring", "k_clique_parallel")


# run_small_graph()

# run_twitter_graph()


if __name__ == "__main__":
    if len(sys.argv) != 3:
        print("Usage: graph processing  ", file=sys.stderr)
        exit(-1)
    graph_folder = sys.argv[1]
    run_twitter_graph()
    sc.stop()
