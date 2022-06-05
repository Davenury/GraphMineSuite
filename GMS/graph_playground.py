import boto3
import sys
from inspect import getfile
from graph_loading import read_graph_from_path
from time import time
from k_clique_module import degeneracy_order, k_clique, k_clique_parallel
import k_clique_module
from typing import List, Callable, Tuple, Type
from VectorSetRoaring import VectorSetRoaring
from VectorSetRDD import VectorSetRDD
from VectorSetDataFrame import VectorSetDataFrame
from Set import Set
from pyspark import SparkContext
from pyspark.sql import SparkSession
import logging
from pyroaring import BitMap

logging.basicConfig(encoding='utf-8', level=logging.WARNING)

spark = SparkSession.builder.getOrCreate()
sc = spark.sparkContext

s3 = boto3.resource('s3')


def load_spark_context():

    def add_class(set_class: Type[Set]):
        sc.addFile(getfile(set_class))

    add_class(Set)
    add_class(VectorSetRDD)
    add_class(VectorSetRoaring)
    add_class(VectorSetDataFrame)
    add_class(BitMap)
    sc.addFile(k_clique_module.__file__)
    VectorSetRDD.set_spark_context(sc)
    VectorSetDataFrame.set_spark_session(spark)


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
graph_dataframe_sets = []
for (idx, neighbours) in enumerate(neighbours):
    graph_rdd_sets.append((idx, VectorSetRDD.from_array(neighbours)))
    graph_roaring_sets.append((idx, VectorSetRoaring.from_array(neighbours)))
    graph_dataframe_sets.append(
        (idx, VectorSetDataFrame.from_array(neighbours)))


def measure_time(function: Callable[[], int], class_name: str, function_name: str):
    start_time = time()
    result = function()
    end_time = time()

    result = f"{class_name} {function_name} \n result: {result} time: {end_time-start_time}"
    print(result)


def run_small_graph(k):
    measure_time(
        lambda: k_clique(graph_roaring_sets, k, VectorSetRoaring), "VectorSetRoaring", "k_clique")
    measure_time(
        lambda: k_clique_parallel(graph_roaring_sets, k, VectorSetRoaring, sc), "VectorSetRoaring", "k_clique_parallel")
    measure_time(
        lambda: k_clique(graph_rdd_sets, k, VectorSetRDD), "VectorSetRDD", "k_clique")
    measure_time(
        lambda: k_clique(graph_dataframe_sets, k, VectorSetDataFrame), "VectorSetDataframe", "k_clique")
    # measure_time(
    #     lambda: k_clique_parallel(graph_dataframe_sets, k, VectorSetDataFrame, sc), "VectorSetDataframe", "k_clique_parallel")


def run_twitter_graph(k):

    graph = read_graph_from_path("./twitter", VectorSetRoaring)
    measure_time(
        lambda: k_clique(graph, k, VectorSetRoaring), "VectorSetRoaring", "k_clique")
    measure_time(
        lambda: k_clique_parallel(graph, k, VectorSetRoaring, sc), "VectorSetRoaring", "k_clique_parallel")

    graph = read_graph_from_path("./twitter", VectorSetRDD)
    measure_time(
        lambda: k_clique(graph, k, VectorSetRDD), "VectorSetRDD", "k_clique")
    graph = read_graph_from_path("./twitter", VectorSetDataFrame)
    measure_time(
        lambda: k_clique(graph, k, VectorSetDataFrame), "VectorSetDataframe", "k_clique")


# run_small_graph(3)

run_twitter_graph(3)
