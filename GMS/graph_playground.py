import boto3
from inspect import getfile
from graph_loading import read_graph_from_path, read_graph_frame_from_path
from time import time
from k_clique_module import k_clique, k_clique_parallel, k_clique_graph_frame
import k_clique_module
from typing import List, Callable, Tuple, Type
from sets.vector_set_roaring import VectorSetRoaring
from sets.vector_set_rdd import VectorSetRDD
from sets.vector_set_dataframe import VectorSetDataFrame
from sets.abstract_set import Set
from pyspark.sql import SparkSession, SQLContext
from graphframes import GraphFrame
import logging
from pyroaring import BitMap
import os
os.environ['PYSPARK_SUBMIT_ARGS'] = '--packages graphframes:graphframes:0.6.0-spark2.3-s_2.11 pyspark-shell'

logging.basicConfig(level=logging.WARNING)

spark = SparkSession.builder.getOrCreate()
sc = spark.sparkContext
sql_context = SQLContext(sc)

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


def build_graph_frame(edges: List[List[int]]) -> GraphFrame:
    vertexes = [(str(vertex_id), len(neighbors))
                for vertex_id, neighbors in enumerate(edges)]
    v = sql_context.createDataFrame(vertexes, ["id", "cardinality"])

    edges = [(str(source), str(destination)) for source, vertex_edges in enumerate(edges)
             for destination in vertex_edges]
    e = sql_context.createDataFrame(edges, ["src", "dst"])

    return GraphFrame(v, e)


# 0  -  1
# |  \
# 3  -  2
neighbours = [[3, 1, 2],
              [0],
              [1, 3, 0],
              [2, 0]]

graph_frame = build_graph_frame(neighbours)

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
    measure_time(lambda: k_clique_graph_frame(graph_frame, k),
                 "GraphFrame", "k_clique_graph_frame")
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

    graph = read_graph_frame_from_path("./twitter")
    measure_time(
        lambda: k_clique_graph_frame(
            graph, k), "GraphFrame", "k_clique_graph_frame"
    )

    graph = read_graph_from_path("./twitter", VectorSetRDD)
    measure_time(
        lambda: k_clique(graph, k, VectorSetRDD), "VectorSetRDD", "k_clique")
    graph = read_graph_from_path("./twitter", VectorSetDataFrame)
    measure_time(
        lambda: k_clique(graph, k, VectorSetDataFrame), "VectorSetDataframe", "k_clique")


run_small_graph(3)

# run_twitter_graph(3)
