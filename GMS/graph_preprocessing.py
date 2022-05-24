from pandas import DataFrame
from typing import List, Tuple
from VectorSetRDD import VectorSetRDD
from pyspark.sql import SparkSession

Graph = List[Tuple[int, VectorSetRDD]]


def split_vertexes(graph: Graph, k: int) -> Tuple[Graph, Graph]:
    to_remove = []
    stayed = []
    for vertex in graph:
        if vertex[1].cardinality() < k:
            to_remove.append(vertex)
        else:
            stayed.append(vertex)
    return (to_remove, stayed)


def remove_vertexes(graph: Graph, to_remove: Graph) -> Graph:
    spark = SparkSession.builder.getOrCreate()
    sc = spark.sparkContext
    to_remove_rdd = sc.parallelize(
        [vertex_id for (vertex_id, _vertex) in to_remove])
    to_remove_vertex = VectorSetRDD(to_remove_rdd, sc)

    return [(vertex_id, vertex.diff(to_remove_vertex)) for (vertex_id, vertex) in graph]


def degeneracy_order(graph: Graph, k: int) -> Graph:

    while len(graph) != 0:
        (to_remove_vertexes, stayed) = split_vertexes(graph, k)

        if len(to_remove_vertexes) == 0:
            return graph

        graph = remove_vertexes(stayed, to_remove_vertexes)

    return graph
