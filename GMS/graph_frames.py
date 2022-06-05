from pyspark.sql import Row
from pyspark.sql import SparkSession
from VectorSetRDD import VectorSetRDD
from graphframes import *
from typing import List
from k_clique_module import degeneracy_order, k_clique

spark = SparkSession.builder.getOrCreate()
sc = spark.sparkContext


def build_graph_frame(edges: List[List[int]]) -> GraphFrame:
    vertexes = [(str(vertex_id), len(neighbors))
                for vertex_id, neighbors in enumerate(edges)]
    v = spark.createDataFrame(vertexes, ["id", "cardinality"])

    edges = [(str(source), str(destination)) for source, vertex_edges in enumerate(edges)
             for destination in vertex_edges]
    e = spark.createDataFrame(edges, ["src", "dst"])

    return GraphFrame(v, e)


# 0  -  1
# |  \
# 3  -  2
# df = spark.createDataFrame([
#     Row(vertex_id=1, neighbors=[4, 2, 3]),
#     Row(vertex_id=2, neighbors=[1, 3]),
#     Row(vertex_id=3, neighbors=[2, 4, 1]),
#     Row(vertex_id=4, neighbors=[3, 1])f
# ])
neighbours = [[3, 1, 2],
              [0],
              [1, 3, 0],
              [2, 0]]

graph_frame = build_graph_frame(neighbours)
graph_frame.inDegrees.show()
