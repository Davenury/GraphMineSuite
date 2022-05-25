from pyspark.sql import Row
from pyspark.sql import SparkSession
from VectorSetRDD import VectorSetRDD
from k_clique import degeneracy_order, k_clique

spark = SparkSession.builder.getOrCreate()
sc = spark.sparkContext
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

graph = []
for (idx, neighbours) in enumerate(neighbours):
    rdd = sc.parallelize(neighbours)
    graph.append((idx, VectorSetRDD(rdd)))

new_graph = degeneracy_order(graph, 2)

print(new_graph)

print(k_clique(graph, 2))
