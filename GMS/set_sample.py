from pyspark.sql import Row
from pyspark.sql import SparkSession
from VectorSet import VectorSet

spark = SparkSession.builder.getOrCreate()
# 1  -  2
# |  \  |
# 4  -  3
df = spark.createDataFrame([
    Row(vertex_id=1, neighbors=[4, 2, 3]),
    Row(vertex_id=2, neighbors=[1, 3]),
    Row(vertex_id=3, neighbors=[2, 4, 1]),
    Row(vertex_id=4, neighbors=[3, 1])
])
df.show()
df.printSchema()


print("Set")
set = VectorSet(df_row=df.filter("vertex_id==1"))
set.intersect(df.filter("vertex_id==2")).show()
set.union(df.filter("vertex_id==2")).show()
set.intersect_count(df.filter("vertex_id==2")).show()
set.union_count(df.filter("vertex_id==2")).show()
set.diff(df.filter("vertex_id==2")).show()
set.cardinality().show()
