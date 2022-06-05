import boto3
from inspect import getfile
from time import time
from typing import List, Callable, Tuple, Type
from pyspark import SparkContext
import pathlib
import os
import shutil
from distutils.dir_util import copy_tree
import logging
from pyroaring import BitMap

logging.basicConfig(level=logging.INFO)

path = pathlib.Path(__file__).parent.resolve()
current_directory = pathlib.Path().resolve()
source = "/home/hadoop"
result = []
for file in os.listdir(source):
    if file.endswith(".py"):
        returned = shutil.copy(f"{source}/{file}",
                               path.__str__()+"/")
        result.append(returned)

copy_tree(f"{source}/twitter", path.__str__()+"/twitter")
copy_tree(f"{source}/twitter", current_directory.__str__()+"/twitter")


print(path.absolute().__str__())
print(os.listdir(path.__str__()))
current_directory = pathlib.Path().resolve()
print(current_directory.__str__())
print(os.listdir(current_directory.__str__()))
print("results: ", "\n".join(result))

# try:
#     from pip._internal.operations import freeze
# except ImportError:  # pip < 10.0
#     from pip.operations import freeze

# print("Freeze: ", "\n".join(list(freeze.freeze())))


sc = SparkContext(appName="GraphProcessing")

s3 = boto3.resource('s3')


def load_spark_context():
    from Set import Set
    from VectorSetRoaring import VectorSetRoaring
    from VectorSetRDD import VectorSetRDD
    import k_clique_module

    def add_class(set_class: Type[Set]):
        sc.addFile(getfile(set_class))

    add_class(Set)
    add_class(VectorSetRDD)
    add_class(VectorSetRoaring)
    add_class(BitMap)
    sc.addFile(k_clique_module.__file__)
    VectorSetRDD.set_spark_context(sc)


load_spark_context()
# 0  -  1
# |  \
# 3  -  2


def measure_time(function: Callable[[], int], class_name: str, function_name: str):
    start_time = time()
    result = function()
    end_time = time()

    object = s3.Object(
        'gms-us-east-1', f'results/{class_name}/{function_name}/result.txt')

    result = f"{class_name} {function_name} \n result: {result} time: {end_time-start_time}".encode(
        "ascii")
    object.put(Body=result)
    print(result)


def run_twitter_graph(k):
    from VectorSetRoaring import VectorSetRoaring
    from VectorSetRDD import VectorSetRDD
    from VectorSetDataFrame import VectorSetDataFrame
    from graph_loading import read_graph_from_path
    from k_clique_module import k_clique, k_clique_parallel

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
        lambda: k_clique(
            graph, k, VectorSetDataFrame), "VectorSetDataFrame", "k_clique"
    )


if __name__ == '__main__':
    run_twitter_graph(5)
