from pkgutil import iter_modules
import boto3
from inspect import getfile
from time import time
from typing import Callable
from pyspark import SparkContext
import pathlib
import os
import shutil
from distutils.dir_util import copy_tree
import logging
import os
import sys
os.environ['PYSPARK_SUBMIT_ARGS'] = '--packages graphframes:graphframes:0.6.0-spark2.3-s_2.11 pyspark-shell'

logging.basicConfig(level=logging.INFO)


sc = SparkContext(appName="GraphProcessing")

s3 = boto3.resource('s3')


def load_GMS_module(bucket: str):
    # Download zip module
    s3.Bucket(bucket).download_file("GMS.zip", "./GMS.zip")

    # Add module to spark context
    sc.addPyFile("./GMS.zip")

    # Load pyroaring
    from pyroaring import BitMap
    sc.addPyFile(getfile(BitMap))

    from sets.vector_set_rdd import VectorSetRDD

    VectorSetRDD.set_spark_context(sc)


def measure_time(function: Callable[[], int], class_name: str, function_name: str, bucket: str):
    start_time = time()
    result = function()
    end_time = time()

    object = s3.Object(
        bucket, f'results/{class_name}/{function_name}/result.txt')

    result = f"{class_name} {function_name} \n result: {result} time: {end_time-start_time}".encode(
        "ascii")
    object.put(Body=result)
    print(result)


def run_twitter_graph(k: int, bucket: str, dataset_path: str = "./twitter"):
    from sets.vector_set_roaring import VectorSetRoaring
    from sets.vector_set_rdd import VectorSetRDD
    from sets.vector_set_dataframe import VectorSetDataFrame
    from graph_loading import read_graph_from_path, read_graph_frame_from_path
    from k_clique_module import k_clique, k_clique_parallel, k_clique_graph_frame

    graph = read_graph_from_path(dataset_path, VectorSetRoaring)
    measure_time(
        lambda: k_clique(graph, k, VectorSetRoaring),
        "VectorSetRoaring",
        "k_clique",
        bucket
    )
    measure_time(
        lambda: k_clique_parallel(graph, k, VectorSetRoaring, sc),
        "VectorSetRoaring",
        "k_clique_parallel",
        bucket
    )

    graph = read_graph_frame_from_path(dataset_path)
    measure_time(
        lambda: k_clique_graph_frame(graph, k),
        "GraphFrame",
        "k_clique_graph_frame",
        bucket
    )

    graph = read_graph_from_path(dataset_path, VectorSetRDD)
    measure_time(
        lambda: k_clique(graph, k, VectorSetRDD),
        "VectorSetRDD",
        "k_clique",
        bucket
    )

    graph = read_graph_from_path(dataset_path, VectorSetDataFrame)
    measure_time(
        lambda: k_clique(graph, k, VectorSetDataFrame),
        "VectorSetDataFrame",
        "k_clique",
        bucket
    )


if __name__ == '__main__':
    args = sys.argv
    dataset_path = args[1]
    bucket = args[2]
    load_GMS_module(bucket)
    run_twitter_graph(5, bucket, dataset_path)
