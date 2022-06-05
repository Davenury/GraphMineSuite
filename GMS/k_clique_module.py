from pandas import DataFrame
from typing import List, Tuple, Dict, Type
from Set import Set
from pyspark.sql import SparkSession
from pyspark import SparkContext

Graph = List[Tuple[int, Set]]

GraphMap = Dict[int, Set]


def split_vertexes(graph: Graph, k: int) -> Tuple[Graph, Graph]:
    to_remove = []
    stayed = []
    for vertex in graph:
        if vertex[1].cardinality() < k - 1:
            to_remove.append(vertex)
        else:
            stayed.append(vertex)
    return (to_remove, stayed)


def remove_vertexes(graph: Graph, to_remove: Graph, set_class: Type[Set]) -> Graph:

    to_remove_ids = [vertex_id for (vertex_id, _vertex) in to_remove]

    to_remove_vertex = set_class.from_array(to_remove_ids)

    return [(vertex_id, vertex.diff(to_remove_vertex)) for (vertex_id, vertex) in graph]


def degeneracy_order(graph: Graph, k: int, set_class: Type[Set]) -> Graph:

    while len(graph) != 0:
        (to_remove_vertexes, stayed) = split_vertexes(graph, k)

        if len(to_remove_vertexes) == 0:
            return graph

        graph = remove_vertexes(stayed, to_remove_vertexes, set_class)

    return graph


def dir(graph: Graph, set_class: Type[Set]) -> Graph:
    #  An edge goes from 𝑣 to 𝑢 iff 𝜂 (𝑣) < 𝜂 (𝑢)
    def normalize_edges(vertex: Set, vertex_id: int) -> Set:
        to_remove = [vertex for vertex in vertex.to_array()
                     if vertex <= vertex_id]
        to_remove_set = set_class.from_array(to_remove)
        return vertex.diff(to_remove_set)

    return [(vertex_id, normalize_edges(vertex, vertex_id)) for (vertex_id, vertex) in graph]


def abstract_count(k: int, set_class: Type[Set], i: int, g: GraphMap, c_i: Set) -> int:
    if i == k:
        return c_i.cardinality()
    else:
        ci = 0
        for v in c_i.to_array():
            c_i1 = g.get(v, set_class.from_array([])).intersect(c_i)
            ci += abstract_count(k, set_class, i+1, g, c_i1)
        return ci


def log_percentage(i, percentage):
    divided = i/percentage
    if divided - round(divided) < 0.01 and 0 < divided < 100:
        print(f"Percentage: {round(divided)}, vertex id: {i}")


def k_clique(graph: Graph, k: int, set_class: Type[Set]):
    percentage = len(graph) / 100

    def count(i: int, g: GraphMap, c_i: Set, vertex_idx: int):
        log_percentage(vertex_idx, percentage)
        return abstract_count(k, set_class, i, g, c_i)

    new_graph = degeneracy_order(graph, k, set_class)

    new_graph = dir(new_graph, set_class)

    percentage = len(new_graph) / 100

    vertex_id_to_vertex = {
        vertex_id: vertex for vertex_id, vertex in new_graph}

    print("Start processing graph")

    return sum([count(2, vertex_id_to_vertex, vertex, vertex_id)for (vertex_id, vertex) in new_graph])


def k_clique_parallel(graph: Graph, k: int, set_class: Type[Set], sc: SparkContext):
    percentage = len(graph) / 100

    def count(i: int, g: GraphMap, c_i: Set, vertex_idx: int):
        log_percentage(vertex_idx, percentage)
        return abstract_count(k, set_class, i, g, c_i)

    new_graph = degeneracy_order(graph, k, set_class)

    new_graph = dir(new_graph, set_class)

    percentage = len(new_graph) / 100

    vertex_id_to_vertex = {
        vertex_id: vertex for vertex_id, vertex in new_graph}

    print("Start processing graph")

    return sc.parallelize(new_graph).map(lambda vertex: count(2, vertex_id_to_vertex, vertex[1], vertex[0])).sum()
