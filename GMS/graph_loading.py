from os import listdir
from re import S
from typing import List
from pyspark.sql import DataFrame, Row
from pyspark.sql import SparkSession


class Vertex:

    """
        This class represent one Vertex of graph from SNAP Stanford Graph DataSet.
        It expects that in directory, which path is passed in constructor will exist following files:
        - id.edges : The edges in the ego network for the node 'nodeId'. Edges are undirected for facebook, 
            and directed (a follows b) for twitter and gplus. The 'ego' node does not appear, but it is assumed that 
            they follow every node id that appears in this file.

        - id.circles : The set of circles for the ego node. Each line contains one circle, consisting of a series of node ids. 
            The first entry in each line is the name of the circle.

        - id.feat : The features for each of the nodes that appears in the edge file.

        - id.egofeat : The features for the ego user.

        - id.featnames : The names of each of the feature dimensions. Features are '1' if the user has this property 
            in their profile, and '0' otherwise. This file has been anonymized for facebook users, since the names 
            of the features would reveal private data.

    """

    def __init__(self, id: int, directory_path: str) -> None:
        self.id = id
        self.neighbours = {}
        self.circles = {}
        self.neighbour_features = {}
        self.features = []
        self.features_names = {}
        self._directory_path = directory_path
        self._read_neighbours()
        self._read_circles()
        self._read_neighbours_features()
        self._read_features()
        self._read_features_names()

    def _read_neighbours(self):
        for splitted_line in self._read_file_with_splitting("edges"):
            vertex1 = int(splitted_line[0])
            vertex2 = int(splitted_line[1])
            self.neighbours[vertex1] = True
            self.neighbours[vertex2] = True
        self.neighbours = list(self.neighbours.keys())

    def _read_circles(self):
        for splitted_line in self._read_file_with_splitting("circles", "\t"):
            splitted_line = self._map_to_int(splitted_line)
            circle_id = splitted_line[0]
            self.circles[circle_id] = splitted_line[1:]

    def _read_neighbours_features(self):
        for splitted_line in self._read_file_with_splitting("feat"):
            splitted_line = self._map_to_int(splitted_line)
            vertex_id = splitted_line[0]
            self.neighbour_features[vertex_id] = splitted_line[1:]

    def _read_features(self):
        self.features = self._read_file_with_splitting("egofeat")[0]

    def _read_features_names(self):
        for splitted_line in self._read_file_with_splitting("featnames"):
            feature_id = int(splitted_line[0])
            self.features_names[feature_id] = splitted_line[1]

    def _read_file_with_splitting(self, file_extension: str, sep=" ") -> List[List[str]]:
        with open(f"{self._directory_path}/{self.id}.{file_extension}") as f:
            return [line.split(sep) for line in f.readlines()]

    def _map_to_int(self, str_list: List[str]) -> List[int]:
        return [int(elem) for elem in str_list]

    def to_data_frame_row(self) -> Row:
        return Row(vertex_id=self.id, neighbors=self.neighbours)


def read_graph_from_path(path: str) -> DataFrame:
    spark = SparkSession.builder.getOrCreate()
    files = [file.split(".") for file in listdir(path)]
    vertexes_ids = [int(file[0]) for file in files if file[1] == "edges"]
    return spark.createDataFrame(
        [Vertex(vertex_id, path).to_data_frame_row()
         for vertex_id in vertexes_ids]
    )


df = read_graph_from_path("./twitter")
df.show()
