from __future__ import annotations
from typing import List
from pandas import DataFrame
from pyspark.sql import Row
from Set import Set
from pyspark.sql import SparkSession
from pyspark.sql.types import StructType, StructField, IntegerType


def intersect_(a, b):
    return list(set(a) & set(b))


def union_(a, b):
    return list(set(a) | set(b))


def diff_(a, b):
    return list(set(a) - set(b))


class VectorSetDataFrame(Set):

    spark_session = None
    schema = StructType([StructField("neighbor", IntegerType(), True)])

    @staticmethod
    def set_spark_session(spark_session: SparkSession):
        VectorSetDataFrame.spark_session = spark_session

    # Set interface has a lot of constructors. As Python doesn't allow overriding methods, here's something we can do
    def __init__(self, df: DataFrame, **kwargs):
        self.df = df

    @staticmethod
    def Range(bound: int) -> VectorSetDataFrame:

        df = VectorSetDataFrame.spark_session.createDataFrame(
            [Row(neighbor=elem) for elem in range(bound)],  VectorSetDataFrame.schema)

        return VectorSetDataFrame(df)

    @staticmethod
    def from_array(list: list) -> VectorSetDataFrame:
        df = VectorSetDataFrame.spark_session.createDataFrame(
            [Row(neighbor=elem) for elem in list], VectorSetDataFrame.schema)
        return VectorSetDataFrame(df)

    def diff(self, b: VectorSetDataFrame) -> VectorSetDataFrame:
        return VectorSetDataFrame(self.df.subtract(b.df))

    def diff_element(self, b) -> VectorSetDataFrame:
        return VectorSetDataFrame(self.df.filter(self.df.neighbor != b))

    def diff_inplace(self, b) -> None:
        raise NotImplementedError(
            "Method diff_inplace(self, Set) is not implemented")

    def diff_element_inplace(self, b) -> None:
        raise NotImplementedError(
            "Method diff_element_inplace(self, SetElement) is not implemented")

    def intersect(self, b: VectorSetDataFrame) -> VectorSetDataFrame:
        return VectorSetDataFrame(self.df.intersect(b.df))

    def intersect_count(self, b: VectorSetDataFrame) -> int:
        return self.intersect(b).cardinality()

    def intersect_inplace(self, b) -> None:
        raise NotImplementedError(
            "Method intersect_inplace(self, Set) is not implemented")

    def union(self, b: DataFrame) -> VectorSetDataFrame:
        return VectorSetDataFrame(self.df.union(b.df))

    def union_element(self, b) -> VectorSetDataFrame:
        vector_set = VectorSetDataFrame.from_array([b])
        return self.union(vector_set)

    def union_count(self, b: VectorSetDataFrame) -> int:
        return self.union(b).cardinality()

    def union_inplace(self, b) -> None:
        raise NotImplementedError(
            "Method union_inplace(self, Set) is not implemented")

    def union_element_inplace(self, b) -> None:
        raise NotImplementedError(
            "Method union_element_inplace(self, SetElement) is not implemented")

    def contains(self, b) -> bool:
        return self.__contains__(b)

    def __contains__(self, item):
        return self.df.filter(self.df.neighbor == item).count() > 0

    def add(self, b) -> None:
        self.__add__(b)

    def __add__(self, other):
        raise NotImplementedError(
            "Method __add__(self, SetElement) is not implemented")

    def remove(self, b) -> None:
        self.__sub__(b)

    def __sub__(self, other):
        raise NotImplementedError(
            "Method __sub__(self, SetElement) is not implemented")

    def cardinality(self) -> int:
        return self.df.count()

    def begin(self):
        raise NotImplementedError("Method begin(self) is not implemented")

    def end(self):
        raise NotImplementedError("Method end(self) is not implemented")

    def clone(self):
        return VectorSetDataFrame(self.df.cache())

    def to_array(self) -> List:
        return [row["neighbor"] for row in self.df.collect()]

    def __eq__(self, other: VectorSetDataFrame) -> bool:
        size1 = self.cardinality()
        size2 = other.cardinality()

        return size1 == size2 and self.intersect_count(other) == size1

    def __ne__(self, other) -> bool:
        equal = self == other
        return not equal
