from __future__ import annotations
import imp
from typing import List
from pyspark import RDD, SparkContext
from Set import Set
from pyspark.sql import SparkSession
from pyroaring import BitMap


class VectorSetRoaring(Set):

    # Set interface has a lot of constructors. As Python doesn't allow overriding methods, here's something we can do
    def __init__(self, roaring_set: BitMap, **kwargs) -> VectorSetRoaring:
        self.set = roaring_set

    @staticmethod
    def Range(bound: int) -> VectorSetRoaring:
        return VectorSetRoaring(range(bound))

    @staticmethod
    def from_array(list: list) -> VectorSetRoaring:
        return VectorSetRoaring(BitMap(list))

    def diff(self: VectorSetRoaring, b: VectorSetRoaring) -> VectorSetRoaring:
        return VectorSetRoaring(self.set.difference(b.set))

    def diff_element(self: VectorSetRoaring, b) -> VectorSetRoaring:
        roaring_set = BitMap(b)
        return VectorSetRoaring(self.set.difference(roaring_set))

    def diff_inplace(self, b) -> None:
        raise NotImplementedError(
            "Method diff_inplace(self, Set) is not implemented")

    def diff_element_inplace(self, b) -> None:
        raise NotImplementedError(
            "Method diff_element_inplace(self, SetElement) is not implemented")

    def intersect(self: VectorSetRoaring, b: VectorSetRoaring) -> VectorSetRoaring:
        return VectorSetRoaring(self.set.intersection(b.set))

    def intersect_count(self: VectorSetRoaring, b: VectorSetRoaring) -> int:
        return self.set.intersection_cardinality(b.set)

    def intersect_inplace(self, b) -> None:
        raise NotImplementedError(
            "Method intersect_inplace(self, Set) is not implemented")

    def union(self: VectorSetRoaring, b: VectorSetRoaring) -> VectorSetRoaring:
        return VectorSetRoaring(self.set.union(b.set))

    def union_element(self: VectorSetRoaring, b) -> VectorSetRoaring:
        roaring_set = BitMap(b)
        return VectorSetRoaring(self.set.union(roaring_set))

    def union_count(self: VectorSetRoaring, b: VectorSetRoaring) -> int:
        return self.set.union_cardinality(b.set)

    def union_inplace(self, b) -> None:
        raise NotImplementedError(
            "Method union_inplace(self, Set) is not implemented")

    def union_element_inplace(self, b) -> None:
        raise NotImplementedError(
            "Method union_element_inplace(self, SetElement) is not implemented")

    def contains(self: VectorSetRoaring, b) -> bool:
        return self.__contains__(b)

    def __contains__(self, item):
        return self.set.issuperset(BitMap([item]))

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

    def cardinality(self: VectorSetRoaring) -> int:
        return self.set.get_statistics()["cardinality"]

    def begin(self):
        raise NotImplementedError("Method begin(self) is not implemented")

    def end(self):
        raise NotImplementedError("Method end(self) is not implemented")

    def clone(self: VectorSetRoaring) -> VectorSetRoaring:
        return VectorSetRoaring(self.set.copy())

    def to_array(self: VectorSetRoaring) -> List:
        return list(self.set.to_array())

    def __eq__(self: VectorSetRoaring, other: VectorSetRoaring) -> bool:
        size_self = self.cardinality()
        size_other = other.cardinality()
        return size_self == size_other and self.intersect_count(other) == size_self

    def __ne__(self: VectorSetRoaring, other: VectorSetRoaring) -> bool:
        equal = self == other
        return not equal
