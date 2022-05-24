from __future__ import annotations
from typing import List
from pyspark.sql import Row
from pyspark import RDD, SparkContext
from Set import Set


class VectorSetRDD(Set):

    # Set interface has a lot of constructors. As Python doesn't allow overriding methods, here's something we can do
    def __init__(self, rdd: RDD, sc: SparkContext, **kwargs) -> VectorSetRDD:
        self.rdd = rdd
        self.sc = sc

    def new_vector_set(self, rdd: RDD) -> VectorSetRDD:
        return VectorSetRDD(rdd, self.sc)

    @staticmethod
    def Range(bound: int, spark_context: SparkContext) -> VectorSetRDD:
        rdd = spark_context.parallalize(range(bound))
        return VectorSetRDD(rdd, spark_context)

    def diff(self: VectorSetRDD, b: VectorSetRDD) -> VectorSetRDD:
        return self.new_vector_set(self.rdd.subtract(b.rdd))

    def diff_element(self: VectorSetRDD, b) -> VectorSetRDD:
        return self.new_vector_set(self.rdd.filter(lambda x: x != b))

    def diff_inplace(self, b) -> None:
        raise NotImplementedError(
            "Method diff_inplace(self, Set) is not implemented")

    def diff_element_inplace(self, b) -> None:
        raise NotImplementedError(
            "Method diff_element_inplace(self, SetElement) is not implemented")

    def intersect(self: VectorSetRDD, b: VectorSetRDD) -> VectorSetRDD:
        return self.new_vector_set(self.rdd.intersection(b.rdd))

    def intersect_count(self: VectorSetRDD, b: VectorSetRDD) -> int:
        return self.rdd.intersection(b.rdd).count()

    def intersect_inplace(self, b) -> None:
        raise NotImplementedError(
            "Method intersect_inplace(self, Set) is not implemented")

    def union(self: VectorSetRDD, b: VectorSetRDD) -> VectorSetRDD:
        return self.new_vector_set(self.rdd.union(b.rdd))

    def union_element(self: VectorSetRDD, b) -> VectorSetRDD:
        elem_rdd = self.sc.parallelize([b])
        return self.new_vector_set(self.rdd.union(elem_rdd))

    def union_count(self: VectorSetRDD, b: VectorSetRDD) -> int:
        return self.rdd.union(b.rdd).count()

    def union_inplace(self, b) -> None:
        raise NotImplementedError(
            "Method union_inplace(self, Set) is not implemented")

    def union_element_inplace(self, b) -> None:
        raise NotImplementedError(
            "Method union_element_inplace(self, SetElement) is not implemented")

    def contains(self: VectorSetRDD, b) -> bool:
        return self.__contains__(b)

    def __contains__(self, item):
        return self.rdd.filter(lambda a: a == item).count() > 0

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

    def cardinality(self: VectorSetRDD) -> int:
        return self.rdd.count()

    def begin(self):
        raise NotImplementedError("Method begin(self) is not implemented")

    def end(self):
        raise NotImplementedError("Method end(self) is not implemented")

    def clone(self: VectorSetRDD) -> VectorSetRDD:
        return self.new_vector_set(self.rdd.cache())

    def to_array(self: VectorSetRDD) -> List:
        return self.rdd.collect()

    def __eq__(self: VectorSetRDD, other: VectorSetRDD) -> bool:
        size_self = self.cardinality()
        size_other = other.cardinality()
        return size_self == size_other and self.intersect_count(other) == size_self

    def __ne__(self: VectorSetRDD, other: VectorSetRDD) -> bool:
        equal = self == other
        return not equal
