from typing import List
from pyspark.sql import Row
from .abstract_set import Set


def intersect_(a, b):
    return list(set(a) & set(b))


def union_(a, b):
    return list(set(a) | set(b))


def diff_(a, b):
    return list(set(a) - set(b))


class VectorSet(Set):

    # Set interface has a lot of constructors. As Python doesn't allow overriding methods, here's something we can do
    def __init__(self, **kwargs):
        if "df_row" in kwargs.keys():
            df_row = kwargs["df_row"]
            self.df_row = df_row

        else:
            raise NotImplementedError(
                "Method __init__(self, **kwargs) is not implemented")

    def apply_operation_on_self(self, operation):
        rdd = self.df_row.rdd
        c = rdd.map(lambda x: operation(x))
        return c.toDF()

    def apply_operation_on_self_and_df(self, b, operation):
        b = b.withColumnRenamed("vertex_id", "vertex_id_").withColumnRenamed(
            "neighbors", "neighbors_")
        joined_rdd = self.df_row.join(b).rdd
        c = joined_rdd.map(lambda x: operation(x))
        return c.toDF()

    @staticmethod
    def Range(bound: int, spark):
        return spark.createDataFrame([
            Row(vertex_id=1, neighbors=list(range(bound)))
        ])

    def diff(self, b):
        return self.apply_operation_on_self_and_df(b, lambda x: (x[0], diff_(x[1], x[3])))

    def diff_element(self, b):
        return self.apply_operation_on_self_and_df(b, lambda x: (x[0], diff_(x[1], [x[3]])))

    def diff_inplace(self, b) -> None:
        raise NotImplementedError(
            "Method diff_inplace(self, Set) is not implemented")

    def diff_element_inplace(self, b) -> None:
        raise NotImplementedError(
            "Method diff_element_inplace(self, SetElement) is not implemented")

    def intersect(self, b):
        return self.apply_operation_on_self_and_df(b, lambda x: (x[0], intersect_(x[1], x[3])))

    def intersect_count(self, b) -> int:
        return self.apply_operation_on_self_and_df(b, lambda x: (x[0], len(intersect_(x[1], x[3]))))

    def intersect_inplace(self, b) -> None:
        raise NotImplementedError(
            "Method intersect_inplace(self, Set) is not implemented")

    def union(self, b):
        return self.apply_operation_on_self_and_df(b, lambda x: (x[0], union_(x[1], x[3])))

    def union_element(self, b):
        return self.apply_operation_on_self_and_df(b, lambda x: (x[0], union_(x[1], [x[3]])))

    def union_count(self, b) -> int:
        return self.apply_operation_on_self_and_df(b, lambda x: (x[0], len(union_(x[1], x[3]))))

    def union_inplace(self, b) -> None:
        raise NotImplementedError(
            "Method union_inplace(self, Set) is not implemented")

    def union_element_inplace(self, b) -> None:
        raise NotImplementedError(
            "Method union_element_inplace(self, SetElement) is not implemented")

    def contains(self, b) -> bool:
        return self.__contains__(b)

    def __contains__(self, item):
        raise NotImplementedError(
            "Method __contains__(self, SetElement) is not implemented")

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
        return self.apply_operation_on_self(lambda x: (x[0], len(set(x[1]))))

    def begin(self):
        raise NotImplementedError("Method begin(self) is not implemented")

    def end(self):
        raise NotImplementedError("Method end(self) is not implemented")

    def clone(self):
        raise NotImplementedError("Method end(self) is not implemented")

    def to_array(self) -> List:
        raise NotImplementedError("Method to_array(self) is not implemented")

    def __eq__(self, other) -> bool:
        def eq_(a, b):
            return set(a) == set(b)

        return self.apply_operation_on_self_and_df(b, lambda x: (x[0], eq_(x[1], x[3])))

    def __ne__(self, other):
        def ne_(a, b):
            return set(a) != set(b)

        return self.apply_operation_on_self_and_df(b, lambda x: (x[0], ne_(x[1], x[3])))
