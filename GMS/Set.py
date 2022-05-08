from __future__ import annotations


class Set:

    # Set interface has a lot of constructors. As Python doesn't allow overriding methods, here's something we can do
    def __init__(self, **kwargs):
        raise NotImplementedError("Method __init__(self, **kwargs) is not implemented")

    @staticmethod
    def Range(bound: int) -> Set:
        raise NotImplementedError("Method Range(int) is not implemented")

    def diff(self, b: Set) -> Set:
        raise NotImplementedError("Method diff(self, Set) is not implemented")

    def diff_element(self, b) -> Set:
        raise NotImplementedError("Method diff_element(self, SetElement) is not implemented")

    def diff_inplace(self, b: Set) -> None:
        raise NotImplementedError("Method diff_inplace(self, Set) is not implemented")

    def diff_element_inplace(self, b) -> None:
        raise NotImplementedError("Method diff_element_inplace(self, SetElement) is not implemented")

    def intersect(self, b: Set) -> Set:
        raise NotImplementedError("Method intersect(self, Set) is not implemented")

    def intersect_count(self, b: Set) -> int:
        raise NotImplementedError("Method intersect_count(self, Set) is not implemented")

    def intersect_inplace(self, b: Set) -> None:
        raise NotImplementedError("Method intersect_inplace(self, Set) is not implemented")

    def union(self, b: Set) -> Set:
        raise NotImplementedError("Method union(self, Set) is not implemented")

    def union_element(self, b) -> Set:
        raise NotImplementedError("Method union_element(self, SetElement) is not implemented")

    def union_count(self, b: Set) -> int:
        raise NotImplementedError("Method union_count(self, Set) is not implemented")

    def union_inplace(self, b: Set) -> None:
        raise NotImplementedError("Method union_inplace(self, Set) is not implemented")

    def union_element_inplace(self, b) -> None:
        raise NotImplementedError("Method union_element_inplace(self, SetElement) is not implemented")

    def contains(self, b) -> boolean:
        return self.__contains__(b)

    def __contains__(self, item):
        raise NotImplementedError("Method __contains__(self, SetElement) is not implemented")

    def add(self, b) -> None:
        self.__add__(b)

    def __add__(self, other):
        raise NotImplementedError("Method __add__(self, SetElement) is not implemented")

    def remove(self, b) -> None:
        self.__sub__(b)

    def __sub__(self, other):
        raise NotImplementedError("Method __sub__(self, SetElement) is not implemented")

    def cardinality(self) -> int:
        raise NotImplementedError("Method cardinality(self) is not implemented")

    def begin(self):
        raise NotImplementedError("Method begin(self) is not implemented")

    def end(self):
        raise NotImplementedError("Method end(self) is not implemented")

    def clone(self) -> Set:
        raise NotImplementedError("Method end(self) is not implemented")

    def to_array(self) -> List:
        raise NotImplementedError("Method to_array(self) is not implemented")

    def __eq__(self, other) -> boolean:
        raise NotImplementedError("Method __eq__(self) is not implemented")

    def __ne__(self, other):
        raise NotImplementedError("Method __ne__(self) is not implemented")
