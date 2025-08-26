import math
from typing import Iterator, TypeVar, Sequence

T = TypeVar("T")

def takeSpread(sequence: Sequence[T], n: int) -> Iterator[T]:
    if (n > len(sequence)):
        raise ValueError("n cannot exceed list size")
    length = float(len(sequence))
    skip = length / n
    for i in range(n):
        yield sequence[int(math.ceil(i * skip))]
