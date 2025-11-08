"""Minimal public API for radarlib used in tests.

Provides:
- YourClass: small container for numeric samples with simple stats.
- your_function: trivial example function (addition).
"""
from typing import Iterable, List, Optional, Union

Number = Union[int, float]


class YourClass:
    """Simple container for numeric samples with basic operations.

    Examples:
        obj = YourClass([1, 2, 3])
        obj.add(4)
        avg = obj.mean()  # 2.5
    """

    def __init__(self, samples: Optional[Iterable[Number]] = None) -> None:
        self.samples: List[Number] = list(samples) if samples is not None else []

    def add(self, value: Number) -> None:
        """Add a sample value."""
        self.samples.append(value)

    def mean(self) -> Optional[float]:
        """Return the arithmetic mean of samples or None when empty."""
        if not self.samples:
            return None
        return sum(self.samples) / len(self.samples)

    def max(self) -> Optional[Number]:
        """Return the maximum sample or None when empty."""
        if not self.samples:
            return None
        return max(self.samples)

    def clear(self) -> None:
        """Remove all samples."""
        self.samples.clear()


def your_function(a: Number, b: Number) -> Number:
    """Simple example function that returns the sum of two numbers."""
    return a + b