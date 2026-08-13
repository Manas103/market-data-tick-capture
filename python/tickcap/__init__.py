"""tickcap -- independent Python tooling for the TCAP capture format.

Written from docs/FORMAT.md rather than from the C++ implementation, so that
agreement between the two is evidence about the specification rather than a
consequence of shared code.
"""

from .reader import TickCapture, ENCODING_NAMES
from .book import BookUniverse, InstrumentBook, BUY, SELL

__all__ = [
    "TickCapture",
    "ENCODING_NAMES",
    "BookUniverse",
    "InstrumentBook",
    "BUY",
    "SELL",
]
