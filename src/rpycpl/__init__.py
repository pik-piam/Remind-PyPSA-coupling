from . import utils
from . import etl

import importlib.metadata

try:
    __version__ = importlib.metadata.version("REMIND-PyPSA-coupling")
except importlib.metadata.PackageNotFoundError:
    __version__ = "unknown"