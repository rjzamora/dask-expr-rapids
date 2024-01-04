import dask_expr._rapids
from dask_expr import _version, datasets
from dask_expr._collection import *
from dask_expr.io._delayed import from_delayed
from dask_expr.io.parquet import to_parquet

__version__ = _version.get_versions()["version"]
