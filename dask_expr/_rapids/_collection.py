import cudf

from dask_expr._collection import DataFrame


@DataFrame.register_dispatch(cudf.DataFrame)
class RapidsDataFrame(DataFrame):
    pass
