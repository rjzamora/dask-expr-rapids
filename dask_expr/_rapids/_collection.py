import cudf

from dask_expr._collection import DataFrame as BaseDataFrame


@BaseDataFrame.register_dispatch(cudf.DataFrame)
class DataFrame(BaseDataFrame):
    pass
