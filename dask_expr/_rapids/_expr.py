import cudf

from dask_expr._expr import Expr as BaseExpr


@BaseExpr.register_dispatch(cudf.DataFrame)
class RapidsExpr(BaseExpr):
    pass
