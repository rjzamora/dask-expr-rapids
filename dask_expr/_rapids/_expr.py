import cudf

from dask_expr._expr import Expr


@Expr.register_dispatch(cudf.DataFrame)
class RapidsExpr(Expr):
    pass
