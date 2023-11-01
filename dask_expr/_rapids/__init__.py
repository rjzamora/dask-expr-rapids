from dask import config

from dask_expr._collection import FrameBase
from dask_expr._expr import Expr
from dask_expr._rapids._dispatching import (
    override_new_collection,
    override_new_expr,
    register_dispatch,
)

# Monkey-patch type-based dispatching
FrameBase.register_dispatch = classmethod(register_dispatch)
FrameBase.__new__ = override_new_collection

Expr.register_dispatch = classmethod(register_dispatch)
Expr.__new__ = override_new_expr


try:
    # Configure for RAPIDS
    # TODO: Are there cases where we want to avoid this?
    import dask_expr._rapids._collection
    import dask_expr._rapids._expr

    shuffle_default = config.get("dataframe.shuffle.method", "tasks")
    config.set(
        {
            "dataframe.backend": "cudf",
            "dataframe.shuffle.method": shuffle_default,
        }
    )
except ImportError:
    pass
