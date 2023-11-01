from __future__ import annotations

from typing import Any

from dask.utils import Dispatch

from dask_expr._expr import Expr

##
## External class-dispatching utilities
##

__ext_dispatch_classes = {}  # Track registered "external" dispatch classes


def _register_ext_dispatch(cls: type, meta_type: type, ext_cls: type | None = None):
    """Register a custom class for type-based dispatching"""

    def wrapper(ext_cls):
        if cls not in __ext_dispatch_classes:
            __ext_dispatch_classes[cls] = Dispatch(f"{cls.__qualname__}_dispatch")
        if isinstance(meta_type, tuple):
            for t in meta_type:
                __ext_dispatch_classes[cls].register(t, ext_cls)
        else:
            __ext_dispatch_classes[cls].register(meta_type, ext_cls)
        return ext_cls

    return wrapper(ext_cls) if ext_cls is not None else wrapper


def _get_ext_dispatch(cls: type, meta: Any) -> None | type:
    """Get the registered dispatch class if one exists"""
    try:
        return __ext_dispatch_classes[cls].dispatch(type(meta))
    except (KeyError, TypeError):
        # Return None by default
        return None


def register_dispatch(cls, meta_type, custom_cls=None):
    """Register an external/custom expression dispatch"""
    return _register_ext_dispatch(cls, meta_type, custom_cls)


def override_new_collection(cls, expr):
    use_cls = _get_ext_dispatch(cls, expr._meta)
    if use_cls:
        return use_cls(expr)
    return object.__new__(cls)


def override_new_expr(cls, *args, **kwargs):
    if args and isinstance(args[0], Expr):
        meta = args[0]._meta
        use_cls = _get_ext_dispatch(cls, meta)
        if use_cls:
            return use_cls(*args, **kwargs)
    return object.__new__(cls)
