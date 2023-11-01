from __future__ import annotations

from dask_expr import DataFrame, expr, from_pandas, new_collection
from dask_expr.tests._util import _backend_library, assert_eq

# Set DataFrame backend for this module
lib = _backend_library()


def test_collection_dispatching():
    pdf = lib.DataFrame({"x": range(100)})
    expect = from_pandas(pdf, npartitions=2) + 1

    class TmpDataFrame(DataFrame):
        pass  # Avoid side effects after the test

    @TmpDataFrame.register_dispatch(lib.DataFrame)
    class _(TmpDataFrame):
        def custom_add(self, val):
            # Add new "custom_add" method that
            # calls TmpDataFrame.__add__, but also
            # adds an "added" column
            result = TmpDataFrame.__add__(self, val)
            result["added"] = val
            return result

    result = TmpDataFrame(from_pandas(pdf, npartitions=2).expr).custom_add(1)
    assert_eq(result[list(expect.columns)], expect)
    assert result["added"].max().compute() == 1


def test_expr_dispatching():
    pdf = lib.DataFrame({"x": range(100)})
    expect = from_pandas(pdf, npartitions=2) + 1

    class TmpAdd(expr.Add):
        pass  # Avoid side effects after the test

    @TmpAdd.register_dispatch(lib.DataFrame)
    class _(TmpAdd):
        def _lower(self):
            # Assign a "new" column if one doesn't
            # already exist. Note that this would be
            # a bad thing to do in practice, because
            # `Add` is not allowed to add/drop columns.
            if "new" not in self.columns:
                new = expr.Assign(self.left, "new", 1)
                return TmpAdd(new, self.right)

    _expr = from_pandas(pdf, npartitions=2).expr
    result = new_collection(TmpAdd(_expr, 1)).compute()
    assert_eq(result[list(expect.columns)], expect)
    assert result["new"].max() == 2
