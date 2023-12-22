from __future__ import annotations

import functools
import os
import weakref
from collections import defaultdict
from collections.abc import Generator

import dask
import pandas as pd
import toolz
from dask.dataframe.core import is_dataframe_like, is_index_like, is_series_like
from dask.typing import TaskGraphFactory
from dask.utils import funcname, import_required, is_arraylike

from dask_expr._util import _BackendData, _tokenize_deterministic


def _unpack_collections(o):
    if isinstance(o, Expr):
        return o

    if hasattr(o, "expr"):
        return o.expr
    else:
        return o


class Expr:
    _parameters = []
    _defaults = {}

    def __init__(self, *args, **kwargs):
        operands = list(args)
        for parameter in type(self)._parameters[len(operands) :]:
            try:
                operands.append(kwargs.pop(parameter))
            except KeyError:
                operands.append(type(self)._defaults[parameter])
        assert not kwargs, kwargs
        operands = [_unpack_collections(o) for o in operands]
        self.operands = operands
        if self._required_attribute:
            dep = next(iter(self.dependencies()))._meta
            if not hasattr(dep, self._required_attribute):
                # Raise a ValueError instead of AttributeError to
                # avoid infinite recursion
                raise ValueError(f"{dep} has no attribute {self._required_attribute}")

    @property
    def _required_attribute(self) -> str:
        # Specify if the first `dependency` must support
        # a specific attribute for valid behavior.
        return None

    def __str__(self):
        s = ", ".join(
            str(param) + "=" + str(operand)
            for param, operand in zip(self._parameters, self.operands)
            if isinstance(operand, Expr) or operand != self._defaults.get(param)
        )
        return f"{type(self).__name__}({s})"

    def __repr__(self):
        return str(self)

    def _tree_repr_lines(self, indent=0, recursive=True):
        header = funcname(type(self)) + ":"
        lines = []
        for i, op in enumerate(self.operands):
            if isinstance(op, Expr):
                if recursive:
                    lines.extend(op._tree_repr_lines(2))
            else:
                try:
                    param = self._parameters[i]
                    default = self._defaults[param]
                except (IndexError, KeyError):
                    param = self._parameters[i] if i < len(self._parameters) else ""
                    default = "--no-default--"

                if isinstance(op, _BackendData):
                    op = op._data

                # TODO: this stuff is pandas-specific
                if isinstance(op, pd.core.base.PandasObject):
                    op = "<pandas>"
                elif is_dataframe_like(op):
                    op = "<dataframe>"
                elif is_index_like(op):
                    op = "<index>"
                elif is_series_like(op):
                    op = "<series>"
                elif is_arraylike(op):
                    op = "<array>"

                if repr(op) != repr(default):
                    if param:
                        header += f" {param}={repr(op)}"
                    else:
                        header += repr(op)
        lines = [header] + lines
        lines = [" " * indent + line for line in lines]

        return lines

    def tree_repr(self):
        return os.linesep.join(self._tree_repr_lines())

    def pprint(self):
        for line in self._tree_repr_lines():
            print(line)

    def __hash__(self):
        return hash(self._name)

    def __reduce__(self):
        if dask.config.get("dask-expr-no-serialize", False):
            raise RuntimeError(f"Serializing a {type(self)} object")
        return type(self), tuple(self.operands)

    def _depth(self):
        """Depth of the expression tree

        Returns
        -------
        depth: int
        """
        if not self.dependencies():
            return 1
        else:
            return max(expr._depth() for expr in self.dependencies()) + 1

    def operand(self, key):
        # Access an operand unambiguously
        # (e.g. if the key is reserved by a method/property)
        return self.operands[type(self)._parameters.index(key)]

    def dependencies(self):
        # Dependencies are `Expr` operands only
        return [operand for operand in self.operands if isinstance(operand, Expr)]

    def _task(self, index: int):
        """The task for the i'th partition

        Parameters
        ----------
        index:
            The index of the partition of this dataframe

        Examples
        --------
        >>> class Add(Expr):
        ...     def _task(self, i):
        ...         return (operator.add, (self.left._name, i), (self.right._name, i))

        Returns
        -------
        task:
            The Dask task to compute this partition

        See Also
        --------
        Expr._layer
        """
        raise NotImplementedError(
            "Expressions should define either _layer (full dictionary) or _task"
            " (single task).  This expression type defines neither"
        )

    def _layer(self) -> dict:
        """The graph layer added by this expression

        Examples
        --------
        >>> class Add(Expr):
        ...     def _layer(self):
        ...         return {
        ...             (self._name, i): (operator.add, (self.left._name, i), (self.right._name, i))
        ...             for i in range(self.npartitions)
        ...         }

        Returns
        -------
        layer: dict
            The Dask task graph added by this expression

        See Also
        --------
        Expr._task
        Expr.__dask_graph__
        """

        return {(self._name, i): self._task(i) for i in range(self.npartitions)}

    def rewrite(self, kind: str):
        """Rewrite an expression

        This leverages the ``._{kind}_down`` and ``._{kind}_up``
        methods defined on each class

        Returns
        -------
        expr:
            output expression
        changed:
            whether or not any change occured
        """
        expr = self
        down_name = f"_{kind}_down"
        up_name = f"_{kind}_up"
        while True:
            _continue = False

            # Rewrite this node
            if down_name in expr.__dir__():
                out = getattr(expr, down_name)()
                if out is None:
                    out = expr
                if not isinstance(out, Expr):
                    return out
                if out._name != expr._name:
                    expr = out
                    continue

            # Allow children to rewrite their parents
            for child in expr.dependencies():
                if up_name in child.__dir__():
                    out = getattr(child, up_name)(expr)
                    if out is None:
                        out = expr
                    if not isinstance(out, Expr):
                        return out
                    if out is not expr and out._name != expr._name:
                        expr = out
                        _continue = True
                        break

            if _continue:
                continue

            # Rewrite all of the children
            new_operands = []
            changed = False
            for operand in expr.operands:
                if isinstance(operand, Expr):
                    new = operand.rewrite(kind=kind)
                    if new._name != operand._name:
                        changed = True
                else:
                    new = operand
                new_operands.append(new)

            if changed:
                expr = type(expr)(*new_operands)
                continue
            else:
                break

        return expr

    def simplify_once(self, dependents: defaultdict):
        """Simplify an expression

        This leverages the ``._simplify_down`` and ``._simplify_up``
        methods defined on each class

        Parameters
        ----------

        dependents: defaultdict[list]
            The dependents for every node.

        Returns
        -------
        expr:
            output expression
        """
        expr = self

        while True:
            out = expr._simplify_down()
            if out is None:
                out = expr
            if not isinstance(out, Expr):
                return out
            if out._name != expr._name:
                expr = out

            # Allow children to simplify their parents
            for child in expr.dependencies():
                out = child._simplify_up(expr, dependents)
                if out is None:
                    out = expr

                if not isinstance(out, Expr):
                    return out
                if out is not expr and out._name != expr._name:
                    expr = out
                    break

            # Rewrite all of the children
            new_operands = []
            changed = False
            for operand in expr.operands:
                if isinstance(operand, Expr):
                    new = operand.simplify_once(dependents=dependents)
                    if new._name != operand._name:
                        changed = True
                else:
                    new = operand
                new_operands.append(new)

            if changed:
                expr = type(expr)(*new_operands)

            break

        return expr

    def simplify(self) -> Expr:
        expr = self
        while True:
            dependents = collect_depdendents(expr)
            new = expr.simplify_once(dependents=dependents)
            if new._name == expr._name:
                break
            expr = new
        return expr

    def _simplify_down(self):
        return

    def _simplify_up(self, parent, dependents):
        return

    def lower_once(self):
        expr = self

        # Lower this node
        out = expr._lower()
        if out is None:
            out = expr
        if not isinstance(out, Expr):
            return out

        # Lower all children
        new_operands = []
        changed = False
        for operand in out.operands:
            if isinstance(operand, Expr):
                new = operand.lower_once()
                if new._name != operand._name:
                    changed = True
            else:
                new = operand
            new_operands.append(new)

        if changed:
            out = type(out)(*new_operands)

        return out

    def lower_completely(self) -> Expr:
        """Lower an expression completely

        This calls the ``lower_once`` method in a loop
        until nothing changes. This function does not
        apply any other optimizations (like ``simplify``).

        Returns
        -------
        expr:
            output expression

        See Also
        --------
        Expr.lower_once
        Expr._lower
        """
        # Lower until nothing changes
        expr = self
        while True:
            new = expr.lower_once()
            if new._name == expr._name:
                break
            expr = new
        return expr

    def _lower(self):
        return

    @functools.cached_property
    def _name(self):
        return (
            funcname(type(self)).lower() + "-" + _tokenize_deterministic(*self.operands)
        )

    @property
    def _meta(self):
        raise NotImplementedError()

    def __getattr__(self, key):
        try:
            return object.__getattribute__(self, key)
        except AttributeError as err:
            if key == "_meta":
                # Avoid a recursive loop if/when `self._meta`
                # produces an `AttributeError`
                raise RuntimeError(
                    f"Failed to generate metadata for {self}. "
                    "This operation may not be supported by the current backend."
                )

            # Allow operands to be accessed as attributes
            # as long as the keys are not already reserved
            # by existing methods/properties
            _parameters = type(self)._parameters
            if key in _parameters:
                idx = _parameters.index(key)
                return self.operands[idx]

            link = "https://github.com/dask-contrib/dask-expr/blob/main/README.md#api-coverage"
            raise AttributeError(
                f"{err}\n\n"
                "This often means that you are attempting to use an unsupported "
                f"API function. Current API coverage is documented here: {link}."
            )

    def get_annotations(self):
        return {}

    def materialize(self):
        """Traverse expression tree, collect layers"""
        stack = [self]
        seen = set()
        layers = []
        while stack:
            expr = stack.pop()

            if expr._name in seen:
                continue
            seen.add(expr._name)

            layers.append(expr._layer())
            for operand in expr.dependencies():
                stack.append(operand)

        return toolz.merge(layers)

    def __dask_output_keys__(self) -> list:
        return [(self._name, i) for i in range(self.npartitions)]

    @property
    def dask(self) -> dict:
        return self.materialize()

    def substitute(self, old, new) -> Expr:
        """Substitute a specific term within the expression

        Note that replacing non-`Expr` terms may produce
        unexpected results, and is not recommended.
        Substituting boolean values is not allowed.

        Parameters
        ----------
        old:
            Old term to find and replace.
        new:
            New term to replace instances of `old` with.

        Examples
        --------
        >>> (df + 10).substitute(10, 20)
        df + 20
        """

        # Check if we are replacing a literal
        if isinstance(old, Expr):
            substitute_literal = False
            if self._name == old._name:
                return new
        else:
            substitute_literal = True
            if isinstance(old, bool):
                raise TypeError("Arguments to `substitute` cannot be bool.")

        new_exprs = []
        update = False
        for operand in self.operands:
            if isinstance(operand, Expr):
                val = operand.substitute(old, new)
                if operand._name != val._name:
                    update = True
                new_exprs.append(val)
            elif (
                "Fused" in type(self).__name__
                and isinstance(operand, list)
                and all(isinstance(op, Expr) for op in operand)
            ):
                # Special handling for `Fused`.
                # We make no promise to dive through a
                # list operand in general, but NEED to
                # do so for the `Fused.exprs` operand.
                val = []
                for op in operand:
                    val.append(op.substitute(old, new))
                    if val[-1]._name != op._name:
                        update = True
                new_exprs.append(val)
            elif (
                substitute_literal
                and not isinstance(operand, bool)
                and isinstance(operand, type(old))
                and operand == old
            ):
                new_exprs.append(new)
                update = True
            else:
                new_exprs.append(operand)

        if update:  # Only recreate if something changed
            return type(self)(*new_exprs)
        return self

    def substitute_parameters(self, substitutions: dict) -> Expr:
        """Substitute specific `Expr` parameters

        Parameters
        ----------
        substitutions:
            Mapping of parameter keys to new values. Keys that
            are not found in ``self._parameters`` will be ignored.
        """
        if not substitutions:
            return self

        changed = False
        new_operands = []
        for i, operand in enumerate(self.operands):
            if i < len(self._parameters) and self._parameters[i] in substitutions:
                new_operands.append(substitutions[self._parameters[i]])
                changed = True
            else:
                new_operands.append(operand)
        if changed:
            return type(self)(*new_operands)
        return self

    def _node_label_args(self):
        """Operands to include in the node label by `visualize`"""
        return self.dependencies()

    def _to_graphviz(
        self,
        rankdir="BT",
        graph_attr=None,
        node_attr=None,
        edge_attr=None,
        **kwargs,
    ):
        from dask.dot import label, name

        graphviz = import_required(
            "graphviz",
            "Drawing dask graphs with the graphviz visualization engine requires the `graphviz` "
            "python library and the `graphviz` system library.\n\n"
            "Please either conda or pip install as follows:\n\n"
            "  conda install python-graphviz     # either conda install\n"
            "  python -m pip install graphviz    # or pip install and follow installation instructions",
        )

        graph_attr = graph_attr or {}
        node_attr = node_attr or {}
        edge_attr = edge_attr or {}

        graph_attr["rankdir"] = rankdir
        node_attr["shape"] = "box"
        node_attr["fontname"] = "helvetica"

        graph_attr.update(kwargs)
        g = graphviz.Digraph(
            graph_attr=graph_attr,
            node_attr=node_attr,
            edge_attr=edge_attr,
        )

        stack = [self]
        seen = set()
        dependencies = {}
        while stack:
            expr = stack.pop()

            if expr._name in seen:
                continue
            seen.add(expr._name)

            dependencies[expr] = set(expr.dependencies())
            for dep in expr.dependencies():
                stack.append(dep)

        cache = {}
        for expr in dependencies:
            expr_name = name(expr)
            attrs = {}

            # Make node label
            deps = [
                funcname(type(dep)) if isinstance(dep, Expr) else str(dep)
                for dep in expr._node_label_args()
            ]
            _label = funcname(type(expr))
            if deps:
                _label = f"{_label}({', '.join(deps)})" if deps else _label
            node_label = label(_label, cache=cache)

            attrs.setdefault("label", str(node_label))
            attrs.setdefault("fontsize", "20")
            g.node(expr_name, **attrs)

        for expr, deps in dependencies.items():
            expr_name = name(expr)
            for dep in deps:
                dep_name = name(dep)
                g.edge(dep_name, expr_name)

        return g

    @classmethod
    def combine_factories(cls, *exprs: Expr) -> Expr:
        """Combine multiple expressions into a single expression

        Parameters
        ----------
        exprs:
            Expressions to combine

        Returns
        -------
        expr:
            Combined expression
        """
        raise NotImplementedError()

    def visualize(self, filename="dask-expr.svg", format=None, **kwargs):
        """
        Visualize the expression graph.
        Requires ``graphviz`` to be installed.

        Parameters
        ----------
        filename : str or None, optional
            The name of the file to write to disk. If the provided `filename`
            doesn't include an extension, '.png' will be used by default.
            If `filename` is None, no file will be written, and the graph is
            rendered in the Jupyter notebook only.
        format : {'png', 'pdf', 'dot', 'svg', 'jpeg', 'jpg'}, optional
            Format in which to write output file. Default is 'svg'.
        **kwargs
           Additional keyword arguments to forward to ``to_graphviz``.
        """
        from dask.dot import graphviz_to_file

        g = self._to_graphviz(**kwargs)
        graphviz_to_file(g, filename, format)
        return g

    def walk(self) -> Generator[Expr]:
        """Iterate through all expressions in the tree

        Returns
        -------
        nodes
            Generator of Expr instances in the graph.
            Ordering is a depth-first search of the expression tree
        """
        stack = [self]
        seen = set()
        while stack:
            node = stack.pop()
            if node._name in seen:
                continue
            seen.add(node._name)

            for dep in node.dependencies():
                stack.append(dep)

            yield node

    def find_operations(self, operation: type | tuple[type]) -> Generator[Expr]:
        """Search the expression graph for a specific operation type

        Parameters
        ----------
        operation
            The operation type to search for.

        Returns
        -------
        nodes
            Generator of `operation` instances. Ordering corresponds
            to a depth-first search of the expression graph.
        """
        assert (
            isinstance(operation, tuple)
            and all(issubclass(e, Expr) for e in operation)
            or issubclass(operation, Expr)
        ), "`operation` must be`Expr` subclass)"
        return (expr for expr in self.walk() if isinstance(expr, operation))


def collect_depdendents(expr) -> defaultdict:
    dependents = defaultdict(list)
    stack = [expr]
    seen = set()
    while stack:
        node = stack.pop()
        if node._name in seen:
            continue
        seen.add(node._name)

        for dep in node.dependencies():
            stack.append(dep)
            dependents[dep._name].append(weakref.ref(node))
    return dependents
