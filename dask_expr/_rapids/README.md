# Using Dask Expressions with RAPIDS

This repository mirrors [dask-contrib/dask-expr](https://github.com/dask-contrib/dask-expr), but also includes the minimal changes needed to support execution with RAPIDS ``cuDF`` (which depends on ``pandas < 2``).


### Step 1. Install ``cudf`` and ``dask_cudf``

Follow the [RAPIDS installation instructions](https://docs.rapids.ai/install). E.g.

```
$ mamba create -n rapids-23.12 -c rapidsai-nightly -c conda-forge -c nvidia rapids=23.12 python=3.10 cuda-version=11.8 pytest
$ conda activate rapids-23.12
```

### Step 2. Install ``dask_expr`` from ``dask-expr-rapids``

It is currently necessary to install ``dask_expr`` from source. E.g.

```
$ git clone git@github.com:rjzamora/dask-expr-rapids.git
$ cd dask-expr-rapids
$ pip install .
```

### Step 3. 

#### Running the test suite

Not all of the ``dask_expr`` test suite will account for differences between ``pandas`` and ``cudf``. However, most of the collection-level tests should pass:

```
$ py.test -v dask_expr/tests/test_collection.py
```

#### Basic usage

Import and use ``dask_expr`` in a way that is very similar to ``dask.dataframe``:

```python
>>> import cudf
>>> import dask_expr as dd
>>> pdf = cudf.DataFrame(
...     {
...         "x": range(100),
...         "y": [1, 2] * 50,
...         "z": ["dog", "cat"] * 50,
...     }
... )
>>> df = dd.from_pandas(pdf, 4)
>>> agg = df.groupby("z")["x"].count()
```

Use ``pprint`` to inspect the expression tree:

```python
>>> agg.pprint()
Count: by=['z'] _slice='x'
  Projection: columns=['x', 'z']
    FromPandas: frame='<dataframe>' npartitions=4
```

Use ``*.optimize()`` to apply query optimization:

```python
>>> agg = agg.optimize()
>>> agg.pprint()
Count(TreeReduce): split_every=8
  Fused(d8f1e):
  | Count(GroupByChunk): chunk=<methodcaller: count> columns=x
  |   FromPandas: frame='<dataframe>' npartitions=4 columns=['x', 'z']
```

Use ``compute``/``persist`` just like ``dask.dataframe``:

```python
>>> agg.compute()
z
cat    50
dog    50
Name: x, dtype: int64
```