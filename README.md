# seipy

Helper functions for data science using python and spark.

## What is it

This library contains helpers and wrappers for common data science libraries in the python stack:
- pandas
- numpy
- scipy
- sklearn
- pyspark

There are also functions that simplify common manipulations for machine learning and data science
in general, as well as interfacing with the following tools:
- s3
- jupyter
- aws
- spark SQL

## Installation
```
# PyPI
pip install seipy
```

## Here are some examples

### pandas

```
from seipy import apply_uniq
df2 = apply_uniq(df, orig_col, new_col, _func)
```
This will return the same DataFrame as performing:
`df[new_col] = df[orig_col].apply(_func)`
but is much more performant when there are many duplicate entries in `orig_col`.

It works by performing the function `_func` only on the unique entries and then joining to original DataFrame.
Originally answered on stack overflow:
https://stackoverflow.com/questions/46798532/how-do-you-effectively-use-pd-dataframe-apply-on-rows-with-duplicate-values/
