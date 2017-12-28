# seipy

Helper functions for the python data science stack as well as spark, AWS, jupyter.

## What is it

This library contains helpers and wrappers for common data science libraries in the python stack:
- pandas
- numpy
- scipy
- sklearn
- matplotlib
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

#### Apply function to unique DataFrame entries only (for speedup)
```
from seipy import apply_uniq
df2 = apply_uniq(df, orig_col, new_col, _func)
```
This will return the same DataFrame as performing:
`df[new_col] = df[orig_col].apply(_func)`
but is much more performant when there are many duplicate entries in `orig_col`.

It works by performing the function `_func` only on the unique entries and then merging with the original DataFrame.
Originally answered on stack overflow:
https://stackoverflow.com/questions/46798532/how-do-you-effectively-use-pd-dataframe-apply-on-rows-with-duplicate-values/

#### Filtering DataFrame with multiple conditions
```
from seipy import filt
# example with keyword arguments
filt(df,
     season="summer",
     age=(">", 18),
     sport=("isin", ["Basketball", "Soccer"]),
     name=("contains", "Armstrong")
    )

# example with dict notation
a = {'season': "summer", 'age': (">", 18)}
filt(df, **a)
```

### linear algebra

```
from seipy import distmat
distmat()
```
This will prints possible distance metrics such as "euclidean" "chebyshev", "hamming".

```
distmat(fframe, metric)
```
This generates a distance matrix using `metric`.
Note, this function is a wrapper of scipy.spatial.distance.cdist


### jupyter

```
from seipy import notebook_contains
notebook_contains(search_str,
                  on_docker=False,
                  git_dir='~/git/experiments/',
                  start_date='2015-01-01', end_date='2018-12-31')
```
Prints a list of notebooks that contain the str `search_str`.
Very useful for these situations: "Where's that notebook where I was trying that one thing that one time?"

### s3
```
from seipy import s3zip_func
s3zip_func(s3zip_path, _func, cred_fpath=cred_fpath, **kwargs)
```
This one's kinda nice. It allows one to apply a function `_func` to each subfile in a zip file sitting on s3.
I use it to filter and enrich some csv files that periodically get zipped to s3, for example.


### spark and s3 on jupyter

```
from seipy import s3spark_init
spark = s3spark_init(cred_fpath)
```
Returns `spark`, a `SparkSession` that makes it possible to interact with s3 from jupyter notebooks.
`cred_fpath` is the file path to the aws credentials file containing your keys.


### Miscellaneous

```
from seiji import merge_two_dicts
merge_two_dicts(dict_1, dict_2)
```
Returns the merged dict `{**dict_1, **dict_2}`.
An extension for mulitple dicts is `reduce(lambda d1,d2: {**d1,**d2}, dict_args[0])`

### Getting help

Please either post an issue on this github repo, or email the author `seiji dot armstrong at gmail` with feedback,
feature requests, or to complain that something doesn't work as expected.


