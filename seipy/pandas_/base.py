"""
helper functions to use with pandas library.
Conventions:
df -> pd.DataFrame

Ideology:
- Write functions that do one thing
- Try write functions that are idempotent
"""

from functools import reduce
from collections import namedtuple
import numpy as np
import pandas as pd
import re


def drop_uniq_cols(df):
    """
    Returns DataFrame with columns that have more than one value (not unique)
    """
    return df.loc[:, df.apply(pd.Series.nunique) > 1]


def non_x_cols(df, x):
    """
    Returns DataFrame with columns that contain values other than x.
    Drops any column that only contains x.
    """
    cond_1 = (df.apply(pd.Series.nunique) == 1)
    cond_2 = (df.iloc[0] == x)
    return df.loc[:, ~(cond_1 & cond_2)]


def df_duplicate_row(df, row, row_cols):
    """
    (Check logic)
    Creates numpy array from df (pd.DataFrame),
    returns non_duplicate rows.
    """
    X = df[row_cols].as_matrix().astype(np.float)
    fv_ix = [ix for ix, el in enumerate(X) if (el == row.values).all()]
    df_rows = df.ix[fv_ix]
    return df_rows


### Useful to remember
def display_max_rows(max_num=500):
    """sets number of rows to display (useful in Jupyter environment)
    """
    pd.set_option('display.max_rows', max_num)


def assign_index(input_df, index_col='idx'):
    """Return input DataFrame with id col.
    """
    out_df = input_df.reset_index(drop=True)
    out_df.loc[:, index_col] = out_df.index
    return out_df


def enumerate_col(input_df, enumerate_over, new_col="newCol"):
    """
    Enumerate unique entries in enumerate_over_col and add new column newCol.
    """
    new_df = input_df.copy()
    unique_vals = new_df[enumerate_over].unique()
    enumerated_dict = {vals: ix for ix, vals in enumerate(unique_vals)}
    new_df.loc[:, new_col] = new_df[enumerate_over].map(lambda x: enumerated_dict[x])
    return new_df


def concat_df_list(list_csv, comment='#', header='infer', sep=',', nrows=None):
    """Concatenates list of dataframes first created by list comprehension.

    """
    return pd.concat([pd.read_csv(el, comment=comment, header=header, sep=sep, nrows=nrows)
                      for el in list_csv], ignore_index=True)


def merge_dfs(dataframe_list, join_meth='outer', on=None, left_index=False, right_index=False):
    """
    Merge together a list of DataFrames.
    """

    def simple_merge(df1, df2):
        return pd.merge(left=df1, right=df2, on=on,
                        left_index=left_index, right_index=right_index, how=join_meth)

    merged_frame = reduce(simple_merge, dataframe_list)
    return merged_frame


def uniq_x_y(X, y, output_index=False):
    """
    Given a X,y pair of numpy arrays (X:2D, y:1D),
    return unique rows (arrays) of X, with corresponding y values.
    """
    uniq_df = pd.DataFrame(X).drop_duplicates()
    X_uniq = uniq_df.values
    y_uniq = y[uniq_df.index]
    if output_index:
        return X_uniq, y_uniq, uniq_df.index
    return X_uniq, y_uniq


def gby_count(df, col, col_full_name=False):
    """
    Perform groupby count aggregation, rename column to count,
    and keep flat structure of DataFrame.
    Wrapper on pd.DataFrame.groupby
    """
    if col_full_name:
        col_name = "count_" + col
    else:
        col_name = "count"
    return df.groupby(col).size().reset_index(name=col_name).sort_values(by=col_name, ascending=False)


def distribute_rows(df, key_col):
    """
    Distributes rows according to the series found in key_col.
    Example:
        >> df = pd.DataFrame([(el+3, el) for el in range(4)], columns=['x', 'n'])
        >> df.T
           0  1  2  3
        x  3  4  5  6
        n  0  1  2  3
        >> df.loc[np.repeat(df.index.values,df.n)].T
           1  2  2  3  3  3
        x  4  5  5  6  6  6
        n  1  2  2  3  3  3
    """
    distribution_index = np.repeat(df.index.values, df[key_col])
    return df.loc[distribution_index]


def resample_by_col(df, gby_col='Cluster ID', scale_by='identity'):
    """
    Resample the rows of DataFrame.
    Groupby is first performed on gby_col, then counts are rescaled by
    function defined in scale_by (identity, log, sqrt).
    Groupby DataFrame is then resampled by new distribution, and an outer
    join is performed with original.drop_duplicates(gby_col).

    Note: inputting `df` and `df.drop_duplicates(gby_col)` lead to different
    results in `gby_col_count` col. Always input original.

    Improvements: create all_n func and do pass function curried like in scala
    """
    gby_df = df.groupby(gby_col).size().reset_index(name=gby_col + "_count")

    def identity(x): return x

    def all_one(x): return 1

    def all_two(x): return 2

    def all_three(x): return 3

    scale_funcs = {'log': np.log, 'sqrt': np.sqrt, 'identity': identity,
                   'all_one': all_one, 'all_two': all_two, 'all_three': all_three}
    distribution = gby_df[gby_col + "_count"].map(scale_funcs[scale_by]).map(np.ceil).map(np.int)
    df_new_dist = gby_df.loc[np.repeat(gby_df.index.values, distribution)]
    return pd.merge(df_new_dist, df.drop_duplicates(gby_col), on=gby_col, how='outer')


def convert_all_elements_tuple(input_df):
    """
    If all elements are lists or numpy arrays, these are not hashable.
    So we can convert to tuples, in order to be able to perform groupbys.
    """
    return input_df.applymap(tuple)


def explode_rows_vertically(df):
    """
    For use on DataFrames containing lists in each field.
    Note: per row, lists in each column must be equal in length.
        They can, however, differ in length from row to row.
    Explode lists in each column of input DataFrame vertically,
        one row at a time.
    Return DataFrame of exploded elements.
    Note we could also use `+= zip(*[df[col][ix] for col in df])`
    """
    exploded_list = []
    for ix, row in df.iterrows():
        exploded_list += zip(*row.values)
    return pd.DataFrame(exploded_list)


def str_list_to_tuple_str_series(col, regex_pattern='[A-Z]\d+'):
    """
    Convert string of lists into tuples of strings,
    for each row in column.
    regex_pattern determines string tokens.
    """
    if not isinstance(col[0], str):
        print("error: str expected, instead {} found.".format(type(col[0])))
        return col
    else:
        p = re.compile(regex_pattern)
        return col.apply(p.findall).apply(tuple)


def df_to_namedtups(df, name="Pandas"):
    """
    Given a DataFrame, df, return a list of namedtuples
    """
    return list(df.itertuples(index=False, name=name))


def gby_uniqlists(in_df, pivot_col=None, col=None) -> pd.DataFrame:
    """
    Computes aggregates for each unique value in `pivot_col` and returns
        new DataFrame with list of unique values in col + nunique in col.

    :param in_df: pd.DataFrame; input DataFrame
    :param pivot_col: str; column to groupby (pivot) off
    :param col: str; column to aggregate values on.
    :return: pd.DataFrame; groupby aggregates DataFrame with unique vals in list + length.
    """

    gby_temp = in_df.groupby(pivot_col)[col].apply(set).apply(list).reset_index()
    return gby_temp.assign(len_=gby_temp[col].apply(len)).rename(
        columns={'len_': col + '_uniq_len', col: col + '_uniq'})


def gby_multiframe(in_df, pivot_col=None, cols=None) -> pd.DataFrame:
    """
    wrapper of velpy.helper.pandas_.gby_uniqlists performed over list of cols,
        then joined on pivot_col.
    :param in_df: pd.DataFrame; input DataFrame
    :param pivot_col: str; column to groupby (pivot) off
    :param cols: list[str]; list of columns to aggregate values on.
    :return: pd.DataFrame; merged groupby aggregates DataFrame
    """
    cols = [col for col in cols if col in in_df.columns]
    return merge_dfs([gby_uniqlists(in_df, pivot_col=pivot_col, col=col)
                      for col in cols], on=pivot_col)


def nonuniq(orig_df):
    """
    return DataFrame of sorted non-uniq counts of fields (greater than 1).
    """
    return (orig_df.apply(pd.Series.nunique)
            .sort_values(ascending=False)
            .reset_index(name='uniq_count')
            .pipe(lambda x: x[x['uniq_count'] > 1])
            )


def update_sub(orig_df, pivot_col=None, pivot_val=None, new_col=None, new_val=None):
    """
    update a subset of DataFrame based on fixed boolean matching.
    """
    out_df = orig_df.copy()
    out_df.loc[out_df[pivot_col] == pivot_val, new_col] = new_val
    return out_df


def sorted_cols(orig_df):
    """
    sort columns by name and return df
    """
    cols = sorted(orig_df.columns)
    return orig_df[cols]


def ddupe_with_precision(orig_df, precision=10):
    """
    round original DataFrame to a given precision, drop_duplicates,
        return original dataframe with those rows indexed.
    """
    return orig_df.loc[orig_df.round(precision).drop_duplicates().index]


def mapping_2cols(orig_df, colx, coly):
    """
    return dict mapping vals in colx to coly. Order is important.
    """
    return dict(orig_df[[colx, coly]].drop_duplicates().values)


def newcol_mapped(orig_df, orig_col, new_col, mapping):
    """
    map a new column from an old column and a mapping dictionary
    """
    new_vals = orig_df[orig_col].map(mapping).values
    return orig_df.assign(**{new_col: new_vals})


def prepend_comment(orig_df):
    """
    prepend the first column name with a '#'
    """
    input_df = orig_df.copy()
    first_col = input_df.columns[0]
    return input_df.rename(columns={first_col: '#' + first_col})


def remove_prepended_comment(orig_df):
    """
    remove the '#' prepend to the first column name
    Note: if there is no comment, this won't do anything (idempotency).
    """
    input_df = orig_df.copy()
    first_col = input_df.columns[0]
    return input_df.rename(columns={first_col: first_col.replace('#', "")})


def filt(orig_df, **params):
    """
    Filter DataFrame on any number of equality conditions.
    Example usage:
    >> filt_df(df,
               season="summer",
               age=(">", 18),
               sport=("isin", ["Basketball", "Soccer"]),
               name=("contains", "Armstrong")
               )
    >> a = { 'season': "summer", 'age': (">", 18)}
    >> filt_df(df, **a) # can feed in dict with **dict notation
    notes:
        any input with single value is assumed to use "equivalent" operation and is modified.
        numpy.all is used to apply AND operation element-wise across 0th axis.
        NA values are filled to False for conditions.
    """
    input_df = orig_df.copy()

    if not params.items():
        return input_df

    def equivalent(a, b): return a == b

    def greater_than(a, b): return a > b

    def greater_or_equal(a, b): return a >= b

    def less_than(a, b): return a < b

    def less_or_equal(a, b): return a <= b

    def isin(a, b): return a.isin(b)

    def notin(a, b): return ~(a.isin(b))

    def contains(a, b): return a.str.contains(b)

    def notcontains(a, b): return ~(a.str.contains(b))

    def not_equivalent(a, b): return a != b

    operation = {"==": equivalent,
                 "!=": not_equivalent,
                 ">": greater_than,
                 ">=": greater_or_equal,
                 "<": less_than,
                 "<=": less_or_equal,
                 "isin": isin,
                 "notin": notin,
                 "contains": contains,
                 "notcontains": notcontains}

    cond = namedtuple('cond', 'key operator val')

    filt_on = [(el[0], el[1]) if isinstance(el[1], tuple) else (el[0], ("==", el[1]))
               for el in params.items()]  # enforcing equivalence operation on single vals.
    conds = [cond(el[0], el[1][0], el[1][1]) for el in filt_on]
    logic = [operation[x.operator](input_df[x.key], x.val).fillna(False) for x in conds]
    return input_df[np.all(logic, axis=0)]


def remove_double_quotes(orig_df: pd.DataFrame, quote_cols, all_cols=False) -> pd.DataFrame:
    """
    Replace double quotes found in fields with two single quotes.
     This must be done for SQL queries to correctly parse fields in Hive (and others)
    """
    def replace_quotes(x):
        if isinstance(x, str):
            return x.replace("\"", "''")
        return x

    out_df = orig_df.copy()
    if all_cols:
        return out_df.applymap(replace_quotes)
    else:
        out_df[quote_cols] = out_df[quote_cols].applymap(replace_quotes)
        return out_df