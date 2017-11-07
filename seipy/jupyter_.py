from os.path import basename
from IPython.display import display, HTML
import numpy as np
import pandas as pd
import sys
from .base import date_range_array, relevant_files_list, files_containing_str


def show_mem_usage():
    """
    Displays memory usage from inspection
    of global variables in this notebook

    taken from
    http://practicalpython.blogspot.com/2017/03/monitoring-memory-usage-in-jupyter.html
    """
    gl = sys._getframe(1).f_globals
    vars = {}
    for k, v in list(gl.items()):
        # for pandas dataframes
        if hasattr(v, 'memory_usage'):
            mem = v.memory_usage(deep=True)
            if not np.isscalar(mem):
                mem = mem.sum()
            vars.setdefault(id(v), [mem]).append(k)
        # work around for a bug
        elif isinstance(v, pd.Panel):
            v = v.values
        vars.setdefault(id(v), [sys.getsizeof(v)]).append(k)
    total = 0
    for k, (value, *names) in vars.items():
        if value > 1e6:
            print(names, "%.3fMB" % (value / 1e6))
        total += value
    print("%.3fMB" % (total / 1e6))


def disp(df):
    """
    display a pandas DataFrame without the index
     (wrapper of IPython.display.display)
    designed for use within Jupyter notebook
    """
    display(HTML(df.to_html(index=False)))


def jupyter_containing_str(search_str='',
                           on_docker=True,
                           git_dir='~/git/experiments/',
                           start_date='2015-01-01', end_date='2018-12-31',
                           exclude_str='checkpoint',
                           include_prefix=False,
                           prefix='notebooks/'):
    """
    return all jupyter notebooks containing search_str in specified time window.
    TO DO: extend to list of search_strings, extend to list of exclude_strings.
    """
    if on_docker:
        base_dir = "/home/jovyan/work/"
    else:
        base_dir = git_dir[:]
    dates = date_range_array(start=start_date, end=end_date)
    rel_files = relevant_files_list(base_dir, dates, exclude_str)
    files = files_containing_str(search_str, rel_files)
    if prefix[-1] == '/':
        prefix = prefix[:-1]
    if include_prefix:
        return [prefix+el.split(basename(prefix))[-1] for el in files]
    else:
        return [el.split(basename(prefix))[-1] for el in files]