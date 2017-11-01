from IPython.display import display, HTML
import numpy as np
import pandas as pd
import sys


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
