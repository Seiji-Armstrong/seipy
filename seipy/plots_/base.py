from matplotlib import pyplot as plt
import pandas as pd


def scatter_2d(orig_df: pd.DataFrame, colx, coly, label_col,
               xmin=None, xmax=None, ymin=None, ymax=None):
    """
    Return scatter plot of 2 columns in a DataFrame, taking labels as colours.
    """
    plt.scatter(orig_df[colx], orig_df[coly],
                c=orig_df[label_col].values, cmap='viridis')
    plt.colorbar()
    plt.xlim(xmin, xmax)
    plt.ylim(ymin, ymax)
    plt.show()