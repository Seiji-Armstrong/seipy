from matplotlib import pyplot as plt
import seaborn as sns
import pandas as pd
from ..ml.linear_algebra import distmat


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


def visualise_dist(fframe=None, metric='euclidean'):
    """
    Plot a distance matrix from a DataFrame containing only feature columns.
        The plot is a heatmap, and the distance metric is specified with `metric`
    """

    plt.figure(figsize=(14, 10))
#     ax = plt.gca()
    sns.heatmap(distmat(fframe, metric=metric))
    plt.show()