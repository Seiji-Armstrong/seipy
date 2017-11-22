from scipy.spatial import distance
from sklearn.metrics.pairwise import cosine_similarity


def cos_sim_sk(a, b):
    """
    Wrapper around sklearn.metrics.pairwise.cosine_similarity
    Useful for bypassing DeprecationWarning by reshaping.

    Inputs are of type numpy.ndarray
    """
    return cosine_similarity(a.reshape(1, -1), b.reshape(1, -1))[0][0]


def cos_sim_sci(vec1, vec2):
    """
    Returns cosine similarity of 2 vectors.
    Uses scipy.spatial.distance.cosine
    """
    return 1 - distance.cosine(vec1, vec2)


def distmat(fframe, metric=None, possible_metrics=False):
    """
    Generate a distance matrix from a DataFrame containing only feature columns.
        The distance metric is specified with `metric`.
    If `possible_metrics` is True, return list of possible metrics.
    """
    if possible_metrics:
        d_metric = ["braycurtis", "canberra", "chebyshev", "cityblock", "correlation",
                    "cosine", "dice", "euclidean", "hamming", "jaccard", "kulsinski",
                    "mahalanobis", "matching", "minkowski", "rogerstanimoto",
                    "russellrao", "seuclidean", "sokalmichener", "sokalsneath",
                    "sqeuclidean", "wminkowski", "yule"]
        return d_metric
    return distance.cdist(fframe.as_matrix(), fframe.as_matrix(), metric)