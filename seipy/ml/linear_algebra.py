from scipy.spatial import distance as spd
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
    return 1 - spd.cosine(vec1, vec2)