import numpy as np


def matlab3d_style(np3_array):
    """
    Return MATLAB style display of 3d Numpy array
    :param np3_array: np.array with shape (x,y,3)
    :return:
    """
    a = np3_array.copy()
    print(np.array([a[:, :, 0], a[:, :, 1], a[:, :, 2]]))

    return np3_array


def identity(x):
    return x


def num_cols(arr: np.array):
    """
    returns number of columns in numpy array
    Note, this work around is necessary as the shape tuple for a one column array
        returns (x, ), so arr.shape[1] doesn't always work.
    """
    return arr[:, np.newaxis].shape[-1]
