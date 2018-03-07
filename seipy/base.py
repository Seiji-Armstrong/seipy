import subprocess
from os import walk
from os.path import join
from functools import reduce
from collections import Counter, namedtuple
import ast
import mmap
import re
import json
import pandas as pd


class Stack:
    """
    simple stack class
    """
    def __init__(self):
        self.items = []

    def isEmpty(self):
        return self.items == []

    def push(self, item):
        self.items.insert(0, item)

    def pop(self):
        return self.items.pop(0)

    def peek(self):
        return self.items[0]

    def size(self):
        return len(self.items)


class LabeledData(dict):
    """Container object for datasets

    Dictionary-like object that exposes its keys as attributes.

    (copied from sklearn.datasets.base.Bunch)
    """

    def __init__(self, **kwargs):
        # type: (object) -> object
        super(LabeledData, self).__init__(kwargs)

    def __setattr__(self, key, value):
        self[key] = value

    def __dir__(self):
        return self.keys()

    def __getattr__(self, key):
        try:
            return self[key]
        except KeyError:
            raise AttributeError(key)

    def __setstate__(self, state):
        # https://github.com/scikit-learn/scikit-learn/issues/6196
        pass


def issue_shell_command(cmd: str, my_env=None):
    """
    Issues a command in a shell and returns the result as str.

    Parameters:
    cmd - command to be issued (str)

    In python3.x, stdout,stderr are both b'' (byte string literal: bytes object)
        and must be decoded to UTF-8 for string concatenation etc
    Example usage (simple):
    >> issue_shell_command(cmd="ls")
    Example usage (more involved):
    >> s3dir = "s3://..."; issue_shell_command("aws s3 ls --recursive {}".format(s3dir))
    """
    pipe = subprocess.run(cmd, shell=True, stdout=subprocess.PIPE,
                          stderr=subprocess.PIPE, env=my_env)

    return pipe.stdout.strip().decode('UTF-8') + '\n' + pipe.stderr.strip().decode('UTF-8')


def merge_dicts(*dict_args):
    """
    Given any number of dicts, shallow copy and merge into a new dict,
    precedence goes to key value pairs in latter dicts.
    """
    result = {}
    for dictionary in dict_args:
        result.update(dictionary)
    return result


def merge_two_dicts(dict_1, dict_2):
    """
    Given two dicts, return one merged dict.
    """
    return {**dict_1, **dict_2}


def merge_multi_dicts(*dict_args):
    """
    Given multiple dicts, return one merge dict by calling `reduce` on `merge_two_dicts`.
    ALternative: reduce(lambda d1,d2: {**d1,**d2}, dict_args[0])
    """
    return reduce(merge_two_dicts, dict_args[0])


def swap_key_val_dict(a_dict):
    """
    swap the keys and values of a dict
    """
    return {val: key for key, val in a_dict.items()}


def enumerate_with_prefix(a_list, prefix='pre_'):
    """
    given a list, return a list enumerated with prefix.
    """
    num_digits = len(str(len(a_list)))  # eg 5 -> 1, 15 -> 2, 150 -> 3 etc.
    enum_list = [prefix + str(idx).zfill(num_digits)
                 for idx, el in enumerate(a_list)]
    return enum_list


def relevant_files(root_dir, include_regex='', exclude="*****"):
    """Return list of files with inclusion regex and exclusion regex.

    inputs:
    "root_dir" is the root directory
    "include_regex" is the string that is searched for within filenames
    "exclude_regex" is the string that will exclude files if found in name.

    returns:
    list of filenames in all subsequent folders, matched with include_regex.
    """
    f_list = []
    for (dir_path, _, file_names) in walk(root_dir):
        f_list.extend(join(dir_path, filename)
                      for filename in file_names
                      if (include_regex in filename))
    f_list = [el for el in f_list if exclude not in el]
    return f_list


def relevant_files_list(root_dir, include_list=[], exclude="*****"):
    """Return list of files with inclusion regex in list and exclusion regex.

    inputs:
    "root_dir" is the root directory
    "include_list" is list of strings that is searched for within filenames
    "exclude_regex" is the string that will exclude files if found in name.

    returns:
    list of filenames in all subsequent folders, matched with include_regex.
    """
    f_list = []
    for (dir_path, _, file_names) in walk(root_dir):
        f_list.extend(join(dir_path, filename)
                      for filename in file_names
                      if any(el in filename for el in include_list))
    f_list = [el for el in f_list if exclude not in el]
    return f_list


def files_containing_str(str_, file_list):
    """
    return all files in provided file_list containing str_.
    """
    return [el for el in file_list if str_in_file(str_, el)]


def date_range_array(start='2016-12-1', end='2016-12-5', strf='%Y-%m-%d'):
    """
    Return a numpy.array of dates in range between start and stop.
    Format of date string is given by strf.
    Wrapper around pandas.date_range
    """
    return pd.date_range(start=start, end=end).map(lambda x: x.strftime(strf))


def str_in_file(str_, file_):
    """
    Returns Boolean: True if str_ in file_, otherwise False.
    First converts string to Bytes.
    Uses mmap in order not to read entire file each time.
    """
    b_ = str_.encode()
    with open(file_, 'rb', 0) as file, \
            mmap.mmap(file.fileno(), 0, access=mmap.ACCESS_READ) as s:
        if re.search(b_, s):
            return True
        return False


def str_from_collec(collection, no_space=False):
    """
    Given a collection, return a string of values
        Collection can be: column of DataFrame (Series), list, etc.

    Useful in creating fv_str strings for example, by feeding row of DataFrame
    containing only feature columns
    """
    if no_space:
        return ','.join(map(str, collection))
    else:
        return ', '.join(map(str, collection))


def uniq_tokens_in_nested_col(col_series):
    """
    Given a column in a dataframe containing lists of tokens,
        return unique tokens.
    Can also receive a list of lists, pd.Series of lists, np.array of lists.
    """
    return set([el for sublist in col_series for el in sublist])


def uniq_tokens_css(thresh, *collection_vals):
    """
    Extract unique tokens in comma separated strings, contained in a collection,
        return list of unique tokens that occur more than a threshold value.
    Apply sorted to output for reproducible output.
    """
    temp_lists = []
    for vals in collection_vals:
        temp_lists += [el.split(',') for el in vals]
    temp_fulllist = [el.strip() for sublist in temp_lists for el in sublist]
    temp_count = Counter(temp_fulllist)
    return sorted([k for k, v in list(temp_count.items()) if v > thresh])


def str_list_to_list_str(str_list, regex_pattern='[A-Z]\d+'):
    """
    Turn a string of a list into a list of string tokens.
    Tokens determined by regex_pattern
    """
    p = re.compile(regex_pattern)
    return p.findall(str_list)


def save_json(dict_to_json, save_path):
    """
    Saves dict to json file
    """
    out_file = open(save_path, "w")
    # save the dictionary to this file
    json.dump(dict_to_json, out_file, indent=4)
    # close the file
    out_file.close()
    print("json file saved at: {}".format(save_path))


def load_json(path_to_load):
    """
    Loads json from file
    """
    with open(path_to_load, 'r') as f:
        return json.load(f)


def vals_sortby_key(dict_to_sort):
    """
    sort dict by keys alphanumerically, then return vals.
    Keys should be "feat_00, feat_01", or "stage_00, stage_01" etc.
    """
    return [val for (key, val) in sorted(dict_to_sort.items())]


def diff_2_lists(list_1, list_2):
    """
    return elements not common to both lists
    Symmetric difference operator ^ is used instead of -.
    """
    return list(set(list_1) ^ set(list_2))


def file_len(fname):
    """
    count number of lines in file
    copy-pasted from http://stackoverflow.com/questions/845058/how-to-get-line-count-cheaply-in-python
    """
    with open(fname) as f:
        for i, l in enumerate(f):
            pass
    return i + 1


def literal(str_field: str):
    """
    converts string of object back to object
    example:
    $ str_literal('['a','b','c']')
    ['a', 'b', 'c'] # type is list
    """
    return ast.literal_eval(str_field)


def cumsum(collec, tuple_list_id=None, thresh=None):
    """
    Return index at which the cumulative sum of the collection exceeds the threshold.
    If the collection is a list of tuples, then a new list is created from only the
        indexed element of the tuple.
    """
    if tuple_list_id is not None:
        collec = [el[tuple_list_id] for el in collec]
    i = 0
    cumsum = 0
    if sum(collec) < thresh:
        print("threshold higher than sum of collection.")
        return len(collec)
    while cumsum < thresh:
        cumsum += collec[i]
        i += 1
    return i


def named_two_tuples(list_1, list_2, tuple_name='ntup', tuple_el_names='x y'):
    """
    Given two ordered lists of x and y
        return a list of namedtuples

    Example usage:
    >> named_two_tuples(feat_importance, feat_names,
                        tuple_name='fimp', tuple_el_names='importance name')
    """
    ntup = namedtuple(tuple_name, tuple_el_names)
    return [ntup(el[0], el[1]) for el in zip(list_1, list_2)]


def replace_rn(strobj):
    """
    There are characters such as "\r" and "\n" that perform line breaks (or returns),
    and these can create parsing dramas when reading/writing to disk.
    This helper function mitigates this by replacing it with a benign alternative.
    """
    return strobj.replace('\r', 'esc-r-').replace('\n', 'esc-n-')


def get_nested_item(data_dict, key_list):
    """
    obtain the deepest nested item in a nested dict given keys in a list.
    """
    item = data_dict.copy()
    for k in key_list:
        item = item[k]
    return item


def infix_to_postfix(infixexpr: str, token_regex: str, precedences: dict):
    """
    convert infix to postfix
    adapted from http://interactivepython.org/runestone/static/pythonds/BasicDS/InfixPrefixandPostfixExpressions.html

    Example usage:

    >> token_regex = "c[0-9]"
    >> prec = {"(": 1,
        "MINUS": 2,
        "OR": 3,
        "AND": 4}
    >> expr = '( c1 AND c2 ) MINUS c3'
    >> infix_to_postfix(expr, token_regex, prec)
    [out]:  'c1 c2 AND c3 MINUS'

    """
    op_stack = Stack()
    postfix_list = []
    token_list = infixexpr.split()
    for token in token_list:
        if re.search(token_regex, token):
            postfix_list.append(token)
        elif token == '(':
            op_stack.push(token)
        elif token == ')':
            top_token = op_stack.pop()
            while top_token != '(':
                postfix_list.append(top_token)
                top_token = op_stack.pop()
        else:
            while (not op_stack.isEmpty()) and \
               (precedences[op_stack.peek()] >= precedences[token]):
                  postfix_list.append(op_stack.pop())
            op_stack.push(token)

    while not op_stack.isEmpty():
        postfix_list.append(op_stack.pop())
    return " ".join(postfix_list)


def postfix_computer(tokens: list, logic: dict, operations: dict):
    """
    compute postfix expression given list of tokens.
     Each token in `tokens` must either be a key in `logic` or `operations`
     Example:
          op_dict = {'AND': lambda x,y: np.logical_and(x,y),
           'OR': lambda x,y: np.logical_or(x,y),
           'MINUS': lambda x,y: np.logical_and(x, np.logical_not(y))
          }
    """

    stack = []
    for token in tokens:
        if token in logic:
            stack.append(logic[token])
            continue

        op2, op1 = stack.pop(), stack.pop()
        if token in operations:
            stack.append(operations[token](op1, op2))

    return stack.pop()


def list_import_modules(library_root):
    """
    given the root dir of python library, return unique list of import modules.
      Useful for `install_requires` flag in setup.py, for example.
    """
    py_files = relevant_files(library_root, include_regex=".py", exclude=".pyc")
    import_lines = []
    for one_file in py_files:
        with open(one_file, "r") as f:
            lines = f.readlines()
        for i, line in enumerate(lines):
            if "import " in line:
                if "from" in line:
                    root = line.strip('\n').split('from ')[1]
                else:
                    root = line.strip('\n').split('import ')[1]
                root = root.split(' ')[0].split('.')[0]
                if root:
                    import_lines.append(root)
    return list(set(import_lines))
