import zipfile
import boto3
import io
import datetime
import requests
import pandas as pd


def get_creds(cred_fpath=None, api_path=None):
    """helper function to obtain aws keys
    """
    if cred_fpath is not None:
        print("reading keys from credentials file")
        keys = pd.read_csv(cred_fpath, sep="=")
        myAccessKey = keys.loc['aws_access_key_id ']['[default]'].strip()
        mySecretKey = keys.loc['aws_secret_access_key ']['[default]'].strip()
        myToken = ""
    else:
        r = requests.get(api_path)
        creds = r.json()
        myAccessKey = creds["AccessKeyId"]
        mySecretKey = creds["SecretAccessKey"]
        myToken = creds["Token"]
    return myAccessKey, mySecretKey, myToken


def s3zip_func(s3zip_path, _func=None, cred_fpath=None, api_path=None, num_files=-1, verbose=False, include=None, **kwargs):
    """
    unzip a zip file on s3 and perform func with kwargs.
     func must accept `fpath` and `fname` as key word arguments.
    fpath: pointer to unzipped subfile in zip file
    fname: str of subfile in zip file
    num_files: int: if -1 include all files

    adapted from https://stackoverflow.com/questions/23376816/python-s3-download-zip-file
    """

    def operate(subfile, _func, verbose, **kwargs):
        if verbose:
            print("current time is {}".format(datetime.datetime.now().strftime('%Y-%m-%d %H:%M:%S')))
            print("{} opened.".format(subfile))
        if _func is None:
            return subfile
        else:
            return _func(fpath=zipf.open(subfile), fname=subfile, **kwargs)

    s3bucket, s3zip = s3zip_path.split("s3://")[-1].split('/', 1)
    myAccessKey, mySecretKey, myToken = get_creds(cred_fpath=cred_fpath, api_path=api_path)

    session = boto3.session.Session(
        aws_access_key_id=myAccessKey,
        aws_secret_access_key=mySecretKey,
        aws_session_token=myToken
    )

    s3 = session.resource("s3")
    bucket = s3.Bucket(s3bucket)
    obj = bucket.Object(s3zip)

    with io.BytesIO(obj.get()["Body"].read()) as tf:

        # rewind the file
        tf.seek(0)

        # Read the file as a zipfile and process the members
        with zipfile.ZipFile(tf, mode='r') as zipf:
            zipfiles = zipf.namelist()
            if include is None:
                include = zipfiles
            if num_files == -1:
                num_files = len(zipfiles)
            results = [operate(subfile, _func, verbose, **kwargs)
                       for subfile in zipfiles[:num_files] if subfile in include]
    return results
