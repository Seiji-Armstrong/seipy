import zipfile
import boto3
import io
import datetime
import pandas as pd


def get_creds(cred_fpath=None):
    """helper function to obtain aws keys
    """
    if cred_fpath is not None:
        print("reading keys from credentials file")
        keys = pd.read_csv(cred_fpath, sep="=")
        myAccessKey = keys.loc['aws_access_key_id ']['[default]'].strip()
        mySecretKey = keys.loc['aws_secret_access_key ']['[default]'].strip()
    else:
        print("type in aws access key yo:")
        myAccessKey = input()
        print("type in aws secret key yo:")
        mySecretKey = input()
    return myAccessKey, mySecretKey


def s3zip_func(s3zip_path, _func, include: list = [], exclude: list = [], cred_fpath=None, verbose=False, **kwargs):
    """
    unzip a zip file on s3 and perform func with kwargs.
     func must accept `fpath` and `fname` as key word arguments.
    fpath: pointer to unzipped subfile in zip file
    fname: str of subfile in zip file

    adapted from https://stackoverflow.com/questions/23376816/python-s3-download-zip-file
    """
    s3bucket, s3zip = s3zip_path.split("s3://")[-1].split('/', 1)
    myAccessKey, mySecretKey = get_creds(cred_fpath=cred_fpath)

    session = boto3.session.Session(
        aws_access_key_id=myAccessKey,
        aws_secret_access_key=mySecretKey
    )

    s3 = session.resource("s3")
    bucket = s3.Bucket(s3bucket)
    obj = bucket.Object(s3zip)

    results = []

    with io.BytesIO(obj.get()["Body"].read()) as tf:

        # rewind the file
        tf.seek(0)

        # Read the file as a zipfile and process the members
        with zipfile.ZipFile(tf, mode='r') as zipf:
            for subfile in zipf.namelist():
                if verbose:
                    print("current time is {}".format(datetime.datetime.now().strftime('%Y-%m-%d %H:%M:%S')))
                if include:
                    if subfile in include:
                        if verbose:
                            print("{} opened.".format(subfile))
                        result = _func(fpath=zipf.open(subfile), fname=subfile, **kwargs)
                        results.append((subfile, result))
                else:
                    if subfile in exclude:
                        if verbose:
                            print("{} skipped.".format(subfile))
                    else:
                        if verbose:
                            print("{} opened.".format(subfile))
                        result = _func(fpath=zipf.open(subfile), fname=subfile, **kwargs)
                        results.append((subfile, result))
    return results
