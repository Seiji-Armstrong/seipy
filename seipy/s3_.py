import zipfile
import boto3
import io


def s3zip_func(s3zip_path, func, **kwargs):
    """
    unzip a zip file on s3 and perform func with kwargs.
     func must accept `fpath` and `fname` as key word arguments.
    fpath: pointer to unzipped subfile in zip file
    fname: str of subfile in zip file

    adapted from https://stackoverflow.com/questions/23376816/python-s3-download-zip-file
    """
    s3bucket, s3zip = s3zip_path.split("s3://")[-1].split('/', 1)
    print("type in aws access key yo:")
    myAccessKey = input()
    print("type in aws secret key yo:")
    mySecretKey = input()
    session = boto3.session.Session(
        aws_access_key_id=myAccessKey,
        aws_secret_access_key=mySecretKey
    )

    s3 = session.resource("s3")
    bucket = s3.Bucket(s3bucket)
    obj = bucket.Object(s3zip)

    with io.BytesIO(obj.get()["Body"].read()) as tf:

        # rewind the file
        tf.seek(0)

        # Read the file as a zipfile and process the members
        with zipfile.ZipFile(tf, mode='r') as zipf:
            for subfile in zipf.namelist():
                print(subfile)
                func(fpath=zipf.open(subfile), fname=subfile, **kwargs)