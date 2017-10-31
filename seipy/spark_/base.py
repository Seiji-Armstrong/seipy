
def s3spark_init():
    """
    initialise SparkSession for use with Jupyter and s3 SQL queries
    Returns spark session
    """
    import os
    os.environ['PYSPARK_SUBMIT_ARGS'] = '--packages com.amazonaws:aws-java-sdk:1.10.34,org.apache.hadoop:hadoop-aws:2.6.0 pyspark-shell'

    from pyspark.sql import SparkSession

    spark = SparkSession \
        .builder \
        .appName("using_s3") \
        .getOrCreate()

    hadoopConf = spark.sparkContext._jsc.hadoopConfiguration()
    print("type in access key yo:")
    myAccessKey = input()
    print("type in secret key yo:")
    mySecretKey = input()

    hadoopConf.set("fs.s3.impl", "org.apache.hadoop.fs.s3native.NativeS3FileSystem")
    hadoopConf.set("fs.s3.awsAccessKeyId", myAccessKey)
    hadoopConf.set("fs.s3.awsSecretAccessKey", mySecretKey)
    return spark