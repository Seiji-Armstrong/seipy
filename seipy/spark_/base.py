

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
    print("type in aws access key yo:")
    myAccessKey = input()
    print("type in aws secret key yo:")
    mySecretKey = input()

    hadoopConf.set("fs.s3.impl", "org.apache.hadoop.fs.s3native.NativeS3FileSystem")
    hadoopConf.set("fs.s3.awsAccessKeyId", myAccessKey)
    hadoopConf.set("fs.s3.awsSecretAccessKey", mySecretKey)
    return spark


def register_sql(spark, files, table_name="table"):
    """
    Register a list of files as a SQL temporary view.

    parameters:
    - files is overloaded: can be one file path or list of file paths.
    - spark: pyspark.sql.SparkSession
    - table_name: this is how we will refer to table in SQL query
    Schema of files must be the same for the table

    Example usage after registering table:
    >> DF = spark.sql("SELECT * FROM table")
    """
    tempFiles = spark.read.csv(files, header=True)
    tempFiles.createOrReplaceTempView(table_name)