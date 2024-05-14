# import os
# import duckdb
# import numpy as np
# from pyspark.sql.functions import pandas_udf
# import pandas as pd
# from urllib.request import urlretrieve
# import gc
from pyspark.sql import SparkSession
from pyspark import SparkConf


def count_rows(spark):
    print("Counting rows")
    sdf = spark.sql("SELECT count(*) FROM proto.joined")
    print("SQL executed")
    print(sdf)
    print("sdf.first()")
    print(sdf.first())
    print("Counted rows")


if __name__ == "__main__":
    print("Starting")
    conf = (
        SparkConf()
        .setAppName('TestJupyter')
        .set("spark.sql.extensions", "org.apache.iceberg.spark.extensions.IcebergSparkSessionExtensions")
        .set("spark.sql.catalog.demo", "org.apache.iceberg.spark.SparkCatalog")
        .set("spark.sql.catalog.demo.type", "rest")
        .set("spark.sql.catalog.demo.uri", "http://iceberg-rest:8181")
        .set("spark.sql.catalog.demo.io-impl", "org.apache.iceberg.aws.s3.S3FileIO")
        .set("spark.sql.catalog.demo.warehouse", "s3://warehouse/")
        .set("spark.sql.catalog.demo.s3.endpoint", "http://minio:9000")
        .set("spark.sql.catalog.demo.s3.path-style-access", "true")
        .set("spark.sql.catalog.demo.s3.access-key-id", "minio")
        .set("spark.sql.catalog.demo.s3.secret-access-key", "password")   
        .set("spark.sql.defaultCatalog", "demo")
        .set("spark.jars.packages", "org.apache.iceberg:iceberg-spark-runtime-3.5_2.12:1.5.0,org.apache.iceberg:iceberg-aws-bundle:1.5.0,org.apache.hadoop:hadoop-aws:3.3.4")
        # .setMaster("k8s://http://127.0.0.1:8001")
        # .set("spark.kubernetes.container.image", "apache/spark:3.5.1-scala2.12-java11-python3-r-ubuntu")
        # .set("deploy-mode", "cluster")
        # .set("spark.executor.memory", "12g")
        # .set("spark.driver.memory", "12g")
        # .set("spark.executor.instances", 1)
        # .set("spark.executor.cores", 1)
    )
    print("Creating spark session")
    spark = SparkSession.builder.config(conf=conf).getOrCreate()
    print("Spark session created")
    count_rows(spark)
