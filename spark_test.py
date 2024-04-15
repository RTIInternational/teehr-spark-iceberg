import pyspark
from pyspark.sql import SparkSession
import numpy as np
from pyspark.sql.functions import pandas_udf
import pandas as pd

conf = (
    pyspark.SparkConf()
    .setAppName('app_name')
    # packages
    .set('spark.jars.packages', 'org.apache.hadoop:hadoop-aws:3.3.4,org.apache.iceberg:iceberg-spark-runtime-3.3_2.12:1.1.0,software.amazon.awssdk:bundle:2.18.31,software.amazon.awssdk:url-connection-client:2.18.31')
    # SQL Extensions
    .set('spark.sql.extensions', 'org.apache.iceberg.spark.extensions.IcebergSparkSessionExtensions')
    # Configuring Catalog
    .set('spark.sql.catalog.icebergcat', 'org.apache.iceberg.spark.SparkCatalog')
    .set('spark.sql.catalog.icebergcat.type', 'hadoop')
    .set('spark.sql.catalog.icebergcat.warehouse', 'iceberg-warehouse')
)

## Start Spark Session
spark = SparkSession.builder.config(conf=conf).getOrCreate()
print("Spark Running")

df = spark.read.parquet("/media/sf_mdenno/Downloads/usgs_2016.parquet")
# df.writeTo("protocol_1.truth").create()
df.createOrReplaceTempView("prim")

@pandas_udf("float")
def mymean(s: pd.Series) -> float:
    if len(s) == 0:
        return np.nan
    return s.mean()

spark.udf.register("mymean", mymean)
spark.sql("SELECT location_id, mymean(value) FROM prim GROUP BY location_id").show()
spark.sql("SELECT location_id, count(*) FROM prim GROUP BY location_id").show()