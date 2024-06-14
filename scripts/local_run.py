"""
curl -s https://repo1.maven.org/maven2/com/amazonaws/aws-java-sdk-bundle/1.12.262/aws-java-sdk-bundle-1.12.262.jar \
    -Lo /home/matt/repos/teehr-spark-iceberg/.venv/lib64/python3.10/site-packages/pyspark/jars/aws-java-sdk-bundle-1.12.262.jar

curl -s https://repo1.maven.org/maven2/org/apache/hadoop/hadoop-aws/3.3.4/hadoop-aws-3.3.4.jar \
    -Lo /home/matt/repos/teehr-spark-iceberg/.venv/lib64/python3.10/site-packages/pyspark/jars/hadoop-aws-3.3.4.jar

"""
from pyspark.sql import SparkSession
from pyspark import SparkConf

SparkSession.builder.master("local[*]").getOrCreate().stop()

conf = (
    SparkConf()
    .setAppName('TEEHR')
    .set("spark.hadoop.fs.s3a.impl", "org.apache.hadoop.fs.s3a.S3AFileSystem")
    .set("spark.hadoop.fs.s3a.aws.credentials.provider", "org.apache.hadoop.fs.s3a.AnonymousAWSCredentialsProvider")
)
## Start Spark Session
spark = SparkSession.builder.config(conf=conf).getOrCreate()
spark.sparkContext.getConf().getAll()

# obs = spark.read.parquet("s3a://ciroh-rti-public-data/teehr/protocols/science-eval/timeseries/usgs*.parquet")
# print(obs.show(5))