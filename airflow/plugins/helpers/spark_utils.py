from pyspark.sql import SparkSession


class SparkUtils:
    @staticmethod
    def create_spark_session():
        spark = SparkSession \
            .builder \
            .config("spark.jars.packages", "org.apache.hadoop:hadoop-aws:2.7.4") \
            .config("spark.driver.memory", "2g") \
            .getOrCreate()
        spark.conf.set("spark.sql.shuffle.partitions", '10')
        return spark
