import sys
import os
import configparser
from pyspark.sql import SparkSession

from schemas import voucher_schema
from column_lists import json_to_estabelecimento, json_to_unidade_gestora, \
    json_to_cartao_funcinario, json_to_pagamentos, json_to_municipio, \
    json_to_uf


def create_spark_session():
    """ Creates the spark session"""
    spark = SparkSession \
        .builder \
        .config("spark.jars.packages", "org.apache.hadoop:hadoop-aws:2.7.0") \
        .getOrCreate()
    spark.conf.set("spark.sql.shuffle.partitions", "1")
    spark.conf.set("spark.sql.sources.partitionOverwriteMode", "dynamic")
    return spark


def process_fact(df, base_path, file, columns, partition):
    """Process fact tables
       Args:
           df (spark dataframe): Spark dataframe containing raw data.
           base_path (str) : Bucket/folder in s3 to save the data
           file (str): Target table filename
           columns (list of str): List of columns to retrieve from raw data.
           partition (list of str): Columns to partition target table
           """
    file_path = os.path.join(base_path, file)
    df.selectExpr(columns) \
        .coalesce(1) \
        .write.partitionBy(partition) \
        .parquet(file_path, mode="overwrite")


def process_dimension(df, base_path, file, columns, partition):
    """Process dimension tables
       Args:
           df (spark dataframe): Spark dataframe containing raw data.
           base_path (str) : Bucket/folder in s3 to save the data
           file (str): Target table filename
           columns (list of str): List of columns to retrieve from raw data.
           partition (list of str): Columns to partition target table
           """
    file_path = os.path.join(base_path, file)
    df.selectExpr(columns) \
        .dropDuplicates() \
        .coalesce(1) \
        .write.partitionBy(partition) \
        .parquet(file_path, mode="overwrite")


def main():
    """Process json raw data from S3 and generates analytical Parquet Tables from data.
       Args:
           file (str): Target parquet table filename possible values, as data is in portuguese,
                       below is an english translation for each table
                       (pagamentos, municipio, estabelecimento, unidadegestora, cartaofuncionario, uf)
                       (payment   , city     , store          , organization  , employee card    , state)
                       are implemented, these are the tables which data is exctracted from Card Vouchers.
           bucket (str): S3 bucket in which data will be saved.
           year (int): Execution date year, used to process Fact tables.
           month (int): Execution date month, used to process Fact tables.
           script_path: path to this script
    """
    file = sys.argv[1]
    bucket = sys.argv[2]
    year = sys.argv[3]
    month = str(sys.argv[4]).zfill(2)
    script_path = sys.argv[5]
    config = configparser.ConfigParser()
    config.read(os.path.join(script_path, 'etl.cfg'))

    os.environ['AWS_ACCESS_KEY_ID'] = config['ENVIRONMENT']['AWS_ACCESS_KEY_ID']
    os.environ['AWS_SECRET_ACCESS_KEY'] = config['ENVIRONMENT']['AWS_SECRET_ACCESS_KEY']

    input_data = os.path.join("s3a://", bucket, "raw_data")
    output_data = os.path.join("s3a://", bucket, "analytic_tables")

    spark = create_spark_session()

    if file == "pagamentos":
        df_vouchers = spark.read.json(os.path.join(input_data,
                                                   "vouchers_{year}_{month}.json"
                                                   ).format(year=year, month=month),
                                      schema=Staging.voucher_schema)

    else:
        df_vouchers = spark.read.json(input_data, schema=voucher_schema)

        if file == "estabelecimento":
            columns = json_to_estabelecimento
            partition = "sigla_uf_part"
        if file == "unidadegestora":
            columns = json_to_unidade_gestora
            partition = "cod_orgao_maximo_part"
        if file == "cartaofuncionario":
            columns = json_to_cartao_funcinario
            partition = "cod_tipo_cartao_part"
        if file == "municipio":
            columns = json_to_municipio
            partition = "sigla_uf_part"
        if file == 'uf':
            columns = json_to_uf
            partition = "sigla_uf_part"
        process_dimension(df_vouchers, output_data, file, columns, partition)


if __name__ == "__main__":
    main()
