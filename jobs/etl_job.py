import os
from pyspark import SparkContext
from pyspark.sql import Row
from pyspark.sql.types import StringType
from pyspark.sql.dataframe import DataFrame
from pyspark.sql.column import Column, _to_java_column
from pyspark.sql.functions import json_tuple, arrays_zip
from pyspark.sql.functions import from_utc_timestamp, unix_timestamp
from pyspark.sql.functions import regexp_replace, split, substring, expr, element_at, lit

from dependencies.spark import start_spark


def main():
    """Main ETL script definition.

    :return: None
    """
    # start Spark application and get Spark session, logger and config
    spark, log, config = start_spark(
        app_name='sample-app',
        files=['configs/etl_config.json'])

    # log that main ETL job is starting
    log.warn('etl_job is up-and-running')

    # no KeyError check here; below values must be present
    db_insert = config['mysql']['insert']
    file_write = config['file']['write']
    load_path = config['file']['load']['path']
    load_db_info = config['mysql']['load']
    save_db_info = config['mysql']['save']
    save_path = config['file']['save']['path']
    types = config['transform']

    # execute ETL pipeline

    log_data = extract_data(spark, load_path)
    category_data = extract_data_from_db(spark, load_db_info)
    data_transformed = transform_data(log_data, category_data, types)

    if db_insert:
        insert_data(data_transformed, save_db_info)
    if file_write:
        save_hdfs(data_transformed, save_path)

    # log the success and terminate Spark application
    log.warn('test_etl_job is finished')
    spark.stop()
    return None


def extract_data(spark, load_path):
    """Load data from Parquet file format.

    :param spark: Spark session object.
    :param load_path: Log files path
    :return: Spark DataFrame.
    """
    df = (
        spark
        .read
        .parquet(load_path))
    return df


def extract_data_from_db(spark, load_db_info):
    """Load data from MySQL

    :param spark: Spark session object.
    :param load_db_info: MySQL Connection Info.
    :return: Spark DataFrame
    """
    df = (
        spark
        .read
        .format('jdbc')
        .options(**load_db_info)
        .load())
    return df


def insert_data(df, save_db_info):
    """Insert data to MySQL

    :param df: Spark DataFrame
    :param save_db_info: MySQL Connection Info.
    :return: None
    """
    (
        df.
        write
        .format('jdbc')
        .options(**save_db_info)
        .mode('append')
        .save())
    return None


def explode_outer(col):
    sc = SparkContext._active_spark_context
    _explode_outer = sc._jvm.org.apache.spark.sql.functions.explode_outer
    return Column(_explode_outer(_to_java_column(col)))


def transform(self, f):
    return f(self)


DataFrame.transform = transform


def transform_data(log_data, category_data, types):
    """Transform Input DataFrame accordingly

    :param log_data: Input log DataFrame
    :param category_data: Input category DataFrame
    :param types: preprocess types
    :return: Final Output Format DataFrame
    """
    return (
        log_data
        .transform(lambda df: select_default(df, types['default']))
        .transform(lambda df: union_all(df, select_type1(log_data, types['type1'])))
        .transform(lambda df: union_all(df, select_type2(log_data, types['type2'])))
        .transform(lambda df: union_all(df, select_type3(log_data, types['type3'])))
        .transform(adjust_timestamp_format)
        .transform(adjust_timezone)
        .transform(split_timestamp)
        .transform(remove_comma)
        .transform(remove_quote)
        .transform(explode_list)
        .transform(select_valid_id)
        .transform(lambda df: join_dfs(df, category_data))
        .transform(drop_duplicates)
    )


def select_default(df, *default):
    """Select fields for default log format; 154992

    :param df: Input DataFrame
    :param default: A list of shopping_sites_id
    :return: Output DataFrame
    """
    return (
        df
        .filter(df.logtype.isin('login', 'purchase', 'cart') & df.info.siteseq.isin(*default))
        .select('maid', 'info.siteseq', 'userid', 'timestamp', 'logtype',
                json_tuple(df.custom, 'productCode', 'productName').alias('productCode', 'productName'))
        .withColumnRenamed('info.siteseq', 'siteseq')
        .unionAll((
            df
            .filter(df.logtype.isin('view') & df.info.siteseq.isin(*default))
            .select('maid', 'info.siteseq', 'userid', 'timestamp', 'logtype',
                    json_tuple(df.custom, 'rb:itemId', 'rb:itemName').alias('productCode', 'productName'))
            .withColumnRenamed('info.siteseq', 'siteseq'))))


def select_type1(df, *type1):
    """Select fields for log format; -48

    :param df: Input DataFrame
    :param type1: A list of shopping_sites_id
    :return: Output DataFrame
    """
    return (
        df
        .filter(df.logtype.isin('login', 'purchase') & df.info.siteseq.isin(*type1))
        .select('maid', 'info.siteseq', 'userid', 'timestamp', 'logtype',
                json_tuple(df.custom, 'goodsCode', 'goodsName').alias('productCode', 'productName'))
        .withColumnRenamed('info.siteseq', 'siteseq')
        .unionAll((
            df
            .filter(df.logtype.isin('cart') & df.info.siteseq.isin(*type1))
            .select('maid', 'info.siteseq', 'userid', 'timestamp', 'logtype',
                    json_tuple(df.custom, 'goodsCode', 'name').alias('productCode', 'productName'))
            .withColumnRenamed('info.siteseq', 'siteseq')))
        .unionAll((
            df
            .filter(df.logtype.isin('view') & df.info.siteseq.isin(*type1))
            .select('maid', 'info.siteseq', 'userid', 'timestamp', 'logtype',
                    json_tuple(df.custom, 'tas:productCode', 'og:title').alias('productCode', 'productName'))
            .withColumnRenamed('info.siteseq', 'siteseq'))))


def select_type2(df, *type2):
    """Select fields for log format; 155138

    :param df: Input DataFrame
    :param type2: A list of shopping_sites_id
    :return: Output DataFrame
    """
    stage_df = (
        df
        .filter(df.logtype.isin('view') & df.info.siteseq.isin(*type2))
        .select('maid', 'info.siteseq', 'userid', 'timestamp', 'logtype',
                json_tuple(df.custom, 'og:url', 'og:title').alias('productCode', 'productName'))
        .withColumnRenamed('info.siteseq', 'siteseq'))
    stage_df = stage_df.withColumn('productCode', split(stage_df['productCode'], '/'))

    return (
        df
        .filter(df.logtype.isin('login', 'purchase', 'cart') & df.info.siteseq.isin(*type2))
        .select('maid', 'info.siteseq', 'userid', 'timestamp', 'logtype',
                json_tuple(df.custom, 'productCode', 'productName').alias('productCode', 'productName'))
        .withColumnRenamed('info.siteseq', 'siteseq')
        .unionAll((
            stage_df.select('maid', 'siteseq', 'userid', 'timestamp', 'logtype',
                            element_at(stage_df.productCode, -1).alias('productCode'), 'productName')
        )))


def select_type3(df, *type3):
    """Select fields for log format; 4550

    :param df: Input DataFrame
    :param type3: A list of shopping_sites_id
    :return: Output DataFrame
    """
    return (
        df
        .filter(df.logtype.isin('login', 'purchase', 'cart') & df.info.siteseq.isin(*type3))
        .select('maid', 'info.siteseq', 'userid', 'timestamp', 'logtype',
                json_tuple(df.custom, 'productCode', 'productName').alias('productCode', 'productName'))
        .withColumnRenamed('info.siteseq', 'siteseq')
        .unionAll((
            df
            .filter(df.logtype.isin('view') & df.info.siteseq.isin(*type3))
            .select('maid', 'info.siteseq', 'userid', 'timestamp', 'logtype',
                    json_tuple(df.custom, 'tas:productCode', 'Title').alias('productCode', 'productName'))
            .withColumnRenamed('info.siteseq', 'siteseq'))))


def union_all(df1, df2):
    """Union All two DataFrames

    :param df1: Input DataFrame
    :param df2: Input DataFrame
    :return: Output DataFrame
    """
    return (
        df1
        .unionAll(df2)
    )


def adjust_timestamp_format(df):
    """Adjust timestamp format so that all can have the same format

    :param df: Input DataFrame
    :return: Output DataFrame
    """
    return (
        df
        .withColumn('timestamp', regexp_replace(df.timestamp, r'(\d+-\d+-\d+T\d+:\d+:\d+)Z', '$1.000Z')))


def adjust_timezone(df):
    """Adjust timezone to KST from UTC

    :param df: Input DataFrame
    :return: Output DataFrame
    """
    return (
        df
        .withColumn('timestamp', from_utc_timestamp(unix_timestamp(df['timestamp'], "yyyy-MM-dd'T'HH:mm:ss.SSSXXX")
                                                    .cast('timestamp'), 'KST')))


def split_timestamp(df):
    """Split timestamp to transaction_time and transaction_date

    :param df: Input DataFrame
    :return: Output DataFrame
    """
    stage_df = df.withColumn('split_timestamp', split(df['timestamp'], ' '))
    return (
        stage_df
        .select('maid', 'siteseq', 'userid', 'logtype', stage_df['split_timestamp']
                .getItem(0).alias('transaction_date'), stage_df['split_timestamp']
                .getItem(1).alias('transaction_time'), 'productCode', 'productName'))


def remove_comma(df):
    """Remove any comma in string within column

    :param df: Input DataFrame
    :return: Output DataFrame
    """
    return (
        df
        .withColumn('productCode', regexp_replace(df.productCode, r"[^\"](\,+)|(\,+)[^\"]", ""))
        .withColumn('productName', regexp_replace(df.productName, r"[^\"](\,+)|(\,+)[^\"]", ""))
    )


def remove_quote(df):
    """Remove any quotation marks in string within column

    :param df: Input DataFrame
    :return: Output DataFrame
    """
    return (
        df
        .withColumn('productCode', split(regexp_replace(df.productCode, r"(^\[)|(\]$)|(\")", ""), ","))
        .withColumn('productName', split(regexp_replace(df.productName, r"(^\[)|(\]$)|(\")", ""), ","))
    )


def explode_list(df):
    """Explode a list of products into separated rows

    :param df: Input DataFrame
    :return: Output DataFrame
    """
    return (
        df
        .withColumn('tmp', arrays_zip('productCode', 'productName'))
        .withColumn('tmp', explode_outer('tmp'))
        .select('maid', 'siteseq', 'userid', 'transaction_date', 'transaction_time',
                'logtype', 'tmp.productCode', 'tmp.productName')
        .withColumnRenamed('tmp.productCode', 'productCode')
        .withColumnRenamed('tmp.productName', 'productName')
    )


def select_valid_id(df):
    """Use maid as userid if userid doesn't exist

    :param df: Input DataFrame
    :return: Output DataFrame
    """
    return (
        df
        .filter(df.userid.isNull())
        .select('maid', 'siteseq', 'transaction_date', 'transaction_time', 'logtype', 'productCode', 'productName')
        .withColumnRenamed('maid', 'userid')
    ).unionAll(
        df.filter(df.userid.isNotNull())
        .select('userid', 'siteseq', 'transaction_date', 'transaction_time', 'logtype', 'productCode', 'productName'))


def drop_duplicates(df):
    """Drop duplicate rows

    :param df: Input DataFrame
    :return: Output DataFrame
    """
    return (
        df.dropDuplicates()
    )


def join_dfs(df1, df2):
    """Join two dfs on keys and select fields needed

    :param df1: log_data DataFrame
    :param df2: category_info DataFrame
    :return: Final result DataFrame
    """
    # column rename lower -> upper
    for column in df1.columns:
        df1 = df1.withColumnRenamed(column, column.upper())

    # select only the ones that with a valid productCode + login data
    stage_df = df1.join(df2, (df1.SITESEQ == df2.SHOPPING_ID) & (df1.PRODUCTCODE == df2.ITEM_CODE))
    login_df = (
        df1
        .filter(df1.LOGTYPE == 'login')
        .select('USERID', 'SITESEQ', 'TRANSACTION_DATE', 'TRANSACTION_TIME', 'LOGTYPE')
        .withColumnRenamed('USERID', 'USER_ID')
        .withColumnRenamed('SITESEQ', 'SHOPPING_ID')
        .withColumnRenamed('LOGTYPE', 'LOG_TYPE')
        .withColumn('ITEM_CODE', lit(None).cast(StringType()))
        .withColumn('INTG_ID', lit(None).cast(StringType()))
        .withColumn('ITEM_NAME', lit(None).cast(StringType()))
        .withColumn('CAT1', lit(None).cast(StringType()))
        .withColumn('CAT2', lit(None).cast(StringType()))
        .withColumn('CAT3', lit(None).cast(StringType()))
        .withColumn('CAT4', lit(None).cast(StringType()))
        .withColumn('INTG_CAT1', lit(None).cast(StringType()))
        .withColumn('INTG_CAT2', lit(None).cast(StringType()))
        .withColumn('INTG_CAT3', lit(None).cast(StringType()))
        .withColumn('INTG_CAT4', lit(None).cast(StringType()))
    )

    return (
        (
            stage_df
            .select('USERID', 'SITESEQ', 'TRANSACTION_DATE', 'TRANSACTION_TIME',
                    'LOGTYPE', 'INTG_ID', 'ITEM_CODE', 'ITEM_NAME', 'CAT1', 'CAT2', 'CAT3', 'CAT4',
                    'INTG_CAT1', 'INTG_CAT2', 'INTG_CAT3', 'INTG_CAT4')
            .withColumnRenamed('USERID', 'USER_ID')
            .withColumnRenamed('LOGTYPE', 'LOG_TYPE')
            .withColumnRenamed('SITESEQ', 'SHOPPING_ID')
            .unionAll(login_df))
        .withColumn('USER_ID', expr('substring(USER_ID, 1, 100)')))


def write_data(df, save_path):
    """Collect data locally and write to CSV.

    :param df: DataFrame to print.
    :param save_path: A Path to save files
    :return: None
    """
    (df
     .coalesce(1)
     .write
     .option('encoding', 'utf-8')
     .csv(save_path, mode='overwrite', header=True))
    return None


def save_hdfs(df, save_path):
    """Collect data and save to HDFS as parquet format

    :param df: DataFrame to save
    :param save_path: A HDFS path to save files
    :return: None
    """
    (
        df
        .write
        .save(save_path, format='parquet', mode='append'))
    return None


def run_test():
    """Running test function

    :return: None
    """
    # start Spark application and get Spark session, logger and config
    spark, log, config = start_spark(
        app_name='my_etl_test_job',
        files=['configs/etl_config.json'])
    create_test_data(spark)
    spark.stop()

    return None


def test_transform(log_data, types):
    """Test transform function

    :param log_data: Input DataFrame
    :param types: shopping_sites_id
    :return: Output DataFrame
    """
    return (
        log_data
        .transform(lambda df: select_default(df, types['default']))
        .transform(lambda df: union_all(df, select_type1(log_data, types['type1'])))
        .transform(lambda df: union_all(df, select_type2(log_data, types['type2'])))
        .transform(lambda df: union_all(df, select_type3(log_data, types['type3'])))
        .transform(adjust_timestamp_format)
        .transform(adjust_timezone)
        .transform(split_timestamp)
        .transform(remove_comma)
        .transform(remove_quote)
        .transform(explode_list)
    )


def create_test_data(spark):
    """Create test data and run test_transform()

    This function creates both both pre- and post- transformation data
    saved as Parquet files in tests/test_data. This will be used for
    unit tests as well as to load as part of the example ETL job.
    :return: None
    """

    # create example data from scratch
    local_records = [
        Row(maid='test_maid1', info=Row(siteseq='4550'), userid='uid-1', custid='cid-1',
            timestamp='2019-06-01T01:43:09.000Z', logtype='purchase',
            custom='{"goodsCode": ["4550-pc1"], "goodsName": ["4550-pn1"]}'),
        Row(maid='test_maid2', info=Row(siteseq='155138'), userid='uid-2', custid='cid-2',
            timestamp='2019-06-01T01:43:09.000Z', logtype='purchase',
            custom='{"goodsCode": ["155138-pc1"], "goodsName": ["155138-pn1"]}'),
        Row(maid='test_maid3', info=Row(siteseq='-48'), userid='uid-3', custid='cid-3',
            timestamp='2019-06-01T01:43:09.000Z', logtype='purchase',
            custom='{"goodsCode": ["-48-pc1", "-48-pc2"], "goodsName":["-48-pn1", "-48-pn2"]}'),
        Row(maid='test_maid4', info=Row(siteseq='155138'), userid='uid-4', custid='cid-4',
            timestamp='2019-06-01T01:43:09.000Z', logtype='purchase',
            custom='{"goodsCode": ["155138-pc1"], "goodsName": ["155138-pn1"]}')
    ]

    df = spark.createDataFrame(local_records)
    base_dir = os.path.dirname(os.path.dirname(__file__))

    # write to Parquet file format
    (df
     .coalesce(1)
     .write
     .parquet(os.path.join(base_dir, 'tests/test_data/test_logs'), mode='overwrite'))

    # create transformed version of data
    types = {
        "default": [154992],
        "type1": [-48],
        "type2": [155138],
        "type3": [4550]
    }
    df_tf = test_transform(df, types)

    # write transformed version of data to Parquet
    (df_tf
     .coalesce(1)
     .write
     .parquet(os.path.join(base_dir, 'tests/test_data/test_logs.comp'), mode='overwrite'))

    return None


# entry point for PySpark ETL application
if __name__ == '__main__':
    main()
