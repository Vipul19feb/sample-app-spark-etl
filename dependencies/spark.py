import __main__

from os import environ, listdir, path
import json
from pyspark import SparkFiles
from pyspark.sql import SparkSession

from dependencies import logging


def start_spark(app_name='my_spark_app', master='local[*]', jar_packages=[],
                files=[], spark_config={}):

    # detect execution environment
    flag_repl = not(hasattr(__main__, '__file__'))
    flag_debug = 'DEBUG' in environ.keys()

    if not (flag_repl or flag_debug):
        spark_builder = (
            SparkSession
            .builder
            .appName(app_name))
    else:
        spark_builder = (
            SparkSession
            .builder
            .master(master)
            .appName(app_name))


        spark_files = ','.join(list(files))
        spark_builder.config('spark.files', spark_files)

        # add other config params
        for key, val in spark_config.items():
            spark_builder.config(key, val)

    # create session and retrieve Spark logger object
    spark_sess = spark_builder.getOrCreate()
    spark_logger = logging.Log4j(spark_sess)

    # get config file if sent to cluster with --files
    spark_files_dir = SparkFiles.getRootDirectory()
    spark_logger.warn('spark_files_dir' + str(spark_files_dir))

    for filename in listdir(spark_files_dir):
        spark_logger.warn('filename' + str(filename))

    config_f = SparkFiles.get('configs/etl_config.json')
    spark_logger.warn('config_f' + str(config_f))

    config_files = [filename
                    for filename in listdir(spark_files_dir)
                    if filename.endswith('config.json')]
    spark_logger.warn('config_files' + str(config_files))
    if config_files:
        path_to_config_file = path.join(spark_files_dir, config_files[0])
        with open(path_to_config_file, 'r') as config_file:
            config_dict = json.load(config_file)
        spark_logger.warn('loaded config from ' + config_files[0])
    else:
        spark_logger.warn('no config file found')
        config_dict = None

    return spark_sess, spark_logger, config_dict
