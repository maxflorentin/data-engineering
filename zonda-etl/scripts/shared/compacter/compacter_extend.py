import argparse
import time
import os
import subprocess

from pyspark.sql import SparkSession
from math import ceil
import math


def get_contexts():
    spark = SparkSession.builder \
        .enableHiveSupport() \
        .appName("DataCompacter") \
        .getOrCreate()
    global log
    log = spark._jvm.org.apache.log4j.LogManager.getLogger('compacter')
    level = spark._jvm.org.apache.log4j.Level.INFO
    log.setLevel(level)
    return spark


def str2bool(v):
    if isinstance(v, bool):
        return v
    if v.lower() in ('yes', 'true', 't', 'y', '1'):
        return True
    elif v.lower() in ('no', 'false', 'f', 'n', '0'):
        return False
    else:
        raise argparse.ArgumentTypeError('Boolean value expected')


def get_current_epoch():
    return int(round(time.time() * 1000))


class Compacter:
    def __init__(self, **kwargs):
        self.spark = get_contexts()

        self.path = kwargs.get('path')
        self.partition_name = kwargs.get('partition_name')
        self.prefix = kwargs.get('prefix')
        self.file_format = kwargs.get('file_format')
        self.partition_date = os.path.expandvars('${partition_date}')
        self.current_epoch = get_current_epoch()
        self.add_partition = kwargs.get('add_partition')
        self.schema_table = kwargs.get('schema_table')
        self.next_partition = os.path.expandvars('${next_date_from}')

    def _get_output(self):
        return "/user/{user}/.sparkStaging/tmp/{app_id}/{current_epoch}".format(user=self.spark.sparkContext.sparkUser(), app_id=self.spark._jsc.sc().applicationId(), current_epoch=self.current_epoch)

    def _get_file_count(self, path):
        df = self.spark.read.json(path)
        file_count = df.count()
        return file_count

    @staticmethod
    def _list_directory(path):
        cmd = 'hdfs dfs -find {}/*.csv'.format(path)
        return subprocess.check_output(cmd, shell=True).decode('utf-8').strip().split('\n')

    @staticmethod
    def _move_files(source, target):
        log.info("path source: {}/*".format(source))
        cmd = 'hdfs dfs -mv {}/* {}'.format(source, target)
        return subprocess.check_output(cmd, shell=True).decode('utf-8').strip().split('\n')

    @staticmethod
    def _delete_path(path):
        cmd = 'hdfs dfs -rm -r {}/*'.format(path)
        return subprocess.check_output(cmd, shell=True).decode('utf-8').strip().split('\n')

    def execute(self):

        cmd = "hdfs dfs -du -s {}/{}={}".format(self.path, self.partition_name, self.partition_date)
        log.info("command to exec: '{}'".format(cmd))
        result = subprocess.check_output(cmd, shell=True).decode('utf-8').strip().split('  ')[0]
        log.info("total files of path size: '{}'".format(result))

        block_size = self.spark._jsc.hadoopConfiguration().get("dfs.blocksize")
        log.info("hdfs block size: '{}'".format(block_size))
        print("result: {}".format(result))
        print("block_size: {}".format(block_size))
        print("calculo: {}".format(ceil(int(result) / int(block_size))))
        repartition_factor = int(math.ceil(float(result) / float(block_size)))
        print("repartition_factor: {}".format(repartition_factor))
        log.info("files count after compactation: '{}'".format(repartition_factor))

        partition_path = "{}/{}={}".format(self.path, self.partition_name, self.partition_date)
        log.info("path to read: {}".format(partition_path))

        if self.file_format == 'parquet':
            df = self.spark.read.parquet("{}".format(partition_path))
            file_input_count = df.count()
            log.info("totally Input files has {} rows".format(file_input_count))
            df.show()

            tmp_path = self._get_output()
            df.repartition(repartition_factor).write.mode('overwrite').parquet(tmp_path)

        elif self.file_format == 'json':
            df = self.spark.read.json("{}".format(partition_path))
            file_input_count = df.count()
            log.info("totally Input files has {} rows".format(file_input_count))
            df.show()

            tmp_path = self._get_output()
            df.repartition(repartition_factor).write.mode('overwrite').json(tmp_path)

        else:
            raise Exception("Incompatible file type. Options are json or parquet")

        log.info("wrote files into path: {}".format(tmp_path))

        file_output_count = self._get_file_count(tmp_path)
        log.info("totally Output files has {} rows".format(file_output_count))

        if file_output_count == file_input_count:

            self._delete_path(partition_path)
            log.info("deleted flume files into path: {}".format(partition_path))
            self._move_files(tmp_path, partition_path)
            log.info("moved file from {} to {}".format(tmp_path, partition_path))

            if self.add_partition:

                log.info("adding partition {next_partition} to table {schema_table}".format(schema_table=self.schema_table, next_partition=self.next_partition))
                self.spark.sql("ALTER TABLE {schema_table} ADD IF NOT EXISTS PARTITION ({partition_name}='{next_partition}')"
                               .format(schema_table=self.schema_table, partition_name=self.partition_name, next_partition=self.next_partition))

        else:
            log.error("Error! file input has {} rows but file output has {}".format(file_input_count, file_output_count))


if __name__ == "__main__":

    # parse arguments
    parser = argparse.ArgumentParser()
    parser.add_argument("--path", help="Path to HDFS where files to compact reside")
    parser.add_argument("--partition_name", help="Name of the partition subfolder. Default: partition_date", default='partition_date')
    parser.add_argument("--file_format", help="Format of files to be compacted. Options: parquet (default) and json", default='parquet')
    parser.add_argument("--add_partition", help="To Flume process, add partition to the next compacted date", type=str2bool, default=False)
    parser.add_argument("--schema_table", help="If add_partition is true, table + schema", default='')
    args = parser.parse_args()

    # validate arguments
    if not args.path:
        raise Exception("Missing parameter: --path")

    # init data_compacter
    data_compacter = Compacter(**vars(args))
    data_compacter.execute()