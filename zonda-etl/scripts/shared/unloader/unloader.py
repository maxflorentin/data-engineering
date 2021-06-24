# coding=utf-8

import argparse
import os
import subprocess
import time

from pyspark.sql import SparkSession
from pyspark.sql.functions import concat_ws, collect_list


def get_current_epoch():
    return int(round(time.time() * 1000))


def str2bool(v):
    if isinstance(v, bool):
        return v
    if v.lower() in ('yes', 'true', 't', 'y', '1'):
        return True
    elif v.lower() in ('no', 'false', 'f', 'n', '0'):
        return False
    else:
        raise argparse.ArgumentTypeError('Boolean value expected')


class Unloader:
    def __init__(self, **kwargs):
        self.session = SparkSession.builder \
            .enableHiveSupport() \
            .appName("DataUnloader") \
            .getOrCreate()
        self.logger = self.session._jvm.org.apache.log4j.LogManager.getLogger(self.__class__.__name__.lower())
        self.level = self._get_logging_level(kwargs.get('level'))
        self.logger.setLevel(self.level)
        self.query = self._read_query(kwargs.get('query'))
        self.current_epoch = get_current_epoch()
        self.delimiter = kwargs.get('delimiter')
        self.header = kwargs.get('header')
        self.output = os.path.expandvars(kwargs.get('output') or self._get_ouput() + '/query_output_{}.csv'.format(self.current_epoch))

    def _get_logging_level(self, level):
        logging_level = self.session._jvm.org.apache.log4j.Level.INFO
        if level.upper() in ('FATAL', 'CRIT', 'CRITICAL'):
            logging_level = self.session._jvm.org.apache.log4j.Level.FATAL
        elif level.upper() in ('WARN', 'WARNING'):
            logging_level = self.session._jvm.org.apache.log4j.Level.WARN
        elif level.upper() == 'ERROR':
            logging_level = self.session._jvm.org.apache.log4j.Level.ERROR
        elif level.upper() == 'DEBUG':
            logging_level = self.session._jvm.org.apache.log4j.Level.DEBUG

        return logging_level

    @staticmethod
    def _is_valid_query(query):
        is_valid = True
        if not query or not query.upper().strip().startswith("SELECT"):
            is_valid = False

        return is_valid

    def _get_staging_dir(self):
        return "/user/{}/.sparkStaging/{}/".format(self.session.sparkContext.sparkUser(), self.session._jsc.sc().applicationId())

    def _read_query(self, query_path):
        query_file_name = os.path.basename(query_path)
        staging_file_path = self._get_staging_dir() + query_file_name

        # read query
        query = self.session.read.option("multiline", "true").text(staging_file_path)
        query = query.agg(concat_ws(" ", collect_list(query["value"])).alias("query")).distinct().collect()
        query = query[0].__getitem__("query").strip()
        query = query[:-1] if query[-1] == ";" else query

        return os.path.expandvars(query)

    def _get_ouput(self):
        return "/user/{}/.sparkStaging/tmp/{}".format(self.session.sparkContext.sparkUser(), self.current_epoch)

    @staticmethod
    def _list_directory(path):
        cmd = 'hdfs dfs -find {}/*.csv'.format(path)
        return subprocess.check_output(cmd, shell=True).decode('utf-8').strip().split('\n')

    @staticmethod
    def _move_file(source, target):
        cmd = 'hdfs dfs -cp -f {} {}'.format(source, target)
        return subprocess.check_output(cmd, shell=True).decode('utf-8').strip().split('\n')

    @staticmethod
    def _delete_path(path):
        cmd = 'hdfs dfs -rm -r {}'.format(path)
        return subprocess.check_output(cmd, shell=True).decode('utf-8').strip().split('\n')

    def execute(self):
        try:
            if self._is_valid_query(self.query):
                df = self.session.sql(self.query)
                df.show()
                df.coalesce(1).write.csv(path=self._get_ouput(), header=self.header, sep=self.delimiter, escape='"', quote='"')
                files = self._list_directory(self._get_ouput())

                # move file to final destination
                if files:
                    self.logger.info("Query executed and saved in {}".format(files[0]))
                    self._move_file(files[0], self.output)
                    self.logger.info("Moved file from {} to {}".format(files[0], self.output))
                    self._delete_path(self._get_ouput())
                    self.logger.info("Deleted temporal output folder {}".format(self.output))

        except Exception as ex:
            self.logger.fatal(str(ex))


if __name__ == "__main__":
    # parse arguments
    parser = argparse.ArgumentParser()
    parser.add_argument("--query", help="Path of file where the query to be executed is",)
    parser.add_argument("--output", help="Path of the file to be saved in HDFS")
    parser.add_argument("--delimiter", help="Column separator. Default: |", default='|')
    parser.add_argument("--level", help="Logging level. Default: INFO", default='INFO')
    parser.add_argument("--header", help="Indicates if the header should be included in the file or not. Default: True", type=str2bool, default=True)
    args = parser.parse_args()

    # validate arguments
    if not args.query:
        raise Exception("Missing parameter: --query")

    if args.level.upper() not in ('FATAL', 'CRIT', 'CRITICAL', 'WARN', 'WARNING', 'ERROR', 'DEBUG', 'INFO'):
        raise Exception("Invalid value for parameter --level (Valid values: CRIT, WARN, ERROR, DEBUG, INFO)")

    # init dawnloader
    data_downloader = Unloader(**vars(args))
    data_downloader.execute()


