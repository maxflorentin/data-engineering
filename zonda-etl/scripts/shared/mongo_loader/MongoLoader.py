# coding=utf-8

import argparse
import os
import time
import json
import pyspark
import pyspark.sql.functions as fspark


def get_current_epoch():
    return int(round(time.time() * 1000))


class MongoLoader:
    def __init__(self, **kwargs):
        conf = pyspark.SparkConf()
        self.session = pyspark.sql.SparkSession.builder.enableHiveSupport().appName('MongoLoader').config(conf=conf).getOrCreate()
        with open(kwargs.get('config')) as f:
            self.executes = json.load(f)['executes']


    @staticmethod
    def _is_valid_query(query):
        is_valid = True
        if not query or not query.upper().strip().startswith("SELECT"):
            is_valid = False

        return is_valid

    def execute(self):
        Total=len(self.executes)
        i=1
        for ex in self.executes:
            if self._is_valid_query(ex['query']):
                print("Executing query Number ",i," of ",Total)
                df = self.session.sql(ex['query'].format(p_ifrs9_tabla= os.environ.get("part_ifrs9"),p_pedt015= os.environ.get("part_malpe_pedt015")))
                df.show()
                df = df.withColumnRenamed("id", "_id")
                user = os.getenv('USER')
                df = df.withColumn("lastModifiedBy", fspark.lit(user))
                df = df.withColumn("lastModifiedAt", fspark.lit(get_current_epoch()))
                df = df.na.fill(0)
                df.show()
                print("Writing information in mongodb : {}.{}".format(ex['database'],ex['collection']))
                df.write.format("mongo").mode("append").option("database", ex['database']).option("collection",ex['collection']).save()
            else:
                print("****************Invalid query*********************")
            i+=1


if __name__ == "__main__":
    # parse arguments
    parser = argparse.ArgumentParser()
    parser.add_argument("--config", help="Path of file where contains querys to be executed is and the collection target")
    args = parser.parse_args()

    # init MongoLoader
    MongoLoad = MongoLoader(**vars(args))
    MongoLoad.execute()


