import json
import sys
import os
from datetime import datetime as dt
from kafka.producer import KafkaProducer
from kafka.errors import NoBrokersAvailable

from pyspark.sql import SparkSession
import pyspark.sql.functions as psf
from pyspark.sql.functions import input_file_name

log = None
KAFKA_BROKERS = os.path.expandvars('${KAFKA_BROKERS}')
QA_KAFKA_TOPIC = os.path.expandvars('${QA_KAFKA_TOPIC}')


def get_contexts():
    spark = SparkSession.builder \
        .enableHiveSupport() \
        .getOrCreate()
    global log
    log = spark._jvm.org.apache.log4j.LogManager.getLogger('sheriff')
    level = spark._jvm.org.apache.log4j.Level.INFO
    log.setLevel(level)
    return spark


def get_config_data(spark):
    control_file_path = '/user/{}/.sparkStaging/{app_id}/{file_name}'.format(spark.sparkContext.sparkUser(),
        app_id=spark._jsc.sc().applicationId(), file_name=sys.argv[1])
    config_raw = spark.read.option("multiline", "true").text(control_file_path)
    config_raw.printSchema()
    config_raw.show()
    config_raw_row = config_raw.agg(
        psf.concat_ws("", psf.collect_list(config_raw["value"])).alias("config")).distinct().collect()
    config_raw_str = config_raw_row[0].__getitem__("config")
    return json.loads(config_raw_str)


def do_control(spark_sql, control_file):

    try:
        producer = KafkaProducer(bootstrap_servers=KAFKA_BROKERS,
                                 key_serializer=str.encode,
                                 value_serializer=lambda v: json.dumps(v).encode('utf-8'))

        table = control_file.get("table")
        partition_date = os.path.expandvars(control_file.get("partition_date"))

        query = "select * from {} where partition_date='{}'".format(table, partition_date)
        df_table = spark_sql.sql(query)

        row_count = df_table.count()

        print(row_count)

        if row_count != 0:

            for control in control_file['controls']:

                if control.get('control_id') == 'duplicates':

                    if isinstance(control.get('field_to_check'), list):

                        if control.get('field_to_check'):
                            error_count = row_count - df_table.dropDuplicates(control.get('field_to_check')).count()
                        else:
                            error_count = row_count - df_table.dropDuplicates().count()

                        column = ', '.join(map(str, control.get('field_to_check'))) if isinstance(
                                    control.get('field_to_check'), list) else control.get('field_to_check')

                        send_to_topic(control, error_count, row_count, table, producer, spark_sql, column, partition_date)

                    else:
                        raise ValueError("At control id '{}', field_to_check should be a list".format(control.get('control_id')))

                elif control.get('control_id') == 'null_values':

                    if isinstance(control.get('field_to_check'), list):

                        for column in control.get('field_to_check'):

                            error_count = df_table.where(psf.isnull(psf.col(column))).count()
                            print("column: {}, error: {}".format(column, error_count))
                            send_to_topic(control, error_count, row_count, table, producer, spark_sql, column, partition_date)

                    else:
                        raise ValueError(
                            "At control id '{}', field_to_check should be a list".format(control.get('control_id')))

                elif control.get('control_id') == 'default_value':

                    if control.get('value'):
                        error_count = df_table.where(psf.col(control.get('field_to_check')) == control.get('value')).count()
                        send_to_topic(control, error_count, row_count, table, producer, spark_sql, control.get('field_to_check'), partition_date)
                    else:
                        raise KeyError("Default Value should be specified")

                elif control.get('control_id') == 'date_format':

                    if control.get('pattern'):

                        if isinstance(control.get('field_to_check'), list):

                            for column in control.get('field_to_check'):

                                error_count = df_table.\
                                    where(psf.isnull(psf.to_timestamp(column))
                                                      & psf.col(column).isNotNull()).count()
                                #show
                                df_table.select(psf.col(column), psf.to_timestamp(column)). \
                                    where(psf.isnull(psf.to_timestamp(column))
                                          & psf.col(column).isNotNull()).show()

                                send_to_topic(control, error_count, row_count, table, producer, spark_sql, column, partition_date)
                        else:
                            raise ValueError(
                                "At control id '{}', field_to_check should be a list".format(control.get('control_id')))
                    else:
                        raise KeyError("Date Pattern should be specified")

                elif control.get('control_id') == 'numericValues':

                    error_count = df_table. \
                        where("{} {} {}".format(control.get('field_to_check'), control.get('symbol'), control.get('value'))).count()
                    send_to_topic(control, error_count, row_count, table, producer, spark_sql, control.get('field_to_check'), partition_date)

                else:
                    raise KeyError("Control Id '{}' does not exists".format(control.get('control_id')))

                log.info("Sending summary to Kafka")

            producer.flush()
            producer.close()
            log.info("Kafka message committed")

        else:

            control = {
                'control_id': 'emptyPartition',
                'threshold': -1
            }
            send_to_topic(control, row_count, 1, table, producer, spark_sql, '', partition_date)

    except NoBrokersAvailable:
        log.error("No Kafka brokers available! NoBrokersAvailable")


def send_to_topic(control, error_count, row_count, table, producer, spark_sql, column, partition_date):

    if control.get('threshold', 0) < (error_count / float(row_count)):
        message = {
            'interface': table,
            'control_source': "zonda sheriff",
            'control_id': control.get('control_id'),
            'row_count': row_count,
            'error_count': error_count,
            'op_timestamp': str(dt.now()),
            'application_id': spark_sql._jsc.sc().applicationId(),
            'extra_data': 'Partition evaluated for column {} was {}'.format(column, partition_date),
            'notify_flag': True
        }

        producer.send(topic=QA_KAFKA_TOPIC, key=str(table), value=message).add_callback("Kafka message committed") \
            .add_errback("No Kafka brokers available! NoBrokersAvailable")


def sheriff():
    spark = get_contexts()

    control_file = get_config_data(spark)
    do_control(spark, control_file)


if __name__ == "__main__":
    sheriff()