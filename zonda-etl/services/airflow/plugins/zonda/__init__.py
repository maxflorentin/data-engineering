#!/usr/bin/python3

from zonda.operators.zonda_garbage_operator import ZondaGDOperator
from zonda.operators.zonda_hdfs_operator import ZondaHDFSOperator
from zonda.operators.zonda_s3_operator import ZondaS3Operator
from zonda.operators.zonda_slack_operator import ZondaSlackOperator
from zonda.operators.zonda_gen_operator import ZondaGenOperator
from zonda.operators.zonda_hive_operator import ZondaHiveOperator
from zonda.operators.zonda_gcs_operator import ZondaGCSOperator
from zonda.operators.zonda_tableau_update_operator import ZondaTableauUpdateOperator
from zonda.operators.zonda_controlm_operator import ZondaControlmOperator
from zonda.operators.zonda_trigger_dag_run_operator import ZondaTriggerDagRunOperator
from zonda.sensors.zonda_file_sensor import ZondaFileSensor
from zonda.hooks.zonda_ssh_hook import ZondaSSHHook
from zonda.hooks.zonda_hive_cli_hook import ZondaHiveCliHook
from airflow.plugins_manager import AirflowPlugin


class ZondaPlugin(AirflowPlugin):
    name = "zonda"
    operators = [ZondaHDFSOperator, ZondaS3Operator, ZondaSlackOperator, ZondaGenOperator, ZondaHiveOperator,
                 ZondaGCSOperator, ZondaTriggerDagRunOperator, ZondaGDOperator, ZondaTableauUpdateOperator,
                 ZondaControlmOperator]
    hooks = [ZondaSSHHook, ZondaHiveCliHook]
    sensors = [ZondaFileSensor]
