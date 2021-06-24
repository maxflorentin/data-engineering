# Sheriff

Sheriff es una aplicación Spark avocada al control de integridad de una determinada tabla. Esta aplicación es valida a partir de la capa common
y realiza los siguientes controles:

 - Detección de duplicados a partir de 1 o mas campos informados. También se le puede decir que evalúe teniendo en cuenta todos los campos de la tabla. Control_Id = "duplicates"
 - Detección de nulos a partir de un campo dado. Control_Id = "null_values"
 - Detección de valores por default a partir de un campo dado. Control_Id = "default_values"
 - Validación de formatos fecha a partir de un campo fecha dado y su respectivo formato. Control_Id = "date_format"

Por cada control se puede asignar un threshold entre 0 y 1. Dicho threshold es el que se evaluará para determinar si el resultado del control
debe informarse por el canal de slack o no. En caso de no informarse, la aplicación toma el valor 0 para informar cualquier anomalía que se detecte.

NOTA: Deben informarse la tabla a la cual se debe realizar el control y su respectiva partición.

### Requisitos Previos

Contar con un pipeline en capa common o superior.

## Uso

Crear el archivo <tabla>_sheriff.json, en el siguiente ejemplo bt_nps_nps_sheriff.json e indicar los controles y las columnas sobre
los cuales se desean realizar dichos controles:

"""
{
  "table": "bi_corp_common.bt_nps_nps",
  "partition_date": "${partition_date}",
  "controls": [
    {
      "control_id": "duplicates",
      "field_to_check": ["cod_per_nup", "cod_nps_respuesta"]
    },
    {
      "control_id": "null_values",
      "field_to_check": "ds_nps_canal",
      "threshold": 0.5
    },
    {
      "control_id": "default_value",
      "field_to_check": "ds_nps_momento_critico",
      "value": "S/D",
      "threshold": 0.8
    },
    {
      "control_id": "date_format",
      "field_to_check": "dt_nps_fecha_encuesta",
      "pattern": "yyyy-MM-dd",
      "threshold": 0.1
    }
  ]
}
"""

Una vez creado nuestro <tabla>_sheriff.json, debemos agregar una tarea de SparkSubmitOperator en nuestro YML / DAG de Airflow.
La misma debe configurarse de la siguiente manera:

- name: SHERIFF_Control
  operator: SparkSubmitOperator
  config:
    name: SHERIFF_Control
    connection_id: cloudera_spark2
    executor_cores: 5
    executor_memory: 8G
    num_executors: 5
    files: ['$ZONDA_DIR/repositories/zonda-etl/scripts/layers/common/nps/fact/bt_nps_nps/bt_nps_nps_sheriff.json']
    application: '$ZONDA_DIR/repositories/zonda-etl/scripts/shared/sheriff/sheriff.py'
    conf: {
      'spark.yarn.appMasterEnv.partition_date': "{{ ti.xcom_pull(task_ids='InputConfig', key='partition_date', dag_id='LOAD_NPS-Common-Daily') }}",
      'spark.yarn.appMasterEnv.KAFKA_BROKERS': "$KAFKA_BROKERS",
      'spark.yarn.appMasterEnv.QA_KAFKA_TOPIC': "$QA_KAFKA_TOPIC"
    }
    application_args: ['bt_nps_nps_sheriff.json']

Donde de acuerdo a su DAG deben modificar los valores en los siguientes parametros:

- files
- spark.yarn.appMasterEnv.partition_date
- application_args

Una vez agregada la tarea deberán pushear los cambios y correr CORE_ColomboDAG para que estos se hagan efectivos.

## Acknowledgments

* To Lucas Hamann quien posee una gran sabiduría en aplicaciones Spark.
