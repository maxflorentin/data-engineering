CREATE EXTERNAL TABLE IF NOT EXISTS bi_corp_staging.rio6_autt_vehiculos(
    auve_nu_serial_carroceria         string,
    auve_nu_serial_motor              string,
    auve_auma_cd_marca                string,
    auve_aumo_cd_modelo               string,
    auve_an_fabricacion               string,
    auve_nu_placa                     string,
    auve_ca_peso_kilogramos           string,
    auve_in_automatico_sincronico     string,
    auve_in_carga_pasajeros           string,
    auve_cp_carga_pasajeros           string,
    auve_auco_cd_color                string,
    auve_auth_cd_tipo_vehiculo        string,
    auve_aucl_cd_clase                string,
    auve_nu_cilindros                 string,
    auve_auco_cd_color2               string,
    auve_auco_cd_color3               string,
    auve_nu_oblea_gnc                 string,
    auve_de_marca_regulador_gnc       string,
    auve_nu_serial_regulador_gnc      string,
    auve_fe_desde_gnc                 string,
    auve_fe_hasta_gnc                 string,
    auve_nu_taller_montaje_gnc        string,
    auve_mt_suma_asegurada_gnc        string,
    auve_cd_tipo_gnc                  string
)

PARTITIONED BY (partition_date string)
ROW FORMAT SERDE 'org.apache.hadoop.hive.ql.io.parquet.serde.ParquetHiveSerDe'
STORED AS INPUTFORMAT 'org.apache.hadoop.hive.ql.io.parquet.MapredParquetInputFormat'
OUTPUTFORMAT 'org.apache.hadoop.hive.ql.io.parquet.MapredParquetOutputFormat'
LOCATION '${DATA_LAKE_SERVER}/santander/bi-corp/staging/rio6/fact/autt_vehiculos';