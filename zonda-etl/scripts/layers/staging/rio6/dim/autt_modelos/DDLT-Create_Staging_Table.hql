CREATE EXTERNAL TABLE IF NOT EXISTS bi_corp_staging.rio6_autt_modelos(
    aumo_auma_cd_marca       string,
    aumo_cd_modelo           string,
    aumo_de_modelo           string,
    aumo_auth_cd_tipo_veh    string,
    aumo_in_importado        string,
    aumo_auus_cd_uso         string,
    aumo_cp_carga_pasajeros  string,
    aumo_tp_riesgo_conductor string,
    aumo_peso_kilogramos     string,
    aumo_cd_clase            string,
    aumo_in_carga_pasajeros  string,
    aumo_in_excluir          string,
    aumo_carp_cd_ramo        string
)

PARTITIONED BY (partition_date string)
ROW FORMAT SERDE 'org.apache.hadoop.hive.ql.io.parquet.serde.ParquetHiveSerDe'
STORED AS INPUTFORMAT 'org.apache.hadoop.hive.ql.io.parquet.MapredParquetInputFormat'
OUTPUTFORMAT 'org.apache.hadoop.hive.ql.io.parquet.MapredParquetOutputFormat'
LOCATION '${DATA_LAKE_SERVER}/santander/bi-corp/staging/rio6/dim/autt_modelos';