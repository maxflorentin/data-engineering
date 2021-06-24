CREATE EXTERNAL TABLE IF NOT EXISTS bi_corp_staging.rio3_gestion(
codigo               string,
descri               string,
tipo                 string,
llamada              string,
contacto             string,
baja                 string,
rellamar             string,
visitar              string,
activo               string,
concosto             string,
conproducto          string,
rellamaauto          string,
validaventa          string,
novalidar            string,
rellamadias          string,
rellamahoras         string,
duracion             string,
terminacion          string,
tg_tipo              string,
duracion_crm         string,
colorfila            string,
llamadain            string,
llamadaoutmanual     string,
llamadaoutorigen     string,
llamadaagenda        string,
sincontacto          string,
nuevoestadocontacto  string,
nuevoestado2contacto string,
activo_ap            string,
depura_agenda        string,
usuario_modif        string,
fecha_modif          string,
llamadaoutinternet   string
)
PARTITIONED BY (partition_date string)
ROW FORMAT SERDE 'org.apache.hadoop.hive.ql.io.parquet.serde.ParquetHiveSerDe'
STORED AS INPUTFORMAT 'org.apache.hadoop.hive.ql.io.parquet.MapredParquetInputFormat'
OUTPUTFORMAT 'org.apache.hadoop.hive.ql.io.parquet.MapredParquetOutputFormat'
LOCATION '${DATA_LAKE_SERVER}/santander/bi-corp/staging/rio3/dim/gestion';