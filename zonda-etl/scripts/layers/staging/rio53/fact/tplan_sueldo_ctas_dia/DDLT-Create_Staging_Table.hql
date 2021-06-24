CREATE EXTERNAL TABLE IF NOT EXISTS bi_corp_staging.rio53_tplan_sueldo_ctas_dia (
    ent_empleado            string,
    suc_empleado            string,
    nro_empleado            string,
    div_empleado            string,
    ent_empleador           string,
    suc_empleador           string,
    nro_empleador           string,
    div_empleador           string,
    cuit_empleador          string,
    importe                 string,
    programa                string,
    sistema                 string,
    cod_convenio            string,
    cbu_empleador           string,
    cuil_empleado           string,
    tipo_cuenta             string,
    tipo_proc               string,
    nup_empleado            string,
    nup_empleador           string,
    nom_empleado            string,
    nom_empleador           string,
    fecha_acreditacion      string,
    suscriptor              string,
    fecha_alta_cta_empleado string,
    suc_adm                 string,
    segmento                string,
    id_empresa              string,
    empresa_pyrip           string,
    cuadrante               string,
    fecha_proceso           string,
    fecha_carga             string,
    tipo_acred              string,
    origen                  string,
    suc_fisica_empleado     string,
    suc_fisica_empleador    string
)
PARTITIONED BY (
  partition_date string)
ROW FORMAT SERDE
  'org.apache.hadoop.hive.ql.io.parquet.serde.ParquetHiveSerDe'
STORED AS INPUTFORMAT
  'org.apache.hadoop.hive.ql.io.parquet.MapredParquetInputFormat'
OUTPUTFORMAT
  'org.apache.hadoop.hive.ql.io.parquet.MapredParquetOutputFormat'
LOCATION
  '${DATA_LAKE_SERVER}/santander/bi-corp/staging/rio53/fact/tplan_sueldo_ctas_dia'