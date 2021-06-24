CREATE EXTERNAL TABLE IF NOT EXISTS bi_corp_common.bt_acr_acreditaciones (
	cod_acr_entidad_empleado        string,
	cod_acr_suc_empleado            bigint,
	cod_acr_suc_operativa_empleado  bigint,
	cod_acr_cta_empleado            bigint,
	cod_div_divisa_empleado         string,
	dt_acr_alta_cta_empleado        string,
	cod_per_nup_empleado            bigint,
	ds_per_cuil_empleado            string,
	ds_per_nombre_empleado          string,
	cod_acr_entidad_empleador       string,
	cod_acr_suc_empleador           bigint,
	cod_acr_suc_operativa_empleador bigint,
	cod_acr_cta_empleador           bigint,
	cod_div_divisa_empleador        string,
	cod_acr_cbu_empleador           string,
	cod_per_nup_empleador           bigint,
	ds_per_cuit_empleador           string,
	ds_per_nombre_empleador         string,
	cod_acr_suscriptor              bigint,
	cod_acr_convenio                string,
	fc_acr_importe                  decimal(14,2),
	dt_acr_acreditacion             string,
	cod_acr_tipo_acreditacion       string,
	ds_acr_tipo_acreditacion        string,
	cod_acr_sistema                 string,
	ds_acr_sistema                  string,
	cod_acr_programa                string,
	ds_acr_origen                   string
)
PARTITIONED BY (
    partition_date          string)
ROW FORMAT SERDE
  'org.apache.hadoop.hive.ql.io.parquet.serde.ParquetHiveSerDe' 
STORED AS INPUTFORMAT
  'org.apache.hadoop.hive.ql.io.parquet.MapredParquetInputFormat'
OUTPUTFORMAT
  'org.apache.hadoop.hive.ql.io.parquet.MapredParquetOutputFormat'
LOCATION
  '${DATA_LAKE_SERVER}/santander/bi-corp/common/acreditaciones/fact/bt_acr_acreditaciones';