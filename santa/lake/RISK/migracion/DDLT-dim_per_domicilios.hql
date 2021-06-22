CREATE EXTERNAL TABLE IF NOT EXISTS bi_corp_common.dim_per_domicilios(
cod_per_nup	string	
, ds_per_secuencia	int	
, ds_per_tipo_domicilio	string	
, ds_per_calle	string	
, ds_per_nro_calle	int	
, ds_per_piso	string	
, ds_per_depto	string	
, ds_per_torre	string	
, ds_per_edificio	string	
, ds_per_entre_calle	string	
, ds_per_y_calle	string	
, ds_per_longitud	string	
, ds_per_latitud	string	
, ds_per_localidad	string	
, cod_per_cod_provincia	string	
, ds_per_provincia	string	
, ds_per_cod_postal	string	
, cod_per_pais	string	
, ds_per_pais	string	
, flag_per_titular_domicilo	string	
, flag_per_normalizacion	string	
, flag_per_domicilio_erroneo	string	
, flag_per_correspondencia	string	
, dt_per_fecha_alta	string	
, dt_per_fecha_modificacion	string	
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
  '/santander/bi-corp/common/personas/dim/dim_per_domicilios'