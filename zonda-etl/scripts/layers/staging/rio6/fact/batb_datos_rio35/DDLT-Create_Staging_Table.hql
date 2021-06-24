CREATE EXTERNAL TABLE IF NOT EXISTS bi_corp_staging.rio6_batb_datos_rio35(
   rio35_cli_nup                      string,  
   rio35_cli_petipdoc                 string,  
   rio35_cli_penumdoc                 string, 
   rio35_cli_cuadrante                string,  
   rio35_cli_color                    string,  
   rio35_eje_user_id                  string,  
   rio35_eje_apenom                   string, 
   rio6_cacn_nu_nup                   string,  
   rio6_cacn_cd_nacionalidad          string,  
   rio6_cacn_nu_cedula_rif            string, 
   rio6_cacn_nm_apellido_razon        string, 
   rio6_cacn_color                    string, 
   rio35_cli_penomfan                 string, 
   rio35_cli_pepriape                 string, 
   rio35_cli_pesegape                 string, 
   rio35_cli_penomper                 string, 
   rio35_cli_pefecing                 string,         
   rio35_cli_pesucadm                 string,  
   rio35_cli_peestciv                 string,  
   rio35_cli_pesexper                 string,  
   rio35_cli_petipper                 string,  
   rio35_cli_pefecnac                 string,         
   rio35_cli_pecdgent                 string,  
   rio35_cli_pebancap                 string,  
   rio35_cli_pedivisi                 string,  
   rio35_cli_petealea                 string,  
   rio35_cli_peejecue                 string,  
   rio35_cli_pejefare                 string,  
   rio35_cli_pegerent                 string,  
   rio35_eje_sucadmact                string,  
   rio35_eje_sucadmant                string,  
   rio35_eje_feciniciogestion         string,         
   rio_fe_ult_actualizacion           string,         
   rio_in_procesada                   string,  
   rio6_cacn_fe_nacimiento            string  
)
PARTITIONED BY (partition_date string)
ROW FORMAT SERDE 'org.apache.hadoop.hive.ql.io.parquet.serde.ParquetHiveSerDe'
STORED AS INPUTFORMAT 'org.apache.hadoop.hive.ql.io.parquet.MapredParquetInputFormat'
OUTPUTFORMAT 'org.apache.hadoop.hive.ql.io.parquet.MapredParquetOutputFormat'
LOCATION '${DATA_LAKE_SERVER}/santander/bi-corp/staging/rio6/fact/batb_datos_rio35';