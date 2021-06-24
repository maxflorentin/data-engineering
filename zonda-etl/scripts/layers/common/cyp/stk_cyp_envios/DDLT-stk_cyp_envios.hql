CREATE EXTERNAL TABLE IF NOT EXISTS bi_corp_common.stk_cyp_envios(
cod_cyp_nro_empr                    BIGINT,
cod_cyp_nro_digempr                 INT,
cod_cyp_nro_prod                    INT,
ds_cyp_nro_instan                   INT,
cod_cyp_tpo_envio                   STRING,
dt_cyp_fec_altaenvio                STRING,
ds_cyp_nro_envio                    INT,
cod_cyp_canal                       INT,
cod_cyp_estado                      STRING,
ds_cyp_can_docacept                 INT,
ds_cyp_can_docrech                  INT,
ds_cyp_can_docinf                   INT,
ds_cyp_can_pgo                      INT,
ds_cyp_cant_otrimpre                INT,
ds_cyp_can_liq                      INT,
fc_cyp_imp_acept                    DECIMAL(15,2),
fc_cyp_imp_rech                     DECIMAL(15,2),
fc_cyp_imp_inf                      DECIMAL(15,2),
dt_cyp_fec_autz                     STRING,
cod_cyp_user_autz                   STRING,
cod_cyp_mot_rech                    STRING,
dt_cyp_fec_bajenvio                 STRING,
ds_cyp_mar_vuelco                   STRING,
flag_cyp_soli_impre                 INT,
flag_cyp_ctrl_firma                 INT,
flag_cyp_mar_autz                   INT,
cod_cyp_suc_origacdo                INT,
dt_cyp_fec_maxbaja                  STRING,
dt_cyp_fec_confirm                  STRING,
ds_cyp_mar_actdeud                  STRING,
ds_cyp_nro_ultrend                  INT,
ds_cyp_periodo                      INT,
dt_cyp_fecha_desde                  STRING,
dt_cyp_fecha_hasta                  STRING,
cod_cyp_cpto_envio                  INT,
cod_cyp_form_pgo                    INT,
cod_cyp_moneda                      INT,
dt_cyp_fec_disp                     STRING,
cod_cyp_cta_creddeb                 STRING,
fc_cyp_imp                          DECIMAL(15,2),
fc_cyp_imp_baja                     DECIMAL(15,2),
dt_cyp_fec_emisioncal               STRING
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
  '${DATA_LAKE_SERVER}/santander/bi-corp/common/cyp/fact/stk_cyp_envios'