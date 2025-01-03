CREATE EXTERNAL TABLE IF NOT EXISTS bi_corp_common.bt_cyp_rendpago(
cod_cyp_nro_organismo                   bigint,
cod_cyp_nro_rend                        int,
ds_cyp_tpo_reg                          string,
ds_cyp_ide_resgistro                    string,
ds_cyp_sec_registo                      int,
dt_cyp_fecha                            string,
ds_cyp_raz_soc                          string,
ds_cyp_can_reg                          bigint,
ds_cyp_can_pgosrend                     bigint,
fc_cyp_tot_imp_rend                     decimal(15,2),
fc_cyp_acum_comdevg                     decimal(15,2),
fc_cyp_tot_impcomcob                    decimal(15,2),
fc_cyp_tot_impcomdvg                    decimal(15,2),
ds_cyp_nro_clte                         string,
ds_cyp_nom_clte                         string,
ds_cyp_cuit                             bigint,
ts_cyp_fecha_pago                       string,
ds_cyp_cant_doc                         int,
ds_cyp_cant_docpdtes                    int,
fc_cyp_tot_pgo                          decimal(15,2),
fc_cyp_imp_copempr                      decimal(15,2),
fc_cyp_imp_cotizvend                    decimal(15,2),
fc_cyp_imp_cotizcomp                    decimal(15,2),
ds_cyp_cant_rentencion                  bigint,
ds_cyp_cant_liquidacion                 bigint,
dt_cyp_fecha_altalote                   string,
ds_cyp_nro_envio                        int,
ds_cyp_nro_originf                      string,
fc_cyp_tot_imppuni                      decimal(15,2),
fc_cyp_tot_bonf                         decimal(15,2),
cod_cyp_canal                           string,
cod_cyp_subcanal                        string,
ds_cyp_formapago                        string,
ds_cyp_nro_instrumento                  string,
fc_cyp_imp                              decimal(15,2),
dt_cyp_fec_acred                        string,
ds_cyp_mar_acreditacion                 string,
dt_cyp_fec_vtocpd                       string,
cod_cyp_no_alaorden                     int,
ds_cyp_mar_confirming                   string,
ds_cyp_mar_float                        string,
cod_cyp_est_instr                       string,
dt_cyp_fec_emision                      string,
ds_cyp_nro_emision                      int,
cod_cyp_nro_sucursaldist                int,
ds_cyp_tipo_dist                        string,
dt_cyp_fec_deb                          string,
cod_cyp_pais_cd                         int,
cod_cyp_mone_cd                         int,
cod_cyp_ent_cbu1                        int,
cod_cyp_suc_cbu1                        int,
ds_cyp_nro_dvcbu1                       int,
ds_cyp_nro_fijcbu2                      int,
ds_cyp_tpo_ctacbu2                      int,
cod_cyp_mone_cbu2                       int,
ds_cyp_nro_ctacbu2                      bigint,
ds_cyp_nro_dvcbu2                       int,
cod_cyp_formapago                       string,
cod_cyp_mot_rechazo                     string,
ds_cyp_tpo_cprb                         string,
ds_cyp_nro_cprb                         string,
ds_cyp_nro_cuo                          string,
dt_cyp_fecha_vto                        string,
ds_cyp_tsa_puni                         decimal(5,2),
fc_cyp_imp_deuda                        decimal(15,2),
fc_cyp_imp_puni                         decimal(15,2),
fc_cyp_imp_dto                          decimal(15,2),
fc_cyp_imp_pgo                          decimal(15,2),
ds_cyp_obs_libre1                       string,
ds_cyp_obs_libre2                       string,
ds_cyp_obs_libre3                       string,
cod_cyp_cpto                            int,
ds_cyp_nro_seccpto                      int,
fc_cyp_imp_cotizdlvca                   decimal(15,2),
fc_cyp_imp_cotizdlcca                   decimal(15,2),
fc_cyp_imp_cpto                         decimal(15,2),
fc_cyp_imp_cptodiscr                    decimal(15,2),
fc_cyp_imp_iva                          decimal(15,2),
fc_cyp_imp_ivaadic                      decimal(15,2),
fc_cyp_imp_ingbru                       decimal(15,2),
dt_cyp_fec_movim                        string,
ds_cyp_tipo_docu                        string,
ds_cyp_nro_docu                         bigint,
ds_cyp_tipo_docu1                       string,
ds_cyp_nro_docu1                        bigint,
ds_cyp_tipo_docu2                       string,
ds_cyp_nro_docu2                        bigint,
cod_cyp_sucursal_dist                   int,
ds_cyp_tpo_recibo                       string,
ds_cyp_nro_secudom                      int

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
  '${DATA_LAKE_SERVER}/santander/bi-corp/common/cyp/fact/bt_cyp_rendpago'