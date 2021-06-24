CREATE EXTERNAL TABLE IF NOT EXISTS bi_corp_common.stk_tbl_tabloncontrato (
cod_tbl_idf_cto_ods                             string,
cod_tbl_contenido                               string,
cod_tbl_sis_origen                              string,
dt_tbl_fecha_data                               string,
cod_tbl_pais                                    int,
cod_tbl_entidad                                 string,
cod_tbl_centro                                  string,
cod_prod_producto                               string,
cod_prod_subproducto                            string,
cod_nro_cuenta                                  bigint,
cod_tbl_secuencia_cto                           string,
cod_div_divisa                                  string,
cod_tbl_per_nup                                 string,
cod_tbl_centro_cont                             string,
cod_tbl_oficina_comercial                       string,
dt_tbl_fecha_alta_cto                           string,
dt_tbl_fecha_vencimiento                        string,
fc_tbl_tasa_int                                 decimal(8,5),
int_tbl_num_dias_demora                         int,
cod_tbl_plz_contractual                         int,
cod_tbl_unidad_frecuencia_pago_int              string,
cod_tbl_metodo_amrt                             string,
cod_tbl_unidad_plz_amrt                         string,
fc_tbl_imp_inicial_mo                           decimal(20,4),
fc_tbl_imp_limite_credito_ml                    decimal(20,4),
fc_tbl_sdo_dispuesto_ml                         decimal(20,4)
)
PARTITIONED BY (
partition_date string,cod_tbl_contenido string,cod_tbl_sis_origen string)
STORED AS PARQUET
LOCATION
'${DATA_LAKE_SERVER}/santander/bi-corp/common/cdg/tablon_contrato/stk_tbl_tabloncontrato'
;