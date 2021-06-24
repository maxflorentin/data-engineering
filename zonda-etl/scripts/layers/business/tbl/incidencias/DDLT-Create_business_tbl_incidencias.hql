CREATE EXTERNAL TABLE IF NOT EXISTS bi_corp_business.agg_rec_incidencias
(
    dt_rec_periodo string,
    id_rec_ide_gestion_nro string,
    cod_rec_cod_est_gest string,
    dt_rec_fec_est_gest string,
    cod_rec_cod_bandeja string,
    id_rec_ide_gestion_sector string,
    cod_rec_tpo_medio string,
    cod_rec_orden_estado string,
    id_rec_ide_circuito string,
    cod_rec_tpo_gestion string,
    cod_rec_cod_cpto string,
    cod_rec_cod_subcpto string,
    dec_rec_NP_20107 DECIMAL(38,21), --NÚMERO DE INCIDENCIAS
    dec_rec_NP_20140 DECIMAL(38,21), --INCIDENCIAS SOBRE DERECHOS DE ACCESO
    dec_rec_NP_20142 DECIMAL(38,21), --INCIDENCIAS SOBRE DERECHOS DE RECTIFICACIÓN
    dec_rec_NP_20144 DECIMAL(38,21), --INCIDENCIAS SOBRE DERECHOS DE CANCELACIÓN
    dec_rec_NP_20150 DECIMAL(38,21) --INCIDENCIAS SOBRE DERECHOS DE OPOSICIÓN
)
PARTITIONED BY ( partition_date string)
STORED AS PARQUET
LOCATION
  '${DATA_LAKE_SERVER}/santander/bi-corp/business/tbl/incidencias'
