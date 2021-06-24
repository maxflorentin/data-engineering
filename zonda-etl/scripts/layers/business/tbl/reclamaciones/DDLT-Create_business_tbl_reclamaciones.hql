CREATE EXTERNAL TABLE IF NOT EXISTS bi_corp_business.agg_rec_reclamaciones
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
    dt_rec_fec_gestion_alta string,
    cod_rec_cod_tipo_favorabilidad string,
    cod_rec_cod_cpto string,
    dec_rec_NP_20067 DECIMAL(38,21), --VOLUMEN DE RECLAMACIONES ENVIADOS AL REGULADOR
    dec_rec_NP_20069 DECIMAL(38,21), --Volumen total de reclamaciones oficiales (por escrito) enviados a los Servicios de atención al Cliente
    dec_rec_NP_20074 DECIMAL(38,21), --# DE RECLAMACIONES RESUELTAS
    dec_rec_NP_20076 DECIMAL(38,21), --# DE RECLAMACIONES CON RESOLUCIÓN FAVORABLE AL BANCO
    dec_rec_NP_20078 DECIMAL(38,21), --# DE RECLAMACIONES CON RESOLUCIÓN FAVORABLE AL CLIENTE
    dec_rec_NP_20080 DECIMAL(38,21), --VALOR ECONÓMICO CONCEDIDO AL CLIENTE EN RESOLUCIÓN DE RECLAMACIONES
    dec_rec_NP_20083 DECIMAL(38,21), --# RECLAMACIONES DE REGULADOR RESUELTOS DENTRO DEL PLAZO LEGAL
    dec_rec_NP_20106 DECIMAL(38,21), --NUMERO DE RECLAMACIONES ENVIADOS AL COMITÉ EJECUTIVO/CONSEJEROS
    dec_rec_NP_20128 DECIMAL(38,21), --RECLAMACIONES SOBRE DERECHOS DE OPOSICIÓN
    dec_rec_NP_20141 DECIMAL(38,21), --RECLAMACIONES SOBRE DERECHOS DE ACCESO
    dec_rec_NP_20143 DECIMAL(38,21), --RECLAMACIONES SOBRE DERECHOS DE RECTIFICACIÓN
    dec_rec_NP_20145 DECIMAL(38,21) --RECLAMACIONES SOBRE DERECHOS DE CANCELACIÓN
)
PARTITIONED BY ( partition_date string)
STORED AS PARQUET
LOCATION
  '${DATA_LAKE_SERVER}/santander/bi-corp/business/tbl/reclamaciones'
