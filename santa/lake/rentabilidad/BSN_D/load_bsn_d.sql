set hive.merge.mapfiles=true;
set hive.merge.mapredfiles=true;
set hive.merge.size.per.task=4000000;
set hive.merge.smallfiles.avgsize=16000000;
set hive.exec.dynamic.partition.mode=nonstrict;
set hive.auto.convert.join=true;
set hive.auto.convert.join.noconditionaltask=true;
set hive.auto.convert.join.noconditionaltask.size=40000000;

DROP TABLE IF EXISTS temp_saldos_depositos_prestamos;
CREATE TEMPORARY TABLE temp_saldos_depositos_prestamos
AS
SELECT
    RD.partition_date AS partition_date,
    RD.cod_ren_pais AS COD_REN_PAIS,
    RD.cod_ren_divisa AS COD_DIVISA,
    RD.cod_ren_area_negocio AREA_NEGOCIO,
    RD.cod_ren_cont_gestion AS COD_CTA_CONT_GESTION,
    RD.cod_ren_entidad_espana AS COD_ENTIDAD_ESPANA,
    RD.dt_ren_altacontrato AS dt_ren_altacontrato,
    RD.dt_ren_vencontrato AS dt_ren_vencontrato,
    RD.fc_ren_tasint as fc_ren_tasint,
    RD.fc_ren_plazcontractual as fc_ren_plazcontractual,
    IF(RD.fc_ren_tasint=0,1,0) as flag_ren_tasa0,
    RD.flag_ren_ind_pool as flag_ren_ind_pool,
   	RD.cod_ren_segmento_gest as cod_ren_segmento_gest,
    RD.cod_ren_producto_gest AS COD_PRODUCTO_GEST,
    RD.ds_ren_producto_niv_3 AS ds_ren_producto_niv_3,
    RD.ds_ren_producto_niv_4 AS ds_ren_producto_niv_4,
    RD.ds_ren_producto_niv_5 AS ds_ren_producto_niv_5,
    RD.ds_ren_producto_niv_6 AS ds_ren_producto_niv_6,
    RD.ds_ren_producto_niv_7 AS ds_ren_producto_niv_7,
    RD.ds_ren_producto_niv_8 AS ds_ren_producto_niv_8,
    RD.cod_ren_producto_niv_3 AS cod_ren_producto_niv_3,
    RD.cod_ren_producto_niv_4 AS cod_ren_producto_niv_4,
    RD.cod_ren_producto_niv_5 AS cod_ren_producto_niv_5,
    RD.cod_ren_producto_niv_6 AS cod_ren_producto_niv_6,
    RD.cod_ren_producto_niv_7 AS cod_ren_producto_niv_7,
    RD.cod_ren_producto_niv_8 AS cod_ren_producto_niv_8,
    RD.cod_ren_area_negocio_niv_2 AS cod_ren_area_negocio_niv_2,
    RD.cod_ren_area_negocio_niv_3 AS cod_ren_area_negocio_niv_3,
    RD.ds_ren_area_negocio_niv_2 AS ds_ren_area_negocio_niv_2,
    RD.ds_ren_area_negocio_niv_3 AS ds_ren_area_negocio_niv_3,
    RD.cod_ren_balance_niv_7 AS cod_ren_balance_niv_7,
    RD.ds_ren_balance_niv_7 AS ds_ren_balance_niv_7,
    sum(RD.fc_ren_saldo_puntual_ml) as fc_ren_saldo_puntual_ml,
    sum(RD.fc_ren_saldo_puntual_mo) as fc_ren_saldo_puntual_mo,
    sum(RD.fc_ren_saldo_medio_ml) as fc_ren_saldo_medio_ml,
    sum(RD.fc_ren_saldo_medio_mo) as fc_ren_saldo_medio_mo,
    sum(RD.fc_ren_saldo_puntual_ml) * (RD.fc_ren_tasint / 100) as fc_ren_saldo_ponderado_ml,
    sum(RD.fc_ren_saldo_puntual_mo) * (RD.fc_ren_tasint / 100) as fc_ren_saldo_ponderado_mo,
    count(DISTINCT RD.cod_ren_contrato) as fc_ren_contratos
FROM
    bi_corp_common.bt_ren_result_dia AS RD
WHERE
--- Completar Key y Dag_id
  RD.partition_date = '2020-12-22'
  AND
    (
    RD.cod_ren_producto_niv_4 IN ('BG120','BG215')
    OR RD.cod_ren_producto_gest IN ( 'AC01IC132', 'AC01BA007', 'AC01CA001', 'PA01CC022', 'PA01CC023', 'PA02CC004', 'AC01CA005' )
        )
GROUP BY
   RD.partition_date,
    RD.cod_ren_pais,
    RD.cod_ren_divisa,
    RD.cod_ren_area_negocio,
    RD.cod_ren_cont_gestion,
    RD.cod_ren_entidad_espana,
    RD.dt_ren_altacontrato,
    RD.dt_ren_vencontrato,
    RD.fc_ren_tasint,
    RD.fc_ren_plazcontractual,
    IF(RD.fc_ren_tasint=0,1,0),
    RD.flag_ren_ind_pool,
    RD.cod_ren_segmento_gest,
    RD.cod_ren_producto_gest,
    RD.ds_ren_producto_niv_3,
    RD.ds_ren_producto_niv_4,
    RD.ds_ren_producto_niv_5,
    RD.ds_ren_producto_niv_6,
    RD.ds_ren_producto_niv_7,
    RD.ds_ren_producto_niv_8,
    RD.cod_ren_producto_niv_3,
    RD.cod_ren_producto_niv_4,
    RD.cod_ren_producto_niv_5,
    RD.cod_ren_producto_niv_6,
    RD.cod_ren_producto_niv_7,
    RD.cod_ren_producto_niv_8,
    RD.cod_ren_area_negocio_niv_2,
    RD.cod_ren_area_negocio_niv_3,
    RD.ds_ren_area_negocio_niv_2,
    RD.ds_ren_area_negocio_niv_3,
    RD.cod_ren_balance_niv_7,
    RD.ds_ren_balance_niv_7;

SET hive.exec.dynamic.partition.mode=nonstrict;

INSERT OVERWRITE TABLE bi_corp_business.agg_ren_prestamos_depositos_daily
PARTITION(partition_date)
SELECT
    RD.COD_DIVISA AS cod_ren_divisa,
    RD.COD_CTA_CONT_GESTION AS cod_ren_cta_cont_gestion,
    RD.COD_ENTIDAD_ESPANA AS cod_ren_entidad_espana,
    RD.dt_ren_altacontrato AS dt_ren_altacontrato,
    RD.dt_ren_vencontrato AS dt_ren_vencontrato,
    RD.fc_ren_tasint as fc_ren_tasint,
    RD.fc_ren_plazcontractual as fc_ren_plazcontractual,
    RD.flag_ren_tasa0 as flag_ren_tasa0,
    RD.flag_ren_ind_pool as flag_ren_ind_pool,
    RD.cod_ren_area_negocio_niv_2 AS cod_ren_area_negocio_niv_2,
    RD.cod_ren_area_negocio_niv_3 AS cod_ren_area_negocio_niv_3,
    RD.ds_ren_area_negocio_niv_2 AS ds_ren_area_negocio_niv_2,
    RD.ds_ren_area_negocio_niv_3 AS ds_ren_area_negocio_niv_3,
    RD.ds_ren_producto_niv_3 AS ds_ren_producto_niv_3,
    RD.ds_ren_producto_niv_4 AS ds_ren_producto_niv_4,
    RD.ds_ren_producto_niv_5 AS ds_ren_producto_niv_5,
    RD.ds_ren_producto_niv_6 AS ds_ren_producto_niv_6,
    RD.ds_ren_producto_niv_7 AS ds_ren_producto_niv_7,
    RD.ds_ren_producto_niv_8 AS ds_ren_producto_niv_8,
    RD.cod_ren_producto_niv_3 AS cod_ren_producto_niv_3,
    RD.cod_ren_producto_niv_4 AS cod_ren_producto_niv_4,
    RD.cod_ren_producto_niv_5 AS cod_ren_producto_niv_5,
    RD.cod_ren_producto_niv_6 AS cod_ren_producto_niv_6,
    RD.cod_ren_producto_niv_7 AS cod_ren_producto_niv_7,
    RD.cod_ren_producto_niv_8 AS cod_ren_producto_niv_8,
    RD.cod_ren_balance_niv_7 AS cod_ren_balance_niv_7,
    RD.ds_ren_balance_niv_7 AS ds_ren_balance_niv_7,
    seg.banca as ds_ren_banca,
    seg.segmento_nivel_1 as ds_ren_segmento_nivel_1,
    seg.segmento_nivel_2 as ds_ren_segmento_nivel_2,
    sum(RD.fc_ren_saldo_puntual_ml) as fc_ren_saldo_puntual_ml,
    sum(RD.fc_ren_saldo_puntual_mo) as fc_ren_saldo_puntual_mo,
	sum(RD.fc_ren_saldo_medio_ml) as fc_ren_saldo_medio_ml,
	sum(RD.fc_ren_saldo_medio_mo) as fc_ren_saldo_medio_mo,
	sum(RD.fc_ren_saldo_ponderado_ml) as fc_ren_saldo_ponderado_ml,
	sum(RD.fc_ren_saldo_ponderado_mo) as fc_ren_saldo_ponderado_mo,
	SUM(RD.fc_ren_contratos) as fc_ren_contratos,
    RD.partition_date
FROM
    temp_saldos_depositos_prestamos RD
LEFT JOIN bi_corp_cg.segmento_cdg seg on substr(rd.cod_ren_segmento_gest,1,1)=seg.cod_seg
GROUP BY
    RD.COD_DIVISA,
    RD.COD_CTA_CONT_GESTION,
    RD.COD_ENTIDAD_ESPANA,
    RD.dt_ren_altacontrato,
    RD.dt_ren_vencontrato,
    RD.fc_ren_tasint,
    RD.fc_ren_plazcontractual,
    RD.flag_ren_tasa0,
    RD.flag_ren_ind_pool,
    RD.cod_ren_area_negocio_niv_2,
    RD.cod_ren_area_negocio_niv_3,
    RD.ds_ren_area_negocio_niv_2,
    RD.ds_ren_area_negocio_niv_3,
    RD.ds_ren_producto_niv_3,
    RD.ds_ren_producto_niv_4,
    RD.ds_ren_producto_niv_5,
    RD.ds_ren_producto_niv_6,
    RD.ds_ren_producto_niv_7,
    RD.ds_ren_producto_niv_8,
    RD.cod_ren_producto_niv_3,
    RD.cod_ren_producto_niv_4,
    RD.cod_ren_producto_niv_5,
    RD.cod_ren_producto_niv_6,
    RD.cod_ren_producto_niv_7,
    RD.cod_ren_producto_niv_8,
	RD.cod_ren_balance_niv_7,
    RD.ds_ren_balance_niv_7,
    seg.banca,
    seg.segmento_nivel_1,
	seg.segmento_nivel_2,
    RD.partition_date;
