set mapred.job.queue.name=root.dataeng;
set hive.merge.mapfiles=true;
set hive.merge.mapredfiles=true;
set hive.merge.size.per.task=4000000;
set hive.merge.smallfiles.avgsize=16000000;
set hive.exec.dynamic.partition.mode=nonstrict;
set hive.auto.convert.join=true;
set hive.auto.convert.join.noconditionaltask=true;
set hive.auto.convert.join.noconditionaltask.size=40000000;

DROP TABLE IF EXISTS temp_prestamos_depositos_tasas;
CREATE TEMPORARY TABLE IF NOT EXISTS temp_prestamos_depositos_tasas
AS
SELECT
RD.partition_date,
RD.cod_ren_contrato,
RD.cod_ren_producto_gest,
RD.cod_ren_segmento_gest,
RD.cod_ren_divisa,
RD.cod_ren_cont_gestion,
RD.cod_ren_entidad_espana,
RD.dt_ren_altacontrato,
RD.dt_ren_vencontrato,
RD.fc_ren_tasint,
RD.fc_ren_plazcontractual,
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
RD.ds_ren_balance_niv_7,
sum(RD.fc_ren_saldo_puntual_ml) as fc_ren_saldo_puntual_ml,
sum(RD.fc_ren_saldo_puntual_mo) as fc_ren_saldo_puntual_mo,
sum(RD.fc_ren_saldo_medio_ml) as fc_ren_saldo_medio_ml,
sum(RD.fc_ren_saldo_medio_mo) as fc_ren_saldo_medio_mo
FROM  bi_corp_common.bt_ren_result_dia RD
WHERE RD.partition_date = '{{ ti.xcom_pull(task_ids='InputConfig', key='partition_date', dag_id='LOAD_BSN_Rentabilidad_PrestamosYDepositos_Tasas_Daily') }}'
AND
(RD.cod_ren_producto_niv_4 IN ('BG120','BG215')
   			 OR RD.cod_ren_producto_gest IN ( 'AC01IC132', 'AC01BA007', 'AC01CA001', 'PA01CC022', 'PA01CC023', 'PA02CC004', 'AC01CA005' ) )
AND RD.flag_ren_ind_pool = 0
AND RD.cod_ren_contrato <> '-99100'
group by
RD.partition_date,
RD.cod_ren_contrato,
RD.cod_ren_producto_gest,
RD.cod_ren_segmento_gest,
RD.cod_ren_divisa,
RD.cod_ren_cont_gestion,
RD.cod_ren_entidad_espana,
RD.dt_ren_altacontrato,
RD.dt_ren_vencontrato,
RD.fc_ren_tasint,
RD.fc_ren_plazcontractual,
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

INSERT OVERWRITE TABLE bi_corp_business.agg_cdg_prestamosdepositostasas_daily
PARTITION(partition_date)
-----------------------------------------------------------------------------
--------------------------------- TARJETAS ----------------------------------
-----------------------------------------------------------------------------

SELECT
RD.cod_ren_divisa AS cod_ren_divisa,
RD.cod_ren_cont_gestion AS cod_ren_cta_cont_gestion,
RD.cod_ren_entidad_espana AS cod_ren_entidad_espana,
RD.dt_ren_altacontrato AS dt_ren_altacontrato,
RD.dt_ren_vencontrato AS dt_ren_vencontrato,
COALESCE(param.tasa_int_nueva, RD.fc_ren_tasint) as fc_ren_tasint,
RD.fc_ren_plazcontractual as fc_ren_plazcontractual,
IF(COALESCE(param.tasa_int_nueva, RD.fc_ren_tasint)=0,1,0) as flag_ren_tasa0,
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
sum(RD.fc_ren_saldo_puntual_ml) * (COALESCE(param.tasa_int_nueva, RD.fc_ren_tasint) / 100) as fc_ren_saldo_ponderado_ml,
sum(RD.fc_ren_saldo_puntual_mo) * (COALESCE(param.tasa_int_nueva, RD.fc_ren_tasint) / 100) as fc_ren_saldo_ponderado_mo,
count(distinct(RD.cod_ren_contrato)) as fc_ren_contratos,
RD.partition_date
FROM  temp_prestamos_depositos_tasas RD
INNER JOIN bi_corp_cg.segmento_cdg  seg on substr(RD.cod_ren_segmento_gest,1,2)= seg.cod_seg -- cambiar lógica 
LEFT JOIN (select param.tasa_int_nueva, param.tasa_int_informada, param.partition_date_desde,  param.partition_date_hasta -- validar paramétrica
				FROM bi_corp_cg.param_dailytasas_tarjetas_tasa param
				WHERE '{{ ti.xcom_pull(task_ids='InputConfig', key='partition_date', dag_id='LOAD_BSN_Rentabilidad_PrestamosYDepositos_Tasas_Daily') }}' >= param.partition_date_desde
				AND '{{ ti.xcom_pull(task_ids='InputConfig', key='partition_date', dag_id='LOAD_BSN_Rentabilidad_PrestamosYDepositos_Tasas_Daily') }}'  <= param.partition_date_hasta) param ON RD.fc_ren_tasint  = param.tasa_int_informada
where
RD.cod_ren_producto_gest in ('AC01TA070', 'AC01TA071', 'AC01TA074')
AND RD.partition_date = RD.dt_ren_altacontrato
GROUP BY
param.partition_date_desde,
param.partition_date_hasta,
RD.cod_ren_divisa,
RD.cod_ren_cont_gestion,
RD.cod_ren_entidad_espana,
RD.dt_ren_altacontrato,
RD.dt_ren_vencontrato,
COALESCE(param.tasa_int_nueva, RD.fc_ren_tasint),
RD.fc_ren_plazcontractual,
IF(COALESCE(param.tasa_int_nueva, RD.fc_ren_tasint)=0,1,0),
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
RD.partition_date

UNION ALL

-----------------------------------------------------------------------------
------------------------------ DESCUBIERTOS ---------------------------------
-----------------------------------------------------------------------------

SELECT
RD.cod_ren_divisa AS cod_ren_divisa,
RD.cod_ren_cont_gestion AS cod_ren_cta_cont_gestion,
RD.cod_ren_entidad_espana AS cod_ren_entidad_espana,
RD.dt_ren_altacontrato AS dt_ren_altacontrato,
RD.dt_ren_vencontrato AS dt_ren_vencontrato,
RD.fc_ren_tasint as fc_ren_tasint,
RD.fc_ren_plazcontractual as fc_ren_plazcontractual,
IF(RD.fc_ren_tasint=0,1,0) as flag_ren_tasa0,
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
sum(RD.fc_ren_saldo_medio_ml) * (RD.fc_ren_tasint / 100) as fc_ren_saldo_ponderado_ml,
sum(RD.fc_ren_saldo_medio_mo) * (RD.fc_ren_tasint / 100) as fc_ren_saldo_ponderado_mo,
count(distinct(RD.cod_ren_contrato)) as fc_ren_contratos,
RD.partition_date
FROM temp_prestamos_depositos_tasas RD
inner JOIN bi_corp_cg.segmento_cdg seg on substr(RD.cod_ren_segmento_gest,1,2)=seg.cod_seg -- cambiar lógica
WHERE
RD.cod_ren_producto_niv_5 = 'PG100360'
and month(RD.dt_ren_altacontrato) = month(RD.partition_date)-- la fecha de contrato este en el mes de proceso
and year(RD.dt_ren_altacontrato) = year(RD.partition_date)
GROUP BY
RD.cod_ren_divisa,
RD.cod_ren_cont_gestion,
RD.cod_ren_entidad_espana,
RD.dt_ren_altacontrato,
RD.dt_ren_vencontrato,
RD.fc_ren_tasint,
RD.fc_ren_plazcontractual,
IF(RD.fc_ren_tasint=0,1,0),
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
RD.partition_date

UNION ALL
-----------------------------------------------------------------------------
------------------------ RESTO DE LOS PRODUCTOS -----------------------------
---------------------- MENOS DESCUBIERTOS Y TARJETAS ------------------------
-----------------------------------------------------------------------------
SELECT
RD.cod_ren_divisa AS cod_ren_divisa,
RD.cod_ren_cont_gestion AS cod_ren_cta_cont_gestion,
RD.cod_ren_entidad_espana AS cod_ren_entidad_espana,
RD.dt_ren_altacontrato AS dt_ren_altacontrato,
RD.dt_ren_vencontrato AS dt_ren_vencontrato,
RD.fc_ren_tasint as fc_ren_tasint,
RD.fc_ren_plazcontractual as fc_ren_plazcontractual,
IF(RD.fc_ren_tasint=0,1,0) as flag_ren_tasa0,
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
sum(RD.fc_ren_saldo_puntual_ml) * (RD.fc_ren_tasint / 100) as fc_ren_saldo_ponderado_ml,
sum(RD.fc_ren_saldo_puntual_mo) * (RD.fc_ren_tasint / 100) as fc_ren_saldo_ponderado_mo,
count(distinct(RD.cod_ren_contrato)) as fc_ren_contratos,
RD.partition_date
FROM temp_prestamos_depositos_tasas RD
INNER JOIN bi_corp_cg.segmento_cdg seg on substr(RD.cod_ren_segmento_gest,1,2)=seg.cod_seg -- cambiar lógica
WHERE
RD.cod_ren_producto_gest not in ('AC01TA070', 'AC01TA071', 'AC01TA074')
AND RD.cod_ren_producto_niv_5 <> 'PG100360'
AND RD.partition_date = RD.dt_ren_altacontrato
GROUP BY
RD.cod_ren_divisa,
RD.cod_ren_cont_gestion,
RD.cod_ren_entidad_espana,
RD.dt_ren_altacontrato,
RD.dt_ren_vencontrato,
RD.fc_ren_tasint,
RD.fc_ren_plazcontractual,
IF(RD.fc_ren_tasint=0,1,0),
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