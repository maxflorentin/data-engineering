set mapred.job.queue.name=root.dataeng;
set hive.merge.mapfiles=true;
set hive.merge.mapredfiles=true;
set hive.merge.size.per.task=4000000;
set hive.merge.smallfiles.avgsize=16000000;
set hive.exec.dynamic.partition.mode=nonstrict;
set hive.auto.convert.join=true;
set hive.auto.convert.join.noconditionaltask=true;
set hive.auto.convert.join.noconditionaltask.size=40000000;


DROP TABLE IF EXISTS temp_saldos_depositos_prestamos_1;
CREATE TEMPORARY TABLE temp_saldos_depositos_prestamos_1
AS
SELECT
    RE.partition_date AS partition_date,
    RE.cod_ren_pais AS COD_REN_PAIS,
    RE.cod_ren_divisa AS COD_DIVISA,
    RE.cod_ren_area_negocio AREA_NEGOCIO,
    RE.cod_ren_cont_gestion AS COD_CTA_CONT_GESTION,
    RE.cod_ren_entidad_espana AS COD_ENTIDAD_ESPANA,
    RE.dt_ren_altacontrato AS dt_ren_altacontrato,
    RE.dt_ren_vencontrato AS dt_ren_vencontrato,
    RE.fc_ren_tasint as fc_ren_tasint,
    RE.fc_ren_plazcontractual as fc_ren_plazcontractual,
    IF(RE.fc_ren_tasint=0,1,0) as flag_ren_tasa0,
    RE.flag_ren_ind_pool as flag_ren_ind_pool,
    RE.cod_ren_segmento_gest as cod_ren_segmento_gest,
    RE.cod_ren_producto_gest AS COD_PRODUCTO_GEST,
    RE.ds_ren_producto_niv_3 AS ds_ren_producto_niv_3,
    RE.ds_ren_producto_niv_4 AS ds_ren_producto_niv_4,
    RE.ds_ren_producto_niv_5 AS ds_ren_producto_niv_5,
    RE.ds_ren_producto_niv_6 AS ds_ren_producto_niv_6,
    RE.ds_ren_producto_niv_7 AS ds_ren_producto_niv_7,
    RE.ds_ren_producto_niv_8 AS ds_ren_producto_niv_8,
    RE.cod_ren_producto_niv_3 AS cod_ren_producto_niv_3,
    RE.cod_ren_producto_niv_4 AS cod_ren_producto_niv_4,
    RE.cod_ren_producto_niv_5 AS cod_ren_producto_niv_5,
    RE.cod_ren_producto_niv_6 AS cod_ren_producto_niv_6,
    RE.cod_ren_producto_niv_7 AS cod_ren_producto_niv_7,
    RE.cod_ren_producto_niv_8 AS cod_ren_producto_niv_8,

    --Aca separo los casos de la Nueva Banca Empresas
    case when RE.cod_ren_area_negocio like '%BE%' then 'NBE'
         else RE.cod_ren_area_negocio_niv_2 end  cod_ren_area_negocio_niv_2,

    case when RE.cod_ren_area_negocio like '%BE%' then 'NBE'
         else RE.cod_ren_area_negocio_niv_3 end  cod_ren_area_negocio_niv_3,

    case when RE.cod_ren_area_negocio like '%BE%' then 'Nueva Banca Empresas'
         else RE.ds_ren_area_negocio_niv_2 end  ds_ren_area_negocio_niv_2,
    case when RE.cod_ren_area_negocio like '%BE%' then 'Nueva Banca Empresas'
         else RE.ds_ren_area_negocio_niv_3 end  ds_ren_area_negocio_niv_3,

    prod2.cod_ren_producto_niv_7 AS cod_ren_balance_niv_7,
    prod2.ds_ren_producto_niv_7 AS ds_ren_balance_niv_7,
	case when RE.cod_ren_area_negocio like '%AG%' and re.cod_ren_area_negocio <> 'REAGINT' then 1
    else 0 end  flag_ren_agro,	
    sum(RE.fc_ren_saldo_puntual_ml) as fc_ren_saldo_puntual_ml,
    sum(RE.fc_ren_saldo_puntual_mo) as fc_ren_saldo_puntual_mo,
    sum(RE.fc_ren_saldo_medio_ml) as fc_ren_saldo_medio_ml,
    sum(RE.fc_ren_saldo_medio_mo) as fc_ren_saldo_medio_mo,
    count(DISTINCT RE.cod_ren_contrato) as fc_ren_contratos

FROM
   bi_corp_common.bt_ren_result_dia AS RE
LEFT JOIN bi_corp_common.dim_ren_productosctrco prod2 ON RE.cod_ren_producto_gest = prod2.cod_ren_producto

WHERE
  RE.partition_date = '{{ ti.xcom_pull(task_ids='InputConfig', key='partition_date', dag_id='LOAD_BSN_Rentabilidad_PrestamosYDepositos_Daily') }}'
  AND
    (
    RE.cod_ren_producto_niv_4 IN ('BG120','BG215')
    OR RE.cod_ren_producto_gest IN ('AC01IC132', 'AC01BA007', 'AC01CA001', 'PA01CC022', 'PA01CC023', 'PA02CC004', 'AC01CA005')
        )

GROUP BY
   RE.partition_date,
    RE.cod_ren_pais,
    RE.cod_ren_divisa,
    RE.cod_ren_area_negocio,
    RE.cod_ren_cont_gestion,
    RE.cod_ren_entidad_espana,
    RE.dt_ren_altacontrato,
    RE.dt_ren_vencontrato,
    RE.fc_ren_tasint,
    RE.fc_ren_plazcontractual,
    IF(RE.fc_ren_tasint=0,1,0),
    RE.flag_ren_ind_pool,
    RE.cod_ren_segmento_gest,
    RE.cod_ren_producto_gest,
    RE.ds_ren_producto_niv_3,
    RE.ds_ren_producto_niv_4,
    RE.ds_ren_producto_niv_5,
    RE.ds_ren_producto_niv_6,
    RE.ds_ren_producto_niv_7,
    RE.ds_ren_producto_niv_8,
    RE.cod_ren_producto_niv_3,
    RE.cod_ren_producto_niv_4,
    RE.cod_ren_producto_niv_5,
    RE.cod_ren_producto_niv_6,
    RE.cod_ren_producto_niv_7,
    RE.cod_ren_producto_niv_8,
    RE.cod_ren_area_negocio_niv_2,
    RE.cod_ren_area_negocio_niv_3,
    RE.ds_ren_area_negocio_niv_2,
    RE.ds_ren_area_negocio_niv_3,
    prod2.cod_ren_producto_niv_7,
    prod2.ds_ren_producto_niv_7,
	case when RE.cod_ren_area_negocio like '%AG%' and re.cod_ren_area_negocio <> 'REAGINT' then 1
    else 0 end
	;




--Acá paso de la temporal a la BUSINESS


   INSERT OVERWRITE TABLE bi_corp_business.agg_cdg_prestamosdepositos_daily
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
    adn.ds_adn_n3 as ds_ren_area_negocio_niv_3,
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
	RD.flag_ren_agro,
    adn.ds_adn_n1 as ds_ren_adn_n1,
    adn.ds_adn_n2 as ds_ren_adn_n2,
    sum(RD.fc_ren_saldo_puntual_ml) as fc_ren_saldo_puntual_ml,
    sum(RD.fc_ren_saldo_puntual_mo) as fc_ren_saldo_puntual_mo,
    sum(RD.fc_ren_saldo_medio_ml) as fc_ren_saldo_medio_ml,
    sum(RD.fc_ren_saldo_medio_mo) as fc_ren_saldo_medio_mo,
    SUM(RD.fc_ren_contratos) as fc_ren_contratos,
    RD.partition_date as partition_date


FROM
    temp_saldos_depositos_prestamos_1 RD
    LEFT JOIN bi_corp_common.dim_areanegocio_marca adn ON RD.AREA_NEGOCIO = adn.cod_ren_area_negocio
    and adn.partition_Date= case when '{{ ti.xcom_pull(task_ids='InputConfig', key='partition_date', dag_id='LOAD_BSN_Rentabilidad_PrestamosYDepositos_Daily') }}' < '2021-04-21' then '2021-04-21' else '{{ ti.xcom_pull(task_ids='InputConfig', key='max_partition_dim_areanegocio_marca', dag_id='LOAD_BSN_Rentabilidad_PrestamosYDepositos_Daily') }}' end

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
    adn.ds_adn_n3,
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
    RD.flag_ren_agro,
    adn.ds_adn_n1,
    adn.ds_adn_n2,
    RD.partition_date
    ;
