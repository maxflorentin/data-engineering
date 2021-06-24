set hive.merge.mapfiles=true;
set hive.merge.mapredfiles=true;
set hive.merge.size.per.task=4000000;
set hive.merge.smallfiles.avgsize=16000000;
set hive.exec.dynamic.partition.mode=nonstrict;
set mapred.job.queue.name=root.dataeng;

-- Calculo la maxima particion PEDT042
CREATE TEMPORARY TABLE tmp_maxpart042 AS
select max(partition_date) AS partition_date
from bi_corp_Staging.malpe_ptb_pedt042
where partition_date >= date_sub('{{ ti.xcom_pull(task_ids='InputConfig', key='partition_date', dag_id='LOAD_CMN_Inversiones') }}',7)
  and partition_date <= '{{ ti.xcom_pull(task_ids='InputConfig', key='partition_date', dag_id='LOAD_CMN_Inversiones') }}'
  and pecdgent = '0072' and pecodpro = '60' and pecodmon = 'ARS';

-- Elimino los  duplicados de PEDT042
CREATE TEMPORARY TABLE IF NOT EXISTS TEMP_SUC_OPERATIVA as
SELECT distinct
    cast(p.pesucope as int) cod_inv_sucursal_operativa,
    cast(p.penumcon as int) cod_inv_cuenta
FROM bi_corp_staging.malpe_ptb_pedt042 p
INNER JOIN tmp_maxpart042 mx42 on (p.partition_date=mx42.partition_date)
WHERE p.pecdgent = '0072' -- (Santander)
    AND p.pecodpro = '60'   -- (Producto Fondos)
    AND p.pecodmon = 'ARS';


insert overwrite table bi_corp_common.bt_inv_mercados
partition(partition_date='{{ ti.xcom_pull(task_ids='InputConfig', key='partition_date', dag_id='LOAD_CMN_Inversiones') }}')
--mercado primario
select
    p.s_producto_negociado as ds_inv_mercado,
    t.cod_inv_sucursal_operativa as cod_inv_sucursal_operativa,
    cast(p.n_cuenta_titulos as int) as cod_inv_cuenta,
    cast(p.s_nup as int) as cod_per_nup,
    p.s_cod_especie as cod_inv_especie,
    p.s_secunit as cod_inv_tipo_especie,
    p.s_desc_especie as ds_inv_especie,
    CAST(NULL as string) as cod_inv_especie_sigla,
    CAST(NULL as string) as cod_inv_especie_cvsa,
    CAST(NULL as int) as cod_inv_categoria_cvsa,
    CAST(NULL as string) as ds_inv_instrumento,
    p.s_tipo_oper as cod_inv_tipo_operacion,
    cast(p.s_plazo as int) as int_inv_plazo,
    p.s_cod_operacion as cod_inv_tipo_operacion_plazo,
    p.s_tipo_oper_descr as ds_inv_tipo_operacion_plazo,
    CAST(NULL as string) as ds_inv_producto,
    if(p.s_moneda = 'ARP', 'ARS', p.s_moneda) as cod_inv_moneda,
    d.cod_inv_moneda_emision as cod_inv_moneda_emision,
    cast(p.s_precio_cierre_moneda as bigint)/100000000 as fc_inv_precio_cierre_moneda,
    if(from_unixtime(unix_timestamp(p.s_fecha_orden,'dd-MMM-yy'),'yyyy-MM-dd') = 'NULL', from_unixtime(unix_timestamp(p.s_fecha_orden,'dd-MMM-yyyy'),'yyyy-MM-dd'), from_unixtime(unix_timestamp(p.s_fecha_orden,'dd-MMM-yy'),'yyyy-MM-dd'))
        as dt_inv_orden,
    p.s_orde_hora as ts_inv_orden_hora,
    if(from_unixtime(unix_timestamp(p.s_fecha_concertacion,'dd-MMM-yy'),'yyyy-MM-dd') = 'NULL', from_unixtime(unix_timestamp(p.s_fecha_concertacion,'dd-MMM-yyyy'),'yyyy-MM-dd'), from_unixtime(unix_timestamp(p.s_fecha_concertacion,'dd-MMM-yy'),'yyyy-MM-dd'))
        as dt_inv_concertacion,
    if(from_unixtime(unix_timestamp(p.s_fecha_liq,'dd-MMM-yy'),'yyyy-MM-dd') = 'NULL', from_unixtime(unix_timestamp(p.s_fecha_liq,'dd-MMM-yyyy'),'yyyy-MM-dd'), from_unixtime(unix_timestamp(p.s_fecha_liq,'dd-MMM-yy'),'yyyy-MM-dd'))
        as dt_inv_liq,
    p.s_oper_hora as ts_inv_operacion_hora,
    p.s_ofer_hora as ts_inv_oferta_hora,
    cast(p.n_operacion as int) as cod_inv_operacion,
    CAST(NULL as int) as cod_inv_oferta,
    cast(p.n_orden as int) as cod_inv_orden,
    cast(p.s_nro_oferta_mercado as int) as cod_inv_oferta_mercado,
    cast(p.n_boleto as int) as cod_inv_boleto,
    CAST(NULL as int) as cod_inv_secuencia_boleto,
    p.s_cno as cod_inv_cno,
    cast(p.f_cantidad as bigint)/100000000 as fc_inv_cantidad,
    cast(p.f_precio as bigint)/100000000 as fc_inv_precio,
    cast(p.f_importe as bigint)/100000000 as fc_inv_importe,
    p.s_desc_comitente as ds_inv_comitente,
    cast(p.n_agente as int) as cod_inv_agente,
    p.s_segmento as cod_inv_canal,
    CAST(NULL as string) as cod_inv_subcanal,

    case
    when p.s_segmento in ('BPRIV', 'OLB')
        then 'TBANCO'
    when p.s_segmento = 'CII'
        then 'CONTACT CENTER'
    end as ds_inv_canal,

    CAST(NULL as string) as cod_inv_operador,
    CAST(NULL as bigint) as fc_inv_arancel,
    CAST(NULL as bigint) as fc_inv_arancel_porc,
    CAST(NULL as bigint) as fc_inv_iva,
    CAST(NULL as bigint) as fc_inv_iva_mercado,
    CAST(NULL as bigint) as fc_inv_iva_nocat,
    CAST(NULL as bigint) as fc_inv_iva_perc,
    CAST(NULL as bigint) as fc_inv_iva_porc,
    CAST(NULL as bigint) as fc_inv_iva1_porc_derechosb,
    CAST(NULL as bigint) as fc_inv_iva1_derechosb,
    CAST(NULL as string) as ds_inv_tipo_iva,
    CAST(NULL as bigint) as fc_inv_neto,
    CAST(NULL as string) as ds_inv_neto_en_letras,
    CAST(NULL as bigint) as fc_inv_precio_promedio,
    CAST(NULL as bigint) as fc_inv_derechosm,
    CAST(NULL as bigint) as fc_inv_derechosb,
    CAST(NULL as bigint) as fc_inv_derechos_porc,
    CAST(NULL as bigint) as fc_inv_derechosb_porc,
    CAST(NULL as bigint) as fc_inv_derechosm_porc,
    CAST(NULL as int) as int_inv_relacion_baja,
    CAST(NULL as string) as ds_inv_relacion_baja,
    p.s_tipo_mercado as cod_inv_tipo_mercado,
    p.s_desc_mercado as ds_inv_tipo_mercado,
    p.s_estado as cod_inv_estado,
    cast(p.s_year as int) as int_inv_year,
    CAST(NULL as string) as cod_inv_instancia,
    CAST(NULL as string) as cod_inv_tipo_boleto,
    p.s_segmento as ds_inv_segmento,
    cast(p.n_tipo_cta_oper as int) as cod_inv_tipo_cuenta_operacion,
    cast(p.s_sucursal as int) as cod_inv_sucursal_operacion,
    cast(p.s_nro_cta_oper as int) as cod_inv_cuenta_operacion,
    CAST(NULL as int) as cod_inv_mercado_operacion,
    CAST(NULL as int) as cod_inv_cuenta_caja,
    regexp_replace(p.s_cuit, '-', '') as ds_inv_cuit,
    p.s_domicilio as ds_inv_domicilio,
    p.s_localidad as ds_inv_localidad,
    p.s_provincia as ds_inv_provincia,
    p.s_pais as ds_inv_pais,
    REGEXP_EXTRACT(p.s_codigo_posta,'[0-9]+', 0) as ds_inv_codigo_postal,
    cast(p.n_comicanum as int) as int_inv_comicanum,
    cast(p.n_piso as int) as int_inv_piso,
    p.n_depto as ds_inv_depto
from bi_corp_staging.sam_mer_pri_sec p
left join TEMP_SUC_OPERATIVA t on t.cod_inv_cuenta = cast(p.n_cuenta_titulos as int)
left join bi_corp_common.dim_inv_especies_titulos d on d.cod_inv_especie = p.s_cod_especie and d.partition_date='{{ ti.xcom_pull(task_ids='InputConfig', key='partition_date', dag_id='LOAD_CMN_Inversiones') }}'
where p.partition_date='{{ ti.xcom_pull(task_ids='InputConfig', key='partition_date', dag_id='LOAD_CMN_Inversiones') }}' and if(from_unixtime(unix_timestamp(p.s_fecha_orden,'dd-MMM-yy'),'yyyy-MM-dd') = 'NULL', from_unixtime(unix_timestamp(p.s_fecha_orden,'dd-MMM-yyyy'),'yyyy-MM-dd'), from_unixtime(unix_timestamp(p.s_fecha_orden,'dd-MMM-yy'),'yyyy-MM-dd')) >= from_unixtime(unix_timestamp('2020-10-01' , 'yyyy-MM-dd'))

UNION ALL

-- mercado secundario
select
    s.s_producto_negociado as ds_inv_mercado,
    t.cod_inv_sucursal_operativa as cod_inv_sucursal_operativa,
    cast(s.n_cuenta_titulos as int) as cod_inv_cuenta,
    cast(s.s_nup as int) as cod_per_nup,
    s.s_cod_especie as cod_inv_especie,
    s.s_secunit as cod_inv_tipo_especie,
    s.s_desc_especie as ds_inv_especie,
    s.s_ric as cod_inv_especie_sigla,
    s.s_especie_cv as cod_inv_especie_cvsa,
    cast(s.n_categoria_cv as int) as cod_inv_categoria_cvsa,
    s.s_t_instrumento as ds_inv_instrumento,
    s.s_tipo_oper as cod_inv_tipo_operacion,
    cast(s.s_plazo as int) as int_inv_plazo,
    s.s_cod_operacion as cod_inv_tipo_operacion_plazo,
    s.s_tipo_oper_descr as ds_inv_tipo_operacion_plazo,
    s.s_producto as ds_inv_producto,

    case
        when s.s_moneda = 'PESOS'
            then 'ARS'
        when s.s_moneda = 'DOLAR BILLETE'
            then 'USB'
        when s.s_moneda = 'DOLAR DIVISA'
            then 'USD'
    end as cod_inv_moneda,

    d.cod_inv_moneda_emision as cod_inv_moneda_emision,
    cast(s.s_precio_cierre_moneda as bigint)/100000000 as fc_inv_precio_cierre_moneda,
    if(from_unixtime(unix_timestamp(s.s_fecha_orden,'dd-MMM-yy'),'yyyy-MM-dd') = 'NULL', from_unixtime(unix_timestamp(s.s_fecha_orden,'dd-MMM-yyyy'),'yyyy-MM-dd'), from_unixtime(unix_timestamp(s.s_fecha_orden,'dd-MMM-yy'),'yyyy-MM-dd'))
        as dt_inv_orden,
    s.s_orde_hora as ts_inv_orden_hora,
    if(from_unixtime(unix_timestamp(s.s_fecha_concertacion,'dd-MMM-yy'),'yyyy-MM-dd') = 'NULL', from_unixtime(unix_timestamp(s.s_fecha_concertacion,'dd-MMM-yyyy'),'yyyy-MM-dd'), from_unixtime(unix_timestamp(s.s_fecha_concertacion,'dd-MMM-yy'),'yyyy-MM-dd'))
        as dt_inv_concertacion,
    if(from_unixtime(unix_timestamp(s.s_fecha_liq,'dd-MMM-yy'),'yyyy-MM-dd') = 'NULL', from_unixtime(unix_timestamp(s.s_fecha_liq,'dd-MMM-yyyy'),'yyyy-MM-dd'), from_unixtime(unix_timestamp(s.s_fecha_liq,'dd-MMM-yy'),'yyyy-MM-dd'))
        as dt_inv_liq,
    s.s_oper_hora as ts_inv_operacion_hora,
    s.s_ofer_hora as ts_inv_oferta_hora,
    cast(s.n_operacion as int) as cod_inv_operacion,
    cast(s.n_oferta as int) as cod_inv_oferta,
    cast(s.n_orden as int) as cod_inv_orden,
    cast(s.s_nro_oferta_mercado as int) as cod_inv_oferta_mercado,
    cast(s.n_boleto as int) as cod_inv_boleto,
    cast(s.n_nro_secuencia_boleto as int) as cod_inv_secuencia_boleto,
    s.n_cno as cod_inv_cno,
    cast(s.f_cantidad as bigint)/100000000 as fc_inv_cantidad,
    cast(s.f_precio as bigint)/100000000 as fc_inv_precio,
    cast(s.f_importe as bigint)/100000000 as fc_inv_importe,
    s.s_desc_comitente as ds_inv_comitente,
    cast(s.n_agente as int) as cod_inv_agente,
    s.canal as cod_inv_canal,
    s.subcanal as cod_inv_subcanal,

    case
        when concat(s.canal, '-', s.subcanal) = '04-0011'
            then 'OBE'
        when concat(s.canal, '-', s.subcanal) in ('04-0099', '79-0000')
            then 'TBANCO'
        when concat(s.canal, '-', s.subcanal) in ('12-0000', '18-0', '18-0000')
            then 'CONTACT CENTER'
        when concat(s.canal, '-', s.subcanal) in ('99-0000', '99-9999')
            then 'OTROS'
        else ''
    end as ds_inv_canal,

    s.operador as cod_inv_operador,
    cast(s.f_arancel as bigint)/100000 as fc_inv_arancel,
    cast(s.n_arancel_porc as bigint)/100000000 as fc_inv_arancel_porc,
    cast(s.f_iva as bigint)/100000000 as fc_inv_iva,
    cast(s.f_iva_mercado as bigint)/100000000 as fc_inv_iva_mercado,
    cast(s.f_iva_nocat as bigint)/100000000 as fc_inv_iva_nocat,
    cast(s.f_iva_perc as bigint)/100000000 as fc_inv_iva_perc,
    cast(s.n_iva_porc as bigint)/100000 as fc_inv_iva_porc,
    cast(s.n_iva1_porc_derechosb as bigint)/100000 as fc_inv_iva1_porc_derechosb,
    cast(s.n_iva1_derechosb as bigint)/100000 as fc_inv_iva1_derechosb,
    s.s_iva_desc as ds_inv_tipo_iva,
    cast(s.f_neto as bigint)/100000000 as fc_inv_neto,
    s.s_cant_en_letras as ds_inv_neto_en_letras,
    cast(s.f_precio_promedio as bigint)/100000000 as fc_inv_precio_promedio,
    cast(s.f_derechosm as bigint)/100000 as fc_inv_derechosm,
    cast(s.n_derechosb as bigint)/100000 as fc_inv_derechosb,
    cast(s.n_derechos_porc as bigint)/100000 as fc_inv_derechos_porc,
    cast(s.n_derechosb_porc as bigint)/100000 as fc_inv_derechosb_porc,
    cast(s.n_derechosm_porc as bigint)/100000 as fc_inv_derechosm_porc,
    cast(s.n_relacion_baja as int) as int_inv_relacion_baja,
    s.s_relacion_baja as ds_inv_relacion_baja,
    s.s_tipo_mercado as cod_inv_tipo_mercado,
    s.s_desc_mercado as ds_inv_tipo_mercado,
    s.s_estado as cod_inv_estado,
    cast(s.s_year as int) as int_inv_year,
    s.s_instancia as cod_inv_instancia,
    s.s_tipo_boleto as cod_inv_tipo_boleto,
    s.s_segmento as ds_inv_segmento,
    cast(s.n_tipo_cta_oper as int) as cod_inv_tipo_cuenta_operacion,
    cast(s.s_sucursal as int) as cod_inv_sucursal_operacion,
    cast(s.s_nro_cta_oper as int) as cod_inv_cuenta_operacion,
    cast(s.n_mercado_operacion as int) as cod_inv_mercado_operacion,
    cast(s.n_cuenta_caja as int) as cod_inv_cuenta_caja,
    regexp_replace(s.s_cuit, '-', '') as ds_inv_cuit,
    s.s_domicilio as ds_inv_domicilio,
    s.s_localidad as ds_inv_localidad,
    s.s_provincia as ds_inv_provincia,
    s.s_pais as ds_inv_pais,
    REGEXP_EXTRACT(s.s_codigo_posta,'[0-9]+', 0) as ds_inv_codigo_postal,
    cast(s.n_comicanum as int) as int_inv_comicanum,
    cast(s.n_piso as int) as int_inv_piso,
    s.n_depto as ds_inv_depto
from bi_corp_staging.sam_mer_sec_gar s
left join TEMP_SUC_OPERATIVA t on t.cod_inv_cuenta = cast(s.n_cuenta_titulos as int)
left join bi_corp_common.dim_inv_especies_titulos d on d.cod_inv_especie = s.s_cod_especie and d.partition_date='{{ ti.xcom_pull(task_ids='InputConfig', key='partition_date', dag_id='LOAD_CMN_Inversiones') }}'
where s.partition_date='{{ ti.xcom_pull(task_ids='InputConfig', key='partition_date', dag_id='LOAD_CMN_Inversiones') }}' and if(from_unixtime(unix_timestamp(s.s_fecha_orden,'dd-MMM-yy'),'yyyy-MM-dd') = 'NULL', from_unixtime(unix_timestamp(s.s_fecha_orden,'dd-MMM-yyyy'),'yyyy-MM-dd'), from_unixtime(unix_timestamp(s.s_fecha_orden,'dd-MMM-yy'),'yyyy-MM-dd')) >= from_unixtime(unix_timestamp('2021-01-01' , 'yyyy-MM-dd'));