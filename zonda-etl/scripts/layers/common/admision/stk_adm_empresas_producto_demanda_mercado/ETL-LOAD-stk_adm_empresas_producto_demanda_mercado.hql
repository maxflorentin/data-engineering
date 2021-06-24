set hive.merge.mapfiles=true;
set hive.merge.mapredfiles=true;
set hive.merge.size.per.task=4000000;
set hive.merge.smallfiles.avgsize=16000000;
set hive.exec.dynamic.partition.mode=nonstrict;
SET mapred.job.queue.name=root.dataeng;

INSERT OVERWRITE TABLE bi_corp_common.stk_adm_empresas_producto_demanda_mercado
    PARTITION (partition_date='{{ ti.xcom_pull(task_ids='InputConfig', key='partition_date', dag_id='LOAD_CMN_Admision_Empresas-Daily') }}')
select
    cast(nro_prop as bigint) as cod_adm_nro_prop,
    tpo_cliente as cod_adm_tpo_cliente,
    gdo_cliente as cod_adm_gdo_cliente,
    antig_cliente as cod_adm_antig_cliente,
    cast(mar_dentro_radio as int) as flag_adm_dentro_radio,
    des_posicion as ds_adm_posicion,
    cast(mar_visitado as int) as flag_adm_visitado,
    date_format(fec_ultima_visita,'yyyy-MM-dd') as dt_adm_ultima_visita,
    cast(mar_marco_visita as int) as flag_adm_marco_visita,
    obs_marco_visita as ds_adm_marco_visita,
    cast(can_empleados as int) as int_adm_can_empleados,
    cast(can_empleados as int) as int_adm_can_clientes,
    cast(porc_ventas_credit as decimal(3, 2)) as dec_adm_porc_ventas_credit,
    cast(mar_datos_gravados as int) as flag_adm_datos_grabados,
    cast(antig_socios as int) as int_adm_antig_socios,
    cast(mar_aval as int) as flag_adm_aval,
    cast(mar_otros_negocios as int) as flag_adm_otros_negocios,
    obs_otros_negocios as ds_adm_otros_negocios,
    des_funcionamiento as ds_adm_funcionamiento,
    cast(mar_gcia_distinta as int) as flag_adm_gcia_distinta,
    cod_usu_alta as cod_adm_usu_alta,
    fec_alta as ts_adm_alta,
    cod_usu_mod as cod_adm_usu_mod,
    fec_mod as ts_adm_mod,
    des_producto1 as ds_adm_producto1,
    des_producto2 as ds_adm_producto2,
    des_producto3 as ds_adm_producto3,
    cast(porc_producto1 as decimal(3, 2)) as dec_adm_porc_producto1,
    cast(porc_producto2 as decimal(3, 2)) as dec_adm_porc_producto2,
    cast(porc_producto3 as decimal(3, 2)) as dec_adm_porc_producto3,
    obs_datos_gravados as ds_adm_datos_grabados,
    cast(mar_deuda as int) as flag_adm_deuda,
    obs_deuda as ds_adm_deuda,
    if(verificado='S', 1, 0) as flag_adm_verificado
from bi_corp_staging.sge_mercados_prop
WHERE partition_date = '{{ ti.xcom_pull(task_ids='InputConfig', key='partition_date', dag_id='LOAD_CMN_Admision_Empresas-Daily') }}';