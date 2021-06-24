SET mapred.job.queue.name=root.dataeng;

SET hive.exec.dynamic.partition.mode = nonstrict;

INSERT OVERWRITE TABLE bi_corp_common.stk_adm_empresas_garantias_operacion
    PARTITION (partition_date='{{ ti.xcom_pull(task_ids='InputConfig', key='partition_date', dag_id='LOAD_CMN_Admision_Empresas-Daily') }}')
SELECT
    cast(g.penumper as bigint) as cod_adm_penumper,
    cast(g.secu_f487 as int) as int_adm_secu_f487,
    cast(g.secu_garan_ope as int) as int_adm_secu_garan_ope,
    g.cod_gartia as cod_adm_gartia,
    g.tipo as ds_adm_tipo,
    cast(g.porc_garantia as int) as int_adm_porc_garantia,
    cast(g.monto_garantia as decimal(16,2)) as fc_adm_monto_garantia,
    g.detalle_garantia as ds_adm_detalle_garantia,
    g.peusualt as ds_adm_peusualt,
    g.pefecalt as ts_adm_pefecalt,
    g.pefecmod as ts_adm_pefecmod,
    g.peusumod as ds_adm_peusumod,
    cast(g.id_garantia_altair as int) as id_adm_garantia_altair,
    cast(g.porc_gtia as int) as int_adm_porc_gtia,
    cast(g.secuencia as int) as int_adm_secuencia,
    g.fecha_vigencia as dt_adm_fecha_vigencia,
    g.moneda_gartia as ds_adm_moneda_gartia,
    cast(g.monto_limite as decimal(16,2)) as fc_adm_monto_limite
FROM bi_corp_staging.sge_garantias_operacion g
WHERE g.partition_date = '{{ ti.xcom_pull(task_ids='InputConfig', key='partition_date', dag_id='LOAD_CMN_Admision_Empresas-Daily') }}';