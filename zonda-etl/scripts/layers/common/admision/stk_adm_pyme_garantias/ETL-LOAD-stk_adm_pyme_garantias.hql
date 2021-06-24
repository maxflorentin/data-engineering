set hive.merge.mapfiles=true;
set hive.merge.mapredfiles=true;
set hive.merge.size.per.task=4000000;
set hive.merge.smallfiles.avgsize=16000000;
set hive.exec.dynamic.partition.mode=nonstrict;
SET mapred.job.queue.name=root.dataeng;

INSERT OVERWRITE TABLE bi_corp_common.stk_adm_pyme_garantias
    PARTITION (partition_date='{{ ti.xcom_pull(task_ids='InputConfig', key='partition_date', dag_id='LOAD_CMN_Admision_Pyme-Daily') }}')
select distinct
    cast(g.nro_prop as int) as cod_adm_nro_prop,
    g.cod_gartia as cod_adm_gartia,
    g.detalle as ds_adm_detalle,
    cast(g.mon_gartia as double) as fc_adm_mon_gartia,
    g.moneda_gartia ds_adm_moneda_gartia,
    cast(g.secuencia as int) as int_adm_secuencia,
    cast(g.porc_gtia as double) as  dec_adm_porc_gtia,
    cast(g.seq_gartia as int) as cod_adm_seq_gartia
from
    bi_corp_staging.sge_garantias_genericas g
where partition_date = '{{ ti.xcom_pull(task_ids='InputConfig', key='partition_date', dag_id='LOAD_CMN_Admision_Pyme-Daily') }}';