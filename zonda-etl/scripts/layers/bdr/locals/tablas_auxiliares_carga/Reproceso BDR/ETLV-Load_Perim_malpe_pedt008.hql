set hive.merge.mapfiles=true;
set hive.merge.mapredfiles=true;
set hive.merge.size.per.task=4000000;
set hive.merge.smallfiles.avgsize=16000000;
set hive.exec.dynamic.partition.mode=nonstrict;
set mapred.job.queue.name=root.dataeng;

insert overwrite table bi_corp_bdr.perim_malpe_pedt008_1er_titular
partition(partition_date='{{ ti.xcom_pull(task_ids='InputConfig', key='month_partition_bdr', dag_id='BDR_LOAD_Tables-Reproceso_BDR_Monthly_201912_202103') }}')
select pecodpro,pecodsub,pecodent,pecodofi,penumcon,
       est_relacion,fec_baja,cal_participacion
from (
       select pecodpro as pecodpro, 
              pecodsub as pecodsub, 
              pecodent as pecodent, 
              pecodofi as pecodofi, 
              penumcon as penumcon,
              peestrel as est_relacion,
              pefecbrb as fec_baja,
              pecalpar as cal_participacion,
              ROW_NUMBER() over(partition by pecodpro, pecodsub, pecodent, pecodofi, penumcon order by peordpar) as orden
       from bi_corp_staging.malpe_pedt008
       where partition_date = '{{ ti.xcom_pull(task_ids='InputConfig', key='max_partition_malpe_pedt008', dag_id='BDR_LOAD_Tables-Reproceso_BDR_Monthly_201912_202103') }}'
              and pecalpar = 'TI'
) subtable
where orden = 1
	and (fec_baja BETWEEN add_months('{{ ti.xcom_pull(task_ids='InputConfig', key='max_partition_malpe_pedt008', dag_id='BDR_LOAD_Tables-Reproceso_BDR_Monthly_201912_202103-') }}', -12) 
                         and '{{ ti.xcom_pull(task_ids='InputConfig', key='max_partition_malpe_pedt008', dag_id='BDR_LOAD_Tables-Reproceso_BDR_Monthly_201912_202103') }}'
	     or fec_baja = '9999-12-31');