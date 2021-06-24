set hive.merge.mapfiles=true;
set hive.merge.mapredfiles=true;
set hive.merge.size.per.task=4000000;
set hive.merge.smallfiles.avgsize=16000000;
set hive.exec.dynamic.partition.mode=nonstrict;
set mapred.job.queue.name=root.dataeng;

INSERT OVERWRITE TABLE bi_corp_common.dim_cjs_tipos_caja
partition(partition_date)
SELECT CAST(tipo_Caja AS BIGINT) cod_cjs_tipo_caja,
       CAST(grupo_caja AS BIGINT) cod_cjs_grupo_caja,
       CASE
          when grupo_caja = '01' then 'CHICA'
          when grupo_caja = '02' then 'MEDIANA'
          when grupo_caja = '03' then 'GRANDE'
          when grupo_caja = '04' then 'EXTRA GRANDE'
       end as ds_cjs_grupo_caja,
       TRIM(medida_caja) cod_cjs_medida_caja,
       partition_date
from bi_corp_staging.acse_tipos_caja
where partition_date = '{{ ti.xcom_pull(task_ids='InputConfig', key='partition_date', dag_id='LOAD_CMN_CajaSeg') }}';