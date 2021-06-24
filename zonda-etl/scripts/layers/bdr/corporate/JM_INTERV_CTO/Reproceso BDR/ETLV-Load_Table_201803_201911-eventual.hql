set hive.merge.mapfiles=true;
set hive.merge.mapredfiles=true;
set hive.merge.size.per.task=4000000;
set hive.merge.smallfiles.avgsize=16000000;
set hive.exec.dynamic.partition.mode=nonstrict;
set mapred.job.queue.name=root.dataeng;


insert OVERWRITE TABLE bi_corp_bdr.jm_interv_cto
partition(partition_date='{{ ti.xcom_pull(task_ids='InputConfig', key='month_partition_bdr', dag_id='BDR_LOAD_jm_interv_cto-eventual-2018-03_2019-11') }}')
SELECT DISTINCT
       c.G4128_FEOPERAC,
       c.G4128_S1EMP,
       c.G4128_CONTRA1,
       c.G4128_TIPINTEV,
       c.G4128_TIPINTV2,
       c.G4128_NUMORDIN,
       c.G4128_IDNUMCLI,
       c.G4128_FORMINTV,
       lpad(concat((case when mae.ind_mancomu != 'O' then 100 else 0 end),'000000') , 9, '0') AS G4128_PORPARTN,
       c.G4128_FECULTMO
  from bi_corp_bdr.jm_interv_cto c
INNER JOIN bi_corp_bdr.normalizacion_id_contratos m
            on c.G4128_CONTRA1 = m.id_cto_bdr
           AND m.partition_date = '{{ ti.xcom_pull(task_ids='InputConfig', key='month_partition_bdr', dag_id='BDR_LOAD_jm_interv_cto-eventual-2018-03_2019-11') }}'
LEFT JOIN bi_corp_staging.bgdtmae mae
            on mae.entidad = m.cred_cod_entidad
           and mae.centro_alta = m.cred_cod_centro
           and mae.cuenta = concat('0', m.cred_cod_producto, substring(m.cred_num_cuenta, 4, 12) )
           and mae.producto = m.cred_cod_producto
           and trim(mae.subprodu) = m.cred_cod_subprodu_altair
           and mae.partition_date = '2019-11-29'
WHERE c.partition_date = '{{ ti.xcom_pull(task_ids='InputConfig', key='month_partition_bdr', dag_id='BDR_LOAD_jm_interv_cto-eventual-2018-03_2019-11') }}';