set hive.merge.mapfiles=true;
set hive.merge.mapredfiles=true;
set hive.merge.size.per.task=4000000;
set hive.merge.smallfiles.avgsize=16000000;
set hive.exec.dynamic.partition.mode=nonstrict;
set mapred.job.queue.name=root.dataeng;

INSERT OVERWRITE TABLE bi_corp_bdr.test_jm_interv_cto
partition(partition_date='{{ ti.xcom_pull(task_ids='InputConfig', key='month_partition_bdr', dag_id='BDR_LOAD_Tactico_Mitre') }}')
--Todo menos Tactico Mitre
SELECT ic.g4128_feoperac, ic.g4128_s1emp,    ic.g4128_contra1,  ic.g4128_tipintev, ic.g4128_tipintv2,
       ic.g4128_numordin, ic.g4128_idnumcli, ic.g4128_formintv, ic.g4128_porpartn, ic.g4128_fecultmo
FROM bi_corp_bdr.jm_interv_cto ic
  LEFT JOIN bi_corp_bdr.test_normalizacion_id_contratos_history nch
    ON ic.g4128_contra1 = nch.id_cto_bdr
    AND nch.source != 'Tactico-Mitre' 
WHERE ic.partition_date = '{{ ti.xcom_pull(task_ids='InputConfig', key='month_partition_bdr', dag_id='BDR_LOAD_Tactico_Mitre') }}'
UNION ALL
--Tactico Mitre
select '{{ ti.xcom_pull(task_ids='InputConfig', key='last_calendar_day', dag_id='BDR_LOAD_Tactico_Mitre') }}' AS g4128_feoperac,
       tmic.empresa_IC   AS g4128_s1emp,
       m.id_cto_bdr      AS g4128_contra1,
       tmic.tipintev_IC  AS g4128_tipintev,
       tmic.tipintv2_IC  AS g4128_tipintv2,
       tmic.numordin_IC  AS g4128_numordin,
       tmic.idnumcli_IC  AS g4128_idnumcli,
       lpad('1', 5, '0') AS g4128_formintv,
       lpad('0', 9, '0') AS g4128_porpartn,
       '{{ ti.xcom_pull(task_ids='InputConfig', key='last_calendar_day', dag_id='BDR_LOAD_Tactico_Mitre') }}' AS g4128_fecultmo
FROM bi_corp_bdr.tactico_mitre_contratos tmc
  LEFT JOIN bi_corp_bdr.test_normalizacion_id_contratos m
    ON tmc.contrato = m.id_cto_source
    AND m.source = 'Tactico-Mitre'
    AND m.partition_date = '{{ ti.xcom_pull(task_ids='InputConfig', key='month_partition_bdr', dag_id='BDR_LOAD_Tactico_Mitre') }}'
  LEFT JOIN bi_corp_bdr.tactico_mitre_intervinientes_contratos tmic
    ON tmc.contrato = tmic.contrato
    AND tmc.empresa_IC = tmic.empresa_IC
WHERE '{{ ti.xcom_pull(task_ids='InputConfig', key='month_partition_bdr', dag_id='BDR_LOAD_Tactico_Mitre') }}' between tmc.fecha_desde and tmc.fecha_hasta
  AND '{{ ti.xcom_pull(task_ids='InputConfig', key='month_partition_bdr', dag_id='BDR_LOAD_Tactico_Mitre') }}' between tmic.fecha_desde and tmic.fecha_hasta;