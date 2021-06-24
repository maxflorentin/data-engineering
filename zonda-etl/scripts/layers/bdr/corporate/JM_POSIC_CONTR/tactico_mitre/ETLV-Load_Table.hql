set hive.merge.mapredfiles=true;
set hive.merge.size.per.task=4000000;
set hive.merge.smallfiles.avgsize=16000000;
set hive.exec.dynamic.partition.mode=nonstrict;
set mapred.job.queue.name=root.dataeng;

INSERT OVERWRITE TABLE bi_corp_bdr.test_jm_posic_contr
PARTITION(partition_date = '{{ ti.xcom_pull(task_ids='InputConfig', key='month_partition_bdr', dag_id='BDR_LOAD_Tactico_Mitre') }}')
--Todo menos Tactico Mitre
SELECT pc.e0621_feoperac, pc.e0621_s1emp,    pc.e0621_contra1, pc.e0621_cta_cont, pc.e0621_tip_impt,
       pc.e0621_fec_mes,  pc.e0621_agrctacb, pc.e0621_importh, pc.e0621_coddiv,   pc.e0621_fecultmo,
       pc.e0621_centctbl, pc.e0621_ctacgbal
FROM bi_corp_bdr.jm_posic_contr pc
    LEFT JOIN bi_corp_bdr.test_normalizacion_id_contratos_history nch
        ON pc.e0621_contra1 = nch.id_cto_bdr
        AND nch.source != 'Tactico-Mitre' 
WHERE pc.partition_date = '{{ ti.xcom_pull(task_ids='InputConfig', key='month_partition_bdr', dag_id='BDR_LOAD_Tactico_Mitre') }}'
UNION ALL
--Tactico Mitre
SELECT '{{ ti.xcom_pull(task_ids='InputConfig', key='last_calendar_day', dag_id='BDR_LOAD_Tactico_Mitre') }}' AS e0621_feoperac,
       tmr.empresa_IPC    AS e0621_s1emp,
       m.id_cto_bdr       AS e0621_contra1,
       tmr.cta_cont_IPC   AS e0621_cta_cont,
       tmr.tip_impt_IPC   AS e0621_tip_impt,
       '{{ ti.xcom_pull(task_ids='InputConfig', key='last_calendar_day', dag_id='BDR_LOAD_Tactico_Mitre') }}' AS e0621_fec_mes,
       tmr.agrctacb_IPC   AS e0621_agrctacb,
       CASE WHEN a.mes_actual = '01' THEN a.enero  
            WHEN a.mes_actual = '02' THEN a.febrero 
            WHEN a.mes_actual = '03' THEN a.marzo 
            WHEN a.mes_actual = '04' THEN a.abril 
            WHEN a.mes_actual = '05' THEN a.mayo
            WHEN a.mes_actual = '06' THEN a.mayo
            WHEN a.mes_actual = '07' THEN a.mayo
            WHEN a.mes_actual = '08' THEN a.mayo
            WHEN a.mes_actual = '09' THEN a.mayo
            WHEN a.mes_actual = '10' THEN a.mayo
            WHEN a.mes_actual = '11' THEN a.mayo
            WHEN a.mes_actual = '12' THEN a.mayo
            ELSE null END AS e0621_importh,
       tmr.coddiv_IPC     AS e0621_coddiv,
       '0001-01-01'       AS e0621_fecultmo,
       lpad(' ', 40, ' ') AS e0621_centctbl,
       tmr.ctacgbal_IPC   AS e0621_ctacgbal
FROM bi_corp_bdr.tactico_mitre_contratos tmc
    LEFT JOIN bi_corp_bdr.test_normalizacion_id_contratos m
        ON tmc.contrato = m.id_cto_source
        AND m.source = 'Tactico-Mitre'
        AND m.partition_date = '{{ ti.xcom_pull(task_ids='InputConfig', key='month_partition_bdr', dag_id='BDR_LOAD_Tactico_Mitre') }}'
    LEFT JOIN bi_corp_bdr.tactico_mitre_rubros tmr
        ON tmc.contrato = tmr.contrato_IPC
        AND tmc.empresa_IPC = tmr.empresa_IPC
    LEFT JOIN 
        (
            select al.*,cast(month('{{ ti.xcom_pull(task_ids='InputConfig', key='month_partition_bdr', dag_id='BDR_LOAD_Tactico_Mitre') }}') as string) AS mes_actual
            FROM bi_corp_staging.alha9600 al
            where al.partition_date = '{{ var.json.jm_posic_contr.alha9600 }}'
        ) a
        ON trim(a.cargabal) = tmr.ctacgbal_IPC
        AND trim(a.rubro_altair) = tmr.cta_cont_IPC
WHERE '{{ ti.xcom_pull(task_ids='InputConfig', key='month_partition_bdr', dag_id='BDR_LOAD_Tactico_Mitre') }}' between tmc.fecha_desde and tmc.fecha_hasta
    AND '{{ ti.xcom_pull(task_ids='InputConfig', key='month_partition_bdr', dag_id='BDR_LOAD_Tactico_Mitre') }}' between tmr.fecha_desde and tmr.fecha_hasta;