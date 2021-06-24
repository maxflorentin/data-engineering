set hive.merge.mapfiles=true;
set hive.merge.mapredfiles=true;
set hive.merge.size.per.task=4000000;
set hive.merge.smallfiles.avgsize=16000000;
set hive.exec.dynamic.partition.mode=nonstrict;
set mapred.job.queue.name=root.dataeng;

INSERT OVERWRITE TABLE bi_corp_bdr.test_jm_contr_bis
PARTITION(partition_date='{{ ti.xcom_pull(task_ids='InputConfig', key='month_partition_bdr', dag_id='BDR_LOAD_Tactico_Mitre') }}')
--Todo menos Tactico Mitre
select cb.G4095_FEOPERAC, cb.G4095_S1EMP,    cb.G4095_CONTRA1,  cb.G4095_IDSUCUR,  cb.G4095_ID_PAIS,  
       cb.G4095_ID_PROD,  cb.G4095_IDPRO_LC, cb.G4095_FECVENTO, cb.G4095_FECVE2,   cb.G4095_FECHAPER,
       cb.G4095_FECCANCE, cb.G4095_FECREES,  cb.G4095_FECREFI,  cb.G4095_FECNOVAC, cb.G4095_IDFINALI, 
       cb.G4095_IDFINALD, cb.G4095_CONTRA2,  cb.G4095_CODDIV,   cb.G4095_ID_CANAL, cb.G4095_ID_CANA2, 
       cb.G4095_ID_NATUR, cb.G4095_ID_NSUBY, cb.G4095_INDLIM,   cb.G4095_INDAVA,   cb.G4095_IND_TITU, 
       cb.G4095_INDDEUD,  cb.G4095_INDDEUD2, cb.G4095_TIP_INTE, cb.G4095_IDEMIS,   cb.G4095_IDEMISI,
       cb.G4095_IDNETING, cb.G4095_IDCOLATE, cb.G4095_FECULTMO, cb.G4095_VENCORIG, cb.G4095_DEUDPREL, 
       cb.G4095_FECENTVI, cb.G4095_IDPROLC2
from bi_corp_bdr.jm_contr_bis cb
        left join bi_corp_bdr.test_normalizacion_id_contratos_history nch
                on cb.G4095_CONTRA1 = nch.id_cto_bdr
                and nch.source != 'Tactico-Mitre' 
where cb.partition_date = '{{ ti.xcom_pull(task_ids='InputConfig', key='month_partition_bdr', dag_id='BDR_LOAD_Tactico_Mitre') }}'
union all
--Táctico Mitre
SELECT '{{ ti.xcom_pull(task_ids='InputConfig', key='last_calendar_day', dag_id='BDR_LOAD_Tactico_Mitre') }}' AS G4095_FEOPERAC,
       tm.empresa_CB   AS G4095_S1EMP,
       m.id_cto_bdr    AS G4095_CONTRA1,
       tm.sucursal_CB  AS G4095_IDSUCUR,
       tm.id_pais_CB   AS G4095_ID_PAIS,
       tm.id_prod_CB   AS G4095_ID_PROD,
       tm.idpro_lc_CB  AS G4095_IDPRO_LC,
       tm.fecvento_CB  AS G4095_FECVENTO,
       tm.fecve2_CB    AS G4095_FECVE2,
       tm.fechaper_CB  AS G4095_FECHAPER, 
       '9999-12-31'    AS G4095_FECCANCE, --Default
       '0001-01-01'    AS G4095_FECREES,  --Default
       '0001-01-01'    AS G4095_FECREFI,  --Default
       '0001-01-01'    AS G4095_FECNOVAC, --Default
       tm.idfinali_CB  AS G4095_IDFINALI,
       lpad(0, 5, '0') AS G4095_IDFINALD, --Default
       lpad(0, 9, '0') AS G4095_CONTRA2,  --Default
       tm.coddiv_CB    AS G4095_CODDIV,
       lpad(0, 5, '0') AS G4095_ID_CANAL, --Default
       lpad(0, 5, '0') AS G4095_ID_CANA2, --Default
       lpad(0, 5, '0') AS G4095_ID_NATUR, --Default
       lpad(0, 5, '0') AS G4095_ID_NSUBY, --Default
       tm.indlim_CB    AS G4095_INDLIM,
       ' '             AS G4095_INDAVA,   --Default
       ' '             AS G4095_IND_TITU, --Default
       ' '             AS G4095_INDDEUD,  --Default
       ' '             AS G4095_INDDEUD2, --Default
       lpad(0, 5, '0') AS G4095_TIP_INTE, --Default
       lpad(0, 5, '0') AS G4095_IDEMIS,   --Default
       rpad(' ', 40, ' ') AS G4095_IDEMISI,  --Default
       lpad(0, 9, '0')    AS G4095_IDNETING, --Default
       lpad(0, 9, '0')    AS G4095_IDCOLATE, --Default
       '{{ ti.xcom_pull(task_ids='InputConfig', key='last_calendar_day', dag_id='BDR_LOAD_Tactico_Mitre') }}' AS G4095_FECULTMO,  --Default
       lpad(0, 9, '0') AS G4095_VENCORIG, --Default
       lpad(0, 5, '0') AS G4095_DEUDPREL, --Default
       '9999-12-31'    AS G4095_FECENTVI, --Default
       lpad(0, 5, '0') AS G4095_IDPROLC2  --Default
FROM bi_corp_bdr.tactico_mitre_contratos tm
        INNER JOIN bi_corp_bdr.test_normalizacion_id_contratos m
                ON tm.contrato = m.id_cto_source
                AND m.source = 'Tactico-Mitre'
                AND m.partition_date = '{{ ti.xcom_pull(task_ids='InputConfig', key='month_partition_bdr', dag_id='BDR_LOAD_Tactico_Mitre') }}'
WHERE '{{ ti.xcom_pull(task_ids='InputConfig', key='month_partition_bdr', dag_id='BDR_LOAD_Tactico_Mitre') }}' between tm.fecha_desde and tm.fecha_hasta;