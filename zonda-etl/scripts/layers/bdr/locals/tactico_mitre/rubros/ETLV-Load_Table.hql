set hive.merge.mapfiles=true;
set hive.merge.mapredfiles=true;
set hive.merge.size.per.task=4000000;
set hive.merge.smallfiles.avgsize=16000000;
set hive.exec.dynamic.partition.mode=nonstrict;
set mapred.job.queue.name=root.dataeng;

INSERT OVERWRITE TABLE bi_corp_bdr.tactico_mitre_rubros
SELECT 'contrato_tactico_mitre_1' AS contrato_IPC,
       '23100'        AS empresa_IPC ,
       '175121125001' AS cta_cont_IPC,
       '00001'        AS tip_impt_IPC,
       '175121'       AS agrctacb_IPC,
       'USD'          AS coddiv_IPC  ,
       '1732911E'     AS ctacgbal_IPC,
       '2021-05'      AS fecha_desde ,
       '9999-12'      AS fecha_hasta
UNION ALL
SELECT 'contrato_tactico_mitre_1' AS contrato_IPC,
       '23100'        AS empresa_IPC ,
       '175121125002' AS cta_cont_IPC,
       '00001'        AS tip_impt_IPC,
       '175121'       AS agrctacb_IPC,
       'USD'          AS coddiv_IPC  ,
       '1732911E'     AS ctacgbal_IPC,
       '2021-05'      AS fecha_desde ,
       '9999-12'      AS fecha_hasta ;