set hive.merge.mapfiles=true;
set hive.merge.mapredfiles=true;
set hive.merge.size.per.task=4000000;
set hive.merge.smallfiles.avgsize=16000000;
set hive.exec.dynamic.partition.mode=nonstrict;
set mapred.job.queue.name=root.dataeng;

INSERT OVERWRITE TABLE bi_corp_bdr.tactico_mitre_intervinientes_contratos
SELECT 'contrato_tactico_mitre_1' AS contrato,
       ----Intervinientes Contratos----
       '23100'             AS empresa_IC,
       '00001'             AS tipintev_IC, --Titular
       '00006'             AS tipintv2_IC, --Titular
       '00000000001000000' AS numordin_IC,
       '071540361'         AS idnumcli_IC,
       ----Validez----
       '2021-05'           AS fecha_desde,
       '9999-12'           AS fecha_hasta
UNION ALL
SELECT 'contrato_tactico_mitre_1' AS contrato,
       ----Intervinientes Contratos----
       '23100'             AS empresa_IC,
       '00002'             AS tipintev_IC, --Avalista
       '00005'             AS tipintv2_IC, --Avalista
       '00000000001000000' AS numordin_IC,
       '060105534'         AS idnumcli_IC,
       ----Validez----
       '2021-05'           AS fecha_desde,
       '9999-12'           AS fecha_hasta;