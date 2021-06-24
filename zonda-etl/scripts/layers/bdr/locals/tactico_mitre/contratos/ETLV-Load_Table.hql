set hive.merge.mapfiles=true;
set hive.merge.mapredfiles=true;
set hive.merge.size.per.task=4000000;
set hive.merge.smallfiles.avgsize=16000000;
set hive.exec.dynamic.partition.mode=nonstrict;
set mapred.job.queue.name=root.dataeng;

insert overwrite table bi_corp_bdr.tactico_mitre_contratos
SELECT 'contrato_tactico_mitre_1' AS contrato,
       ----Contratos Bis----
       '23100'             AS empresa_CB,
       '0000'              AS sucursal_CB,
       '00032'             AS id_pais_CB, 
       '00013'             AS id_prod_CB, 
       '30001'             AS idpro_lc_CB,
       '2022-07-12'        AS fecvento_CB,
       '2022-07-12'        AS fecve2_CB,  
       '2019-07-19'        AS fechaper_CB,
       '00015'             AS idfinali_CB,
       'USD'               AS coddiv_CB,  
       'N'                 AS indlim_CB,
       ----Intervinientes Contratos----
       '23100'             AS empresa_IC,
       ----Contratos Otros Datos----
       '23100'             AS empresa_CO,
       '30001'             AS idsubprd_CO,
       '99999'             AS gest_sit_CO,
       '99999'             AS ges2_sit_CO,
       '23100'             AS emprepor_CO,
       '0000-00-00'        AS finiutct_CO,
       '0000-00-00'        AS ffinutct_CO,
       ----Importe Posicion Contrato----
       '23100'             AS empresa_IPC,
       ----Validez----
       '2021-05'           AS fecha_desde,
       '9999-12'           AS fecha_hasta;