set hive.merge.mapfiles=true;
set hive.merge.mapredfiles=true;
set hive.merge.size.per.task=4000000;
set hive.merge.smallfiles.avgsize=16000000;
set hive.exec.dynamic.partition.mode=nonstrict;
set mapred.job.queue.name=root.dataeng;

insert overwrite table bi_corp_bdr.tactico_mitre_clientes
SELECT '23100'             AS empresa_CB ,
       '071540361'         AS idnumcli_CB,
       lpad('2', 5, '0')   AS tip_pers_CB, --Juridica 
       --'FIDEICOMISO FINANCIERO BUENOS' AS apnomper_CB,
       ' '                 AS apnomper_CB,
       '00001'             AS carter_CB  ,
       '00032'             AS id_pais_CB ,
       '00015'             AS cod_sect_CB,
       '00015'             AS cod_sec2_CB,
       '00002'             AS cod_sec3_CB,
       '00001'             AS clisegm_CB ,
       rpad('WO', 40, ' ') AS tipsegl1_CB,
       rpad('OT', 40, ' ') AS tipsegl2_CB,
       --Validez--
       '2021-05'           AS fecha_desde,
       '9999-12'           AS fecha_hasta;