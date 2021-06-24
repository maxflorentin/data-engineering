set hive.merge.mapfiles=true;
set hive.merge.mapredfiles=true;
set hive.merge.size.per.task=4000000;
set hive.merge.smallfiles.avgsize=16000000;
set hive.exec.dynamic.partition.mode=nonstrict;
set mapred.job.queue.name=root.dataeng;

INSERT OVERWRITE TABLE bi_corp_bdr.tactico_mitre_garantias
SELECT 'contrato_tactico_mitre_1' AS contrato,
       ----Garantias Contrato----
       '23100'      AS s1emp    ,
       '00001'      AS tip_gara ,
       '060105534'  AS biengar1 ,
       '2019-07-19' AS fecini   ,
       '9999-12-31' AS fecbaja  ,
       '2022-07-12' AS fecvcto  ,
       '00001'      AS cod_gar  ,
       '00001'      AS cod_gar2 ,
       '00000'      AS tipo_ins ,
       'N'          AS ind_pign ,
       '00001'      AS tip_aval ,
       '00002'      AS tip_cobe ,
       '00001'      AS est_gara ,
       '99999'      AS comf_let ,
       '00000'      AS num_aseg ,
       'USD'        AS coddiv   ,
       '071540361'  AS idnumcli ,
       'N'          AS indblo   ,
       'N'          AS indrgosb ,
       'N'          AS indcobpf ,
       'S'          AS valaseju ,
       '00001'      AS ordapgar ,
       'N'          AS rangohip ,
       '00000'      AS n_impago ,
       ----Validez----
       '2021-05'    AS fecha_desde,
       '9999-12'    AS fecha_hasta;