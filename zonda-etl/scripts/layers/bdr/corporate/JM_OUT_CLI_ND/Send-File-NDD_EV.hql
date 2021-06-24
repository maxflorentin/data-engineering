SELECT  ORDEN
       ,feoperac
       ,s1emp
       ,idnumcli
       ,ind_def
       ,dias_pp
       ,dias_atr
       ,fec_def
       ,fini_pp
       ,ffin_pp
       ,imp_atr
       ,sonbldef
       ,flgtecni
       ,finalind
FROM 
(
    SELECT  0          AS ORDEN
           ,'feoperac' AS feoperac
           ,'s1emp'    AS s1emp
           ,'idnumcli' AS idnumcli
           ,'ind_def'  AS ind_def
           ,'dias_pp'  AS dias_pp
           ,'dias_atr' AS dias_atr
           ,'fec_def'  AS fec_def
           ,'fini_pp'  AS fini_pp
           ,'ffin_pp'  AS ffin_pp
           ,'imp_atr'  AS imp_atr
           ,'sonbldef' AS sonbldef
           ,'flgtecni' AS flgtecni
           ,'finalind' AS finalind 
    UNION ALL
    SELECT  DISTINCT 1 AS ORDEN
           ,concat(SUBSTRING(trim(B.r9741_feoperac),1,4),'-',SUBSTRING(trim(B.r9741_feoperac),5,2),'-',SUBSTRING(trim(B.r9741_feoperac),7,2)) AS feoperac
           ,trim(B.r9741_s1emp)                                                                                                            AS s1emp
           ,trim(B.r9741_idnumcli)                                                                                                         AS idnumcli
           ,trim(B.r9741_ind_def)                                                                                                          AS ind_def
           ,trim(B.r9741_dias_pp)                                                                                                          AS dias_pp
           ,trim(B.r9741_dias_atr)                                                                                                         AS dias_atr
           ,concat(SUBSTRING(trim(B.r9741_fec_def),1,4),'-',SUBSTRING(trim(B.r9741_fec_def),5,2),'-',SUBSTRING(trim(B.r9741_fec_def),7,2)) AS fec_def
           ,concat(SUBSTRING(trim(B.r9741_fini_pp),1,4),'-',SUBSTRING(trim(B.r9741_fini_pp),5,2),'-',SUBSTRING(trim(B.r9741_fini_pp),7,2)) AS fini_pp
           ,concat(SUBSTRING(trim(B.r9741_ffin_pp),1,4),'-',SUBSTRING(trim(B.r9741_ffin_pp),5,2),'-',SUBSTRING(trim(B.r9741_ffin_pp),7,2)) AS ffin_pp
           ,concat(substring(trim(B.r9741_imp_atr),1,15),'.',substring(trim(B.r9741_imp_atr),17,2))                                        AS imp_atr
           ,concat(substring(trim(B.r9741_sonbldef),1,15),'.',substring(trim(B.r9741_sonbldef),17,2))                                      AS sonbldef
           ,trim(B.r9741_flgtecni)                                                                                                         AS flgtecni
           ,trim(B.r9741_finalind)                                                                                                         AS finalind
    FROM bi_corp_bdr.jm_client_bii_bkup AS A
    LEFT JOIN bi_corp_bdr.jm_out_cli_nd AS B
    ON (A.g4093_idnumcli=B.r9741_idnumcli)
    WHERE A.partition_date = '2021-03' 
    AND B.partition_date = '2021-03-31' 
    AND cast(B.r9741_ind_def AS bigint) = 3 
    AND cast(A.g4093_utp_cli AS bigint) IN (1,2,3) 
    AND A.g4093_finiutcl < '2021-01-01' 
    AND cast(B.r9741_finalind AS bigint) = 1  
    UNION ALL
    SELECT  DISTINCT 1                                                                                                                        AS ORDEN
           ,concat(SUBSTRING(trim(B.r9741_feoperac),1,4),'-',SUBSTRING(trim(B.r9741_feoperac),5,2),'-',SUBSTRING(trim(B.r9741_feoperac),7,2)) AS feoperac
           ,trim(B.r9741_s1emp)                                                                                                               AS s1emp
           ,trim(B.r9741_idnumcli)                                                                                                            AS idnumcli
           ,'99999'			/*campo ajustado*/					                                                                              AS ind_def
           ,trim(B.r9741_dias_pp)                                                                                                             AS dias_pp
           ,trim(B.r9741_dias_atr)                                                                                                            AS dias_atr
           ,'9999-12-31'	/*campo ajustado*/							                                                                       AS fec_def
           ,'9999-12-31'	/*campo ajustado*/							                                                                       AS fini_pp
           ,'9999-12-31'	/*campo ajustado*/							                                                                       AS ffin_pp
           ,concat(substring(trim(B.r9741_imp_atr),1,15),'.',substring(trim(B.r9741_imp_atr),17,2))                                           AS imp_atr
           ,concat(substring(trim(B.r9741_sonbldef),1,15),'.',substring(trim(B.r9741_sonbldef),17,2))                                         AS sonbldef
           ,trim(B.r9741_flgtecni)                                                                                                            AS flgtecni
           ,'00002'			/*campo ajustado*/										                                                                            AS finalind
    FROM bi_corp_bdr.jm_client_bii_bkup AS A
    LEFT JOIN bi_corp_bdr.jm_out_cli_nd AS B
    ON (A.g4093_idnumcli=B.r9741_idnumcli)
    WHERE A.partition_date = '2021-03' 
    AND B.partition_date = '2021-03-31' 
    AND cast(B.r9741_ind_def AS bigint) = 3 
    AND cast(A.g4093_utp_cli AS bigint) IN (1,2,3) 
    AND A.g4093_finiutcl < '2021-01-01' 
    AND cast(B.r9741_finalind AS bigint) = 1 
    UNION ALL
    SELECT  NDD.ORDEN 
           ,NDD.feoperac 
           ,NDD.s1emp 
           ,NDD.idnumcli 
           ,NDD.ind_def 
           ,NDD.dias_pp 
           ,NDD.dias_atr 
           ,NDD.fec_def 
           ,NDD.fini_pp 
           ,NDD.ffin_pp 
           ,NDD.imp_atr 
           ,NDD.sonbldef 
           ,NDD.flgtecni 
           ,NDD.finalind
    FROM 
    (
        SELECT  DISTINCT 1 AS ORDEN 
                ,concat(SUBSTRING(trim(B.r9741_feoperac),1,4),'-',SUBSTRING(trim(B.r9741_feoperac),5,2),'-',SUBSTRING(trim(B.r9741_feoperac),7,2)) AS feoperac
               ,trim(B.r9741_s1emp)                                                                                                            AS s1emp
               ,trim(B.r9741_idnumcli)                                                                                                         AS idnumcli
               ,trim(B.r9741_ind_def)                                                                                                          AS ind_def
               ,trim(B.r9741_dias_pp)                                                                                                          AS dias_pp
               ,trim(B.r9741_dias_atr)                                                                                                         AS dias_atr
               ,concat(SUBSTRING(trim(B.r9741_fec_def),1,4),'-',SUBSTRING(trim(B.r9741_fec_def),5,2),'-',SUBSTRING(trim(B.r9741_fec_def),7,2)) AS fec_def
               ,concat(SUBSTRING(trim(B.r9741_fini_pp),1,4),'-',SUBSTRING(trim(B.r9741_fini_pp),5,2),'-',SUBSTRING(trim(B.r9741_fini_pp),7,2)) AS fini_pp
               ,concat(SUBSTRING(trim(B.r9741_ffin_pp),1,4),'-',SUBSTRING(trim(B.r9741_ffin_pp),5,2),'-',SUBSTRING(trim(B.r9741_ffin_pp),7,2)) AS ffin_pp
               ,concat(substring(trim(B.r9741_imp_atr),1,15),'.',substring(trim(B.r9741_imp_atr),17,2))                                        AS imp_atr
               ,concat(substring(trim(B.r9741_sonbldef),1,15),'.',substring(trim(B.r9741_sonbldef),17,2))                                      AS sonbldef
               ,trim(B.r9741_flgtecni)                                                                                                         AS flgtecni
               ,trim(B.r9741_finalind)                                                                                                         AS finalind
        FROM bi_corp_bdr.jm_client_bii_bkup AS A
        LEFT JOIN bi_corp_bdr.jm_out_cli_nd AS B
        ON (A.g4093_idnumcli=B.r9741_idnumcli)
        WHERE A.partition_date = '2021-03' 
        AND B.partition_date = '2021-03-31' 
        AND cast(B.r9741_ind_def AS bigint) = 3 
        AND cast(A.g4093_utp_cli AS bigint) IN (1,2,3) 
        AND A.g4093_finiutcl < '2021-01-01' 
        AND cast(B.r9741_finalind AS bigint) = 1 
    ) AS utp_ant
    RIGHT JOIN 
    (
        SELECT  DISTINCT 1                 AS ORDEN
               ,trim(feoperac)             AS feoperac
               ,trim(s1emp)                AS s1emp
               ,lpad(trim(idnumcli),9,"0") AS idnumcli
               ,lpad(trim(ind_def),5,"0")  AS ind_def
               ,lpad(trim(dias_pp),9,"0")  AS dias_pp
               ,lpad(trim(dias_atr),9,"0") AS dias_atr
               ,trim(fec_def)              AS fec_def
               ,trim(fini_pp)              AS fini_pp
               ,trim(ffin_pp)              AS ffin_pp
               ,case when cast(trim(imp_atr) as double)  >= 0
                             then  concat(substring(lpad(regexp_replace(regexp_replace(format_number(coalesce (cast(trim(imp_atr) as double), 0), 2),"\\,|\\.",""),"\\,|\\.|\\-",""),17,"0") ,1,15),'.',substring(lpad(regexp_replace(regexp_replace(format_number(coalesce (cast(trim(imp_atr) as double), 0), 2),"\\,|\\.",""),"\\,|\\.|\\-",""),17,"0") ,16,2)) 
                             else  concat(substring(concat("-", lpad(regexp_replace(regexp_replace(format_number(coalesce (cast(trim(imp_atr) as double), 0), 2),"\\,|\\.",""),"\\,|\\.|\\-","") ,16,"0") ),1,15),'.',substring(concat("-", lpad(regexp_replace(regexp_replace(format_number(coalesce (cast(trim(imp_atr) as double), 0), 2),"\\,|\\.",""),"\\,|\\.|\\-","") ,16,"0") ),16,2)) 
                        end as imp_atr
               ,case when cast(trim(sonbldef) as double)  >= 0
                             then  concat(substring(lpad(regexp_replace(regexp_replace(format_number(coalesce (cast(trim(sonbldef) as double), 0), 2),"\\,|\\.",""),"\\,|\\.|\\-",""),17,"0") ,1,15),'.',substring(lpad(regexp_replace(regexp_replace(format_number(coalesce (cast(trim(sonbldef) as double), 0), 2),"\\,|\\.",""),"\\,|\\.|\\-",""),17,"0") ,16,2)) 
                             else  concat(substring(concat("-", lpad(regexp_replace(regexp_replace(format_number(coalesce (cast(trim(sonbldef) as double), 0), 2),"\\,|\\.",""),"\\,|\\.|\\-","") ,16,"0") ),1,15),'.',substring(concat("-", lpad(regexp_replace(regexp_replace(format_number(coalesce (cast(trim(sonbldef) as double), 0), 2),"\\,|\\.",""),"\\,|\\.|\\-","") ,16,"0") ),16,2)) 
                       end as sonbldef
               ,trim(flg_tecnico)          AS flgtecni
               ,lpad(trim(finalind),5,"0") AS finalind
        FROM bi_corp_bdr.jm_out_cli_ndd
        WHERE partition_date = '2021-03-31' 
    ) AS NDD
    ON (utp_ant.idnumcli = NDD.idnumcli)
    WHERE utp_ant.idnumcli is null  
) A
ORDER BY ORDEN ASC;