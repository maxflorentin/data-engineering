SELECT  
       feoperac
       ,s1emp
       ,contra1
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
    SELECT  
           concat(SUBSTRING(trim(B.r9739_feoperac),1,4),'-',SUBSTRING(trim(B.r9739_feoperac),5,2),'-',SUBSTRING(trim(B.r9739_feoperac),7,2)) AS feoperac 
           ,trim(B.r9739_s1emp)                                                                                                               AS s1emp 
           ,lpad(trim(B.r9739_contra1),9,"0")                                                                                                 AS contra1 
           ,lpad(trim(B.r9739_ind_def),5,"0")                                                                                                 AS ind_def 
           ,lpad(trim(B.r9739_dias_pp),9,"0")                                                                                                 AS dias_pp 
           ,lpad(trim(B.r9739_dias_atr),9,"0")                                                                                                AS dias_atr 
           ,trim(B.r9739_fec_def)                                                                                                             AS fec_def 
           ,trim(B.r9739_fini_pp)                                                                                                             AS fini_pp 
           ,trim(B.r9739_ffin_pp)                                                                                                             AS ffin_pp 
           ,concat(substring(trim(B.r9739_imp_atr),1,15),'.',substring(trim(B.r9739_imp_atr),17,2))                                           AS imp_atr 
           ,concat(substring(trim(B.r9739_sonbldef),1,15),'.',substring(trim(B.r9739_sonbldef),17,2))                                         AS sonbldef 
           ,trim(B.r9739_flgtecni)                                                                                                            AS flgtecni 
           ,lpad(trim(B.r9739_finalind),5,"0")                                                                                                AS finalind
    FROM bi_corp_bdr.jm_contr_otros_bkup AS A
    LEFT JOIN bi_corp_bdr.jm_out_cto_nd AS B
    ON (A.e0623_contra1=B.r9739_contra1)
    WHERE A.partition_date = '2021-03' 
    AND B.partition_date = '2021-03-31' 
    AND cast(B.r9739_ind_def AS bigint) = 2 
    AND cast(A.e0623_gest_sit AS bigint) IN (5,7,8,9,13,24) 
    AND A.e0623_finiutct < '2021-01-01' 
    AND cast(B.r9739_finalind AS bigint) = 1  
    UNION ALL
    SELECT  
           concat(SUBSTRING(trim(B.r9739_feoperac),1,4),'-',SUBSTRING(trim(B.r9739_feoperac),5,2),'-',SUBSTRING(trim(B.r9739_feoperac),7,2)) AS feoperac 
           ,trim(B.r9739_s1emp)                                                                                                               AS s1emp 
           ,lpad(trim(B.r9739_contra1),9,"0")                                                                                                 AS contra1 
           ,lpad(trim(B.r9739_ind_def),5,"0")                                                                                                 AS ind_def 
           ,lpad(trim(B.r9739_dias_pp),9,"0")                                                                                                 AS dias_pp 
           ,lpad(trim(B.r9739_dias_atr),9,"0")                                                                                                AS dias_atr 
           ,trim(B.r9739_fec_def)                                                                                                             AS fec_def 
           ,trim(B.r9739_fini_pp)                                                                                                             AS fini_pp 
           ,trim(B.r9739_ffin_pp)                                                                                                             AS ffin_pp 
           ,concat(substring(trim(B.r9739_imp_atr),1,15),'.',substring(trim(B.r9739_imp_atr),17,2))                                           AS imp_atr 
           ,concat(substring(trim(B.r9739_sonbldef),1,15),'.',substring(trim(B.r9739_sonbldef),17,2))                                         AS sonbldef 
           ,trim(B.r9739_flgtecni)                                                                                                            AS flgtecni 
           ,'00002'		/*campo ajustado*/																											          AS finalind
    FROM bi_corp_bdr.jm_contr_otros_bkup AS A
    LEFT JOIN bi_corp_bdr.jm_out_cto_nd AS B
    ON (A.e0623_contra1=B.r9739_contra1)
    WHERE A.partition_date = '2021-03' 
    AND B.partition_date = '2021-03-31' 
    AND cast(B.r9739_ind_def AS bigint) = 2 
    AND cast(A.e0623_gest_sit AS bigint) IN (5,7,8,9,13,24) 
    AND A.e0623_finiutct < '2021-01-01' 
    AND cast(B.r9739_finalind AS bigint) = 1  
    UNION ALL
    SELECT   
           concat(SUBSTRING(trim(B.r9739_feoperac),1,4),'-',SUBSTRING(trim(B.r9739_feoperac),5,2),'-',SUBSTRING(trim(B.r9739_feoperac),7,2)) AS feoperac 
           ,trim(B.r9739_s1emp)                                                                                                               AS s1emp 
           ,lpad(trim(B.r9739_contra1),9,"0")                                                                                                 AS contra1 
           ,'00040'	/*campo ajustado*/							                                                                             AS ind_def 
           ,lpad(trim(B.r9739_dias_pp),9,"0")                                                                                                 AS dias_pp 
           ,lpad(trim(B.r9739_dias_atr),9,"0")                                                                                                AS dias_atr 
           ,trim(B.r9739_fec_def)                                                                                                             AS fec_def 
           ,trim(B.r9739_fini_pp)                                                                                                             AS fini_pp 
           ,trim(B.r9739_ffin_pp)                                                                                                             AS ffin_pp 
           ,concat(substring(trim(B.r9739_imp_atr),1,15),'.',substring(trim(B.r9739_imp_atr),17,2))                                           AS imp_atr 
           ,concat(substring(trim(B.r9739_sonbldef),1,15),'.',substring(trim(B.r9739_sonbldef),17,2))                                         AS sonbldef 
           ,trim(B.r9739_flgtecni)                                                                                                            AS flgtecni 
           ,'00003'	/*campo ajustado*/											                                                              AS finalind
    FROM bi_corp_bdr.jm_contr_otros_bkup AS A
    LEFT JOIN bi_corp_bdr.jm_out_cto_nd AS B
    ON (A.e0623_contra1=B.r9739_contra1)
    WHERE A.partition_date = '2021-03' 
    AND B.partition_date = '2021-03-31' 
    AND cast(B.r9739_ind_def AS bigint) = 2 
    AND cast(A.e0623_gest_sit AS bigint) IN (5,7,8,9,13,24) 
    AND A.e0623_finiutct < '2021-01-01' 
    AND cast(B.r9739_finalind AS bigint) = 1  
    UNION ALL
    SELECT 
           NDD.feoperac 
           ,NDD.s1emp 
           ,NDD.contra1 
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
        SELECT   
               concat(SUBSTRING(trim(B.r9739_feoperac),1,4),'-',SUBSTRING(trim(B.r9739_feoperac),5,2),'-',SUBSTRING(trim(B.r9739_feoperac),7,2)) AS feoperac 
               ,trim(B.r9739_s1emp)                                                                                                               AS s1emp 
               ,lpad(trim(B.r9739_contra1),9,"0")                                                                                                 AS contra1 
               ,lpad(trim(B.r9739_ind_def),5,"0")                                                                                                 AS ind_def 
               ,lpad(trim(B.r9739_dias_pp),9,"0")                                                                                                 AS dias_pp 
               ,lpad(trim(B.r9739_dias_atr),9,"0")                                                                                                AS dias_atr 
               ,trim(B.r9739_fec_def)                                                                                                             AS fec_def 
               ,trim(B.r9739_fini_pp)                                                                                                             AS fini_pp 
               ,trim(B.r9739_ffin_pp)                                                                                                             AS ffin_pp 
               ,concat(substring(trim(B.r9739_imp_atr),1,15),'.',substring(trim(B.r9739_imp_atr),17,2))                                           AS imp_atr 
               ,concat(substring(trim(B.r9739_sonbldef),1,15),'.',substring(trim(B.r9739_sonbldef),17,2))                                         AS sonbldef 
               ,trim(B.r9739_flgtecni)                                                                                                            AS flgtecni 
               ,lpad(trim(B.r9739_finalind),5,"0")                                                                                                AS finalind
        FROM bi_corp_bdr.jm_contr_otros_bkup AS A
        LEFT JOIN bi_corp_bdr.jm_out_cto_nd AS B
        ON (A.e0623_contra1=B.r9739_contra1)
        WHERE A.partition_date = '2021-03' 
        AND B.partition_date = '2021-03-31' 
        AND cast(B.r9739_ind_def AS bigint) = 2 
        AND cast(A.e0623_gest_sit AS bigint) IN (5,7,8,9,13,24) 
        AND A.e0623_finiutct < '2021-01-01' 
        AND cast(B.r9739_finalind AS bigint) = 1  
    ) AS utp_ant
    RIGHT JOIN 
    (
        SELECT  
               trim(feoperac)             AS feoperac 
               ,trim(s1emp)                AS s1emp 
               ,lpad(trim(contra1),9,"0")  AS contra1 
               ,lpad(trim(ind_def),5,"0")  AS ind_def 
               ,lpad(trim(dias_pp),9,"0")  AS dias_pp 
               ,lpad(trim(dias_atr),9,"0") AS dias_atr 
               ,trim(fec_def)              AS fec_def 
               ,trim(fini_pp)              AS fini_pp 
               ,trim(ffin_pp)              AS ffin_pp 
               ,CASE WHEN cast(trim(imp_atr) AS double) >= 0 
                    THEN concat(substring(lpad(regexp_replace(regexp_replace(format_number(coalesce (cast(trim(imp_atr) AS double),0),2),"\\,|\\.",""),"\\,|\\.|\\-",""),17,"0") ,1,15),'.',substring(lpad(regexp_replace(regexp_replace(format_number(coalesce (cast(trim(imp_atr) AS double),0),2),"\\,|\\.",""),"\\,|\\.|\\-",""),17,"0") ,16,2))  
                    ELSE concat(substring(concat("-",lpad(regexp_replace(regexp_replace(format_number(coalesce (cast(trim(imp_atr) AS double),0),2),"\\,|\\.",""),"\\,|\\.|\\-","") ,16,"0") ),1,15),'.',substring(concat("-",lpad(regexp_replace(regexp_replace(format_number(coalesce (cast(trim(imp_atr) AS double),0),2),"\\,|\\.",""),"\\,|\\.|\\-","") ,16,"0") ),16,2)) 
                END AS imp_atr 
               ,CASE WHEN cast(trim(sonbldef) AS double) >= 0 
                    THEN concat(substring(lpad(regexp_replace(regexp_replace(format_number(coalesce (cast(trim(sonbldef) AS double),0),2),"\\,|\\.",""),"\\,|\\.|\\-",""),17,"0") ,1,15),'.',substring(lpad(regexp_replace(regexp_replace(format_number(coalesce (cast(trim(sonbldef) AS double),0),2),"\\,|\\.",""),"\\,|\\.|\\-",""),17,"0") ,16,2))  
                    ELSE concat(substring(concat("-",lpad(regexp_replace(regexp_replace(format_number(coalesce (cast(trim(sonbldef) AS double),0),2),"\\,|\\.",""),"\\,|\\.|\\-","") ,16,"0") ),1,15),'.',substring(concat("-",lpad(regexp_replace(regexp_replace(format_number(coalesce (cast(trim(sonbldef) AS double),0),2),"\\,|\\.",""),"\\,|\\.|\\-","") ,16,"0") ),16,2)) 
                END AS sonbldef 
               ,trim(flg_tecnico)          AS flgtecni 
               ,lpad(trim(finalind),5,"0") AS finalind
        FROM bi_corp_bdr.jm_out_cto_ndd
        WHERE partition_date = '2021-03-31'  
    ) AS NDD
    ON (utp_ant.contra1 = NDD.contra1)
    WHERE utp_ant.contra1 is null  
) A;