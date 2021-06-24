SELECT  feoperac,s1emp,contra1,
       ind_def,dias_pp,dias_atr,
       fec_def,fini_pp,ffin_pp,
       imp_atr,sonbldef,flgtecni,finalind
FROM 
(
	SELECT  0 AS ORDEN,'feoperac' AS feoperac,'s1emp' AS s1emp,'contra1' AS contra1,
              'ind_def'  AS ind_def,'dias_pp'  AS dias_pp,'dias_atr' AS dias_atr,
              'fec_def'  AS fec_def,'fini_pp'  AS fini_pp,'ffin_pp'  AS ffin_pp,
              'imp_atr'  AS imp_atr,'sonbldef' AS sonbldef, 'flgtecni' as flgtecni,
              'finalind' AS finalind 
	UNION ALL
	SELECT  DISTINCT 1 AS ORDEN,
            trim(feoperac) as feoperac,
            trim(s1emp)    as s1emp,
            lpad(trim(contra1),9,"0") as contra1,
            lpad(trim(ind_def),5,"0")  as ind_def,
            lpad(trim(dias_pp),9,"0")  as dias_pp,
            lpad(trim(dias_atr),9,"0") as dias_atr,
            trim(fec_def)  as fec_def,
            trim(fini_pp)  as fini_pp,
            trim(ffin_pp)  as ffin_pp,
            case when cast(trim(imp_atr) as double)  >= 0
                    then  concat(substring(lpad(regexp_replace(regexp_replace(format_number(coalesce (cast(trim(imp_atr) as double), 0), 2),"\\,|\\.",""),"\\,|\\.|\\-",""),17,"0") ,1,15),'.',substring(lpad(regexp_replace(regexp_replace(format_number(coalesce (cast(trim(imp_atr) as double), 0), 2),"\\,|\\.",""),"\\,|\\.|\\-",""),17,"0") ,16,2)) 
                    else  concat(substring(concat("-", lpad(regexp_replace(regexp_replace(format_number(coalesce (cast(trim(imp_atr) as double), 0), 2),"\\,|\\.",""),"\\,|\\.|\\-","") ,16,"0") ),1,15),'.',substring(concat("-", lpad(regexp_replace(regexp_replace(format_number(coalesce (cast(trim(imp_atr) as double), 0), 2),"\\,|\\.",""),"\\,|\\.|\\-","") ,16,"0") ),16,2)) 
            end as imp_atr, 
            case when cast(trim(sonbldef) as double)  >= 0
                    then  concat(substring(lpad(regexp_replace(regexp_replace(format_number(coalesce (cast(trim(sonbldef) as double), 0), 2),"\\,|\\.",""),"\\,|\\.|\\-",""),17,"0") ,1,15),'.',substring(lpad(regexp_replace(regexp_replace(format_number(coalesce (cast(trim(sonbldef) as double), 0), 2),"\\,|\\.",""),"\\,|\\.|\\-",""),17,"0") ,16,2)) 
                    else  concat(substring(concat("-", lpad(regexp_replace(regexp_replace(format_number(coalesce (cast(trim(sonbldef) as double), 0), 2),"\\,|\\.",""),"\\,|\\.|\\-","") ,16,"0") ),1,15),'.',substring(concat("-", lpad(regexp_replace(regexp_replace(format_number(coalesce (cast(trim(sonbldef) as double), 0), 2),"\\,|\\.",""),"\\,|\\.|\\-","") ,16,"0") ),16,2)) 
            end as sonbldef, 
            trim(flg_tecnico) as flgtecni,
            lpad(trim(finalind),5,"0") as finalind
	FROM bi_corp_bdr.jm_out_cto_ndd
	WHERE partition_date = '$last_calendar_day' 
) A
ORDER BY ORDEN ASC;
