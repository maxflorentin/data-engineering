SELECT  feoperac,s1emp,contra1,idnumcli,
        nivelapl,idumbral,salonbal,umbrabs,
        umbrrel,umbrpul,diaprper,umbpuls3
FROM 
(
	select  0 as orden,'feoperac' as feoperac,'s1emp' as s1emp,
            'contra1' as contra1,'idnumcli' as idnumcli,'nivelapl' as nivelapl,
            'idumbral' as idumbral,'salonbal' as salonbal,'umbrabs' as umbrabs,
            'umbrrel' as umbrrel,'umbrpul' as umbrpul,'diaprper' as diaprper,
            'umbpuls3' as umbpuls3 
	UNION ALL
	select  distinct 1 as orden,
            trim(feoperac) as feoperac,
            trim(s1emp)    as s1emp,
            lpad(trim(contra1),9,"0")  as contra1,
            lpad(trim(idnumcli),9,"0") as idnumcli,
            lpad(trim(nivelapl),5, "0") as nivelapl,
            lpad(trim(idumbral),9,"0") as idumbral,
            case when cast(trim(salonbal) as double)  >= 0
                    then  concat(substring(lpad(regexp_replace(regexp_replace(format_number(coalesce (cast(trim(salonbal) as double), 0), 2),"\\,|\\.",""),"\\,|\\.|\\-",""),17,"0") ,1,15),'.',substring(lpad(regexp_replace(regexp_replace(format_number(coalesce (cast(trim(salonbal) as double), 0), 2),"\\,|\\.",""),"\\,|\\.|\\-",""),17,"0") ,16,2)) 
                    else  concat(substring(concat("-", lpad(regexp_replace(regexp_replace(format_number(coalesce (cast(trim(salonbal) as double), 0), 2),"\\,|\\.",""),"\\,|\\.|\\-","") ,16,"0") ),1,15),'.',substring(concat("-", lpad(regexp_replace(regexp_replace(format_number(coalesce (cast(trim(salonbal) as double), 0), 2),"\\,|\\.",""),"\\,|\\.|\\-","") ,16,"0") ),16,2)) 
            end as salonbal,
            case when cast(trim(umbrabs) as double)  >= 0
                    then  concat(substring(lpad(regexp_replace(regexp_replace(format_number(coalesce (cast(trim(umbrabs) as double), 0), 2),"\\,|\\.",""),"\\,|\\.|\\-",""),17,"0") ,1,15),'.',substring(lpad(regexp_replace(regexp_replace(format_number(coalesce (cast(trim(umbrabs) as double), 0), 2),"\\,|\\.",""),"\\,|\\.|\\-",""),17,"0") ,16,2)) 
                    else  concat(substring(concat("-", lpad(regexp_replace(regexp_replace(format_number(coalesce (cast(trim(umbrabs) as double), 0), 2),"\\,|\\.",""),"\\,|\\.|\\-","") ,16,"0") ),1,15),'.',substring(concat("-", lpad(regexp_replace(regexp_replace(format_number(coalesce (cast(trim(umbrabs) as double), 0), 2),"\\,|\\.",""),"\\,|\\.|\\-","") ,16,"0") ),16,2)) 
            end as umbrabs,
            concat(lpad(substring(trim(umbrrel),1,1),3,"0"),'.',rpad(substring(trim(umbrrel),3,6),6,"0")) as umbrrel,
            concat(lpad(substring(trim(umbrpul),1,1),3,"0"),'.',rpad(substring(trim(umbrpul),3,6),6,"0")) as umbrpul,
            lpad(trim(diaprper),9,"0")as diaprper,
            concat(lpad(substring(trim(umbrpul),1,1),3,"0"),'.',rpad(substring(trim(umbrpul),3,6),6,"0")) as umbpuls3
	FROM bi_corp_bdr.jm_contr_ndd
	WHERE partition_date = '$last_calendar_day' 
) A
ORDER BY ORDEN ASC;