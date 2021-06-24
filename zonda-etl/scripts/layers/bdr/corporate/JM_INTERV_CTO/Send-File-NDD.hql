SELECT feoperac,s1emp,contra1,tipintev,tipintv2,numordin,idnumcli,formintv FROM(
    SELECT 0 AS ORDEN ,
        'feoperac' as feoperac, 's1emp' as s1emp, 'contra1' as contra1,
        'tipintev' as tipintev, 'tipintv2' as tipintv2, 'numordin' as numordin,
        'idnumcli'as idnumcli, 'formintv' as formintv
    UNION ALL
    select DISTINCT 1 AS ORDEN,
            feoperac,
            s1emp,
            contra1,
            tipintev,
            tipintv2,
            numordin,
            idnumcli,
            formintv
    from 
        (
            SELECT ROW_NUMBER() OVER(PARTITION BY feoperac,s1emp,contra1,tipintev,tipintv2,numordin ORDER BY contra1,idnumcli) AS N_duplicidad,
                    feoperac,
                    s1emp,
                    contra1,
                    tipintev,
                    tipintv2,
                    numordin,
                    idnumcli,
                    formintv
                FROM (
                            SELECT DISTINCT  
                                    trim(g4128_feoperac) as feoperac,
                                    trim(g4128_s1emp) as s1emp,
                                    trim(g4128_contra1) as contra1,
                                    trim(g4128_tipintev) as tipintev,
                                    trim(g4128_tipintv2) as tipintv2,
                                    concat(substring(trim(g4128_numordin),1,11),'.',substring(trim(g4128_numordin),12,6)) as numordin,
                                    trim(g4128_idnumcli) as idnumcli,
                                    trim(g4128_formintv) as formintv
                            FROM bi_corp_bdr.jm_interv_cto_bkup
                            WHERE partition_date = '$month_partition_bdr'
                    ) a	
        ) a
    where N_duplicidad = 1) A
ORDER BY ORDEN ASC;