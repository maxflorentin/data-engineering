SELECT feoperac ,s1emp,
        contra1,  metaplic,
        fasecalc, ead FROM(
    SELECT 0 AS ORDEN ,
        'feoperac' as feoperac ,'s1emp'    as s1emp,
        'contra1'  as contra1,  'metaplic' as metaplic,
        'fasecalc' as fasecalc, 'ead'      as ead
    UNION ALL
    SELECT DISTINCT 1 AS ORDEN,
        trim(g5519_feoperac) as feoperac,
        trim(g5519_s1emp)    as s1emp,
        trim(g5519_contra1)  as contra1,
        trim(g5519_metaplic) as metaplic,
        trim(g5519_fasecalc) as fasecalc,
        concat(substring(trim(g5519_ead),1,15),'.',substring(trim(g5519_ead),16,2)) as ead
    FROM bi_corp_bdr.jm_ead_contr_bkup
    WHERE partition_date = '$month_partition_bdr') A
ORDER BY ORDEN ASC;    