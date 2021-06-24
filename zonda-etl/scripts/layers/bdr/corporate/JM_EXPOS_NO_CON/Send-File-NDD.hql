SELECT feoperac, s1emp,
        contra1,  centctbl,
        ctacgbal, cta_cont,
        agrctacb, importh FROM(
    SELECT 0 AS ORDEN ,
        'feoperac' as feoperac, 's1emp'    as s1emp,
        'contra1'  as contra1,  'centctbl' as centctbl,
        'ctacgbal' as ctacgbal, 'cta_cont' as cta_cont,
        'agrctacb' as agrctacb, 'importh'  as importh
    UNION ALL
    SELECT DISTINCT 1 AS ORDEN,
        trim(e0627_feoperac) as feoperac,
        trim(e0627_s1emp)    as s1emp,
        trim(e0627_contra1)  as contra1,
        trim(e0627_centctbl) as centctbl,
        trim(e0627_ctacgbal) as ctacgbal,
        trim(e0627_cta_cont) as cta_cont,
        trim(e0627_agrctacb) as agrctacb,
        concat(substring(trim(e0627_importh),1,15),'.',substring(trim(e0627_importh),16,2)) as importh
    FROM bi_corp_bdr.jm_expos_no_con_bkup
    WHERE partition_date = '$month_partition_bdr' and e0627_s1emp <> '00000') A
ORDER BY ORDEN ASC;