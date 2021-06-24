SELECT feoperac,s1emp,contra1,cta_cont,tip_impt,centctbl,ctacgbal,agrctacb,importh FROM(
    SELECT 0 AS ORDEN ,
        'feoperac' as feoperac, 's1emp' as s1emp, 'contra1' as contra1,
        'cta_cont' as cta_cont, 'tip_impt' as tip_impt, 'centctbl' as centctbl,
        'ctacgbal' as ctacgbal, 'agrctacb' as agrctacb, 'importh' as importh
    UNION ALL
    SELECT DISTINCT 1 AS ORDEN,
        trim(e0621_feoperac) as feoperac,
        trim(e0621_s1emp)    as s1emp,
        trim(e0621_contra1)  as contra1,
        trim(e0621_cta_cont) as cta_cont,
        trim(e0621_tip_impt) as tip_impt,
        trim(e0621_centctbl) as centctbl,
        trim(e0621_ctacgbal) as ctacgbal,
        trim(e0621_agrctacb) as agrctacb,
        concat(substring(trim(e0621_importh),1,15),'.',substring(trim(e0621_importh),16,2)) as importh
    FROM bi_corp_bdr.jm_posic_contr_bkup
    WHERE partition_date = '$month_partition_bdr') A
ORDER BY ORDEN ASC;