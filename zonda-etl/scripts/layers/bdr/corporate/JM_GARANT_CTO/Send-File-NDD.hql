SELECT feoperac,s1emp,contra1,tip_gara,biengar1,cod_gar FROM(
    SELECT 0 AS ORDEN ,
        'feoperac' as feoperac, 's1emp'    as s1emp,
        'contra1'  as contra1,  'tip_gara' as tip_gara,
        'biengar1' as biengar1, 'cod_gar'  as cod_gar
    UNION ALL
    SELECT DISTINCT 1 AS ORDEN,
        trim(g4124_feoperac) as feoperac ,
        trim(g4124_s1emp)    as s1emp,
        trim(g4124_contra1)  as contra1,
        trim(g4124_tip_gara) as tip_gara,
        trim(g4124_biengar1) as biengar1,
        trim(g4124_cod_gar)  as cod_gar
    FROM bi_corp_bdr.JM_GARANT_CTO_bkup
    WHERE partition_date = '$month_partition_bdr') A
ORDER BY ORDEN ASC;