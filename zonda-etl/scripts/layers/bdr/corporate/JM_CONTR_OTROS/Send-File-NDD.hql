SELECT feoperac, s1emp, contra1,  idsubprd, ctb_situ, gest_sit,
       alimact,  inv_nego, finiutct, ffinutct FROM(
    SELECT 0 AS ORDEN ,
        'feoperac' as feoperac, 's1emp'    as s1emp,
        'contra1'  as contra1,  'idsubprd' as idsubprd,
        'ctb_situ' as ctb_situ, 'gest_sit' as gest_sit,
        'alimact'  as alimact,  'inv_nego' as inv_nego,
        'finiutct' as finiutct, 'ffinutct' as ffinutct
    UNION ALL
    SELECT DISTINCT 1 AS ORDEN,
        trim(e0623_feoperac) as feoperac,
        trim(e0623_s1emp)    as s1emp,
        trim(e0623_contra1)  as contra1,
        trim(e0623_idsubprd) as idsubprd,
        trim(e0623_ctb_situ) as ctb_situ,
        trim(e0623_gest_sit) as gest_sit,
        concat(substring(trim(e0623_alimact),1,15),'.',substring(trim(e0623_alimact),16,2)) as alimact,
        trim(e0623_inv_nego) as inv_nego,
        trim(e0623_finiutct) as finiutct,
        trim(e0623_ffinutct) as ffinutct
    FROM bi_corp_bdr.jm_contr_otros_bkup
    WHERE partition_date = '$month_partition_bdr') A
ORDER BY ORDEN ASC;