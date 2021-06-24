SELECT feoperac,s1emp,grup_eco,impfactm,tot_acti,fecinfac FROM(
    SELECT 0 AS ORDEN ,
        'feoperac' as feoperac, 's1emp' as s1emp, 'grup_eco' as grup_eco,
        'impfactm' as impfactm, 'tot_acti' as tot_acti, 'fecinfac' as fecinfac
    UNION ALL
    SELECT DISTINCT 1 AS ORDEN,
        trim(g5512_feoperac) as feoperac,
        trim(g5512_s1emp)    as s1emp,
        trim(g5512_grup_eco) as grup_eco,
        concat(substring(trim(g5512_impfactm),1,15),'.',substring(trim(g5512_impfactm),16,2)) as impfactm,
        concat(substring(trim(g5512_tot_acti),1,15),'.',substring(trim(g5512_tot_acti),16,2)) as tot_acti,
        trim(g5512_fecinfac) as fecinfac
    FROM bi_corp_bdr.jm_grup_econo_bkup
    WHERE partition_date = '$month_partition_bdr') A
ORDER BY ORDEN ASC;