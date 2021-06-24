SELECT feoperac, s1emp,contra1,fvtacart,imppdte,precioob,ind_credit FROM(
    SELECT 0 AS ORDEN ,
        'feoperac' as feoperac, 's1emp'   as s1emp,   'contra1'  as contra1,
        'fvtacart' as fvtacart, 'imppdte' as imppdte, 'precioob' as precioob,
        'ind_credit' as ind_credit
    UNION ALL
    SELECT DISTINCT 1 AS ORDEN,
        trim(r9736_feoperac) as feoperac,
        trim(r9736_s1emp) as s1emp,
        trim(r9736_contra1) as contra1,
        trim(r9736_fvtacart) as fvtacart,
        concat(substring(trim(r9736_imppdte),1,15),'.',substring(trim(r9736_imppdte),16,2)) as imppdte,
        concat(substring(trim(r9736_precioob),1,15),'.',substring(trim(r9736_precioob),16,2)) as precioob,
        trim(r9736_ind_credit) as ind_credit
    FROM bi_corp_bdr.jm_vta_carter_bkup
    WHERE partition_date = '$month_partition_bdr') A
ORDER BY ORDEN ASC;