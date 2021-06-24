SELECT feoperac, s1emp,contra1,fecmvto,clasmvto,importe,salonbal FROM(
    SELECT 0 AS ORDEN ,
        'feoperac' as feoperac, 's1emp'  as s1emp,
        'contra1'  as contra1,  'fecmvto' as fecmvto,
        'clasmvto' as clasmvto, 'importe' as importe,
        'salonbal' as salonbal
    UNION ALL
    SELECT DISTINCT 1 AS ORDEN,
        trim(r9746_feoperac) as feoperac,
        trim(r9746_s1emp)    as s1emp,
        trim(r9746_contra1)  as contra1,
        trim(r9746_fecmvto)  as fecmvto,
        trim(r9746_clasmvto) as clasmvto,
        concat(substring(trim(r9746_importe),1,15),'.',substring(trim(r9746_importe),16,2)) as importe,
        concat(substring(trim(r9746_salonbal),1,15),'.',substring(trim(r9746_salonbal),16,2)) as salonbal
    FROM bi_corp_bdr.jm_flujos_bkup
    WHERE partition_date = '$month_partition_bdr') A
ORDER BY ORDEN ASC;    