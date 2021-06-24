SELECT feoperac,s1emp,grup_eco,idnumcli,rol_jera FROM(
    SELECT 0 AS ORDEN ,
        'feoperac' as feoperac, 's1emp' as s1emp, 'grup_eco' as grup_eco,
        'idnumcli' as idnumcli,'rol_jera' as rol_jera
    UNION ALL
    SELECT DISTINCT 1 AS ORDEN,
        trim(g5515_feoperac) as feoperac,
        trim(g5515_s1emp)    as s1emp,
        trim(g5515_grup_eco) as grup_eco,
        trim(g5515_idnumcli) as idnumcli,
        trim(g5515_rol_jera) as rol_jera
    FROM bi_corp_bdr.jm_grup_rela_bkup
    WHERE partition_date = '$month_partition_bdr') A
ORDER BY ORDEN ASC;