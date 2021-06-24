SELECT s1emp,contra1,emp_ant,cont_ant,motrenu,fealtrel,motrenug,fec_baja FROM(
    SELECT 0 AS ORDEN ,
        's1emp' as s1emp, 'contra1' as contra1, 'emp_ant' as emp_ant,
        'cont_ant' as cont_ant, 'motrenu' as motrenu, 'fealtrel' as fealtrel,
        'motrenug' as motrenug, 'fec_baja' as fec_baja
    UNION ALL
    SELECT DISTINCT 1 AS ORDEN,
        trim(g7025_s1emp) as s1emp,
        trim(g7025_contra1) as contra1,
        trim(g7025_emp_ant) as emp_ant,
        trim(g7025_cont_ant) as cont_ant,
        trim(g7025_motrenu) as motrenu,
        trim(g7025_fealtrel) as fealtrel,
        trim(g7025_motrenug) as motrenug,
        trim(g7025_fec_baja) as fec_baja
    FROM bi_corp_bdr.jm_trz_cont_ren_bkup
    WHERE partition_date = '$month_partition_bdr') A
ORDER BY ORDEN ASC;