SELECT DISTINCT CONCAT(H0711_FEOPERAC, 
                       H0711_S1EMP, 
                       H0711_CONTRA1, 
                       lpad(trim(H0711_MOTVCANC),5,"0"), 
                       H0711_FECULTMO) AS line 
FROM bi_corp_bdr.jm_ctos_cance 
WHERE partition_date = '$month_partition_bdr';