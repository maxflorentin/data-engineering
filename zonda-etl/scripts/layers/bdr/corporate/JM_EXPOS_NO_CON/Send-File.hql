SELECT DISTINCT CONCAT(E0627_FEOPERAC, E0627_S1EMP, E0627_CONTRA1, E0627_FEC_MES, E0627_CTA_CONT, E0627_AGRCTACB, E0627_IMPORTH, E0627_IDCTACEN, E0621_FECULTMO, E0627_CENTCTBL, E0627_ENTCGBAL, E0627_CTACGBAL) AS line FROM bi_corp_bdr.jm_expos_no_con WHERE partition_date = '$month_partition_bdr' and e0627_s1emp <> '00000';