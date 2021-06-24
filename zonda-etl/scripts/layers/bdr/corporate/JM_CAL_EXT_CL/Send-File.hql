SELECT DISTINCT CONCAT(
        R9415_FEOPERAC, R9415_S1EMP   , R9415_IDNUMCLI,
        R9415_COD_AGEN, R9415_CCODPLZ , R9415_TIPMONED,
        R9415_FECCALIF, R9415_CALIFMAE) AS line
FROM bi_corp_bdr.jm_cal_ext_cl
WHERE partition_date = '$month_partition_bdr';