SELECT DISTINCT CONCAT(g5512_feoperac,g5512_s1emp,g5512_grup_eco,g5512_dcodgrup,g5512_dgrupo,g5512_idsucur,g5512_id_pais,g5512_impfactm,g5512_tot_acti,g5512_num_empl,g5512_orig_fac,g5512_orig_act,g5512_orig_emp,g5512_impt_rgo,g5512_fecinfac,g5512_grecosec,g5512_coddiv,g5512_fecultmo,g5512_tdeudagr,g5512_flgempno) AS line FROM bi_corp_bdr.jm_grup_econo WHERE partition_date = '$month_partition_bdr';