SELECT DISTINCT CONCAT( g5519_feoperac,
						g5519_s1emp,
						g5519_contra1,
						g5519_metaplic,
						g5519_fasecalc,
						g5519_mtm,
						g5519_ead,
						g5519_espeprov,
						g5519_fecultmo,
						g5519_impnomct,
						g5519_addonbru,
						g5519_addonnet,
						g5519_coefregu,
						g5519_metliqui,
						g5519_mtm_brut) AS line
  FROM bi_corp_bdr.test_jm_ead_contr
 WHERE partition_date = '$month_partition_bdr';