SELECT DISTINCT CONCAT( e0665_s1emp,
						e0665_id_emisi,
						e0665_cod_emis,
						e0665_cod_agen,
						e0665_ccodplz,
						e0665_feccali,
						e0665_codmercd,
						e0665_fechasta,
						e0665_califmae,
						e0665_fecultmo) AS line
  FROM bi_corp_bdr.jm_cal_emision
 WHERE partition_date = '$month_partition_bdr';