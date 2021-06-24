SELECT feoperac, s1emp, idnumcli, tip_pers, carter, clisegm, clisegl1, clisegl2,
       tipsegl2, utp_cli, finiutcl, ffinutcl FROM(
	SELECT 0 AS ORDEN ,
		'feoperac' as feoperac, 's1emp'    as s1emp,
		'idnumcli' as idnumcli, 'tip_pers' as tip_pers,
		'carter'   as carter, 'clisegm'    as clisegm,
		'clisegl1' as clisegl1, 'clisegl2' as clisegl2,
		'tipsegl2' as tipsegl2, 'utp_cli'  as utp_cli,
		'finiutcl' as finiutcl, 'ffinutcl' as ffinutcl
	UNION ALL
	SELECT DISTINCT 1 AS ORDEN,
		trim(g4093_feoperac) as feoperac,
		trim(g4093_s1emp)    as s1emp,
		trim(g4093_idnumcli) as idnumcli,
		trim(g4093_tip_pers) as tip_pers,
		trim(g4093_carter)   as carter,
		trim(g4093_clisegm)  as clisegm,
		trim(g4093_clisegl1) as clisegl1,
		trim(g4093_clisegl2) as clisegl2,
		trim(g4093_tipsegl2) as tipsegl2,
		trim(g4093_utp_cli)  as utp_cli,
		trim(g4093_finiutcl) as finiutcl,
		trim(g4093_ffinutcl) as ffinutcl
	FROM bi_corp_bdr.jm_client_bii_bkup
	WHERE partition_date = '$month_partition_bdr') A
ORDER BY ORDEN ASC;