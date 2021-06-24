SELECT feoperac, s1emp, idnumcli, inffecha, impfactm, tot_acti FROM(
	SELECT 0 AS ORDEN ,
		'feoperac' as feoperac, 's1emp' as s1emp,
		'idnumcli' as idnumcli, 'inffecha' as inffecha,
		'impfactm' as impfactm, 'tot_acti' as tot_acti
	UNION ALL
	SELECT DISTINCT 1 AS ORDEN,
		trim(g5508_feoperac) as feoperac,
		trim(g5508_s1emp) as s1emp,
		trim(g5508_idnumcli) as idnumcli,
		trim(g5508_inffecha) as inffecha,
		concat(substring(trim(g5508_impfactm),1,15),'.',substring(trim(g5508_impfactm),16,2)) as impfactm,
		concat(substring(trim(g5508_tot_acti),1,15),'.',substring(trim(g5508_tot_acti),16,2)) as tot_acti
	FROM bi_corp_bdr.jm_clien_juri_bkup
	WHERE partition_date = '$month_partition_bdr') A
ORDER BY ORDEN ASC;