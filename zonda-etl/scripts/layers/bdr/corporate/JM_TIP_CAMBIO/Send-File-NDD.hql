SELECT
	feoperac, coddiv, coddiv2, ortipcam, tpcam
FROM
	(
		SELECT
			0 AS ORDEN, 'feoperac' as feoperac, 'coddiv'as coddiv, 
			'coddiv2'  as coddiv2, 'ortipcam' as ortipcam, 'tpcam' as tpcam
		UNION ALL
		SELECT DISTINCT
			1                                                                              AS ORDEN
		  , concat(SUBSTRING(trim(n0738_feoperac),1,4),'-',SUBSTRING(trim(n0738_feoperac),5,2),'-',SUBSTRING(trim(n0738_feoperac),7,2)) as feoperac
		  , trim(n0738_coddiv)                                                             as coddiv
		  , trim(n0738_coddiv2)                                                            as coddiv2
		  , trim(n0738_ortipcam)                                                           as ortipcam
		  , concat(substring(trim(n0738_tpcam),1,5),'.',substring(trim(n0738_tpcam),7,13)) as tpcam
		FROM
			bi_corp_bdr.jm_tip_cambio
		WHERE
			partition_date = '$last_calendar_day'
	) A
ORDER BY ORDEN ASC;