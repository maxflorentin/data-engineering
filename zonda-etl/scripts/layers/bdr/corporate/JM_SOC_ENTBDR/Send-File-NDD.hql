SELECT s1emp, entcgbal, descsoci,
	   coddiv, fecdesde, fechasta FROM(
	SELECT 0 AS ORDEN,
		's1emp' as s1emp, 'entcgbal' as entcgbal,
		'descsoci' as descsoci, 'coddiv' as coddiv,
		'fecdesde' as fecdesde, 'fechasta' as fechasta
	UNION ALL
	SELECT DISTINCT 1 AS ORDEN,
		trim(r3903_s1emp) as s1emp,
		trim(r3903_entcgbal) as entcgbal,
		trim(r3903_descsoci) as descsoci,
		trim(r3903_coddiv) as coddiv,
        concat(SUBSTRING(trim(r3903_fecdesde),1,4),'-',SUBSTRING(trim(r3903_fecdesde),5,2),'-',SUBSTRING(trim(r3903_fecdesde),7,2)) as fecdesde,
        concat(SUBSTRING(trim(r3903_fechasta),1,4),'-',SUBSTRING(trim(r3903_fechasta),5,2),'-',SUBSTRING(trim(r3903_fechasta),7,2)) as fechasta
	FROM bi_corp_bdr.jm_soc_entbdr
WHERE partition_date = '$last_calendar_day') A
ORDER BY ORDEN ASC;