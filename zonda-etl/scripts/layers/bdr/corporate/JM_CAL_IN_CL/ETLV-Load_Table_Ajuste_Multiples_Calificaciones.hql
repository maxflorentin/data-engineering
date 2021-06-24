set hive.merge.mapfiles=true;
set hive.merge.mapredfiles=true;
set hive.merge.size.per.task=4000000;
set hive.merge.smallfiles.avgsize=16000000;
set hive.exec.dynamic.partition.mode=nonstrict;
set mapred.job.queue.name=root.dataeng;

--Incidencia: desaparecian calificaciones en la tabla de calificación cliente. 
--Causa de la incidencia: esto se debe a un procedimiento de Valida en el cual 
--                        cuando hay dos calificaciones para un cliente, se 
--                        eliminan ambas calificaciones. 
--Solucion: se definio la siguiente logica para que por cada cliente solo haya
--          una y solo una calificacion por mes.

drop table temp_jm_cal_in_cl;

CREATE TEMPORARY TABLE temp_jm_cal_in_cl (
	feoperac string, s1emp    string, idnumcli string, feccali  string, tipmodel string, 
	tipmode2 string, idmodel  string, tipo     string, idpunsco string, feccaduc string, 
	c1tarpun string, c1spid   string, c1digcon string, fecultmo string, motivfor string, 
	idpunsc2 string, fecrepfi string, fecinofc string
);

WITH multiplicadosParte1 AS (
	SELECT e9954_idnumcli
	FROM bi_corp_bdr.jm_cal_in_cl
	WHERE partition_date = '{{ ti.xcom_pull(task_ids='InputConfig', key='month_partition_bdr', dag_id='BDR_LOAD_Tables-Monthly') }}'
	GROUP BY e9954_idnumcli
	HAVING COUNT(*) != 1 
),
multiplicadosParte2 AS (
	SELECT cal.e9954_feoperac, cal.e9954_s1emp,    cal.e9954_idnumcli,
    	   cal.e9954_feccali,  cal.e9954_tipmodel, cal.e9954_tipmode2,
	       cal.e9954_idmodel,  cal.e9954_tipo,     cal.e9954_idpunsco,
	       cal.e9954_feccaduc, cal.e9954_c1tarpun, cal.e9954_c1spid,
	       cal.e9954_c1digcon, cal.e0621_fecultmo, cal.e9954_motivfor,
       	   cal.e9954_idpunsc2, cal.e9954_fecrepfi, cal.e9954_fecinofc
	FROM bi_corp_bdr.jm_cal_in_cl cal 
		INNER JOIN multiplicadosParte1 m
			ON cal.e9954_idnumcli = m.e9954_idnumcli
			AND cal.partition_date = '{{ ti.xcom_pull(task_ids='InputConfig', key='month_partition_bdr', dag_id='BDR_LOAD_Tables-Monthly') }}'
	WHERE partition_date = '{{ ti.xcom_pull(task_ids='InputConfig', key='month_partition_bdr', dag_id='BDR_LOAD_Tables-Monthly') }}'
),
simples AS (
	SELECT cal.e9954_feoperac, cal.e9954_s1emp,       cal.e9954_idnumcli,
           cal.e9954_feccali,  cal.e9954_tipmodel,    cal.e9954_tipmode2,
           cal.e9954_idmodel,  '00093' AS e9954_tipo, cal.e9954_idpunsco,
           cal.e9954_feccaduc, cal.e9954_c1tarpun,    cal.e9954_c1spid,
           cal.e9954_c1digcon, cal.e0621_fecultmo,    cal.e9954_motivfor,
           cal.e9954_idpunsc2, cal.e9954_fecrepfi,    cal.e9954_fecinofc
	FROM bi_corp_bdr.jm_cal_in_cl cal 
		LEFT JOIN multiplicadosParte1 m
			ON cal.e9954_idnumcli = m.e9954_idnumcli
			AND cal.partition_date = '{{ ti.xcom_pull(task_ids='InputConfig', key='month_partition_bdr', dag_id='BDR_LOAD_Tables-Monthly') }}'
	WHERE partition_date = '{{ ti.xcom_pull(task_ids='InputConfig', key='month_partition_bdr', dag_id='BDR_LOAD_Tables-Monthly') }}'
		AND m.e9954_idnumcli IS NULL
),
filtroInicialParte1 AS (
	SELECT *
	FROM multiplicadosParte2
	WHERE e9954_tipmode2 NOT IN ('00000', '00301', '00306', '00307',
                                 '00310', '00315', '00316', '99999')
),
filtroInicialParte2 AS (
	SELECT *
	FROM filtroInicialParte1
	WHERE (e9954_tipmode2 != '00324' AND cast(e9954_idpunsco AS bigint) >= 10000000) --me quedo con los >= a '1' para los tipmode != 324. Pongo 10.000.000 ya que el numero posee 7 decimales
		OR (e9954_tipmode2 = '00324' AND cast(e9954_idpunsco AS bigint) > 1000000000) --me quedo con los > a '100' para los tipmode = 324. Pongo 1.000.000.000 ya que el numero posee 7 decimales 
),
segundoFiltroParte1 AS ( --101 vs 103
	SELECT ROW_NUMBER() OVER(partition by e9954_idnumcli order by e9954_feccali desc) AS orden, 
		   t1.*
	FROM filtroInicialParte2 t1	
	WHERE t1.e9954_tipmode2 IN ('00101','00103')
),
segundoFiltroParte2 AS ( --101 vs 103
	SELECT e9954_feoperac, e9954_s1emp,           e9954_idnumcli,
		   e9954_feccali,  e9954_tipmodel,        e9954_tipmode2,
		   e9954_idmodel,  '00093' AS e9954_tipo, e9954_idpunsco,
		   e9954_feccaduc, e9954_c1tarpun,        e9954_c1spid,
		   e9954_c1digcon, e0621_fecultmo,        e9954_motivfor,
		   e9954_idpunsc2, e9954_fecrepfi,        e9954_fecinofc
	FROM segundoFiltroParte1
	WHERE orden = 1
),
clientesRestantes AS (
	SELECT fi.*
	FROM filtroInicialParte2 fi
		LEFT JOIN segundoFiltroParte2 sf
			ON fi.e9954_idnumcli = sf.e9954_idnumcli
	WHERE sf.e9954_idnumcli IS NULL
),
losMultiplicadosDeLosRestantes AS (
	SELECT e9954_idnumcli
	FROM clientesRestantes
	GROUP BY e9954_idnumcli
	HAVING count(*) != 1
),
losMultiplicadosDeLosRestantesCompleto AS (
	SELECT cr.*
	FROM clientesRestantes cr 
		INNER JOIN losMultiplicadosDeLosRestantes h
			ON cr.e9954_idnumcli = h.e9954_idnumcli
),
losSimplesDeLosRestantes AS (
	SELECT cr.e9954_feoperac, cr.e9954_s1emp,        cr.e9954_idnumcli,
		   cr.e9954_feccali,  cr.e9954_tipmodel,     cr.e9954_tipmode2,
		   cr.e9954_idmodel,  '00093' AS e9954_tipo, cr.e9954_idpunsco,
           cr.e9954_feccaduc, cr.e9954_c1tarpun,     cr.e9954_c1spid,
           cr.e9954_c1digcon, cr.e0621_fecultmo,     cr.e9954_motivfor,
           cr.e9954_idpunsc2, cr.e9954_fecrepfi,     cr.e9954_fecinofc
	FROM clientesRestantes cr 
		LEFT JOIN losMultiplicadosDeLosRestantes h
			ON cr.e9954_idnumcli = h.e9954_idnumcli
	WHERE h.e9954_idnumcli IS NULL
)
INSERT OVERWRITE TABLE temp_jm_cal_in_cl 
SELECT * FROM segundoFiltroParte2                    UNION ALL 
SELECT * FROM losMultiplicadosDeLosRestantesCompleto UNION ALL
SELECT * FROM losSimplesDeLosRestantes               UNION ALL
SELECT * FROM simples;

--------------------------------------

WITH temporal AS (
	SELECT * 
	FROM temp_jm_cal_in_cl
	WHERE tipo IS NULL 
		AND tipmode2 IN ('00302','00303','00304','00305','00308', 
		                 '00309','00323','00326','00327','00404') 
),
orden AS (
	SELECT '00404' AS calif, 0 AS orden UNION ALL
	SELECT '00327' AS calif, 1 AS orden UNION ALL
	SELECT '00326' AS calif, 2 AS orden UNION ALL
	SELECT '00323' AS calif, 3 AS orden UNION ALL
	SELECT '00309' AS calif, 4 AS orden UNION ALL
	SELECT '00308' AS calif, 5 AS orden UNION ALL
	SELECT '00305' AS calif, 6 AS orden UNION ALL
	SELECT '00304' AS calif, 7 AS orden UNION ALL
	SELECT '00303' AS calif, 8 AS orden UNION ALL
	SELECT '00302' AS calif, 9 AS orden
),
numeracion AS (
	SELECT o.*, t.*
	FROM temporal t 
		LEFT JOIN orden o 
			ON t.tipmode2 = o.calif
),
primerFiltro AS ( --404 vs lista
	SELECT t1.feoperac, 
           t1.idnumcli AS numcli, 
           t1.feccali,
           t1.tipmode2 AS tipmode2_del_401, 
           t1.orden AS orden_del_401,
           t2.tipmode2 AS tipmode2_de_lista, 
		   CAST(months_between(t1.feoperac,t1.feccali) AS int) AS month_dif,
           t2.orden AS orden_de_lista
	FROM numeracion t1 
		INNER JOIN numeracion t2
			ON t1.idnumcli = t2.idnumcli
			AND t1.tipmode2 = '00404'
			AND t2.tipmode2 IN ('00302','00303','00304','00305','00308', 
			                    '00309','00323','00326','00327')
),
restaFechas AS (
	SELECT DISTINCT numcli, 
					CASE WHEN month_dif <= 6 THEN tipmode2_del_401 
					     ELSE tipmode2_de_lista END AS tipmode2,
					CASE WHEN month_dif <= 6 THEN orden_del_401 
					     ELSE orden_de_lista END AS orden
	FROM primerFiltro 
),
priorizacion AS (
	SELECT ROW_NUMBER() over(partition by numcli order by orden asc) AS ordenNuevo, 
	       cr.numcli, 
		   cr.tipmode2
	FROM restaFechas cr
),
filtroPriorizacion AS (
	SELECT numcli, tipmode2 
	FROM priorizacion 
	WHERE ordennuevo = 1
),
losRestantes AS (
	SELECT n.*
	FROM numeracion n 
		LEFT JOIN filtroPriorizacion fp 
			ON n.idnumcli = fp.numcli
	WHERE fp.numcli IS NULL
),
priorizacionRestantes AS (
	SELECT ROW_NUMBER() over(partition by idnumcli order by orden asc) AS ordenNuevo, 
	       cr.*
	FROM losRestantes cr
),
filtroPriorizacionRestantes AS (
	SELECT feoperac, s1emp,   idnumcli, feccali,  tipmodel, tipmode2, idmodel,  
           '00093' AS tipo,   idpunsco, feccaduc, c1tarpun, c1spid,   c1digcon, 
		   fecultmo, motivfor,idpunsc2, fecrepfi, fecinofc
	FROM priorizacionRestantes 
	WHERE ordennuevo = 1
),
todos AS (
	SELECT c.feoperac, c.s1emp,    c.idnumcli, c.feccali, c.tipmodel, c.tipmode2, c.idmodel,  
		   CASE WHEN c.tipo IS NULL THEN c.tipo ELSE '00093' END AS tipo,
		   c.idpunsco, c.feccaduc, c.c1tarpun, c.c1spid,  c.c1digcon, c.fecultmo, c.motivfor,
		   c.idpunsc2, c.fecrepfi, c.fecinofc
	FROM temporal c 
		INNER JOIN filtroPriorizacion fn
			ON fn.numcli = c.idnumcli 
			AND fn.tipmode2 = c.tipmode2
	UNION ALL
	SELECT c.feoperac, c.s1emp,    c.idnumcli, c.feccali, c.tipmodel, c.tipmode2, c.idmodel,  
		   CASE WHEN fn.tipo IS NULL THEN c.tipo ELSE '00093' END AS tipo,
   		   c.idpunsco, c.feccaduc, c.c1tarpun, c.c1spid,  c.c1digcon, c.fecultmo, c.motivfor,
		   c.idpunsc2, c.fecrepfi, c.fecinofc
	FROM temporal c 
		INNER JOIN filtroPriorizacionRestantes fn
			ON fn.idnumcli = c.idnumcli 
			AND fn.tipmode2 = c.tipmode2
)
INSERT OVERWRITE TABLE temp_jm_cal_in_cl
SELECT c.feoperac, c.s1emp,    c.idnumcli, c.feccali,  c.tipmodel, c.tipmode2, c.idmodel,
	   c.tipo,     c.idpunsco, c.feccaduc, c.c1tarpun, c.c1spid,   c.c1digcon, c.fecultmo,
	   c.motivfor, c.idpunsc2, c.fecrepfi, c.fecinofc
FROM temp_jm_cal_in_cl c
	LEFT JOIN todos t
		ON t.idnumcli = c.idnumcli 
		AND t.tipmode2 = c.tipmode2
WHERE t.idnumcli IS NULL 
UNION ALL 
SELECT * 
FROM todos;

--------------------------------------

WITH temporal AS (
	SELECT * 
	FROM temp_jm_cal_in_cl 
	WHERE tipo IS NULL 
		AND tipmode2 in ('00321','00322','00502','00503','00504','00501')
),
orden AS (
	SELECT '00501' AS calif, 0 AS orden UNION ALL 
	SELECT '00322' AS calif, 1 AS orden UNION ALL 
	SELECT '00321' AS calif, 2 AS orden UNION ALL 
	SELECT '00502' AS calif, 3 AS orden UNION ALL 
	SELECT '00503' AS calif, 4 AS orden UNION ALL 
	SELECT '00504' AS calif, 5 AS orden
),
numeracion AS (
	SELECT o.*, t.*
	FROM temporal t 
		LEFT JOIN orden o 
			ON t.tipmode2 = o.calif
),
primerFiltro AS ( --501 vs lista
	SELECT t1.feoperac, 
		   t1.idnumcli AS numcli, 
           t1.feccali,
           t1.tipmode2 AS tipmode2_del_401, 
           t1.orden AS orden_del_401,
           t2.tipmode2 AS tipmode2_de_lista, 
		   CAST(months_between(t1.feoperac,t1.feccali) AS int) AS month_dif,
           t2.orden AS orden_de_lista
	FROM numeracion t1 
		INNER JOIN numeracion t2
			ON t1.idnumcli = t2.idnumcli
			AND t1.tipmode2 = '00501'
			AND t2.tipmode2 IN ('00321','00322','00502','00503','00504')
),
restaFechas AS (
	SELECT DISTINCT numcli, 
	                CASE WHEN month_dif <= 6 THEN tipmode2_del_401 
					     ELSE tipmode2_de_lista END AS tipmode2,
                	CASE WHEN month_dif <= 6 THEN orden_del_401 
					     ELSE orden_de_lista END AS orden
	FROM primerFiltro 
),
priorizacion AS (
	SELECT ROW_NUMBER() over(partition by numcli order by orden asc) AS ordenNuevo, 
	       cr.numcli, 
		   cr.tipmode2
	FROM restaFechas cr
),
filtroPriorizacion AS (
	SELECT numcli, tipmode2 
	FROM priorizacion 
	WHERE ordennuevo = 1
),
losRestantes AS (
	SELECT n.*
	FROM numeracion n 
		LEFT JOIN filtroPriorizacion fp 
			ON n.idnumcli = fp.numcli
	WHERE fp.numcli IS NULL
),
priorizacionRestantes AS (
	SELECT ROW_NUMBER() over(partition by idnumcli order by orden asc) AS ordenNuevo, 
	       cr.*
	FROM losRestantes cr
),
filtroPriorizacionRestantes AS (
	SELECT feoperac, s1emp,    idnumcli, feccali,  tipmodel, tipmode2, idmodel,  
           '00093' AS tipo,    idpunsco, feccaduc, c1tarpun, c1spid,   c1digcon, 
		   fecultmo, motivfor, idpunsc2, fecrepfi, fecinofc
	FROM priorizacionRestantes 
	WHERE ordennuevo = 1
),
todos AS (
	SELECT c.feoperac, c.s1emp, c.idnumcli, c.feccali, c.tipmodel, c.tipmode2,c.idmodel,  
		   CASE WHEN c.tipo IS NULL THEN '00093' 
		        ELSE c.tipo END AS tipo,
		   c.idpunsco, c.feccaduc, c.c1tarpun, c.c1spid,   c.c1digcon,
		   c.fecultmo, c.motivfor, c.idpunsc2, c.fecrepfi, c.fecinofc
	FROM temporal c 
		INNER JOIN filtroPriorizacion fn
			ON fn.numcli = c.idnumcli 
			AND fn.tipmode2 = c.tipmode2
	UNION ALL
	SELECT c.feoperac, c.s1emp,    c.idnumcli, c.feccali, c.tipmodel, c.tipmode2, c.idmodel,  
           CASE WHEN fn.tipo IS NULL THEN c.tipo 
		        ELSE '00093' END AS tipo,
           c.idpunsco, c.feccaduc, c.c1tarpun, c.c1spid,  c.c1digcon, c.fecultmo, c.motivfor,
           c.idpunsc2, c.fecrepfi, c.fecinofc
	FROM temporal c 
		INNER JOIN filtroPriorizacionRestantes fn
			ON fn.idnumcli = c.idnumcli 
			AND fn.tipmode2 = c.tipmode2
)
INSERT OVERWRITE TABLE temp_jm_cal_in_cl
SELECT c.feoperac, c.s1emp,    c.idnumcli, c.feccali,  c.tipmodel, c.tipmode2, c.idmodel,
       c.tipo,     c.idpunsco, c.feccaduc, c.c1tarpun, c.c1spid,   c.c1digcon, c.fecultmo,
	   c.motivfor, c.idpunsc2, c.fecrepfi, c.fecinofc
FROM temp_jm_cal_in_cl c
	LEFT JOIN todos t
		ON t.idnumcli = c.idnumcli 
		AND t.tipmode2 = c.tipmode2
WHERE t.idnumcli IS NULL 
UNION ALL 
SELECT * 
FROM todos;

--------------------------------------

WITH temporal AS (
	SELECT * 
	FROM temp_jm_cal_in_cl 
	WHERE tipo IS NULL 
		AND tipmode2 in ('00311','00312','00313','00314','00317','00318',
				         '00324','00325','00319','00328','00401')
),
orden AS (
	SELECT '00401' AS calif, 0 AS orden UNION ALL
	SELECT '00328' AS calif, 1 AS orden UNION ALL
	SELECT '00311' AS calif, 2 AS orden UNION ALL
	SELECT '00312' AS calif, 3 AS orden UNION ALL
	SELECT '00313' AS calif, 4 AS orden UNION ALL
	SELECT '00314' AS calif, 5 AS orden UNION ALL
	SELECT '00317' AS calif, 6 AS orden UNION ALL
	SELECT '00318' AS calif, 7 AS orden UNION ALL
	SELECT '00324' AS calif, 8 AS orden UNION ALL
	SELECT '00325' AS calif, 9 AS orden UNION ALL
	SELECT '00319' AS calif,10 AS orden
),
numeracion AS (
	SELECT o.*, t.*
	FROM temporal t 
		LEFT JOIN orden o 
			ON t.tipmode2 = o.calif
),
primerFiltro AS ( --401 vs lista
	SELECT t1.feoperac, 
	       t1.idnumcli AS numcli, 
	       t1.feccali,
	       t1.tipmode2 AS tipmode2_del_401, 
	       t1.orden AS orden_del_401,
	       t2.tipmode2 AS tipmode2_de_lista, 
	       CAST(months_between(t1.feoperac,t1.feccali) AS int) AS month_dif,
	       t2.orden AS orden_de_lista
	FROM numeracion t1 
		INNER JOIN numeracion t2
			ON t1.idnumcli = t2.idnumcli
			AND t1.tipmode2 = '00401'
			AND t2.tipmode2 IN ('00328','00311','00312','00313','00314','00317',
			                    '00318','00324','00325','00319')
),
restaFechas AS (
	SELECT DISTINCT numcli, 
	                CASE WHEN month_dif <= 6 THEN tipmode2_del_401 
					     ELSE tipmode2_de_lista END AS tipmode2,
					CASE WHEN month_dif <= 6 THEN orden_del_401 
					     ELSE orden_de_lista END AS orden
	FROM primerFiltro 
),
priorizacion AS (
	SELECT ROW_NUMBER() over(partition by numcli order by orden asc) AS ordenNuevo, 
	       cr.numcli, 
		   cr.tipmode2
	FROM restaFechas cr
),
filtroPriorizacion AS (
	SELECT numcli, tipmode2 
	FROM priorizacion 
	WHERE ordennuevo = 1
),
losRestantes AS (
	SELECT n.*
	FROM numeracion n 
		LEFT JOIN filtroPriorizacion fp 
			ON n.idnumcli = fp.numcli
	WHERE fp.numcli IS NULL
),
priorizacionRestantes AS (
	SELECT ROW_NUMBER() over(partition by idnumcli order by orden asc) AS ordenNuevo, 
		   cr.*
	FROM losRestantes cr
),
filtroPriorizacionRestantes AS (
	SELECT feoperac, s1emp,    idnumcli, feccali,  tipmodel, tipmode2, idmodel,  
		   '00093' AS tipo,    idpunsco, feccaduc, c1tarpun, c1spid,   c1digcon, 
		   fecultmo, motivfor, idpunsc2, fecrepfi, fecinofc
	FROM priorizacionRestantes 
	WHERE ordennuevo = 1
),
todos AS (
	SELECT c.feoperac, c.s1emp,    c.idnumcli, c.feccali, c.tipmodel, c.tipmode2, c.idmodel,  
		   CASE WHEN c.tipo IS NULL THEN '00093' 
		        ELSE c.tipo END AS tipo,
		   c.idpunsco, c.feccaduc, c.c1tarpun, c.c1spid,  c.c1digcon, c.fecultmo, c.motivfor,
		   c.idpunsc2, c.fecrepfi, c.fecinofc
	FROM temporal c 
		INNER JOIN filtroPriorizacion fn
			ON fn.numcli = c.idnumcli 
			AND fn.tipmode2 = c.tipmode2
	UNION ALL
	SELECT c.feoperac, c.s1emp,    c.idnumcli, c.feccali, c.tipmodel, c.tipmode2, c.idmodel,  
		   CASE WHEN fn.tipo IS NULL THEN c.tipo 
		        ELSE '00093' END AS tipo,
           c.idpunsco, c.feccaduc, c.c1tarpun, c.c1spid,  c.c1digcon, c.fecultmo, c.motivfor,
		   c.idpunsc2, c.fecrepfi, c.fecinofc
	FROM temporal c 
		INNER JOIN filtroPriorizacionRestantes fn
			ON fn.idnumcli = c.idnumcli 
			AND fn.tipmode2 = c.tipmode2
)
INSERT OVERWRITE TABLE temp_jm_cal_in_cl 
SELECT c.feoperac, c.s1emp,    c.idnumcli, c.feccali,  c.tipmodel, c.tipmode2, c.idmodel,
       c.tipo,     c.idpunsco, c.feccaduc, c.c1tarpun, c.c1spid,   c.c1digcon, c.fecultmo,
	   c.motivfor, c.idpunsc2, c.fecrepfi, c.fecinofc
FROM temp_jm_cal_in_cl c
	LEFT JOIN todos t
		ON t.idnumcli = c.idnumcli 
		AND t.tipmode2 = c.tipmode2
WHERE t.idnumcli IS NULL 
UNION ALL 
SELECT * 
FROM todos;

----------------------------------------

WITH temporal AS (
	SELECT * 
	FROM temp_jm_cal_in_cl 
	WHERE tipo IS NULL 
		AND tipmode2 in ('00402','00320')
),
primerFiltro AS ( --402 vs 320
	SELECT t1.idnumcli AS numcli, 
		   t1.tipmode2 AS tipmode2_del_402, 
		   t2.tipmode2 AS tipmode2_del_320, 
		   CAST(months_between(t1.feoperac,t1.feccali) AS int) AS month_dif
	FROM temporal t1 
		INNER JOIN temporal t2
			ON t1.idnumcli = t2.idnumcli
			AND t1.tipmode2 = '00402'
			AND t2.tipmode2 = '00320'
),
restaFechas AS (
	SELECT DISTINCT numcli,
					CASE WHEN month_dif <= 6 THEN tipmode2_del_402
						 ELSE tipmode2_del_320 END AS tipmode2
	FROM primerFiltro 
),
losRestantes AS (
	SELECT c.feoperac, c.s1emp,    c.idnumcli, c.feccali,  c.tipmodel, c.tipmode2, c.idmodel,
	       '00093' AS tipo,        c.idpunsco, c.feccaduc, c.c1tarpun, c.c1spid,   c.c1digcon, 
		   c.fecultmo, c.motivfor, c.idpunsc2, c.fecrepfi, c.fecinofc
	FROM temporal c 
		LEFT JOIN restaFechas fp 
			ON c.idnumcli = fp.numcli
	WHERE fp.numcli is null
),
todos AS ( 
	SELECT c.feoperac, c.s1emp,    c.idnumcli, c.feccali, c.tipmodel, c.tipmode2, c.idmodel,  
           CASE WHEN numcli IS NULL THEN c.tipo 
		        ELSE '00093' END AS tipo,
           c.idpunsco, c.feccaduc, c.c1tarpun, c.c1spid,  c.c1digcon, c.fecultmo, c.motivfor,
           c.idpunsc2, c.fecrepfi, c.fecinofc
	FROM temporal c 
		LEFT JOIN restaFechas fn
			ON fn.numcli = c.idnumcli 
			AND fn.tipmode2 = c.tipmode2
	UNION ALL
	SELECT *
	FROM losRestantes
)
INSERT OVERWRITE TABLE temp_jm_cal_in_cl 
SELECT c.feoperac, c.s1emp,    c.idnumcli, c.feccali,  c.tipmodel, c.tipmode2, c.idmodel,
       c.tipo,     c.idpunsco, c.feccaduc, c.c1tarpun, c.c1spid,   c.c1digcon, c.fecultmo,
	   c.motivfor, c.idpunsc2, c.fecrepfi, c.fecinofc
FROM temp_jm_cal_in_cl c
	LEFT JOIN todos t
		ON t.idnumcli = c.idnumcli 
		AND t.tipmode2 = c.tipmode2
WHERE t.idnumcli IS NULL 
UNION ALL 
SELECT * 
FROM todos;

------------------------------------------

WITH sobrantes AS (
	SELECT cl.E9954_FEOPERAC, cl.E9954_S1EMP,    cl.E9954_IDNUMCLI, cl.E9954_FECCALI,  
	       cl.E9954_TIPMODEL, cl.E9954_TIPMODE2, cl.E9954_IDMODEL,  cl.E9954_TIPO,     
		   cl.E9954_IDPUNSCO, cl.E9954_FECCADUC, cl.E9954_C1TARPUN, cl.E9954_C1SPID,   
		   cl.E9954_C1DIGCON, cl.E0621_FECULTMO, cl.E9954_MOTIVFOR, cl.E9954_IDPUNSC2, 
		   cl.E9954_FECREPFI, cl.E9954_FECINOFC
	FROM bi_corp_bdr.jm_cal_in_cl cl
		LEFT JOIN temp_jm_cal_in_cl f1
			ON cl.E9954_IDNUMCLI = f1.IDNUMCLI 
			AND cl.E9954_FECCALI  = f1.FECCALI
			AND cl.E9954_TIPMODEL = f1.TIPMODEL
			AND cl.E9954_TIPMODE2 = f1.TIPMODE2
			AND cl.E9954_IDMODEL  = f1.IDMODEL
			AND cl.E9954_IDPUNSCO = f1.IDPUNSCO
			AND cl.E9954_FECCADUC = f1.FECCADUC
			AND cl.E9954_C1TARPUN = f1.C1TARPUN
			AND cl.E9954_C1SPID   = f1.C1SPID
			AND cl.E9954_C1DIGCON = f1.C1DIGCON
			AND cl.E9954_MOTIVFOR = f1.MOTIVFOR
			AND cl.E9954_IDPUNSC2 = f1.IDPUNSC2
			AND cl.E9954_FECREPFI = f1.FECREPFI
			AND cl.E9954_FECINOFC = f1.FECINOFC
	WHERE f1.tipo is null 
		AND cl.partition_date = '{{ ti.xcom_pull(task_ids='InputConfig', key='month_partition_bdr', dag_id='BDR_LOAD_Tables-Monthly') }}'
		AND cl.e9954_tipmode2 IN ('00302','00303','00304','00305','00308','00309',
		                          '00323','00326','00311','00312','00313','00314',
								  '00317','00318','00324','00325','00319')
),
multiplicadosParte1 AS (
	SELECT e9954_idnumcli
	FROM sobrantes
	GROUP BY e9954_idnumcli
	HAVING COUNT(*) != 1 
),
multiplicadosParte2 AS (
	SELECT cal.*
	FROM sobrantes cal 
		INNER JOIN multiplicadosParte1 m
			ON cal.e9954_idnumcli = m.e9954_idnumcli
),
simples AS (
	SELECT cal.*
	FROM sobrantes cal 
		LEFT JOIN multiplicadosParte1 m
			ON cal.e9954_idnumcli = m.e9954_idnumcli
	WHERE m.e9954_idnumcli is null
),
numeracion AS (
	SELECT ROW_NUMBER() over(partition by e9954_idnumcli order by e9954_feccali desc) AS orden, 
	       m.*
	FROM multiplicadosParte2 m
),
filtroNumeracion AS (
	SELECT * 
	FROM numeracion 
	WHERE orden = 1
),
fase2 AS (
	SELECT E9954_FEOPERAC, E9954_S1EMP,    E9954_IDNUMCLI, E9954_FECCALI,  
		   E9954_TIPMODEL, E9954_TIPMODE2, E9954_IDMODEL,
		   CASE WHEN E9954_TIPMODE2 = '00302' THEN '00092'
				WHEN E9954_TIPMODE2 = '00303' THEN '00092'
				WHEN E9954_TIPMODE2 = '00304' THEN '00092'
				WHEN E9954_TIPMODE2 = '00305' THEN '00092'
				WHEN E9954_TIPMODE2 = '00308' THEN '00092'
				WHEN E9954_TIPMODE2 = '00309' THEN '00092'
				WHEN E9954_TIPMODE2 = '00323' THEN '00092'
				WHEN E9954_TIPMODE2 = '00326' THEN '00092'
				WHEN E9954_TIPMODE2 = '00311' THEN '00092'
				WHEN E9954_TIPMODE2 = '00312' THEN '00092'
				WHEN E9954_TIPMODE2 = '00313' THEN '00092'
				WHEN E9954_TIPMODE2 = '00314' THEN '00092'
				WHEN E9954_TIPMODE2 = '00317' THEN '00092'
				WHEN E9954_TIPMODE2 = '00318' THEN '00092'
				WHEN E9954_TIPMODE2 = '00324' THEN '00092'
				WHEN E9954_TIPMODE2 = '00325' THEN '00092'
				WHEN E9954_TIPMODE2 = '00319' THEN '00092'
				ELSE null END AS E9954_TIPO,
		   E9954_IDPUNSCO, E9954_FECCADUC, E9954_C1TARPUN, E9954_C1SPID,   E9954_C1DIGCON, 
           E0621_FECULTMO, E9954_MOTIVFOR, E9954_IDPUNSC2, E9954_FECREPFI, E9954_FECINOFC
	FROM filtroNumeracion
),
todos AS (
	SELECT E9954_FEOPERAC, E9954_S1EMP,    E9954_IDNUMCLI, E9954_FECCALI,  E9954_TIPMODEL,
	       E9954_TIPMODE2, E9954_IDMODEL,  E9954_TIPO,     E9954_IDPUNSCO, E9954_FECCADUC, 
		   E9954_C1TARPUN, E9954_C1SPID,   E9954_C1DIGCON, E0621_FECULTMO, E9954_MOTIVFOR, 
		   E9954_IDPUNSC2, E9954_FECREPFI, E9954_FECINOFC
	FROM fase2
	UNION ALL
	SELECT E9954_FEOPERAC, E9954_S1EMP,    E9954_IDNUMCLI, E9954_FECCALI,  E9954_TIPMODEL,
	       E9954_TIPMODE2, E9954_IDMODEL,  '00092' AS E9954_TIPO,          E9954_IDPUNSCO, 
		   E9954_FECCADUC, E9954_C1TARPUN, E9954_C1SPID,   E9954_C1DIGCON, E0621_FECULTMO, 
		   E9954_MOTIVFOR, E9954_IDPUNSC2, E9954_FECREPFI, E9954_FECINOFC
	FROM simples
)
INSERT OVERWRITE TABLE temp_jm_cal_in_cl
SELECT c.feoperac, c.s1emp,    c.idnumcli, c.feccali,  c.tipmodel, c.tipmode2, c.idmodel,
       c.tipo,     c.idpunsco, c.feccaduc, c.c1tarpun, c.c1spid,   c.c1digcon, c.fecultmo,
	   c.motivfor, c.idpunsc2, c.fecrepfi, c.fecinofc
FROM temp_jm_cal_in_cl c
	LEFT JOIN todos t
		ON t.e9954_idnumcli = c.idnumcli 
		AND t.e9954_tipmode2 = c.tipmode2
WHERE t.e9954_idnumcli IS NULL  
UNION ALL 
SELECT E9954_FEOPERAC AS feoperac, E9954_S1EMP AS s1emp,       E9954_IDNUMCLI AS idnumcli,
       E9954_FECCALI AS feccali,   E9954_TIPMODEL AS tipmodel, E9954_TIPMODE2 AS tipmode2,
       E9954_IDMODEL AS idmodel,   E9954_TIPO AS tipo,         E9954_IDPUNSCO AS idpunsco, 
       E9954_FECCADUC AS feccaduc, E9954_C1TARPUN AS c1tarpun, E9954_C1SPID AS c1spid,   
       E9954_C1DIGCON AS c1digcon, E0621_FECULTMO AS fecultmo, E9954_MOTIVFOR AS motivfor, 
       E9954_IDPUNSC2 AS idpunsc2, E9954_FECREPFI AS fecrepfi, E9954_FECINOFC AS fecinofc
FROM todos;

----------------------------------------------

with sobrantes AS (
	SELECT cl.E9954_FEOPERAC, cl.E9954_S1EMP,    cl.E9954_IDNUMCLI, cl.E9954_FECCALI,  cl.E9954_TIPMODEL,
    	   cl.E9954_TIPMODE2, cl.E9954_IDMODEL,  cl.E9954_TIPMODE2 AS E9954_TIPO,      cl.E9954_IDPUNSCO, 
	   	   cl.E9954_FECCADUC, cl.E9954_C1TARPUN, cl.E9954_C1SPID,   cl.E9954_C1DIGCON, cl.E0621_FECULTMO, 
	   	   cl.E9954_MOTIVFOR, cl.E9954_IDPUNSC2, cl.E9954_FECREPFI, cl.E9954_FECINOFC
	FROM bi_corp_bdr.jm_cal_in_cl cl
		LEFT JOIN temp_jm_cal_in_cl f1
			ON cl.E9954_IDNUMCLI = f1.IDNUMCLI 
			AND cl.E9954_FECCALI  = f1.FECCALI
			AND cl.E9954_TIPMODEL = f1.TIPMODEL
			AND cl.E9954_TIPMODE2 = f1.TIPMODE2
			AND cl.E9954_IDMODEL  = f1.IDMODEL
			AND cl.E9954_IDPUNSCO = f1.IDPUNSCO
			AND cl.E9954_FECCADUC = f1.FECCADUC
			AND cl.E9954_C1TARPUN = f1.C1TARPUN
			AND cl.E9954_C1SPID   = f1.C1SPID
			AND cl.E9954_C1DIGCON = f1.C1DIGCON
			AND cl.E9954_MOTIVFOR = f1.MOTIVFOR
			AND cl.E9954_IDPUNSC2 = f1.IDPUNSC2
			AND cl.E9954_FECREPFI = f1.FECREPFI
			AND cl.E9954_FECINOFC = f1.FECINOFC
		WHERE f1.tipo is null
			AND cl.partition_date = '{{ ti.xcom_pull(task_ids='InputConfig', key='month_partition_bdr', dag_id='BDR_LOAD_Tables-Monthly') }}'
),
multiplicadosParte1 AS (
	SELECT e9954_idnumcli,E9954_TIPMODE2,e9954_tipo
	FROM sobrantes
	GROUP BY e9954_idnumcli,E9954_TIPMODE2,e9954_tipo  
	HAVING COUNT(*) != 1 
),
multiplicadosParte2 AS (
	SELECT cal.*
	FROM sobrantes cal 
		INNER JOIN multiplicadosParte1 m
			ON cal.e9954_idnumcli = m.e9954_idnumcli
			AND cal.E9954_TIPMODE2 = m.E9954_TIPMODE2
			AND cal.e9954_tipo = m.e9954_tipo
),
simples AS (
	SELECT cal.*
	FROM sobrantes cal 
		LEFT JOIN multiplicadosParte1 m
			ON cal.e9954_idnumcli = m.e9954_idnumcli
			AND cal.E9954_TIPMODE2 = m.E9954_TIPMODE2
			AND cal.e9954_tipo = m.e9954_tipo
	WHERE m.e9954_idnumcli is null
),
numeracion AS (
	SELECT ROW_NUMBER() over(partition by e9954_idnumcli,e9954_tipmode2,e9954_tipo order by e9954_feccali desc) AS orden, 
	       m.*
	FROM multiplicadosParte2 m
),
filtroNumeracion AS (
	SELECT * 
	FROM numeracion 
	WHERE orden = 1
)
INSERT OVERWRITE TABLE bi_corp_bdr.jm_cal_in_cl 
PARTITION(partition_date= '{{ ti.xcom_pull(task_ids='InputConfig', key='month_partition_bdr', dag_id='BDR_LOAD_Tables-Monthly') }}')
SELECT E9954_FEOPERAC, E9954_S1EMP,    E9954_IDNUMCLI, E9954_FECCALI,  E9954_TIPMODEL,
       E9954_TIPMODE2, E9954_IDMODEL,  E9954_TIPO,     E9954_IDPUNSCO, E9954_FECCADUC, 
	   E9954_C1TARPUN, E9954_C1SPID,   E9954_C1DIGCON, E0621_FECULTMO, E9954_MOTIVFOR, 
	   E9954_IDPUNSC2, E9954_FECREPFI, E9954_FECINOFC
FROM filtroNumeracion
UNION ALL
SELECT E9954_FEOPERAC, E9954_S1EMP,    E9954_IDNUMCLI, E9954_FECCALI,  E9954_TIPMODEL,
       E9954_TIPMODE2, E9954_IDMODEL,  E9954_TIPO,     E9954_IDPUNSCO, E9954_FECCADUC, 
	   E9954_C1TARPUN, E9954_C1SPID,   E9954_C1DIGCON, E0621_FECULTMO, E9954_MOTIVFOR, 
	   E9954_IDPUNSC2, E9954_FECREPFI, E9954_FECINOFC
FROM simples
UNION ALL 
SELECT feoperac AS E9954_FEOPERAC, s1emp AS E9954_S1EMP,       idnumcli AS E9954_IDNUMCLI, 
       feccali AS E9954_FECCALI,   tipmodel AS E9954_TIPMODEL, tipmode2 AS E9954_TIPMODE2, 
	   idmodel AS E9954_IDMODEL,   tipo AS E9954_TIPO,         idpunsco AS E9954_IDPUNSCO, 
	   feccaduc AS E9954_FECCADUC, c1tarpun AS E9954_C1TARPUN, c1spid AS E9954_C1SPID,   
       c1digcon AS E9954_C1DIGCON, fecultmo AS E0621_FECULTMO, motivfor AS E9954_MOTIVFOR, 
	   idpunsc2 AS E9954_IDPUNSC2, fecrepfi AS E9954_FECREPFI, fecinofc AS E9954_FECINOFC
FROM temp_jm_cal_in_cl
WHERE tipo in ('00093','00092');