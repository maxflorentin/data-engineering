set hive.merge.mapfiles=true;
set hive.merge.mapredfiles=true;
set hive.merge.size.per.task=4000000;
set hive.merge.smallfiles.avgsize=16000000;
set hive.exec.dynamic.partition.mode=nonstrict;
--set mapred.job.queue.name=root.dataeng;
SET spark.yarn.queue=root.dataeng;
set hive.execution.engine=spark;

--Incidencia: desaparecian calificaciones en la tabla de calificación cliente. 
--Causa de la incidencia: esto se debe a un procedimiento de Valida en el cual 
--                        cuando hay dos calificaciones para un cliente, se 
--                        eliminan ambas calificaciones. 
--Solucion: se definio la siguiente logica para que por cada cliente solo haya
--          una y solo una calificacion por mes.

drop table temp_jm_cal_in_cl;

CREATE TEMPORARY TABLE temp_jm_cal_in_cl (
feoperac string, s1emp    string, idnumcli string, feccali  string,
tipmodel string, tipmode2 string, idmodel  string, tipo     string,
idpunsco string, feccaduc string, c1tarpun string, c1spid   string,
c1digcon string, fecultmo string, motivfor string, idpunsc2 string,
fecrepfi string, fecinofc string
);

with multiplicadosParte1 as (
SELECT e9954_idnumcli
FROM bi_corp_bdr.test_jm_cal_in_cl
where partition_date = '{{ ti.xcom_pull(task_ids='InputConfig', key='month_partition_bdr', dag_id='BDR_LOAD_CAL_IN_CL') }}'
group by e9954_idnumcli
having COUNT(*) != 1 
),
multiplicadosParte2 as (
select cal.e9954_feoperac, cal.e9954_s1emp,       cal.e9954_idnumcli,
cal.e9954_feccali,  cal.e9954_tipmodel,    cal.e9954_tipmode2,
cal.e9954_idmodel,  cal.e9954_tipo,        cal.e9954_idpunsco,
cal.e9954_feccaduc, cal.e9954_c1tarpun,    cal.e9954_c1spid,
cal.e9954_c1digcon, cal.e0621_fecultmo,    cal.e9954_motivfor,
cal.e9954_idpunsc2, cal.e9954_fecrepfi,    cal.e9954_fecinofc
FROM bi_corp_bdr.test_jm_cal_in_cl cal inner join multiplicadosParte1 m
on cal.e9954_idnumcli = m.e9954_idnumcli
and cal.partition_date = '{{ ti.xcom_pull(task_ids='InputConfig', key='month_partition_bdr', dag_id='BDR_LOAD_CAL_IN_CL') }}'
where partition_date = '{{ ti.xcom_pull(task_ids='InputConfig', key='month_partition_bdr', dag_id='BDR_LOAD_CAL_IN_CL') }}'
),
simples as (
select cal.e9954_feoperac, cal.e9954_s1emp,       cal.e9954_idnumcli,
cal.e9954_feccali,  cal.e9954_tipmodel,    cal.e9954_tipmode2,
cal.e9954_idmodel,  '00093' as e9954_tipo, cal.e9954_idpunsco,
cal.e9954_feccaduc, cal.e9954_c1tarpun,    cal.e9954_c1spid,
cal.e9954_c1digcon, cal.e0621_fecultmo,    cal.e9954_motivfor,
cal.e9954_idpunsc2, cal.e9954_fecrepfi,    cal.e9954_fecinofc
FROM bi_corp_bdr.test_jm_cal_in_cl cal left join multiplicadosParte1 m
on cal.e9954_idnumcli = m.e9954_idnumcli
and cal.partition_date = '{{ ti.xcom_pull(task_ids='InputConfig', key='month_partition_bdr', dag_id='BDR_LOAD_CAL_IN_CL') }}'
where partition_date = '{{ ti.xcom_pull(task_ids='InputConfig', key='month_partition_bdr', dag_id='BDR_LOAD_CAL_IN_CL') }}'
and m.e9954_idnumcli is null
),
filtroInicialParte1 as (
SELECT *
FROM multiplicadosParte2
where e9954_tipmode2 not in ('00000', '00301', '00306', '00307',
'00310', '00315', '00316', '99999')
),
filtroInicialParte2 as (
select *
from filtroInicialParte1
where (e9954_tipmode2 != '00324' and cast(e9954_idpunsco as bigint) >= 10000000) --me quedo con los >= a '1' para los tipmode != 324. Pongo 10.000.000 ya que el numero posee 7 decimales
or (e9954_tipmode2 = '00324' and cast(e9954_idpunsco as bigint) > 1000000000) --me quedo con los > a '100' para los tipmode = 324. Pongo 1.000.000.000 ya que el numero posee 7 decimales 
),
--primerFiltro as ( --324 vs 319
--select t1.e9954_feoperac, t1.e9954_s1emp,        t1.e9954_idnumcli,
--t1.e9954_feccali,  t1.e9954_tipmodel,     t1.e9954_tipmode2,
--t1.e9954_idmodel,  '00093' as e9954_tipo, t1.e9954_idpunsco,
--t1.e9954_feccaduc, t1.e9954_c1tarpun,     t1.e9954_c1spid,
--t1.e9954_c1digcon, t1.e0621_fecultmo,     t1.e9954_motivfor,
--t1.e9954_idpunsc2, t1.e9954_fecrepfi,     t1.e9954_fecinofc
--from filtroInicialParte2 t1 inner join filtroInicialParte2 t2
--on t1.e9954_idnumcli = t2.e9954_idnumcli
--and t1.e9954_tipmode2 = '00319'
--and t2.e9954_tipmode2 = '00324'
--),
segundoFiltroParte1 as ( --101 vs 103
select ROW_NUMBER() over(partition by e9954_idnumcli order by e9954_feccali desc) as orden, t1.*
from filtroInicialParte2 t1	
where t1.e9954_tipmode2 in ('00101','00103')
),
segundoFiltroParte2 as ( --101 vs 103
select e9954_feoperac, e9954_s1emp,           e9954_idnumcli,
e9954_feccali,  e9954_tipmodel,        e9954_tipmode2,
e9954_idmodel,  '00093' as e9954_tipo, e9954_idpunsco,
e9954_feccaduc, e9954_c1tarpun,        e9954_c1spid,
e9954_c1digcon, e0621_fecultmo,        e9954_motivfor,
e9954_idpunsc2, e9954_fecrepfi,        e9954_fecinofc
from segundoFiltroParte1
where orden = 1
),
clientesRestantes as (
select fi.*
from filtroInicialParte2 fi --left join primerFiltro pf
--on fi.e9954_idnumcli = pf.e9954_idnumcli
left join segundoFiltroParte2 sf
on fi.e9954_idnumcli = sf.e9954_idnumcli
where --pf.e9954_idnumcli is null and 
sf.e9954_idnumcli is null
),
losMultiplicadosDeLosRestantes as (
select e9954_idnumcli
from clientesRestantes
group by e9954_idnumcli
having count(*) != 1
),
losMultiplicadosDeLosRestantesCompleto as (
select cr.*
from clientesRestantes cr 
inner join losMultiplicadosDeLosRestantes h
on cr.e9954_idnumcli = h.e9954_idnumcli
),
losSimplesDeLosRestantes as (
select cr.e9954_feoperac, cr.e9954_s1emp,        cr.e9954_idnumcli,
cr.e9954_feccali,  cr.e9954_tipmodel,     cr.e9954_tipmode2,
cr.e9954_idmodel,  '00093' as e9954_tipo, cr.e9954_idpunsco,
cr.e9954_feccaduc, cr.e9954_c1tarpun,     cr.e9954_c1spid,
cr.e9954_c1digcon, cr.e0621_fecultmo,     cr.e9954_motivfor,
cr.e9954_idpunsc2, cr.e9954_fecrepfi,     cr.e9954_fecinofc
from clientesRestantes cr 
left join losMultiplicadosDeLosRestantes h
on cr.e9954_idnumcli = h.e9954_idnumcli
where h.e9954_idnumcli is null
)
insert overwrite table temp_jm_cal_in_cl 
--select * from primerFiltro                           union all
select * from segundoFiltroParte2                    union all
select * from losMultiplicadosDeLosRestantesCompleto union all
select * from losSimplesDeLosRestantes               union all
select * from simples;

--------------------------------------

WITH temporal AS (
SELECT * FROM temp_jm_cal_in_cl
WHERE tipo IS NULL AND tipmode2 in ('00302','00303','00304','00305','00308', '00309', '00323', '00326', '00327', '00404') 
--SELECT * FROM bi_corp_bdr.test_jm_cal_in_cl WHERE partition_date = '2031-04'
--AND e9954_tipo IS NULL AND e9954_tipmode2 in ('00302','00303','00304','00305','00308', '00309', '00323', '00326', '00327', '00404')
),
orden as (
select '00404' as calif, 0 as orden union all
select '00327' as calif, 1 as orden union all
select '00326' as calif, 2 as orden union all
select '00323' as calif, 3 as orden union all
select '00309' as calif, 4 as orden union all
select '00308' as calif, 5 as orden union all
select '00305' as calif, 6 as orden union all
select '00304' as calif, 7 as orden union all
select '00303' as calif, 8 as orden union all
select '00302' as calif, 9 as orden
),
numeracion as (
select o.*, t.*
from temporal t left join orden o 
on t.tipmode2 = o.calif
--on t.e9954_tipmode2 = o.calif
),
primerFiltro as ( --404 vs lista
select t1.feoperac, 
t1.idnumcli AS numcli, 
t1.feccali,
t1.tipmode2 AS tipmode2_del_401, 
t1.orden AS orden_del_401,
t2.tipmode2 AS tipmode2_de_lista, CAST(months_between(t1.feoperac,t1.feccali) AS int) AS month_dif,
t2.orden AS orden_de_lista
from numeracion t1 inner join numeracion t2
on t1.idnumcli = t2.idnumcli
and t1.tipmode2 = '00404'
and t2.tipmode2 in ('00302','00303','00304','00305','00308', '00309', '00323', '00326', '00327')
--select t1.e9954_feoperac, 
--t1.e9954_idnumcli AS numcli, 
--t1.e9954_feccali,
--t1.e9954_tipmode2 AS tipmode2_del_401, 
--t1.orden AS orden_del_401,
--t2.e9954_tipmode2 AS tipmode2_de_lista, CAST(months_between(t1.e9954_feoperac,t1.e9954_feccali) AS int) AS month_dif,
--t2.orden AS orden_de_lista
--from numeracion t1 inner join numeracion t2
--on t1.e9954_idnumcli = t2.e9954_idnumcli
--and t1.e9954_tipmode2 = '00404'
--and t2.e9954_tipmode2 in ('00302','00303','00304','00305','00308', '00309', '00323', '00326', '00327')
),
restaFechas AS (
SELECT DISTINCT numcli, CASE WHEN month_dif <= 6 THEN tipmode2_del_401 ELSE tipmode2_de_lista END AS tipmode2,
CASE WHEN month_dif <= 6 THEN orden_del_401 ELSE orden_de_lista END AS orden
FROM primerFiltro 
),
priorizacion as (
select ROW_NUMBER() over(partition by numcli order by orden asc) as ordenNuevo, cr.numcli, cr.tipmode2
from restaFechas cr
),
filtroPriorizacion as (
select numcli, tipmode2 from priorizacion where ordennuevo = 1
),
losRestantes as (
select n.*
from numeracion n 
left join filtroPriorizacion fp 
on n.idnumcli = fp.numcli
--on n.e9954_idnumcli = fp.numcli
where fp.numcli is null
),
priorizacionRestantes as (
select ROW_NUMBER() over(partition by idnumcli order by orden asc) as ordenNuevo, cr.*
--select ROW_NUMBER() over(partition by e9954_idnumcli order by orden asc) as ordenNuevo, cr.*
from losRestantes cr
),
filtroPriorizacionRestantes as (
select feoperac, s1emp, idnumcli,
feccali, tipmodel, tipmode2,idmodel,  
'00093' AS tipo,
idpunsco,feccaduc, c1tarpun,  c1spid,
c1digcon, fecultmo,     motivfor,
idpunsc2, fecrepfi,     fecinofc
--select e9954_feoperac, e9954_s1emp, e9954_idnumcli,
--e9954_feccali, e9954_tipmodel, e9954_tipmode2,e9954_idmodel,  
--'00093' AS e9954_tipo,
--e9954_idpunsco,e9954_feccaduc, e9954_c1tarpun,     e9954_c1spid,
--e9954_c1digcon, e0621_fecultmo,     e9954_motivfor,
--e9954_idpunsc2, e9954_fecrepfi,     e9954_fecinofc
from priorizacionRestantes where ordennuevo = 1
),
todos AS (
SELECT c.feoperac, c.s1emp, c.idnumcli,
c.feccali, c.tipmodel, c.tipmode2,c.idmodel,  
CASE WHEN c.tipo IS NULL THEN c.tipo ELSE '00093' END AS tipo,
c.idpunsco,c.feccaduc,c.c1tarpun,c.c1spid,c.c1digcon,
c.fecultmo,c.motivfor,c.idpunsc2,c.fecrepfi,c.fecinofc
FROM temporal c 
inner JOIN filtroPriorizacion fn
ON fn.numcli = c.idnumcli 
AND fn.tipmode2 = c.tipmode2
--SELECT c.e9954_feoperac, c.e9954_s1emp, c.e9954_idnumcli,
--c.e9954_feccali, c.e9954_tipmodel, c.e9954_tipmode2,c.e9954_idmodel,  
--CASE WHEN c.e9954_tipo IS NULL THEN c.e9954_tipo ELSE '00093' END AS e9954_tipo,
--c.e9954_idpunsco,c.e9954_feccaduc,c.e9954_c1tarpun,c.e9954_c1spid,c.e9954_c1digcon,
--c.e0621_fecultmo,c.e9954_motivfor,c.e9954_idpunsc2,c.e9954_fecrepfi,c.e9954_fecinofc
--FROM temporal c 
--inner JOIN filtroPriorizacion fn
--ON fn.numcli = c.e9954_idnumcli 
--AND fn.tipmode2 = c.e9954_tipmode2
UNION ALL
SELECT
c.feoperac, c.s1emp, c.idnumcli,
c.feccali, c.tipmodel, c.tipmode2,c.idmodel,  
CASE WHEN fn.tipo IS NULL THEN c.tipo ELSE '00093' END AS tipo,
c.idpunsco,c.feccaduc, c.c1tarpun,     c.c1spid,
c.c1digcon, c.fecultmo,     c.motivfor,
c.idpunsc2, c.fecrepfi,     c.fecinofc
FROM temporal c 
inner JOIN filtroPriorizacionRestantes fn
ON fn.idnumcli = c.idnumcli 
AND fn.tipmode2 = c.tipmode2
--SELECT 
--c.e9954_feoperac, c.e9954_s1emp, c.e9954_idnumcli,
--c.e9954_feccali, c.e9954_tipmodel, c.e9954_tipmode2,c.e9954_idmodel,  
--CASE WHEN fn.e9954_tipo IS NULL THEN c.e9954_tipo ELSE '00093' END AS e9954_tipo,
--c.e9954_idpunsco,c.e9954_feccaduc, c.e9954_c1tarpun,     c.e9954_c1spid,
--c.e9954_c1digcon, c.e0621_fecultmo,     c.e9954_motivfor,
--c.e9954_idpunsc2, c.e9954_fecrepfi,     c.e9954_fecinofc
--FROM temporal c 
--inner JOIN filtroPriorizacionRestantes fn
--ON fn.e9954_idnumcli = c.e9954_idnumcli 
--AND fn.e9954_tipmode2 = c.e9954_tipmode2
)
insert overwrite table temp_jm_cal_in_cl
--insert overwrite table bi_corp_bdr.test_jm_cal_in_cl 
--partition(partition_date= '2031-05')
SELECT c.feoperac, c.s1emp, c.idnumcli,
c.feccali, c.tipmodel, c.tipmode2,c.idmodel,c.tipo,
c.idpunsco,c.feccaduc, c.c1tarpun,     c.c1spid,
c.c1digcon, c.fecultmo,     c.motivfor,
c.idpunsc2, c.fecrepfi,     c.fecinofc
FROM temp_jm_cal_in_cl c
LEFT JOIN todos t
ON t.idnumcli = c.idnumcli 
AND t.tipmode2 = c.tipmode2
WHERE t.idnumcli IS NULL 
--SELECT c.e9954_feoperac, c.e9954_s1emp, c.e9954_idnumcli,
--c.e9954_feccali, c.e9954_tipmodel, c.e9954_tipmode2,c.e9954_idmodel,c.e9954_tipo,
--c.e9954_idpunsco,c.e9954_feccaduc, c.e9954_c1tarpun,     c.e9954_c1spid,
--c.e9954_c1digcon, c.e0621_fecultmo,     c.e9954_motivfor,
--c.e9954_idpunsc2, c.e9954_fecrepfi,     c.e9954_fecinofc
--FROM bi_corp_bdr.test_jm_cal_in_cl c
--LEFT JOIN todos t
--ON t.e9954_idnumcli = c.e9954_idnumcli 
--AND t.e9954_tipmode2 = c.e9954_tipmode2
--WHERE partition_date = '2031-04'
--AND t.e9954_idnumcli IS NULL 
UNION ALL 
SELECT * 
FROM todos;

--------------------------------------

WITH temporal AS (
SELECT * FROM temp_jm_cal_in_cl 
WHERE tipo IS NULL AND tipmode2 in ('00321','00322','00502','00503','00504','00501')
--SELECT * FROM bi_corp_bdr.test_jm_cal_in_cl WHERE partition_date = '2031-05'
--AND e9954_tipo IS NULL AND e9954_tipmode2 in ('00321','00322','00502','00503','00504','00501')
),
orden as (
select '00501' as calif, 0 as orden union all
select '00322' as calif, 1 as orden union all
select '00321' as calif, 2 as orden union all
select '00502' as calif, 3 as orden union all
select '00503' as calif, 4 as orden union all
select '00504' as calif, 5 as orden
),
numeracion as (
select o.*, t.*
from temporal t left join orden o 
on t.tipmode2 = o.calif
--on t.e9954_tipmode2 = o.calif
),
primerFiltro as ( --501 vs lista
select t1.feoperac, 
t1.idnumcli AS numcli, 
t1.feccali,
t1.tipmode2 AS tipmode2_del_401, 
t1.orden AS orden_del_401,
t2.tipmode2 AS tipmode2_de_lista, CAST(months_between(t1.feoperac,t1.feccali) AS int) AS month_dif,
t2.orden AS orden_de_lista
from numeracion t1 inner join numeracion t2
on t1.idnumcli = t2.idnumcli
and t1.tipmode2 = '00501'
and t2.tipmode2 in ('00321','00322','00502','00503','00504')
--select t1.e9954_feoperac, 
--t1.e9954_idnumcli AS numcli, 
--t1.e9954_feccali,
--t1.e9954_tipmode2 AS tipmode2_del_401, 
--t1.orden AS orden_del_401,
--t2.e9954_tipmode2 AS tipmode2_de_lista, CAST(months_between(t1.e9954_feoperac,t1.e9954_feccali) AS int) AS month_dif,
--t2.orden AS orden_de_lista
--from numeracion t1 inner join numeracion t2
--on t1.e9954_idnumcli = t2.e9954_idnumcli
--and t1.e9954_tipmode2 = '00501'
--and t2.e9954_tipmode2 in ('00321','00322','00502','00503','00504')
),
restaFechas AS (
SELECT DISTINCT numcli, CASE WHEN month_dif <= 6 THEN tipmode2_del_401 ELSE tipmode2_de_lista END AS tipmode2,
CASE WHEN month_dif <= 6 THEN orden_del_401 ELSE orden_de_lista END AS orden
FROM primerFiltro 
),
priorizacion as (
select ROW_NUMBER() over(partition by numcli order by orden asc) as ordenNuevo, cr.numcli, cr.tipmode2
from restaFechas cr
),
filtroPriorizacion as (
select numcli, tipmode2 from priorizacion where ordennuevo = 1
),
losRestantes as (
select n.*
from numeracion n 
left join filtroPriorizacion fp 
on n.idnumcli = fp.numcli
--on n.e9954_idnumcli = fp.numcli
where fp.numcli is null
),
priorizacionRestantes as (
select ROW_NUMBER() over(partition by idnumcli order by orden asc) as ordenNuevo, cr.*
--select ROW_NUMBER() over(partition by e9954_idnumcli order by orden asc) as ordenNuevo, cr.*
from losRestantes cr
),
filtroPriorizacionRestantes as (
select feoperac, s1emp, idnumcli,
feccali, tipmodel, tipmode2,idmodel,  
'00093' AS tipo,
idpunsco,feccaduc, c1tarpun,     c1spid,
c1digcon, fecultmo,     motivfor,
idpunsc2, fecrepfi,     fecinofc
--select e9954_feoperac, e9954_s1emp, e9954_idnumcli,
--e9954_feccali, e9954_tipmodel, e9954_tipmode2,e9954_idmodel,  
--'00093' AS e9954_tipo,
--e9954_idpunsco,e9954_feccaduc, e9954_c1tarpun,     e9954_c1spid,
--e9954_c1digcon, e0621_fecultmo,     e9954_motivfor,
--e9954_idpunsc2, e9954_fecrepfi,     e9954_fecinofc
from priorizacionRestantes where ordennuevo = 1
),
todos AS (
SELECT c.feoperac, c.s1emp, c.idnumcli,
c.feccali, c.tipmodel, c.tipmode2,c.idmodel,  
CASE WHEN c.tipo IS NULL THEN '00093' ELSE c.tipo END AS tipo,
c.idpunsco,c.feccaduc,c.c1tarpun,c.c1spid,c.c1digcon,
c.fecultmo,c.motivfor,c.idpunsc2,c.fecrepfi,c.fecinofc
FROM temporal c 
inner JOIN filtroPriorizacion fn
ON fn.numcli = c.idnumcli 
AND fn.tipmode2 = c.tipmode2
--SELECT c.e9954_feoperac, c.e9954_s1emp, c.e9954_idnumcli,
--c.e9954_feccali, c.e9954_tipmodel, c.e9954_tipmode2,c.e9954_idmodel,  
--CASE WHEN c.e9954_tipo IS NULL THEN '00093' ELSE c.e9954_tipo END AS e9954_tipo,
--c.e9954_idpunsco,c.e9954_feccaduc,c.e9954_c1tarpun,c.e9954_c1spid,c.e9954_c1digcon,
--c.e0621_fecultmo,c.e9954_motivfor,c.e9954_idpunsc2,c.e9954_fecrepfi,c.e9954_fecinofc
--FROM temporal c 
--inner JOIN filtroPriorizacion fn
--ON fn.numcli = c.e9954_idnumcli 
--AND fn.tipmode2 = c.e9954_tipmode2
UNION ALL
SELECT 
c.feoperac, c.s1emp, c.idnumcli,
c.feccali, c.tipmodel, c.tipmode2,c.idmodel,  
CASE WHEN fn.tipo IS NULL THEN c.tipo ELSE '00093' END AS tipo,
c.idpunsco,c.feccaduc, c.c1tarpun,     c.c1spid,
c.c1digcon, c.fecultmo,     c.motivfor,
c.idpunsc2, c.fecrepfi,     c.fecinofc
FROM temporal c 
inner JOIN filtroPriorizacionRestantes fn
ON fn.idnumcli = c.idnumcli 
AND fn.tipmode2 = c.tipmode2
--SELECT 
--c.e9954_feoperac, c.e9954_s1emp, c.e9954_idnumcli,
--c.e9954_feccali, c.e9954_tipmodel, c.e9954_tipmode2,c.e9954_idmodel,  
--CASE WHEN fn.e9954_tipo IS NULL THEN c.e9954_tipo ELSE '00093' END AS e9954_tipo,
--c.e9954_idpunsco,c.e9954_feccaduc, c.e9954_c1tarpun,     c.e9954_c1spid,
--c.e9954_c1digcon, c.e0621_fecultmo,     c.e9954_motivfor,
--c.e9954_idpunsc2, c.e9954_fecrepfi,     c.e9954_fecinofc
--FROM temporal c 
--inner JOIN filtroPriorizacionRestantes fn
--ON fn.e9954_idnumcli = c.e9954_idnumcli 
--AND fn.e9954_tipmode2 = c.e9954_tipmode2
)
insert overwrite table temp_jm_cal_in_cl
--insert overwrite table bi_corp_bdr.test_jm_cal_in_cl 
--partition(partition_date= '2031-06')
SELECT c.feoperac, c.s1emp, c.idnumcli,
c.feccali, c.tipmodel, c.tipmode2,c.idmodel,c.tipo,
c.idpunsco,c.feccaduc, c.c1tarpun,     c.c1spid,
c.c1digcon, c.fecultmo,     c.motivfor,
c.idpunsc2, c.fecrepfi,     c.fecinofc
FROM temp_jm_cal_in_cl c
LEFT JOIN todos t
ON t.idnumcli = c.idnumcli 
AND t.tipmode2 = c.tipmode2
WHERE t.idnumcli IS NULL 
--SELECT c.e9954_feoperac, c.e9954_s1emp, c.e9954_idnumcli,
--c.e9954_feccali, c.e9954_tipmodel, c.e9954_tipmode2,c.e9954_idmodel,c.e9954_tipo,
--c.e9954_idpunsco,c.e9954_feccaduc, c.e9954_c1tarpun,     c.e9954_c1spid,
--c.e9954_c1digcon, c.e0621_fecultmo,     c.e9954_motivfor,
--c.e9954_idpunsc2, c.e9954_fecrepfi,     c.e9954_fecinofc
--FROM bi_corp_bdr.test_jm_cal_in_cl c
--LEFT JOIN todos t
--ON t.e9954_idnumcli = c.e9954_idnumcli 
--AND t.e9954_tipmode2 = c.e9954_tipmode2
--WHERE partition_date = '2031-05'
--AND t.e9954_idnumcli IS NULL 
UNION ALL 
SELECT * 
FROM todos;

--------------------------------------

WITH temporal AS (
SELECT * FROM temp_jm_cal_in_cl WHERE tipo IS NULL 
AND tipmode2 in ('00311','00312','00313','00314','00317','00318','00324',
'00325','00319','00328','00401')
--SELECT * FROM bi_corp_bdr.test_jm_cal_in_cl WHERE partition_date = '2031-06'
--AND e9954_tipo IS NULL AND e9954_tipmode2 in ('00311','00312','00313','00314','00317','00318','00324',
--'00325','00319','00328','00401')
),
orden as (
select '00401' as calif, 0 as orden union all
select '00328' as calif, 1 as orden union all
select '00311' as calif, 2 as orden union all
select '00312' as calif, 3 as orden union all
select '00313' as calif, 4 as orden union all
select '00314' as calif, 5 as orden union all
select '00317' as calif, 6 as orden union all
select '00318' as calif, 7 as orden union all
select '00324' as calif, 8 as orden union all
select '00325' as calif, 9 as orden union all
select '00319' as calif,10 as orden
),
numeracion as (
select o.*, t.*
from temporal t left join orden o 
on t.tipmode2 = o.calif
--on t.e9954_tipmode2 = o.calif
),
primerFiltro as ( --401 vs lista
select t1.feoperac, 
t1.idnumcli AS numcli, 
t1.feccali,
t1.tipmode2 AS tipmode2_del_401, 
t1.orden AS orden_del_401,
t2.tipmode2 AS tipmode2_de_lista, CAST(months_between(t1.feoperac,t1.feccali) AS int) AS month_dif,
t2.orden AS orden_de_lista
from numeracion t1 inner join numeracion t2
on t1.idnumcli = t2.idnumcli
and t1.tipmode2 = '00401'
and t2.tipmode2 in ('00328','00311','00312','00313','00314','00317','00318','00324','00325','00319')
--select t1.e9954_feoperac, 
--t1.e9954_idnumcli AS numcli, 
--t1.e9954_feccali,
--t1.e9954_tipmode2 AS tipmode2_del_401, 
--t1.orden AS orden_del_401,
--t2.e9954_tipmode2 AS tipmode2_de_lista, CAST(months_between(t1.e9954_feoperac,t1.e9954_feccali) AS int) AS month_dif,
--t2.orden AS orden_de_lista
--from numeracion t1 inner join numeracion t2
--on t1.e9954_idnumcli = t2.e9954_idnumcli
--and t1.e9954_tipmode2 = '00401'
--and t2.e9954_tipmode2 in ('00328','00311','00312','00313','00314','00317','00318','00324','00325','00319')
),
restaFechas AS (
SELECT DISTINCT numcli, CASE WHEN month_dif <= 6 THEN tipmode2_del_401 ELSE tipmode2_de_lista END AS tipmode2,
CASE WHEN month_dif <= 6 THEN orden_del_401 ELSE orden_de_lista END AS orden
FROM primerFiltro 
),
priorizacion as (
select ROW_NUMBER() over(partition by numcli order by orden asc) as ordenNuevo, cr.numcli, cr.tipmode2
from restaFechas cr
),
filtroPriorizacion as (
select numcli, tipmode2 from priorizacion where ordennuevo = 1
),
losRestantes as (
select n.*
from numeracion n 
left join filtroPriorizacion fp 
on n.idnumcli = fp.numcli
--on n.e9954_idnumcli = fp.numcli
where fp.numcli is null
),
priorizacionRestantes as (
select ROW_NUMBER() over(partition by idnumcli order by orden asc) as ordenNuevo, cr.*
--select ROW_NUMBER() over(partition by e9954_idnumcli order by orden asc) as ordenNuevo, cr.*
from losRestantes cr
),
filtroPriorizacionRestantes as (
select feoperac, s1emp, idnumcli,
feccali, tipmodel, tipmode2,idmodel,  
'00093' AS tipo,
idpunsco,feccaduc, c1tarpun,     c1spid,
c1digcon, fecultmo,     motivfor,
idpunsc2, fecrepfi,     fecinofc
--select e9954_feoperac, e9954_s1emp, e9954_idnumcli,
--e9954_feccali, e9954_tipmodel, e9954_tipmode2,e9954_idmodel,  
--'00093' AS e9954_tipo,
--e9954_idpunsco,e9954_feccaduc, e9954_c1tarpun,     e9954_c1spid,
--e9954_c1digcon, e0621_fecultmo,     e9954_motivfor,
--e9954_idpunsc2, e9954_fecrepfi,     e9954_fecinofc
from priorizacionRestantes where ordennuevo = 1
),
todos AS (
SELECT c.feoperac, c.s1emp, c.idnumcli,
c.feccali, c.tipmodel, c.tipmode2,c.idmodel,  
CASE WHEN c.tipo IS NULL THEN '00093' ELSE c.tipo END AS tipo,
c.idpunsco,c.feccaduc,c.c1tarpun,c.c1spid,c.c1digcon,
c.fecultmo,c.motivfor,c.idpunsc2,c.fecrepfi,c.fecinofc
FROM temporal c 
inner JOIN filtroPriorizacion fn
ON fn.numcli = c.idnumcli 
AND fn.tipmode2 = c.tipmode2
--SELECT c.e9954_feoperac, c.e9954_s1emp, c.e9954_idnumcli,
--c.e9954_feccali, c.e9954_tipmodel, c.e9954_tipmode2,c.e9954_idmodel,  
--CASE WHEN c.e9954_tipo IS NULL THEN '00093' ELSE c.e9954_tipo END AS e9954_tipo,
--c.e9954_idpunsco,c.e9954_feccaduc,c.e9954_c1tarpun,c.e9954_c1spid,c.e9954_c1digcon,
--c.e0621_fecultmo,c.e9954_motivfor,c.e9954_idpunsc2,c.e9954_fecrepfi,c.e9954_fecinofc
--FROM temporal c 
--inner JOIN filtroPriorizacion fn
--ON fn.numcli = c.e9954_idnumcli 
--AND fn.tipmode2 = c.e9954_tipmode2
UNION ALL
SELECT 
c.feoperac, c.s1emp, c.idnumcli,
c.feccali, c.tipmodel, c.tipmode2,c.idmodel,  
CASE WHEN fn.tipo IS NULL THEN c.tipo ELSE '00093' END AS tipo,
c.idpunsco,c.feccaduc, c.c1tarpun,     c.c1spid,
c.c1digcon, c.fecultmo,     c.motivfor,
c.idpunsc2, c.fecrepfi,     c.fecinofc
FROM temporal c 
inner JOIN filtroPriorizacionRestantes fn
ON fn.idnumcli = c.idnumcli 
AND fn.tipmode2 = c.tipmode2
--SELECT 
--c.e9954_feoperac, c.e9954_s1emp, c.e9954_idnumcli,
--c.e9954_feccali, c.e9954_tipmodel, c.e9954_tipmode2,c.e9954_idmodel,  
--CASE WHEN fn.e9954_tipo IS NULL THEN c.e9954_tipo ELSE '00093' END AS e9954_tipo,
--c.e9954_idpunsco,c.e9954_feccaduc, c.e9954_c1tarpun,     c.e9954_c1spid,
--c.e9954_c1digcon, c.e0621_fecultmo,     c.e9954_motivfor,
--c.e9954_idpunsc2, c.e9954_fecrepfi,     c.e9954_fecinofc
--FROM temporal c 
--inner JOIN filtroPriorizacionRestantes fn
--ON fn.e9954_idnumcli = c.e9954_idnumcli 
--AND fn.e9954_tipmode2 = c.e9954_tipmode2
)
insert overwrite table temp_jm_cal_in_cl 
--insert overwrite table bi_corp_bdr.test_jm_cal_in_cl 
--partition(partition_date= '2031-07')
SELECT c.feoperac, c.s1emp, c.idnumcli,
c.feccali, c.tipmodel, c.tipmode2,c.idmodel,c.tipo,
c.idpunsco,c.feccaduc, c.c1tarpun,     c.c1spid,
c.c1digcon, c.fecultmo,     c.motivfor,
c.idpunsc2, c.fecrepfi,     c.fecinofc
FROM temp_jm_cal_in_cl c
LEFT JOIN todos t
ON t.idnumcli = c.idnumcli 
AND t.tipmode2 = c.tipmode2
WHERE t.idnumcli IS NULL 
--SELECT c.e9954_feoperac, c.e9954_s1emp, c.e9954_idnumcli,
--c.e9954_feccali, c.e9954_tipmodel, c.e9954_tipmode2,c.e9954_idmodel,c.e9954_tipo,
--c.e9954_idpunsco,c.e9954_feccaduc, c.e9954_c1tarpun,     c.e9954_c1spid,
--c.e9954_c1digcon, c.e0621_fecultmo,     c.e9954_motivfor,
--c.e9954_idpunsc2, c.e9954_fecrepfi,     c.e9954_fecinofc
--FROM bi_corp_bdr.test_jm_cal_in_cl c
--LEFT JOIN todos t
--ON t.e9954_idnumcli = c.e9954_idnumcli 
--AND t.e9954_tipmode2 = c.e9954_tipmode2
--WHERE partition_date = '2031-06'
--AND t.e9954_idnumcli IS NULL 
UNION ALL 
SELECT * 
FROM todos;

----------------------------------------

WITH temporal AS (
SELECT * FROM temp_jm_cal_in_cl WHERE tipo IS NULL AND tipmode2 in ('00402','00320')
--SELECT * 
--FROM bi_corp_bdr.test_jm_cal_in_cl WHERE partition_date = '2031-07'
--and e9954_tipo IS NULL AND e9954_tipmode2 in ('00402','00320')
),
primerFiltro as ( --402 vs 320
select t1.idnumcli AS numcli, 
t1.tipmode2 AS tipmode2_del_402, 
t2.tipmode2 AS tipmode2_del_320, 
CAST(months_between(t1.feoperac,t1.feccali) AS int) AS month_dif
from temporal t1 inner join temporal t2
on t1.idnumcli = t2.idnumcli
and t1.tipmode2 = '00402'
and t2.tipmode2 = '00320'
--select t1.e9954_idnumcli AS numcli, 
--t1.e9954_tipmode2 AS tipmode2_del_402, 
--t2.e9954_tipmode2 AS tipmode2_del_320, 
--CAST(months_between(t1.e9954_feoperac,t1.e9954_feccali) AS int) AS month_dif
--from temporal t1 inner join temporal t2
--on t1.e9954_idnumcli = t2.e9954_idnumcli
--and t1.e9954_tipmode2 = '00402'
--and t2.e9954_tipmode2 = '00320'
),
restaFechas AS (
SELECT DISTINCT 
numcli,
CASE WHEN month_dif <= 6
THEN tipmode2_del_402
ELSE tipmode2_del_320
END AS tipmode2
FROM primerFiltro 
),
losRestantes as (
select c.feoperac, c.s1emp, c.idnumcli,
c.feccali, c.tipmodel,c.tipmode2,c.idmodel,'00093' AS tipo,
c.idpunsco,c.feccaduc,c.c1tarpun,c.c1spid,
c.c1digcon, c.fecultmo,     c.motivfor,
c.idpunsc2, c.fecrepfi,     c.fecinofc
from temporal c 
left join restaFechas fp 
on c.idnumcli = fp.numcli
where fp.numcli is null
--select c.e9954_feoperac, c.e9954_s1emp, c.e9954_idnumcli,
--c.e9954_feccali, c.e9954_tipmodel,c.e9954_tipmode2,c.e9954_idmodel,'00093' AS e9954_tipo,
--c.e9954_idpunsco,c.e9954_feccaduc,c.e9954_c1tarpun,c.e9954_c1spid,
--c.e9954_c1digcon, c.e0621_fecultmo,     c.e9954_motivfor,
--c.e9954_idpunsc2, c.e9954_fecrepfi,     c.e9954_fecinofc
--from temporal c 
--left join restaFechas fp 
--on c.e9954_idnumcli = fp.numcli
--where fp.numcli is null
),
todos AS ( 
SELECT c.feoperac, c.s1emp, c.idnumcli,
c.feccali, c.tipmodel, c.tipmode2,
c.idmodel,  
CASE WHEN numcli IS NULL THEN c.tipo ELSE '00093' END AS tipo,
c.idpunsco,
c.feccaduc, c.c1tarpun,     c.c1spid,
c.c1digcon, c.fecultmo,     c.motivfor,
c.idpunsc2, c.fecrepfi,     c.fecinofc
FROM temporal c LEFT JOIN restaFechas fn
ON fn.numcli = c.idnumcli 
AND fn.tipmode2 = c.tipmode2
--SELECT c.e9954_feoperac, c.e9954_s1emp, c.e9954_idnumcli,
--c.e9954_feccali, c.e9954_tipmodel, c.e9954_tipmode2,
--c.e9954_idmodel,  
--CASE WHEN numcli IS NULL THEN c.e9954_tipo ELSE '00093' END AS e9954_tipo,
--c.e9954_idpunsco,
--c.e9954_feccaduc, c.e9954_c1tarpun,     c.e9954_c1spid,
--c.e9954_c1digcon, c.e0621_fecultmo,     c.e9954_motivfor,
--c.e9954_idpunsc2, c.e9954_fecrepfi,     c.e9954_fecinofc
--FROM temporal c LEFT JOIN restaFechas fn
--ON fn.numcli = c.e9954_idnumcli 
--AND fn.tipmode2 = c.e9954_tipmode2
union all
select *
from losRestantes
)
insert overwrite table temp_jm_cal_in_cl 
--insert overwrite table bi_corp_bdr.test_jm_cal_in_cl 
--partition(partition_date= '2031-08')
SELECT c.feoperac, c.s1emp, c.idnumcli,
c.feccali, c.tipmodel, c.tipmode2,c.idmodel,c.tipo,
c.idpunsco,c.feccaduc, c.c1tarpun,     c.c1spid,
c.c1digcon, c.fecultmo,     c.motivfor,
c.idpunsc2, c.fecrepfi,     c.fecinofc
FROM temp_jm_cal_in_cl c
LEFT JOIN todos t
ON t.idnumcli = c.idnumcli 
AND t.tipmode2 = c.tipmode2
WHERE t.idnumcli IS NULL 
--SELECT c.e9954_feoperac, c.e9954_s1emp, c.e9954_idnumcli,
--c.e9954_feccali, c.e9954_tipmodel, c.e9954_tipmode2,c.e9954_idmodel,c.e9954_tipo,
--c.e9954_idpunsco,c.e9954_feccaduc, c.e9954_c1tarpun,     c.e9954_c1spid,
--c.e9954_c1digcon, c.e0621_fecultmo,     c.e9954_motivfor,
--c.e9954_idpunsc2, c.e9954_fecrepfi,     c.e9954_fecinofc
--FROM bi_corp_bdr.test_jm_cal_in_cl c
--LEFT JOIN todos t
--ON t.e9954_idnumcli = c.e9954_idnumcli 
--AND t.e9954_tipmode2 = c.e9954_tipmode2
--WHERE partition_date = '2031-07'
--AND t.e9954_idnumcli IS NULL 
UNION ALL 
SELECT * 
FROM todos;

------------------------------------------

with sobrantes as (
select cl.E9954_FEOPERAC, cl.E9954_S1EMP,    cl.E9954_IDNUMCLI, cl.E9954_FECCALI,  cl.E9954_TIPMODEL,
cl.E9954_TIPMODE2, cl.E9954_IDMODEL,  cl.E9954_TIPO,     cl.E9954_IDPUNSCO, cl.E9954_FECCADUC,
cl.E9954_C1TARPUN, cl.E9954_C1SPID,   cl.E9954_C1DIGCON, cl.E0621_FECULTMO, cl.E9954_MOTIVFOR,
cl.E9954_IDPUNSC2, cl.E9954_FECREPFI, cl.E9954_FECINOFC
from bi_corp_bdr.test_jm_cal_in_cl cl
left join temp_jm_cal_in_cl f1
on cl.E9954_IDNUMCLI = f1.IDNUMCLI 
and cl.E9954_FECCALI  = f1.FECCALI
and cl.E9954_TIPMODEL = f1.TIPMODEL
and cl.E9954_TIPMODE2 = f1.TIPMODE2
and cl.E9954_IDMODEL  = f1.IDMODEL
and cl.E9954_IDPUNSCO = f1.IDPUNSCO
and cl.E9954_FECCADUC = f1.FECCADUC
and cl.E9954_C1TARPUN = f1.C1TARPUN
and cl.E9954_C1SPID   = f1.C1SPID
and cl.E9954_C1DIGCON = f1.C1DIGCON
and cl.E9954_MOTIVFOR = f1.MOTIVFOR
and cl.E9954_IDPUNSC2 = f1.IDPUNSC2
and cl.E9954_FECREPFI = f1.FECREPFI
and cl.E9954_FECINOFC = f1.FECINOFC
--left join bi_corp_bdr.test_jm_cal_in_cl f1
--on cl.E9954_IDNUMCLI = f1.E9954_IDNUMCLI 
--and cl.E9954_FECCALI  = f1.E9954_FECCALI
--and cl.E9954_TIPMODEL = f1.E9954_TIPMODEL
--and cl.E9954_TIPMODE2 = f1.E9954_TIPMODE2
--and cl.E9954_IDMODEL  = f1.E9954_IDMODEL
--and cl.E9954_IDPUNSCO = f1.E9954_IDPUNSCO
--and cl.E9954_FECCADUC = f1.E9954_FECCADUC
--and cl.E9954_C1TARPUN = f1.E9954_C1TARPUN
--and cl.E9954_C1SPID   = f1.E9954_C1SPID
--and cl.E9954_C1DIGCON = f1.E9954_C1DIGCON
--and cl.E9954_MOTIVFOR = f1.E9954_MOTIVFOR
--and cl.E9954_IDPUNSC2 = f1.E9954_IDPUNSC2
--and cl.E9954_FECREPFI = f1.E9954_FECREPFI
--and cl.E9954_FECINOFC = f1.E9954_FECINOFC
where f1.tipo is null 
--where f1.e9954_tipo is null 
and cl.partition_date = '{{ ti.xcom_pull(task_ids='InputConfig', key='month_partition_bdr', dag_id='BDR_LOAD_CAL_IN_CL') }}'
--and f1.partition_date = '2031-08'
and cl.e9954_tipmode2 IN ('00302','00303','00304','00305',
'00308','00309','00323','00326','00311','00312',
'00313','00314','00317','00318','00324','00325','00319')
),
multiplicadosParte1 as (
SELECT e9954_idnumcli
FROM sobrantes
group by e9954_idnumcli
having COUNT(*) != 1 
),
multiplicadosParte2 as (
select cal.*
FROM sobrantes cal inner join multiplicadosParte1 m
on cal.e9954_idnumcli = m.e9954_idnumcli
),
simples as (
select cal.*
FROM sobrantes cal left join multiplicadosParte1 m
on cal.e9954_idnumcli = m.e9954_idnumcli
where m.e9954_idnumcli is null
),
numeracion as (
select ROW_NUMBER() over(partition by e9954_idnumcli order by e9954_feccali desc) as orden, m.*
from multiplicadosParte2 m
),
filtroNumeracion as (
select * from numeracion where orden = 1
),
fase2 as (
select E9954_FEOPERAC, E9954_S1EMP,    E9954_IDNUMCLI, E9954_FECCALI,  
E9954_TIPMODEL, E9954_TIPMODE2, E9954_IDMODEL,
case when E9954_TIPMODE2 = '00302' then '00092'
when E9954_TIPMODE2 = '00303' then '00092'
when E9954_TIPMODE2 = '00304' then '00092'
when E9954_TIPMODE2 = '00305' then '00092'
when E9954_TIPMODE2 = '00308' then '00092'
when E9954_TIPMODE2 = '00309' then '00092'
when E9954_TIPMODE2 = '00323' then '00092'
when E9954_TIPMODE2 = '00326' then '00092'
when E9954_TIPMODE2 = '00311' then '00092'
when E9954_TIPMODE2 = '00312' then '00092'
when E9954_TIPMODE2 = '00313' then '00092'
when E9954_TIPMODE2 = '00314' then '00092'
when E9954_TIPMODE2 = '00317' then '00092'
when E9954_TIPMODE2 = '00318' then '00092'
when E9954_TIPMODE2 = '00324' then '00092'
when E9954_TIPMODE2 = '00325' then '00092'
when E9954_TIPMODE2 = '00319' then '00092'
else null end as E9954_TIPO,
E9954_IDPUNSCO, E9954_FECCADUC, E9954_C1TARPUN, E9954_C1SPID,   E9954_C1DIGCON, 
E0621_FECULTMO, E9954_MOTIVFOR, E9954_IDPUNSC2, E9954_FECREPFI, E9954_FECINOFC
from filtroNumeracion
),
todos as (
SELECT E9954_FEOPERAC,E9954_S1EMP,E9954_IDNUMCLI,E9954_FECCALI,E9954_TIPMODEL,E9954_TIPMODE2,E9954_IDMODEL,
E9954_TIPO,E9954_IDPUNSCO, E9954_FECCADUC, E9954_C1TARPUN, E9954_C1SPID,   E9954_C1DIGCON, 
E0621_FECULTMO, E9954_MOTIVFOR, E9954_IDPUNSC2, E9954_FECREPFI, E9954_FECINOFC
FROM fase2
UNION ALL
SELECT E9954_FEOPERAC,E9954_S1EMP,E9954_IDNUMCLI,E9954_FECCALI,E9954_TIPMODEL,E9954_TIPMODE2,E9954_IDMODEL,
'00092' as E9954_TIPO,E9954_IDPUNSCO, E9954_FECCADUC, E9954_C1TARPUN, E9954_C1SPID,   E9954_C1DIGCON, 
E0621_FECULTMO, E9954_MOTIVFOR, E9954_IDPUNSC2, E9954_FECREPFI, E9954_FECINOFC
FROM simples
)
insert overwrite table temp_jm_cal_in_cl
--insert overwrite table bi_corp_bdr.test_jm_cal_in_cl 
--partition(partition_date= '2031-10')
SELECT c.feoperac, c.s1emp, c.idnumcli,
c.feccali, c.tipmodel, c.tipmode2,c.idmodel,c.tipo,
c.idpunsco,c.feccaduc, c.c1tarpun,     c.c1spid,
c.c1digcon, c.fecultmo,     c.motivfor,
c.idpunsc2, c.fecrepfi,     c.fecinofc
--SELECT c.e9954_feoperac, c.e9954_s1emp, c.e9954_idnumcli,
--c.e9954_feccali, c.e9954_tipmodel, c.e9954_tipmode2,c.e9954_idmodel,c.e9954_tipo,
--c.e9954_idpunsco,c.e9954_feccaduc, c.e9954_c1tarpun,     c.e9954_c1spid,
--c.e9954_c1digcon, c.e0621_fecultmo,     c.e9954_motivfor,
--c.e9954_idpunsc2, c.e9954_fecrepfi,     c.e9954_fecinofc
FROM temp_jm_cal_in_cl c
--FROM bi_corp_bdr.test_jm_cal_in_cl c
LEFT JOIN todos t
ON t.e9954_idnumcli = c.idnumcli 
AND t.e9954_tipmode2 = c.tipmode2
WHERE t.e9954_idnumcli IS NULL 
--ON t.e9954_idnumcli = c.e9954_idnumcli 
--AND t.e9954_tipmode2 = c.e9954_tipmode2
--WHERE partition_date = '2031-08'
--AND t.e9954_idnumcli IS NULL 
UNION ALL 
SELECT E9954_FEOPERAC as feoperac, E9954_S1EMP as s1emp, E9954_IDNUMCLI as idnumcli,
E9954_FECCALI as feccali, E9954_TIPMODEL as tipmodel, E9954_TIPMODE2 as tipmode2,
E9954_IDMODEL as idmodel, E9954_TIPO as tipo, E9954_IDPUNSCO as idpunsco, 
E9954_FECCADUC as feccaduc, E9954_C1TARPUN as c1tarpun, E9954_C1SPID as c1spid,   
E9954_C1DIGCON as c1digcon, E0621_FECULTMO as fecultmo, E9954_MOTIVFOR as motivfor, 
E9954_IDPUNSC2 as idpunsc2, E9954_FECREPFI as fecrepfi, E9954_FECINOFC as fecinofc
FROM todos;

----------------------------------------------

with sobrantes as (
select cl.E9954_FEOPERAC, cl.E9954_S1EMP,    cl.E9954_IDNUMCLI, cl.E9954_FECCALI,  cl.E9954_TIPMODEL,
cl.E9954_TIPMODE2, cl.E9954_IDMODEL,  
cl.E9954_TIPMODE2 as E9954_TIPO,     
cl.E9954_IDPUNSCO, cl.E9954_FECCADUC,
cl.E9954_C1TARPUN, cl.E9954_C1SPID,   cl.E9954_C1DIGCON, cl.E0621_FECULTMO, cl.E9954_MOTIVFOR,
cl.E9954_IDPUNSC2, cl.E9954_FECREPFI, cl.E9954_FECINOFC
from bi_corp_bdr.test_jm_cal_in_cl cl
left join temp_jm_cal_in_cl f1
on cl.E9954_IDNUMCLI = f1.IDNUMCLI 
and cl.E9954_FECCALI  = f1.FECCALI
and cl.E9954_TIPMODEL = f1.TIPMODEL
and cl.E9954_TIPMODE2 = f1.TIPMODE2
and cl.E9954_IDMODEL  = f1.IDMODEL
and cl.E9954_IDPUNSCO = f1.IDPUNSCO
and cl.E9954_FECCADUC = f1.FECCADUC
and cl.E9954_C1TARPUN = f1.C1TARPUN
and cl.E9954_C1SPID   = f1.C1SPID
and cl.E9954_C1DIGCON = f1.C1DIGCON
and cl.E9954_MOTIVFOR = f1.MOTIVFOR
and cl.E9954_IDPUNSC2 = f1.IDPUNSC2
and cl.E9954_FECREPFI = f1.FECREPFI
and cl.E9954_FECINOFC = f1.FECINOFC
where f1.tipo is null
and cl.partition_date = '{{ ti.xcom_pull(task_ids='InputConfig', key='month_partition_bdr', dag_id='BDR_LOAD_CAL_IN_CL') }}'
),
multiplicadosParte1 as (
SELECT e9954_idnumcli,E9954_TIPMODE2,e9954_tipo
FROM sobrantes
group by e9954_idnumcli,E9954_TIPMODE2,e9954_tipo  
having COUNT(*) != 1 
),
multiplicadosParte2 as (
select cal.*
FROM sobrantes cal inner join multiplicadosParte1 m
on cal.e9954_idnumcli = m.e9954_idnumcli
and cal.E9954_TIPMODE2 = m.E9954_TIPMODE2
and cal.e9954_tipo = m.e9954_tipo
),
simples as (
select cal.*
FROM sobrantes cal left join multiplicadosParte1 m
on cal.e9954_idnumcli = m.e9954_idnumcli
and cal.E9954_TIPMODE2 = m.E9954_TIPMODE2
and cal.e9954_tipo = m.e9954_tipo
where m.e9954_idnumcli is null
),
numeracion as (
select ROW_NUMBER() over(partition by e9954_idnumcli,e9954_tipmode2,e9954_tipo order by e9954_feccali desc) as orden, m.*
from multiplicadosParte2 m
),
filtroNumeracion as (
select * from numeracion where orden = 1
)
insert overwrite table bi_corp_bdr.test_jm_cal_in_cl 
partition(partition_date= '2031-13') --'{{ ti.xcom_pull(task_ids='InputConfig', key='month_partition_bdr', dag_id='BDR_LOAD_CAL_IN_CL') }}')
SELECT E9954_FEOPERAC,E9954_S1EMP,E9954_IDNUMCLI,E9954_FECCALI,E9954_TIPMODEL,E9954_TIPMODE2,E9954_IDMODEL,
E9954_TIPO,E9954_IDPUNSCO, E9954_FECCADUC, E9954_C1TARPUN, E9954_C1SPID,   E9954_C1DIGCON, 
E0621_FECULTMO, E9954_MOTIVFOR, E9954_IDPUNSC2, E9954_FECREPFI, E9954_FECINOFC
FROM filtroNumeracion
UNION ALL
SELECT E9954_FEOPERAC,E9954_S1EMP,E9954_IDNUMCLI,E9954_FECCALI,E9954_TIPMODEL,E9954_TIPMODE2,E9954_IDMODEL,
E9954_TIPO,E9954_IDPUNSCO, E9954_FECCADUC, E9954_C1TARPUN, E9954_C1SPID,   E9954_C1DIGCON, 
E0621_FECULTMO, E9954_MOTIVFOR, E9954_IDPUNSC2, E9954_FECREPFI, E9954_FECINOFC
FROM simples
UNION ALL 
SELECT feoperac AS E9954_FEOPERAC, s1emp AS E9954_S1EMP, idnumcli AS E9954_IDNUMCLI, feccali AS E9954_FECCALI,
tipmodel AS E9954_TIPMODEL, tipmode2 AS E9954_TIPMODE2, idmodel AS E9954_IDMODEL, tipo AS E9954_TIPO,
idpunsco AS E9954_IDPUNSCO, feccaduc AS E9954_FECCADUC, c1tarpun AS E9954_C1TARPUN, c1spid AS E9954_C1SPID,   
c1digcon AS E9954_C1DIGCON, fecultmo AS E0621_FECULTMO, motivfor AS E9954_MOTIVFOR, idpunsc2 AS E9954_IDPUNSC2, 
fecrepfi AS E9954_FECREPFI, fecinofc AS E9954_FECINOFC
FROM temp_jm_cal_in_cl
WHERE tipo in ('00093','00092');