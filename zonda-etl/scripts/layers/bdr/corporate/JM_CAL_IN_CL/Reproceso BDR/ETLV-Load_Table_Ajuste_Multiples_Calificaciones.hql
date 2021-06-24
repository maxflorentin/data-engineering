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

with multiplicadosParte1 as (
	SELECT e9954_idnumcli
	FROM bi_corp_bdr.jm_cal_in_cl
	where partition_date = '{{ ti.xcom_pull(task_ids='InputConfig', key='month_partition_bdr', dag_id='BDR_LOAD_Tables-Reproceso_BDR_Monthly_201912_202103') }}'
	group by e9954_idnumcli
	having COUNT(*) != 1 
),
multiplicadosParte2 as (
	select cal.e9954_feoperac, cal.e9954_s1emp,    cal.e9954_idnumcli,
           cal.e9954_feccali,  cal.e9954_tipmodel, cal.e9954_tipmode2,
           cal.e9954_idmodel,  cal.e9954_tipo,     cal.e9954_idpunsco,
           cal.e9954_feccaduc, cal.e9954_c1tarpun, cal.e9954_c1spid,
           cal.e9954_c1digcon, cal.e0621_fecultmo, cal.e9954_motivfor,
           cal.e9954_idpunsc2, cal.e9954_fecrepfi, cal.e9954_fecinofc
	FROM bi_corp_bdr.jm_cal_in_cl cal inner join multiplicadosParte1 m
		on cal.e9954_idnumcli = m.e9954_idnumcli
		and cal.partition_date = '{{ ti.xcom_pull(task_ids='InputConfig', key='month_partition_bdr', dag_id='BDR_LOAD_Tables-Reproceso_BDR_Monthly_201912_202103') }}'
	where partition_date = '{{ ti.xcom_pull(task_ids='InputConfig', key='month_partition_bdr', dag_id='BDR_LOAD_Tables-Reproceso_BDR_Monthly_201912_202103') }}'
),
simples as (
	select cal.e9954_feoperac, cal.e9954_s1emp,    cal.e9954_idnumcli,
           cal.e9954_feccali,  cal.e9954_tipmodel, cal.e9954_tipmode2,
           cal.e9954_idmodel,  cal.e9954_tipo,     cal.e9954_idpunsco,
           cal.e9954_feccaduc, cal.e9954_c1tarpun, cal.e9954_c1spid,
           cal.e9954_c1digcon, cal.e0621_fecultmo, cal.e9954_motivfor,
           cal.e9954_idpunsc2, cal.e9954_fecrepfi, cal.e9954_fecinofc
	FROM bi_corp_bdr.jm_cal_in_cl cal left join multiplicadosParte1 m
		on cal.e9954_idnumcli = m.e9954_idnumcli
		and cal.partition_date = '{{ ti.xcom_pull(task_ids='InputConfig', key='month_partition_bdr', dag_id='BDR_LOAD_Tables-Reproceso_BDR_Monthly_201912_202103') }}'
	where partition_date = '{{ ti.xcom_pull(task_ids='InputConfig', key='month_partition_bdr', dag_id='BDR_LOAD_Tables-Reproceso_BDR_Monthly_201912_202103') }}'
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
    --where e9954_idpunsco != '00000000000'
	where cast(e9954_idpunsco as bigint) >= 10000000 --me quedo con los >= a '1'. Pongo 10.000.000 ya que el numero posee 7 decimales
),
primerFiltro as ( --324 vs 319
	select t1.e9954_feoperac, t1.e9954_s1emp,    t1.e9954_idnumcli,
           t1.e9954_feccali,  t1.e9954_tipmodel, t1.e9954_tipmode2,
           t1.e9954_idmodel,  t1.e9954_tipo,     t1.e9954_idpunsco,
           t1.e9954_feccaduc, t1.e9954_c1tarpun, t1.e9954_c1spid,
           t1.e9954_c1digcon, t1.e0621_fecultmo, t1.e9954_motivfor,
           t1.e9954_idpunsc2, t1.e9954_fecrepfi, t1.e9954_fecinofc
	from filtroInicialParte2 t1 inner join filtroInicialParte2 t2
		on t1.e9954_idnumcli = t2.e9954_idnumcli
		and t1.e9954_tipmode2 = '00319'
		and t2.e9954_tipmode2 = '00324'
),
segundoFiltroParte1 as ( --101 vs 103
	select ROW_NUMBER() over(partition by e9954_idnumcli order by e9954_feccali desc) as orden, t1.*
	from filtroInicialParte2 t1	
	where t1.e9954_tipmode2 in ('00101','00103')
),
segundoFiltroParte2 as ( --101 vs 103
	select e9954_feoperac, e9954_s1emp,    e9954_idnumcli,
           e9954_feccali,  e9954_tipmodel, e9954_tipmode2,
           e9954_idmodel,  e9954_tipo,     e9954_idpunsco,
           e9954_feccaduc, e9954_c1tarpun, e9954_c1spid,
           e9954_c1digcon, e0621_fecultmo, e9954_motivfor,
           e9954_idpunsc2, e9954_fecrepfi, e9954_fecinofc
	from segundoFiltroParte1
	where orden = 1
),
clientesRestantes as (
	select fi.*
	from filtroInicialParte2 fi left join primerFiltro pf
		on fi.e9954_idnumcli = pf.e9954_idnumcli
		left join segundoFiltroParte2 sf
		on fi.e9954_idnumcli = sf.e9954_idnumcli
	where pf.e9954_idnumcli is null 
		and sf.e9954_idnumcli is null
),
orden as (
	select '00327' as calif, 1 as orden union all
    select '00328' as calif, 2 as orden union all
    select '00311' as calif, 3 as orden union all
	select '00312' as calif, 4 as orden union all
	select '00313' as calif, 5 as orden union all
	select '00314' as calif, 6 as orden union all
	select '00317' as calif, 7 as orden union all
	select '00318' as calif, 8 as orden union all
	select '00323' as calif, 9 as orden union all
	select '00324' as calif,10 as orden union all	
	select '00326' as calif,11 as orden union all	
	select '00319' as calif,12 as orden union all	
	select '00320' as calif,13 as orden union all	
	select '00321' as calif,14 as orden union all	
	select '00322' as calif,15 as orden union all	
	select '00102' as calif,16 as orden union all	
	select '00101' as calif,17 as orden union all	
	select '00103' as calif,18 as orden
),
numeracion as (
	select ROW_NUMBER() over(partition by e9954_idnumcli order by orden asc) as ordenNuevo,o.*, cr.*
	from clientesRestantes cr left join orden o 
		on cr.e9954_tipmode2 = o.calif
),
filtroNumeracion as (
	select e9954_feoperac, e9954_s1emp,    e9954_idnumcli,
           e9954_feccali,  e9954_tipmodel, e9954_tipmode2,
           e9954_idmodel,  e9954_tipo,     e9954_idpunsco,
           e9954_feccaduc, e9954_c1tarpun, e9954_c1spid,
           e9954_c1digcon, e0621_fecultmo, e9954_motivfor,
           e9954_idpunsc2, e9954_fecrepfi, e9954_fecinofc
	from numeracion 
	where ordennuevo = 1
)
insert overwrite table bi_corp_bdr.jm_cal_in_cl 
partition(partition_date= '{{ ti.xcom_pull(task_ids='InputConfig', key='month_partition_bdr', dag_id='BDR_LOAD_Tables-Reproceso_BDR_Monthly_201912_202103') }}')
select *
from filtroNumeracion 
union all
select *
from primerFiltro 
union all
select *
from segundoFiltroParte2
union all
select *
from simples;
