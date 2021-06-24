set hive.merge.mapfiles=true;
set hive.merge.mapredfiles=true;
set hive.merge.size.per.task=4000000;
set hive.merge.smallfiles.avgsize=16000000;
set hive.exec.dynamic.partition.mode=nonstrict;
set mapred.job.queue.name=root.dataeng;


-- MORIA
with moria_contratos as (
select r.cod_entidadg, r.cod_centrog, r.num_contratg, r.cod_productg, trim(r.cod_subprodg) as cod_subprodg, r.cod_entidad, r.cod_centro, r.num_contrato, r.cod_producto, trim(r.cod_subprodu) as cod_subprodu, r.fec_altareg, r.cod_divisa, r.fec_baja, c.imp_deuda,
row_number() over( partition by r.cod_entidadg, r.cod_centrog, r.num_contratg, r.cod_productg, r.cod_subprodg, r.cod_entidad, r.cod_centro, r.num_contrato, r.cod_producto, r.cod_subprodu order by r.cod_divisa ) as rownum,
sum(c.imp_deuda) over( partition by r.cod_entidadg, r.cod_centrog, r.num_contratg, r.cod_productg, r.cod_subprodg, r.cod_entidad, r.cod_centro, r.num_contrato, r.cod_producto, r.cod_subprodu) as sum_imp_deuda
from bi_corp_staging.mddtccn r
LEFT JOIN santander_business_risk_arda.contratos c on c.cod_entidad = r.cod_entidad and c.cod_centro = r.cod_centro and c.num_cuenta = r.num_contrato and c.cod_producto = r.cod_producto and r.cod_subprodu = c.cod_subprodu_altair and c.cod_divisa = r.cod_divisa
where r.partition_date = '2019-12-19' -- valor fijo para procesar hasta DICIEMBRE 19
and c.data_date_part = '{{ ti.xcom_pull(task_ids='InputConfig', key='last_working_day_arda', dag_id='LOAD_BDR_RENUMERACION_mar18_jun18') }}'
and r.fec_altareg between '{{ ti.xcom_pull(task_ids='InputConfig', key='first_calendar_day', dag_id='LOAD_BDR_RENUMERACION_mar18_jun18') }}'
and '{{ ti.xcom_pull(task_ids='InputConfig', key='last_calendar_day', dag_id='LOAD_BDR_RENUMERACION_mar18_jun18') }}'
),
moria_contratos_max as (
select x.cod_entidadg, x.cod_centrog, x.num_contratg, x.cod_productg, x.cod_subprodg, x.cod_entidad, x.cod_centro, x.num_contrato, x.cod_producto, x.cod_subprodu, x.fec_altareg, x.cod_divisa, x.fec_baja, x.rownum, x.sum_imp_deuda,
max(rownum) over ( partition by x.cod_entidadg, x.cod_centrog, x.num_contratg, x.cod_productg, x.cod_subprodg, x.cod_entidad, x.cod_centro, x.num_contrato, x.cod_producto, x.cod_subprodu ) as maxrownum
from moria_contratos x
),
renum_moria as (
select z.cod_entidadg, z.cod_centrog, z.num_contratg, z.cod_productg, z.cod_subprodg, z.cod_entidad, z.cod_centro, z.num_contrato, z.cod_producto, z.cod_subprodu, z.fec_altareg, z.fec_baja, z.sum_imp_deuda, case maxrownum when 1 then z.cod_divisa else 'ARS' end as cod_divisa
from moria_contratos_max z
where rownum = maxrownum
)
INSERT OVERWRITE TABLE bi_corp_bdr.jm_trz_cont_ren
partition(partition_date)
SELECT DISTINCT
'23100' as G7025_S1EMP,
n_new.id_cto_bdr as G7025_CONTRA1,
'23100' as G7025_EMP_ANT,
n_old.id_cto_bdr as G7025_CONT_ANT,
lpad(bl.cod_baremo_local, 5, '0') as G7025_MOTRENU,
r.fec_altareg as G7025_FEALTREL,
x.fec_yyyy_mm_dd as G7025_FEC_MOD,
concat('-', lpad(regexp_replace(cast(cast(nvl(r.sum_imp_deuda, 0) as decimal(17,2)) as string), '\\.', ''), 16, '0')) as G7025_IMPRESTR, -- PENDIENTE DE DEFINICION
r.cod_divisa as G7025_CODDIV,
lpad(mp.cod_baremo_global, 5, '0')  as G7025_MOTRENUG,
CASE WHEN r.fec_baja IS NOT NULL THEN r.fec_baja ELSE '9999-12-31' END as G7025_FEC_BAJA,
'{{ ti.xcom_pull(task_ids='InputConfig', key='month_partition_bdr', dag_id='LOAD_BDR_RENUMERACION_mar18_jun18') }}' as partition_date
FROM renum_moria r
INNER JOIN bi_corp_bdr.normalizacion_id_contratos n_old on n_old.cred_cod_entidad = r.cod_entidadg and n_old.cred_cod_centro = r.cod_centrog and n_old.cred_num_cuenta = r.num_contratg and n_old.cred_cod_producto = r.cod_productg and n_old.cred_cod_subprodu_altair = r.cod_subprodg
INNER JOIN bi_corp_bdr.normalizacion_id_contratos n_new on n_new.cred_cod_entidad = r.cod_entidad and n_new.cred_cod_centro = r.cod_centro and n_new.cred_num_cuenta = r.num_contrato and n_new.cred_cod_producto = r.cod_producto and n_new.cred_cod_subprodu_altair = r.cod_subprodu and n_new.partition_date = n_old.partition_date
LEFT JOIN bi_corp_bdr.v_baremos_local bl on bl.cod_negocio_local = '90' and bl.cod_baremo_local = '4'
LEFT JOIN bi_corp_bdr.v_map_baremos_global_local mp on mp.cod_negocio_local = 90 and mp.cod_baremo_local = bl.cod_baremo_local
LEFT JOIN santander_business_risk_arda.calendario x
on x.data_date_part = '2019-07-31'
and x.fec_yyyymmdd = '{{ ti.xcom_pull(task_ids='InputConfig', key='last_working_day_arda', dag_id='LOAD_BDR_RENUMERACION_mar18_jun18') }}'
WHERE n_old.partition_date = '{{ ti.xcom_pull(task_ids='InputConfig', key='month_partition_bdr', dag_id='LOAD_BDR_RENUMERACION_mar18_jun18') }}';


-- SUCURSALES
with sucursales_contratos as (
select r.pecdgent, r.datos_comunes_peofiori, r.penumcon, r.pecodpro, trim(r.pecodsub) as pecodsub, r.pecdgentd, r.datos_comunes_peofides,r.penumcond, r.pecodprod, trim(r.pecodsubd) as pecodsubd, r.pecodmond, p.pefecest, r.pesdoant,
row_number() over( partition by r.pecdgent, r.datos_comunes_peofiori, r.penumcon, r.pecodpro, r.pecodsub, r.pecdgentd, r.datos_comunes_peofides,r.penumcond, r.pecodprod, r.pecodsubd order by r.pecodmond ) as rownum
from bi_corp_staging.malpe_peec867c r
LEFT JOIN bi_corp_staging.malpe_pedt042 p on r.pecdgent = p.pecdgent and p.pecodofi = r.datos_comunes_peofiori and substring(p.penumcon, 4) = substring(r.penumcon, 4) and p.pecodpro = r.pecodpro and p.pecodsub = r.pecodsub
where r.partition_date = '2019-12-13' -- valor fijo hasta que se cargue una novedad. no programado
and p.partition_date = '2019-12-04' -- valor fijo para procesar hasta noviembre 2019
and p.pefecest between '{{ ti.xcom_pull(task_ids='InputConfig', key='first_calendar_day', dag_id='LOAD_BDR_RENUMERACION_mar18_jun18') }}'
and '{{ ti.xcom_pull(task_ids='InputConfig', key='last_calendar_day', dag_id='LOAD_BDR_RENUMERACION_mar18_jun18') }}'
and p.peestope = 'C'
),
sucursales_contratos_max as (
select x.pecdgent, x.datos_comunes_peofiori, x.penumcon, x.pecodpro, x.pecodsub, x.pecdgentd, x.datos_comunes_peofides, x.penumcond, x.pecodprod, x.pecodsubd, x.pecodmond, x.pefecest, x.pesdoant, x.rownum,
max(rownum) over ( partition by x.pecdgent, x.datos_comunes_peofiori, x.penumcon, x.pecodpro, x.pecodsub, x.pecdgentd, x.datos_comunes_peofides, x.penumcond, x.pecodprod, x.pecodsubd ) as maxrownum
from sucursales_contratos x
),
renum_sucursales as (
select z.pecdgent, z.datos_comunes_peofiori, z.penumcon, z.pecodpro, z.pecodsub, z.pecdgentd, z.datos_comunes_peofides, z.penumcond, z.pecodprod, z.pecodsubd, z.pefecest, z.pesdoant ,
case maxrownum when 1 then z.pecodmond else 'ARS' end as pecodmond
from sucursales_contratos_max z
where rownum = maxrownum
)
INSERT INTO TABLE bi_corp_bdr.jm_trz_cont_ren
partition(partition_date)
SELECT DISTINCT
'23100' as G7025_S1EMP,
n_new.id_cto_bdr as G7025_CONTRA1,
'23100' as G7025_EMP_ANT,
n_old.id_cto_bdr as G7025_CONT_ANT,
lpad(bl.cod_baremo_local, 5, '0') as G7025_MOTRENU,
r.pefecest as G7025_FEALTREL,
x.fec_yyyy_mm_dd as G7025_FEC_MOD,
concat('-', lpad(regexp_replace(cast(cast(nvl(r.pesdoant, 0) as decimal(17,2)) as string), '\\.', ''), 16, '0')) as G7025_IMPRESTR, -- PENDIENTE DE DEFINICION
r.pecodmond as G7025_CODDIV,
lpad(mp.cod_baremo_global, 5, '0')  as G7025_MOTRENUG,
CASE WHEN r.pefecest IS NOT NULL THEN r.pefecest ELSE '9999-12-31' END as G7025_FEC_BAJA,
'{{ ti.xcom_pull(task_ids='InputConfig', key='month_partition_bdr', dag_id='LOAD_BDR_RENUMERACION_mar18_jun18') }}' as partition_date
FROM renum_sucursales r
INNER JOIN bi_corp_bdr.normalizacion_id_contratos n_old on n_old.cred_cod_entidad = r.pecdgent and n_old.cred_cod_centro = r.datos_comunes_peofiori and substring(n_old.cred_num_cuenta, 4) = substring(r.penumcon, 4) and n_old.cred_cod_producto = r.pecodpro and n_old.cred_cod_subprodu_altair = r.pecodsub
INNER JOIN bi_corp_bdr.normalizacion_id_contratos n_new on n_new.cred_cod_entidad = r.pecdgentd and n_new.cred_cod_centro = r.datos_comunes_peofides and substring(n_new.cred_num_cuenta, 4) = substring(r.penumcond, 4) and n_new.cred_cod_producto = r.pecodprod and n_new.cred_cod_subprodu_altair = r.pecodsubd and n_new.partition_date = n_old.partition_date
LEFT JOIN bi_corp_bdr.v_baremos_local bl on bl.cod_negocio_local = '90' and bl.cod_baremo_local = '3'
LEFT JOIN bi_corp_bdr.v_map_baremos_global_local mp on mp.cod_negocio_local = 90 and mp.cod_baremo_local = bl.cod_baremo_local
LEFT JOIN santander_business_risk_arda.calendario x
on x.data_date_part = '2019-07-31'
and x.fec_yyyymmdd = '{{ ti.xcom_pull(task_ids='InputConfig', key='last_working_day_arda', dag_id='LOAD_BDR_RENUMERACION_mar18_jun18') }}'
WHERE n_old.partition_date = '{{ ti.xcom_pull(task_ids='InputConfig', key='month_partition_bdr', dag_id='LOAD_BDR_RENUMERACION_mar18_jun18') }}'
;


-- MIGRACION DE CUENTAS
with migracion_contratos as (
select r.mig_old_entidad, r.mig_old_cent_alta, r.mig_old_cuenta, p_old.pecodpro as old_pecodpro, trim(p_old.pecodsub) as old_pecodsub, r.mig_new_entidad, r.mig_new_cent_alta, r.mig_new_cuenta, p_new.pecodpro as new_pecodpro, trim(p_new.pecodsub) as new_pecodsub, r.mig_old_fech_baja, r.mig_new_divisa,
row_number() over( partition by r.mig_old_entidad, r.mig_old_cent_alta, r.mig_old_cuenta, p_old.pecodpro, trim(p_old.pecodsub), r.mig_new_entidad, r.mig_new_cent_alta, r.mig_new_cuenta, p_new.pecodpro, trim(p_new.pecodsub) order by r.mig_new_divisa ) as rownum
from bi_corp_staging.malbgc_zbdtmig r
LEFT JOIN bi_corp_staging.malpe_pedt008 p_old on p_old.pecodent = r.mig_old_entidad and p_old.pecodofi = r.mig_old_cent_alta and concat("0", p_old.pecodpro, substring(p_old.penumcon, 4) )  = r.mig_old_cuenta
LEFT JOIN bi_corp_staging.malpe_pedt008 p_new on p_new.pecodent = r.mig_new_entidad and p_new.pecodofi = r.mig_new_cent_alta and concat("0",p_new.pecodpro, substring(p_new.penumcon, 4) )  = r.mig_new_cuenta and p_old.partition_date = p_new.partition_date
where r.partition_date = '2019-12-16' -- cambiar por last_working_day luego de DICIEMBRE
and p_old.partition_date = '2019-12-04' -- cambiar por last_working_day luego de DICIEMBRE
and r.mig_old_fech_baja between '{{ ti.xcom_pull(task_ids='InputConfig', key='first_calendar_day', dag_id='LOAD_BDR_RENUMERACION_mar18_jun18') }}'
and '{{ ti.xcom_pull(task_ids='InputConfig', key='last_calendar_day', dag_id='LOAD_BDR_RENUMERACION_mar18_jun18') }}'
),
migracion_contratos_max as (
select x.mig_old_entidad, x.mig_old_cent_alta, x.mig_old_cuenta, x.old_pecodpro, x.old_pecodsub, x.mig_new_entidad, x.mig_new_cent_alta, x.mig_new_cuenta, x.new_pecodpro, x.new_pecodsub, x.mig_old_fech_baja, x.rownum, x.mig_new_divisa,
max(rownum) over ( partition by x.mig_old_entidad, x.mig_old_cent_alta, x.mig_old_cuenta, x.old_pecodpro, x.old_pecodsub, x.mig_new_entidad, x.mig_new_cent_alta, x.mig_new_cuenta, x.new_pecodpro, x.new_pecodsub ) as maxrownum
from migracion_contratos x
),
renum_migracion as (
select z.mig_old_entidad, z.mig_old_cent_alta, z.mig_old_cuenta, z.old_pecodpro, z.old_pecodsub, z.mig_new_entidad, z.mig_new_cent_alta, z.mig_new_cuenta, z.new_pecodpro, z.new_pecodsub, z.mig_old_fech_baja,
case z.maxrownum when 1 then z.mig_new_divisa else 'ARS' end as mig_new_divisa
from migracion_contratos_max z
where z.rownum = z.maxrownum
)
INSERT INTO TABLE bi_corp_bdr.jm_trz_cont_ren
partition(partition_date)
SELECT
'23100' as G7025_S1EMP,
n_new.id_cto_bdr as G7025_CONTRA1,
'23100' as G7025_EMP_ANT,
n_old.id_cto_bdr as G7025_CONT_ANT,
lpad(bl.cod_baremo_local, 5, '0') as G7025_MOTRENU,
r.mig_old_fech_baja	 as G7025_FEALTREL,
x.fec_yyyy_mm_dd as G7025_FEC_MOD,
concat('-', lpad('0', 16, '0')) as G7025_IMPRESTR,
r.mig_new_divisa as G7025_CODDIV,
lpad(mp.cod_baremo_global, 5, '0')  as G7025_MOTRENUG,
CASE WHEN r.mig_old_fech_baja IS NOT NULL THEN r.mig_old_fech_baja ELSE '9999-12-31' END as G7025_FEC_BAJA,
'{{ ti.xcom_pull(task_ids='InputConfig', key='month_partition_bdr', dag_id='LOAD_BDR_RENUMERACION_mar18_jun18') }}' as partition_date
FROM renum_migracion r
INNER JOIN bi_corp_bdr.normalizacion_id_contratos n_old on n_old.cred_cod_entidad = r.mig_old_entidad and n_old.cred_cod_centro = r.mig_old_cent_alta and substring(n_old.cred_num_cuenta, 4) = substring(r.mig_old_cuenta, 4) and n_old.cred_cod_producto = r.old_pecodpro and n_old.cred_cod_subprodu_altair = r.old_pecodsub
INNER JOIN bi_corp_bdr.normalizacion_id_contratos n_new on n_new.cred_cod_entidad = r.mig_new_entidad and n_new.cred_cod_centro = r.mig_new_cent_alta and substring(n_new.cred_num_cuenta, 4) = substring(r.mig_new_cuenta, 4) and n_new.cred_cod_producto = r.new_pecodpro and n_new.cred_cod_subprodu_altair = r.new_pecodsub and n_new.partition_date = n_old.partition_date
LEFT JOIN bi_corp_bdr.v_baremos_local bl on bl.cod_negocio_local = '90' and bl.cod_baremo_local = '1'
LEFT JOIN bi_corp_bdr.v_map_baremos_global_local mp on mp.cod_negocio_local = 90 and mp.cod_baremo_local = bl.cod_baremo_local
LEFT JOIN santander_business_risk_arda.calendario x
on x.data_date_part = '2019-07-31'
and x.fec_yyyymmdd = '{{ ti.xcom_pull(task_ids='InputConfig', key='last_working_day_arda', dag_id='LOAD_BDR_RENUMERACION_mar18_jun18') }}'
WHERE n_old.partition_date = '{{ ti.xcom_pull(task_ids='InputConfig', key='month_partition_bdr', dag_id='LOAD_BDR_RENUMERACION_mar18_jun18') }}';


-- UPGRADE DE TARJETAS
INSERT INTO TABLE bi_corp_bdr.jm_trz_cont_ren
partition(partition_date)
SELECT DISTINCT
'23100' as G7025_S1EMP,
n_new.id_cto_bdr as G7025_CONTRA1,
'23100' as G7025_EMP_ANT,
n_old.id_cto_bdr as G7025_CONT_ANT,
lpad(bl.cod_baremo_local, 5, '0') as G7025_MOTRENU,
r.fec_renum	 as G7025_FEALTREL,
x.fec_yyyy_mm_dd as G7025_FEC_MOD,
concat('-', lpad(regexp_replace(cast(cast(nvl(r.imp_reest, 0) as decimal(17,2)) as string), '\\.', ''), 16, '0')) as G7025_IMPRESTR,
r.divisa as G7025_CODDIV,
lpad(mp.cod_baremo_global, 5, '0')  as G7025_MOTRENUG,
CASE WHEN r.fec_renum IS NOT NULL THEN r.fec_renum ELSE '9999-12-31' END as G7025_FEC_BAJA,
'{{ ti.xcom_pull(task_ids='InputConfig', key='month_partition_bdr', dag_id='LOAD_BDR_RENUMERACION_mar18_jun18') }}' as partition_date
FROM bi_corp_bdr.upgrade_tarjetas r
INNER JOIN bi_corp_bdr.normalizacion_id_contratos n_old on n_old.cred_cod_entidad = r.old_entidad and n_old.cred_cod_centro = r.old_centro and substring(n_old.cred_num_cuenta, 2) = substring(r.old_cuenta, 2) and n_old.cred_cod_producto = r.old_producto and n_old.cred_cod_subprodu_altair = trim(r.old_subprodu)
INNER JOIN bi_corp_bdr.normalizacion_id_contratos n_new on n_new.cred_cod_entidad = r.new_entidad and n_new.cred_cod_centro = r.new_centro and substring(n_new.cred_num_cuenta, 2) = substring(r.new_cuenta, 2) and n_new.cred_cod_producto = r.new_producto and n_new.cred_cod_subprodu_altair = trim(r.new_subprodu) and n_new.partition_date = n_old.partition_date
LEFT JOIN bi_corp_bdr.v_baremos_local bl on bl.cod_negocio_local = '90' and bl.cod_baremo_local = '6'
LEFT JOIN bi_corp_bdr.v_map_baremos_global_local mp on mp.cod_negocio_local = 90 and mp.cod_baremo_local = bl.cod_baremo_local
LEFT JOIN santander_business_risk_arda.calendario x
on x.data_date_part = '2019-07-31'
and x.fec_yyyymmdd = '{{ ti.xcom_pull(task_ids='InputConfig', key='last_working_day_arda', dag_id='LOAD_BDR_RENUMERACION_mar18_jun18') }}'
WHERE n_old.partition_date = '{{ ti.xcom_pull(task_ids='InputConfig', key='month_partition_bdr', dag_id='LOAD_BDR_RENUMERACION_mar18_jun18') }}'
AND r.partition_date between '{{ ti.xcom_pull(task_ids='InputConfig', key='first_working_day', dag_id='LOAD_BDR_RENUMERACION_mar18_jun18') }}'
AND '{{ ti.xcom_pull(task_ids='InputConfig', key='last_working_day', dag_id='LOAD_BDR_RENUMERACION_mar18_jun18') }}';

