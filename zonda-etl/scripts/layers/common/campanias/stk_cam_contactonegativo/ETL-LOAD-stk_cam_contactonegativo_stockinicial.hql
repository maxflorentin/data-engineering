set mapred.job.queue.name=root.dataeng;
set hive.exec.dynamic.partition.mode=nonstrict;

--------------Creo un tabla temporal con la ultima particion de los datos de Cuenta - Cliente sin Mora
DROP TABLE IF EXISTS pedt008_temp_sin_mora;
CREATE TEMPORARY TABLE pedt008_temp_sin_mora AS
select p.pecodofi, cast(p.penumcon as bigint) as penumcon, p.pecodpro, p.pecodsub, p.penumper,p.pefecalt,concat('0', p.pecodpro, SUBSTRING(p.penumcon, 4)) AS cuenta,pemotbaj
from bi_corp_staging.malpe_ptb_pedt008 p
inner join
(select pecodofi, penumcon, pecodpro, pecodsub, min(cast(case when REGEXP_REPLACE(PEORDPAR, "^0+", '')='' then '0' else REGEXP_REPLACE(PEORDPAR, "^0+", '') end as int)) PEORDPAR
from bi_corp_staging.malpe_ptb_pedt008
where partition_date ='{{ ti.xcom_pull(task_ids='InputConfig', key='partition_date', dag_id='LOAD_CMN_CAMPANIAS') }}'
AND pecalpar = 'TI'
AND pecodpro IN ('02', '03', '04', '05', '06', '07', '08', '98', '99', '14')
AND peestrel='A'
group by pecodofi, penumcon, pecodpro, pecodsub) t2
on t2.pecodofi=p.pecodofi and t2.penumcon=p.penumcon and t2.pecodpro=p.pecodpro and t2.pecodsub=p.pecodsub and t2.peordpar=cast(case when REGEXP_REPLACE(p.PEORDPAR, "^0+", '')='' then '0' else REGEXP_REPLACE(p.PEORDPAR, "^0+", '') end as int)
where partition_date ='{{ ti.xcom_pull(task_ids='InputConfig', key='partition_date', dag_id='LOAD_CMN_CAMPANIAS') }}'
AND p.pecalpar = 'TI';


--------------Creo un tabla temporal con la ultima particion de los datos de Cuenta - Cliente con Mora
DROP TABLE IF EXISTS pedt008_temp_con_mora;
CREATE TEMPORARY TABLE pedt008_temp_con_mora AS
select p.pecodofi, cast(SUBSTRING(p.penumcon,4) as bigint) as penumcon, p.pecodpro, p.pecodsub, p.penumper,p.pefecalt,concat('0', p.pecodpro, SUBSTRING(p.penumcon, 4)) AS cuenta,pemotbaj
from bi_corp_staging.malpe_ptb_pedt008 p
inner join
(select pecodofi, penumcon, pecodpro, pecodsub, min(cast(case when REGEXP_REPLACE(PEORDPAR, "^0+", '')='' then '0' else REGEXP_REPLACE(PEORDPAR, "^0+", '') end as int)) PEORDPAR
from bi_corp_staging.malpe_ptb_pedt008
where partition_date ='{{ ti.xcom_pull(task_ids='InputConfig', key='partition_date', dag_id='LOAD_CMN_CAMPANIAS') }}'
AND pecalpar = 'TI'
AND pecodpro IN ('70')
AND peestrel='A'
group by pecodofi, penumcon, pecodpro, pecodsub) t2
on t2.pecodofi=p.pecodofi and t2.penumcon=p.penumcon and t2.pecodpro=p.pecodpro and t2.pecodsub=p.pecodsub and t2.peordpar=cast(case when REGEXP_REPLACE(p.PEORDPAR, "^0+", '')='' then '0' else REGEXP_REPLACE(p.PEORDPAR, "^0+", '') end as int)
where partition_date ='{{ ti.xcom_pull(task_ids='InputConfig', key='partition_date', dag_id='LOAD_CMN_CAMPANIAS') }}'
AND p.pecalpar = 'TI';


DROP TABLE IF EXISTS pedt008_prod_ori;
CREATE TEMPORARY TABLE pedt008_prod_ori AS
SELECT  pecodofi,
		penumcon,
		pecodpro,
		pecodsub,
		penumper,
		pefecalt,
		cuenta,
		pemotbaj,
		b.codigo_producto AS producto_original,
		b.codigo_subproducto AS subproducto_original
FROM pedt008_temp_con_mora a
LEFT JOIN bi_corp_staging.risksql5_productos b
ON a.pecodpro = b.codigo_producto_mora
AND a.pecodsub = b.codigo_subproducto_mora
AND b.fecha_informacion = IF('{{ ti.xcom_pull(task_ids='InputConfig', key='partition_date', dag_id='LOAD_CMN_CAMPANIAS') }}' < '2021-05-04' ,'2021-05-04' ,'{{ ti.xcom_pull(task_ids='InputConfig', key='partition_date', dag_id='LOAD_CMN_CAMPANIAS') }}')


GROUP BY
     	pecodofi,
		penumcon,
		pecodpro,
		pecodsub,
		penumper,
		pefecalt,
		cuenta,
		pemotbaj,
		b.codigo_producto,
		b.codigo_subproducto;

------------ Universo de cuentas/cliente sin mora o con mora con el producto original
DROP TABLE IF EXISTS pedt008_temp_0;
CREATE TEMPORARY TABLE pedt008_temp_0 AS
SELECT
        pecodofi,
		penumcon,
		pecodpro,
		pecodsub,
		penumper,
		pefecalt,
		cuenta,
		pemotbaj,
		pecodpro as producto_original,
		pecodsub as subproducto_original
FROM
    pedt008_temp_sin_mora
union all
SELECT
        pecodofi,
		penumcon,
		pecodpro,
		pecodsub,
		penumper,
		pefecalt,
		cuenta,
		pemotbaj,
		producto_original,
		subproducto_original
FROM
    pedt008_prod_ori;

------------ Calculo la maxima fecha de alta de las cuentas-clientes
DROP TABLE IF EXISTS pedt008_temp_1;
CREATE TEMPORARY TABLE pedt008_temp_1 AS
select pecodofi,penumcon,producto_original,subproducto_original, max(pefecalt) pefecalt
from pedt008_temp_0
group by pecodofi,penumcon,producto_original,subproducto_original;


------------ Me quedo con las cuentas activas que tenga la mayor fecha de alta y el ultimo producto
DROP TABLE IF EXISTS pedt008_temp_2;
CREATE TEMPORARY TABLE pedt008_temp_2 AS
select A.pecodofi,
		A.penumcon,
		A.producto_original,
		A.subproducto_original,
		A.pefecalt,
	    A.penumper,
	    A.pemotbaj,
		A.pecodsub,
		A.pecodpro
from pedt008_temp_0 A
inner join pedt008_temp_1 B
on A.pecodofi=B.pecodofi
and A.penumcon=B.penumcon
and A.producto_original=B.producto_original
and A.subproducto_original=B.subproducto_original
and A.pefecalt=B.pefecalt;

DROP TABLE IF EXISTS pedt008_temp_3;
CREATE TEMPORARY TABLE pedt008_temp_3 AS
select pecodofi,penumcon,producto_original,subproducto_original, max(pecodpro) pecodpro
from pedt008_temp_2
group by pecodofi,penumcon,producto_original,subproducto_original;


DROP TABLE IF EXISTS pedt008_temp;
CREATE TEMPORARY TABLE pedt008_temp AS
select A.pecodofi,
		A.penumcon,
		A.producto_original,
		A.subproducto_original,
		A.pefecalt,
	    A.penumper,
	    A.pemotbaj,
		A.pecodsub,
		A.pecodpro
from pedt008_temp_2 A
inner join pedt008_temp_3 B
on A.pecodofi=B.pecodofi
and A.penumcon=B.penumcon
and A.producto_original=B.producto_original
and A.subproducto_original=B.subproducto_original
and A.pecodpro=B.pecodpro;


--------------Creo un tabla temporal con la ultima particion de los datos de Cuenta - Cliente que en la tabla mae está activo pero no en la pedt008
DROP TABLE IF EXISTS pedt008_temp_sin_prod_act;
CREATE TEMPORARY TABLE pedt008_temp_sin_prod_act AS
select p.pecodofi, cast(p.penumcon as bigint) as penumcon, p.pecodpro, p.pecodsub, p.penumper,p.pefecalt,concat('0', p.pecodpro, SUBSTRING(p.penumcon, 4)) AS cuenta,pemotbaj
from bi_corp_staging.malpe_ptb_pedt008 p
inner join
(select pecodofi, penumcon, pecodpro, pecodsub, min(cast(case when REGEXP_REPLACE(PEORDPAR, "^0+", '')='' then '0' else REGEXP_REPLACE(PEORDPAR, "^0+", '') end as int)) PEORDPAR
from bi_corp_staging.malpe_ptb_pedt008
where partition_date ='{{ ti.xcom_pull(task_ids='InputConfig', key='partition_date', dag_id='LOAD_CMN_CAMPANIAS') }}'
AND pecalpar = 'TI'
AND pecodpro IN ('02', '03', '04', '05', '06', '07', '08', '98', '99', '14')
--AND peestrel='A'
group by pecodofi, penumcon, pecodpro, pecodsub) t2
on t2.pecodofi=p.pecodofi and t2.penumcon=p.penumcon and t2.pecodpro=p.pecodpro and t2.pecodsub=p.pecodsub and t2.peordpar=cast(case when REGEXP_REPLACE(p.PEORDPAR, "^0+", '')='' then '0' else REGEXP_REPLACE(p.PEORDPAR, "^0+", '') end as int)
where partition_date ='{{ ti.xcom_pull(task_ids='InputConfig', key='partition_date', dag_id='LOAD_CMN_CAMPANIAS') }}'
AND p.pecalpar = 'TI';

DROP TABLE IF EXISTS pedt008_temp_sin_prod_act_1;
CREATE TEMPORARY TABLE pedt008_temp_sin_prod_act_1 AS
SELECT
	A.pecodofi,
	A.penumcon,
	A.pecodpro,
	A.pecodsub,
	A.penumper,
	A.pemotbaj
FROM pedt008_temp_sin_prod_act A
LEFT JOIN pedt008_temp_0 B
on A.pecodofi=B.pecodofi
and A.penumcon=B.penumcon
and A.pecodpro=B.producto_original
and A.pecodsub=B.subproducto_original
and A.penumper=B.penumper
WHERE B.penumper is null;


-----------STOCK BASE NEGATIVOS----------------
DROP TABLE IF EXISTS base_negativos;
CREATE TEMPORARY TABLE base_negativos AS
select
DISTINCT cast(penumper as bigint) cod_per_nup,
SUBSTRING(fecha_carga,1,10) as fecha_alta
from bi_corp_staging.rio35_base_de_negativos
where partition_date='{{ ti.xcom_pull(task_ids='InputConfig', key='partition_date', dag_id='LOAD_CMN_CAMPANIAS') }}';


----------- CUENTAS---------------
DROP TABLE IF EXISTS CUENTAS;
CREATE TEMPORARY TABLE CUENTAS AS
select
cast(coalesce(trim(pedt008.penumper),sin_prod_act.penumper) as bigint) as cod_per_nup,
'CUENTAS' as fuente,
cast(NULL as string) as ds_cam_motivo_descarte,
cast(NULL as string) as dt_cam_fecha_alta,
cast(NULL as string) as dt_cam_fecha_baja,
mae.partition_date
from bi_corp_staging.bgdtmae mae
LEFT JOIN pedt008_temp pedt008
ON mae.centro_alta = pedt008.pecodofi
AND CAST(SUBSTRING(mae.cuenta,4) AS bigint) = pedt008.penumcon
AND trim(mae.producto) = trim(pedt008.producto_original)
AND trim(mae.subprodu) = trim(pedt008.subproducto_original)
LEFT JOIN pedt008_temp_sin_prod_act_1 sin_prod_act
ON mae.centro_alta = sin_prod_act.pecodofi
AND CAST(SUBSTRING(mae.cuenta,4) AS bigint) = sin_prod_act.penumcon
AND trim(mae.producto) = sin_prod_act.pecodpro
AND trim(mae.subprodu) = sin_prod_act.pecodsub
WHERE mae.partition_date='{{ ti.xcom_pull(task_ids='InputConfig', key='partition_date', dag_id='LOAD_CMN_CAMPANIAS') }}'
AND divisa='ARS'
AND (mae.indesta = 'A' OR pedt008.pecodpro='70')
AND mae.num_convenio IN (select mco_num_convenio
					 from  bi_corp_staging.malbgc_bgdtmco co
					 where mco_suscriptor in ('00000144326','00000081186')
					 and co.partition_date='{{ ti.xcom_pull(task_ids='InputConfig', key='partition_date', dag_id='LOAD_CMN_CAMPANIAS') }}') ;


DROP TABLE IF EXISTS stk_negativos;
CREATE TEMPORARY TABLE stk_negativos AS
select
con.cod_per_nup,
'TLMK' as fuente,
cast(NULL as string) as ds_cam_motivo,
IF(ts_cc_operacion is not null, substring(ts_cc_operacion,1,10), fecha_alta) as dt_cam_fecha_alta,
cast(NULL as string) as dt_cam_fecha_baja,
con.partition_date
from bi_corp_common.stk_cc_sivdcontacto con
join  bi_corp_common.bt_cc_sivdoperacion op on (op.cod_cc_contacto=con.cod_cc_contacto)
LEFT JOIN base_negativos b on b.cod_per_nup=con.cod_per_nup
where
con.partition_date='{{ ti.xcom_pull(task_ids='InputConfig', key='partition_date', dag_id='LOAD_CMN_CAMPANIAS') }}'
and cod_cc_gestion in ('10','340','735')
and con.cod_per_nup is not null

union all

SELECT
c.cod_per_nup,
fuente,
cast(NULL as string) as ds_cam_motivo,
IF (fecha_alta is not null,b.fecha_alta, c.partition_Date) as dt_cam_fecha_alta,
cast(NULL as string) as dt_cam_fecha_baja,
c.partition_date
FROM cuentas c
LEFT JOIN base_negativos b on b.cod_per_nup=c.cod_per_nup


union all

select a.cod_per_nup,
'SGC' as fuente,
cast(NULL as string) as ds_cam_motivo,
IF(ts_rec_gestion_alta is not null,SUBSTRING(CAST(ts_rec_gestion_alta AS STRING),1,10),ba.fecha_alta) as dt_cam_fecha_alta,
cast(NULL as string) as dt_cam_fecha_baja,
a.partition_date
from bi_corp_common.stk_rec_gestiones_sgc a
join bi_corp_staging.rio56_informacion_adjunta b on a.cod_rec_sector = b.ide_gestion_Sector and a.cod_rec_gestion_nro = cast(b.ide_gestion_nro as bigint) and b.cod_info_doc_reque = '1759' and b.dato_info_doc_reque = '1'
LEFT JOIN base_negativos ba on ba.cod_per_nup=a.cod_per_nup
where a.partition_Date = '{{ ti.xcom_pull(task_ids='InputConfig', key='partition_date', dag_id='LOAD_CMN_CAMPANIAS') }}'
and a.cod_rec_circuito in (32112, 32625, 43769);


DROP TABLE IF EXISTS stk_cam_contaconegativo;
CREATE TEMPORARY TABLE stk_cam_contaconegativo AS

select
cod_per_nup,
fuente,
ds_cam_motivo,
dt_cam_fecha_alta,
dt_cam_fecha_baja,
partition_Date

from stk_negativos

union all

select
neg.cod_per_nup,
cast(NULL as string) as fuente,
cast(NULL as string) as ds_cam_motivo,
fecha_alta as dt_cam_fecha_alta,
cast(NULL as string) as dt_cam_fecha_baja,
'{{ ti.xcom_pull(task_ids='InputConfig', key='partition_date', dag_id='LOAD_CMN_CAMPANIAS') }}' as partition_Date

from base_negativos neg
where neg.cod_per_nup is not null
and neg.cod_per_nup not in (select stk.cod_per_nup
								 from stk_negativos stk )


insert overwrite table bi_corp_common.stk_cam_contaconegativo
partition(partition_date)

select
cod_per_nup,
ds_cam_fuente,
ds_cam_motivo,
dt_cam_fecha_alta,
dt_cam_fecha_baja,
partition_Date
from (select  row_number() OVER (partition by cod_per_nup  order by  dt_cam_fecha_alta asc) as orden,
 cod_per_nup,
ds_cam_fuente,
ds_cam_motivo,
dt_cam_fecha_alta,
dt_cam_fecha_baja,
partition_date
from stk_cam_contaconegativo
) p where orden=1