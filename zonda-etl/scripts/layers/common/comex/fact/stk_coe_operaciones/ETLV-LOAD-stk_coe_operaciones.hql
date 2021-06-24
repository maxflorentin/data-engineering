set hive.merge.mapfiles=true;
set hive.merge.mapredfiles=true;
set hive.merge.size.per.task=4000000;
set hive.merge.smallfiles.avgsize=16000000;
set hive.exec.dynamic.partition.mode=nonstrict;
SET mapred.job.queue.name=root.dataeng;


---------------------------------------------------------------------------------
-- TABLA TEMPORAL CW_FORMULARIOS PARA BUSCAR DIMENSIONES --
---------------------------------------------------------------------------------
 -- se busca la maxima partición de cada operacion y secuencia para luego cruzar por cod_ops y secuencia

 create temporary table analytics.cw_formularios_temp
 as
 select
 a.cod_ops,
 a.secuencia,
 a.canal,
 a.usu_orig_rechazo,
 a.usu_retro,
 a.cod_concep,
 a.estado,
 a.fecha_verif,
 a.tipo_form,
 a.moneda,
 a.nup,
 a.nro_form,
 a.id_celula,
 a.monto,
 a.user_verif,
 a.nro_oper_rel,
 max(partition_date)
 from bi_corp_staging.comex_rio39_cw_formularios a
 group by a.cod_ops,
 a.secuencia,
 a.canal,
 a.usu_orig_rechazo,
 a.usu_retro,
 a.cod_concep,
 a.estado,
 a.fecha_verif,
 a.tipo_form,
 a.moneda,
 a.nup,
 a.nro_form,
 a.id_celula,
 a.monto,
 a.user_verif,
 a.nro_oper_rel;

---------------------------------------------------------------------------------
-- TABLA TEMPORAL OPP_OPS PARA BUSCAR DIMENSIONES --
---------------------------------------------------------------------------------

create temporary table analytics.opp_ops_temp
as
select
distinct trim(a.cod_ops) as cod_ops,
a.numero as cod_bco_corresponsal,
a.pais_origen,
a.pais_benef
from bi_corp_staging.comex_rio39_opp_ops a;

----------------------------------------------------------------------------------------------------------
-- TABLA TEMPORAL DE OPERACIONES LIQUIDADAS Y PENDIENTES DE LIQUIDAR (SOLO ORD. PAGO Y TRANSFERENCIAS)  --
----------------------------------------------------------------------------------------------------------
create temporary table analytics.liquidadas_temp
as
select
case a.cod_est_pago when '2' then a.fecha_liq when '9' then a.f_verif when '12' then a.f_verif else 'n/a' end fecha_operacion,
case a.cod_est_pago when '12' then 'Pendientes de cotizar' when '9' then 'Pendientes de cotizar' when '2' then 'Liquidadas' else 'n/a' end estado_operacion,
cast(a.cod_producto as int) as cod_producto,
a.moneda as cod_moneda,
case b.canal when '07' then 'OBCM' when '04' then 'OLB' when '00' then 'Sucursal' else 'Sin canal' end as canal,
lpad(trim(a.nup),8,'0') as nup,
a.cod_ops as cod_operacion,
a.liq_bkt as nro_operacion,
case a.secuencia when 'null' then cast(null as string) else a.secuencia end secuencia,
a.id_celula as cod_celula,
cast(a.importe as double) as importe_operacion,
trim(a.usu_carga) as id_usuario_opera,
case b.usu_orig_rechazo when 'null' then cast(null as string) else trim(b.usu_orig_rechazo) end id_usu_rechaza_incorrectamente,
case b.usu_retro when 'null' then cast(null as string) else trim(b.usu_retro) end id_usu_relanza,
trim(b.cod_concep) as cod_concep,
c.cod_bco_corresponsal,
c.pais_origen as cod_pais_origen,
c.pais_benef as cod_pais_beneficiario
from bi_corp_staging.comex_rio39_opp_ops_pagos a
left join analytics.cw_formularios_temp b on (a.cod_ops = b.cod_ops and a.secuencia = b.secuencia)
left join analytics.opp_ops_temp c on a.cod_ops = c.cod_ops
where a.cod_producto in ('1', '2')
and a.cod_est_pago in ('12', '2', '9');

----------------------------------------------------------------------------------------------------------
-- TABLA TEMPORAL DE OPERACIONES PENDIENTES PROYECTADAS (SOLO ORD. PAGO Y TRANSFERENCIAS)  --
----------------------------------------------------------------------------------------------------------


---------------------------------------------------------------------------------
-- TABLA TEMPORAL DE OPERACIONES RECHAZADAS (SOLO ORD. PAGO Y TRANSFERENCIAS) --
---------------------------------------------------------------------------------
create temporary table analytics.rechazos_temp
as
select
a.fecha_verif as fecha_operacion,
'Rechazada' as estado_operacion,
cast (case tipo_form when 'TO' then '2' when 'EO' then '1' end as int) as cod_producto,
a.moneda as cod_moneda,
case a.canal when '07' then 'OBCM' when '04' then 'OLB' when '00' then 'Sucursal' else 'Sin canal' end as canal,
lpad(trim(a.nup),8,'0') as nup,
a.nro_form as cod_operacion,
a.nro_oper_rel as nro_operacion,
case a.secuencia when 'null' then cast(null as string) else a.secuencia end secuencia,
a.id_celula as cod_celula,
a.monto as importe_operacion,
trim(a.user_verif) as id_usuario_opera,
trim(a.usu_orig_rechazo) as id_usu_rechaza_incorrectamente,
trim(a.usu_retro) as id_usu_relanza,
trim(a.cod_concep) as cod_concep,
cast(null as string) as cod_bco_corresponsal,
cast(null as string) as cod_pais_origen,
cast(null as string) as cod_pais_beneficiario
from analytics.cw_formularios_temp a
where((a.tipo_form = 'TO' and a.estado='K') or (a.tipo_form = 'EO' and a.estado='R'));

---------------------------------------------------------------------------------
-- UNION DE OPERACIONES LIQUIDADAS, PENDIENTES Y RECHAZADAS --
---------------------------------------------------------------------------------
create temporary table analytics.liq_y_rechazos_temp
as
select
*
from analytics.liquidadas_temp
union all
select
*
from analytics.rechazos_temp;

---------------------------------------------------------------------------------
-- TABLA TEMPORAL DE NOMINA --
---------------------------------------------------------------------------------
create temporary table analytics.nomina_temp
as
select
distinct(a.cod_legajo) as cod_legajo,
a.nombre as nombre_oficial
from santander_business_risk_arda.nomina_empleados a
where substring(a.cod_periodo, 1, 4) >= '2020';

---------------------------------------------------------------------------------
-- TABLA TEMPORAL DE SUCURSAL --
---------------------------------------------------------------------------------
create temporary table analytics.sucursal_temp
as
select
cast (trim(a.cod_centro) as string) as cod_sucursal,
a.desc_comp_centro_op as desc_sucursal
from bi_corp_staging.tcdt050 a
where a.partition_date = '{{ ti.xcom_pull(task_ids='InputConfig', key='partition_date', dag_id='LOAD_CMN_Comex-Daily') }}';


---------------------------------------------------------------------------------
-- TABLA TEMPORAL DE PERSONAS --
---------------------------------------------------------------------------------
create temporary table analytics.personas_temp
as
select
a.cod_per_nup as nup_personas,
lpad(cast(a.cod_per_sucursaladministradora as string),5,'0') as cod_per_sucursaladministradora_rel,
a.ds_per_segmento as segmento,
a.ds_per_subsegmento as subsegmento,
a.cod_per_cuadrante as cuadrante,
cast(a.flag_per_uge as string) as flag_uge,
c.desc_sucursal
from bi_corp_common.stk_per_personas a
left join analytics.sucursal_temp c on lpad(cast(a.cod_per_sucursaladministradora as string),5,'0') = c.cod_sucursal
where  a.partition_date = '{{ ti.xcom_pull(task_ids='InputConfig', key='partition_date', dag_id='LOAD_CMN_Comex-Daily') }}';


---------------------------------------------------------------------------------
-- TABLA TEMPORAL DE CONCEPTOS --
---------------------------------------------------------------------------------
create temporary table analytics.conceptos_temp
as
select
trim(a.id_concepto) as id_concepto,
a.cod_concepto,
a.desc_concepto
from bi_corp_staging.comex_rio39_config_conceptos a
where a.partition_date = '{{ ti.xcom_pull(task_ids='InputConfig', key='partition_date', dag_id='LOAD_CMN_Comex-Daily') }}';


---------------------------------------------------------------------------------
-- TABLA TEMPORAL DE MONEDA --
---------------------------------------------------------------------------------
create temporary table analytics.monedas_temp
as
select
a.moncod as cod_moneda,
a.mondes as moneda
from bi_corp_staging.comex_rio39_opp_monedas a
where a.partition_date = '{{ ti.xcom_pull(task_ids='InputConfig', key='partition_date', dag_id='LOAD_CMN_Comex-Daily') }}';


---------------------------------------------------------------------------------
-- TABLA TEMPORAL DE PRODUCTOS --
---------------------------------------------------------------------------------
create temporary table analytics.productos_temp
as
select
cast(a.cod_prod as int) as cod_prod,
a.desc_prod as producto
from  bi_corp_staging.comex_rio39_opp_productos a
where a.partition_date = '{{ ti.xcom_pull(task_ids='InputConfig', key='partition_date', dag_id='LOAD_CMN_Comex-Daily') }}'
and( a.cod_prod ='1' or a.cod_prod ='2');


---------------------------------------------------------------------------------
-- TABLA TEMPORAL DE MOTIVO RECHAZOS --
---------------------------------------------------------------------------------

create temporary table analytics.motivo_rechazo_temp1
as
select
*
from bi_corp_staging.comex_rio39_motivos_rechazo a
where a.partition_date = '{{ ti.xcom_pull(task_ids='InputConfig', key='partition_date', dag_id='LOAD_CMN_Comex-Daily') }}';


create temporary table analytics.motivo_rechazo_temp
as
select
a.nro_form,
a.motivo_rechazo,
b.motivo_interno,
b.motivo_externo,
b.id_agrupador
from bi_corp_staging.comex_rio39_rechazos_formulario a
inner join analytics.motivo_rechazo_temp1 b ON a.motivo_rechazo = b.id
where a.partition_date = '{{ ti.xcom_pull(task_ids='InputConfig', key='partition_date', dag_id='LOAD_CMN_Comex-Daily') }}';


---------------------------------------------------------------------------------
-- TABLA TEMPORAL DE PAISES --
---------------------------------------------------------------------------------

create temporary table analytics.paises_temp
as
select
a.cod_bcra,
a.descripcion as pais
from bi_corp_staging.comex_rio39_paises a
where a.prohibido = 'N'
and a.partition_date = '{{ ti.xcom_pull(task_ids='InputConfig', key='partition_date', dag_id='LOAD_CMN_Comex-Daily') }}';


---------------------------------------------------------------------------------
-- TABLA TEMPORAL DE CELULA --
---------------------------------------------------------------------------------

create temporary table analytics.tipo_celula_temp
as
select
a.id as cod_tipo_celula,
a.descripcion as ds_tipo_celula
FROM bi_corp_staging.comex_rio39_tipo_celula a
where a.activo = 'S'
and a.partition_date = '{{ ti.xcom_pull(task_ids='InputConfig', key='partition_date', dag_id='LOAD_CMN_Comex-Daily') }}';



create temporary table analytics.celulas_temp
as
select
a.id as cod_celula,
a.descripcion as ds_celula,
a.tipo_celula as cod_tipo_celula,
b.ds_tipo_celula
from bi_corp_staging.comex_rio39_celulas a
left join analytics.tipo_celula_temp b on a.tipo_celula = b.cod_tipo_celula
where a.activo = 'S'
and a.partition_date ='{{ ti.xcom_pull(task_ids='InputConfig', key='partition_date', dag_id='LOAD_CMN_Comex-Daily') }}';





insert overwrite table bi_corp_common.stk_coe_operaciones
partition(partition_date)


select
liq_y_rechazos_temp.cod_operacion as cod_coe_operacion,
liq_y_rechazos_temp.nro_operacion as cod_coe_nrooperacion,
liq_y_rechazos_temp.secuencia as ds_coe_secuencia,
liq_y_rechazos_temp.fecha_operacion as dt_coe_fecha_operacion,
liq_y_rechazos_temp.estado_operacion as ds_coe_estado_operacion,
liq_y_rechazos_temp.cod_producto as cod_coe_producto,
liq_y_rechazos_temp.cod_moneda as cod_coe_moneda,
liq_y_rechazos_temp.canal as ds_coe_canal,
liq_y_rechazos_temp.nup as cod_per_nup, --> se utiliza para contabilizar la cantidad de clientes
liq_y_rechazos_temp.id_usuario_opera as cod_coe_oficial_opera,
liq_y_rechazos_temp.id_usu_rechaza_incorrectamente as cod_coe_oficial_rechaza_inc,
liq_y_rechazos_temp.id_usu_relanza as cod_coe_oficial_relanza,
liq_y_rechazos_temp.importe_operacion as fc_coe_importe,
liq_y_rechazos_temp.cod_bco_corresponsal as cod_coe_bco_corresponsal,

conceptos_temp.desc_concepto as ds_coe_concepto,
conceptos_temp.cod_concepto as cod_coe_concepto,

personas_temp.cod_per_sucursaladministradora_rel as cod_coe_sucursal,
personas_temp.desc_sucursal as ds_coe_sucursal,
personas_temp.segmento as ds_coe_segmento,
personas_temp.subsegmento as ds_coe_subsegmento,
personas_temp.cuadrante as cod_coe_cuadrante,
personas_temp.flag_uge as flag_coe_uge,

a.nombre_oficial as ds_coe_oficial_opera,
b.nombre_oficial as ds_coe_oficial_rechaza_inc,
c.nombre_oficial as ds_coe_ofcial_relanza,

monedas_temp.moneda as ds_coe_moneda,

productos_temp.producto as ds_coe_producto,

motivo_rechazo_temp.motivo_rechazo as cod_coe_motivo_rechazo,
motivo_rechazo_temp.motivo_interno as ds_coe_motivo_rechazo_interno,
motivo_rechazo_temp.motivo_externo as ds_coe_motivo_rechazo_externo,
motivo_rechazo_temp.id_agrupador as cod_coe_agrupador_motivo,

d.pais as ds_coe_pais_origen,
liq_y_rechazos_temp.cod_pais_origen as cod_coe_pais_origen,
e.pais as ds_coe_pais_beneficiario,
liq_y_rechazos_temp.cod_pais_beneficiario as cod_coe_pais_beneficiario,

celulas_temp.cod_celula as cod_coe_celula,
celulas_temp.ds_celula as ds_coe_celula,
celulas_temp.cod_tipo_celula as cod_coe_tipo_celula,
celulas_temp.ds_tipo_celula as ds_coe_tipo_celula,

'{{ ti.xcom_pull(task_ids='InputConfig', key='partition_date', dag_id='LOAD_CMN_Comex-Daily') }}' as partition_date

from analytics.liq_y_rechazos_temp
left join analytics.nomina_temp a on liq_y_rechazos_temp.id_usuario_opera = a.cod_legajo
left join analytics.nomina_temp b on liq_y_rechazos_temp.id_usu_rechaza_incorrectamente = b.cod_legajo
left join analytics.nomina_temp c on liq_y_rechazos_temp.id_usu_relanza = c.cod_legajo
left join analytics.personas_temp on liq_y_rechazos_temp.nup = personas_temp.nup_personas
left join analytics.conceptos_temp on liq_y_rechazos_temp.cod_concep = conceptos_temp.id_concepto
left join analytics.monedas_temp on liq_y_rechazos_temp.cod_moneda = monedas_temp.cod_moneda
left join analytics.productos_temp on liq_y_rechazos_temp.cod_producto = productos_temp.cod_prod
left join analytics.motivo_rechazo_temp on liq_y_rechazos_temp.cod_operacion = motivo_rechazo_temp.nro_form
left join analytics.paises_temp d on liq_y_rechazos_temp.cod_pais_origen = d.cod_bcra
left join analytics.paises_temp e on liq_y_rechazos_temp.cod_pais_beneficiario = e.cod_bcra
left join analytics.celulas_temp on liq_y_rechazos_temp.cod_celula = celulas_temp.cod_celula
;