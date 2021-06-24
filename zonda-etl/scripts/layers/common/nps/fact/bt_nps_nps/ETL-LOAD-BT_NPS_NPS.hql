SET hive.execution.engine=spark;
SET spark.yarn.queue=root.dataeng;
SET hive.exec.dynamic.partition.mode=nonstrict;


INSERT OVERWRITE TABLE bi_corp_common.bt_nps_nps
    PARTITION (partition_date)
SELECT DISTINCT j.cod_per_nup,
cod_nps_respuesta,
if (UPPER(ds_nps_tipo_encuesta) LIKE 'GETNET%', CAST(cod_per_nro_doc as bigINT) ,CAST(p1.ds_per_numdoc AS bigINT)) as cod_per_nro_doc,
UPPER(cod_nps_nro_ubicacion),
ds_nps_agrupados,
UPPER(ds_nps_canal) as ds_nps_canal,
UPPER(ds_nps_descripcion),
UPPER(ds_nps_desc_ubicacion),
p1.fc_per_edad as ds_per_edad,
UPPER(ds_nps_ejecutivo),
UPPER(ds_nps_legajo),
UPPER(ds_nps_mejor_prod),
UPPER(ds_nps_momento_critico),
UPPER(ds_nps_negocio),
if (UPPER(ds_nps_tipo_encuesta) LIKE 'GETNET%',UPPER(j.ds_per_nombre),CONCAT( if (TRIM(p1.ds_per_apellido) is null,' ',CONCAT(TRIM(p1.ds_per_apellido), ', ')), TRIM(p1.ds_per_nombre))) as ds_per_nombre,
UPPER(ds_nps_nombre_sucursal),
UPPER(ds_nps_renta),
CAST(ds_nps_rentabilidad AS DECIMAL(12,2)),
UPPER(p1.ds_per_segmento) as ds_per_segmento,
UPPER(p1.cod_per_sexo) as ds_per_sexo,
if (UPPER(ds_nps_tipo_encuesta) LIKE 'GETNET%',UPPER(ds_per_tipo_doc),UPPER(p1.ds_per_tipodoc)) as ds_per_tipo_doc,
UPPER(ds_nps_tipo_encuesta),
dt_nps_fecha_encuesta,
ds_nps_antiguedad_meses,
ds_nps_orden,
ds_nps_pregunta,
ds_nps_respuesta,
UPPER(p1.ds_per_subsegmento) as ds_per_subsegmento,
j.partition_date
FROM (

Select
lpad(nup,8,0) as cod_per_nup,
responseid as cod_nps_respuesta,
lpad(nro_doc,11,0) as cod_per_nro_doc,
if (nro_ubicacion = 'WorkCafe','0812', LPAD (nro_ubicacion, 4, 0)) as cod_nps_nro_ubicacion,
q1_nps_group as ds_nps_agrupados,
'SUCURSAL' as ds_nps_canal,
CASE WHEN ucase (desc_transaccion) LIKE '%EXTRACCION%' THEN 'EXTRACCION'
     WHEN ucase (desc_transaccion) LIKE 'CONSULTA%' THEN 'CONSULTA'
     WHEN ucase (desc_transaccion) LIKE 'DEP%SITO%' THEN 'DEPOSITO'
     WHEN ucase (desc_transaccion) LIKE 'TRANSFERENCIA%' THEN 'TRANSFERENCIA'
     ELSE 'OTROS'
END as ds_nps_descripcion,
cast(null as string) as ds_nps_desc_ubicacion,
edad as ds_per_edad,
cast(null as string) as ds_nps_ejecutivo,
cast(null as string) as ds_nps_legajo,
desc_mejor_producto as ds_nps_mejor_prod,
'ATM'  as ds_nps_momento_critico,
cast(null as string) as ds_nps_negocio,
nombre as ds_per_nombre,
nombre_sucursal as ds_nps_nombre_sucursal,
CASE
   WHEN UCASE (renta) LIKE 'BANCA PRIVADA%' THEN 'BANCA PRIVADA'
   WHEN UCASE (renta) LIKE 'RENTA ALTA SUPERIOR%' THEN 'SELECT'
   WHEN UCASE (renta) LIKE 'RENTA ALTA%' THEN 'ALTA'
   WHEN UCASE (renta) LIKE 'RENTA MEDIA%' THEN 'MEDIA'
   WHEN UCASE (renta) LIKE 'RENTA MASIVA%' THEN 'MASIVA'
   WHEN UCASE (renta) LIKE 'INSTITUCION%' THEN 'INSTITUCION'
   WHEN UCASE (renta) LIKE 'PYME 1%' THEN 'PYME 1'
   WHEN UCASE (renta) LIKE 'PYME 2%' THEN 'PYME 2'
   WHEN UCASE (renta) LIKE 'COMERCIO%' THEN 'COMERCIO'
   WHEN UCASE (renta) LIKE 'EMPRESA%' THEN 'EMPRESA'
   WHEN UCASE (renta) LIKE '%GRAN%EMPRESA%' THEN 'GRAN EMPRESA'
   ELSE 'VER'
END as ds_nps_renta,
cast(null as decimal(12,2)) as ds_nps_rentabilidad,
segmento as ds_nps_segmento,
sexo as ds_per_sexo,
cast(null as string) as ds_per_tipo_doc,
'ATM' as ds_nps_tipo_encuesta,
to_date(from_unixtime(UNIX_TIMESTAMP(partition_date, 'yyyy-MM-dd'))) as dt_nps_fecha_encuesta,
cast(null as int) as ds_nps_antiguedad_meses,
rank() over (partition by responseid order by question_id asc) as ds_nps_orden,
question_desc as ds_nps_pregunta,
respuesta as ds_nps_respuesta,id_respuesta,
partition_date
from bi_corp_staging.qualtrics_nps_atm
where partition_date='{{ ti.xcom_pull(task_ids='InputConfig', key='partition_date', dag_id='LOAD_CMN_NPS-Daily') }}'
and nup is not null


union all

Select
lpad(penumber,8,0) as cod_per_nup,
responseid as cod_nps_respuesta,
lpad(nro_doc,11,0) as cod_per_nro_doc,
LPAD (nro_ubicacion, 4, 0) as cod_nps_nro_ubicacion,
q1_nps_group as ds_nps_agrupados,
'SUCURSAL'  as ds_nps_canal,
IF (agrup_producto = 'Extracciones Cuentas', 'EXTRACCION','OTROS') as ds_nps_descripcion,
ucase(ubicacion) as ds_nps_desc_ubicacion,
edad as ds_per_edad,
if (nombre_usuario is null, ejecutivo, nombre_usuario) as ds_nps_ejecutivo,
if (nombre_usuario is null, legajo, usuario_atencion) as ds_nps_legajo,
ucase(mejor_producto) as ds_nps_mejor_prod,
'CAJA'as ds_nps_momento_critico,
cast(null as string) as ds_nps_negocio,
ucase(nombre) as ds_per_nombre,
ucase(nombre_sucursal) as ds_nps_nombre_sucursal,
CASE
   WHEN UCASE (renta) LIKE 'BANCA PRIVADA%' THEN 'BANCA PRIVADA'
   WHEN UCASE (renta) LIKE 'RENTA ALTA SUPERIOR%' THEN 'SELECT'
   WHEN UCASE (renta) LIKE 'RENTA ALTA%' THEN 'ALTA'
   WHEN UCASE (renta) LIKE 'RENTA MEDIA%' THEN 'MEDIA'
   WHEN UCASE (renta) LIKE 'RENTA MASIVA%' THEN 'MASIVA'
   WHEN UCASE (renta) LIKE 'INSTITUCION%' THEN 'INSTITUCION'
   WHEN UCASE (renta) LIKE 'PYME 1%' THEN 'PYME 1'
   WHEN UCASE (renta) LIKE 'PYME 2%' THEN 'PYME 2'
   WHEN UCASE (renta) LIKE 'COMERCIO%' THEN 'COMERCIO'
   WHEN UCASE (renta) LIKE 'EMPRESA%' THEN 'EMPRESA'
   WHEN UCASE (renta) LIKE '%GRAN%EMPRESA%' THEN 'GRAN EMPRESA'
   ELSE 'VER'
END as ds_nps_renta,
rentabilidad as ds_nps_rentabilidad,
ucase(segmento) as ds_nps_segmento,
sexo as ds_per_sexo,
tipo_doc as ds_per_tipo_doc,
'caja' as ds_nps_tipo_encuesta,
to_date(from_unixtime(UNIX_TIMESTAMP(partition_date, 'yyyy-MM-dd'))) as dt_nps_fecha_encuesta,
antiguedad_en_meses as ds_nps_antiguedad_meses,
rank() over (partition by responseid order by question_id asc) as ds_nps_orden,
question_desc as ds_nps_pregunta,
respuesta as ds_nps_respuesta,id_respuesta,
partition_date
from bi_corp_staging.qualtrics_nps_caja
where partition_date='{{ ti.xcom_pull(task_ids='InputConfig', key='partition_date', dag_id='LOAD_CMN_NPS-Daily') }}'
AND penumber IS NOT NULL


union all

Select
lpad(penumber,8,0)  as cod_per_nup,
responseid as cod_nps_respuesta,
lpad(nro_doc,11,0) as cod_per_nro_doc,
LPAD (nro_ubicacion, 4, 0) as cod_nps_nro_ubicacion,
q1_nps_group as ds_nps_agrupados,
'SUCURSAL' as ds_nps_canal,
IF (agrup_producto = 'Extracciones Cuentas', 'EXTRACCION','OTROS') as ds_nps_descripcion,
upper(ubicacion) as ds_nps_desc_ubicacion,
edad as ds_per_edad,
if (nombre_usuario is null, ejecutivo, nombre_usuario) as ds_nps_ejecutivo,
if (nombre_usuario is null, legajo, usuario_atencion) as ds_nps_legajo,
ucase(mejor_producto) as ds_nps_mejor_prod,
'CAJA_PYM' as ds_nps_momento_critico,
cast(null as string) as ds_nps_negocio,
nombre as ds_per_nombre,
ucase(nombre_sucursal) as ds_nps_nombre_sucursal,
CASE
   WHEN UCASE (renta) LIKE 'BANCA PRIVADA%' THEN 'BANCA PRIVADA'
   WHEN UCASE (renta) LIKE 'RENTA ALTA SUPERIOR%' THEN 'SELECT'
   WHEN UCASE (renta) LIKE 'RENTA ALTA%' THEN 'ALTA'
   WHEN UCASE (renta) LIKE 'RENTA MEDIA%' THEN 'MEDIA'
   WHEN UCASE (renta) LIKE 'RENTA MASIVA%' THEN 'MASIVA'
   WHEN UCASE (renta) LIKE 'INSTITUCION%' THEN 'INSTITUCION'
   WHEN UCASE (renta) LIKE 'PYME 1%' THEN 'PYME 1'
   WHEN UCASE (renta) LIKE 'PYME 2%' THEN 'PYME 2'
   WHEN UCASE (renta) LIKE 'COMERCIO%' THEN 'COMERCIO'
   WHEN UCASE (renta) LIKE 'EMPRESA%' THEN 'EMPRESA'
   WHEN UCASE (renta) LIKE '%GRAN%EMPRESA%' THEN 'GRAN EMPRESA'
   ELSE 'VER'
END as ds_nps_renta,
rentabilidad as ds_nps_rentabilidad,
ucase(segmento) as ds_nps_segmento,
sexo as ds_per_sexo,
tipo_doc as ds_per_tipo_doc,
'caja pymes' as ds_nps_tipo_encuesta,
to_date(from_unixtime(UNIX_TIMESTAMP(partition_date, 'yyyy-MM-dd'))) as dt_nps_fecha_encuesta,
antiguedad_en_meses as ds_nps_antiguedad_meses,
rank() over (partition by responseid order by question_id asc) as ds_nps_orden,
question_desc as ds_nps_pregunta,
respuesta as ds_nps_respuesta,id_respuesta,
partition_date
from bi_corp_staging.qualtrics_nps_caja_pym
where partition_date='{{ ti.xcom_pull(task_ids='InputConfig', key='partition_date', dag_id='LOAD_CMN_NPS-Daily') }}'
AND  penumber IS NOT NULL


Union all

Select
lpad(nup,8,0) as cod_per_nup,
response_id as cod_nps_respuesta,
lpad(nro_doc,11,0) as cod_per_nro_doc,
IF(nro_ubicacion like 'A%',ubicacion,if (nro_ubicacion like 'B%',ubicacion,nro_ubicacion)) cod_nps_nro_ubicacion,
q1_nps_group as ds_nps_agrupados,
ucase(if (ucase(momento_critico) LIKE '%CONTACT CENTER','CONTACT CENTER',if(momento_critico is null,'CONTACT CENTER', momento_Critico))) as ds_nps_canal,
CASE
     WHEN descripcion LIKE '%INFORMACION%' THEN 'ASESORAMIENTO'
     WHEN descripcion LIKE '%TRANSFERENCIA%' THEN 'TRANSFERENCIA'
     WHEN descripcion LIKE '%MARCA POR VIAJE%' THEN 'MARCA POR VIAJE'
     WHEN descripcion LIKE '%VENTA%' THEN 'VENTA'
     WHEN descripcion LIKE '%BAJA%' THEN 'BAJA'
     WHEN descripcion LIKE '%INVERSIONES%' THEN 'INVERSIONES'
     WHEN descripcion LIKE '%PRESTAMO%' THEN 'PRESTAMOS'
     WHEN descripcion LIKE '%PRENDARIO%' THEN 'PRESTAMOS'
     WHEN descripcion LIKE '%PROMO%' THEN 'PROMOCIONES'
     WHEN descripcion LIKE '%SEGUROS%' THEN 'SEGUROS'
     ELSE 'OTROS'
END as ds_nps_descripcion,
ubicacion as ds_nps_desc_ubicacion,
edad as ds_per_edad,
regexp_replace(ejecutivo, 'IVR', 'S/D') as ds_nps_ejecutivo,
regexp_replace(legajo, 'IVR', 'S/D') as ds_nps_legajo,
UCASE(mejor_producto) as ds_nps_mejor_prod,
ucase(if (ucase(momento_critico) LIKE '%CONTACT CENTER','CONTACT CENTER',if(momento_critico is null,'CONTACT CENTER', momento_Critico))) as ds_nps_momento_critico,
ucase(negocio) as ds_nps_negocio,
nombre as ds_per_nombre,
ucase(nombre_sucursal) as ds_nps_nombre_sucursal,
CASE
   WHEN UCASE (renta) LIKE 'BANCA PRIVADA%' THEN 'BANCA PRIVADA'
   WHEN UCASE (renta) LIKE 'RENTA ALTA SUPERIOR%' THEN 'SELECT'
   WHEN UCASE (renta) LIKE 'RENTA ALTA%' THEN 'ALTA'
   WHEN UCASE (renta) LIKE 'RENTA MEDIA%' THEN 'MEDIA'
   WHEN UCASE (renta) LIKE 'RENTA MASIVA%' THEN 'MASIVA'
   WHEN UCASE (renta) LIKE 'INSTITUCION%' THEN 'INSTITUCION'
   WHEN UCASE (renta) LIKE 'PYME 1%' THEN 'PYME 1'
   WHEN UCASE (renta) LIKE 'PYME 2%' THEN 'PYME 2'
   WHEN UCASE (renta) LIKE 'COMERCIO%' THEN 'COMERCIO'
   WHEN UCASE (renta) LIKE 'EMPRESA%' THEN 'EMPRESA'
   WHEN UCASE (renta) LIKE '%GRAN%EMPRESA%' THEN 'GRAN EMPRESA'
   ELSE 'VER'
END as ds_nps_renta,
rentabilidad as ds_nps_rentabilidad,
ucase(segmento) as ds_nps_segmento,
sexo as ds_per_sexo,
tipo_doc as ds_per_tipo_doc,
'contact center' as ds_nps_tipo_encuesta,
to_date(from_unixtime(UNIX_TIMESTAMP(partition_date, 'yyyy-MM-dd'))) as dt_nps_fecha_encuesta,
antiguedad_en_meses as ds_nps_antiguedad_meses,
rank() over (partition by response_id order by question_id asc) as ds_nps_orden,
question_desc as ds_nps_pregunta,
respuesta as ds_nps_respuesta,id_respuesta,
partition_date
from bi_corp_staging.qualtrics_nps_cont_cent
where partition_date='{{ ti.xcom_pull(task_ids='InputConfig', key='partition_date', dag_id='LOAD_CMN_NPS-Daily') }}'
AND nup IS NOT NULL


union all

Select
lpad(nup,8,0) as cod_per_nup,
response_id as cod_nps_respuesta,
lpad(nro_doc,11,0) as cod_per_nro_doc,
IF(nro_ubicacion like 'A%',ubicacion,if (nro_ubicacion like 'B%',ubicacion,nro_ubicacion)) cod_nps_nro_ubicacion,
CASE WHEN  cast(red_social_nps as int) BETWEEN 0 AND 6 THEN 1
 	WHEN cast(red_social_nps as int) BETWEEN 7 AND 8 THEN 2
	WHEN cast(red_social_nps as int) BETWEEN 9 AND 10 THEN 3
else cast(null as string)
end as ds_nps_agrupados,
ucase(if (ucase(momento_critico) LIKE '%CONTACT CENTER','CONTACT CENTER',if(momento_critico is null,'CONTACT CENTER', momento_Critico))) as ds_nps_canal,
CASE
     WHEN descripcion LIKE '%INFORMACION%' THEN 'ASESORAMIENTO'
     WHEN descripcion LIKE '%TRANSFERENCIA%' THEN 'TRANSFERENCIA'
     WHEN descripcion LIKE '%MARCA POR VIAJE%' THEN 'MARCA POR VIAJE'
     WHEN descripcion LIKE '%VENTA%' THEN 'VENTA'
     WHEN descripcion LIKE '%BAJA%' THEN 'BAJA'
     WHEN descripcion LIKE '%INVERSIONES%' THEN 'INVERSIONES'
     WHEN descripcion LIKE '%PRESTAMO%' THEN 'PRESTAMOS'
     WHEN descripcion LIKE '%PRENDARIO%' THEN 'PRESTAMOS'
     WHEN descripcion LIKE '%PROMO%' THEN 'PROMOCIONES'
     WHEN descripcion LIKE '%SEGUROS%' THEN 'SEGUROS'
     ELSE 'OTROS'
END as ds_nps_descripcion,
ubicacion as ds_nps_desc_ubicacion,
edad as ds_per_edad,
regexp_replace(ejecutivo, 'IVR', 'S/D') as ds_nps_ejecutivo,
regexp_replace(legajo, 'IVR', 'S/D') as ds_nps_legajo,
UCASE(mejor_producto) as ds_nps_mejor_prod,
ucase(if (ucase(momento_critico) LIKE '%CONTACT CENTER','CONTACT CENTER',if(momento_critico is null,'CONTACT CENTER', momento_Critico))) as ds_nps_momento_critico,
ucase(negocio) as ds_nps_negocio,
nombre as ds_per_nombre,
ucase(nombre_sucursal) as ds_nps_nombre_sucursal,
CASE
   WHEN UCASE (renta) LIKE 'BANCA PRIVADA%' THEN 'BANCA PRIVADA'
   WHEN UCASE (renta) LIKE 'RENTA ALTA SUPERIOR%' THEN 'SELECT'
   WHEN UCASE (renta) LIKE 'RENTA ALTA%' THEN 'ALTA'
   WHEN UCASE (renta) LIKE 'RENTA MEDIA%' THEN 'MEDIA'
   WHEN UCASE (renta) LIKE 'RENTA MASIVA%' THEN 'MASIVA'
   WHEN UCASE (renta) LIKE 'INSTITUCION%' THEN 'INSTITUCION'
   WHEN UCASE (renta) LIKE 'PYME 1%' THEN 'PYME 1'
   WHEN UCASE (renta) LIKE 'PYME 2%' THEN 'PYME 2'
   WHEN UCASE (renta) LIKE 'COMERCIO%' THEN 'COMERCIO'
   WHEN UCASE (renta) LIKE 'EMPRESA%' THEN 'EMPRESA'
   WHEN UCASE (renta) LIKE '%GRAN%EMPRESA%' THEN 'GRAN EMPRESA'
   ELSE 'VER'
END as ds_nps_renta,
rentabilidad as ds_nps_rentabilidad,
ucase(segmento) as ds_nps_segmento,
sexo as ds_per_sexo,
tipo_doc as ds_per_tipo_doc,
'contact center rs' as ds_nps_tipo_encuesta,
to_date(from_unixtime(UNIX_TIMESTAMP(partition_date, 'yyyy-MM-dd'))) as dt_nps_fecha_encuesta,
antiguedad_en_meses as ds_nps_antiguedad_meses,
rank() over (partition by response_id order by if(question_id='Q3','Q4',if(question_id='Q4','Q3',question_id)) desc) as ds_nps_orden,
question_desc as ds_nps_pregunta,
respuesta as ds_nps_respuesta,id_respuesta,
partition_date
from bi_corp_staging.qualtrics_nps_cont_cent_rs
where partition_date='{{ ti.xcom_pull(task_ids='InputConfig', key='partition_date', dag_id='LOAD_CMN_NPS-Daily') }}'
AND nup IS NOT NULL


union all

Select
lpad(nup,8,0) as cod_per_nup,
responseid as cod_nps_respuesta,
lpad(nro_doc,11,0) as cod_per_nro_doc,
IF(nro_ubicacion like 'A%',ubicacion,if (nro_ubicacion like 'B%',ubicacion,nro_ubicacion)) cod_nps_nro_ubicacion,
q1_nps_group as ds_nps_agrupados,
ucase(if (ucase(momento_critico) LIKE '%CONTACT CENTER','CONTACT CENTER',if(momento_critico is null,'CONTACT CENTER', momento_Critico))) as ds_nps_canal,
CASE
     WHEN descripcion LIKE '%INFORMACION%' THEN 'ASESORAMIENTO'
     WHEN descripcion LIKE '%TRANSFERENCIA%' THEN 'TRANSFERENCIA'
     WHEN descripcion LIKE '%MARCA POR VIAJE%' THEN 'MARCA POR VIAJE'
     WHEN descripcion LIKE '%VENTA%' THEN 'VENTA'
     WHEN descripcion LIKE '%BAJA%' THEN 'BAJA'
     WHEN descripcion LIKE '%INVERSIONES%' THEN 'INVERSIONES'
     WHEN descripcion LIKE '%PRESTAMO%' THEN 'PRESTAMOS'
     WHEN descripcion LIKE '%PRENDARIO%' THEN 'PRESTAMOS'
     WHEN descripcion LIKE '%PROMO%' THEN 'PROMOCIONES'
     WHEN descripcion LIKE '%SEGUROS%' THEN 'SEGUROS'
     ELSE 'OTROS'
END as ds_nps_descripcion,
ubicacion as ds_nps_desc_ubicacion,
edad as ds_per_edad,
ejecutivo as ds_nps_ejecutivo,
legajo as ds_nps_legajo,
ucase(mejor_producto) as ds_nps_mejor_prod,
ucase(if (ucase(momento_critico) LIKE '%CONTACT CENTER','CONTACT CENTER',if(momento_critico is null,'CONTACT CENTER', momento_Critico))) as ds_nps_momento_critico,
ucase(negocio) as ds_nps_negocio,
ucase(nombre) as ds_per_nombre,
ucase(nombre_sucursal) as ds_nps_nombre_sucursal,
CASE
   WHEN UCASE (renta) LIKE 'BANCA PRIVADA%' THEN 'BANCA PRIVADA'
   WHEN UCASE (renta) LIKE 'RENTA ALTA SUPERIOR%' THEN 'SELECT'
   WHEN UCASE (renta) LIKE 'RENTA ALTA%' THEN 'ALTA'
   WHEN UCASE (renta) LIKE 'RENTA MEDIA%' THEN 'MEDIA'
   WHEN UCASE (renta) LIKE 'RENTA MASIVA%' THEN 'MASIVA'
   WHEN UCASE (renta) LIKE 'INSTITUCION%' THEN 'INSTITUCION'
   WHEN UCASE (renta) LIKE 'PYME 1%' THEN 'PYME 1'
   WHEN UCASE (renta) LIKE 'PYME 2%' THEN 'PYME 2'
   WHEN UCASE (renta) LIKE 'COMERCIO%' THEN 'COMERCIO'
   WHEN UCASE (renta) LIKE 'EMPRESA%' THEN 'EMPRESA'
   WHEN UCASE (renta) LIKE '%GRAN%EMPRESA%' THEN 'GRAN EMPRESA'
   ELSE 'VER'
END as ds_nps_renta,
rentabilidad as ds_nps_rentabilidad,
segmento as ds_nps_segmento,
sexo as ds_per_sexo,
tipo_doc as ds_per_tipo_doc,
'contact center pymes' as ds_nps_tipo_encuesta,
to_date(from_unixtime(UNIX_TIMESTAMP(partition_date, 'yyyy-MM-dd'))) as dt_nps_fecha_encuesta,
antiguedad_en_meses as ds_nps_antiguedad_meses,
rank() over (partition by responseid order by question_id asc) as ds_nps_orden,
question_desc as ds_nps_pregunta,
respuesta as ds_nps_respuesta,id_respuesta,
partition_date
from bi_corp_staging.qualtrics_nps_cont_cent_pym
where partition_date='{{ ti.xcom_pull(task_ids='InputConfig', key='partition_date', dag_id='LOAD_CMN_NPS-Daily') }}'
AND nup IS NOT NULL


union all


Select
lpad(nup,8,0) as cod_per_nup,
responseid as cod_nps_respuesta,
lpad(nro_doc,11,0) as cod_per_nro_doc,
ubicacion as cod_nps_nro_ubicacion,
q1_nps_group as ds_nps_agrupados,
case
	when ubicacion like 'SELECT ONLINE%' then 'EJEC. REMOTO SELECT'
	when ubicacion like 'EQUIPO%' then 'EJEC. REMOTO'
	when canal='CONTAC CENTER' then 'CONTACT CENTER'
	when canal='SUCURSAL' then 'SUCURSAL'
else 'CONTACT CENTER'
end as ds_nps_canal,
UCASE(categoria_descripcion) as ds_nps_descripcion,
ubicacion as ds_nps_desc_ubicacion,
edad as ds_per_edad,
ucase(apellido) as ds_nps_ejecutivo,
legajo as ds_nps_legajo,
cast(null as string) as ds_nps_mejor_prod,
UCASE(momento_critico) as ds_nps_momento_critico,
cast(null as string) as ds_nps_negocio,
nombre as ds_per_nombre,
ucase(nombre_sucursal) as ds_nps_nombre_sucursal,
CASE
   WHEN UCASE (renta) LIKE 'BANCA PRIVADA%' THEN 'BANCA PRIVADA'
   WHEN UCASE (renta) LIKE 'RENTA ALTA SUPERIOR%' THEN 'SELECT'
   WHEN UCASE (renta) LIKE 'RENTA ALTA%' THEN 'ALTA'
   WHEN UCASE (renta) LIKE 'RENTA MEDIA%' THEN 'MEDIA'
   WHEN UCASE (renta) LIKE 'RENTA MASIVA%' THEN 'MASIVA'
   WHEN UCASE (renta) LIKE 'INSTITUCION%' THEN 'INSTITUCION'
   WHEN UCASE (renta) LIKE 'PYME 1%' THEN 'PYME 1'
   WHEN UCASE (renta) LIKE 'PYME 2%' THEN 'PYME 2'
   WHEN UCASE (renta) LIKE 'COMERCIO%' THEN 'COMERCIO'
   WHEN UCASE (renta) LIKE 'EMPRESA%' THEN 'EMPRESA'
   WHEN UCASE (renta) LIKE '%GRAN%EMPRESA%' THEN 'GRAN EMPRESA'
   ELSE 'VER'
END as ds_nps_renta,
rentabilidad as ds_nps_rentabilidad,
cast(null as string) as ds_nps_segmento,
sexo as ds_per_sexo,
tipo_doc as ds_per_tipo_doc,
'cosmos' as ds_nps_tipo_encuesta,
to_date(from_unixtime(UNIX_TIMESTAMP(partition_date, 'yyyy-MM-dd'))) as dt_nps_fecha_encuesta,
antiguedad_en_meses as ds_nps_antiguedad_meses,
rank() over (partition by responseid order by question_id asc) as ds_nps_orden,
question_desc as ds_nps_pregunta,
respuesta as ds_nps_respuesta,id_respuesta,
partition_date
from bi_corp_staging.qualtrics_nps_cosmos
where partition_date='{{ ti.xcom_pull(task_ids='InputConfig', key='partition_date', dag_id='LOAD_CMN_NPS-Daily') }}'
AND nup IS NOT NULL


union all

Select
lpad(nup,8,0) as cod_per_nup,
responseid as cod_nps_respuesta,
lpad(nro_doc,11,0) as cod_per_nro_doc,
grupo_usuario as cod_nps_nro_ubicacion,
q1_nps_group as ds_nps_agrupados,
case
    when UCASE(negocio) like '%VIP%' then 'EJEC. REMOTO VIP'
    when UCASE(negocio) not like '%VIP%' and UCASE(negocio) like '%DUO%' then 'EJEC. REMOTO DUO'
    when UCASE(negocio) not like '%VIP%' and UCASE(negocio) not like '%DUO%' and UCASE(negocio) like '%AGRO%' then 'EJEC. REMOTO AGRO'
    when UCASE(negocio) not like '%VIP%' and UCASE(negocio) not like '%DUO%' and UCASE(negocio) not like '%AGRO%' and UCASE(negocio) like  '%PYME %'then 'EJEC. REMOTO PYME'
    when UCASE(negocio) not like '%VIP%' and UCASE(negocio) not like '%DUO%' and UCASE(negocio) not like '%AGRO%' and UCASE(negocio) NOT like  '%PYME %' and UCASE(negocio) like '%SELECT%' then 'EJEC. REMOTO SELECT'
    when UCASE(negocio) not like '%VIP%' and UCASE(negocio) not like '%DUO%' and UCASE(negocio) not like '%AGRO%' and UCASE(negocio) NOT like  '%PYME %' and UCASE(negocio) not like '%SELECT%' and sector = 'CONTACT CENTER' then 'CONTACT CENTER'
    else 'EJEC. REMOTO SELECT'
end as ds_nps_canal,
CASE
   WHEN descripcion LIKE '%INFORMACION%' THEN 'ASESORAMIENTO'
   WHEN descripcion LIKE '%MARCA POR VIAJE%' THEN 'MARCA POR VIAJE'
   WHEN descripcion LIKE '%VENTA%' THEN 'VENTA'
   WHEN descripcion LIKE '%BAJA%' THEN 'BAJA'
   ELSE 'CONSULTA'
END as ds_nps_descripcion,
cast(null as string) as ds_nps_desc_ubicacion,
edad as ds_per_edad,
usuario as ds_nps_ejecutivo,
cod_usuario as ds_nps_legajo,
mejor_producto as ds_nps_mejor_prod,
'EJECUTIVO REMOTO' as ds_nps_momento_critico,
ucase(negocio) as ds_nps_negocio,
ucase(nombre) as ds_per_nombre,
ucase(nombre_sucursal) as ds_nps_nombre_sucursal,
case when UCASE(renta) like 'BANCA PRIVADA%' then 'BANCA PRIVADA'
     when UCASE(renta) like 'RENTA ALTA SUPERIOR%' then 'SELECT'
     when UCASE(renta) like 'RENTA ALTA%' then 'ALTA'
     when UCASE(renta) like 'RENTA MEDIA%' then 'MEDIA'
     when UCASE(renta) like 'RENTA MASIVA%' then 'MASIVA'
     when UCASE(renta) like 'INSTITUCION%' then 'INSTITUCION'
     when UCASE(renta) like 'COMERCIO%' then 'COMERCIO'
     WHEN UCASE (renta) LIKE 'PYME 2%' THEN 'PYME 2'
     WHEN UCASE (renta) LIKE 'PYME 1%' THEN 'PYME 1'
     when UCASE(renta) like 'EMPRESA%' then 'EMPRESA'
     WHEN UCASE (renta) LIKE '%GRAN%EMPRESA%' THEN 'GRAN EMPRESA'
     else 'VER'
end as ds_nps_renta,
rentabilidad as ds_nps_rentabilidad,
cast(null as string) as ds_nps_segmento,
sexo as ds_per_sexo,
tipo_doc as ds_per_tipo_doc,
'ejecutivo select' as ds_nps_tipo_encuesta,
to_date(from_unixtime(UNIX_TIMESTAMP(partition_date, 'yyyy-MM-dd'))) as dt_nps_fecha_encuesta,
antiguedad_en_meses as ds_nps_antiguedad_meses,
rank() over (partition by responseid order by question_id asc) as ds_nps_orden,
question_desc as ds_nps_pregunta,
respuesta as ds_nps_respuesta,id_respuesta,
partition_date
from bi_corp_staging.qualtrics_nps_ejecutivo_select
where partition_date='{{ ti.xcom_pull(task_ids='InputConfig', key='partition_date', dag_id='LOAD_CMN_NPS-Daily') }}'
AND nup IS NOT NULL


union all

Select
lpad(nup,8,0) as cod_per_nup,
responseid as cod_nps_respuesta,
lpad(nro_doc,11,0) as cod_per_nro_doc,
if (ucase(nro_ubicacion) like '/FUERZA COMERCIAL', 'FUERZA COMERCIAL', IF(nro_ubicacion like 'A%',ubicacion,if (nro_ubicacion like 'B%',ubicacion,UCASE(nro_ubicacion)))) as cod_nps_nro_ubicacion,
q1_nps_group as ds_nps_agrupados,
case
    when UCASE(negociodesc) like '%VIP%' then 'EJEC. REMOTO VIP'
    when UCASE(negociodesc) not like '%VIP%' and UCASE(negociodesc) like '%DUO%' then 'EJEC. REMOTO DUO'
    when UCASE(negociodesc) not like '%VIP%' and UCASE(negociodesc) not like '%DUO%' and UCASE(negociodesc) like '%AGRO%' then 'EJEC. REMOTO AGRO'
    when UCASE(negociodesc) not like '%VIP%' and UCASE(negociodesc) not like '%DUO%' and UCASE(negociodesc) not like '%AGRO%' and UCASE(negociodesc) like '%SELECT%' then 'EJEC. REMOTO SELECT'
    when UCASE(nro_ubicacion) like '%SYSTEM%' then 'CONTACT CENTER'
    when UCASE(nro_ubicacion) like '%FUERZA COMERCIAL%' then 'FUERZA COMERCIAL'
    when UCASE(canal) like 'SUCURSAL' and (UCASE(negociodesc) like '%TLMK OUT MA¿ANA%' or UCASE(negociodesc) like '%TLMK IN MA¿ANA%') and ucase(nro_ubicacion) like '%GRUPO%'  then 'CONTACT CENTER'
    when UCASE(canal) like 'SUCURSAL' and ucase(nro_ubicacion) like '%SISTEMAS ESTRATEGICOS%'  then 'CONTACT CENTER'
    when UCASE(canal) like 'SUCURSAL' and ucase(nro_ubicacion) like '%SIN GESTION%' and UCASE(negociodesc) like '%GONZALEZ V'  then 'CONTACT CENTER'
    when UCASE(canal) is null then 'OTROS'
    else UCASE(canal)
end as ds_nps_canal,
concat(if (tipo_evento = 'Update', 'UPGRADE ', IF (tipo_evento IS NULL, 'OTROS', UCASE(tipo_evento))) , ' ',
if (b.nombre is null, ' ', ucase(b.nombre ))) as ds_nps_descripcion,
ubicacion as ds_nps_desc_ubicacion,
edad as ds_per_edad,
ejecutivo as ds_nps_ejecutivo,
legajo as ds_nps_legajo,
cast(null as string) as ds_nps_mejor_prod,
'ONBOARDING DIG' as ds_nps_momento_critico,
cast(null as string) as ds_nps_negocio,
onb.nombre as ds_per_nombre,
cast(null as string) as ds_nps_nombre_sucursal,
CASE
   WHEN UCASE (renta) LIKE 'BANCA PRIVADA%' THEN 'BANCA PRIVADA'
   WHEN UCASE (renta) LIKE 'RENTA ALTA SUPERIOR%' THEN 'SELECT'
   WHEN UCASE (renta) LIKE 'RENTA ALTA%' THEN 'ALTA'
   WHEN UCASE (renta) LIKE 'RENTA MEDIA%' THEN 'MEDIA'
   WHEN UCASE (renta) LIKE 'RENTA MASIVA%' THEN 'MASIVA'
   WHEN UCASE (renta) LIKE 'INSTITUCION%' THEN 'INSTITUCION'
   WHEN UCASE (renta) LIKE 'PYME 1%' THEN 'PYME 1'
   WHEN UCASE (renta) LIKE 'PYME 2%' THEN 'PYME 2'
   WHEN UCASE (renta) LIKE 'COMERCIO%' THEN 'COMERCIO'
   WHEN UCASE (renta) LIKE 'EMPRESA%' THEN 'EMPRESA'
   WHEN UCASE (renta) LIKE '%GRAN%EMPRESA%' THEN 'GRAN EMPRESA'
   ELSE 'VER'
END as ds_nps_renta,
rentabilidad as ds_nps_rentabilidad,
ucase(segmento) as ds_nps_segmento,
sexo as ds_per_sexo,
tipo_doc as ds_per_tipo_doc,
'onboarding digital' as ds_nps_tipo_encuesta,
to_date(from_unixtime(UNIX_TIMESTAMP(onb.partition_date, 'yyyy-MM-dd'))) as dt_nps_fecha_encuesta,
antiguedad_en_meses as ds_nps_antiguedad_meses,
rank() over (partition by responseid order by question_id asc) as ds_nps_orden,
question_desc as ds_nps_pregunta,
respuesta as ds_nps_respuesta,id_respuesta,
onb.partition_date
from bi_corp_staging.qualtrics_nps_onboard_dig as onb
left join bi_corp_staging.exa_dim_productos as b
       ON (onb.producto_contab = b.producto
       AND onb.subprod_contab = b.subproducto
       and b.partition_date='{{ ti.xcom_pull(task_ids='InputConfig', key='partition_date', dag_id='LOAD_CMN_NPS-Daily') }}')
where onb.partition_date='{{ ti.xcom_pull(task_ids='InputConfig', key='partition_date', dag_id='LOAD_CMN_NPS-Daily') }}'
AND nup IS NOT NULL


UNION all

Select
lpad(nup,8,0) as cod_per_nup,
responseid as cod_nps_respuesta,
lpad(nro_doc,11,0) as cod_per_nro_doc,
if (ucase(nro_ubicacion) like '/FUERZA COMERCIAL', 'FUERZA COMERCIAL', IF(nro_ubicacion like 'A%',ubicacion,if (nro_ubicacion like 'B%',ubicacion,UCASE(nro_ubicacion)))) as cod_nps_nro_ubicacion,
q1_nps_group as ds_nps_agrupados,
case
    when UCASE(negociodesc) like '%VIP%' then 'EJEC. REMOTO VIP'
    when UCASE(negociodesc) not like '%VIP%' and UCASE(negociodesc) like '%DUO%' then 'EJEC. REMOTO DUO'
    when UCASE(negociodesc) not like '%VIP%' and UCASE(negociodesc) not like '%DUO%' and UCASE(negociodesc) like '%AGRO%' then 'EJEC. REMOTO AGRO'
    when UCASE(negociodesc) not like '%VIP%' and UCASE(negociodesc) not like '%DUO%' and UCASE(negociodesc) not like '%AGRO%' and UCASE(negociodesc) like '%SELECT%' then 'EJEC. REMOTO SELECT'
    when UCASE(nro_ubicacion) like '%SYSTEM%' then 'CONTACT CENTER'
    when UCASE(nro_ubicacion) like '%FUERZA COMERCIAL%' then 'FUERZA COMERCIAL'
    when UCASE(canal) like 'SUCURSAL' and (UCASE(negociodesc) like '%TLMK OUT MA¿ANA%' or UCASE(negociodesc) like '%TLMK IN MA¿ANA%') and ucase(nro_ubicacion) like '%GRUPO%'  then 'CONTACT CENTER'
    when UCASE(canal) like 'SUCURSAL' and ucase(nro_ubicacion) like '%SISTEMAS ESTRATEGICOS%'  then 'CONTACT CENTER'
    when UCASE(canal) like 'SUCURSAL' and ucase(nro_ubicacion) like '%SIN GESTION%' and UCASE(negociodesc) like '%GONZALEZ V'  then 'CONTACT CENTER'
    when UCASE(canal) is null then 'OTROS'
    else UCASE(canal)
end as ds_nps_canal,
concat(if (tipo_evento = 'Update', 'UPGRADE ', IF (tipo_evento IS NULL, 'OTROS', UCASE(tipo_evento))) , ' ',
if (b.nombre is null, ' ', ucase(b.nombre ))) as ds_nps_descripcion,
if (ubicacion= '/Fuerza Comercial','FUERZA COMERCIAL',ubicacion) as ds_nps_desc_ubicacion,
edad as ds_per_edad,
ejecutivo as ds_nps_ejecutivo,
legajo as ds_nps_legajo,
cast(null as string) as ds_nps_mejor_prod,
'ONBOARDING DIG DELIVERY' as ds_nps_momento_critico,
cast(null as string) as ds_nps_negocio,
onb.nombre as ds_per_nombre,
cast(null as string) as ds_nps_nombre_sucursal,
CASE
   WHEN UCASE (renta) LIKE 'BANCA PRIVADA%' THEN 'BANCA PRIVADA'
   WHEN UCASE (renta) LIKE 'RENTA ALTA SUPERIOR%' THEN 'SELECT'
   WHEN UCASE (renta) LIKE 'RENTA ALTA%' THEN 'ALTA'
   WHEN UCASE (renta) LIKE 'RENTA MEDIA%' THEN 'MEDIA'
   WHEN UCASE (renta) LIKE 'RENTA MASIVA%' THEN 'MASIVA'
   WHEN UCASE (renta) LIKE 'INSTITUCION%' THEN 'INSTITUCION'
   WHEN UCASE (renta) LIKE 'PYME 1%' THEN 'PYME 1'
   WHEN UCASE (renta) LIKE 'PYME 2%' THEN 'PYME 2'
   WHEN UCASE (renta) LIKE 'COMERCIO%' THEN 'COMERCIO'
   WHEN UCASE (renta) LIKE 'EMPRESA%' THEN 'EMPRESA'
   WHEN UCASE (renta) LIKE '%GRAN%EMPRESA%' THEN 'GRAN EMPRESA'
   ELSE 'VER'
END as ds_nps_renta,
rentabilidad as ds_nps_rentabilidad,
segmento as ds_nps_segmento,
sexo as ds_per_sexo,
tipo_doc as ds_per_tipo_doc,
'onboarding digital delivery' as ds_nps_tipo_encuesta,
to_date(from_unixtime(UNIX_TIMESTAMP(onb.partition_date, 'yyyy-MM-dd'))) as dt_nps_fecha_encuesta,
antiguedad_en_meses as ds_nps_antiguedad_meses,
rank() over (partition by responseid order by question_id asc) as ds_nps_orden,
question_desc as ds_nps_pregunta,
respuesta as ds_nps_respuesta,id_respuesta,
onb.partition_date
from bi_corp_staging.qualtrics_nps_onboard_dig_dlv as onb
left join bi_corp_staging.exa_dim_productos as b
       ON (onb.producto_contab = b.producto
       AND onb.subprod_contab = b.subproducto
       and b.partition_date='{{ ti.xcom_pull(task_ids='InputConfig', key='partition_date', dag_id='LOAD_CMN_NPS-Daily') }}')
where onb.partition_date='{{ ti.xcom_pull(task_ids='InputConfig', key='partition_date', dag_id='LOAD_CMN_NPS-Daily') }}'
AND nup IS NOT NULL


union all

Select
lpad(nup,8,0) as cod_per_nup,
responseid as cod_nps_respuesta,
lpad(nro_doc,11,0) as cod_per_nro_doc,
if (ucase(nro_ubicacion) like '/FUERZA COMERCIAL', 'FUERZA COMERCIAL', IF(nro_ubicacion like 'A%',ubicacion,if (nro_ubicacion like 'B%',ubicacion,UCASE(nro_ubicacion)))) as cod_nps_nro_ubicacion,
q1_nps_group as ds_nps_agrupados,
case
    when UCASE(negociodesc) like '%VIP%' then 'EJEC. REMOTO VIP'
    when UCASE(negociodesc) not like '%VIP%' and UCASE(negociodesc) like '%DUO%' then 'EJEC. REMOTO DUO'
    when UCASE(negociodesc) not like '%VIP%' and UCASE(negociodesc) not like '%DUO%' and UCASE(negociodesc) like '%AGRO%' then 'EJEC. REMOTO AGRO'
    when UCASE(negociodesc) not like '%VIP%' and UCASE(negociodesc) not like '%DUO%' and UCASE(negociodesc) not like '%AGRO%' and UCASE(negociodesc) like '%SELECT%' then 'EJEC. REMOTO SELECT'
    when UCASE(nro_ubicacion) like '%SYSTEM%' then 'CONTACT CENTER'
    when UCASE(nro_ubicacion) like '%FUERZA COMERCIAL%' then 'FUERZA COMERCIAL'
    when UCASE(canal) like 'SUCURSAL' and (UCASE(negociodesc) like '%TLMK OUT MA¿ANA%' or UCASE(negociodesc) like '%TLMK IN MA¿ANA%') and ucase(nro_ubicacion) like '%GRUPO%'  then 'CONTACT CENTER'
    when UCASE(canal) like 'SUCURSAL' and ucase(nro_ubicacion) like '%SISTEMAS ESTRATEGICOS%'  then 'CONTACT CENTER'
    when UCASE(canal) like 'SUCURSAL' and ucase(nro_ubicacion) like '%SIN GESTION%' and UCASE(negociodesc) like '%GONZALEZ V'  then 'CONTACT CENTER'
    when UCASE(canal) is null then 'OTROS'
    else UCASE(canal)
end as ds_nps_canal,
concat(if (tipo_evento = 'Update', 'UPGRADE ', IF (tipo_evento IS NULL, 'OTROS', UCASE(tipo_evento))) , ' ',
if (b.nombre is null, ' ', ucase(b.nombre ))) as ds_nps_descripcion,
if (ubicacion= '/Fuerza Comercial','FUERZA COMERCIAL',ubicacion) as ds_nps_desc_ubicacion,
edad as ds_per_edad,
ejecutivo as ds_nps_ejecutivo,
legajo as ds_nps_legajo,
cast(null as string) as ds_nps_mejor_prod,
'ONBOARDING' as ds_nps_momento_critico,
cast(null as string) as ds_nps_negocio,
onb.nombre as ds_per_nombre,
cast(null as string) as ds_nps_nombre_sucursal,
CASE
   WHEN UCASE (renta) LIKE 'BANCA PRIVADA%' THEN 'BANCA PRIVADA'
   WHEN UCASE (renta) LIKE 'RENTA ALTA SUPERIOR%' THEN 'SELECT'
   WHEN UCASE (renta) LIKE 'RENTA ALTA%' THEN 'ALTA'
   WHEN UCASE (renta) LIKE 'RENTA MEDIA%' THEN 'MEDIA'
   WHEN UCASE (renta) LIKE 'RENTA MASIVA%' THEN 'MASIVA'
   WHEN UCASE (renta) LIKE 'INSTITUCION%' THEN 'INSTITUCION'
   WHEN UCASE (renta) LIKE 'PYME 1%' THEN 'PYME 1'
   WHEN UCASE (renta) LIKE 'PYME 2%' THEN 'PYME 2'
   WHEN UCASE (renta) LIKE 'COMERCIO%' THEN 'COMERCIO'
   WHEN UCASE (renta) LIKE 'EMPRESA%' THEN 'EMPRESA'
   WHEN UCASE (renta) LIKE '%GRAN%EMPRESA%' THEN 'GRAN EMPRESA'
   ELSE 'VER'
END as ds_nps_renta,
rentabilidad as ds_nps_rentabilidad,
ucase(segmento) as ds_nps_segmento,
sexo as ds_per_sexo,
tipo_doc as ds_per_tipo_doc,
'onboarding' as ds_nps_tipo_encuesta,
to_date(from_unixtime(UNIX_TIMESTAMP(onb.partition_date, 'yyyy-MM-dd'))) as dt_nps_fecha_encuesta,
antiguedad_en_meses as ds_nps_antiguedad_meses,
rank() over (partition by responseid order by question_id asc) as ds_nps_orden,
question_desc as ds_nps_pregunta,
respuesta as ds_nps_respuesta,id_respuesta,
onb.partition_date
from bi_corp_staging.qualtrics_nps_onboarding as onb
left join bi_corp_staging.exa_dim_productos as b
       ON (onb.producto_contab = b.producto
       AND onb.subprod_contab = b.subproducto
       and b.partition_date='{{ ti.xcom_pull(task_ids='InputConfig', key='partition_date', dag_id='LOAD_CMN_NPS-Daily') }}')
where onb.partition_date='{{ ti.xcom_pull(task_ids='InputConfig', key='partition_date', dag_id='LOAD_CMN_NPS-Daily') }}'
AND nup IS NOT NULL


union all

Select
lpad(nup,8,0) as cod_per_nup,
responseid as cod_nps_respuesta,
lpad(nro_doc,11,0) as cod_per_nro_doc,
if (ucase(nro_ubicacion) like '/FUERZA COMERCIAL', 'FUERZA COMERCIAL', IF(nro_ubicacion like 'A%',ubicacion,if (nro_ubicacion like 'B%',ubicacion,UCASE(nro_ubicacion)))) as cod_nps_nro_ubicacion,
q1_nps_group as ds_nps_agrupados,
case
    when UCASE(negociodesc) like '%VIP%' then 'EJEC. REMOTO VIP'
    when UCASE(negociodesc) not like '%VIP%' and UCASE(negociodesc) like '%DUO%' then 'EJEC. REMOTO DUO'
    when UCASE(negociodesc) not like '%VIP%' and UCASE(negociodesc) not like '%DUO%' and UCASE(negociodesc) like '%AGRO%' then 'EJEC. REMOTO AGRO'
    when UCASE(negociodesc) not like '%VIP%' and UCASE(negociodesc) not like '%DUO%' and UCASE(negociodesc) not like '%AGRO%' and UCASE(negociodesc) like '%SELECT%' then 'EJEC. REMOTO SELECT'
    when UCASE(nro_ubicacion) like '%SYSTEM%' then 'CONTACT CENTER'
    when UCASE(nro_ubicacion) like '%FUERZA COMERCIAL%' then 'FUERZA COMERCIAL'
    when UCASE(canal) like 'SUCURSAL' and (UCASE(negociodesc) like '%TLMK OUT MA¿ANA%' or UCASE(negociodesc) like '%TLMK IN MA¿ANA%') and ucase(nro_ubicacion) like '%GRUPO%'  then 'CONTACT CENTER'
    when UCASE(canal) like 'SUCURSAL' and ucase(nro_ubicacion) like '%SISTEMAS ESTRATEGICOS%'  then 'CONTACT CENTER'
    when UCASE(canal) like 'SUCURSAL' and ucase(nro_ubicacion) like '%SIN GESTION%' and UCASE(negociodesc) like '%GONZALEZ V'  then 'CONTACT CENTER'
    when UCASE(canal) is null then 'OTROS'
    else UCASE(canal)
end as ds_nps_canal,
concat(if (tipo_evento = 'Update', 'UPGRADE ', IF (tipo_evento IS NULL, 'OTROS', UCASE(tipo_evento))) , ' ',
if (b.nombre is null, ' ', ucase(b.nombre ))) as ds_nps_descripcion,
if (ubicacion= '/Fuerza Comercial','FUERZA COMERCIAL',ubicacion) as ds_nps_desc_ubicacion,
edad as ds_per_edad,
ejecutivo as ds_nps_ejecutivo,
legajo as ds_nps_legajo,
cast(null as string) as ds_nps_mejor_prod,
'ONBOARDING DELIVERY' as ds_nps_momento_critico,
cast(null as string) as ds_nps_negocio,
onb.nombre as ds_per_nombre,
cast(null as string) as ds_nps_nombre_sucursal,
CASE
   WHEN UCASE (renta) LIKE 'BANCA PRIVADA%' THEN 'BANCA PRIVADA'
   WHEN UCASE (renta) LIKE 'RENTA ALTA SUPERIOR%' THEN 'SELECT'
   WHEN UCASE (renta) LIKE 'RENTA ALTA%' THEN 'ALTA'
   WHEN UCASE (renta) LIKE 'RENTA MEDIA%' THEN 'MEDIA'
   WHEN UCASE (renta) LIKE 'RENTA MASIVA%' THEN 'MASIVA'
   WHEN UCASE (renta) LIKE 'INSTITUCION%' THEN 'INSTITUCION'
   WHEN UCASE (renta) LIKE 'PYME 1%' THEN 'PYME 1'
   WHEN UCASE (renta) LIKE 'PYME 2%' THEN 'PYME 2'
   WHEN UCASE (renta) LIKE 'COMERCIO%' THEN 'COMERCIO'
   WHEN UCASE (renta) LIKE 'EMPRESA%' THEN 'EMPRESA'
   WHEN UCASE (renta) LIKE '%GRAN%EMPRESA%' THEN 'GRAN EMPRESA'
   ELSE 'VER'
END as ds_nps_renta,
rentabilidad as ds_nps_rentabilidad,
segmento as ds_nps_segmento,
sexo as ds_per_sexo,
tipo_doc as ds_per_tipo_doc,
'onboarding delivery' as ds_nps_tipo_encuesta,
to_date(from_unixtime(UNIX_TIMESTAMP(onb.partition_date, 'yyyy-MM-dd'))) as dt_nps_fecha_encuesta,
antiguedad_en_meses as ds_nps_antiguedad_meses,
rank() over (partition by responseid order by question_id asc) as ds_nps_orden,
question_desc as ds_nps_pregunta,
respuesta as ds_nps_respuesta,id_respuesta,
onb.partition_date
from bi_corp_staging.qualtrics_nps_onboarding_dlv as onb
left join bi_corp_staging.exa_dim_productos as b
       ON (onb.producto_contab = b.producto
       AND onb.subprod_contab = b.subproducto
       and b.partition_date='{{ ti.xcom_pull(task_ids='InputConfig', key='partition_date', dag_id='LOAD_CMN_NPS-Daily') }}')
where onb.partition_date='{{ ti.xcom_pull(task_ids='InputConfig', key='partition_date', dag_id='LOAD_CMN_NPS-Daily') }}'
AND nup IS NOT NULL


union all

Select
lpad(penumber,8,0)  as cod_per_nup,
response_id as cod_nps_respuesta,
lpad(nro_doc,11,0) as cod_per_nro_doc,
LPAD(nro_ubicacion,4,0) as cod_nps_nro_ubicacion,
q1_nps_group as ds_nps_agrupados,
'SUCURSAL' as ds_nps_canal,
CASE
   WHEN gestion = 'ASESORAMIENTO DE VENTA' THEN 'ASESORAMIENTO'
   WHEN gestion = 'CLIENTE NOTIFICADO' THEN 'CONSULTA'
   WHEN gestion = 'VENDIDO' THEN 'VENTA'
   ELSE 'OTROS'
END as ds_nps_descripcion,
ubicacion as ds_nps_desc_ubicacion,
edad as ds_per_edad,
if (nombre_usuario is null, ejecutivo, nombre_usuario) as ds_nps_ejecutivo,
if (nombre_usuario is null, legajo, usuario_atencion) as ds_nps_legajo,
mejor_producto as ds_nps_mejor_prod,
'PLATAFORMA' as ds_nps_momento_critico,
cast(null as string) as ds_nps_negocio,
nombre as ds_per_nombre,
nombre_sucursal as ds_nps_nombre_sucursal,
CASE
   WHEN UCASE (renta) LIKE 'BANCA PRIVADA%' THEN 'BANCA PRIVADA'
   WHEN UCASE (renta) LIKE 'RENTA ALTA SUPERIOR%' THEN 'SELECT'
   WHEN UCASE (renta) LIKE 'RENTA ALTA%' THEN 'ALTA'
   WHEN UCASE (renta) LIKE 'RENTA MEDIA%' THEN 'MEDIA'
   WHEN UCASE (renta) LIKE 'RENTA MASIVA%' THEN 'MASIVA'
   WHEN UCASE (renta) LIKE 'INSTITUCION%' THEN 'INSTITUCION'
   WHEN UCASE (renta) LIKE 'PYME 1%' THEN 'PYME 1'
   WHEN UCASE (renta) LIKE 'PYME 2%' THEN 'PYME 2'
   WHEN UCASE (renta) LIKE 'COMERCIO%' THEN 'COMERCIO'
   WHEN UCASE (renta) LIKE 'EMPRESA%' THEN 'EMPRESA'
   WHEN UCASE (renta) LIKE '%GRAN%EMPRESA%' THEN 'GRAN EMPRESA'
   ELSE 'VER'
end as ds_nps_renta,
rentabilidad as ds_nps_rentabilidad,
segmento as ds_nps_segmento,
sexo as ds_per_sexo,
tipo_doc as ds_per_tipo_doc,
'plataforma' as ds_nps_tipo_encuesta,
to_date(from_unixtime(UNIX_TIMESTAMP(partition_date, 'yyyy-MM-dd'))) as dt_nps_fecha_encuesta,
antiguedad_en_meses as ds_nps_antiguedad_meses,
rank() over (partition by response_id order by question_id asc) as ds_nps_orden,
question_desc as ds_nps_pregunta,
respuesta as ds_nps_respuesta,id_respuesta,
partition_date
from bi_corp_staging.qualtrics_nps_plataforma
where partition_date='{{ ti.xcom_pull(task_ids='InputConfig', key='partition_date', dag_id='LOAD_CMN_NPS-Daily') }}'
AND  penumber IS NOT NULL


union all

Select
lpad(penumber,8,0)  as cod_per_nup,
response_id as cod_nps_respuesta,
lpad(nro_doc,11,0) as cod_per_nro_doc,
LPAD(nro_ubicacion,4,0) as cod_nps_nro_ubicacion,
q1_nps_group as ds_nps_agrupados,
IF (UCASE(canal) IS null,'SUCURSAL',UCASE(canal)) as ds_nps_canal,
CASE
   WHEN gestion = 'ASESORAMIENTO DE VENTA' THEN 'ASESORAMIENTO'
   WHEN gestion = 'CLIENTE NOTIFICADO' THEN 'CONSULTA'
   WHEN gestion = 'VENDIDO' THEN 'VENTA'
   ELSE 'OTROS'
END as ds_nps_descripcion,
ubicacion as ds_nps_desc_ubicacion,
edad as ds_per_edad,
if (nombre_usuario is null, ejecutivo, nombre_usuario) as ds_nps_ejecutivo,
if (nombre_usuario is null, legajo, usuario_atencion) as ds_nps_legajo,
mejor_producto as ds_nps_mejor_prod,
UCASE(momento_critico) as ds_nps_momento_critico,
cast(null as string) as ds_nps_negocio,
nombre as ds_per_nombre,
nombre_sucursal as ds_nps_nombre_sucursal,
CASE
   WHEN UCASE (renta) LIKE 'BANCA PRIVADA%' THEN 'BANCA PRIVADA'
   WHEN UCASE (renta) LIKE 'RENTA ALTA SUPERIOR%' THEN 'SELECT'
   WHEN UCASE (renta) LIKE 'RENTA ALTA%' THEN 'ALTA'
   WHEN UCASE (renta) LIKE 'RENTA MEDIA%' THEN 'MEDIA'
   WHEN UCASE (renta) LIKE 'RENTA MASIVA%' THEN 'MASIVA'
   WHEN UCASE (renta) LIKE 'INSTITUCION%' THEN 'INSTITUCION'
   WHEN UCASE (renta) LIKE 'PYME 1%' THEN 'PYME 1'
   WHEN UCASE (renta) LIKE 'PYME 2%' THEN 'PYME 2'
   WHEN UCASE (renta) LIKE 'COMERCIO%' THEN 'COMERCIO'
   WHEN UCASE (renta) LIKE 'EMPRESA%' THEN 'EMPRESA'
   WHEN UCASE (renta) LIKE '%GRAN%EMPRESA%' THEN 'GRAN EMPRESA'
   ELSE 'VER'
end as ds_nps_renta,
rentabilidad as ds_nps_rentabilidad,
segmento as ds_nps_segmento,
sexo as ds_per_sexo,
tipo_doc as ds_per_tipo_doc,
'plataforma pymes' as ds_nps_tipo_encuesta,
to_date(from_unixtime(UNIX_TIMESTAMP(partition_date, 'yyyy-MM-dd'))) as dt_nps_fecha_encuesta,
antiguedad_en_meses as ds_nps_antiguedad_meses,
rank() over (partition by response_id order by question_id asc) as ds_nps_orden,
question_desc as ds_nps_pregunta,
respuesta as ds_nps_respuesta,id_respuesta,
partition_date
from bi_corp_staging.qualtrics_nps_plataforma_pym
where partition_date='{{ ti.xcom_pull(task_ids='InputConfig', key='partition_date', dag_id='LOAD_CMN_NPS-Daily') }}'
AND  penumber IS NOT NULL


union all

Select
lpad(nup,8,0) as cod_per_nup,
responseid as cod_nps_respuesta,
lpad(nro_doc,11,0) as cod_per_nro_doc,
IF(nro_ubicacion like 'A%',ubicacion,if (nro_ubicacion like 'B%',ubicacion,nro_ubicacion)) cod_nps_nro_ubicacion,
q1_nps_group as ds_nps_agrupados,
ucase(canal) as ds_nps_canal,
UCASE (CASE
   WHEN UCASE (motivo_reclamo) = 'ALTAS, BAJAS Y MODIFICACIONES DE TARJETAS DE D¿BITO' THEN 'ALTAS, BAJAS Y MODIFICACIONES DE TARJETAS DE DÉBITO'
   WHEN UCASE (motivo_reclamo) = 'ATENCI¿N EN CANALES' THEN 'ATENCIÓN EN CANALES'
   WHEN UCASE (motivo_reclamo) = 'AUMENTO DE L¿MITE DE TARJETA NO PROCESADO' THEN 'AUMENTO DE LÍMITE DE TARJETA NO PROCESADO'
   WHEN UCASE (motivo_reclamo) = 'BAJAS Y MODIFICACIONES DE TARJETAS DE CR¿DITO' THEN 'BAJAS Y MODIFICACIONES DE TARJETAS DE CRÉDITO'
   WHEN UCASE (motivo_reclamo) = 'DEP¿SITOS EN TAS' THEN 'DEPÓSITOS EN TAS'
   WHEN UCASE (motivo_reclamo) = 'DEP¿SITOS NO ACREDITADOS EN ATM' THEN 'DEPÓSITOS NO ACREDITADOS EN ATM'
   WHEN UCASE (motivo_reclamo) = 'DISCONFORMIDAD CON EL OFRECIMIENTO DE CAMPA¿AS' THEN 'DISCONFORMIDAD CON EL OFRECIMIENTO DE CAMPAÑAS'
   WHEN UCASE (motivo_reclamo) = 'DISCONFORMIDAD CON LA RESOLUCI¿N DEL RECLAMO' THEN 'DISCONFORMIDAD CON LA RESOLUCIÓN DEL RECLAMO'
   WHEN UCASE (motivo_reclamo) = 'ENV¿O DE RES¿MENES' THEN 'ENVÍO DE RESÚMENES'
   WHEN UCASE (motivo_reclamo) = 'INCONVENIENTES CON CAJERO AUTOM¿TICO' THEN 'INCONVENIENTES CON CAJERO AUTOMÁTICO'
   WHEN UCASE (motivo_reclamo) = 'INCONVENIENTES CON DEP¿SITOS POR CAJA' THEN 'INCONVENIENTES CON DEPÓSITOS POR CAJA'
   WHEN UCASE (motivo_reclamo) = 'INCONVENIENTES CON LA ACREDITACI¿N DE SUELDOS' THEN  'INCONVENIENTES CON LA ACREDITACIÓN DE SUELDOS'
   WHEN UCASE (motivo_reclamo) = 'INCONVENIENTES CON LA GESTI¿N DE SINIESTROS' THEN 'INCONVENIENTES CON LA GESTIÓN DE SINIESTROS'
   WHEN UCASE (motivo_reclamo) =  'INCONVENIENTES CON PAGO ELECTR¿NICO DE SERVICIOS (ADDI, PES)' THEN 'INCONVENIENTES CON PAGO ELECTRÓNICO DE SERVICIOS (ADDI, PES)'
   WHEN UCASE (motivo_reclamo) = 'INCONVENIENTES CON PR¿STAMOS' THEN 'INCONVENIENTES CON PRÉSTAMOS'
   WHEN UCASE (motivo_reclamo) = 'INCONVENIENTES/FALTA DE ACREDITACI¿N DE CUPONES DE COMERCIOS' THEN 'INCONVENIENTES/FALTA DE ACREDITACIÓN DE CUPONES DE COMERCIOS'
   WHEN UCASE (motivo_reclamo) = 'ORGANISMOS DE INFORMACI¿N FINANCIERA (VERAZ, BCRA)' THEN 'ORGANISMOS DE INFORMACIÓN FINANCIERA (VERAZ, BCRA)'
   WHEN UCASE (motivo_reclamo) = 'PAGO NO IMPUTADO EN TARJETA DE CR¿DITO' THEN 'PAGO NO IMPUTADO EN TARJETA DE CRÉDITO'
   WHEN UCASE (motivo_reclamo) = 'PROBLEMAS CON ENTREGA DE LIBRE DE DEUDA O INFORMACI¿N DE DEUDA ACTUALIZADA' THEN 'PROBLEMAS CON ENTREGA DE LIBRE DE DEUDA O INFORMACIÓN DE DEUDA ACTUALIZADA'
   WHEN UCASE (motivo_reclamo) = 'SEGUROS: NOTIFICACI¿N NO RECIBIDA' THEN 'SEGUROS: NOTIFICACIÓN NO RECIBIDA'
   ELSE UCASE (motivo_reclamo)
END) as ds_nps_descripcion,
ubicacion as ds_nps_desc_ubicacion,
edad as ds_per_edad,
cast(null as string) as ds_nps_ejecutivo,
cast(null as string) as ds_nps_legajo,
cast(null as string) as ds_nps_mejor_prod,
ucase(momento_critico) as ds_nps_momento_critico,
cast(null as string) as ds_nps_negocio,
nombre as ds_per_nombre,
ucase(nombre_sucursal) as ds_nps_nombre_sucursal,
CASE
   WHEN UCASE (renta) LIKE 'BANCA PRIVADA%' THEN 'BANCA PRIVADA'
   WHEN UCASE (renta) LIKE 'RENTA ALTA SUPERIOR%' THEN 'SELECT'
   WHEN UCASE (renta) LIKE 'RENTA ALTA%' THEN 'ALTA'
   WHEN UCASE (renta) LIKE 'RENTA MEDIA%' THEN 'MEDIA'
   WHEN UCASE (renta) LIKE 'RENTA MASIVA%' THEN 'MASIVA'
   WHEN UCASE (renta) LIKE 'INSTITUCION%' THEN 'INSTITUCION'
   WHEN UCASE (renta) LIKE 'PYME 1%' THEN 'PYME 1'
   WHEN UCASE (renta) LIKE 'PYME 2%' THEN 'PYME 2'
   WHEN UCASE (renta) LIKE 'COMERCIO%' THEN 'COMERCIO'
   WHEN UCASE (renta) LIKE 'EMPRESA%' THEN 'EMPRESA'
   WHEN UCASE (renta) LIKE '%GRAN%EMPRESA%' THEN 'GRAN EMPRESA'
   ELSE 'VER'
END as ds_nps_renta,
rentabilidad as ds_nps_rentabilidad,
ucase(segmento) as ds_nps_segmento,
sexo as ds_per_sexo,
tipo_doc as ds_per_tipo_doc,
'reclamos' as ds_nps_tipo_encuesta,
to_date(from_unixtime(UNIX_TIMESTAMP(partition_date, 'yyyy-MM-dd'))) as dt_nps_fecha_encuesta,
antiguedad_en_meses as ds_nps_antiguedad_meses,
rank() over (partition by responseid order by question_id asc) as ds_nps_orden,
question_desc as ds_nps_pregunta,
respuesta as ds_nps_respuesta,id_respuesta,
partition_date
from bi_corp_staging.qualtrics_nps_reclamos
where partition_date='{{ ti.xcom_pull(task_ids='InputConfig', key='partition_date', dag_id='LOAD_CMN_NPS-Daily') }}'
AND nup IS NOT NULL


union all

Select
lpad(nup,8,0) as cod_per_nup,
responseid as cod_nps_respuesta,
lpad(nro_doc,11,0) as cod_per_nro_doc,
IF(nro_ubicacion like 'A%',ubicacion,if (nro_ubicacion like 'B%',ubicacion,nro_ubicacion)) cod_nps_nro_ubicacion,
q1_nps_group as ds_nps_agrupados,
ucase(canal) as ds_nps_canal,
UCASE (CASE
   WHEN UCASE (motivo_reclamo) = 'ALTAS, BAJAS Y MODIFICACIONES DE TARJETAS DE D¿BITO' THEN 'ALTAS, BAJAS Y MODIFICACIONES DE TARJETAS DE DÉBITO'
   WHEN UCASE (motivo_reclamo) = 'ATENCI¿N EN CANALES' THEN 'ATENCIÓN EN CANALES'
   WHEN UCASE (motivo_reclamo) = 'AUMENTO DE L¿MITE DE TARJETA NO PROCESADO' THEN 'AUMENTO DE LÍMITE DE TARJETA NO PROCESADO'
   WHEN UCASE (motivo_reclamo) = 'BAJAS Y MODIFICACIONES DE TARJETAS DE CR¿DITO' THEN 'BAJAS Y MODIFICACIONES DE TARJETAS DE CRÉDITO'
   WHEN UCASE (motivo_reclamo) = 'DEP¿SITOS EN TAS' THEN 'DEPÓSITOS EN TAS'
   WHEN UCASE (motivo_reclamo) = 'DEP¿SITOS NO ACREDITADOS EN ATM' THEN 'DEPÓSITOS NO ACREDITADOS EN ATM'
   WHEN UCASE (motivo_reclamo) = 'DISCONFORMIDAD CON EL OFRECIMIENTO DE CAMPA¿AS' THEN 'DISCONFORMIDAD CON EL OFRECIMIENTO DE CAMPAÑAS'
   WHEN UCASE (motivo_reclamo) = 'DISCONFORMIDAD CON LA RESOLUCI¿N DEL RECLAMO' THEN 'DISCONFORMIDAD CON LA RESOLUCIÓN DEL RECLAMO'
   WHEN UCASE (motivo_reclamo) = 'ENV¿O DE RES¿MENES' THEN 'ENVÍO DE RESÚMENES'
   WHEN UCASE (motivo_reclamo) = 'INCONVENIENTES CON CAJERO AUTOM¿TICO' THEN 'INCONVENIENTES CON CAJERO AUTOMÁTICO'
   WHEN UCASE (motivo_reclamo) = 'INCONVENIENTES CON DEP¿SITOS POR CAJA' THEN 'INCONVENIENTES CON DEPÓSITOS POR CAJA'
   WHEN UCASE (motivo_reclamo) = 'INCONVENIENTES CON LA ACREDITACI¿N DE SUELDOS' THEN  'INCONVENIENTES CON LA ACREDITACIÓN DE SUELDOS'
   WHEN UCASE (motivo_reclamo) = 'INCONVENIENTES CON LA GESTI¿N DE SINIESTROS' THEN 'INCONVENIENTES CON LA GESTIÓN DE SINIESTROS'
   WHEN UCASE (motivo_reclamo) =  'INCONVENIENTES CON PAGO ELECTR¿NICO DE SERVICIOS (ADDI, PES)' THEN 'INCONVENIENTES CON PAGO ELECTRÓNICO DE SERVICIOS (ADDI, PES)'
   WHEN UCASE (motivo_reclamo) = 'INCONVENIENTES CON PR¿STAMOS' THEN 'INCONVENIENTES CON PRÉSTAMOS'
   WHEN UCASE (motivo_reclamo) = 'INCONVENIENTES/FALTA DE ACREDITACI¿N DE CUPONES DE COMERCIOS' THEN 'INCONVENIENTES/FALTA DE ACREDITACIÓN DE CUPONES DE COMERCIOS'
   WHEN UCASE (motivo_reclamo) = 'ORGANISMOS DE INFORMACI¿N FINANCIERA (VERAZ, BCRA)' THEN 'ORGANISMOS DE INFORMACIÓN FINANCIERA (VERAZ, BCRA)'
   WHEN UCASE (motivo_reclamo) = 'PAGO NO IMPUTADO EN TARJETA DE CR¿DITO' THEN 'PAGO NO IMPUTADO EN TARJETA DE CRÉDITO'
   WHEN UCASE (motivo_reclamo) = 'PROBLEMAS CON ENTREGA DE LIBRE DE DEUDA O INFORMACI¿N DE DEUDA ACTUALIZADA' THEN 'PROBLEMAS CON ENTREGA DE LIBRE DE DEUDA O INFORMACIÓN DE DEUDA ACTUALIZADA'
   WHEN UCASE (motivo_reclamo) = 'SEGUROS: NOTIFICACI¿N NO RECIBIDA' THEN 'SEGUROS: NOTIFICACIÓN NO RECIBIDA'
   ELSE UCASE (motivo_reclamo)
END) as ds_nps_descripcion,
ubicacion as ds_nps_desc_ubicacion,
edad as ds_per_edad,
cast(null as string) as ds_nps_ejecutivo,
cast(null as string) as ds_nps_legajo,
cast(null as string) as ds_nps_mejor_prod,
'RECLAMOS PYM' as ds_nps_momento_critico,
cast(null as string) as ds_nps_negocio,
nombre as ds_per_nombre,
ucase(nombre_sucursal) as ds_nps_nombre_sucursal,
CASE
   WHEN UCASE (renta) LIKE 'BANCA PRIVADA%' THEN 'BANCA PRIVADA'
   WHEN UCASE (renta) LIKE 'RENTA ALTA SUPERIOR%' THEN 'SELECT'
   WHEN UCASE (renta) LIKE 'RENTA ALTA%' THEN 'ALTA'
   WHEN UCASE (renta) LIKE 'RENTA MEDIA%' THEN 'MEDIA'
   WHEN UCASE (renta) LIKE 'RENTA MASIVA%' THEN 'MASIVA'
   WHEN UCASE (renta) LIKE 'INSTITUCION%' THEN 'INSTITUCION'
   WHEN UCASE (renta) LIKE 'PYME 1%' THEN 'PYME 1'
   WHEN UCASE (renta) LIKE 'PYME 2%' THEN 'PYME 2'
   WHEN UCASE (renta) LIKE 'COMERCIO%' THEN 'COMERCIO'
   WHEN UCASE (renta) LIKE 'EMPRESA%' THEN 'EMPRESA'
   WHEN UCASE (renta) LIKE '%GRAN%EMPRESA%' THEN 'GRAN EMPRESA'
   ELSE 'VER'
END as ds_nps_renta,
rentabilidad as ds_nps_rentabilidad,
segmento as ds_nps_segmento,
sexo as ds_per_sexo,
tipo_doc as ds_per_tipo_doc,
'reclamos pymes' as ds_nps_tipo_encuesta,
to_date(from_unixtime(UNIX_TIMESTAMP(partition_date, 'yyyy-MM-dd'))) as dt_nps_fecha_encuesta,
antiguedad_en_meses as ds_nps_antiguedad_meses,
rank() over (partition by responseid order by question_id asc) as ds_nps_orden,
question_desc as ds_nps_pregunta,
respuesta as ds_nps_respuesta,id_respuesta,
partition_date
from bi_corp_staging.qualtrics_nps_reclamos_pym
where partition_date='{{ ti.xcom_pull(task_ids='InputConfig', key='partition_date', dag_id='LOAD_CMN_NPS-Daily') }}'
AND nup IS NOT NULL


union all



Select
lpad(nup,8,0) as cod_per_nup,
responseid as cod_nps_respuesta,
LPAD (nro_doc, 11, 0) as cod_per_nro_doc,
lpad(cod_suc_uc,4,0)  as cod_nps_nro_ubicacion,
q1_nps_group as ds_nps_agrupados,
'BANCO' as ds_nps_canal,
cast(null as string) as ds_nps_descripcion,
cast(null as string) as ds_nps_desc_ubicacion,
edad as ds_per_edad,
cast(null as string) as ds_nps_ejecutivo,
cast(null as string) as ds_nps_legajo,
mejor_producto as ds_nps_mejor_prod,
'RELACIONAL' as ds_nps_momento_critico,
cast(null as string) as ds_nps_negocio,
nombre as ds_per_nombre,
cast(null as string) as ds_nps_nombre_sucursal,
CASE
   WHEN UCASE (renta) LIKE '%BANCA PRIVADA%' THEN 'BANCA PRIVADA'
   WHEN UCASE (renta) LIKE '%RENTA ALTA SUPERIOR%' THEN 'SELECT'
   WHEN UCASE (renta) LIKE '%RENTA SELECT%' THEN 'SELECT'
   WHEN UCASE (renta) LIKE '%RENTA ALTA%' THEN 'ALTA'
   WHEN UCASE (renta) LIKE '%RENTA MEDIA%' THEN 'MEDIA'
   WHEN UCASE (renta) LIKE '%RENTA MASIVA%' THEN 'MASIVA'
   WHEN UCASE (renta) LIKE '%RENTA BAJA%' THEN 'MASIVA'
   WHEN UCASE (renta) LIKE '%INSTITUCION%' THEN 'INSTITUCION'
   WHEN UCASE (renta) LIKE '%PYME 1%' THEN 'PYME 1'
   WHEN UCASE (renta) LIKE '%PYME 2%' THEN 'PYME 2'
   WHEN UCASE (renta) LIKE '%COMERCIO%' THEN 'COMERCIO'
   WHEN UCASE (renta) LIKE '%EMPRESA%' THEN 'EMPRESA'
   WHEN UCASE (renta) LIKE '%GRAN%EMPRESA%' THEN 'GRAN EMPRESA'
   ELSE 'VER'
END as ds_nps_renta,
cast(null as decimal(12,2)) as ds_nps_rentabilidad,
cast(null as string) as ds_nps_segmento,
sexo as ds_per_sexo,
tipo_doc as ds_per_tipo_doc,
'relacional' as ds_nps_tipo_encuesta,
to_date(from_unixtime(UNIX_TIMESTAMP(partition_date, 'yyyy-MM-dd'))) as dt_nps_fecha_encuesta,
antiguedad_en_meses as ds_nps_antiguedad_meses,
rank() over (partition by responseid order by question_id asc) as ds_nps_orden,
question_desc as ds_nps_pregunta,
respuesta as ds_nps_respuesta,id_respuesta,
partition_date
from bi_corp_staging.qualtrics_nps_relacional
where partition_date='{{ ti.xcom_pull(task_ids='InputConfig', key='partition_date', dag_id='LOAD_CMN_NPS-Daily') }}'
AND nup IS NOT NULL


union all

Select
lpad(nup,8,0) as cod_per_nup,
responseid as cod_nps_respuesta,
LPAD (nro_doc, 11, 0) as cod_per_nro_doc,
lpad(cod_suc_uc,4,0)  as cod_nps_nro_ubicacion,
q1_nps_group as ds_nps_agrupados,
'BANCO' as ds_nps_canal,
cast(null as string) as ds_nps_descripcion,
cast(null as string) as ds_nps_desc_ubicacion,
edad as ds_per_edad,
cast(null as string) as ds_nps_ejecutivo,
cast(null as string) as ds_nps_legajo,
mejor_producto as ds_nps_mejor_prod,
'RELACIONAL 65' as ds_nps_momento_critico,
cast(null as string) as ds_nps_negocio,
nombre as ds_per_nombre,
cast(null as string) as ds_nps_nombre_sucursal,
CASE
   WHEN UCASE (renta) LIKE '%BANCA PRIVADA%' THEN 'BANCA PRIVADA'
   WHEN UCASE (renta) LIKE '%RENTA ALTA SUPERIOR%' THEN 'SELECT'
   WHEN UCASE (renta) LIKE '%RENTA SELECT%' THEN 'SELECT'
   WHEN UCASE (renta) LIKE '%RENTA ALTA%' THEN 'ALTA'
   WHEN UCASE (renta) LIKE '%RENTA MEDIA%' THEN 'MEDIA'
   WHEN UCASE (renta) LIKE '%RENTA MASIVA%' THEN 'MASIVA'
   WHEN UCASE (renta) LIKE '%RENTA BAJA%' THEN 'MASIVA'
   WHEN UCASE (renta) LIKE '%INSTITUCION%' THEN 'INSTITUCION'
   WHEN UCASE (renta) LIKE '%PYME 1%' THEN 'PYME 1'
   WHEN UCASE (renta) LIKE '%PYME 2%' THEN 'PYME 2'
   WHEN UCASE (renta) LIKE '%COMERCIO%' THEN 'COMERCIO'
   WHEN UCASE (renta) LIKE '%EMPRESA%' THEN 'EMPRESA'
   WHEN UCASE (renta) LIKE '%GRAN%EMPRESA%' THEN 'GRAN EMPRESA'
   ELSE 'VER'
END as ds_nps_renta,
cast(null as decimal(12,2)) as ds_nps_rentabilidad,
cast(null as string) as ds_nps_segmento,
sexo as ds_per_sexo,
tipo_doc as ds_per_tipo_doc,
'relacional 65' as ds_nps_tipo_encuesta,
to_date(from_unixtime(UNIX_TIMESTAMP(partition_date, 'yyyy-MM-dd'))) as dt_nps_fecha_encuesta,
antiguedad_en_meses as ds_nps_antiguedad_meses,
rank() over (partition by responseid order by question_id asc) as ds_nps_orden,
question_desc as ds_nps_pregunta,
respuesta as ds_nps_respuesta,id_respuesta,
partition_date
from bi_corp_staging.qualtrics_nps_relacional_65
where partition_date='{{ ti.xcom_pull(task_ids='InputConfig', key='partition_date', dag_id='LOAD_CMN_NPS-Daily') }}'
AND nup IS NOT NULL


union all

Select
lpad(nup,8,0) as cod_per_nup,
responseid as cod_nps_respuesta,
lpad(nro_doc,11,0) as cod_per_nro_doc,
LPAD (nro_ubicacion, 4, 0) as cod_nps_nro_ubicacion,
q1_nps_group as ds_nps_agrupados,
case
    when UCASE(LEGAJO) = 'WEBAPP'  and UCASE(ejecutivo) = 'APP MOBILE'  then 'MOBILE'
    when UCASE(LEGAJO) = 'HMBK' and UCASE(ejecutivo) = 'HOMEBANKING'   then 'ONLINE BANKING'
    else if (UCASE(canal) = 'ESOL','EJEC. REMOTO SELECT', UCASE(CANAL))
end  as ds_nps_canal,
UCASE(de_ramo) as ds_nps_descripcion,
ubicacion as ds_nps_desc_ubicacion,
edad as ds_per_edad,
ejecutivo as ds_nps_ejecutivo,
legajo as ds_nps_legajo,
cast(null as string) as ds_nps_mejor_prod,
ucase(momento_critico) as ds_nps_momento_critico,
cast(null as string) as ds_nps_negocio,
nombre as ds_per_nombre,
cast(null as string) as ds_nps_nombre_sucursal,
CASE
   WHEN UCASE (renta) LIKE '%BANCA PRIVADA%' THEN 'BANCA PRIVADA'
   WHEN UCASE (renta) LIKE '%RENTA ALTA SUPERIOR%' THEN 'SELECT'
   WHEN UCASE (renta) LIKE '%SELECT%' THEN 'SELECT'
   WHEN UCASE (renta) LIKE '%RENTA ALTA%' THEN 'ALTA'
   WHEN UCASE (renta) LIKE '%RENTA MEDIA%' THEN 'MEDIA'
   WHEN UCASE (renta) LIKE '%RENTA MASIVA%' THEN 'MASIVA'
   WHEN UCASE (renta) LIKE '%INSTITUCION%' THEN 'INSTITUCION'
   WHEN UCASE (renta) LIKE '%COMERCIO%' THEN 'COMERCIO'
   WHEN UCASE (renta) LIKE 'PYME 1%' THEN 'PYME 1'
   WHEN UCASE (renta) LIKE 'PYME 2%' THEN 'PYME 2'
   WHEN UCASE (renta) LIKE '%EMPRESA%' THEN 'EMPRESA'
   WHEN UCASE (renta) LIKE '%GRAN%EMPRESA%' THEN 'GRAN EMPRESA'
   ELSE 'VER'
END as ds_nps_renta,
rentabilidad as ds_nps_rentabilidad,
segmento as ds_nps_segmento,
sexo as ds_per_sexo,
tipo_doc as ds_per_tipo_doc,
'seguros' as ds_nps_tipo_encuesta,
to_date(from_unixtime(UNIX_TIMESTAMP(partition_date, 'yyyy-MM-dd'))) as dt_nps_fecha_encuesta,
cast(null as int) as ds_nps_antiguedad_meses,
rank() over (partition by responseid order by question_id asc) as ds_nps_orden,
question_desc as ds_nps_pregunta,
respuesta as ds_nps_respuesta,id_respuesta,
partition_date
from bi_corp_staging.qualtrics_nps_seguros
where partition_date='{{ ti.xcom_pull(task_ids='InputConfig', key='partition_date', dag_id='LOAD_CMN_NPS-Daily') }}'
AND nup IS NOT NULL


union all

Select
lpad(nup,8,0) as cod_per_nup,
responseid as cod_nps_respuesta,
lpad(nro_doc,11,0) as cod_per_nro_doc,
'OTROS' as cod_nps_nro_ubicacion,
q1_nps_group as ds_nps_agrupados,
'DELIVERY' as ds_nps_canal,
CASE
	WHEN motivo_visita LIKE 'REPOSICION%' THEN concat('REPOSICION ', if (tipo_producto ='AMER. EXP', 'AMEX', if(tipo_producto is null,' ',tipo_producto)))
    WHEN motivo_visita LIKE 'REIMPRESION TARJETA%' THEN  concat('REIMPRESION ' , if (tipo_producto ='AMER. EXP', 'AMEX', if(tipo_producto is null,' ',tipo_producto)))
    WHEN motivo_visita = 'REIMPRESION T.DEBITO' THEN 'REIMPRESION BANELCO'
    WHEN motivo_visita = 'REIMPRESION T.CREDITO' THEN 'REIMPRESION VISA'
    WHEN motivo_visita = 'REIMPRESION' THEN concat('REIMPRESION ', if (tipo_producto ='AMER. EXP', 'AMEX', if(tipo_producto is null,' ',tipo_producto)))
    ELSE 'REIMPRESION' END
as ds_nps_descripcion,
ubicacion as ds_nps_desc_ubicacion,
edad as ds_per_edad,
ejecutivo as ds_nps_ejecutivo,
legajo as ds_nps_legajo,
cast(null as string) as ds_nps_mejor_prod,
'SUST TARJETA' as ds_nps_momento_critico,
cast(null as string) as ds_nps_negocio,
nombre as ds_per_nombre,
cast(null as string) as ds_nps_nombre_sucursal,
CASE
   WHEN UCASE (renta) LIKE 'BANCA PRIVADA%' THEN 'BANCA PRIVADA'
   WHEN UCASE (renta) LIKE 'RENTA ALTA SUPERIOR%' THEN 'SELECT'
   WHEN UCASE (renta) LIKE 'RENTA ALTA%' THEN 'ALTA'
   WHEN UCASE (renta) LIKE 'RENTA MEDIA%' THEN 'MEDIA'
   WHEN UCASE (renta) LIKE 'RENTA MASIVA%' THEN 'MASIVA'
   WHEN UCASE (renta) LIKE 'PYME 1%' THEN 'PYME 1'
   WHEN UCASE (renta) LIKE 'PYME 2%' THEN 'PYME 2'
   WHEN UCASE (renta) LIKE 'INSTITUCION%' THEN 'INSTITUCION'
   WHEN UCASE (renta) LIKE 'COMERCIO%' THEN 'COMERCIO'
   WHEN UCASE (renta) LIKE 'EMPRESA%' THEN 'EMPRESA'
   WHEN UCASE (renta) LIKE '%GRAN%EMPRESA%' THEN 'GRAN EMPRESA'
   ELSE 'VER'
END
as ds_nps_renta,
rentabilidad as ds_nps_rentabilidad,
segmento as ds_nps_segmento,
sexo as ds_per_sexo,
tipo_doc as ds_per_tipo_doc,
'sustitucion tarjetas' as ds_nps_tipo_encuesta,
to_date(from_unixtime(UNIX_TIMESTAMP(partition_date, 'yyyy-MM-dd'))) as dt_nps_fecha_encuesta,
antiguedad_en_meses as ds_nps_antiguedad_meses,
rank() over (partition by responseid order by question_id asc) as ds_nps_orden,
question_desc as ds_nps_pregunta,
respuesta as ds_nps_respuesta,id_respuesta,
partition_date
from bi_corp_staging.qualtrics_nps_sust_tarj
where partition_date='{{ ti.xcom_pull(task_ids='InputConfig', key='partition_date', dag_id='LOAD_CMN_NPS-Daily') }}'
AND nup IS NOT NULL


union all

Select
lpad(nup,8,0) as cod_per_nup,
responseid as cod_nps_respuesta,
lpad(nro_doc,11,0) as cod_per_nro_doc,
LPAD (nro_ubicacion, 4, 0) as cod_nps_nro_ubicacion,
q1_nps_group as ds_nps_agrupados,
'SUCURSAL' as ds_nps_canal,
CASE
  WHEN ucase (desc_transaccion) like 'SIMULACION DEPOSITO%' THEN 'SIMULACION DEPOSITO'
  WHEN ucase (desc_transaccion) = 'BLANQUEO DE PIN' THEN 'BLANQUEO DE PIN'
  WHEN UCASE (desc_transaccion) like 'EXTRACCION%' THEN 'EXTRACCION'
  WHEN UCASE (desc_transaccion) LIKE 'CONSULTA%' THEN 'CONSULTA'
  WHEN UCASE (desc_transaccion) LIKE 'PAGO%' THEN 'PAGO'
  WHEN UCASE (desc_transaccion) LIKE 'DEP%SITO%' THEN 'DEPOSITO'
  ELSE 'OTROS'
END as ds_nps_descripcion,
cast(null as string) as ds_nps_desc_ubicacion,
edad as ds_per_edad,
cast(null as string) as ds_nps_ejecutivo,
cast(null as string) as ds_nps_legajo,
desc_mejor_producto as ds_nps_mejor_prod,
 ucase(if (momento_critico= 'Tas', 'TAS',if(momento_critico is null,'TAS', momento_critico))) as ds_nps_momento_critico,
cast(null as string) as ds_nps_negocio,
nombre as ds_per_nombre,
nombre_sucursal as ds_nps_nombre_sucursal,
CASE
   WHEN UCASE (renta) LIKE 'BANCA PRIVADA%' THEN 'BANCA PRIVADA'
   WHEN UCASE (renta) LIKE 'RENTA ALTA SUPERIOR%' THEN 'SELECT'
   WHEN UCASE (renta) LIKE 'RENTA ALTA%' THEN 'ALTA'
   WHEN UCASE (renta) LIKE 'RENTA MEDIA%' THEN 'MEDIA'
   WHEN UCASE (renta) LIKE 'RENTA MASIVA%' THEN 'MASIVA'
   WHEN UCASE (renta) LIKE 'INSTITUCION%' THEN 'INSTITUCION'
   WHEN UCASE (renta) LIKE 'PYME 1%' THEN 'PYME 1'
   WHEN UCASE (renta) LIKE 'PYME 2%' THEN 'PYME 2'
   WHEN UCASE (renta) LIKE 'COMERCIO%' THEN 'COMERCIO'
   WHEN UCASE (renta) LIKE 'EMPRESA%' THEN 'EMPRESA'
   WHEN UCASE (renta) LIKE '%GRAN%EMPRESA%' THEN 'GRAN EMPRESA'
   ELSE 'VER'
END as ds_nps_renta,
cast(null as decimal(12,2)) as ds_nps_rentabilidad,
segmento as ds_nps_segmento,
sexo as ds_per_sexo,
cast(null as string) as ds_per_tipo_doc,
'tas' as ds_nps_tipo_encuesta,
to_date(from_unixtime(UNIX_TIMESTAMP(partition_date, 'yyyy-MM-dd'))) as dt_nps_fecha_encuesta,
cast(null as int) as ds_nps_antiguedad_meses,
rank() over (partition by responseid order by question_id asc) as ds_nps_orden,
question_desc as ds_nps_pregunta,
respuesta as ds_nps_respuesta,id_respuesta,
partition_date
from bi_corp_staging.qualtrics_nps_tas
where partition_date='{{ ti.xcom_pull(task_ids='InputConfig', key='partition_date', dag_id='LOAD_CMN_NPS-Daily') }}'
AND nup IS NOT NULL


union all

Select
lpad(nup,8,0) as cod_per_nup,
responseid as cod_nps_respuesta,
lpad(nro_doc,11,0) as cod_per_nro_doc,
case
  when hotspot like '%812%' then '0812'
  when hotspot like'%813%' then '0813'
  when hotspot is null then '0812'
  when hotspot = 'BARI_PB' then '0812'
  when hotspot = 'BARI_1P' then '0812'
  else hotspot
end as cod_nps_nro_ubicacion,
q1_nps_group as ds_nps_agrupados,
'SUCURSAL' as ds_nps_canal,
'WORK_CAFE' as ds_nps_descripcion,
cast(null as string) as ds_nps_desc_ubicacion,
edad as ds_per_edad,
ejecutivo as ds_nps_ejecutivo,
legajo as ds_nps_legajo,
cast(null as string) as ds_nps_mejor_prod,
'WORK_CAFE' as ds_nps_momento_critico,
cast(null as string) as ds_nps_negocio,
nombre as ds_per_nombre,
cast(null as string) as ds_nps_nombre_sucursal,
cast(null as string) as ds_nps_renta,
cast(null as decimal(12,2)) as ds_nps_rentabilidad,
cast(null as string) as ds_nps_segmento,
sexo as ds_per_sexo,
tipo_doc as ds_per_tipo_doc,
'work cafe' as ds_nps_tipo_encuesta,
to_date(from_unixtime(UNIX_TIMESTAMP(partition_date, 'yyyy-MM-dd'))) as dt_nps_fecha_encuesta,
antiguedad_en_meses as ds_nps_antiguedad_meses,
rank() over (partition by responseid order by question_id asc) as ds_nps_orden,
question_desc as ds_nps_pregunta,
respuesta as ds_nps_respuesta,id_respuesta,
partition_date
from bi_corp_staging.qualtrics_nps_work_cafe
where partition_date='{{ ti.xcom_pull(task_ids='InputConfig', key='partition_date', dag_id='LOAD_CMN_NPS-Daily') }}'
AND nro_doc IS NOT NULL


union all

Select
lpad(nup,8,0) as cod_per_nup,
response_id as cod_nps_respuesta,
lpad(nro_doc,11,0) as cod_per_nro_doc,
suc_operacion as cod_nps_nro_ubicacion,
q1_nps_group as ds_nps_agrupados,
if (desc_canal is null,'OTROS', UCASE(desc_canal))  as ds_nps_canal,
producto  as ds_nps_descripcion,
cast(null as string) as ds_nps_desc_ubicacion,
edad as ds_per_edad,
cast(null as string) as ds_nps_ejecutivo,
cast(null as string) as ds_nps_legajo,
cast(null as string) as ds_nps_mejor_prod,
momento_critico as ds_nps_momento_critico,
cast(null as string) as ds_nps_negocio,
nombre as ds_per_nombre,
cast(null as string) as ds_nps_nombre_sucursal,
CASE
   WHEN UCASE (renta) LIKE 'BANCA PRIVADA%' THEN 'BANCA PRIVADA'
   WHEN UCASE (renta) LIKE 'RENTA ALTA SUPERIOR%' THEN 'SELECT'
   WHEN UCASE (renta) LIKE 'RENTA ALTA%' THEN 'ALTA'
   WHEN UCASE (renta) LIKE 'RENTA MEDIA%' THEN 'MEDIA'
   WHEN UCASE (renta) LIKE 'RENTA MASIVA%' THEN 'MASIVA'
   WHEN UCASE (renta) LIKE 'INSTITUCION%' THEN 'INSTITUCION'
   WHEN UCASE (renta) LIKE 'PYME 1%' THEN 'PYME 1'
   WHEN UCASE (renta) LIKE 'PYME 2%' THEN 'PYME 2'
   WHEN UCASE (renta) LIKE 'COMERCIO%' THEN 'COMERCIO'
   WHEN UCASE (renta) LIKE 'EMPRESA%' THEN 'EMPRESA'
   WHEN UCASE (renta) LIKE '%GRAN%EMPRESA%' THEN 'GRAN EMPRESA'
   ELSE 'VER'
END  as ds_nps_renta,
rentabilidad as ds_nps_rentabilidad,
cast(null as string) as ds_nps_segmento,
sexo as ds_per_sexo,
tipo_doc as ds_per_tipo_doc,
'inversiones' as ds_nps_tipo_encuesta,
to_date(from_unixtime(UNIX_TIMESTAMP(partition_date, 'yyyy-MM-dd'))) as dt_nps_fecha_encuesta,
antiguedad as ds_nps_antiguedad_meses,
rank() over (partition by responseid order by question_id asc) as ds_nps_orden,
question_desc as ds_nps_pregunta,
respuesta as ds_nps_respuesta,id_respuesta,
partition_date
from bi_corp_staging.qualtrics_nps_inversiones
where partition_date='{{ ti.xcom_pull(task_ids='InputConfig', key='partition_date', dag_id='LOAD_CMN_NPS-Daily') }}'
and nup is not null


union all

Select
lpad(nup,8,0) as cod_per_nup,
response_id as cod_nps_respuesta,
lpad(nro_doc,11,0) as cod_per_nro_doc,
case
  when UCASE(pecanven)='WW' then 'CANALES DIGITALES'
  else LPAD(centro_alta,4,0)
  end
as cod_nps_nro_ubicacion,
q1_nps_group as ds_nps_agrupados,
case
  when UCASE(pecanven) = 'ER' and UCASE(puesto_usuarioventa) like '%VIP%' then 'EJEC. REMOTO VIP'
  when UCASE(pecanven) = 'ER' and UCASE(puesto_usuarioventa) not like '%VIP%' and UCASE(puesto_usuarioventa) like '%DUO%' then 'EJEC. REMOTO DUO'
  when UCASE(pecanven) = 'ER' and UCASE(puesto_usuarioventa) not like '%VIP%' and UCASE(puesto_usuarioventa) not like '%DUO%' and UCASE(puesto_usuarioventa) like '%AGRO%' then 'EJEC. REMOTO AGRO'
  when UCASE(pecanven) = 'ER' and UCASE(puesto_usuarioventa) not like '%VIP%' and UCASE(puesto_usuarioventa) not like '%DUO%' and UCASE(puesto_usuarioventa) not like '%AGRO%'   and UCASE(puesto_usuarioventa) like '%SELECT%' then 'EJEC. REMOTO SELECT'
  when UCASE(pecanven) ='TK' then 'CONTACT CENTER'
  when UCASE(pecanven)='SA' then 'OTROS'
  when UCASE(pecanven)='WW' then 'ONLINE BANKING'
  else desc_canal_venta
end as ds_nps_canal,
concat(tipo_evento ,' ',UCASE(descrip1))  as ds_nps_descripcion,
cast(null as string) as ds_nps_desc_ubicacion,
edad as ds_per_edad,
cast(null as string) as ds_nps_ejecutivo,
usuarioventa as ds_nps_legajo,
cast(null as string) as ds_nps_mejor_prod,
UCASE(momento_critico) as ds_nps_momento_critico,
cast(null as string) as ds_nps_negocio,
nombre as ds_per_nombre,
cast(null as string) as ds_nps_nombre_sucursal,
CASE
  WHEN UCASE (renta) LIKE 'BANCA PRIVADA%' THEN 'BANCA PRIVADA'
  WHEN UCASE (renta) LIKE 'RENTA ALTA SUPERIOR%' THEN 'SELECT'
  WHEN UCASE (renta) LIKE 'RENTA ALTA%' THEN 'ALTA'
  WHEN UCASE (renta) LIKE 'RENTA MEDIA%' THEN 'MEDIA'
  WHEN UCASE (renta) LIKE 'RENTA MASIVA%' THEN 'MASIVA'
  WHEN UCASE (renta) LIKE 'INSTITUCION%' THEN 'INSTITUCION'
  WHEN UCASE (renta) LIKE 'PYME 1%' THEN 'PYME 1'
  WHEN UCASE (renta) LIKE 'PYME 2%' THEN 'PYME 2'
  WHEN UCASE (renta) LIKE 'COMERCIO%' THEN 'COMERCIO'
  WHEN UCASE (renta) LIKE 'EMPRESA%' THEN 'EMPRESA'
  WHEN UCASE (renta) LIKE '%GRAN%EMPRESA%' THEN 'GRAN EMPRESA'
  ELSE 'VER'
END as ds_nps_renta,
rentabilidad as ds_nps_rentabilidad,
cast(null as string) as ds_nps_segmento,
sexo as ds_per_sexo,
tipo_doc as ds_per_tipo_doc,
'onboarding pyme' as ds_nps_tipo_encuesta,
to_date(from_unixtime(UNIX_TIMESTAMP(partition_date, 'yyyy-MM-dd'))) as dt_nps_fecha_encuesta,
antiguedad as ds_nps_antiguedad_meses,
rank() over (partition by responseid order by question_id asc) as ds_nps_orden,
question_desc as ds_nps_pregunta,
respuesta as ds_nps_respuesta,id_respuesta,
partition_date
from bi_corp_staging.qualtrics_nps_onboarding_pym
where partition_date='{{ ti.xcom_pull(task_ids='InputConfig', key='partition_date', dag_id='LOAD_CMN_NPS-Daily') }}'
and nup is not null


union all

Select
lpad(nup,8,0) as cod_per_nup,
response_id as cod_nps_respuesta,
lpad(nro_doc,11,0) as cod_per_nro_doc,
LPAD(cod_suc_uc,4,0) as cod_nps_nro_ubicacion,
cast(null as string) as ds_nps_agrupados,
cast(null as string) as ds_nps_canal,
cast(null as string) as ds_nps_descripcion,
nombre_suc as ds_nps_desc_ubicacion,
edad as ds_per_edad,
cast(null as string) as ds_nps_ejecutivo,
cast(null as string) as ds_nps_legajo,
cast(null as string) as ds_nps_mejor_prod,
momento_critico as ds_nps_momento_critico,
cast(null as string) as ds_nps_negocio,
nombre as ds_per_nombre,
cast(null as string) as ds_nps_nombre_sucursal,
CASE
  WHEN UCASE (renta) LIKE 'BANCA PRIVADA%' THEN 'BANCA PRIVADA'
  WHEN UCASE (renta) LIKE 'RENTA ALTA SUPERIOR%' THEN 'SELECT'
  WHEN UCASE (renta) LIKE 'RENTA ALTA%' THEN 'ALTA'
  WHEN UCASE (renta) LIKE 'RENTA MEDIA%' THEN 'MEDIA'
  WHEN UCASE (renta) LIKE 'RENTA MASIVA%' THEN 'MASIVA'
  WHEN UCASE (renta) LIKE 'INSTITUCION%' THEN 'INSTITUCION'
  WHEN UCASE (renta) LIKE 'PYME 1%' THEN 'PYME 1'
  WHEN UCASE (renta) LIKE 'PYME 2%' THEN 'PYME 2'
  WHEN UCASE (renta) LIKE 'COMERCIO%' THEN 'COMERCIO'
  WHEN UCASE (renta) LIKE 'EMPRESA%' THEN 'EMPRESA'
  WHEN UCASE (renta) LIKE '%GRAN%EMPRESA%' THEN 'GRAN EMPRESA'
  ELSE 'VER'
END as ds_nps_renta,
rentabilidad as ds_nps_rentabilidad,
cast(null as string) as ds_nps_segmento,
sexo as ds_per_sexo,
tipo_doc as ds_per_tipo_doc,
'baja cliente' as ds_nps_tipo_encuesta,
to_date(from_unixtime(UNIX_TIMESTAMP(partition_date, 'yyyy-MM-dd'))) as dt_nps_fecha_encuesta,
antiguedad as ds_nps_antiguedad_meses,
rank() over (partition by responseid order by question_id asc) as ds_nps_orden,
question_desc as ds_nps_pregunta,
respuesta as ds_nps_respuesta,id_respuesta,
partition_date
from bi_corp_staging.qualtrics_nps_baja_cliente
where partition_date='{{ ti.xcom_pull(task_ids='InputConfig', key='partition_date', dag_id='LOAD_CMN_NPS-Daily') }}'
and nup is not null


union all

select
lpad(nup,8,0) as cod_per_nup,
response_id as cod_nps_respuesta,
lpad(nro_doc,11,0) as cod_per_nro_doc,
LPAD(cod_suc_uc,4,0) as cod_nps_nro_ubicacion,
q1_nps_group as ds_nps_agrupados,
UCASE(canal) as ds_nps_canal,
submotivo as ds_nps_descripcion,
cast(null as string) as ds_nps_desc_ubicacion,
edad as ds_per_edad,
cast(null as string) as ds_nps_ejecutivo,
ejecutivo as ds_nps_legajo,
cast(null as string) as ds_nps_mejor_prod,
momento_critico as ds_nps_momento_critico,
cast(null as string) as ds_nps_negocio,
nombre as ds_per_nombre,
cast(null as string) as ds_nps_nombre_sucursal,
CASE
   WHEN UCASE (renta) LIKE 'BANCA PRIVADA%' THEN 'BANCA PRIVADA'
   WHEN UCASE (renta) LIKE 'RENTA ALTA SUPERIOR%' THEN 'SELECT'
   WHEN UCASE (renta) LIKE 'RENTA ALTA%' THEN 'ALTA'
   WHEN UCASE (renta) LIKE 'RENTA MEDIA%' THEN 'MEDIA'
   WHEN UCASE (renta) LIKE 'RENTA MASIVA%' THEN 'MASIVA'
   WHEN UCASE (renta) LIKE 'INSTITUCION%' THEN 'INSTITUCION'
   WHEN UCASE (renta) LIKE 'PYME 1%' THEN 'PYME 1'
   WHEN UCASE (renta) LIKE 'PYME 2%' THEN 'PYME 2'
   WHEN UCASE (renta) LIKE 'COMERCIO%' THEN 'COMERCIO'
   WHEN UCASE (renta) LIKE 'EMPRESA%' THEN 'EMPRESA'
   WHEN UCASE (renta) LIKE '%GRAN%EMPRESA%' THEN 'GRAN EMPRESA'
   ELSE 'VER'
END as ds_nps_renta,
rentabilidad as ds_nps_rentabilidad,
segmento as ds_nps_segmento,
sexo as ds_per_sexo,
tipo_doc as ds_per_tipo_doc,
'at. remota sucursales' as ds_nps_tipo_encuesta,
to_date(from_unixtime(UNIX_TIMESTAMP(partition_date, 'yyyy-MM-dd'))) as dt_nps_fecha_encuesta,
antiguedad as ds_nps_antiguedad_meses,
rank() over (partition by responseid order by question_id asc) as ds_nps_orden,
question_desc as ds_nps_pregunta,
respuesta as ds_nps_respuesta,id_respuesta,
partition_date
from bi_corp_staging.qualtrics_nps_ejecutivo_remoto
where partition_date='{{ ti.xcom_pull(task_ids='InputConfig', key='partition_date', dag_id='LOAD_CMN_NPS-Daily') }}'
and nup is not null


union all

Select
LPAD(nup,8,0) as cod_per_nup,
response_id as cod_nps_respuesta,
lpad(nro_doc,11,0) as cod_per_nro_doc,
'CANALES DIGITALES'  as cod_nps_nro_ubicacion,
q1_nps_group as ds_nps_agrupados,
if(cod_canal='0499','ONLINE BANKING',if (cod_canal='70','MOBILE', 'OTROS')) as ds_nps_canal,
desc_articulo as ds_nps_descripcion,
cast(null as string) as ds_nps_desc_ubicacion,
edad as ds_per_edad,
cast(null as string) as ds_nps_ejecutivo,
cast(null as string) as ds_nps_legajo,
cast(null as string) as ds_nps_mejor_prod,
momento_critico as ds_nps_momento_critico,
cast(null as string) as ds_nps_negocio,
nombre as ds_per_nombre,
cast(null as string) as ds_nps_nombre_sucursal,
CASE
    WHEN UCASE (renta) LIKE 'BANCA PRIVADA%' THEN 'BANCA PRIVADA'
    WHEN UCASE (renta) LIKE 'RENTA ALTA SUPERIOR%' THEN 'SELECT'
    WHEN UCASE (renta) LIKE 'RENTA ALTA%' THEN 'ALTA'
    WHEN UCASE (renta) LIKE 'RENTA MEDIA%' THEN 'MEDIA'
    WHEN UCASE (renta) LIKE 'RENTA MASIVA%' THEN 'MASIVA'
    WHEN UCASE (renta) LIKE 'INSTITUCION%' THEN 'INSTITUCION'
    WHEN UCASE (renta) LIKE 'PYME 1%' THEN 'PYME 1'
    WHEN UCASE (renta) LIKE 'PYME 2%' THEN 'PYME 2'
    WHEN UCASE (renta) LIKE 'COMERCIO%' THEN 'COMERCIO'
    WHEN UCASE (renta) LIKE 'EMPRESA%' THEN 'EMPRESA'
    WHEN UCASE (renta) LIKE '%GRAN%EMPRESA%' THEN 'GRAN EMPRESA'
	ELSE 'VER'
END as ds_nps_renta,
rentabilidad as ds_nps_rentabilidad,
cast(null as string) as ds_nps_segmento,
sexo as ds_per_sexo,
tipo_doc as ds_per_tipo_doc,
'superclub fisico' as ds_nps_tipo_encuesta,
to_date(from_unixtime(UNIX_TIMESTAMP(partition_date, 'yyyy-MM-dd'))) as dt_nps_fecha_encuesta,
antiguedad as ds_nps_antiguedad_meses,
rank() over (partition by responseid order by question_id asc) as ds_nps_orden,
question_desc as ds_nps_pregunta,
respuesta as ds_nps_respuesta,id_respuesta,
partition_date
from bi_corp_staging.qualtrics_nps_superclub_fisico
where partition_date='{{ ti.xcom_pull(task_ids='InputConfig', key='partition_date', dag_id='LOAD_CMN_NPS-Daily') }}'
AND nup IS NOT NULL


union all

Select
lpad(nup,8,0) as cod_per_nup,
response_id as cod_nps_respuesta,
mer_cuit as cod_per_nro_doc,
case
when UCASE(mer_codigo_canal)= 'MASCHESUC'  then LPAD(cod_suc_uc,4,0)
when mer_codigo_canal in ('OBE', 'OBP', 'SELF') then 'CANALES DIGITALES'
else LPAD(cod_suc_uc,4,0)
end as cod_nps_nro_ubicacion,
q1_nps_group as ds_nps_agrupados,
case
when mer_codigo_canal = 'MASCHESUC' then 'SUCURSAL'
when mer_codigo_canal = 'OBE' then 'ONLINE BANKING EMPRESAS'
when mer_codigo_canal = 'OBP' then 'ONLINE BANKING'
when mer_codigo_canal = 'SELF' then 'SELF'
else 'OTROS'
end as ds_nps_canal,
'GETNET ALTA' as ds_nps_descripcion,
cast(null as string) as ds_nps_desc_ubicacion,
edad as ds_per_edad,
cast(null as string) as ds_nps_ejecutivo,
cast(null as string) as ds_nps_legajo,
cast(null as string) as ds_nps_mejor_prod,
momento_critico as ds_nps_momento_critico,
cast(null as string) as ds_nps_negocio,
mer_legal_name as ds_per_nombre,
cast(null as string) as ds_nps_nombre_sucursal,
CASE
   WHEN UCASE (renta) LIKE 'BANCA PRIVADA%' THEN 'BANCA PRIVADA'
   WHEN UCASE (renta) LIKE 'RENTA ALTA SUPERIOR%' THEN 'SELECT'
   WHEN UCASE (renta) LIKE 'RENTA ALTA%' THEN 'ALTA'
   WHEN UCASE (renta) LIKE 'RENTA MEDIA%' THEN 'MEDIA'
   WHEN UCASE (renta) LIKE 'RENTA MASIVA%' THEN 'MASIVA'
   WHEN UCASE (renta) LIKE 'INSTITUCION%' THEN 'INSTITUCION'
   WHEN UCASE (renta) LIKE 'PYME 1%' THEN 'PYME 1'
   WHEN UCASE (renta) LIKE 'PYME 2%' THEN 'PYME 2'
   WHEN UCASE (renta) LIKE 'COMERCIO%' THEN 'COMERCIO'
   WHEN UCASE (renta) LIKE 'EMPRESA%' THEN 'EMPRESA'
   WHEN UCASE (renta) LIKE '%GRAN%EMPRESA%' THEN 'GRAN EMPRESA'
   ELSE 'VER'
END as ds_nps_renta,
rentabilidad as ds_nps_rentabilidad,
cast(null as string) as ds_nps_segmento,
sexo as ds_per_sexo,
'CUIT' as ds_per_tipo_doc,
'getnet alta' as ds_nps_tipo_encuesta,
to_date(from_unixtime(UNIX_TIMESTAMP(partition_date, 'yyyy-MM-dd'))) as dt_nps_fecha_encuesta,
antiguedad as ds_nps_antiguedad_meses,
rank() over (partition by responseid order by question_id asc) as ds_nps_orden,
question_desc as ds_nps_pregunta,
respuesta as ds_nps_respuesta,id_respuesta,
partition_date
from bi_corp_staging.qualtrics_nps_getnet_alta
where partition_date='{{ ti.xcom_pull(task_ids='InputConfig', key='partition_date', dag_id='LOAD_CMN_NPS-Daily') }}'
and mer_cuit is not null


union all

Select
lpad(nup,8,0) as cod_per_nup,
response_id as cod_nps_respuesta,
lpad(nro_doc,11,0) as cod_per_nro_doc,
lpad(cod_suc_uc,4,0) as cod_nps_nro_ubicacion,
q1_nps_group as ds_nps_agrupados,
'BANCO' as ds_nps_canal,
cast(null as string) as ds_nps_descripcion,
cast(null as string) as ds_nps_desc_ubicacion,
edad as ds_per_edad,
cast(null as string) as ds_nps_ejecutivo,
cast(null as string) as ds_nps_legajo,
cast(null as string) as ds_nps_mejor_prod,
momento_critico as ds_nps_momento_critico,
cast(null as string) as ds_nps_negocio,
nombre as ds_per_nombre,
cast(null as string) as ds_nps_nombre_sucursal,
CASE
   WHEN UCASE (renta) LIKE 'BANCA PRIVADA%' THEN 'BANCA PRIVADA'
   WHEN UCASE (renta) LIKE 'RENTA ALTA SUPERIOR%' THEN 'SELECT'
   WHEN UCASE (renta) LIKE 'RENTA ALTA%' THEN 'ALTA'
   WHEN UCASE (renta) LIKE 'RENTA MEDIA%' THEN 'MEDIA'
   WHEN UCASE (renta) LIKE 'RENTA MASIVA%' THEN 'MASIVA'
   WHEN UCASE (renta) LIKE 'INSTITUCION%' THEN 'INSTITUCION'
   WHEN UCASE (renta) LIKE 'PYME 1%' THEN 'PYME 1'
   WHEN UCASE (renta) LIKE 'PYME 2%' THEN 'PYME 2'
   WHEN UCASE (renta) LIKE 'COMERCIO%' THEN 'COMERCIO'
   WHEN UCASE (renta) LIKE 'EMPRESA%' THEN 'EMPRESA'
   WHEN UCASE (renta) LIKE '%GRAN%EMPRESA%' THEN 'GRAN EMPRESA'
   ELSE 'VER'
END as ds_nps_renta,
rentabilidad as ds_nps_rentabilidad,
cast(null as string) as ds_nps_segmento,
sexo as ds_per_sexo,
tipo_doc as ds_per_tipo_doc,
'relacional promo' as ds_nps_tipo_encuesta,
to_date(from_unixtime(UNIX_TIMESTAMP(partition_date, 'yyyy-MM-dd'))) as dt_nps_fecha_encuesta,
antiguedad as ds_nps_antiguedad_meses,
rank() over (partition by responseid order by question_id asc) as ds_nps_orden,
question_desc as ds_nps_pregunta,
respuesta as ds_nps_respuesta,id_respuesta,
partition_date
from bi_corp_staging.qualtrics_nps_relacional_promo
where partition_date='{{ ti.xcom_pull(task_ids='InputConfig', key='partition_date', dag_id='LOAD_CMN_NPS-Daily') }}'
and nup is not null


union all

Select
lpad(nup,8,0) as cod_per_nup,
responseid as cod_nps_respuesta,
lpad(nro_doc,11,0) as cod_per_nro_doc,
lpad(cod_suc_uc,4,0) as cod_nps_nro_ubicacion,
q1_nps_group as ds_nps_agrupados,
'OTROS' as ds_nps_canal,
cast(null as string) as ds_nps_descripcion,
cast(null as string) as ds_nps_desc_ubicacion,
edad as ds_per_edad,
cast(null as string) as ds_nps_ejecutivo,
cast(null as string) as ds_nps_legajo,
desc_mej_prod as ds_nps_mejor_prod,
momento_critico as ds_nps_momento_critico,
cast(null as string) as ds_nps_negocio,
nombre as ds_per_nombre,
cast(null as string) as ds_nps_nombre_sucursal,
CASE
   WHEN UCASE (renta) LIKE 'BANCA PRIVADA%' THEN 'BANCA PRIVADA'
   WHEN UCASE (renta) LIKE 'RENTA ALTA SUPERIOR%' THEN 'SELECT'
   WHEN UCASE (renta) LIKE 'RENTA ALTA%' THEN 'ALTA'
   WHEN UCASE (renta) LIKE 'RENTA MEDIA%' THEN 'MEDIA'
   WHEN UCASE (renta) LIKE 'RENTA MASIVA%' THEN 'MASIVA'
   WHEN UCASE (renta) LIKE 'INSTITUCION%' THEN 'INSTITUCION'
   WHEN UCASE (renta) LIKE 'PYME 1%' THEN 'PYME 1'
   WHEN UCASE (renta) LIKE 'PYME 2%' THEN 'PYME 2'
   WHEN UCASE (renta) LIKE 'COMERCIO%' THEN 'COMERCIO'
   WHEN UCASE (renta) LIKE 'EMPRESA%' THEN 'EMPRESA'
   WHEN UCASE (renta) LIKE '%GRAN%EMPRESA%' THEN 'GRAN EMPRESA'
   ELSE 'VER'
END as ds_nps_renta,
rentabilidad as ds_nps_rentabilidad,
cast(null as string) as ds_nps_segmento,
sexo as ds_per_sexo,
tipo_doc as ds_per_tipo_doc,
'consumo td' as ds_nps_tipo_encuesta,
to_date(from_unixtime(UNIX_TIMESTAMP(partition_date, 'yyyy-MM-dd'))) as dt_nps_fecha_encuesta,
antiguedad as ds_nps_antiguedad_meses,
rank() over (partition by responseid order by question_id asc) as ds_nps_orden,
question_desc as ds_nps_pregunta,
respuesta as ds_nps_respuesta,id_respuesta,
partition_date
from bi_corp_staging.qualtrics_nps_consumo_td
where partition_date='{{ ti.xcom_pull(task_ids='InputConfig', key='partition_date', dag_id='LOAD_CMN_NPS-Daily') }}'
and nup is not null


union all

Select
lpad(nup,8,0) as cod_per_nup,
responseid as cod_nps_respuesta,
lpad(nro_doc,11,0) as cod_per_nro_doc,
lpad(cod_suc_uc,4,0) as cod_nps_nro_ubicacion,
q1_nps_group as ds_nps_agrupados,
'OTROS' as ds_nps_canal,
cast(null as string) as ds_nps_descripcion,
cast(null as string) as ds_nps_desc_ubicacion,
edad as ds_per_edad,
cast(null as string) as ds_nps_ejecutivo,
cast(null as string) as ds_nps_legajo,
desc_mej_prod as ds_nps_mejor_prod,
momento_critico as ds_nps_momento_critico,
cast(null as string) as ds_nps_negocio,
nombre as ds_per_nombre,
cast(null as string) as ds_nps_nombre_sucursal,
CASE
   WHEN UCASE (renta) LIKE 'BANCA PRIVADA%' THEN 'BANCA PRIVADA'
   WHEN UCASE (renta) LIKE 'RENTA ALTA SUPERIOR%' THEN 'SELECT'
   WHEN UCASE (renta) LIKE 'RENTA ALTA%' THEN 'ALTA'
   WHEN UCASE (renta) LIKE 'RENTA MEDIA%' THEN 'MEDIA'
   WHEN UCASE (renta) LIKE 'RENTA MASIVA%' THEN 'MASIVA'
   WHEN UCASE (renta) LIKE 'INSTITUCION%' THEN 'INSTITUCION'
   WHEN UCASE (renta) LIKE 'PYME 1%' THEN 'PYME 1'
   WHEN UCASE (renta) LIKE 'PYME 2%' THEN 'PYME 2'
   WHEN UCASE (renta) LIKE 'COMERCIO%' THEN 'COMERCIO'
   WHEN UCASE (renta) LIKE 'EMPRESA%' THEN 'EMPRESA'
   WHEN UCASE (renta) LIKE '%GRAN%EMPRESA%' THEN 'GRAN EMPRESA'
   ELSE 'VER'
END  as ds_nps_renta,
rentabilidad as ds_nps_rentabilidad,
cast(null as string) as ds_nps_segmento,
sexo as ds_per_sexo,
tipo_doc as ds_per_tipo_doc,
'consumo tc' as ds_nps_tipo_encuesta,
to_date(from_unixtime(UNIX_TIMESTAMP(partition_date, 'yyyy-MM-dd'))) as dt_nps_fecha_encuesta,
antiguedad as ds_nps_antiguedad_meses,
rank() over (partition by responseid order by question_id asc) as ds_nps_orden,
question_desc as ds_nps_pregunta,
respuesta as ds_nps_respuesta,id_respuesta,
partition_date
from bi_corp_staging.qualtrics_nps_consumo_tc
where partition_date='{{ ti.xcom_pull(task_ids='InputConfig', key='partition_date', dag_id='LOAD_CMN_NPS-Daily') }}'
and nup is not null


union all

Select
lpad(nup,8,0) as cod_per_nup,
responseid as cod_nps_respuesta,
lpad(nro_doc,11,0) as cod_per_nro_doc,
'CANALES DIGITALES' as cod_nps_nro_ubicacion,
q1_nps_group as ds_nps_agrupados,
case
    when UCASE(canal) like 'APPM%' then 'MOBILE'
    when UCASE(canal) like 'TBAN%' then 'ONLINE BANKING'
    else 'ONLINE BANKING'
end as ds_nps_canal,
UCASE(intent) as ds_nps_descripcion,
cast(null as string) as ds_nps_desc_ubicacion,
edad as ds_per_edad,
cast(null as string) as ds_nps_ejecutivo,
asesor as ds_nps_legajo,
cast(null as string) as ds_nps_mejor_prod,
momento_critico as ds_nps_momento_critico,
cast(null as string) as ds_nps_negocio,
nombre as ds_per_nombre,
cast(null as string) as ds_nps_nombre_sucursal,
CASE
   WHEN UCASE (renta) LIKE '%BANCA PRIVADA%' THEN 'BANCA PRIVADA'
   WHEN UCASE (renta) LIKE '%RENTA ALTA SUPERIOR%' THEN 'SELECT'
   WHEN UCASE (renta) LIKE '%SELECT%' THEN 'SELECT'
   WHEN UCASE (renta) LIKE '%RENTA ALTA%' THEN 'ALTA'
   WHEN UCASE (renta) LIKE '%RENTA MEDIA%' THEN 'MEDIA'
   WHEN UCASE (renta) LIKE '%RENTA MASIVA%' THEN 'MASIVA'
   WHEN UCASE (renta) LIKE '%INSTITUCION%' THEN 'INSTITUCION'
   WHEN UCASE (renta) LIKE '%COMERCIO%' THEN 'COMERCIO'
   WHEN UCASE (renta) LIKE 'PYME 1%' THEN 'PYME 1'
   WHEN UCASE (renta) LIKE 'PYME 2%' THEN 'PYME 2'
   WHEN UCASE (renta) LIKE '%EMPRESA%' THEN 'EMPRESA'
   WHEN UCASE (renta) LIKE '%GRAN%EMPRESA%' THEN 'GRAN EMPRESA'
   ELSE 'VER'
END as ds_nps_renta,
rentabilidad as ds_nps_rentabilidad,
segmento as ds_nps_segmento,
sexo as ds_per_sexo,
tipo_doc as ds_per_tipo_doc,
'watson' as ds_nps_tipo_encuesta,
to_date(from_unixtime(UNIX_TIMESTAMP(partition_date, 'yyyy-MM-dd'))) as dt_nps_fecha_encuesta,
antiguedad as ds_nps_antiguedad_meses,
rank() over (partition by responseid order by question_id asc) as ds_nps_orden,
question_desc as ds_nps_pregunta,
respuesta as ds_nps_respuesta,id_respuesta,
partition_date
from bi_corp_staging.qualtrics_nps_watson
where partition_date='{{ ti.xcom_pull(task_ids='InputConfig', key='partition_date', dag_id='LOAD_CMN_NPS-Daily') }}'
and nup is not null


union all

Select
lpad(nup,8,0) as cod_per_nup,
responseid as cod_nps_respuesta,
LPAD (nro_Doc, 11, 0) as cod_per_nro_doc,
lpad (cod_suc_uc,4,0) as cod_nps_nro_ubicacion,
q1_nps_group as ds_nps_agrupados,
'BANCO' as ds_nps_canal,
cast(null as string) as ds_nps_descripcion,
cast(null as string) as ds_nps_desc_ubicacion,
cast(null as string) as ds_per_edad,
cast(null as string) as ds_nps_ejecutivo,
cast(null as string) as ds_nps_legajo,
cast(null as string) as ds_nps_mejor_prod,
'RELACIONAL_PERSONAS_JURIDICAS'  as ds_nps_momento_critico,
cast(null as string) as ds_nps_negocio,
nombre as ds_per_nombre,
cast(null as string) as ds_nps_nombre_sucursal,
CASE
   WHEN UCASE (renta) LIKE '%BANCA PRIVADA%' THEN 'BANCA PRIVADA'
   WHEN UCASE (renta) LIKE '%RENTA ALTA SUPERIOR%' THEN 'SELECT'
   WHEN UCASE (renta) LIKE '%RENTA SELECT%' THEN 'SELECT'
   WHEN UCASE (renta) LIKE '%RENTA ALTA%' THEN 'ALTA'
   WHEN UCASE (renta) LIKE '%RENTA MEDIA%' THEN 'MEDIA'
   WHEN UCASE (renta) LIKE '%RENTA MASIVA%' THEN 'MASIVA'
   WHEN UCASE (renta) LIKE '%RENTA BAJA%' THEN 'MASIVA'
   WHEN UCASE (renta) LIKE '%INSTITUCION%' THEN 'INSTITUCION'
   WHEN UCASE (renta) LIKE '%PYME 1%' THEN 'PYME 1'
   WHEN UCASE (renta) LIKE '%PYME 2%' THEN 'PYME 2'
   WHEN UCASE (renta) LIKE '%COMERCIO%' THEN 'COMERCIO'
   WHEN UCASE (renta) LIKE '%EMPRESA%' THEN 'EMPRESA'
   WHEN UCASE (renta) LIKE '%GRAN%EMPRESA%' THEN 'GRAN EMPRESA'
   ELSE 'VER'
END as ds_nps_renta,
cast(null as decimal(12,2)) as ds_nps_rentabilidad,
cast(null as string) as ds_nps_segmento,
cast(null as string) as ds_per_sexo,
tipo_doc as ds_per_tipo_doc,
'relacional pj' as ds_nps_tipo_encuesta,
to_date(from_unixtime(UNIX_TIMESTAMP(partition_date, 'yyyy-MM-dd'))) as dt_nps_fecha_encuesta,
antiguedad as ds_nps_antiguedad_meses,
rank() over (partition by responseid order by question_id asc) as ds_nps_orden,
question_desc as ds_nps_pregunta,
respuesta as ds_nps_respuesta,id_respuesta,
partition_date
from bi_corp_staging.qualtrics_nps_relacional_pj
where partition_date='{{ ti.xcom_pull(task_ids='InputConfig', key='partition_date', dag_id='LOAD_CMN_NPS-Daily') }}'
and nup is not null


union all

Select
lpad(nup,8,0) as cod_per_nup,
responseid as cod_nps_respuesta,
LPAD (nro_Doc, 11, 0) as cod_per_nro_doc,
lpad (cod_suc_uc,4,0) as cod_nps_nro_ubicacion,
q1_nps_group as ds_nps_agrupados,
'BANCO' as ds_nps_canal,
cast(null as string) as ds_nps_descripcion,
cast(null as string) as ds_nps_desc_ubicacion,
edad as ds_per_edad,
cast(null as string) as ds_nps_ejecutivo,
cast(null as string) as ds_nps_legajo,
desc_mejor_prod as ds_nps_mejor_prod,
'RELACIONAL_DUO' as ds_nps_momento_critico,
cast(null as string) as ds_nps_negocio,
nombre as ds_per_nombre,
cast(null as string) as ds_nps_nombre_sucursal,
CASE
    WHEN UCASE (renta) LIKE '%BANCA PRIVADA%' THEN 'BANCA PRIVADA'
    WHEN UCASE (renta) LIKE '%RENTA ALTA SUPERIOR%' THEN 'SELECT'
    WHEN UCASE (renta) LIKE '%RENTA SELECT%' THEN 'SELECT'
    WHEN UCASE (renta) LIKE '%RENTA ALTA%' THEN 'ALTA'
    WHEN UCASE (renta) LIKE '%RENTA MEDIA%' THEN 'MEDIA'
    WHEN UCASE (renta) LIKE '%RENTA MASIVA%' THEN 'MASIVA'
    WHEN UCASE (renta) LIKE '%RENTA BAJA%' THEN 'MASIVA'
    WHEN UCASE (renta) LIKE '%INSTITUCION%' THEN 'INSTITUCION'
    WHEN UCASE (renta) LIKE '%PYME 1%' THEN 'PYME 1'
    WHEN UCASE (renta) LIKE '%PYME 2%' THEN 'PYME 2'
    WHEN UCASE (renta) LIKE '%COMERCIO%' THEN 'COMERCIO'
    WHEN UCASE (renta) LIKE '%EMPRESA%' THEN 'EMPRESA'
    WHEN UCASE (renta) LIKE '%GRAN%EMPRESA%' THEN 'GRAN EMPRESA'
    ELSE 'VER'
END as ds_nps_renta,
cast(null as decimal(12,2)) as ds_nps_rentabilidad,
cast(null as string) as ds_nps_segmento,
sexo as ds_per_sexo,
tipo_doc as ds_per_tipo_doc,
'relacional duo' as ds_nps_tipo_encuesta,
to_date(from_unixtime(UNIX_TIMESTAMP(partition_date, 'yyyy-MM-dd'))) as dt_nps_fecha_encuesta,
antiguedad as ds_nps_antiguedad_meses,
rank() over (partition by responseid order by question_id asc) as ds_nps_orden,
question_desc as ds_nps_pregunta,
respuesta as ds_nps_respuesta,id_respuesta,
partition_date
from bi_corp_staging.qualtrics_nps_relacional_duo
where partition_date='{{ ti.xcom_pull(task_ids='InputConfig', key='partition_date', dag_id='LOAD_CMN_NPS-Daily') }}'
and nup is not null


union all

Select
lpad(nup,8,0) as cod_per_nup,
responseid as cod_nps_respuesta,
lpad(nro_doc,11,0) as cod_per_nro_doc,
case
when desc_canal in ('OBCM','OBP') then 'CANALES DIGITALES'
else  lpad(cod_suc_uc,4,0)
end as cod_nps_nro_ubicacion,
q1_nps_group as ds_nps_agrupados,
if (desc_canal='OBCM','CASH_MANAGEMENT',if (desc_canal='OBP', 'ONLINE BANKING',if(desc_canal='Carga Manual','OTROS', UCASE(desc_canal)))) as ds_nps_canal,
sector as ds_nps_descripcion,
nombre_suc as ds_nps_desc_ubicacion,
edad as ds_per_edad,
cast(null as string) as ds_nps_ejecutivo,
cast(null as string) as ds_nps_legajo,
cast(null as string) as ds_nps_mejor_prod,
'COMERCIO_EXTERIOR' as ds_nps_momento_critico,
cast(null as string) as ds_nps_negocio,
nombre as ds_per_nombre,
nombre_suc as ds_nps_nombre_sucursal,
CASE
   WHEN UCASE (renta) LIKE 'BANCA PRIVADA%' THEN 'BANCA PRIVADA'
   WHEN UCASE (renta) LIKE 'RENTA ALTA SUPERIOR%' THEN 'SELECT'
   WHEN UCASE (renta) LIKE 'RENTA ALTA%' THEN 'ALTA'
   WHEN UCASE (renta) LIKE 'RENTA MEDIA%' THEN 'MEDIA'
   WHEN UCASE (renta) LIKE 'RENTA MASIVA%' THEN 'MASIVA'
   WHEN UCASE (renta) LIKE 'PYME 1%' THEN 'PYME 1'
   WHEN UCASE (renta) LIKE 'PYME 2%' THEN 'PYME 2'
   WHEN UCASE (renta) LIKE 'INSTITUCION%' THEN 'INSTITUCION'
   WHEN UCASE (renta) LIKE 'COMERCIO%' THEN 'COMERCIO'
   WHEN UCASE (renta) LIKE 'EMPRESA%' THEN 'EMPRESA'
   WHEN UCASE (renta) LIKE '%GRAN%EMPRESA%' THEN 'GRAN EMPRESA'
   ELSE 'VER'
END  as ds_nps_renta,
rentabilidad as ds_nps_rentabilidad,
marcarenta as ds_nps_segmento,
sexo as ds_per_sexo,
tipo_doc as ds_per_tipo_doc,
'comex' as ds_nps_tipo_encuesta,
to_date(from_unixtime(UNIX_TIMESTAMP(partition_date, 'yyyy-MM-dd'))) as dt_nps_fecha_encuesta,
antiguedad as ds_nps_antiguedad_meses,
rank() over (partition by responseid order by question_id asc) as ds_nps_orden,
question_desc as ds_nps_pregunta,
respuesta as ds_nps_respuesta,id_respuesta,
partition_date
from bi_corp_staging.qualtrics_nps_comex
where partition_date='{{ ti.xcom_pull(task_ids='InputConfig', key='partition_date', dag_id='LOAD_CMN_NPS-Daily') }}'
and nup is not null


union all

Select
lpad(nup,8,0) as cod_per_nup,
responseid as cod_nps_respuesta,
lpad(nro_doc,11,0) as cod_per_nro_doc,
LPAD(cod_suc_uc,4,0) as cod_nps_nro_ubicacion,
q1_nps_group as ds_nps_agrupados,
if (cod_canal = '02','CONCESIONARIOS','SUCURSAL') as ds_nps_canal,
concat('PRESTAMOS PRENDARIOS ',if (producto='39','PYME',' ')) as ds_nps_descripcion,
cast(null as string) as ds_nps_desc_ubicacion,
edad as ds_per_edad,
cast(null as string) as ds_nps_ejecutivo,
cast(null as string) as ds_nps_legajo,
cast(null as string) as ds_nps_mejor_prod,
momento_critico as ds_nps_momento_critico,
cast(null as string) as ds_nps_negocio,
nombre as ds_per_nombre,
cast(null as string) as ds_nps_nombre_sucursal,
CASE
   WHEN UCASE (renta) LIKE 'BANCA PRIVADA%' THEN 'BANCA PRIVADA'
   WHEN UCASE (renta) LIKE 'RENTA ALTA SUPERIOR%' THEN 'SELECT'
   WHEN UCASE (renta) LIKE 'RENTA ALTA%' THEN 'ALTA'
   WHEN UCASE (renta) LIKE 'RENTA MEDIA%' THEN 'MEDIA'
   WHEN UCASE (renta) LIKE 'RENTA MASIVA%' THEN 'MASIVA'
   WHEN UCASE (renta) LIKE 'INSTITUCION%' THEN 'INSTITUCION'
   WHEN UCASE (renta) LIKE 'PYME 1%' THEN 'PYME 1'
   WHEN UCASE (renta) LIKE 'PYME 2%' THEN 'PYME 2'
   WHEN UCASE (renta) LIKE 'COMERCIO%' THEN 'COMERCIO'
   WHEN UCASE (renta) LIKE 'EMPRESA%' THEN 'EMPRESA'
   WHEN UCASE (renta) LIKE '%GRAN%EMPRESA%' THEN 'GRAN EMPRESA'
   ELSE 'VER'
END as ds_nps_renta,
rentabilidad as ds_nps_rentabilidad,
segmento as ds_nps_segmento,
sexo as ds_per_sexo,
tipo_doc as ds_per_tipo_doc,
'prendarios' as ds_nps_tipo_encuesta,
to_date(from_unixtime(UNIX_TIMESTAMP(partition_date, 'yyyy-MM-dd'))) as dt_nps_fecha_encuesta,
antiguedad as ds_nps_antiguedad_meses,
rank() over (partition by responseid order by question_id asc) as ds_nps_orden,
question_desc as ds_nps_pregunta,
respuesta as ds_nps_respuesta,id_respuesta,
partition_date
from bi_corp_staging.qualtrics_nps_prendarios
where partition_date='{{ ti.xcom_pull(task_ids='InputConfig', key='partition_date', dag_id='LOAD_CMN_NPS-Daily') }}'
and nup is not null


union all

Select
lpad(nup,8,0) as cod_per_nup,
responseid as cod_nps_respuesta,
lpad(nro_doc,11,0) as cod_per_nro_doc,
if (ucase(nro_ubicacion) like '/FUERZA COMERCIAL', 'FUERZA COMERCIAL', IF(nro_ubicacion like 'A%',ubicacion,if (nro_ubicacion like 'B%',ubicacion,UCASE(nro_ubicacion)))) as cod_nps_nro_ubicacion,
q1_nps_group as ds_nps_agrupados,
case
    when UCASE(negociodesc) like '%VIP%' then 'EJEC. REMOTO VIP'
    when UCASE(negociodesc) not like '%VIP%' and UCASE(negociodesc) like '%DUO%' then 'EJEC. REMOTO DUO'
    when UCASE(negociodesc) not like '%VIP%' and UCASE(negociodesc) not like '%DUO%' and UCASE(negociodesc) like '%AGRO%' then 'EJEC. REMOTO AGRO'
    when UCASE(negociodesc) not like '%VIP%' and UCASE(negociodesc) not like '%DUO%' and UCASE(negociodesc) not like '%AGRO%' and UCASE(negociodesc) like '%SELECT%' then 'EJEC. REMOTO SELECT'
    when UCASE(nro_ubicacion) like '%SYSTEM%' then 'CONTACT CENTER'
    when UCASE(nro_ubicacion) like '%FUERZA COMERCIAL%' then 'FUERZA COMERCIAL'
    when UCASE(canal) like 'SUCURSAL' and (UCASE(negociodesc) like '%TLMK OUT MA¿ANA%' or UCASE(negociodesc) like '%TLMK IN MA¿ANA%') and ucase(nro_ubicacion) like '%GRUPO%'  then 'CONTACT CENTER'
    when UCASE(canal) like 'SUCURSAL' and ucase(nro_ubicacion) like '%SISTEMAS ESTRATEGICOS%'  then 'CONTACT CENTER'
    when UCASE(canal) like 'SUCURSAL' and ucase(nro_ubicacion) like '%SIN GESTION%' and UCASE(negociodesc) like '%GONZALEZ V'  then 'CONTACT CENTER'
    when UCASE(canal) is null then 'OTROS'
    else UCASE(canal)
end as ds_nps_canal,
concat (UCASE(tipo_evento), ' ', desc_prod_contab) as ds_nps_descripcion,
if (ubicacion= '/Fuerza Comercial','FUERZA COMERCIAL',ubicacion) as ds_nps_desc_ubicacion,
edad as ds_per_edad,
cast(null as string) as ds_nps_ejecutivo,
usuario_venta as ds_nps_legajo,
cast(null as string) as ds_nps_mejor_prod,
UCASE(momento_critico) as ds_nps_momento_critico,
cast(null as string) as ds_nps_negocio,
nombre as ds_per_nombre,
cast(null as string) as ds_nps_nombre_sucursal,
CASE
   WHEN UCASE (renta) LIKE 'BANCA PRIVADA%' THEN 'BANCA PRIVADA'
   WHEN UCASE (renta) LIKE 'RENTA ALTA SUPERIOR%' THEN 'SELECT'
   WHEN UCASE (renta) LIKE 'RENTA ALTA%' THEN 'ALTA'
   WHEN UCASE (renta) LIKE 'RENTA MEDIA%' THEN 'MEDIA'
   WHEN UCASE (renta) LIKE 'RENTA MASIVA%' THEN 'MASIVA'
   WHEN UCASE (renta) LIKE 'INSTITUCION%' THEN 'INSTITUCION'
   WHEN UCASE (renta) LIKE 'PYME 1%' THEN 'PYME 1'
   WHEN UCASE (renta) LIKE 'PYME 2%' THEN 'PYME 2'
   WHEN UCASE (renta) LIKE 'COMERCIO%' THEN 'COMERCIO'
   WHEN UCASE (renta) LIKE 'EMPRESA%' THEN 'EMPRESA'
   WHEN UCASE (renta) LIKE '%GRAN%EMPRESA%' THEN 'GRAN EMPRESA'
   ELSE 'VER'
END as ds_nps_renta,
rentabilidad as ds_nps_rentabilidad,
cast(null as string) as ds_nps_segmento,
sexo as ds_per_sexo,
tipo_doc as ds_per_tipo_doc,
'onboarding 90d' as ds_nps_tipo_encuesta,
to_date(from_unixtime(UNIX_TIMESTAMP(partition_date, 'yyyy-MM-dd'))) as dt_nps_fecha_encuesta,
antiguedad as ds_nps_antiguedad_meses,
rank() over (partition by responseid order by question_id asc) as ds_nps_orden,
question_desc as ds_nps_pregunta,
respuesta as ds_nps_respuesta,id_respuesta,
partition_date
from bi_corp_staging.qualtrics_nps_onboarding_90d
where partition_date='{{ ti.xcom_pull(task_ids='InputConfig', key='partition_date', dag_id='LOAD_CMN_NPS-Daily') }}'
and nup is not null


union all

Select
lpad(nup,8,0) as cod_per_nup,
responseid as cod_nps_respuesta,
cuit as cod_per_nro_doc,
case
when UCASE(codigo_canal)= 'MASCHESUC'  then LPAD(cod_suc_uc,4,0)
when codigo_canal in ('OBE', 'OBP', 'SELF') then 'CANALES DIGITALES'
else LPAD(cod_suc_uc,4,0)
end as cod_nps_nro_ubicacion,
q1_nps_group as ds_nps_agrupados,
if (UCASE(codigo_canal)= 'MASCHESUC', 'SUCURSAL',if (UCASE(codigo_canal)='OBE', 'ONLINE BANKING EMPRESAS', if( UCASE(codigo_canal)='OBP', 'ONLINE BANKING',if (UCASE(codigo_canal) is NULL,'OTROS',UCASE(codigo_canal))))) as ds_nps_canal,
'GETNET COBROS' as ds_nps_descripcion,
cast(null as string) as ds_nps_desc_ubicacion,
edad as ds_per_edad,
cast(null as string) as ds_nps_ejecutivo,
cast(null as string) as ds_nps_legajo,
cast(null as string) as ds_nps_mejor_prod,
UCASE(momento_critico) as ds_nps_momento_critico,
cast(null as string) as ds_nps_negocio,
legal_name as ds_per_nombre,
cast(null as string) as ds_nps_nombre_sucursal,
CASE
    WHEN UCASE (renta) LIKE 'BANCA PRIVADA%' THEN 'BANCA PRIVADA'
    WHEN UCASE (renta) LIKE 'RENTA ALTA SUPERIOR%' THEN 'SELECT'
    WHEN UCASE (renta) LIKE 'RENTA ALTA%' THEN 'ALTA'
    WHEN UCASE (renta) LIKE 'RENTA MEDIA%' THEN 'MEDIA'
    WHEN UCASE (renta) LIKE 'RENTA MASIVA%' THEN 'MASIVA'
    WHEN UCASE (renta) LIKE 'INSTITUCION%' THEN 'INSTITUCION'
    WHEN UCASE (renta) LIKE 'PYME 1%' THEN 'PYME 1'
    WHEN UCASE (renta) LIKE 'PYME 2%' THEN 'PYME 2'
    WHEN UCASE (renta) LIKE 'COMERCIO%' THEN 'COMERCIO'
    WHEN UCASE (renta) LIKE 'EMPRESA%' THEN 'EMPRESA'
    WHEN UCASE (renta) LIKE '%GRAN%EMPRESA%' THEN 'GRAN EMPRESA'
    ELSE 'VER'
END as ds_nps_renta,
rentabilidad as ds_nps_rentabilidad,
cast(null as string) as ds_nps_segmento,
sexo as ds_per_sexo,
'CUIT' as ds_per_tipo_doc,
'getnet mpago' as ds_nps_tipo_encuesta,
to_date(from_unixtime(UNIX_TIMESTAMP(partition_date, 'yyyy-MM-dd'))) as dt_nps_fecha_encuesta,
antiguedad as ds_nps_antiguedad_meses,
rank() over (partition by responseid order by question_id asc) as ds_nps_orden,
question_desc as ds_nps_pregunta,
respuesta as ds_nps_respuesta,id_respuesta,
partition_date
from bi_corp_staging.qualtrics_nps_getnet_mpago
where partition_date='{{ ti.xcom_pull(task_ids='InputConfig', key='partition_date', dag_id='LOAD_CMN_NPS-Daily') }}'
and cuit is not null


union all

Select
lpad(nup,8,0) as cod_per_nup,
responseid as cod_nps_respuesta,
cuit as cod_per_nro_doc,
cod_suc_uc as cod_nps_nro_ubicacion,
q1_nps_group as ds_nps_agrupados,
'DELIVERY' as ds_nps_canal,
'GETNET DELIVERY APARATITO' as ds_nps_descripcion,
cast(null as string) as ds_nps_desc_ubicacion,
edad as ds_per_edad,
cast(null as string) as ds_nps_ejecutivo,
cast(null as string) as ds_nps_legajo,
cast(null as string) as ds_nps_mejor_prod,
UCASE(momento_critico) as ds_nps_momento_critico,
cast(null as string) as ds_nps_negocio,
legal_name as ds_per_nombre,
cast(null as string) as ds_nps_nombre_sucursal,
CASE
    WHEN UCASE (renta) LIKE 'BANCA PRIVADA%' THEN 'BANCA PRIVADA'
    WHEN UCASE (renta) LIKE 'RENTA ALTA SUPERIOR%' THEN 'SELECT'
    WHEN UCASE (renta) LIKE 'RENTA ALTA%' THEN 'ALTA'
    WHEN UCASE (renta) LIKE 'RENTA MEDIA%' THEN 'MEDIA'
    WHEN UCASE (renta) LIKE 'RENTA MASIVA%' THEN 'MASIVA'
    WHEN UCASE (renta) LIKE 'INSTITUCION%' THEN 'INSTITUCION'
    WHEN UCASE (renta) LIKE 'PYME 1%' THEN 'PYME 1'
    WHEN UCASE (renta) LIKE 'PYME 2%' THEN 'PYME 2'
    WHEN UCASE (renta) LIKE 'COMERCIO%' THEN 'COMERCIO'
    WHEN UCASE (renta) LIKE 'EMPRESA%' THEN 'EMPRESA'
    WHEN UCASE (renta) LIKE '%GRAN%EMPRESA%' THEN 'GRAN EMPRESA'
    ELSE 'VER'
END as ds_nps_renta,
rentabilidad as ds_nps_rentabilidad,
cast(null as string) as ds_nps_segmento,
sexo as ds_per_sexo,
'CUIT' as ds_per_tipo_doc,
'getnet dongle' as ds_nps_tipo_encuesta,
to_date(from_unixtime(UNIX_TIMESTAMP(partition_date, 'yyyy-MM-dd'))) as dt_nps_fecha_encuesta,
antiguedad as ds_nps_antiguedad_meses,
rank() over (partition by responseid order by question_id asc) as ds_nps_orden,
question_desc as ds_nps_pregunta,
respuesta as ds_nps_respuesta,id_respuesta,
partition_date
from bi_corp_staging.qualtrics_nps_getnet_dongle
where partition_date='{{ ti.xcom_pull(task_ids='InputConfig', key='partition_date', dag_id='LOAD_CMN_NPS-Daily') }}'
and cuit is not null


union all

Select
lpad(nup,8,0) as cod_per_nup,
response_id as cod_nps_respuesta,
LPAD (nro_doc, 11, 0) as cod_per_nro_doc,
sucursal_id  as cod_nps_nro_ubicacion,
q1_nps_group as ds_nps_agrupados,
corresponsalia as ds_nps_canal,
CASE
 WHEN UCASE (tipo_transacc) LIKE '%DEP%SITO%' THEN 'DEPOSITO'
 WHEN UCASE (tipo_transacc) like '%EXTRACCI%N%' THEN 'EXTRACCION'
 else UCASE (tipo_transacc)
end  as ds_nps_descripcion,
sucursal_nombre as ds_nps_desc_ubicacion,
edad as ds_per_edad,
cast(null as string) as ds_nps_ejecutivo,
cast(null as string) as ds_nps_legajo,
cast(null as string) as ds_nps_mejor_prod,
'CORRESPONSALIAS' as ds_nps_momento_critico,
cast(null as string) as ds_nps_negocio,
nombre as ds_per_nombre,
cast(null as string) as ds_nps_nombre_sucursal,
CASE
   WHEN UCASE (renta) LIKE 'BANCA PRIVADA%' THEN 'BANCA PRIVADA'
   WHEN UCASE (renta) LIKE 'RENTA ALTA SUPERIOR%' THEN 'SELECT'
   WHEN UCASE (renta) LIKE 'RENTA ALTA%' THEN 'ALTA'
   WHEN UCASE (renta) LIKE 'RENTA MEDIA%' THEN 'MEDIA'
   WHEN UCASE (renta) LIKE 'RENTA MASIVA%' THEN 'MASIVA'
   WHEN UCASE (renta) LIKE 'INSTITUCION%' THEN 'INSTITUCION'
   WHEN UCASE (renta) LIKE 'PYME 1%' THEN 'PYME 1'
   WHEN UCASE (renta) LIKE 'PYME 2%' THEN 'PYME 2'
   WHEN UCASE (renta) LIKE 'COMERCIO%' THEN 'COMERCIO'
   WHEN UCASE (renta) LIKE 'EMPRESA%' THEN 'EMPRESA'
   WHEN UCASE (renta) LIKE '%GRAN%EMPRESA%' THEN 'GRAN EMPRESA'
   ELSE 'VER'
END as ds_nps_renta,
rentabilidad as ds_nps_rentabilidad,
cast(null as string) as ds_nps_segmento,
sexo as ds_per_sexo,
tipo_doc as ds_per_tipo_doc,
'corresponsalias' as ds_nps_tipo_encuesta,
to_date(from_unixtime(UNIX_TIMESTAMP(partition_date, 'yyyy-MM-dd'))) as dt_nps_fecha_encuesta,
cast(null as string) as ds_nps_antiguedad_meses,
rank() over (partition by response_id order by question_id asc) as ds_nps_orden,
question_desc as ds_nps_pregunta,
respuesta as ds_nps_respuesta,id_respuesta,
partition_date
from bi_corp_staging.qualtrics_nps_corresponsalias
where partition_date='{{ ti.xcom_pull(task_ids='InputConfig', key='partition_date', dag_id='LOAD_CMN_NPS-Daily') }}'
and nup is not null


union all

Select
lpad(nup_firmante,8,0) as cod_per_nup,
response_id as cod_nps_respuesta,
lpad(nro_doc,11,0) as cod_per_nro_doc,
LPAD(cod_suc_uc,4,0) as cod_nps_nro_ubicacion,
q1_nps_group as ds_nps_agrupados,
origen  as ds_nps_canal,
cast(null as string) as ds_nps_descripcion,
cast(null as string) as ds_nps_desc_ubicacion,
cast(null as string) as ds_per_edad,
cast(null as string) as ds_nps_ejecutivo,
cast(null as string) as ds_nps_legajo,
cast(null as string) as ds_nps_mejor_prod,
momento_critico as ds_nps_momento_critico,
cast(null as string) as ds_nps_negocio,
nombre as ds_per_nombre,
cast(null as string) as ds_nps_nombre_sucursal,
CASE
   WHEN UCASE (renta) LIKE 'BANCA PRIVADA%' THEN 'BANCA PRIVADA'
   WHEN UCASE (renta) LIKE 'RENTA ALTA SUPERIOR%' THEN 'SELECT'
   WHEN UCASE (renta) LIKE 'RENTA ALTA%' THEN 'ALTA'
   WHEN UCASE (renta) LIKE 'RENTA MEDIA%' THEN 'MEDIA'
   WHEN UCASE (renta) LIKE 'RENTA MASIVA%' THEN 'MASIVA'
   WHEN UCASE (renta) LIKE 'INSTITUCION%' THEN 'INSTITUCION'
   WHEN UCASE (renta) LIKE 'PYME 1%' THEN 'PYME 1'
   WHEN UCASE (renta) LIKE 'PYME 2%' THEN 'PYME 2'
   WHEN UCASE (renta) LIKE 'COMERCIO%' THEN 'COMERCIO'
   WHEN UCASE (renta) LIKE 'EMPRESA%' THEN 'EMPRESA'
   WHEN UCASE (renta) LIKE '%GRAN%EMPRESA%' THEN 'GRAN EMPRESA'
   ELSE 'VER'
END as ds_nps_renta,
rentabilidad as ds_nps_rentabilidad,
cast(null as string) as ds_nps_segmento,
sexo as ds_per_sexo,
tipo_doc as ds_per_tipo_doc,
'empresa pas' as ds_nps_tipo_encuesta,
to_date(from_unixtime(UNIX_TIMESTAMP(partition_date, 'yyyy-MM-dd'))) as dt_nps_fecha_encuesta,
antiguedad as ds_nps_antiguedad_meses,
rank() over (partition by response_id order by question_id asc) as ds_nps_orden,
question_desc as ds_nps_pregunta,
respuesta as ds_nps_respuesta,id_respuesta,
partition_date
from bi_corp_staging.qualtrics_nps_empresa_pas
where partition_date='{{ ti.xcom_pull(task_ids='InputConfig', key='partition_date', dag_id='LOAD_CMN_NPS-Daily') }}'
and nup_firmante is not null


union all

Select
lpad(nup,8,0) as cod_per_nup,
response_id as cod_nps_respuesta,
lpad(nro_doc,11,0) as cod_per_nro_doc,
lpad(cod_suc_uc,4,0) as cod_nps_nro_ubicacion,
cast(null as string) as ds_nps_agrupados,
cast(null as string) as ds_nps_canal,
cast(null as string) as ds_nps_descripcion,
nombre_suc as ds_nps_desc_ubicacion,
edad as ds_per_edad,
cast(null as string) as ds_nps_ejecutivo,
cast(null as string) as ds_nps_legajo,
cast(null as string) as ds_nps_mejor_prod,
momento_critico as ds_nps_momento_critico,
cast(null as string) as ds_nps_negocio,
nombre as ds_per_nombre,
cast(null as string) as ds_nps_nombre_sucursal,
CASE
    WHEN UCASE (renta) LIKE 'BANCA PRIVADA%' THEN 'BANCA PRIVADA'
    WHEN UCASE (renta) LIKE 'RENTA ALTA SUPERIOR%' THEN 'SELECT'
    WHEN UCASE (renta) LIKE 'RENTA ALTA%' THEN 'ALTA'
    WHEN UCASE (renta) LIKE 'RENTA MEDIA%' THEN 'MEDIA'
    WHEN UCASE (renta) LIKE 'RENTA MASIVA%' THEN 'MASIVA'
    WHEN UCASE (renta) LIKE 'INSTITUCION%' THEN 'INSTITUCION'
    WHEN UCASE (renta) LIKE 'PYME 1%' THEN 'PYME 1'
    WHEN UCASE (renta) LIKE 'PYME 2%' THEN 'PYME 2'
    WHEN UCASE (renta) LIKE 'COMERCIO%' THEN 'COMERCIO'
    WHEN UCASE (renta) LIKE 'EMPRESA%' THEN 'EMPRESA'
    WHEN UCASE (renta) LIKE '%GRAN%EMPRESA%' THEN 'GRAN EMPRESA'
    ELSE 'VER'
END as ds_nps_renta,
rentabilidad as ds_nps_rentabilidad,
cast(null as string) as ds_nps_segmento,
sexo as ds_per_sexo,
tipo_doc as ds_per_tipo_doc,
'baja cliente pj' as ds_nps_tipo_encuesta,
to_date(from_unixtime(UNIX_TIMESTAMP(partition_date, 'yyyy-MM-dd'))) as dt_nps_fecha_encuesta,
antiguedad as ds_nps_antiguedad_meses,
rank() over (partition by responseid order by question_id asc) as ds_nps_orden,
question_desc as ds_nps_pregunta,
respuesta as ds_nps_respuesta,id_respuesta,
partition_date
from bi_corp_staging.qualtrics_nps_baja_cliente_pj
where partition_date='{{ ti.xcom_pull(task_ids='InputConfig', key='partition_date', dag_id='LOAD_CMN_NPS-Daily') }}'
and nup is not null


union all

Select
lpad(nup,8,0) as cod_per_nup,
response_id as cod_nps_respuesta,
cuit as cod_per_nro_doc,
case
when UCASE(codigo_canal)= 'MASCHESUC'  then LPAD(cod_suc_uc,4,0)
when codigo_canal in ('OBE', 'OBP', 'SELF') then 'CANALES DIGITALES'
else LPAD(cod_suc_uc,4,0)
end as cod_nps_nro_ubicacion,
q1_nps_group as ds_nps_agrupados,
case
when codigo_canal = 'MASCHESUC' then 'SUCURSAL'
when codigo_canal = 'OBE' then 'ONLINE BANKING EMPRESAS'
when codigo_canal = 'OBP' then 'ONLINE BANKING'
when codigo_canal = 'SELF' then 'SELF'
else 'OTROS'
end as ds_nps_canal,
'GETNET ATENCION TEL' as ds_nps_descripcion,
cast(null as string)  as ds_nps_desc_ubicacion,
edad as ds_per_edad,
col_name as ds_nps_ejecutivo,
cast(null as string) as ds_nps_legajo,
cast(null as string) as ds_nps_mejor_prod,
momento_critico as ds_nps_momento_critico,
cast(null as string) as ds_nps_negocio,
name as ds_per_nombre,
cast(null as string) as ds_nps_nombre_sucursal,
CASE
   WHEN UCASE (renta) LIKE 'BANCA PRIVADA%' THEN 'BANCA PRIVADA'
   WHEN UCASE (renta) LIKE 'RENTA ALTA SUPERIOR%' THEN 'SELECT'
   WHEN UCASE (renta) LIKE 'RENTA ALTA%' THEN 'ALTA'
   WHEN UCASE (renta) LIKE 'RENTA MEDIA%' THEN 'MEDIA'
   WHEN UCASE (renta) LIKE 'RENTA MASIVA%' THEN 'MASIVA'
   WHEN UCASE (renta) LIKE 'INSTITUCION%' THEN 'INSTITUCION'
   WHEN UCASE (renta) LIKE 'PYME 1%' THEN 'PYME 1'
   WHEN UCASE (renta) LIKE 'PYME 2%' THEN 'PYME 2'
   WHEN UCASE (renta) LIKE 'COMERCIO%' THEN 'COMERCIO'
   WHEN UCASE (renta) LIKE 'EMPRESA%' THEN 'EMPRESA'
   WHEN UCASE (renta) LIKE '%GRAN%EMPRESA%' THEN 'GRAN EMPRESA'
   ELSE 'VER'
END as ds_nps_renta,
rentabilidad as ds_nps_rentabilidad,
cast(null as string) as ds_nps_segmento,
sexo as ds_per_sexo,
'CUIT' as ds_per_tipo_doc,
'getnet cust serv' as ds_nps_tipo_encuesta,
to_date(from_unixtime(UNIX_TIMESTAMP(partition_date, 'yyyy-MM-dd'))) as dt_nps_fecha_encuesta,
antiguedad as ds_nps_antiguedad_meses,
rank() over (partition by responseid order by question_id asc) as ds_nps_orden,
question_desc as ds_nps_pregunta,
respuesta as ds_nps_respuesta,id_respuesta,
partition_date
from bi_corp_staging.qualtrics_nps_getnet_cust_serv
where partition_date='{{ ti.xcom_pull(task_ids='InputConfig', key='partition_date', dag_id='LOAD_CMN_NPS-Daily') }}'
and cuit is not null


union all

Select
lpad(nup,8,0) as cod_per_nup,
response_id as cod_nps_respuesta,
lpad(nro_doc,11,0) as cod_per_nro_doc,
if (cod_oferta='work','SIN_DATOS', LPAD(cod_oferta,4,0)) as cod_nps_nro_ubicacion,
q1_nps_group as ds_nps_agrupados,
'SUCURSAL' as ds_nps_canal,
ucase(canal) as ds_nps_descripcion,
cast(null as string) as ds_nps_desc_ubicacion,
edad as ds_per_edad,
ejecutivo as ds_nps_ejecutivo,
legajo as ds_nps_legajo,
cast(null as string) as ds_nps_mejor_prod,
'WORK_CAFE_EFIC' as ds_nps_momento_critico,
cast(null as string) as ds_nps_negocio,
nombre as ds_per_nombre,
cast(null as string) as ds_nps_nombre_sucursal,
CASE
   WHEN UCASE (renta) LIKE 'BANCA PRIVADA%' THEN 'BANCA PRIVADA'
   WHEN UCASE (renta) LIKE 'RENTA ALTA SUPERIOR%' THEN 'SELECT'
   WHEN UCASE (renta) LIKE 'RENTA ALTA%' THEN 'ALTA'
   WHEN UCASE (renta) LIKE 'RENTA MEDIA%' THEN 'MEDIA'
   WHEN UCASE (renta) LIKE 'RENTA MASIVA%' THEN 'MASIVA'
   WHEN UCASE (renta) LIKE 'INSTITUCION%' THEN 'INSTITUCION'
   WHEN UCASE (renta) LIKE 'PYME 1%' THEN 'PYME 1'
   WHEN UCASE (renta) LIKE 'PYME 2%' THEN 'PYME 2'
   WHEN UCASE (renta) LIKE 'COMERCIO%' THEN 'COMERCIO'
   WHEN UCASE (renta) LIKE 'EMPRESA%' THEN 'EMPRESA'
   WHEN UCASE (renta) LIKE '%GRAN%EMPRESA%' THEN 'GRAN EMPRESA'
   ELSE 'VER'
END as ds_nps_renta,
rentabilidad as ds_nps_rentabilidad,
cast(null as string) as ds_nps_segmento,
sexo as ds_per_sexo,
tipo_doc as ds_per_tipo_doc,
'work cafe efic' as ds_nps_tipo_encuesta,
to_date(from_unixtime(UNIX_TIMESTAMP(partition_date, 'yyyy-MM-dd'))) as dt_nps_fecha_encuesta,
antiguedad_en_meses as ds_nps_antiguedad_meses,
rank() over (partition by responseid order by question_id asc) as ds_nps_orden,
question_desc as ds_nps_pregunta,
respuesta as ds_nps_respuesta,id_respuesta,
partition_date
from bi_corp_staging.qualtrics_nps_work_cafe_efic
where partition_date='{{ ti.xcom_pull(task_ids='InputConfig', key='partition_date', dag_id='LOAD_CMN_NPS-Daily') }}'
and nup is not null


union all

Select
cast(null as string) as cod_per_nup,
responseid as cod_nps_respuesta,
cast(null as string) as cod_per_nro_doc,
ubicacion as cod_nps_nro_ubicacion,
q1_nps_group as ds_nps_agrupados,
canal as ds_nps_canal,
cast(null as string) as ds_nps_descripcion,
'NODO BANFIELD' as ds_nps_desc_ubicacion,
cast(null as string) as ds_per_edad,
cast(null as string) as ds_nps_ejecutivo,
cast(null as string) as ds_nps_legajo,
cast(null as string) as ds_nps_mejor_prod,
momento_critico as ds_nps_momento_critico,
cast(null as string) as ds_nps_negocio,
cast(null as string) as ds_per_nombre,
'NODO BANFIELD' as ds_nps_nombre_sucursal,
cast(null as string) as ds_nps_renta,
cast(null as decimal(12,2)) as ds_nps_rentabilidad,
cast(null as string) as ds_nps_segmento,
cast(null as string) as ds_per_sexo,
cast(null as string) as ds_per_tipo_doc,
'nodo banfield' as ds_nps_tipo_encuesta,
to_date(from_unixtime(UNIX_TIMESTAMP(partition_date, 'yyyy-MM-dd'))) as dt_nps_fecha_encuesta,
cast(null as int) as ds_nps_antiguedad_meses,
rank() over (partition by responseid order by question_id asc) as ds_nps_orden,
question_desc as ds_nps_pregunta,
respuesta as ds_nps_respuesta,id_respuesta,
partition_date
from bi_corp_staging.qualtrics_nps_nodo_banfield
where partition_date='{{ ti.xcom_pull(task_ids='InputConfig', key='partition_date', dag_id='LOAD_CMN_NPS-Daily') }}'


union all

Select
cast(null as string) as cod_per_nup,
responseid as cod_nps_respuesta,
cast(null as string) as cod_per_nro_doc,
ubicacion as cod_nps_nro_ubicacion,
q1_nps_group as ds_nps_agrupados,
canal as ds_nps_canal,
cast(null as string) as ds_nps_descripcion,
'NODO CASA CENTRAL' as ds_nps_desc_ubicacion,
cast(null as string) as ds_per_edad,
cast(null as string) as ds_nps_ejecutivo,
cast(null as string) as ds_nps_legajo,
cast(null as string) as ds_nps_mejor_prod,
momento_critico as ds_nps_momento_critico,
cast(null as string) as ds_nps_negocio,
cast(null as string) as ds_per_nombre,
'NODO CASA CENTRAL' as ds_nps_nombre_sucursal,
cast(null as string) as ds_nps_renta,
cast(null as decimal(12,2)) as ds_nps_rentabilidad,
cast(null as string) as ds_nps_segmento,
cast(null as string) as ds_per_sexo,
cast(null as string) as ds_per_tipo_doc,
'nodo casa central' as ds_nps_tipo_encuesta,
to_date(from_unixtime(UNIX_TIMESTAMP(partition_date, 'yyyy-MM-dd'))) as dt_nps_fecha_encuesta,
cast(null as int) as ds_nps_antiguedad_meses,
rank() over (partition by responseid order by question_id asc) as ds_nps_orden,
question_desc as ds_nps_pregunta,
respuesta as ds_nps_respuesta,id_respuesta,
partition_date
from bi_corp_staging.qualtrics_nps_nodo_casa_central
where partition_date='{{ ti.xcom_pull(task_ids='InputConfig', key='partition_date', dag_id='LOAD_CMN_NPS-Daily') }}'


union all

Select
cast(null as string) as cod_per_nup,
responseid as cod_nps_respuesta,
cast(null as string) as cod_per_nro_doc,
ubicacion as cod_nps_nro_ubicacion,
q1_nps_group as ds_nps_agrupados,
canal as ds_nps_canal,
cast(null as string) as ds_nps_descripcion,
'NODO CORDOBA' as ds_nps_desc_ubicacion,
cast(null as string) as ds_per_edad,
cast(null as string) as ds_nps_ejecutivo,
cast(null as string) as ds_nps_legajo,
cast(null as string) as ds_nps_mejor_prod,
momento_critico as ds_nps_momento_critico,
cast(null as string) as ds_nps_negocio,
cast(null as string) as ds_per_nombre,
'NODO CORDOBA' as ds_nps_nombre_sucursal,
cast(null as string) as ds_nps_renta,
cast(null as decimal(12,2)) as ds_nps_rentabilidad,
cast(null as string) as ds_nps_segmento,
cast(null as string) as ds_per_sexo,
cast(null as string) as ds_per_tipo_doc,
'nodo cordoba' as ds_nps_tipo_encuesta,
to_date(from_unixtime(UNIX_TIMESTAMP(partition_date, 'yyyy-MM-dd'))) as dt_nps_fecha_encuesta,
cast(null as int) as ds_nps_antiguedad_meses,
rank() over (partition by responseid order by question_id asc) as ds_nps_orden,
question_desc as ds_nps_pregunta,
respuesta as ds_nps_respuesta,id_respuesta,
partition_date
from bi_corp_staging.qualtrics_nps_nodo_cordoba
where partition_date='{{ ti.xcom_pull(task_ids='InputConfig', key='partition_date', dag_id='LOAD_CMN_NPS-Daily') }}'


union all

Select
cast(null as string) as cod_per_nup,
responseid as cod_nps_respuesta,
cast(null as string) as cod_per_nro_doc,
ubicacion as cod_nps_nro_ubicacion,
q1_nps_group as ds_nps_agrupados,
canal as ds_nps_canal,
cast(null as string) as ds_nps_descripcion,
'NODO FISHERTON' as ds_nps_desc_ubicacion,
cast(null as string) as ds_per_edad,
cast(null as string) as ds_nps_ejecutivo,
cast(null as string) as ds_nps_legajo,
cast(null as string) as ds_nps_mejor_prod,
momento_critico as ds_nps_momento_critico,
cast(null as string) as ds_nps_negocio,
cast(null as string) as ds_per_nombre,
'NODO FISHERTON' as ds_nps_nombre_sucursal,
cast(null as string) as ds_nps_renta,
cast(null as decimal(12,2)) as ds_nps_rentabilidad,
cast(null as string) as ds_nps_segmento,
cast(null as string) as ds_per_sexo,
cast(null as string) as ds_per_tipo_doc,
'nodo fisherton' as ds_nps_tipo_encuesta,
to_date(from_unixtime(UNIX_TIMESTAMP(partition_date, 'yyyy-MM-dd'))) as dt_nps_fecha_encuesta,
cast(null as int) as ds_nps_antiguedad_meses,
rank() over (partition by responseid order by question_id asc) as ds_nps_orden,
question_desc as ds_nps_pregunta,
respuesta as ds_nps_respuesta,id_respuesta,
partition_date
from bi_corp_staging.qualtrics_nps_nodo_fisherton
where partition_date='{{ ti.xcom_pull(task_ids='InputConfig', key='partition_date', dag_id='LOAD_CMN_NPS-Daily') }}'


union all
Select
cast(null as string) as cod_per_nup,
responseid as cod_nps_respuesta,
cast(null as string) as cod_per_nro_doc,
ubicacion as cod_nps_nro_ubicacion,
q1_nps_group as ds_nps_agrupados,
canal as ds_nps_canal,
cast(null as string) as ds_nps_descripcion,
'NODO LA PLATA' as ds_nps_desc_ubicacion,
cast(null as string) as ds_per_edad,
cast(null as string) as ds_nps_ejecutivo,
cast(null as string) as ds_nps_legajo,
cast(null as string) as ds_nps_mejor_prod,
momento_critico as ds_nps_momento_critico,
cast(null as string) as ds_nps_negocio,
cast(null as string) as ds_per_nombre,
'NODO LA PLATA' as ds_nps_nombre_sucursal,
cast(null as string) as ds_nps_renta,
cast(null as decimal(12,2)) as ds_nps_rentabilidad,
cast(null as string) as ds_nps_segmento,
cast(null as string) as ds_per_sexo,
cast(null as string) as ds_per_tipo_doc,
'nodo la plata' as ds_nps_tipo_encuesta,
to_date(from_unixtime(UNIX_TIMESTAMP(partition_date, 'yyyy-MM-dd'))) as dt_nps_fecha_encuesta,
cast(null as int) as ds_nps_antiguedad_meses,
rank() over (partition by responseid order by question_id asc) as ds_nps_orden,
question_desc as ds_nps_pregunta,
respuesta as ds_nps_respuesta,id_respuesta,
partition_date
from bi_corp_staging.qualtrics_nps_nodo_la_plata
where partition_date='{{ ti.xcom_pull(task_ids='InputConfig', key='partition_date', dag_id='LOAD_CMN_NPS-Daily') }}'


union all
Select
cast(null as string) as cod_per_nup,
responseid as cod_nps_respuesta,
cast(null as string) as cod_per_nro_doc,
ubicacion as cod_nps_nro_ubicacion,
q1_nps_group as ds_nps_agrupados,
canal as ds_nps_canal,
cast(null as string) as ds_nps_descripcion,
'NODO LELOIR' as ds_nps_desc_ubicacion,
cast(null as string) as ds_per_edad,
cast(null as string) as ds_nps_ejecutivo,
cast(null as string) as ds_nps_legajo,
cast(null as string) as ds_nps_mejor_prod,
momento_critico as ds_nps_momento_critico,
cast(null as string) as ds_nps_negocio,
cast(null as string) as ds_per_nombre,
'NODO LELOIR' as ds_nps_nombre_sucursal,
cast(null as string) as ds_nps_renta,
cast(null as decimal(12,2)) as ds_nps_rentabilidad,
cast(null as string) as ds_nps_segmento,
cast(null as string) as ds_per_sexo,
cast(null as string) as ds_per_tipo_doc,
'nodo leloir' as ds_nps_tipo_encuesta,
to_date(from_unixtime(UNIX_TIMESTAMP(partition_date, 'yyyy-MM-dd'))) as dt_nps_fecha_encuesta,
cast(null as int) as ds_nps_antiguedad_meses,
rank() over (partition by responseid order by question_id asc) as ds_nps_orden,
question_desc as ds_nps_pregunta,
respuesta as ds_nps_respuesta,id_respuesta,
partition_date
from bi_corp_staging.qualtrics_nps_nodo_leloir
where partition_date='{{ ti.xcom_pull(task_ids='InputConfig', key='partition_date', dag_id='LOAD_CMN_NPS-Daily') }}'


union all
Select
cast(null as string) as cod_per_nup,
responseid as cod_nps_respuesta,
cast(null as string) as cod_per_nro_doc,
ubicacion as cod_nps_nro_ubicacion,
q1_nps_group as ds_nps_agrupados,
canal as ds_nps_canal,
cast(null as string) as ds_nps_descripcion,
'NODO MARDEL' as ds_nps_desc_ubicacion,
cast(null as string) as ds_per_edad,
cast(null as string) as ds_nps_ejecutivo,
cast(null as string) as ds_nps_legajo,
cast(null as string) as ds_nps_mejor_prod,
momento_critico as ds_nps_momento_critico,
cast(null as string) as ds_nps_negocio,
cast(null as string) as ds_per_nombre,
'NODO MARDEL' as ds_nps_nombre_sucursal,
cast(null as string) as ds_nps_renta,
cast(null as decimal(12,2)) as ds_nps_rentabilidad,
cast(null as string) as ds_nps_segmento,
cast(null as string) as ds_per_sexo,
cast(null as string) as ds_per_tipo_doc,
'nodo mardel' as ds_nps_tipo_encuesta,
to_date(from_unixtime(UNIX_TIMESTAMP(partition_date, 'yyyy-MM-dd'))) as dt_nps_fecha_encuesta,
cast(null as int) as ds_nps_antiguedad_meses,
rank() over (partition by responseid order by question_id asc) as ds_nps_orden,
question_desc as ds_nps_pregunta,
respuesta as ds_nps_respuesta,id_respuesta,
partition_date
from bi_corp_staging.qualtrics_nps_nodo_mardel
where partition_date='{{ ti.xcom_pull(task_ids='InputConfig', key='partition_date', dag_id='LOAD_CMN_NPS-Daily') }}'


union all
Select
cast(null as string) as cod_per_nup,
responseid as cod_nps_respuesta,
cast(null as string) as cod_per_nro_doc,
ubicacion as cod_nps_nro_ubicacion,
q1_nps_group as ds_nps_agrupados,
canal as ds_nps_canal,
cast(null as string) as ds_nps_descripcion,
'NODO MENDOZA' as ds_nps_desc_ubicacion,
cast(null as string) as ds_per_edad,
cast(null as string) as ds_nps_ejecutivo,
cast(null as string) as ds_nps_legajo,
cast(null as string) as ds_nps_mejor_prod,
momento_critico as ds_nps_momento_critico,
cast(null as string) as ds_nps_negocio,
cast(null as string) as ds_per_nombre,
'NODO MENDOZA' as ds_nps_nombre_sucursal,
cast(null as string) as ds_nps_renta,
cast(null as decimal(12,2)) as ds_nps_rentabilidad,
cast(null as string) as ds_nps_segmento,
cast(null as string) as ds_per_sexo,
cast(null as string) as ds_per_tipo_doc,
'nodo mendoza' as ds_nps_tipo_encuesta,
to_date(from_unixtime(UNIX_TIMESTAMP(partition_date, 'yyyy-MM-dd'))) as dt_nps_fecha_encuesta,
cast(null as int) as ds_nps_antiguedad_meses,
rank() over (partition by responseid order by question_id asc) as ds_nps_orden,
question_desc as ds_nps_pregunta,
respuesta as ds_nps_respuesta,id_respuesta,
partition_date
from bi_corp_staging.qualtrics_nps_nodo_mendoza
where partition_date='{{ ti.xcom_pull(task_ids='InputConfig', key='partition_date', dag_id='LOAD_CMN_NPS-Daily') }}'


union all
Select
cast(null as string) as cod_per_nup,
responseid as cod_nps_respuesta,
cast(null as string) as cod_per_nro_doc,
ubicacion as cod_nps_nro_ubicacion,
q1_nps_group as ds_nps_agrupados,
canal as ds_nps_canal,
cast(null as string) as ds_nps_descripcion,
'NODO ORONIO' as ds_nps_desc_ubicacion,
cast(null as string) as ds_per_edad,
cast(null as string) as ds_nps_ejecutivo,
cast(null as string) as ds_nps_legajo,
cast(null as string) as ds_nps_mejor_prod,
momento_critico as ds_nps_momento_critico,
cast(null as string) as ds_nps_negocio,
cast(null as string) as ds_per_nombre,
'NODO ORONIO' as ds_nps_nombre_sucursal,
cast(null as string) as ds_nps_renta,
cast(null as decimal(12,2)) as ds_nps_rentabilidad,
cast(null as string) as ds_nps_segmento,
cast(null as string) as ds_per_sexo,
cast(null as string) as ds_per_tipo_doc,
'nodo oronio' as ds_nps_tipo_encuesta,
to_date(from_unixtime(UNIX_TIMESTAMP(partition_date, 'yyyy-MM-dd'))) as dt_nps_fecha_encuesta,
cast(null as int) as ds_nps_antiguedad_meses,
rank() over (partition by responseid order by question_id asc) as ds_nps_orden,
question_desc as ds_nps_pregunta,
respuesta as ds_nps_respuesta,id_respuesta,
partition_date
from bi_corp_staging.qualtrics_nps_nodo_oronio
where partition_date='{{ ti.xcom_pull(task_ids='InputConfig', key='partition_date', dag_id='LOAD_CMN_NPS-Daily') }}'


union all

Select
cast(null as string) as cod_per_nup,
responseid as cod_nps_respuesta,
cast(null as string) as cod_per_nro_doc,
ubicacion as cod_nps_nro_ubicacion,
q1_nps_group as ds_nps_agrupados,
canal as ds_nps_canal,
cast(null as string) as ds_nps_descripcion,
'NODO SAN JUAN' as ds_nps_desc_ubicacion,
cast(null as string) as ds_per_edad,
cast(null as string) as ds_nps_ejecutivo,
cast(null as string) as ds_nps_legajo,
cast(null as string) as ds_nps_mejor_prod,
momento_critico as ds_nps_momento_critico,
cast(null as string) as ds_nps_negocio,
cast(null as string) as ds_per_nombre,
'NODO SAN JUAN' as ds_nps_nombre_sucursal,
cast(null as string) as ds_nps_renta,
cast(null as decimal(12,2)) as ds_nps_rentabilidad,
cast(null as string) as ds_nps_segmento,
cast(null as string) as ds_per_sexo,
cast(null as string) as ds_per_tipo_doc,
'nodo san juan' as ds_nps_tipo_encuesta,
to_date(from_unixtime(UNIX_TIMESTAMP(partition_date, 'yyyy-MM-dd'))) as dt_nps_fecha_encuesta,
cast(null as int) as ds_nps_antiguedad_meses,
rank() over (partition by responseid order by question_id asc) as ds_nps_orden,
question_desc as ds_nps_pregunta,
respuesta as ds_nps_respuesta,id_respuesta,
partition_date
from bi_corp_staging.qualtrics_nps_nodo_san_juan
where partition_date='{{ ti.xcom_pull(task_ids='InputConfig', key='partition_date', dag_id='LOAD_CMN_NPS-Daily') }}'


union all

Select
cast(null as string) as cod_per_nup,
responseid as cod_nps_respuesta,
cast(null as string) as cod_per_nro_doc,
ubicacion as cod_nps_nro_ubicacion,
q1_nps_group as ds_nps_agrupados,
canal as ds_nps_canal,
cast(null as string) as ds_nps_descripcion,
'NODO TUCUMAN' as ds_nps_desc_ubicacion,
cast(null as string) as ds_per_edad,
cast(null as string) as ds_nps_ejecutivo,
cast(null as string) as ds_nps_legajo,
cast(null as string) as ds_nps_mejor_prod,
momento_critico as ds_nps_momento_critico,
cast(null as string) as ds_nps_negocio,
cast(null as string) as ds_per_nombre,
'NODO TUCUMAN' as ds_nps_nombre_sucursal,
cast(null as string) as ds_nps_renta,
cast(null as decimal(12,2)) as ds_nps_rentabilidad,
cast(null as string) as ds_nps_segmento,
cast(null as string) as ds_per_sexo,
cast(null as string) as ds_per_tipo_doc,
'nodo tucuman' as ds_nps_tipo_encuesta,
to_date(from_unixtime(UNIX_TIMESTAMP(partition_date, 'yyyy-MM-dd'))) as dt_nps_fecha_encuesta,
cast(null as int) as ds_nps_antiguedad_meses,
rank() over (partition by responseid order by question_id asc) as ds_nps_orden,
question_desc as ds_nps_pregunta,
respuesta as ds_nps_respuesta,id_respuesta,
partition_date
from bi_corp_staging.qualtrics_nps_nodo_tucuman
where partition_date='{{ ti.xcom_pull(task_ids='InputConfig', key='partition_date', dag_id='LOAD_CMN_NPS-Daily') }}'


union all

Select
cast(null as string) as cod_per_nup,
responseid as cod_nps_respuesta,
cast(null as string) as cod_per_nro_doc,
ubicacion as cod_nps_nro_ubicacion,
q1_nps_group as ds_nps_agrupados,
canal as ds_nps_canal,
cast(null as string) as ds_nps_descripcion,
'NODO WILDE' as ds_nps_desc_ubicacion,
cast(null as string) as ds_per_edad,
cast(null as string) as ds_nps_ejecutivo,
cast(null as string) as ds_nps_legajo,
cast(null as string) as ds_nps_mejor_prod,
momento_critico as ds_nps_momento_critico,
cast(null as string) as ds_nps_negocio,
cast(null as string) as ds_per_nombre,
'NODO WILDE' as ds_nps_nombre_sucursal,
cast(null as string) as ds_nps_renta,
cast(null as decimal(12,2)) as ds_nps_rentabilidad,
cast(null as string) as ds_nps_segmento,
cast(null as string) as ds_per_sexo,
cast(null as string) as ds_per_tipo_doc,
'nodo wilde' as ds_nps_tipo_encuesta,
to_date(from_unixtime(UNIX_TIMESTAMP(partition_date, 'yyyy-MM-dd'))) as dt_nps_fecha_encuesta,
cast(null as int) as ds_nps_antiguedad_meses,
rank() over (partition by responseid order by question_id asc) as ds_nps_orden,
question_desc as ds_nps_pregunta,
respuesta as ds_nps_respuesta,id_respuesta,
partition_date
from bi_corp_staging.qualtrics_nps_nodo_wilde
where partition_date='{{ ti.xcom_pull(task_ids='InputConfig', key='partition_date', dag_id='LOAD_CMN_NPS-Daily') }}'


union all

Select
nup_empresa as cod_per_nup,
response_id as cod_nps_respuesta,
case
	when canal = 'MBEBIN' then cuit_Empresa
	when canal = 'OBE' then cuit_Empresa
	when canal = 'OBCM' then cuit_Empresa
	else cast(null as string)
end  as cod_per_nro_doc,
'CANALES DIGITALES' as cod_nps_nro_ubicacion,
CASE WHEN  cast(respuesta_1 as int) BETWEEN 0 AND 6 THEN 1
 	WHEN cast(respuesta_1 as int) BETWEEN 7 AND 8 THEN 2
	WHEN cast(respuesta_1 as int) BETWEEN 9 AND 10 THEN 3
	else cast(null as string)
END as ds_nps_agrupados,
case
	when canal = 'MBEBIN' then 'MOBILE_PJ'
	when canal = 'OBCM' then 'CASH_MANAGEMENT'
	when canal = 'OBE' then 'ONLINE BANKING EMPRESAS'
end  as ds_nps_canal,
cast(null as string) as ds_nps_descripcion,
cast(null as string) as ds_nps_desc_ubicacion,
edad as ds_per_edad,
cast(null as string) as ds_nps_ejecutivo,
cast(null as string) as ds_nps_legajo,
cast(null as string) as ds_nps_mejor_prod,
case
	when canal = 'MBEBIN' then 'MOBILE_PJ'
	when canal = 'OBCM' then 'CASH_MANAGEMENT'
	when canal = 'OBE' then 'ONLINE BANKING EMPRESAS'
end as ds_nps_momento_critico,
cast(null as string) as ds_nps_negocio,
nombre as ds_per_nombre,
cast(null as string) as ds_nps_nombre_sucursal,
CASE
   WHEN UCASE (renta) LIKE 'BANCA PRIVADA%' THEN 'BANCA PRIVADA'
   WHEN UCASE (renta) LIKE 'RENTA ALTA SUPERIOR%' THEN 'SELECT'
   WHEN UCASE (renta) LIKE 'RENTA ALTA%' THEN 'ALTA'
   WHEN UCASE (renta) LIKE 'RENTA MEDIA%' THEN 'MEDIA'
   WHEN UCASE (renta) LIKE 'RENTA MASIVA%' THEN 'MASIVA'
   WHEN UCASE (renta) LIKE 'INSTITUCION%' THEN 'INSTITUCION'
   WHEN UCASE (renta) LIKE 'PYME 1%' THEN 'PYME 1'
   WHEN UCASE (renta) LIKE 'PYME 2%' THEN 'PYME 2'
   WHEN UCASE (renta) LIKE 'COMERCIO%' THEN 'COMERCIO'
   WHEN UCASE (renta) LIKE 'EMPRESA%' THEN 'EMPRESA'
   WHEN UCASE (renta) LIKE '%GRAN%EMPRESA%' THEN 'GRAN EMPRESA'
   WHEN UCASE (renta) LIKE 'CORPORATE%' THEN 'CORPORATE'
   ELSE 'VER'
END as ds_nps_renta,
rentabilidad as ds_nps_rentabilidad,
cast(null as string) as ds_nps_segmento,
sexo as ds_per_sexo,
cast(null as string) as ds_per_tipo_doc,
'canales empresa' as ds_nps_tipo_encuesta,
to_date(from_unixtime(UNIX_TIMESTAMP(partition_date, 'yyyy-MM-dd'))) as dt_nps_fecha_encuesta,
antiguedad as ds_nps_antiguedad_meses,
rank() over (partition by response_id order by question_id asc) as ds_nps_orden,
question_desc as ds_nps_pregunta,
if (question_id = 'Q1',respuesta_1,respuesta_2)  as ds_nps_respuesta, id_respuesta,
partition_date
from  bi_corp_staging.qualtrics_nps_canales_emp
where partition_date='{{ ti.xcom_pull(task_ids='InputConfig', key='partition_date', dag_id='LOAD_CMN_NPS-Daily') }}'
and nup_empresa is not null


union all

Select
lpad(nup,8,0) as cod_per_nup,
response_id as cod_nps_respuesta,
cast(null as string) as cod_per_nro_doc,
'CANALES DIGITALES' as cod_nps_nro_ubicacion,
CASE WHEN  cast(respuesta_1 as int) BETWEEN 0 AND 6 THEN 1
 	WHEN cast(respuesta_1 as int) BETWEEN 7 AND 8 THEN 2
	WHEN cast(respuesta_1 as int) BETWEEN 9 AND 10 THEN 3
	else cast(null as string)
END as ds_nps_agrupados,
case
	when canal = 'MOBP' then 'ONLINE BANKING'
	when canal = 'MOBILE' then 'MOBILE'
end  as ds_nps_canal,
cast(null as string) as ds_nps_descripcion,
cast(null as string) as ds_nps_desc_ubicacion,
edad as ds_per_edad,
cast(null as string) as ds_nps_ejecutivo,
cast(null as string) as ds_nps_legajo,
cast(null as string) as ds_nps_mejor_prod,
case
	when canal = 'MOBP' then 'ONLINE BANKING'
	when canal = 'MOBILE' then 'MOBILE'
end as ds_nps_momento_critico,
cast(null as string) as ds_nps_negocio,
nombre as ds_per_nombre,
cast(null as string) as ds_nps_nombre_sucursal,
CASE
   WHEN UCASE (renta) LIKE 'BANCA PRIVADA%' THEN 'BANCA PRIVADA'
   WHEN UCASE (renta) LIKE 'RENTA ALTA SUPERIOR%' THEN 'SELECT'
   WHEN UCASE (renta) LIKE 'RENTA ALTA%' THEN 'ALTA'
   WHEN UCASE (renta) LIKE 'RENTA MEDIA%' THEN 'MEDIA'
   WHEN UCASE (renta) LIKE 'RENTA MASIVA%' THEN 'MASIVA'
   WHEN UCASE (renta) LIKE 'INSTITUCION%' THEN 'INSTITUCION'
   WHEN UCASE (renta) LIKE 'PYME 1%' THEN 'PYME 1'
   WHEN UCASE (renta) LIKE 'PYME 2%' THEN 'PYME 2'
   WHEN UCASE (renta) LIKE 'COMERCIO%' THEN 'COMERCIO'
   WHEN UCASE (renta) LIKE 'EMPRESA%' THEN 'EMPRESA'
   WHEN UCASE (renta) LIKE '%GRAN%EMPRESA%' THEN 'GRAN EMPRESA'
   ELSE 'VER'
END as ds_nps_renta,
rentabilidad as ds_nps_rentabilidad,
cast(null as string) as ds_nps_segmento,
sexo as ds_per_sexo,
cast(null as string) as ds_per_tipo_doc,
'canales individuo' as ds_nps_tipo_encuesta,
to_date(from_unixtime(UNIX_TIMESTAMP(partition_date, 'yyyy-MM-dd'))) as dt_nps_fecha_encuesta,
antiguedad as ds_nps_antiguedad_meses,
rank() over (partition by response_id order by question_id asc) as ds_nps_orden,
question_desc as ds_nps_pregunta,
if (question_id = 'Q1',respuesta_1,respuesta_2)  as ds_nps_respuesta, id_respuesta,
partition_date
from  bi_corp_staging.qualtrics_nps_canales_ind
where partition_date='{{ ti.xcom_pull(task_ids='InputConfig', key='partition_date', dag_id='LOAD_CMN_NPS-Daily') }}'
and nup is not null


union all

Select
lpad(nup,8,0) as cod_per_nup,
response_id as cod_nps_respuesta,
lpad(nro_doc,11,0) as cod_per_nro_doc,
lpad(cod_suc_uc,4,0) as cod_nps_nro_ubicacion,
q1_nps_group as ds_nps_agrupados,
'BANCO' as ds_nps_canal,
cast(null as string) as ds_nps_descripcion,
cast(null as string) as ds_nps_desc_ubicacion,
edad as ds_per_edad,
cast(null as string) as ds_nps_ejecutivo,
cast(null as string) as ds_nps_legajo,
cast(null as string) as ds_nps_mejor_prod,
momento_critico as ds_nps_momento_critico,
cast(null as string) as ds_nps_negocio,
nombre as ds_per_nombre,
cast(null as string) as ds_nps_nombre_sucursal,
CASE
   WHEN UCASE (renta) LIKE 'BANCA PRIVADA%' THEN 'BANCA PRIVADA'
   WHEN UCASE (renta) LIKE 'RENTA ALTA SUPERIOR%' THEN 'SELECT'
   WHEN UCASE (renta) LIKE 'RENTA ALTA%' THEN 'ALTA'
   WHEN UCASE (renta) LIKE 'RENTA MEDIA%' THEN 'MEDIA'
   WHEN UCASE (renta) LIKE 'RENTA MASIVA%' THEN 'MASIVA'
   WHEN UCASE (renta) LIKE 'INSTITUCION%' THEN 'INSTITUCION'
   WHEN UCASE (renta) LIKE 'PYME 1%' THEN 'PYME 1'
   WHEN UCASE (renta) LIKE 'PYME 2%' THEN 'PYME 2'
   WHEN UCASE (renta) LIKE 'COMERCIO%' THEN 'COMERCIO'
   WHEN UCASE (renta) LIKE 'EMPRESA%' THEN 'EMPRESA'
   WHEN UCASE (renta) LIKE '%GRAN%EMPRESA%' THEN 'GRAN EMPRESA'
   ELSE 'VER'
END as ds_nps_renta,
rentabilidad as ds_nps_rentabilidad,
cast(null as string) as ds_nps_segmento,
sexo as ds_per_sexo,
tipo_doc as ds_per_tipo_doc,
'relacional sorpresa' as ds_nps_tipo_encuesta,
to_date(from_unixtime(UNIX_TIMESTAMP(partition_date, 'yyyy-MM-dd'))) as dt_nps_fecha_encuesta,
antiguedad as ds_nps_antiguedad_meses,
rank() over (partition by responseid order by question_id asc) as ds_nps_orden,
question_desc as ds_nps_pregunta,
respuesta as ds_nps_respuesta,id_respuesta,
partition_date
from bi_corp_staging.qualtrics_nps_relacional_sorpresa
where partition_date='{{ ti.xcom_pull(task_ids='InputConfig', key='partition_date', dag_id='LOAD_CMN_NPS-Daily') }}'
and nup is not null


) j
left join  (select cod_per_nup, ds_per_apellido, ds_per_nombre, ds_per_segmento, cod_per_sexo, ds_per_subsegmento,fc_per_edad, ds_per_numdoc,ds_per_tipodoc, partition_date  from bi_corp_common.stk_per_personas) as p1
on (cast(p1.cod_per_nup as bigint) =  cast(j.cod_per_nup as bigint)
and p1.partition_date ='{{ ti.xcom_pull(task_ids='InputConfig', key='max_partition_stk_per_personas', dag_id='LOAD_CMN_NPS-Daily') }}')
and j.cod_nps_respuesta  like 'R%'
