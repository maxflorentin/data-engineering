SET hive.execution.engine=spark;
SET spark.yarn.queue=root.dataeng;
SET hive.exec.dynamic.partition.mode=nonstrict;


INSERT INTO TABLE bi_corp_common.bt_nps_nps
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
where partition_date='{{ ti.xcom_pull(task_ids='InputConfig', key='partition_date', dag_id='LOAD_CMN_NPS_REPROCESO-Daily') }}'
AND  penumber IS NOT NULL

Union all

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
where partition_date='{{ ti.xcom_pull(task_ids='InputConfig', key='partition_date', dag_id='LOAD_CMN_NPS_REPROCESO-Daily') }}'
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
where partition_date='{{ ti.xcom_pull(task_ids='InputConfig', key='partition_date', dag_id='LOAD_CMN_NPS_REPROCESO-Daily') }}'
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
       and b.partition_date='{{ ti.xcom_pull(task_ids='InputConfig', key='partition_date', dag_id='LOAD_CMN_NPS_REPROCESO-Daily') }}')
where onb.partition_date='{{ ti.xcom_pull(task_ids='InputConfig', key='partition_date', dag_id='LOAD_CMN_NPS_REPROCESO-Daily') }}'
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
       and b.partition_date='{{ ti.xcom_pull(task_ids='InputConfig', key='partition_date', dag_id='LOAD_CMN_NPS_REPROCESO-Daily') }}')
where onb.partition_date='{{ ti.xcom_pull(task_ids='InputConfig', key='partition_date', dag_id='LOAD_CMN_NPS_REPROCESO-Daily') }}'
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
       and b.partition_date='{{ ti.xcom_pull(task_ids='InputConfig', key='partition_date', dag_id='LOAD_CMN_NPS_REPROCESO-Daily') }}')
where onb.partition_date='{{ ti.xcom_pull(task_ids='InputConfig', key='partition_date', dag_id='LOAD_CMN_NPS_REPROCESO-Daily') }}'
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
       and b.partition_date='{{ ti.xcom_pull(task_ids='InputConfig', key='partition_date', dag_id='LOAD_CMN_NPS_REPROCESO-Daily') }}')
where onb.partition_date='{{ ti.xcom_pull(task_ids='InputConfig', key='partition_date', dag_id='LOAD_CMN_NPS_REPROCESO-Daily') }}'
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
where partition_date='{{ ti.xcom_pull(task_ids='InputConfig', key='partition_date', dag_id='LOAD_CMN_NPS_REPROCESO-Daily') }}'
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
where partition_date='{{ ti.xcom_pull(task_ids='InputConfig', key='partition_date', dag_id='LOAD_CMN_NPS_REPROCESO-Daily') }}'
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
where partition_date='{{ ti.xcom_pull(task_ids='InputConfig', key='partition_date', dag_id='LOAD_CMN_NPS_REPROCESO-Daily') }}'
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
where partition_date='{{ ti.xcom_pull(task_ids='InputConfig', key='partition_date', dag_id='LOAD_CMN_NPS_REPROCESO-Daily') }}'
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
where partition_date='{{ ti.xcom_pull(task_ids='InputConfig', key='partition_date', dag_id='LOAD_CMN_NPS_REPROCESO-Daily') }}'
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
where partition_date='{{ ti.xcom_pull(task_ids='InputConfig', key='partition_date', dag_id='LOAD_CMN_NPS_REPROCESO-Daily') }}'
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
where partition_date='{{ ti.xcom_pull(task_ids='InputConfig', key='partition_date', dag_id='LOAD_CMN_NPS_REPROCESO-Daily') }}'
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
where partition_date='{{ ti.xcom_pull(task_ids='InputConfig', key='partition_date', dag_id='LOAD_CMN_NPS_REPROCESO-Daily') }}'
AND nro_doc IS NOT NULL


) j
left join  (select cod_per_nup, ds_per_apellido, ds_per_nombre, ds_per_segmento, cod_per_sexo, ds_per_subsegmento,fc_per_edad, ds_per_numdoc,ds_per_tipodoc, partition_date  from bi_corp_common.stk_per_personas) as p1
on (cast(p1.cod_per_nup as bigint) =  cast(j.cod_per_nup as bigint)
and p1.partition_date ='{{ ti.xcom_pull(task_ids='InputConfig', key='max_partition_stk_per_personas', dag_id='LOAD_CMN_NPS_REPROCESO-Daily') }}')
and j.cod_nps_respuesta  like 'R%'
