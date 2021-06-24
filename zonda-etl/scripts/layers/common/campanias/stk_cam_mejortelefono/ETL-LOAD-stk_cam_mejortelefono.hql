set hive.merge.mapfiles=true;
set hive.merge.mapredfiles=true;
set hive.merge.size.per.task=4000000;
set hive.merge.smallfiles.avgsize=16000000;
set hive.exec.dynamic.partition.mode=nonstrict;
set mapred.job.queue.name=root.dataeng;

DROP TABLE IF EXISTS tmp_nuevos_altair;
CREATE TEMPORARY TABLE tmp_nuevos_altair as
SELECT  a.penumper,
a.ddn_original,
a.NumTelefono_Original,
nvl(IF(a.ddn_original='',NULL,a.ddn_original) , '*' ) as ddn_original_con,
pesecdom,
MIN(a.pesectel ) as pesectel
FROM(
	SELECT  penumper,
	trim( b.pepretel ) as ddn_original,
	CONCAT( trim( b.pecartel ), trim( b.penumtel )) as NumTelefono_Original,
	b.pesectel,
	trim(pesecdom) pesecdom
	from    bi_corp_staging.malpe_pedt023 b
	where   partition_date='2021-06-07'--MAX PARTITION
	and 	TRIM(TRANSLATE(trim(concat(b.pecartel,b.penumtel)), rpad(SUBSTRING(trim(concat(b.pecartel,b.penumtel) ), 1, 1 ), LENGTH(concat(b.pecartel,b.penumtel)), SUBSTRING( trim( concat(b.pecartel,b.penumtel) ), 1, 1 ) ), lpad( ' ', LENGTH(concat(b.pecartel,b.penumtel)), ' ' ) ) ) != ''--Busca si todos los caracteres son iguales los descarta
	and     (substr(b.pehstamp, 1, 10)) > date_sub('2021-06-07', 365)
	and     length( trim( CONCAT( b.pecartel,b.penumtel) )) > 4
	and     nvl(CASE WHEN TRIM(SUBSTRING( b.peobserv2, 30, 1 ))= '' THEN NULL ELSE TRIM(SUBSTRING( b.peobserv2, 30, 1 )) END , '2' ) not in ( '3', '4' )
	) as a
group by
a.penumper,
a.ddn_original,
nvl(IF(a.ddn_original='',NULL,a.ddn_original) , '*' ),
a.NumTelefono_Original,
pesecdom;

DROP TABLE IF EXISTS pedt003;
CREATE TEMPORARY TABLE pedt003 as
SELECT trim(pesecdom) pesecdom,
	trim(penumper) penumper,
	trim(upper(pedesloc)) pedesloc,
	trim(pecodpos) pecodpos
		FROM bi_corp_staging.malpe_pedt003
		WHERE partition_date='2021-06-07'
		and pecdgent='0072';


DROP TABLE IF EXISTS tmp_nuevos_altair_loc;
CREATE TEMPORARY TABLE tmp_nuevos_altair_loc as
select
c.penumper,
c.ddn_original,
c.numtelefono_original,
c.pesectel,
c.pesecdom,
a.pedesloc as localidad,
a.pecodpos as codpostal
FROM tmp_nuevos_altair c
LEFT JOIN pedt003 as a on ( c.pesecdom = a.pesecdom AND c.penumper = a.penumper)
group by
c.penumper,
c.ddn_original,
c.numtelefono_original,
c.pesectel,
c.pesecdom,
a.pedesloc,
a.pecodpos;


DROP TABLE IF EXISTS bi_corp_common.nrm_normal_telefonos;
CREATE TABLE bi_corp_common.nrm_normal_telefonos as -- BORRAR distinct con row_num?
select
distinct
a.penumper as inter_cif_id ,
--a.pepriape as xapellido ,
c.ddn_original as xprefijo_interurbano,
c.numtelefono_original as xNumero_Telefono,
--1 as ultimo,
0 as vueltas,
CASE WHEN b.codpostal is null THEN c.codpostal ELSE b.codpostal END as xcodigo_postal,
ROW_NUMBER() OVER () as id,
1 as busqueda_guia,
CASE WHEN loc.localidad_ok is null THEN c.localidad ELSE loc.localidad_ok END as xlocalidad,
'ALTAIR' as origen,
pesectel as pesectel,
a.partition_date
from
 tmp_nuevos_altair_loc c
JOIN	(select penumper ,pepriape,pecdgent , partition_date
	from bi_corp_staging.malpe_pedt001 p01
	WHERE p01.partition_date='2021-06-07'--MAX PARTITION?
	and pecdgent = '0072') a on(trim(a.penumper) = trim(c.penumper))
LEFT JOIN (select localidad_ok,localidad_mal
			from bi_corp_staging.rio35_nrm_localidad_normalizada nr
			where nr.partition_date='2021-06-07'--MAX PARTITION?
			) loc on ( trim(c.localidad) = trim(loc.localidad_mal) )
LEFT JOIN (	select  ddn, codpostal
			from bi_corp_staging.rio35_nrm_ddn_postal ddn
			where ddn.partition_date='2021-06-07'--MAX PARTITION?
			) b on (trim(b.ddn) = trim(c.ddn_original))
;

DROP TABLE IF  enacom_fin;
CREATE  TABLE enacom_fin as
with enacom as (
select
          case when trim( operador ) in ( 'TELEFONICA MOVILES ARGENTINA SOCIEDAD ANONIMA (CUIT 30-67881435-7)', 'COMPAÑIA DE RADIOCOMUNICACIONES MOVILES SOCIEDAD ANONIMA', 'TELEFONICA MOVILES ARGENTINA SOCIEDAD ANONIMA' ) and nvl( modalidad, '*' ) != 'BASICA' then 'MOVI'
                 when trim( operador ) in ( 'NEXTEL COMMUNICATIONS ARGENTINA SOCIEDAD DE RESPONSABILIDAD LIMITADA' ) and  nvl( modalidad, '*' ) != 'BASICA' then 'NEXT'
                 when trim( operador ) in ( 'AMX ARGENTINA SOCIEDAD ANONIMA (CUIT 30-66328849-7)' ) and nvl( modalidad, '*' ) != 'BASICA' then 'CTI'
                 when trim( operador ) in ( 'TELECOM ARGENTINA SOCIEDAD ANONIMA (CUIT 30-63945373-8)' ) and nvl( modalidad, '*' ) != 'BASICA' then 'PERS'
            else 'OTRA' end as compania,
           concat('0' , indicativo) as ddn,
          case when modalidad = 'CPP' then 'CELULAR' else 'FIJO' end as tipo,
          bloque ,
          localidad
       from bi_corp_staging.enacom_num_geo ),

 enacom_tr as (

 select
 compania,
 ddn,
 tipo ,
 localidad,
 case when tipo = 'FIJO' then bloque else concat('15', bloque) end as caracteristica
from enacom
 ),

 enacom_tr_2 as (
 select
 compania,
 ddn,
 localidad,
 tipo,
 caracteristica,
 concat(ddn,caracteristica) as tel_comienzo
 from enacom_tr)


select
tel_comienzo,
tipo,
localidad,
ddn,
case when tipo = 'FIJO' then
            substr( caracteristica, 1, 11 - length(ddn) - 4 )
        else
            concat('15' , substr( caracteristica, 3, 11 - length( ddn ) - 4 ))
        end as caracteristica ,
'N' as existe_guia,
compania
  from enacom_tr_2;




insert /*+ append*/ into sdc_telefono
(
id, penumper, ddn,
numtelefono, codpostal, pesectel, vigente,
erroneo,
lista_negra,
fecha_act, falta, origen,
telefono_Completo,
ddn_original, numtelefono_original,
compania)
(
--HACE FALTA EL JOIN CON PERSONAS?

select  /*+ index ( cp ddn_uni ) */
--sdc_id.nextval,
per.cod_per_nup, xPrefijo_Interurbano,
xNumero_Telefono, nvl( xCodigo_Postal, cp.codpostal ), pesectel, 'S',
decode( erroneo, 'S', 'S', 'T', 'T', DECODE( normalizado, null, 'S', 'N' ) ),
decode( n.telefono_completo , null , 'N' , 'S'),
trunc(sysdate),trunc(sysdate), origen,
concat(xPrefijo_Interurbano, xNumero_Telefono),
prefijo_interurbano, telefono,
trim( nor.compania )
from 	nrm_normal_telefonos nor
JOIN (	select cod_per_nup
		from bi_corp_common.stk_per_personas
		where partition_date='2021-04-06'--MAX PARTITION
		) per ON (per.cod_per_nup = nor.inter_cif_id)
LEFT JOIN (	select ddn,codpostal
			FROM bi_corp_staging.rio35_tb_ddns_codpostal
			WHERE partition_date='2021-04-12'--MAX PARTITION
			) cp ON (nor.xPrefijo_Interurbano = cp.ddn)
LEFT JOIN	(select telefono_completo
			from bi_corp_staging.rio35_nrm_tel_lista_negra
			where partition_date='2021-04-14') n ON ( CONCAT(nor.xPrefijo_Interurbano, nor.xNumero_Telefono) = n.telefono_completo);





INSERT OVERWRITE TABLE  bi_corp_common.stk_cam_mejortelefono
PARTITION(partition_date)

select

from repetidos_i

