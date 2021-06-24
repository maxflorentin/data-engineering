CREATE VIEW IF NOT EXISTS bi_corp_common.vstk_per_personas AS
SELECT p001.penumper cod_per_nup
, p001.penomper per_nombre
, p001.pepriape per_apellido
, tcd_0113.descripcion per_tipo_doc
, p001.penumdoc per_num_doc
, p001.pesucadm cod_sucursal_administradora
, p002.flag_empleado flag_empleado
, NULL cod_clanae
, p030.pesegcla cod_per_segmento_duro
, p040.pebancap cod_per_banca
, p052.flag_banca_privada flag_banca_privada
, p008.flag_cuenta_interna flag_cuenta_interna
, tarj.flag_woman flag_woman
, CASE WHEN p008.flag_titular = 1 AND p001.fc_per_edad BETWEEN 18 AND 31 THEN 1 ELSE 0 END flag_IU
, tarj.flag_nova flag_nova
, p052.flag_citi flag_citi
, p223.flag_universitario flag_universitario
, p223.cod_universitario cod_universitario
, tcd_1154.descripcion ds_universitario
, p052.flag_international_desk flag_international_desk
, p052.flag_uge flag_uge
, p052.flag_vip flag_vip
, NULL flag_plan_sueldo
, p008.flag_titular flag_titular
, p008.flag_cotitular flag_cotitular
, p008.flag_adicional flag_adicional
, p008.flag_firmante flag_firmante
, p052.flag_supervivencia flag_supervivencia
, NULL flag_jubilado
, NULL flag_cliente_anses
, NULL flag_colectivo
, NULL flag_digital_30
, p025_024.penumgru cod_per_grupo_economico
, p025_024.pedesgru ds_per_grupo_economico
, case when sge.nro_grupo is not null then sge.nro_grupo  else aqua_asoc.cod_grupo end cod_top_level_grupo_economico
, case when sge.nro_grupo is not null then NULL else aqua_asoc.ds_codigo end ds_top_level_grupo_economico
, NULL ds_tipo_empresa
, p005.peramemp cod_per_ramo_empresa
, p052.cod_vinculacion cod_per_vinculacion
, tcd_0680_avi.descripcion ds_per_vinculacion
, NULL ds_mapa_seguimiento
, contra.cod_kgr cod_kgr
, NULL dt_ultima_acreditacion
, p030.ds_per_segmento ds_per_segmento
, p030.ds_per_subsegmento ds_per_subsegmento
, p030.cod_per_cuadrante cod_per_cuadrante
, NULL dt_fecha_antiguedad
, NULL ds_test_inversor
, NULL dt_test_inversor_ult_fecha
, cuentas.flag_superclub flag_superclub
, cuentas.flag_AA flag_AA
, NULL flag_sorpresa
, NULL flag_clave_automatico
, p052.flag_cuenta_blanca flag_cuenta_blanca
, NULL cod_ofical_asignado
, NULL ds_ofical_asignado
, NULL ds_modelo_atencion
, p040.flag_agro flag_agro
, p040.pedivisi cod_division
, NULL fc_maximo_dias_mora
, NULL fc_productos_mora
, NULL fc_score_comportamiento
, NULL fc_score_veraz
, NULL fc_raiting
, NULL flag_raiting_plus
, NULL ds_principalidad_veraz_behaviour
, NULL ds_motivo_baja
, p001.cod_per_sexo cod_per_sexo
, p001.petipper cod_per_tipo_persona
, p001.pefecnac dt_per_fecha_nacimiento
, p001.peestciv cod_per_estado_civil
, p001.pepaiori cod_per_pais_nacimiento
, p001.penacper cod_per_nacionalidad
, p001.pepaires cod_per_residencia
, p063.pecodafip cod_actividad_afip
, tcd_0231.descripcion ds_tipo_sociedad
, p001.peconper cod_per_condicion_cliente
, NULL cod_sit_bcra_bsr
, NULL cod_sit_bcra_sist_financiero
, NULL fc_deuda_sist_financiero
, p052.flag_mipyme flag_mipyme
, tcd_0680_mip.cod_mipyme cod_mipyme
, tcd_0680_mip.ds_mipyme ds_mipyme
, p001.fc_per_edad fc_per_edad
, NULL fc_cuentas
, cuentas.fc_tarjetas fc_tarjetas
, NULL fc_prestamos
, NULL fc_seguros
, NULL fc_plazo_fijo
, NULL fc_fci
, NULL fc_letras
, NULL fc_acciones
, NULL fc_bonos
, NULL fc_caja_seguridad
, NULL limite_pre_acordado
, p001.petipocu cod_ocupacion
, tcd_0305.descripcion ds_ocupacion
, NULL flag_fallecido
, NULL cod_canal_venta
, p001.pecodact cod_actividad_bcra
, tcd_0304.descripcion ds_actividad_bcra
, NULL cod_campana
, NULL cod_sujeto
, p001.pefecini dt_fecha_inicio_actividades
, p001.peforjur cod_forma_juridica
, NULL fc_score_cliente1
, NULL fc_score_cliente2
, NULL fc_score_alineado_cliente1
, NULL fc_score_alineado_cliente2
, NULL fc_score_bruto_cliente1
, NULL fc_score_bruto_cliente2
, NULL cod_renta
, NULL cod_segmento_triad
, NULL cod_suc_paquete
, NULL cod_zona
, NULL fc_limite_disponible_compra_TC
, NULL ft_monto_total_impago_cliente
, NULL ft_ingreso_bruto_calificacion
, NULL dt_fecha_ultima_calificacion
, NULL ft_mon_ingreso
, p001.partition_date

FROM
(SELECT penumper, penomper, pepriape, trim(petipdoc) petipdoc, penumdoc, pesucadm, petipper, pefecnac, peestciv, pepaiori
, penacper, pepaires, peconper , pefecini, petipocu ,pecodact, peforjur,partition_date
, (year(current_timestamp()) - year(pefecnac)) + (CASE WHEN (month(current_timestamp()) < month(pefecnac))
OR (month(current_timestamp()) = month(pefecnac) AND day(current_timestamp()) > day(pefecnac))
THEN -1 ELSE 0 END) fc_per_edad
, CASE WHEN pesexper = 'F' THEN 'M' WHEN petipper = 'J' OR pesexper = '1' THEN NULL ELSE pesexper END cod_per_sexo
FROM bi_corp_staging.malpe_pedt001 WHERE partition_date = '2020-05-07' ) p001

LEFT OUTER JOIN
(SELECT penumper, pemaremp flag_empleado, petipact
FROM bi_corp_staging.malpe_pedt002 WHERE partition_date = '2020-05-07' ) p002
	ON p001.penumper = p002.penumper

LEFT JOIN
(SELECT penumper, peramemp
FROM bi_corp_staging.malpe_pedt005 WHERE partition_date = '2020-04-30' ) p005
	ON p001.penumper = p005.penumper



LEFT JOIN
(SELECT penumper, CASE WHEN petipreu IS NOT NULL THEN 1 ELSE 0 END flag_universitario,
 substr(petipreu,2,1) cod_universitario
FROM bi_corp_staging.malpe_pedt223 WHERE partition_date = '2020-05-07' ) p223
	ON p001.penumper = p223.penumper

LEFT JOIN
(SELECT penumper, pebancap, pedivisi,
 CASE WHEN pejefare IN ('0005','0065') then 1 else 0 end flag_agro
FROM bi_corp_staging.malpe_pedt040 WHERE partition_date = '2020-05-07' ) p040
	ON p001.penumper = p040.penumper

LEFT JOIN
(SELECT penumper, pecodafip
FROM bi_corp_staging.malpe_pedt063 WHERE partition_date = '2020-05-07' ) p063
	ON p001.penumper = p063.penumper

LEFT JOIN
(SELECT  p25.penumpar,p25.penumgru, p24.pedesgru
FROM bi_corp_staging.malpe_pedt025 p25
LEFT JOIN bi_corp_staging.malpe_pedt024 p24 on p24.penumgru = p25.penumgru
WHERE p25.partition_date= '2020-04-30' AND p24.partition_date=p25.partition_date) p025_024
    ON p001.penumper = p025_024.penumpar


LEFT JOIN
(select trim(gen_clave) as peforjur,trim(substr(gen_datos,1,40)) as descripcion
	from bi_corp_staging.tcdtgen
	where gentabla = '0231' and partition_Date = '2020-05-18') tcd_0231
	on p001.peforjur = tcd_0231.peforjur

LEFT JOIN
(	select trim(gen_clave) as peforjur,trim(substr(gen_datos,1,40)) as descripcion
	from bi_corp_staging.tcdtgen
	where gentabla = '0305' and partition_Date = '2020-05-18') tcd_0305
	on p001.peforjur = tcd_0305.peforjur

LEFT JOIN (
 select trim(substr(gen_datos,1,40)) descripcion, substr(gen_clave,2,1) codigo
 		from bi_corp_staging.tcdtgen
 		where gentabla = '1154' and partition_date = '2020-05-18') tcd_1154
 on p223.cod_universitario = tcd_1154.codigo

LEFT JOIN
(select trim(substr(gen_clave,1,1)) as petipdoc, gen_datos as descripcion
	from  bi_corp_staging.tcdtgen
	where gentabla = '0113' and partition_date = '2020-05-18') tcd_0113
    on p001.petipdoc = tcd_0113.petipdoc

LEFT JOIN (
	select trim(gen_clave) as pecodact, trim(substr(gen_datos,1,40)) as descripcion
	from bi_corp_staging.tcdtgen where gentabla = '0304' and partition_date= '2020-05-18') tcd_0304
ON p001.pecodact = tcd_0304.pecodact

LEFT JOIN
(SELECT penumper , MAX(flag_banca_privada) flag_banca_privada , MAX(flag_citi) flag_citi
, MAX(flag_international_desk) flag_international_desk , MAX(flag_uge) flag_uge
, MAX(flag_vip) flag_vip , MAX(flag_supervivencia) flag_supervivencia
, MAX(cod_vinculacion) cod_vinculacion
,MAX(flag_mipyme) flag_mipyme
, MAX(cod_mipyme) cod_mipyme
, MAX(flag_cuenta_blanca) flag_cuenta_blanca
FROM (SELECT penumper
, CASE WHEN peindica = 'BPR' THEN 1 ELSE 0 END flag_banca_privada
, CASE WHEN peindica = 'CIT' THEN 1 ELSE 0 END flag_citi
, CASE WHEN peindica = 'GEX' THEN 1 ELSE 0 END flag_international_desk
, CASE WHEN peindica = 'UGE' THEN 1 ELSE 0 END flag_uge
, CASE WHEN peindica = 'VIP' THEN 1 ELSE 0 END flag_vip
, CASE WHEN peindica = 'SUP' THEN 1 ELSE 0 END flag_supervivencia
, CASE WHEN peindica= 'SAL'  THEN 1 ELSE 0 END flag_cuenta_blanca
, CASE WHEN peindica IN ('AVI') THEN pevalind ELSE NULL END cod_vinculacion
, CASE WHEN peindica = 'MIP' THEN pevalind ELSE NULL END cod_mipyme
, CASE WHEN peindica = 'MIP' THEN 1 ELSE 0 END flag_mipyme
FROM bi_corp_staging.malpe_pedt052 WHERE partition_date = '2020-05-07')p52 GROUP BY penumper) p052
	ON p001.penumper = p052.penumper

LEFT JOIN
	(select substr(gen_clave,1,3) as peindica, substr(gen_clave,4,1) as pevalind, trim(substr(gen_datos,4)) as descripcion
	from bi_corp_staging.tcdtgen where gentabla = '0680' and substr(gen_clave,1,3) = 'AVI'
	and partition_date = '2020-05-18') tcd_0680_avi
on p052.cod_vinculacion = tcd_0680_avi.pevalind


LEFT JOIN(
		select alias_nup nup, shortname cod_kgr
		from bi_corp_staging.mdr_contrapartes
		where partition_date= '2020-04-29') contra
	on p001.penumper= contra.nup

LEFT JOIN (
			select nro_grupo, nup
			from bi_corp_staging.sge_grupos_economicos
			where partition_date = '2020-04-14') sge
	on p001.penumper= sge.nup

LEFT JOIN (
      	select unidad_operativa, entidad_legal cod_grupo, nombre_legal_kgl ds_codigo, glcs_grupo
				from bi_corp_staging.aqua_clientes_asoc_geconomicos
				where partition_date = '2020-04-01') aqua_asoc
		on aqua_asoc.unidad_operativa= contra.cod_kgr

LEFT JOIN
(select tar.cod_per_nup_tarjeta nup, max(tar.flag_woman) flag_woman, max(tar.flag_nova) flag_nova
   from (select cod_per_nup_tarjeta,
  	 case when stk_tcr_tarjetas.cod_tcr_estado_cuenta = 10
		and stk_tcr_tarjetas.cod_tcr_estado_tarjeta in (20,22)
		and concat(int_tcr_vigencia_anio_hasta ,lpad(int_tcr_vigencia_mes_hasta,0,2)) >= SUBSTRING(regexp_replace(partition_date,'-',''),0,6)
		and stk_tcr_tarjetas.ds_tcr_grupo_afinidad = 'NOVA' and stk_tcr_tarjetas.flag_tcr_titular = 0 then 1 else 0 end flag_nova,
	case when stk_tcr_tarjetas.cod_tcr_estado_cuenta = 10
		and stk_tcr_tarjetas.cod_tcr_estado_tarjeta in (20,22)
		and concat(int_tcr_vigencia_anio_hasta ,lpad(int_tcr_vigencia_mes_hasta,0,2)) >= SUBSTRING(regexp_replace(partition_date,'-',''),0,6)
		and stk_tcr_tarjetas.cod_tcr_tipo_promo = 'W' then 1 else 0 end flag_woman
	from bi_corp_common.stk_tcr_tarjetas
	where partition_date = '2020-05-27') tar group by cod_per_nup_tarjeta) tarj
	ON p001.penumper = tarj.nup


LEFT JOIN
(select cod_tcr_nup_titular nup , sum(fc_tarjetas) fc_tarjetas, max(flag_superclub) flag_superclub, max(flag_AA) flag_AA
	from (select cod_tcr_nup_titular ,
		  CASE WHEN cod_tcr_estado_cuenta = 10 and cod_tcr_nup_titular is not null and fc_tcr_importe_limite_compra >= 2 THEN 1 ELSE 0 END fc_tarjetas,
		  CASE WHEN cod_tcr_estado_cuenta = 10 and cod_tcr_nup_titular is not null and fc_tcr_importe_limite_compra > 2 and ds_tcr_grupo_afinidad = 'SUPERCLUB' THEN 1 ELSE 0 END flag_superclub,
          CASE WHEN cod_tcr_estado_cuenta = 10 and cod_tcr_nup_titular is not null and fc_tcr_importe_limite_compra > 2 and ds_tcr_grupo_afinidad = 'AMERICAN AIRLINES' THEN 1 ELSE 0 END flag_AA
          from bi_corp_common.stk_tcr_cuentas where partition_date = '2020-05-18' and cod_tcr_nup_titular is not null)tc GROUP by cod_tcr_nup_titular) cuentas
	ON p001.penumper = cuentas.nup

LEFT JOIN
(
SELECT penumper , MAX(flag_cuenta_interna) flag_cuenta_interna
, MAX(flag_titular) flag_titular , MAX(flag_adicional) flag_adicional
, MAX(flag_cotitular) flag_cotitular
, MAX(flag_firmante) flag_firmante
FROM (SELECT penumper
, CASE WHEN pecodpro IN ('98','99') THEN 1 ELSE 0 END flag_cuenta_interna
, CASE WHEN pecalpar = 'TI' and pecodpro in (01,02,03,05,06,07,14,35,36,37,38,39,40,41,42,45,46,74,80) and peestrel = 'A'
and pefecbrb = '9999-12-31'  THEN 1 ELSE 0 END flag_titular
, CASE WHEN pecalpar = 'AD' and peestrel = 'A' and pefecbrb = '9999-12-31'  THEN 1 ELSE 0 END flag_adicional
, CASE WHEN pecalpar = 'CT' AND COUNT(peordpar) OVER(PARTITION BY penumper)>1  and peestrel = 'A' and pefecbrb = '9999-12-31' THEN 1 ELSE 0 END flag_cotitular
, CASE WHEN pecalpar not in ('TI','CT','AD') AND peestrel = 'A' and pefecbrb = '9999-12-31' THEN 1 ELSE 0 END flag_firmante
FROM bi_corp_staging.malpe_pedt008 WHERE partition_date = '2020-05-07' )p8 GROUP BY penumper) p008
	ON p001.penumper = p008.penumper

LEFT JOIN
(SELECT penumper, pesegcla
, CASE WHEN SUBSTR(pesegcla,1,1) IN ('6','7','8','A','B','C','D','E','F','G','H','I') THEN 'INDIVIDUOS'
	WHEN SUBSTR(pesegcla,1,1) IN ('5','4','J','K','L','M') THEN 'PYME'
	WHEN SUBSTR(pesegcla,1,1) IN ('N','O','P','Q','3','9','0') THEN 'BEIA'
	WHEN SUBSTR(pesegcla,1,1) IN ('R','S','T','U','V','W','X','Y','Z','1','2') THEN 'BMA' END ds_per_segmento
, CASE WHEN SUBSTR(pesegcla,1,1) IN ('6','7','8') THEN 'SELECT'
	WHEN SUBSTR(pesegcla,1,1) IN ('A','B','C') THEN 'RENTA ALTA'
	WHEN SUBSTR(pesegcla,1,1) IN ('D','E','F') THEN 'RENTA MEDIA'
	WHEN SUBSTR(pesegcla,1,1) IN ('G','H','I') THEN 'RENTA MASIVA'
	WHEN SUBSTR(pesegcla,1,1) IN ('5','4') THEN 'DUO'
	WHEN SUBSTR(pesegcla,1,1) IN ('J','K') THEN 'PYME 1'
	WHEN SUBSTR(pesegcla,1,1) IN ('L','M') THEN 'PYME 2'
	WHEN SUBSTR(pesegcla,1,1) IN ('N','O') THEN 'EMPRESA'
	WHEN SUBSTR(pesegcla,1,1) IN ('P','Q') THEN 'GRAN EMPRESA' -- G1
	WHEN SUBSTR(pesegcla,1,1) IN ('3','9','0') THEN 'INSTITUCIONES'
	WHEN SUBSTR(pesegcla,1,1) IN ('R','S','T','U','V','W','X','Y','Z','1','2') THEN 'BMA' END ds_per_subsegmento
, CASE WHEN SUBSTR(pesegcla,1,1) = '6' THEN 'S1'
	WHEN SUBSTR(pesegcla,1,1) = '7' THEN 'S2'
	WHEN SUBSTR(pesegcla,1,1) = '8' THEN 'S3'
	WHEN SUBSTR(pesegcla,1,1) = 'A' THEN 'A1'
	WHEN SUBSTR(pesegcla,1,1) = 'B' THEN 'A2'
	WHEN SUBSTR(pesegcla,1,1) = 'C' THEN 'A3'
	WHEN SUBSTR(pesegcla,1,1) = 'D' THEN 'B1'
	WHEN SUBSTR(pesegcla,1,1) = 'E' THEN 'B2'
	WHEN SUBSTR(pesegcla,1,1) = 'F' THEN 'B3'
	WHEN SUBSTR(pesegcla,1,1) = 'G' THEN 'C1'
	WHEN SUBSTR(pesegcla,1,1) = 'H' THEN 'C2'
	WHEN SUBSTR(pesegcla,1,1) = 'I' THEN 'C3'
	WHEN SUBSTR(pesegcla,1,1) IN ('J','K') THEN 'P1'
	WHEN SUBSTR(pesegcla,1,1) IN ('L','M') THEN 'P2'
	WHEN SUBSTR(pesegcla,1,1) IN ('5','4') THEN 'C1'
	WHEN SUBSTR(pesegcla,1,1) IN ('N','O') THEN 'EM'
	WHEN SUBSTR(pesegcla,1,1) IN ('P','Q') THEN 'GE' -- G1
	WHEN SUBSTR(pesegcla,1,1) = '0' THEN 'IU'
	WHEN SUBSTR(pesegcla,1,1) IN ('3','9') THEN 'IP'
	WHEN SUBSTR(pesegcla,1,1) IN ('R','S','T','U','V','W','X','Y','Z','1','2') THEN 'C' END cod_per_cuadrante
FROM bi_corp_staging.malpe_pedt030 WHERE partition_date = '2020-05-07' ) p030
	ON p001.penumper = p030.penumper



LEFT JOIN
(select gen_clave as pevalind,substr(gen_datos,13,2) cod_mipyme, trim(substr(gen_clave, 4,1)) cod_mipyme_letra,
	CASE
		WHEN substr(gen_Clave, 1, 3) = 'MIP' AND substr(gen_datos, 13, 2) = '41' THEN "Microempresa – Agropecuario"
		WHEN substr(gen_Clave, 1, 3) = 'MIP' AND substr(gen_datos, 13, 2) = '42' THEN "Microempresa – Industria y Minería"
		WHEN substr(gen_Clave, 1, 3) = 'MIP' AND substr(gen_datos, 13, 2) = '43' THEN "Microempresa – Comercio"
		WHEN substr(gen_Clave, 1, 3) = 'MIP' AND substr(gen_datos, 13, 2) = '44' THEN "Microempresa – Servicios"
		WHEN substr(gen_Clave, 1, 3) = 'MIP' AND substr(gen_datos, 13, 2) = '45' THEN "Microempresa – Construccion"
		WHEN substr(gen_Clave, 1, 3) = 'MIP' AND substr(gen_datos, 13, 2) = '51' THEN "Pequeña Empresa – Agropecuario"
		WHEN substr(gen_Clave, 1, 3) = 'MIP' AND substr(gen_datos, 13, 2) = '52' THEN "Pequeña Empresa – Industria y Minería"
		WHEN substr(gen_Clave, 1, 3) = 'MIP' AND substr(gen_datos, 13, 2) = '53' THEN "Pequeña Empresa – Comercio"
		WHEN substr(gen_Clave, 1, 3) = 'MIP' AND substr(gen_datos, 13, 2) = '54' THEN "Pequeña Empresa – Servicios"
		WHEN substr(gen_Clave, 1, 3) = 'MIP' AND substr(gen_datos, 13, 2) = '55' THEN "Pequeña Empresa – Construccion"
		WHEN substr(gen_Clave, 1, 3) = 'MIP' AND substr(gen_datos, 13, 2) = '61' THEN "Mediana Empresa tramo 1 – Agropecuario"
		WHEN substr(gen_Clave, 1, 3) = 'MIP' AND substr(gen_datos, 13, 2) = '62' THEN "Mediana Empresa tramo 1 – Industria y Minería"
		WHEN substr(gen_Clave, 1, 3) = 'MIP' AND substr(gen_datos, 13, 2) = '63' THEN "Mediana Empresa tramo 1 – Comercio"
		WHEN substr(gen_Clave, 1, 3) = 'MIP' AND substr(gen_datos, 13, 2) = '64' THEN "Mediana Empresa tramo 1 – Servicios"
		WHEN substr(gen_Clave, 1, 3) = 'MIP' AND substr(gen_datos, 13, 2) = '65' THEN "Mediana Empresa tramo 1 – Construccion"
		WHEN substr(gen_Clave, 1, 3) = 'MIP' AND substr(gen_datos, 13, 2) = '71' THEN "Mediana Empresa tramo 2 – Agropecuario"
		WHEN substr(gen_Clave, 1, 3) = 'MIP' AND substr(gen_datos, 13, 2) = '72' THEN "Mediana Empresa tramo 2 – Industria y Minería"
		WHEN substr(gen_Clave, 1, 3) = 'MIP' AND substr(gen_datos, 13, 2) = '73' THEN "Mediana Empresa tramo 2 – Comercio"
		WHEN substr(gen_Clave, 1, 3) = 'MIP' AND substr(gen_datos, 13, 2) = '74' THEN "Mediana Empresa tramo 2 – Servicios"
		WHEN substr(gen_Clave, 1, 3) = 'MIP' AND substr(gen_datos, 13, 2) = '75' THEN "Mediana Empresa tramo 2 – Construccion"
		WHEN substr(gen_Clave, 1, 3) = 'MIP' AND substr(gen_datos, 13, 2) = '9'  THEN "Sin Sector "
		ELSE NULL
	END ds_mipyme
from bi_corp_staging.tcdtgen
where gentabla = '0680' and gen_clave like 'MIP%' and partition_date= '2020-05-18') tcd_0680_mip
on p052.cod_mipyme=tcd_0680_mip.cod_mipyme_letra ;