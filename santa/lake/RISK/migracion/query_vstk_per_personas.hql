CREATE TEMPORARY TABLE stk_persona2 AS 
SELECT DISTINCT B.* FROM (
SELECT A.cod_per_nup , A.cod_sucursal_administradora , A.flag_empleado , A.cod_clanae
, A.cod_per_segmento_duro , A.cod_per_banca , A.flag_banca_privada , A.flag_cuenta_interna
, A.flag_woman 
, CASE WHEN (MAX(A.flag_titular) OVER(PARTITION BY A.cod_per_nup) = 1) AND (A.fc_per_edad BETWEEN 18 AND 31) THEN 1 ELSE 0 END flag_IU
, A.flag_nova , A.flag_citi , A.flag_universitario , A.flag_international_desk , A.flag_uge 
, A.flag_vip , A.flag_plan_sueldo
, MAX(A.flag_titular) OVER(PARTITION BY A.cod_per_nup) flag_titular
, MAX(A.flag_cotitular) OVER(PARTITION BY A.cod_per_nup) flag_cotitular
, A.flag_adicional , A.flag_firmante , A.flag_supervivincia , A.flag_jubilado
, A.flag_cliente_anses , A.flag_colectivo , A.flag_digital_30
, A.cod_per_grupo_economico , A.cod_grupo_economico
, A.cod_top_level_grupo_economico , A.ds_top_level_grupo_economico
, A.ds_tipo_empresa, A.cod_per_ramo_empresa , A.flag_vinculacion
, A.ds_mapa_seguimiento , A.cod_kgr , A.dt_ultima_acreditacion
, A.ds_per_segmento , A.ds_per_subsegmento , A.cod_per_cuadrante
, A.dt_fecha_antiguedad , A.ds_test_inversor , A.dt_test_inversor_ult_fecha , A.flag_superclub 
, A.flag_AA , A.flag_sorpresa , A.flag_clave_automatico , A.cod_ofical_asignado , A.ds_ofical_asignado
, A.ds_modelo_atencion , A.flag_agro , A.cod_division , A.fc_maximo_dias_mora
, A.fc_productos_mora , A.fc_score_riesgo , A.fc_score_veraz , A.fc_raiting
, A.flag_raiting_plus , A.ds_principalidad_veraz_behaviour , A.ds_motivo_baja
, A.cod_per_sexo , A.cod_per_tipo_persona , A.dt_per_fecha_nacimiento , A.cod_per_estado_civil
, A.cod_per_pais_nacimiento , A.cod_per_nacionalidad , A.cod_per_residencia , A.cod_actividad_afip
, A.ds_tipo_sociedad , A.cod_per_condicion_cliente , A.cod_sit_bcra_bsr , A.cod_sit_bcra_sist_financiero
, A.fc_deuda_sist_financiero , A.flag_mi_pyme , A.fc_per_edad , A.ft_cuentas
, A.ft_tarjetas , A.ft_prestamos , A.ft_seguros , A.ft_plazo_fijo , A.ft_fci , A.ft_letras 
, A.ft_acciones , A.ft_bonos , A.ft_caja_seguridad , A.limite_pre_acordado , A.cod_ocupacion
, A.cod_actividad , A.flag_fallecido , A.cod_canal_venta , A.cod_actividad_bcra
, A.cod_campana , A.cod_sujeto , A.dt_fecha_inicio_actividades , A.cod_forma_juridica
, A.ft_score_cliente1 , A.ft_score_cliente2 , A.ft_score_alineado_cliente1
, A.ft_score_alineado_cliente2 , A.ft_score_bruto_cliente1 , A.ft_score_bruto_cliente2
, A.cod_renta , A.cod_segmento_triad , A.cod_suc_paquete , A.cod_zona
, A.ft_limite_disponible_compra_TC , A.ft_monto_total_impago_cliente
, A.ft_ingreso_bruto_calificacion , A.dt_fecha_ultima_calificacion
, A.ft_mon_ingreso , A.partition_date 
FROM (
SELECT p001.penumper cod_per_nup
, p001.pesucadm cod_sucursal_administradora
, MAX(p002.pemaremp) OVER(PARTITION BY p001.penumper) flag_empleado
, NULL cod_clanae
, p030.pesegcla cod_per_segmento_duro
, p040.pebancap cod_per_banca
, MAX(CASE WHEN p052.peindica = 'BPR' THEN 1 ELSE 0 END) OVER(PARTITION BY p001.penumper) flag_banca_privada
, MAX(CASE WHEN p008.pecodpro IN ('98','99') THEN 1 ELSE 0 END) OVER(PARTITION BY p001.penumper) flag_cuenta_interna
, NULL flag_woman
, NULL flag_nova
, MAX(CASE WHEN p052.peindica = 'CIT' THEN 1 ELSE 0 END) OVER(PARTITION BY p001.penumper) flag_citi
, MAX(CASE WHEN p223.petipreu IS NOT NULL THEN 1 ELSE 0 END) OVER(PARTITION BY p001.penumper) flag_universitario
, MAX(CASE WHEN p052.peindica = 'GEX' THEN 1 ELSE 0 END) OVER(PARTITION BY p001.penumper) flag_international_desk
, MAX(CASE WHEN p052.peindica = 'UGE' THEN 1 ELSE 0 END) OVER(PARTITION BY p001.penumper) flag_uge
, MAX(CASE WHEN p052.peindica = 'VIP' THEN 1 ELSE 0 END) OVER(PARTITION BY p001.penumper) flag_vip
, NULL flag_plan_sueldo 
, CASE WHEN p008.pecalpar = 'TI' AND p008.peordpar = (MIN(p008.peordpar) OVER(PARTITION BY p008.penumper))  THEN 1 ELSE 0 END flag_titular
, CASE WHEN p008.pecalpar = 'CT' AND (COUNT(p008.peordpar) OVER(PARTITION BY p008.penumper))>1 THEN 1 ELSE 0 END flag_cotitular
, MAX(CASE WHEN p008.pecalpar = 'AD' THEN 1 ELSE 0 END) OVER(PARTITION BY p001.penumper)  flag_adicional
, NULL flag_firmante
, MAX(CASE WHEN p052.peindica = 'SUP' THEN 1 ELSE 0 END) OVER(PARTITION BY p001.penumper) flag_supervivincia
, NULL flag_jubilado
, NULL flag_cliente_anses , NULL flag_colectivo , NULL flag_digital_30
, NULL cod_per_grupo_economico , NULL cod_grupo_economico
, NULL cod_top_level_grupo_economico , NULL ds_top_level_grupo_economico
, NULL ds_tipo_empresa
, p005.peramemp cod_per_ramo_empresa
, MAX(CASE WHEN p052.peindica IN ('AVI','AYV') THEN 1 ELSE 0 END) OVER(PARTITION BY p001.penumper) flag_vinculacion
, NULL ds_mapa_seguimiento
, NULL cod_kgr
, NULL dt_ultima_acreditacion
, CASE WHEN SUBSTR(p030.pesegcla,1,1) IN ('6','7','8','A','B','C','D','E','F','G','H','I') THEN 'INDIVIDUOS'
	WHEN SUBSTR(p030.pesegcla,1,1) IN ('5','4','J','K','L','M') THEN 'PYME'
	WHEN SUBSTR(p030.pesegcla,1,1) IN ('N','O','P','Q','3','9','0') THEN 'BEIA'
	WHEN SUBSTR(p030.pesegcla,1,1) IN ('R','S','T','U','V','W','X','Y','Z','1','2') THEN 'BMA' END ds_per_segmento
, CASE WHEN SUBSTR(p030.pesegcla,1,1) IN ('6','7','8') THEN 'SELECT'
	WHEN SUBSTR(p030.pesegcla,1,1) IN ('A','B','C') THEN 'RENTA ALTA'
	WHEN SUBSTR(p030.pesegcla,1,1) IN ('D','E','F') THEN 'RENTA MEDIA'
	WHEN SUBSTR(p030.pesegcla,1,1) IN ('G','H','I') THEN 'RENTA MASIVA'
	WHEN SUBSTR(p030.pesegcla,1,1) IN ('5','4') THEN 'DUO'
	WHEN SUBSTR(p030.pesegcla,1,1) IN ('J','K') THEN 'PYME 1'
	WHEN SUBSTR(p030.pesegcla,1,1) IN ('L','M') THEN 'PYME 2'
	WHEN SUBSTR(p030.pesegcla,1,1) IN ('N','O') THEN 'EMPRESA'
	WHEN SUBSTR(p030.pesegcla,1,1) IN ('P','Q') THEN 'GRAN EMPRESA' -- G1
	WHEN SUBSTR(p030.pesegcla,1,1) IN ('3','9','0') THEN 'INSTITUCIONES'
	WHEN SUBSTR(p030.pesegcla,1,1) IN ('R','S','T','U','V','W','X','Y','Z','1','2') THEN 'BMA' END ds_per_subsegmento
, CASE WHEN SUBSTR(p030.pesegcla,1,1) = '6' THEN 'S1'
	WHEN SUBSTR(p030.pesegcla,1,1) = '7' THEN 'S2'
	WHEN SUBSTR(p030.pesegcla,1,1) = '8' THEN 'S3'
	WHEN SUBSTR(p030.pesegcla,1,1) = 'A' THEN 'A1'
	WHEN SUBSTR(p030.pesegcla,1,1) = 'B' THEN 'A2'
	WHEN SUBSTR(p030.pesegcla,1,1) = 'C' THEN 'A3'
	WHEN SUBSTR(p030.pesegcla,1,1) = 'D' THEN 'B1'
	WHEN SUBSTR(p030.pesegcla,1,1) = 'E' THEN 'B2'
	WHEN SUBSTR(p030.pesegcla,1,1) = 'F' THEN 'B3'
	WHEN SUBSTR(p030.pesegcla,1,1) = 'G' THEN 'C1'
	WHEN SUBSTR(p030.pesegcla,1,1) = 'H' THEN 'C2'
	WHEN SUBSTR(p030.pesegcla,1,1) = 'I' THEN 'C3'
	WHEN SUBSTR(p030.pesegcla,1,1) IN ('J','K') THEN 'P1'
	WHEN SUBSTR(p030.pesegcla,1,1) IN ('L','M') THEN 'P2'
	WHEN SUBSTR(p030.pesegcla,1,1) IN ('5','4') THEN 'PC1'
	WHEN SUBSTR(p030.pesegcla,1,1) IN ('N','O') THEN 'EM'
	WHEN SUBSTR(p030.pesegcla,1,1) IN ('P','Q') THEN 'GE' -- G1
	WHEN SUBSTR(p030.pesegcla,1,1) = '0' THEN 'IU'
	WHEN SUBSTR(p030.pesegcla,1,1) IN ('3','9') THEN 'IP'
	WHEN SUBSTR(p030.pesegcla,1,1) IN ('R','S','T','U','V','W','X','Y','Z','1','2') THEN 'C' END cod_per_cuadrante
, NULL dt_fecha_antiguedad
, NULL ds_test_inversor , NULL dt_test_inversor_ult_fecha , NULL flag_superclub , NULL flag_AA
, NULL flag_sorpresa , NULL flag_clave_automatico , NULL cod_ofical_asignado , NULL ds_ofical_asignado
, NULL ds_modelo_atencion , NULL flag_agro , NULL cod_division , NULL fc_maximo_dias_mora
, NULL fc_productos_mora , NULL fc_score_riesgo , NULL fc_score_veraz , NULL fc_raiting
, NULL flag_raiting_plus , NULL ds_principalidad_veraz_behaviour , NULL ds_motivo_baja
, CASE WHEN p001.pesexper = 'F' THEN 'M' WHEN p001.petipper = 'J' OR p001.pesexper = '1' THEN NULL ELSE p001.pesexper END cod_per_sexo
, p001.petipper cod_per_tipo_persona
, p001.pefecnac dt_per_fecha_nacimiento
, p001.peestciv cod_per_estado_civil
, p001.pepaiori cod_per_pais_nacimiento
, p001.penacper cod_per_nacionalidad
, p001.pepaires cod_per_residencia
, p063.pecodafip cod_actividad_afip
, NULL ds_tipo_sociedad
, p001.peconper cod_per_condicion_cliente
, NULL cod_sit_bcra_bsr
, NULL cod_sit_bcra_sist_financiero
, NULL fc_deuda_sist_financiero
, NULL flag_mi_pyme
, (year(current_timestamp()) - year(p001.pefecnac)) + (CASE WHEN (month(current_timestamp()) < month(p001.pefecnac))
	OR (month(current_timestamp()) = month(p001.pefecnac) AND day(current_timestamp()) > day(p001.pefecnac)) THEN -1 ELSE 0 END) fc_per_edad
, NULL ft_cuentas 
, NULL ft_tarjetas , NULL ft_prestamos , NULL ft_seguros , NULL ft_plazo_fijo
, NULL ft_fci , NULL ft_letras , NULL ft_acciones , NULL ft_bonos
, NULL ft_caja_seguridad , NULL limite_pre_acordado , NULL cod_ocupacion
, NULL cod_actividad , NULL flag_fallecido , NULL cod_canal_venta
, p002.petipact cod_actividad_bcra
, NULL cod_campana , NULL cod_sujeto , NULL dt_fecha_inicio_actividades , NULL cod_forma_juridica
, NULL ft_score_cliente1 , NULL ft_score_cliente2 , NULL ft_score_alineado_cliente1
, NULL ft_score_alineado_cliente2 , NULL ft_score_bruto_cliente1 , NULL ft_score_bruto_cliente2
, NULL cod_renta , NULL cod_segmento_triad , NULL cod_suc_paquete , NULL cod_zona
, NULL ft_limite_disponible_compra_TC , NULL ft_monto_total_impago_cliente
, NULL ft_ingreso_bruto_calificacion , NULL dt_fecha_ultima_calificacion
, NULL ft_mon_ingreso , p001.partition_date 
FROM bi_corp_staging.malpe_pedt001 p001
LEFT OUTER JOIN bi_corp_staging.malpe_pedt002 p002
	ON p001.penumper = p002.penumper 
	AND p001.partition_date = p002.partition_date 
LEFT JOIN bi_corp_staging.malpe_pedt005 p005
	ON p001.penumper = p005.penumper 
	AND p001.partition_date = p005.partition_date 
LEFT JOIN bi_corp_staging.malpe_pedt008 p008
	ON p001.penumper = p008.penumper 
	AND p001.partition_date = p008.partition_date 
LEFT JOIN bi_corp_staging.malpe_pedt030 p030
	ON p001.penumper = p030.penumper 
	AND p001.partition_date = p030.partition_date
LEFT JOIN bi_corp_staging.malpe_pedt052 p052
	ON p001.penumper = p052.penumper 
	AND p001.partition_date = p052.partition_date
LEFT JOIN bi_corp_staging.malpe_pedt223 p223
	ON p001.penumper = p223.penumper 
	AND p001.partition_date = p223.partition_date
--LEFT JOIN bi_corp_staging.malpe_pedt025 p025
	--ON p001.penumper = p025.penumper 
--	AND p001.partition_date = p025.partition_date
LEFT JOIN bi_corp_staging.malpe_pedt040 p040
	ON p001.penumper = p040.penumper 
	AND p001.partition_date = p040.partition_date
LEFT JOIN bi_corp_staging.malpe_pedt063 p063
	ON p001.penumper = p063.penumper 
	AND p001.partition_date = p063.partition_date
WHERE p001.partition_date = '2020-05-04' 
)A)B;
	
	
--'{{ ti.xcom_pull(task_ids='InputConfig', key='partition_date', dag_id='LOAD_Malpe-Daily') }}'


--LEFT JOIN bi_corp_staging.malpe_pedt025 p025
--	ON p001.penumper = p025.penumper 
--	AND p025.partition_date = '2020-03-31'
	--'{{ ti.xcom_pull(task_ids='InputConfig', key='previous_month_to', dag_id='LOAD_Malpe-Daily') }}'


