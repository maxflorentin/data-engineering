set mapred.job.queue.name=root.dataeng;
set hive.exec.dynamic.partition.mode=nonstrict;

-- Tablas para traerme el cuit correspondiente sin duplicados

with tablat as (
select penumper,penumdoc cuit , trim(petipdoc) petidoc
from bi_corp_staging.malpe_ptb_pedt015
WHERE partition_date = '{{ ti.xcom_pull(task_ids='InputConfig', key='partition_date', dag_id='LOAD_CMN_Personas-Daily') }}'
and trim(petipdoc) = 'T'

),
tablal as (
select penumper,penumdoc cuit , trim(petipdoc) petidoc
from bi_corp_staging.malpe_ptb_pedt015
WHERE partition_date = '{{ ti.xcom_pull(task_ids='InputConfig', key='partition_date', dag_id='LOAD_CMN_Personas-Daily') }}'
and trim(petipdoc) = 'L'
),
tablad as (
select penumper,penumdoc cuit , trim(petipdoc) petidoc
from bi_corp_staging.malpe_ptb_pedt015
WHERE partition_date = '{{ ti.xcom_pull(task_ids='InputConfig', key='partition_date', dag_id='LOAD_CMN_Personas-Daily') }}'
and trim(petipdoc) = 'D'
)


INSERT OVERWRITE TABLE  bi_corp_common.stk_per_personas
PARTITION(partition_date)

SELECT
DISTINCT CAST(p001.penumper AS BIGINT) 																						cod_per_nup
,TRIM(p001.penomper) 																								ds_per_nombre
,CASE WHEN TRIM(p001.pepriape) = '' THEN NULL ELSE TRIM(p001.pepriape) END 											ds_per_apellido
,TRIM(p001.petipdoc)																								cod_per_tipodoc
,TRIM(tcd_0113.descripcion)  																						ds_per_tipodoc
,CAST(p001.penumdoc AS BIGINT) 																						ds_per_numdoc
,CAST((CASE when p015l.petidoc IS NOT NULL THEN p015l.cuit
 	 ELSE
 	 	 CASE WHEN p015t.petidoc IS NOT NULL THEN p015t.cuit
 	 	 	  ELSE p015d.cuit
 	 	 END
 END ) AS BIGINT) 																									ds_per_cuit
, CAST(p001.pesucadm AS BIGINT) 																					cod_per_sucursaladministradora
, CASE WHEN p002.flag_empleado = 'S' THEN 1 ELSE 0 END 																flag_per_empleado
, TRIM(p030.pesegcla) 																								cod_per_segmentoduro
, p030.ds_per_segmento 																								ds_per_segmento
, p030.ds_per_subsegmento 																							ds_per_subsegmento
, p030.cod_per_cuadrante 																							cod_per_cuadrante
,p030.ds_per_grupo 																									ds_per_segmento_grupo
, TRIM(p001.petipper) 																								cod_per_tipopersona
, TRIM(p040.pebancap) 																								cod_per_banca
, tcd_0255.descripcion																								ds_per_banca
, CASE WHEN p052.flag_banca_privada is not null THEN p052.flag_banca_privada ELSE 0 END 							flag_per_bancaprivada
, CASE WHEN p008.flag_cuenta_interna is not null THEN p008.flag_cuenta_interna ELSE 0 END 							flag_per_cuentainterna
, CASE WHEN p008.flag_cotitular is not null THEN p008.flag_cotitular ELSE 0 END 									flag_per_cotitular
, CASE WHEN p008.flag_firmante is not null THEN p008.flag_firmante  ELSE 0 END 										flag_per_firmante
, IF(p001.partition_date >='2020-08-28',
    CASE WHEN p052.flag_per_iu is not null THEN p052.flag_per_iu  ELSE 0 END,
    CASE WHEN p008.flag_titular = 1 AND p001.petipper = 'F' AND p001.fc_per_edad BETWEEN 18 AND 31 THEN 1 ELSE 0 END)	flag_per_iu
, IF(p001.partition_date >='2020-10-05',
  CASE WHEN p052.flag_per_nova_titular is not null THEN p052.flag_per_nova_titular  ELSE 0 END ,
  CASE WHEN nova.nova_titular is not null THEN nova.nova_titular  ELSE 0 END)					                    flag_per_nova_titular
, IF(p001.partition_date >='2020-10-05',
  CASE WHEN p052.flag_per_nova_usuario is not null THEN p052.flag_per_nova_usuario  ELSE 0 END ,
  CASE WHEN nova.nova_usuario is not null THEN nova.nova_usuario  ELSE 0 END)										flag_per_nova_usuario
, CASE WHEN p052.flag_citi is not null THEN p052.flag_citi ELSE 0 END 												flag_per_citi
, CASE WHEN p052.flag_international_desk is not null THEN p052.flag_international_desk ELSE 0 END 					flag_per_internationaldesk
, CASE WHEN p052.flag_uge  is not null THEN p052.flag_uge ELSE 0 END 												flag_per_uge
, CASE WHEN p052.flag_vip is not null THEN p052.flag_vip ELSE 0 END 												flag_per_vip
, CASE WHEN p052.flag_supervivencia is not null THEN p052.flag_supervivencia ELSE 0 END 							flag_per_supervivencia
, CASE WHEN p052.flag_per_comex is not null THEN p052.flag_per_comex ELSE 0 END 									flag_per_comex
, TRIM(p025_024.penumgru) 																							cod_per_grupoeconomico
, TRIM(p025_024.pedesgru) 																							ds_per_grupoeconomico
, CASE WHEN TRIM(p005.peramemp)= '' THEN NULL ELSE TRIM(p005.peramemp) END 											cod_per_ramoempresa
, TRIM(contra.cod_kgr) 																								cod_per_kgr
, CASE WHEN p052.flag_cuenta_blanca is not null THEN p052.flag_cuenta_blanca ELSE 0 END 							flag_per_cuentablanca
, IF(p001.partition_date < '2021-01-01',CASE WHEN p040.flag_agro is not null THEN p040.flag_agro ELSE 0 END,
	p030.flag_agro)										                                                            flag_per_agro
, p040.pedivisi 																									cod_per_division
, TRIM(p001.cod_per_sexo) 																							cod_per_sexo
, TO_DATE(p001.pefecnac) 																							dt_per_fechanacimiento
, p001.fc_per_edad																									fc_per_edad
, CASE WHEN TRIM(p001.peestciv) = '' THEN NULL ELSE TRIM(p001.peestciv) END 										cod_per_estadocivil
, tcd_0116.descripcion																								ds_per_estadocivil
, CASE WHEN TRIM(p001.pepaiori)= '' THEN NULL ELSE TRIM(p001.pepaiori) END 											cod_per_paisnacimiento
, CASE WHEN TRIM(p001.penacper)= '' THEN NULL ELSE TRIM(p001.penacper) END  										cod_per_nacionalidad
, tcd_0112.descripcion																								ds_per_nacionalidad
, CASE WHEN TRIM(p001.pepaires)= '' THEN NULL ELSE TRIM(p001.pepaires) END 											cod_per_residencia
, CASE WHEN TRIM(p063.pecodafip)= '' THEN NULL ELSE TRIM(p063.pecodafip) END 										cod_per_actividadafip
, tcd_1677.descripcion 																								ds_per_actividadafip
, case when trim(p001.peforjur) = '' then null else TRIM(p001.peforjur) end  										cod_per_tiposociedad
, TRIM(tcd_0231.descripcion)																						ds_per_tiposociedad
, TRIM(p001.peconper) 																								cod_per_condicioncliente
, CASE WHEN p052.flag_mipyme is not null THEN p052.flag_mipyme ELSE 0 END 											flag_per_mipyme
, TRIM(p052.cod_mipyme) 																							cod_per_mipyme
, TRIM(tcd_0680_mip.ds_mipyme) 																						ds_per_mipyme
, CASE WHEN (flag_per_plus1=1 and p030.cod_per_cuadrante in ('P1','PC1'))
	OR (flag_per_plus2=1 and p030.cod_per_cuadrante in ('IU','IP')) THEN 1 ELSE 0 END								flag_per_plus
, p052.flag_per_gestorremoto																						flag_per_gestorremoto
, CASE WHEN trim(p001.petipocu) = '' THEN NULL ELSE TRIM(p001.petipocu) END 										cod_per_ocupacion
, TRIM(tcd_0305.descripcion) 																						ds_per_ocupacion
, adsf.situacion_bcra 																								cod_per_sitbcrabs
, CASE WHEN TRIM(p001.pecodact) = '' THEN NULL ELSE TRIM(p001.pecodact) END 										cod_per_actividadbcra
, TRIM(tcd_0304.descripcion) 																						ds_per_actividadbcra
, tcdtrac.tcdtrac_cod_aco_csf10  																					cod_per_clanae
, p052.cod_per_rsg																									cod_per_rsg
, p052.cod_per_ade																									cod_per_ade
, p052.cod_per_adp																									cod_per_adp
, p009.peresiva 										 															cod_per_iva
, CASE WHEN p223.flag_universitario is not null THEN p223.flag_universitario ELSE 0 END 							flag_per_universitario
, TRIM(p223.cod_universitario) 																						cod_per_universitario
, TRIM(TRANSLATE(tcd_1154.descripcion,'�', 'Ñ'))																	ds_per_universitario
, p223.cod_per_siglauniversitaria   																				cod_per_siglauniversitaria
, tcd_0351.ds_per_siglauniversitaria																				ds_per_siglauniversitaria
, tcd_0351.ds_per_siglauniversitaria_agrup																			ds_per_siglauniversitaria_agrup
, p223.cod_per_caruniversitaria																						cod_per_caruniversitaria
, tcd_0508.descripcion																								ds_per_caruniversitaria
, p052.flag_per_getnet																								flag_per_getnet
, CASE WHEN  cas.sorpresa_suscripto is not null THEN cas.sorpresa_suscripto ELSE 0 END 		    					flag_per_sorpresa_suscripto
, CASE WHEN  cas.sorpresa_suscriptor is not null THEN  cas.sorpresa_suscriptor ELSE 0 END   						flag_per_sorpresa_suscriptor
, p052.flag_per_jubiladoanses 																						flag_per_jubiladoanses
, p052.flag_per_jubiladoresto				 																		flag_per_jubiladoresto
, NULL                                                                                                              flag_per_pas
, NULL																												flag_per_sectorpublico_trf
, NULL 																												flag_per_sectorpublico_acred
, pre.cod_rechazo																									cod_per_rechazo
, pre.mot_rechazo																									ds_per_motivo_rechazo
, pre.linea_pes																										fc_per_limite_preacordado
, pre.linea_disp_pes																								fc_per_limite_disponible
, pre.cuota_pes 																									ds_per_cuota_maxima
, p036.peimping 																									fc_per_moningreso
, garra.max_dias_atraso 																							fc_per_maximodiasatraso
, garra.productos_atrasaros 																						fc_per_productosatrasados
, garra.monto_total_impago 																							fc_per_montoirregularcliente
, TRIM(mesac.ds_test_inversor) 																						ds_per_testinversor
, mesac.dt_test_inversor_ult_fecha  																				dt_per_testinversorultfecha
, p016.cod_per_nup_ant																								cod_per_nup_anterior
, case when p001.pefecing='0001-01-01' then null else  p001.pefecing end 											dt_per_ingreso_banco
, p001.partition_date 																								partition_date

FROM
(SELECT penumper, penomper, pepriape, trim(petipdoc) petipdoc, penumdoc, pesucadm, petipper, pefecnac, peestciv, pepaiori
, penacper, pepaires, peconper , petipocu ,pecodact, peforjur,partition_date
, (year(partition_date) - year(pefecnac)) + (CASE WHEN (month(partition_date) < month(pefecnac))
OR (month(partition_date) = month(pefecnac) AND day(partition_date) < day(pefecnac))
THEN -1 ELSE 0 END) fc_per_edad
, CASE WHEN pesexper = 'F' THEN 'M' WHEN petipper = 'J' OR pesexper = '1' THEN NULL ELSE pesexper END cod_per_sexo
, pefecing
FROM bi_corp_staging.malpe_ptb_pedt001
WHERE partition_date =  '{{ ti.xcom_pull(task_ids='InputConfig', key='partition_date', dag_id='LOAD_CMN_Personas-Daily') }}' ) p001

LEFT OUTER JOIN
(SELECT penumper, pemaremp flag_empleado, petipact
FROM bi_corp_staging.malpe_pedt002
WHERE partition_date =  '{{ ti.xcom_pull(task_ids='InputConfig', key='partition_date', dag_id='LOAD_CMN_Personas-Daily') }}' ) p002
	ON p001.penumper = p002.penumper

LEFT JOIN
(SELECT penumper, peramemp
FROM bi_corp_staging.malpe_pedt005
WHERE partition_date =  '{{ ti.xcom_pull(task_ids='InputConfig', key='partition_date', dag_id='LOAD_CMN_Personas-Daily') }}' ) p005
	ON p001.penumper = p005.penumper

LEFT JOIN
(
SELECT penumper , MAX(flag_cuenta_interna) flag_cuenta_interna
, MAX(flag_titular) flag_titular , MAX(flag_adicional) flag_adicional
, MAX(flag_cotitular) flag_cotitular
, MAX(flag_firmante) flag_firmante
FROM (SELECT penumper
, CASE WHEN pecodpro IN ('98','99')  and peestrel = 'A' and pefecbrb  = '9999-12-31' THEN 1 ELSE 0 END flag_cuenta_interna
, CASE WHEN pecalpar = 'TI' and pecodpro in ('01','02','03','05','06','07','14','35','36','37','38','39','40','41','42','45','46','74','80') and peestrel = 'A'
and pefecbrb = '9999-12-31'  THEN 1 ELSE 0 END flag_titular
, CASE WHEN pecalpar = 'AD' and peestrel = 'A' and pefecbrb = '9999-12-31'  THEN 1 ELSE 0 END flag_adicional
, CASE WHEN pecalpar = 'CT'  and pecodpro in ('01','02','03','05','06','07','14','35','36','37','38','39','40','41','42','45','46','74','80')  and peestrel = 'A' and pefecbrb = '9999-12-31' THEN 1 ELSE 0 END flag_cotitular
, CASE WHEN pecalpar not in ('TI','CT','AD') AND peestrel = 'A' and pefecbrb = '9999-12-31' THEN 1 ELSE 0 END flag_firmante
FROM bi_corp_staging.malpe_ptb_pedt008
WHERE partition_date =  '{{ ti.xcom_pull(task_ids='InputConfig', key='partition_date', dag_id='LOAD_CMN_Personas-Daily') }}' )p8 GROUP BY penumper) p008
	ON p001.penumper = p008.penumper

 LEFT JOIN
 ( select peresiva, penumper
   from (select
  	 row_number() OVER  (partition by pedt009_penumper order by pedt009_pehstamp desc) as orden,
  	pedt009_peresiva peresiva,
  	pedt009_penumper penumper
  from bi_corp_staging.malpe_pedt009
where partition_Date=IF('{{ ti.xcom_pull(task_ids='InputConfig', key='partition_date', dag_id='LOAD_CMN_Personas-Daily') }}' <= '2021-02-18','2021-02-18','{{ ti.xcom_pull(task_ids='InputConfig', key='partition_date', dag_id='LOAD_CMN_Personas-Daily') }}' )and
 '{{ ti.xcom_pull(task_ids='InputConfig', key='partition_date', dag_id='LOAD_CMN_Personas-Daily') }}'
 	between pedt009_pefecini  and  case when pedt009_pefecfin ='9999-12-31' then '{{ ti.xcom_pull(task_ids='InputConfig', key='partition_date', dag_id='LOAD_CMN_Personas-Daily') }}'
 	else  pedt009_pefecfin end)p9 where orden=1 ) p009
 	ON p001.penumper = p009.penumper

 LEFT JOIN
( select distinct cod_per_nup, cod_per_nup_ant
 from (
select row_number() OVER  (partition by penumper order by pehstamp desc) as orden1,
penumper cod_per_nup,
penclref cod_per_nup_ant
from bi_corp_staging.malpe_pedt016
where partition_date = '{{ ti.xcom_pull(task_ids='InputConfig', key='partition_date', dag_id='LOAD_CMN_Personas-Daily') }}'
and peestref = 'R' ) p16 where orden1=1 ) p016
 	ON p001.penumper = p016.cod_per_nup

 LEFT JOIN
 (SELECT pe30.penumper penumper, trim(pe30.pesegcla) pesegcla,
  CASE WHEN trim(pe30.pesegcla)  in ('K','M', '4', 'O', 'Q') THEN 1 ELSE 0 END flag_agro,
  seg.ds_per_segmento ds_per_segmento, seg.ds_per_subsegmento ds_per_subsegmento, seg.cod_per_cuadrante cod_per_cuadrante,
  seg.ds_per_grupo, seg.ds_per_marca
 FROM bi_corp_staging.malpe_ptb_pedt030 pe30
 LEFT JOIN bi_corp_common.dim_per_segmentos seg ON trim(pe30.pesegcla)= trim(seg.cod_per_segmentoduro)
 WHERE pe30.partition_date ='{{ ti.xcom_pull(task_ids='InputConfig', key='partition_date', dag_id='LOAD_CMN_Personas-Daily') }}')p030
 	ON p001.penumper = p030.penumper


LEFT JOIN
(SELECT penumper , MAX(flag_banca_privada) flag_banca_privada , MAX(flag_citi) flag_citi
, MAX(flag_international_desk) flag_international_desk , MAX(flag_uge) flag_uge
, MAX(flag_vip) flag_vip , MAX(flag_supervivencia) flag_supervivencia
, MAX(flag_mipyme) flag_mipyme
, MAX(cod_mipyme) cod_mipyme
, MAX(flag_cuenta_blanca) flag_cuenta_blanca
, MAX(flag_per_jubiladoanses) flag_per_jubiladoanses
, MAX(flag_per_jubiladoresto) flag_per_jubiladoresto
, MAX(flag_per_getnet) flag_per_getnet
, MAX(flag_per_plus1) flag_per_plus1
, MAX(flag_per_plus2) flag_per_plus2
, MAX(flag_per_gestorremoto) flag_per_gestorremoto
, MAX(cod_per_rsg) cod_per_rsg
, MAX(cod_per_ade) cod_per_ade
, MAX(cod_per_adp) cod_per_adp
, MAX(flag_per_iu) flag_per_iu
, MAX(flag_per_nova_titular) flag_per_nova_titular
, MAX(flag_per_nova_usuario) flag_per_nova_usuario
, MAX(flag_per_comex) flag_per_comex


FROM (SELECT penumper
, CASE WHEN peindica = 'BPR' THEN 1 ELSE 0 END flag_banca_privada
, CASE WHEN peindica = 'CIT' THEN 1 ELSE 0 END flag_citi
, CASE WHEN peindica = 'GEX' THEN 1 ELSE 0 END flag_international_desk
, CASE WHEN peindica = 'UGE' THEN 1 ELSE 0 END flag_uge
, CASE WHEN peindica = 'VIP' THEN 1 ELSE 0 END flag_vip
, CASE WHEN peindica = 'SUP' THEN 1 ELSE 0 END flag_supervivencia
, CASE WHEN peindica= 'SAL'  THEN 1 ELSE 0 END flag_cuenta_blanca
, CASE WHEN peindica = 'MIP' THEN pevalind ELSE NULL END cod_mipyme
, CASE WHEN peindica = 'MIP' THEN 1 ELSE 0 END flag_mipyme
, CASE WHEN peindica = 'RSG' THEN pevalind ELSE NULL END cod_per_rsg
, CASE WHEN peindica = 'ADE' THEN pevalind ELSE NULL END cod_per_ade
, CASE WHEN peindica = 'ADP' THEN pevalind ELSE NULL END cod_per_adp
, CASE WHEN peindica = 'CSR' AND  pevalind = 'S' THEN 1 ELSE 0 END flag_per_jubiladoanses
, CASE WHEN peindica = 'CSR' AND pevalind = 'P' THEN 1 ELSE 0 END flag_per_jubiladoresto
, CASE WHEN peindica = 'NET' and pevalind = 'S' THEN 1 ELSE 0 END flag_per_getnet
, CASE WHEN peindica = 'GES'  THEN 1 ELSE 0 END flag_per_gestorremoto
, CASE WHEN peindica = 'SEL' and pevalind in ('E','B') THEN 1 ELSE 0 END flag_per_plus1
, CASE WHEN peindica = 'SEL' and pevalind in ('I') THEN 1 ELSE 0 END flag_per_plus2
, CASE WHEN peindica = 'IUU'  THEN 1 ELSE 0 END flag_per_iu
, CASE WHEN peindica = 'NOV' and pevalind in ('A') THEN 1 ELSE 0 END flag_per_nova_titular
, CASE WHEN peindica = 'NOV' and pevalind in ('M') THEN 1 ELSE 0 END flag_per_nova_usuario
, CASE WHEN peindica = 'CEX' THEN 1 ELSE 0 END flag_per_comex


FROM bi_corp_staging.malpe_ptb_pedt052
WHERE partition_date = '{{ ti.xcom_pull(task_ids='InputConfig', key='partition_date', dag_id='LOAD_CMN_Personas-Daily') }}')p52 GROUP BY penumper) p052
	ON p001.penumper = p052.penumper

LEFT JOIN
(SELECT max(a.pedt036_peimping) peimping, a.pedt036_penumper penumper
 FROM bi_corp_staging.malpe_pedt036 a
 WHERE  a.pedt036_pefecing IN (select max(b.pedt036_pefecing)
                            from  bi_corp_staging.malpe_pedt036 b where
                            b.pedt036_pefecing  <= IF('{{ ti.xcom_pull(task_ids='InputConfig', key='partition_date', dag_id='LOAD_CMN_Personas-Daily') }}' <  '2020-06-26',  '{{ ti.xcom_pull(task_ids='InputConfig', key='last_working_day', dag_id='LOAD_CMN_Personas-Daily') }}','{{ ti.xcom_pull(task_ids='InputConfig', key='partition_date', dag_id='LOAD_CMN_Personas-Daily') }}' )
                            and a.pedt036_penumper=b.pedt036_penumper)

    and a.partition_date =  IF('{{ ti.xcom_pull(task_ids='InputConfig', key='partition_date', dag_id='LOAD_CMN_Personas-Daily') }}' <  '2020-06-26',  '{{ ti.xcom_pull(task_ids='InputConfig', key='last_working_day', dag_id='LOAD_CMN_Personas-Daily') }}','{{ ti.xcom_pull(task_ids='InputConfig', key='partition_date', dag_id='LOAD_CMN_Personas-Daily') }}')
    group by  a.pedt036_penumper ) p036
 	ON p001.penumper = p036.penumper

LEFT JOIN
(SELECT penumper, CASE WHEN petipreu IS NOT NULL THEN 1 ELSE 0 END flag_universitario,
 substr(petipreu,2,1) cod_universitario,
 peentpre cod_per_siglauniversitaria,
 pecodcar cod_per_caruniversitaria
FROM bi_corp_staging.malpe_pedt223
WHERE partition_date =  '{{ ti.xcom_pull(task_ids='InputConfig', key='partition_date', dag_id='LOAD_CMN_Personas-Daily') }}' ) p223
	ON p001.penumper = p223.penumper

LEFT JOIN
(SELECT penumper, pebancap, pedivisi,
 CASE WHEN pejefare IN ('0005','0065') then 1 else 0 end flag_agro
FROM bi_corp_staging.malpe_pedt040
WHERE partition_date = '{{ ti.xcom_pull(task_ids='InputConfig', key='partition_date', dag_id='LOAD_CMN_Personas-Daily') }}' ) p040
	ON p001.penumper = p040.penumper

LEFT JOIN
(SELECT penumper, pecodafip
FROM bi_corp_staging.malpe_pedt063
WHERE partition_date =  '{{ ti.xcom_pull(task_ids='InputConfig', key='partition_date', dag_id='LOAD_CMN_Personas-Daily') }}' ) p063
	ON p001.penumper = p063.penumper

LEFT JOIN
(SELECT tcdtrac_cod_aco_afip , tcdtrac_cod_aco_csf10
 FROM  bi_corp_staging.maltc_tcdtrac
 where partition_date = IF('{{ ti.xcom_pull(task_ids='InputConfig', key='partition_date', dag_id='LOAD_CMN_Personas-Daily') }}' < '2020-06-16','2020-06-16','{{ ti.xcom_pull(task_ids='InputConfig', key='partition_date', dag_id='LOAD_CMN_Personas-Daily') }}')) tcdtrac
ON tcdtrac.tcdtrac_cod_aco_afip = p063.pecodafip

LEFT JOIN
(select p.penumpar, p.penumgru, p.pedesgru
 FROM
 (select row_number() OVER (partition by p25.penumpar  order by p25.pefecalt desc) as orden,
         p25.penumpar ,
 	 	 p25.penumgru ,
 	     p24.pedesgru
  	  FROM bi_corp_staging.malpe_pedt025 p25
      LEFT JOIN bi_corp_staging.malpe_pedt024 p24 on p24.penumgru = p25.penumgru
      WHERE p25.partition_date=  '{{ ti.xcom_pull(task_ids='InputConfig', key='previous_month_last_working_day', dag_id='LOAD_CMN_Personas-Daily') }}'
 	  AND p24.partition_date='{{ ti.xcom_pull(task_ids='InputConfig', key='previous_month_last_working_day', dag_id='LOAD_CMN_Personas-Daily') }}') p WHERE p.orden=1) p025_024
    ON p001.penumper = p025_024.penumpar


LEFT JOIN(
select trim(gen_clave) gen_clave ,trim(substr(gen_datos,1,50)) descripcion
from bi_corp_staging.tcdtgen
where gentabla = '0116' AND
	partition_Date = IF('{{ ti.xcom_pull(task_ids='InputConfig', key='partition_date', dag_id='LOAD_CMN_Personas-Daily') }}'< '2020-01-08','2020-01-08','{{ ti.xcom_pull(task_ids='InputConfig', key='partition_date', dag_id='LOAD_CMN_Personas-Daily') }}')) tcd_0116
ON TRIM(p001.peestciv)=tcd_0116.gen_clave


LEFT JOIN
(select trim(gen_clave) as peforjur,trim(substr(gen_datos,1,40)) as descripcion
	from bi_corp_staging.tcdtgen
	where gentabla = '0231' and
	partition_Date = IF('{{ ti.xcom_pull(task_ids='InputConfig', key='partition_date', dag_id='LOAD_CMN_Personas-Daily') }}'< '2020-01-08','2020-01-08','{{ ti.xcom_pull(task_ids='InputConfig', key='partition_date', dag_id='LOAD_CMN_Personas-Daily') }}')) tcd_0231
	on p001.peforjur = tcd_0231.peforjur

LEFT JOIN
(	select trim(gen_clave) as petipocu,trim(substr(gen_datos,1,40)) as descripcion
	from bi_corp_staging.tcdtgen
	where gentabla = '0305' and
	partition_Date = IF('{{ ti.xcom_pull(task_ids='InputConfig', key='partition_date', dag_id='LOAD_CMN_Personas-Daily') }}'< '2020-01-08','2020-01-08','{{ ti.xcom_pull(task_ids='InputConfig', key='partition_date', dag_id='LOAD_CMN_Personas-Daily') }}')) tcd_0305
	on p001.petipocu = tcd_0305.petipocu

LEFT JOIN(
select trim(gen_clave) gen_clave ,TRIM(SUBSTRING(gen_datos,1,202)) descripcion
from bi_corp_staging.tcdtgen
where gentabla = '1677' AND
	partition_Date = IF('{{ ti.xcom_pull(task_ids='InputConfig', key='partition_date', dag_id='LOAD_CMN_Personas-Daily') }}'< '2020-01-08','2020-01-08','{{ ti.xcom_pull(task_ids='InputConfig', key='partition_date', dag_id='LOAD_CMN_Personas-Daily') }}')) tcd_1677 --46
ON p063.pecodafip=tcd_1677.gen_clave
LEFT JOIN
(
select trim(substr(gen_clave,2)) gen_clave,TRIM(gen_datos) descripcion
from bi_corp_staging.tcdtgen
where gentabla = '0255' AND
partition_Date = IF('{{ ti.xcom_pull(task_ids='InputConfig', key='partition_date', dag_id='LOAD_CMN_Personas-Daily') }}'< '2020-01-08','2020-01-08','{{ ti.xcom_pull(task_ids='InputConfig', key='partition_date', dag_id='LOAD_CMN_Personas-Daily') }}')) tcd_0255
ON p040.pebancap=tcd_0255.gen_clave

LEFT JOIN (
 select trim(substr(gen_datos,1,40)) descripcion, substr(gen_clave,2,1) codigo
 		from bi_corp_staging.tcdtgen
 		where gentabla = '1154' and
		partition_Date = IF('{{ ti.xcom_pull(task_ids='InputConfig', key='partition_date', dag_id='LOAD_CMN_Personas-Daily') }}'< '2020-01-08','2020-01-08','{{ ti.xcom_pull(task_ids='InputConfig', key='partition_date', dag_id='LOAD_CMN_Personas-Daily') }}')) tcd_1154
 on p223.cod_universitario = tcd_1154.codigo

LEFT JOIN
(select trim(substr(gen_clave,1,1)) as petipdoc, gen_datos as descripcion
	from  bi_corp_staging.tcdtgen
	where gentabla = '0113' and
	partition_Date = IF('{{ ti.xcom_pull(task_ids='InputConfig', key='partition_date', dag_id='LOAD_CMN_Personas-Daily') }}'< '2020-01-08','2020-01-08','{{ ti.xcom_pull(task_ids='InputConfig', key='partition_date', dag_id='LOAD_CMN_Personas-Daily') }}')) tcd_0113
    on p001.petipdoc = tcd_0113.petipdoc

LEFT JOIN (
	select trim(gen_clave) as pecodact, trim(substr(gen_datos,1,40)) as descripcion
	from bi_corp_staging.tcdtgen
	where gentabla = '0304' and
	partition_Date = IF('{{ ti.xcom_pull(task_ids='InputConfig', key='partition_date', dag_id='LOAD_CMN_Personas-Daily') }}'< '2020-01-08','2020-01-08','{{ ti.xcom_pull(task_ids='InputConfig', key='partition_date', dag_id='LOAD_CMN_Personas-Daily') }}')) tcd_0304
ON p001.pecodact = tcd_0304.pecodact

LEFT JOIN (
select trim(gen_clave) gen_clave ,TRIM(SUBSTRING(gen_datos,1,40)) descripcion
from bi_corp_staging.tcdtgen
where gentabla = '0112' and gen_idioma = 'E' and
	partition_Date = IF('{{ ti.xcom_pull(task_ids='InputConfig', key='partition_date', dag_id='LOAD_CMN_Personas-Daily') }}'< '2020-01-08','2020-01-08','{{ ti.xcom_pull(task_ids='InputConfig', key='partition_date', dag_id='LOAD_CMN_Personas-Daily') }}')) tcd_0112
ON p001.penacper = tcd_0112.gen_clave

LEFT JOIN (
select trim(gen_clave) gen_clave ,trim(SUBSTRING(gen_datos,1,50)) ds_per_siglauniversitaria
, substr(trim(gen_datos),-4) ds_per_siglauniversitaria_agrup
from bi_corp_staging.tcdtgen
where gentabla = '0351' AND
	partition_Date = IF('{{ ti.xcom_pull(task_ids='InputConfig', key='partition_date', dag_id='LOAD_CMN_Personas-Daily') }}'< '2020-01-08','2020-01-08','{{ ti.xcom_pull(task_ids='InputConfig', key='partition_date', dag_id='LOAD_CMN_Personas-Daily') }}')) tcd_0351
ON p223.cod_per_siglauniversitaria = tcd_0351.gen_clave

LEFT JOIN (
select trim(gen_clave) gen_clave ,trim(SUBSTRING(gen_datos,1,30)) descripcion
from bi_corp_staging.tcdtgen
where gentabla = '0508' AND
	partition_Date = IF('{{ ti.xcom_pull(task_ids='InputConfig', key='partition_date', dag_id='LOAD_CMN_Personas-Daily') }}'< '2020-01-08','2020-01-08','{{ ti.xcom_pull(task_ids='InputConfig', key='partition_date', dag_id='LOAD_CMN_Personas-Daily') }}')) tcd_0508
ON p223.cod_per_caruniversitaria = tcd_0508.gen_clave


LEFT JOIN
(select gen_clave as pevalind,substr(gen_datos,13,2) cod_mipyme, trim(substr(gen_clave, 4,1)) cod_mipyme_letra,
	CASE
		WHEN substr(gen_Clave, 1, 3) = 'MIP' AND substr(gen_datos, 13, 2) = '41' THEN "Microempresa – Agropecuario (41)"
		WHEN substr(gen_Clave, 1, 3) = 'MIP' AND substr(gen_datos, 13, 2) = '42' THEN "Microempresa – Industria y Minería (42)"
		WHEN substr(gen_Clave, 1, 3) = 'MIP' AND substr(gen_datos, 13, 2) = '43' THEN "Microempresa – Comercio (43)"
		WHEN substr(gen_Clave, 1, 3) = 'MIP' AND substr(gen_datos, 13, 2) = '44' THEN "Microempresa – Servicios (44)"
		WHEN substr(gen_Clave, 1, 3) = 'MIP' AND substr(gen_datos, 13, 2) = '45' THEN "Microempresa – Construccion (45)"
		WHEN substr(gen_Clave, 1, 3) = 'MIP' AND substr(gen_datos, 13, 2) = '51' THEN "Pequeña Empresa – Agropecuario (51)"
		WHEN substr(gen_Clave, 1, 3) = 'MIP' AND substr(gen_datos, 13, 2) = '52' THEN "Pequeña Empresa – Industria y Minería (52)"
		WHEN substr(gen_Clave, 1, 3) = 'MIP' AND substr(gen_datos, 13, 2) = '53' THEN "Pequeña Empresa – Comercio (53)"
		WHEN substr(gen_Clave, 1, 3) = 'MIP' AND substr(gen_datos, 13, 2) = '54' THEN "Pequeña Empresa – Servicios (54)"
		WHEN substr(gen_Clave, 1, 3) = 'MIP' AND substr(gen_datos, 13, 2) = '55' THEN "Pequeña Empresa – Construccion (55)"
		WHEN substr(gen_Clave, 1, 3) = 'MIP' AND substr(gen_datos, 13, 2) = '61' THEN "Mediana Empresa tramo 1 – Agropecuario (61)"
		WHEN substr(gen_Clave, 1, 3) = 'MIP' AND substr(gen_datos, 13, 2) = '62' THEN "Mediana Empresa tramo 1 – Industria y Minería (62)"
		WHEN substr(gen_Clave, 1, 3) = 'MIP' AND substr(gen_datos, 13, 2) = '63' THEN "Mediana Empresa tramo 1 – Comercio (63)"
		WHEN substr(gen_Clave, 1, 3) = 'MIP' AND substr(gen_datos, 13, 2) = '64' THEN "Mediana Empresa tramo 1 – Servicios (64)"
		WHEN substr(gen_Clave, 1, 3) = 'MIP' AND substr(gen_datos, 13, 2) = '65' THEN "Mediana Empresa tramo 1 – Construccion (65)"
		WHEN substr(gen_Clave, 1, 3) = 'MIP' AND substr(gen_datos, 13, 2) = '71' THEN "Mediana Empresa tramo 2 – Agropecuario (71)"
		WHEN substr(gen_Clave, 1, 3) = 'MIP' AND substr(gen_datos, 13, 2) = '72' THEN "Mediana Empresa tramo 2 – Industria y Minería (72)"
		WHEN substr(gen_Clave, 1, 3) = 'MIP' AND substr(gen_datos, 13, 2) = '73' THEN "Mediana Empresa tramo 2 – Comercio (73)"
		WHEN substr(gen_Clave, 1, 3) = 'MIP' AND substr(gen_datos, 13, 2) = '74' THEN "Mediana Empresa tramo 2 – Servicios (74)"
		WHEN substr(gen_Clave, 1, 3) = 'MIP' AND substr(gen_datos, 13, 2) = '75' THEN "Mediana Empresa tramo 2 – Construccion (75)"
		WHEN substr(gen_Clave, 1, 3) = 'MIP' AND substr(gen_datos, 13, 2) = '8'  THEN "MIPyME ingresan por incluidos"
		WHEN substr(gen_Clave, 1, 3) = 'MIP' AND substr(gen_datos, 13, 2) = '9'  THEN "MIPyME ingresan por padron SEPyME"
		ELSE NULL
	END ds_mipyme
from bi_corp_staging.tcdtgen
where gentabla = '0680' and gen_clave like 'MIP%' and
partition_Date = IF('{{ ti.xcom_pull(task_ids='InputConfig', key='partition_date', dag_id='LOAD_CMN_Personas-Daily') }}'< '2020-01-08','2020-01-08','{{ ti.xcom_pull(task_ids='InputConfig', key='partition_date', dag_id='LOAD_CMN_Personas-Daily') }}')) tcd_0680_mip
on p052.cod_mipyme=tcd_0680_mip.cod_mipyme_letra



left join(
		select alias_nup nup, shortname cod_kgr
		from bi_corp_staging.mdr_contrapartes
		where partition_date= IF('{{ ti.xcom_pull(task_ids='InputConfig', key='partition_date', dag_id='LOAD_CMN_Personas-Daily') }}' < '2020-04-03','2020-04-03','{{ ti.xcom_pull(task_ids='InputConfig', key='partition_date', dag_id='LOAD_CMN_Personas-Daily') }}' )) contra
	on p001.penumper= contra.nup


left join
(select me.dt_test_inversor_ult_fecha dt_test_inversor_ult_fecha ,
                 me.cod_test_inversor cod_test_inversor,
                 me.ds_test_inversor ds_test_inversor,
                 me.cod_per_nup cod_per_nup,orden
   from (select  row_number() OVER (partition by core.nup_cliente order by from_unixtime(unix_timestamp(core.fecha_test,'dd/MM/yyyy'), 'yyyy-MM-dd') desc) as orden,
         from_unixtime(unix_timestamp(core.fecha_test,'dd/MM/yyyy'), 'yyyy-MM-dd') as dt_test_inversor_ult_fecha,
         core.id_perfil as cod_test_inversor,
         cat.desc_perfil as ds_test_inversor,
         core.nup_cliente as cod_per_nup
         from bi_corp_staging.mesac_test_inversor_usuario core
         left join bi_corp_staging.MESAC_PERFILES_INVERSOR cat on cat.cod_perfil = core.id_perfil
        where cat.partition_date  = '{{ ti.xcom_pull(task_ids='InputConfig', key='partition_date', dag_id='LOAD_CMN_Personas-Daily') }}'
        and core.partition_date <= '{{ ti.xcom_pull(task_ids='InputConfig', key='partition_date', dag_id='LOAD_CMN_Personas-Daily') }}') me
        where me.orden = 1) mesac
       ON mesac.cod_per_nup= p001.penumper

LEFT JOIN(
SELECT waguxdex_num_persona nup, max(waguxdex_con_diaimpag) max_dias_atraso, sum (waguxdex_imp_irremolo) monto_total_impago , sum (productos_atrasados) productos_atrasaros
FROM (select waguxdex_con_diaimpag,
             waguxdex_imp_irremolo,
             waguxdex_num_persona ,
             case when waguxdex_con_diaimpag > 0 then 1 else 0 end productos_atrasados
       FROM bi_corp_staging.garra_wagucdex
       WHERE partition_date='{{ ti.xcom_pull(task_ids='InputConfig', key='partition_date', dag_id='LOAD_CMN_Personas-Daily') }}') g group by g.waguxdex_num_persona) garra
        ON garra.nup= p001.penumper


LEFT JOIN
(select distinct isec_nup, isec_cod_cla_efd situacion_bcra
 from  bi_corp_staging.contratos_adsf_cf
 WHERE partition_date = '{{ ti.xcom_pull(task_ids='InputConfig', key='first_working_day', dag_id='LOAD_CMN_Personas-Daily') }}' ) adsf
 ON p001.penumper = adsf.isec_nup

 LEFT JOIN (
 select
 nup,
 CASE WHEN TRIM(cod_rechazo)='' THEN NULL ELSE cod_rechazo END cod_rechazo,
 CASE WHEN TRIM(mot_rechazo)='' THEN NULL ELSE mot_rechazo END mot_rechazo,
 cast(linea_pes as decimal(15,2))/100 linea_pes,
 cast(linea_disp_pes as decimal(15,2))/100 linea_disp_pes,
 cast(cuota_pes as decimal(15,2))/100 cuota_pes
from bi_corp_staging.acal_ptmo_preacordado
where  partition_Date='{{ ti.xcom_pull(task_ids='InputConfig', key='partition_date', dag_id='LOAD_CMN_Personas-Daily') }}'
and '{{ ti.xcom_pull(task_ids='InputConfig', key='partition_date', dag_id='LOAD_CMN_Personas-Daily') }}'>='2021-04-08'

union all

select penumper as nup,
codigo_rechazo as cod_rechazo,
motivo_rechazo as mot_rechazo,
CAST(linea_pesos AS DECIMAL(15,4)) as linea_pes ,
CAST(pres_pesos_disponible AS DECIMAL(15,4)) as linea_disp_pes,
CAST(cuota_pesos AS DECIMAL(15,4)) as cuota_pes
from bi_corp_staging.rio35_ries_motor_infinity
where  partition_Date='{{ ti.xcom_pull(task_ids='InputConfig', key='partition_date', dag_id='LOAD_CMN_Personas-Daily') }}'
and '{{ ti.xcom_pull(task_ids='InputConfig', key='partition_date', dag_id='LOAD_CMN_Personas-Daily') }}' <'2021-04-08'

) pre
ON p001.penumper = pre.nup

LEFT JOIN (
select nup as penumper,
1 as  sorpresa_suscripto,
CASE WHEN cascada_yn= 'N' THEN 1 ELSE 0 END sorpresa_suscriptor
from bi_corp_staging.rio102_sorpresa_cascada
where partition_Date= '{{ ti.xcom_pull(task_ids='InputConfig', key='partition_date', dag_id='LOAD_CMN_Personas-Daily') }}'
and '{{ ti.xcom_pull(task_ids='InputConfig', key='partition_date', dag_id='LOAD_CMN_Personas-Daily') }}' >= '2021-03-14'
and fecha_fin is null

union all

select penumper, cast(sorpresa_suscripto as int), cast(sorpresa_suscriptor as int)
from bi_corp_staging.rio35_tmp2_sorpresa_cascada_hist
where partition_Date= '{{ ti.xcom_pull(task_ids='InputConfig', key='partition_date', dag_id='LOAD_CMN_Personas-Daily') }}'
and '{{ ti.xcom_pull(task_ids='InputConfig', key='partition_date', dag_id='LOAD_CMN_Personas-Daily') }}' < '2021-03-14' ) cas
on p001.penumper=cas.penumper

LEFT JOIN (
   select
   penumper,
   CAST(nova_titular AS INT)    AS nova_titular,
   CAST(nova_usuario AS INT)    AS nova_usuario
   from bi_corp_staging.rio35_tmp_nova_hist
   where partition_Date = '{{ ti.xcom_pull(task_ids='InputConfig', key='partition_date', dag_id='LOAD_CMN_Personas') }}') nova
 ON p001.penumper=nova.penumper

LEFT JOIN tablat p015t on p001.penumper = p015t.penumper
LEFT JOIN tablal p015l on p001.penumper = p015l.penumper
LEFT JOIN tablad p015d on p001.penumper = p015d.penumper
