set hive.merge.mapfiles=true;
set hive.merge.mapredfiles=true;
set hive.merge.size.per.task=4000000;
set hive.merge.smallfiles.avgsize=16000000;
set hive.exec.dynamic.partition.mode=nonstrict;
set mapred.job.queue.name=root.dataeng;

-- Calculamos los reversos
DROP TABLE IF EXISTS abae_wabaetlx_reversos;
CREATE TEMPORARY TABLE abae_wabaetlx_reversos AS
select
		tlx_semcacct,
		tlx_semtrdat,
		tlx_semtrtim,
		tlx_semtrenu,
		tlx_semtatmi
from bi_corp_staging.abae_wabaetlx
where partition_date='{{ ti.xcom_pull(task_ids='InputConfig', key='partition_date', dag_id='LOAD_CMN_Cuentas_Abae') }}'
	  AND tlx_semsgtyp=400;



DROP TABLE IF EXISTS abae_wabaetlfcre_reversos;
CREATE TEMPORARY TABLE abae_wabaetlfcre_reversos AS
select
		tlf_semcacct,
		tlf_semtrdat,
		tlf_semtrtim,
		tlf_semtrenu,
		tlf_semtatmi
from bi_corp_staging.abae_wabaetlf_cre
where partition_date='{{ ti.xcom_pull(task_ids='InputConfig', key='partition_date', dag_id='LOAD_CMN_Cuentas_Abae') }}'
	  AND tlf_semsgtyp=400;



DROP TABLE IF EXISTS abae_wabaetlf_reversos;
CREATE TEMPORARY TABLE abae_wabaetlf_reversos AS
select
		tlf_semcacct,
		tlf_semtrdat,
		tlf_semtrtim,
		tlf_semtrenu,
		tlf_semtatmi
from bi_corp_staging.abae_wabaetlf
where partition_date='{{ ti.xcom_pull(task_ids='InputConfig', key='partition_date', dag_id='LOAD_CMN_Cuentas_Abae') }}'
	  AND tlf_semsgtyp=400;

-- Descripcion del codigo pais
DROP TABLE IF EXISTS pais;
CREATE TEMPORARY TABLE pais AS
SELECT *
FROM
     (select
		case when LENGTH(trim(substring(gen_Datos,47,2)))=0 then null else trim(substring(gen_Datos,47,2)) end as  cod_cue_pais,
		trim(substring(gen_datos,1,40)) as ds_cue_pais
from bi_corp_staging.tcdtgen
where partition_date='{{ ti.xcom_pull(task_ids='InputConfig', key='max_partition_tcdtgen', dag_id='LOAD_CMN_Cuentas_Abae') }}'
	  and genTabla = '0112'
group by trim(substring(gen_Datos,47,2)) ,
		 trim(substring(gen_datos,1,40))
	  ) A
WHERE cod_cue_pais is not null
;


--Insertamos en la tabla final los movimientos sin reversos
insert overwrite table bi_corp_common.bt_cue_movimientos_abae
partition(partition_date)
SELECT
	 wabaetlx.tlx_stat AS cod_cue_estado
	,CASE WHEN wabaetlx.tlx_stat=0 THEN 'OK'
		  WHEN wabaetlx.tlx_stat=11 THEN 'Destino no Disponible'
		  WHEN wabaetlx.tlx_stat=12 THEN 'Línea no Disponible'
		  WHEN wabaetlx.tlx_stat=20 THEN 'Originador no Disponible'
		  WHEN wabaetlx.tlx_stat=21 THEN 'Tipo de Mensaje Desconocido'
		  WHEN wabaetlx.tlx_stat=22 THEN 'Número de Tarjeta Desconocido'
		  ELSE NULL
	 END AS ds_cue_estado
	,CASE WHEN trim(wabaetlx.tlx_1er_uso)='P' THEN 1 ELSE 0 end   flag_cue_primer_uso
	,wabaetlx.tlx_semsgtyp AS cod_cue_tipo_mensaje
	,CASE WHEN wabaetlx.tlx_semsgtyp=0210 THEN 'Respuesta de Autorización (Aprobación o Denegación)'
	      WHEN wabaetlx.tlx_semsgtyp=0215 THEN 'Transacción de Consulta de Últimos Movimientos, de Consulta de Últimos Aportes y de Consulta de Servicios por Vencer.'
	      WHEN wabaetlx.tlx_semsgtyp=0220 THEN 'Aviso de Autorización de Transacción'
	      WHEN wabaetlx.tlx_semsgtyp=0400 THEN 'Reverso Contable de Operación Aprobada'
	      WHEN wabaetlx.tlx_semsgtyp=0420 THEN 'Aviso de Reverso de Transacción'
	      ELSE NULL
	 END ds_cue_tipo_mensaje
	,CASE WHEN LENGTH(trim(wabaetlx.tlx_semcacct))=0 THEN NULL ELSE trim(wabaetlx.tlx_semcacct) END AS cod_cue_tarjeta
	,wabaetlx.tlx_semtcode AS cod_cue_tipo_movimiento
	,CASE WHEN wabaetlx.tlx_semtcode=15 THEN 'Compra (POS)'
	      WHEN wabaetlx.tlx_semtcode=17 THEN 'Devolución descuento crédito (POS)'
	      WHEN wabaetlx.tlx_semtcode=18 THEN 'Devolución de Compra (POS)'
	      WHEN wabaetlx.tlx_semtcode=19 THEN 'Devolución descuento débito (POS)'
	      WHEN wabaetlx.tlx_semtcode=22 THEN 'Anulación de devolución de compra (POS)'
	      WHEN wabaetlx.tlx_semtcode=23 THEN 'Anulación de compra (POS)'
	      WHEN wabaetlx.tlx_semtcode=24 THEN 'Anulación de compra con cashback (POS)'
	      WHEN wabaetlx.tlx_semtcode=26 THEN 'Compra con cashback (POS)'
	      WHEN wabaetlx.tlx_semtcode=35 THEN 'Consulta de disponible de compra (POS)'
		  ELSE NULL
	 END  AS ds_cue_tipo_movimiento
	,wabaetlx.tlx_semtfacc AS  cod_cue_tipo_cuenta_desde
	,CASE WHEN wabaetlx.tlx_semtfacc=1 THEN 'Cuenta corriente en pesos'
		  WHEN wabaetlx.tlx_semtfacc=2 THEN 'Cuenta corriente en dólares'
		  WHEN wabaetlx.tlx_semtfacc=11 THEN 'Caja de ahorros en pesos'
		  WHEN wabaetlx.tlx_semtfacc=12 THEN 'Caja de ahorros en dólares'
		  WHEN wabaetlx.tlx_semtfacc=13 THEN 'Cuenta especial'
		  WHEN wabaetlx.tlx_semtfacc=20 THEN 'Cuenta de CBU'
		  WHEN wabaetlx.tlx_semtfacc=31 THEN 'Cuenta de crédito en pesos'
		  WHEN wabaetlx.tlx_semtfacc=32 THEN 'Cuenta de crédito en dólares'
		  ELSE NULL
	 END AS  ds_cue_tipo_cuenta_desde
	,wabaetlx.tlx_semttacc AS  cod_cue_tipo_cuenta_hasta
	,CASE WHEN wabaetlx.tlx_semttacc=1 THEN 'Cuenta corriente en pesos'
		  WHEN wabaetlx.tlx_semttacc=2 THEN 'Cuenta corriente en dólares'
		  WHEN wabaetlx.tlx_semttacc=11 THEN 'Caja de ahorros en pesos'
		  WHEN wabaetlx.tlx_semttacc=12 THEN 'Caja de ahorros en dólares'
		  WHEN wabaetlx.tlx_semttacc=13 THEN 'Cuenta especial'
		  WHEN wabaetlx.tlx_semttacc=20 THEN 'Cuenta de CBU'
		  WHEN wabaetlx.tlx_semttacc=31 THEN 'Cuenta de crédito en pesos'
		  WHEN wabaetlx.tlx_semttacc=32 THEN 'Cuenta de crédito en dólares'
		  ELSE NULL
	 END AS  ds_cue_tipo_cuenta_hasta
	,wabaetlx.tlx_semamt1 AS fc_cue_importe_cuenta
	,wabaetlx.tlx_semamt2 AS fc_cue_importe_impuesto
	,CASE when LENGTH(wabaetlx.tlx_semamt3)=0 THEN NULL ELSE wabaetlx.tlx_semamt3 end  AS fc_cue_importe_compra_usd
	,CASE WHEN LENGTH(trim(wabaetlx.tlx_fiid))=0 OR trim(wabaetlx.tlx_fiid)='0000' THEN NULL ELSE trim(wabaetlx.tlx_fiid) END AS cod_cue_comercio_camp
	,CASE WHEN wabaetlx.tlx_auth_code=0 THEN NULL ELSE wabaetlx.tlx_auth_code END AS cod_cue_autorizacion
	,CASE WHEN LENGTH(trim(wabaetlx.tlx_sys_id))=0 THEN NULL ELSE trim(wabaetlx.tlx_sys_id) END AS cod_cue_tipo_tarjeta_banelco
	,CASE WHEN LENGTH(trim(wabaetlx.tlx_sys_id))=0 OR trim(wabaetlx.tlx_fiid) IS NULL THEN 0 ELSE 1 END AS flag_cue_tarjeta_banelco
	,CASE WHEN wabaetlx.tlx_trace_no=0 THEN NULL ELSE wabaetlx.tlx_trace_no END AS cod_cue_trace_host
    ,concat(
			  CASE
	          WHEN LENGTH(CAST(wabaetlx.tlx_semtrdat AS string)) = 3 THEN from_unixtime(unix_timestamp(concat(substring(partition_date, 1, 4),'0',CAST(wabaetlx.tlx_semtrdat AS string)), 'yyyyMMdd'),'yyyy-MM-dd')
	          WHEN length(CAST(wabaetlx.tlx_semtrdat AS string)) = 4 THEN from_unixtime(unix_timestamp(concat(substring(partition_date, 1, 4),CAST(wabaetlx.tlx_semtrdat AS string)), 'yyyyMMdd'),'yyyy-MM-dd')
	          ELSE NULL
	          END,' ',substring(cast(wabaetlx.tlx_semtrtim as string),1,2),':',substring(cast(wabaetlx.tlx_semtrtim as string),3,2),':',substring(cast(wabaetlx.tlx_semtrtim as string),5,2)
             ) AS ts_cue_transaccion
	,CASE
          WHEN LENGTH(CAST(wabaetlx.tlx_sempdat AS string)) = 3 THEN from_unixtime(unix_timestamp(concat(substring(partition_date, 1, 4),'0',CAST(wabaetlx.tlx_sempdat AS string)), 'yyyyMMdd'),'yyyy-MM-dd')
          WHEN length(CAST(wabaetlx.tlx_sempdat AS string)) = 4 THEN from_unixtime(unix_timestamp(concat(substring(partition_date, 1, 4),CAST(wabaetlx.tlx_sempdat AS string)), 'yyyyMMdd'),'yyyy-MM-dd')
          ELSE NULL
          END  AS dt_cue_fecha_banco
     ,CASE WHEN LENGTH(trim(wabaetlx.tlx_semcod_term))=0 THEN NULL ELSE trim(wabaetlx.tlx_semcod_term) END AS cod_cue_terminal
     ,CASE WHEN LENGTH(trim(wabaetlx.tlx_semcod_tarj))=0 THEN NULL ELSE trim(wabaetlx.tlx_semcod_tarj) END AS id_cue_entidad_tarjeta
     ,CASE WHEN LENGTH(trim(wabaetlx.tlx_semccode))=0 THEN NULL ELSE trim(wabaetlx.tlx_semccode) END AS cod_cue_moneda
	,CASE WHEN trim(wabaetlx.tlx_semccode)='032' THEN 'Pesos'
		  WHEN trim(wabaetlx.tlx_semccode)='840' THEN 'Dólares'
  	   	  WHEN trim(wabaetlx.tlx_semccode)='858' THEN 'Pesos Uruguayos'
	 ELSE NULL
	 END AS ds_cue_moneda
	,wabaetlx.tlx_semrrev AS cod_cue_reverso
	,CASE WHEN wabaetlx.tlx_semrrev=1 THEN 'Tiempo para completar la autorización excedida'
		  WHEN wabaetlx.tlx_semrrev=2 THEN 'Respuesta rechazada por la terminal'
		  WHEN wabaetlx.tlx_semrrev=3 THEN 'Destino (terminal) no disponible'
		  WHEN wabaetlx.tlx_semrrev=8 THEN 'Cancelación del cliente'
		  WHEN wabaetlx.tlx_semrrev=10 THEN 'Error de hardware. Incluye a los reversos parciales'
		  WHEN wabaetlx.tlx_semrrev=20 THEN 'Transacción sospechosa. No se recibió la confirmación'
	ELSE NULL END ds_cue_reverso
    ,CASE WHEN LENGTH(trim(wabaetlx.tlx_id_canal))=0 THEN NULL ELSE trim(wabaetlx.tlx_id_canal) END AS cod_cue_canal
	,NULL AS ds_cue_canal
	,CASE WHEN trim(wabaetlx.tlx_marca_iva)='1' THEN '1' ELSE 0 end AS flag_cue_consumidor_final
    ,CASE WHEN LENGTH(trim(wabaetlx.tlx_semrcard))=0 THEN NULL ELSE trim(wabaetlx.tlx_semrcard) END AS cod_cue_semrcard
    ,CASE WHEN LENGTH(trim(wabaetlx.tlx_semtrnad))=0 THEN NULL ELSE trim(wabaetlx.tlx_semtrnad) END AS cod_cue_semtrnad
	,CASE WHEN LENGTH(trim(wabaetlx.tlx_semcity))=0 THEN NULL ELSE trim(wabaetlx.tlx_semcity) END ds_cue_terminal_ciudad
	,CAST(substring(wabaetlx.tlx_semfanum,0,3) AS int) AS cod_suc_sucursal_desde
	,CAST(substring(wabaetlx.tlx_semfanum,3,12) AS bigint) AS cod_cue_cuenta_desde
	,CAST(substring(wabaetlx.tlx_semtanum,0,3) AS int) AS cod_suc_sucursal_hasta
	,CAST(substring(wabaetlx.tlx_semtanum,3,12) AS bigint) AS cod_suc_cuenta_hasta
	,CASE WHEN trim(wabaetlx.tlx_semterm_country)='00' OR LENGTH(trim(wabaetlx.tlx_semterm_country))=0  THEN NULL ELSE trim(wabaetlx.tlx_semterm_country) END AS cod_cue_terminal_pais
	,pais.ds_cue_pais AS ds_cue_terminal_pais
	,CASE WHEN LENGTH(trim(wabaetlx.tlx_sembanc))=0 THEN NULL ELSE trim(wabaetlx.tlx_sembanc) END cod_cue_tarjeta_banco
	,CASE WHEN LENGTH(trim(wabaetlx.tlx_semfiid))=0 THEN NULL ELSE trim(wabaetlx.tlx_semfiid) END cod_cue_terminal_banco
	,NULL AS cod_cbu_destino
	,trim(wabaetlx.tlx_semtnloc) AS ds_cue_establecimiento
	,CASE WHEN LENGTH(trim(wabaetlx.tlx_semtcomer))=0 THEN NULL ELSE trim(wabaetlx.tlx_semtcomer) END cod_cue_comercio
	,CASE WHEN LENGTH(trim(wabaetlx.tlx_semtatmi))=0 THEN NULL ELSE trim(wabaetlx.tlx_semtatmi) END cod_cue_atm
	,wabaetlx.tlx_semtrenu AS cod_cue_comprobante
	,trim(wabaetlx.tlx_nombre_origen) AS ds_cue_origen
	,CASE WHEN LENGTH(trim(wabaetlx.tlx_tip_debin))=0 THEN NULL ELSE trim(wabaetlx.tlx_tip_debin) END AS cod_cue_debin_tipo
	,trim(wabaetlx.tlx_moneda_origen) AS cod_cue_moneda_origen
	,CASE WHEN trim(wabaetlx.tlx_moneda_origen)='032' THEN 'Pesos'
		  WHEN trim(wabaetlx.tlx_moneda_origen)='840' THEN 'Dólares'
  	   	  WHEN trim(wabaetlx.tlx_moneda_origen)='858' THEN 'Pesos Uruguayos'
	 ELSE NULL
	 END AS ds_cue_moneda_origen
	,wabaetlx.tlx_semimpo_original AS fc_cue_importe_origen
	,trim(wabaetlx.tlx_semnro_cargo) AS cod_cue_cupon
	,trim(wabaetlx.tlx_porc_devl_clte) AS cod_cue_porc_devl_clte
	,trim(wabaetlx.tlx_porc_devl_comer) AS cod_cue_porc_devl_comer
	,trim(wabaetlx.tlx_rubro) AS cod_cue_rubro
	,NULL AS cod_cue_debin
	,NULL AS cod_cue_cuit_bco_cdor
	,NULL AS cod_cue_cbu_bco_cdor
	,NULL AS cod_cue_cuit_bco_vdor
	,NULL AS cod_cue_cbu_bco_vdor
	,NULL AS dt_cue_neg_coel
	,NULL AS cod_cue_debin_scoring
	,NULL AS cod_cue_debin_cpto
	,NULL AS ds_cue_cpto_deb
	,NULL AS cod_cue_corresp_titu
	,partition_date
FROM bi_corp_staging.abae_wabaetlx wabaetlx
LEFT JOIN abae_wabaetlx_reversos wabaetlx_reversos
ON 	wabaetlx.tlx_semcacct=wabaetlx_reversos.tlx_semcacct
AND	wabaetlx.tlx_semtrdat=wabaetlx_reversos.tlx_semtrdat
AND	wabaetlx.tlx_semtrtim=wabaetlx_reversos.tlx_semtrtim
AND	wabaetlx.tlx_semtrenu=wabaetlx_reversos.tlx_semtrenu
AND	wabaetlx.tlx_semtatmi=wabaetlx_reversos.tlx_semtatmi
LEFT JOIN pais pais
on trim(wabaetlx.tlx_semterm_country)=pais.cod_cue_pais
WHERE partition_date='{{ ti.xcom_pull(task_ids='InputConfig', key='partition_date', dag_id='LOAD_CMN_Cuentas_Abae') }}'
	  and wabaetlx_reversos.tlx_semtrenu is null -- Saco los reversos

UNION ALL

SELECT
	 wabaetlf.tlf_stat AS cod_cue_estado
	,CASE WHEN wabaetlf.tlf_stat=0 THEN 'OK'
		  WHEN wabaetlf.tlf_stat=11 THEN 'Destino no Disponible'
		  WHEN wabaetlf.tlf_stat=12 THEN 'Línea no Disponible'
		  WHEN wabaetlf.tlf_stat=20 THEN 'Originador no Disponible'
		  WHEN wabaetlf.tlf_stat=21 THEN 'Tipo de Mensaje Desconocido'
		  WHEN wabaetlf.tlf_stat=22 THEN 'Número de Tarjeta Desconocido'
		  ELSE NULL
	 END AS ds_cue_estado
	,CASE WHEN trim(wabaetlf.tlf_1er_uso)='P' THEN 1 ELSE 0 end   flag_cue_primer_uso
	,wabaetlf.tlf_semsgtyp AS cod_cue_tipo_mensaje
	,CASE WHEN wabaetlf.tlf_semsgtyp=0210 THEN 'Respuesta de Autorización (Aprobación o Denegación)'
	      WHEN wabaetlf.tlf_semsgtyp=0215 THEN 'Transacción de Consulta de Últimos Movimientos, de Consulta de Últimos Aportes y de Consulta de Servicios por Vencer.'
	      WHEN wabaetlf.tlf_semsgtyp=0220 THEN 'Aviso de Autorización de Transacción'
	      WHEN wabaetlf.tlf_semsgtyp=0400 THEN 'Reverso Contable de Operación Aprobada'
	      WHEN wabaetlf.tlf_semsgtyp=0420 THEN 'Aviso de Reverso de Transacción'
	      ELSE NULL
	 END ds_cue_tipo_mensaje
	,CASE WHEN LENGTH(trim(wabaetlf.tlf_semcacct))=0 THEN NULL ELSE trim(wabaetlf.tlf_semcacct) END AS cod_cue_tarjeta
	,wabaetlf.tlf_semtcode AS cod_cue_tipo_movimiento
	,CASE WHEN wabaetlf.tlf_semtcode=15 THEN 'Compra (POS)'
	      WHEN wabaetlf.tlf_semtcode=17 THEN 'Devolución descuento crédito (POS)'
	      WHEN wabaetlf.tlf_semtcode=18 THEN 'Devolución de Compra (POS)'
	      WHEN wabaetlf.tlf_semtcode=19 THEN 'Devolución descuento débito (POS)'
	      WHEN wabaetlf.tlf_semtcode=22 THEN 'Anulación de devolución de compra (POS)'
	      WHEN wabaetlf.tlf_semtcode=23 THEN 'Anulación de compra (POS)'
	      WHEN wabaetlf.tlf_semtcode=24 THEN 'Anulación de compra con cashback (POS)'
	      WHEN wabaetlf.tlf_semtcode=26 THEN 'Compra con cashback (POS)'
	      WHEN wabaetlf.tlf_semtcode=35 THEN 'Consulta de disponible de compra (POS)'
		  ELSE NULL
	 END  AS ds_cue_tipo_movimiento
	,wabaetlf.tlf_semtfacc AS  cod_cue_tipo_cuenta_desde
	,CASE WHEN wabaetlf.tlf_semtfacc=1 THEN 'Cuenta corriente en pesos'
		  WHEN wabaetlf.tlf_semtfacc=2 THEN 'Cuenta corriente en dólares'
		  WHEN wabaetlf.tlf_semtfacc=11 THEN 'Caja de ahorros en pesos'
		  WHEN wabaetlf.tlf_semtfacc=12 THEN 'Caja de ahorros en dólares'
		  WHEN wabaetlf.tlf_semtfacc=13 THEN 'Cuenta especial'
		  WHEN wabaetlf.tlf_semtfacc=20 THEN 'Cuenta de CBU'
		  WHEN wabaetlf.tlf_semtfacc=31 THEN 'Cuenta de crédito en pesos'
		  WHEN wabaetlf.tlf_semtfacc=32 THEN 'Cuenta de crédito en dólares'
		  ELSE NULL
	 END AS  ds_cue_tipo_cuenta_desde
	,wabaetlf.tlf_semttacc AS  cod_cue_tipo_cuenta_hasta
	,CASE WHEN wabaetlf.tlf_semttacc=1 THEN 'Cuenta corriente en pesos'
		  WHEN wabaetlf.tlf_semttacc=2 THEN 'Cuenta corriente en dólares'
		  WHEN wabaetlf.tlf_semttacc=11 THEN 'Caja de ahorros en pesos'
		  WHEN wabaetlf.tlf_semttacc=12 THEN 'Caja de ahorros en dólares'
		  WHEN wabaetlf.tlf_semttacc=13 THEN 'Cuenta especial'
		  WHEN wabaetlf.tlf_semttacc=20 THEN 'Cuenta de CBU'
		  WHEN wabaetlf.tlf_semttacc=31 THEN 'Cuenta de crédito en pesos'
		  WHEN wabaetlf.tlf_semttacc=32 THEN 'Cuenta de crédito en dólares'
		  ELSE NULL
	 END AS  ds_cue_tipo_cuenta_hasta
	,wabaetlf.tlf_semamt1 AS fc_cue_importe_cuenta
	,wabaetlf.tlf_semamt2 AS fc_cue_importe_impuesto
	,CASE when LENGTH(wabaetlf.tlf_semamt3)=0 THEN NULL ELSE wabaetlf.tlf_semamt3 end  AS fc_cue_importe_compra_usd
	,CASE WHEN LENGTH(trim(wabaetlf.tlf_fiid))=0 OR trim(wabaetlf.tlf_fiid)='0000' THEN NULL ELSE trim(wabaetlf.tlf_fiid) END AS cod_cue_comercio_camp
	,CASE WHEN wabaetlf.tlf_auth_code=0 THEN NULL ELSE wabaetlf.tlf_auth_code END AS cod_cue_autorizacion
	,CASE WHEN LENGTH(trim(wabaetlf.tlf_sys_id))=0 THEN NULL ELSE trim(wabaetlf.tlf_sys_id) END AS cod_cue_tipo_tarjeta_banelco
	,CASE WHEN LENGTH(trim(wabaetlf.tlf_sys_id))=0 OR trim(wabaetlf.tlf_fiid) IS NULL THEN 0 ELSE 1 END AS flag_cue_tarjeta_banelco
	,CASE WHEN wabaetlf.tlf_trace_no=0 THEN NULL ELSE wabaetlf.tlf_trace_no END AS cod_cue_trace_host
    ,concat(
			  CASE
	          WHEN LENGTH(CAST(wabaetlf.tlf_semtrdat AS string)) = 3 THEN from_unixtime(unix_timestamp(concat(substring(partition_date, 1, 4),'0',CAST(wabaetlf.tlf_semtrdat AS string)), 'yyyyMMdd'),'yyyy-MM-dd')
	          WHEN length(CAST(wabaetlf.tlf_semtrdat AS string)) = 4 THEN from_unixtime(unix_timestamp(concat(substring(partition_date, 1, 4),CAST(wabaetlf.tlf_semtrdat AS string)), 'yyyyMMdd'),'yyyy-MM-dd')
	          ELSE NULL
	          END,' ',substring(cast(wabaetlf.tlf_semtrtim as string),1,2),':',substring(cast(wabaetlf.tlf_semtrtim as string),3,2),':',substring(cast(wabaetlf.tlf_semtrtim as string),5,2)
             ) AS  ts_cue_transaccion
	,CASE
          WHEN LENGTH(CAST(wabaetlf.tlf_sempdat AS string)) = 3 THEN from_unixtime(unix_timestamp(concat(substring(partition_date, 1, 4),'0',CAST(wabaetlf.tlf_sempdat AS string)), 'yyyyMMdd'),'yyyy-MM-dd')
          WHEN length(CAST(wabaetlf.tlf_sempdat AS string)) = 4 THEN from_unixtime(unix_timestamp(concat(substring(partition_date, 1, 4),CAST(wabaetlf.tlf_sempdat AS string)), 'yyyyMMdd'),'yyyy-MM-dd')
          ELSE NULL
          END  AS dt_cue_fecha_banco
     ,CASE WHEN LENGTH(trim(wabaetlf.tlf_semcod_term))=0 THEN NULL ELSE trim(wabaetlf.tlf_semcod_term) END AS cod_cue_terminal
     ,CASE WHEN LENGTH(trim(wabaetlf.tlf_semcod_tarj))=0 THEN NULL ELSE trim(wabaetlf.tlf_semcod_tarj) END AS id_cue_entidad_tarjeta
     ,CASE WHEN LENGTH(trim(wabaetlf.tlf_semccode))=0 THEN NULL ELSE trim(wabaetlf.tlf_semccode) END AS cod_cue_moneda
	,CASE WHEN trim(wabaetlf.tlf_semccode)='032' THEN 'Pesos'
		  WHEN trim(wabaetlf.tlf_semccode)='840' THEN 'Dólares'
  	   	  WHEN trim(wabaetlf.tlf_semccode)='858' THEN 'Pesos Uruguayos'
	 ELSE NULL
	 END AS ds_cue_moneda
	,wabaetlf.tlf_semrrev AS cod_cue_reverso
	,CASE WHEN wabaetlf.tlf_semrrev=1 THEN 'Tiempo para completar la autorización excedida'
		  WHEN wabaetlf.tlf_semrrev=2 THEN 'Respuesta rechazada por la terminal'
		  WHEN wabaetlf.tlf_semrrev=3 THEN 'Destino (terminal) no disponible'
		  WHEN wabaetlf.tlf_semrrev=8 THEN 'Cancelación del cliente'
		  WHEN wabaetlf.tlf_semrrev=10 THEN 'Error de hardware. Incluye a los reversos parciales'
		  WHEN wabaetlf.tlf_semrrev=20 THEN 'Transacción sospechosa. No se recibió la confirmación'
	ELSE NULL END ds_cue_reverso
    ,CASE WHEN LENGTH(trim(wabaetlf.tlf_id_canal))=0 THEN NULL ELSE trim(wabaetlf.tlf_id_canal) END AS cod_cue_canal
	,NULL AS ds_cue_canal
	,CASE WHEN trim(wabaetlf.tlf_marca_iva)='1' THEN '1' ELSE 0 end AS flag_cue_consumidor_final
    ,CASE WHEN LENGTH(trim(wabaetlf.tlf_semrcard))=0 THEN NULL ELSE trim(wabaetlf.tlf_semrcard) END AS cod_cue_semrcard
    ,CASE WHEN LENGTH(trim(wabaetlf.tlf_semtrnad))=0 THEN NULL ELSE trim(wabaetlf.tlf_semtrnad) END AS cod_cue_semtrnad
	,CASE WHEN LENGTH(trim(wabaetlf.tlf_semcity))=0 THEN NULL ELSE trim(wabaetlf.tlf_semcity) END ds_cue_terminal_ciudad
	,CAST(substring(wabaetlf.tlf_semfanum,0,3) AS int) AS cod_suc_sucursal_desde
	,CAST(substring(wabaetlf.tlf_semfanum,3,12) AS bigint) AS cod_cue_cuenta_desde
	,CAST(substring(wabaetlf.tlf_semtanum,0,3) AS int) AS cod_suc_sucursal_hasta
	,CAST(substring(wabaetlf.tlf_semtanum,3,12) AS bigint) AS cod_suc_cuenta_hasta
	,CASE WHEN trim(wabaetlf.tlf_semterm_country)='00' OR LENGTH(trim(wabaetlf.tlf_semterm_country))=0  THEN NULL ELSE trim(wabaetlf.tlf_semterm_country) END AS cod_cue_terminal_pais
	,pais.ds_cue_pais AS ds_cue_terminal_pais
	,CASE WHEN LENGTH(trim(wabaetlf.tlf_sembanc))=0 THEN NULL ELSE trim(wabaetlf.tlf_sembanc) END cod_cue_tarjeta_banco
	,CASE WHEN LENGTH(trim(wabaetlf.tlf_semfiid))=0 THEN NULL ELSE trim(wabaetlf.tlf_semfiid) END cod_cue_terminal_banco
	,CASE WHEN LENGTH(trim(wabaetlf.tlf_cbu_destino))=0 THEN NULL ELSE trim(wabaetlf.tlf_cbu_destino) END cod_cbu_destino
	,NULL AS ds_cue_establecimiento
	,CASE WHEN LENGTH(trim(wabaetlf.tlf_semtcomer))=0 THEN NULL ELSE trim(wabaetlf.tlf_semtcomer) END cod_cue_comercio
	,CASE WHEN LENGTH(trim(wabaetlf.tlf_semtatmi))=0 THEN NULL ELSE trim(wabaetlf.tlf_semtatmi) END cod_cue_atm
	,wabaetlf.tlf_semtrenu AS cod_cue_comprobante
	,trim(wabaetlf.tlf_nombre_origen) AS ds_cue_origen
	,CASE WHEN LENGTH(trim(wabaetlf.tlf_tip_debin))=0 THEN NULL ELSE trim(wabaetlf.tlf_tip_debin) END AS cod_cue_debin_tipo
	,NULL AS cod_cue_moneda_origen
	,NULL ds_cue_moneda_origen
	,NULL AS fc_cue_importe_origen
	,NULL AS cod_cue_cupon
	,NULL AS cod_cue_porc_devl_clte
	,NULL AS cod_cue_porc_devl_comer
	,NULL AS cod_cue_rubro
	,CASE WHEN trim(wabaetlf.tlf_id_debin)='0000        000000' OR LENGTH(trim(wabaetlf.tlf_id_debin))=0  THEN NULL ELSE trim(wabaetlf.tlf_id_debin) END AS cod_cue_debin
	,CASE WHEN LENGTH(trim(wabaetlf.tlf_cuit_bco_cdor))=0 THEN NULL ELSE trim(wabaetlf.tlf_cuit_bco_cdor) END AS cod_cue_cuit_bco_cdor
	,CASE WHEN LENGTH(trim(wabaetlf.tlf_cbu_bco_cdor))=0 THEN NULL ELSE trim(wabaetlf.tlf_cbu_bco_cdor) END AS cod_cue_cbu_bco_cdor
	,trim(wabaetlf.tlf_cuit_bco_vdor) AS cod_cue_cuit_bco_vdor
	,CASE WHEN LENGTH(trim(wabaetlf.tlf_cbu_bco_vdor))=0 THEN NULL ELSE trim(wabaetlf.tlf_cbu_bco_vdor) END AS cod_cue_cbu_bco_vdor
	,CASE
          WHEN LENGTH(CAST(wabaetlf.tlf_fec_neg_coel AS string)) = 3 THEN concat(substring(partition_date, 1, 4),'0',CAST(wabaetlf.tlf_fec_neg_coel AS string))
          WHEN length(CAST(wabaetlf.tlf_fec_neg_coel AS string)) = 4 THEN concat(substring(partition_date, 1, 4),CAST(wabaetlf.tlf_fec_neg_coel AS string))
          ELSE NULL
     END  AS dt_cue_neg_coel
	,wabaetlf.tlf_scoring_debin AS cod_cue_debin_scoring
	,CASE WHEN wabaetlf.tlf_cpto_debin=0 THEN NULL ELSE wabaetlf.tlf_cpto_debin END AS cod_cue_debin_cpto
	,trim(wabaetlf.tlf_desc_cpto_deb) AS ds_cue_cpto_deb
	,CASE WHEN LENGTH(trim(wabaetlf.tlf_corresp_titu))=0 THEN NULL ELSE trim(wabaetlf.tlf_corresp_titu) END AS cod_cue_corresp_titu
	,partition_date
FROM bi_corp_staging.abae_wabaetlf wabaetlf
LEFT JOIN abae_wabaetlf_reversos wabaetlf_reversos
ON 	wabaetlf.tlf_semcacct=wabaetlf_reversos.tlf_semcacct
AND	wabaetlf.tlf_semtrdat=wabaetlf_reversos.tlf_semtrdat
AND	wabaetlf.tlf_semtrtim=wabaetlf_reversos.tlf_semtrtim
AND	wabaetlf.tlf_semtrenu=wabaetlf_reversos.tlf_semtrenu
AND	wabaetlf.tlf_semtatmi=wabaetlf_reversos.tlf_semtatmi
LEFT JOIN pais pais
on trim(wabaetlf.tlf_semterm_country)=pais.cod_cue_pais
WHERE partition_date='{{ ti.xcom_pull(task_ids='InputConfig', key='partition_date', dag_id='LOAD_CMN_Cuentas_Abae') }}'
	and wabaetlf_reversos.tlf_semtrenu is null -- Saco los reversos

UNION ALL

SELECT
	 wabaetlf_cre.tlf_stat AS cod_cue_estado
	,CASE WHEN wabaetlf_cre.tlf_stat=0 THEN 'OK'
		  WHEN wabaetlf_cre.tlf_stat=11 THEN 'Destino no Disponible'
		  WHEN wabaetlf_cre.tlf_stat=12 THEN 'Línea no Disponible'
		  WHEN wabaetlf_cre.tlf_stat=20 THEN 'Originador no Disponible'
		  WHEN wabaetlf_cre.tlf_stat=21 THEN 'Tipo de Mensaje Desconocido'
		  WHEN wabaetlf_cre.tlf_stat=22 THEN 'Número de Tarjeta Desconocido'
		  ELSE NULL
	 END AS ds_cue_estado
	,CASE WHEN trim(wabaetlf_cre.tlf_1er_uso)='P' THEN 1 ELSE 0 end   flag_cue_primer_uso
	,wabaetlf_cre.tlf_semsgtyp AS cod_cue_tipo_mensaje
	,CASE WHEN wabaetlf_cre.tlf_semsgtyp=0210 THEN 'Respuesta de Autorización (Aprobación o Denegación)'
	      WHEN wabaetlf_cre.tlf_semsgtyp=0215 THEN 'Transacción de Consulta de Últimos Movimientos, de Consulta de Últimos Aportes y de Consulta de Servicios por Vencer.'
	      WHEN wabaetlf_cre.tlf_semsgtyp=0220 THEN 'Aviso de Autorización de Transacción'
	      WHEN wabaetlf_cre.tlf_semsgtyp=0400 THEN 'Reverso Contable de Operación Aprobada'
	      WHEN wabaetlf_cre.tlf_semsgtyp=0420 THEN 'Aviso de Reverso de Transacción'
	      ELSE NULL
	 END ds_cue_tipo_mensaje
	,CASE WHEN LENGTH(trim(wabaetlf_cre.tlf_semcacct))=0 THEN NULL ELSE trim(wabaetlf_cre.tlf_semcacct) END AS cod_cue_tarjeta
	,wabaetlf_cre.tlf_semtcode AS cod_cue_tipo_movimiento
	,CASE WHEN wabaetlf_cre.tlf_semtcode=15 THEN 'Compra (POS)'
	      WHEN wabaetlf_cre.tlf_semtcode=17 THEN 'Devolución descuento crédito (POS)'
	      WHEN wabaetlf_cre.tlf_semtcode=18 THEN 'Devolución de Compra (POS)'
	      WHEN wabaetlf_cre.tlf_semtcode=19 THEN 'Devolución descuento débito (POS)'
	      WHEN wabaetlf_cre.tlf_semtcode=22 THEN 'Anulación de devolución de compra (POS)'
	      WHEN wabaetlf_cre.tlf_semtcode=23 THEN 'Anulación de compra (POS)'
	      WHEN wabaetlf_cre.tlf_semtcode=24 THEN 'Anulación de compra con cashback (POS)'
	      WHEN wabaetlf_cre.tlf_semtcode=26 THEN 'Compra con cashback (POS)'
	      WHEN wabaetlf_cre.tlf_semtcode=35 THEN 'Consulta de disponible de compra (POS)'
		  ELSE NULL
	 END  AS ds_cue_tipo_movimiento
	,wabaetlf_cre.tlf_semtfacc AS  cod_cue_tipo_cuenta_desde
	,CASE WHEN wabaetlf_cre.tlf_semtfacc=1 THEN 'Cuenta corriente en pesos'
		  WHEN wabaetlf_cre.tlf_semtfacc=2 THEN 'Cuenta corriente en dólares'
		  WHEN wabaetlf_cre.tlf_semtfacc=11 THEN 'Caja de ahorros en pesos'
		  WHEN wabaetlf_cre.tlf_semtfacc=12 THEN 'Caja de ahorros en dólares'
		  WHEN wabaetlf_cre.tlf_semtfacc=13 THEN 'Cuenta especial'
		  WHEN wabaetlf_cre.tlf_semtfacc=20 THEN 'Cuenta de CBU'
		  WHEN wabaetlf_cre.tlf_semtfacc=31 THEN 'Cuenta de crédito en pesos'
		  WHEN wabaetlf_cre.tlf_semtfacc=32 THEN 'Cuenta de crédito en dólares'
		  ELSE NULL
	 END AS  ds_cue_tipo_cuenta_desde
	,wabaetlf_cre.tlf_semttacc AS  cod_cue_tipo_cuenta_hasta
	,CASE WHEN wabaetlf_cre.tlf_semttacc=1 THEN 'Cuenta corriente en pesos'
		  WHEN wabaetlf_cre.tlf_semttacc=2 THEN 'Cuenta corriente en dólares'
		  WHEN wabaetlf_cre.tlf_semttacc=11 THEN 'Caja de ahorros en pesos'
		  WHEN wabaetlf_cre.tlf_semttacc=12 THEN 'Caja de ahorros en dólares'
		  WHEN wabaetlf_cre.tlf_semttacc=13 THEN 'Cuenta especial'
		  WHEN wabaetlf_cre.tlf_semttacc=20 THEN 'Cuenta de CBU'
		  WHEN wabaetlf_cre.tlf_semttacc=31 THEN 'Cuenta de crédito en pesos'
		  WHEN wabaetlf_cre.tlf_semttacc=32 THEN 'Cuenta de crédito en dólares'
		  ELSE NULL
	 END AS  ds_cue_tipo_cuenta_hasta
	,wabaetlf_cre.tlf_semamt1 AS fc_cue_importe_cuenta
	,wabaetlf_cre.tlf_semamt2 AS fc_cue_importe_impuesto
	,CASE when LENGTH(wabaetlf_cre.tlf_semamt3)=0 THEN NULL ELSE wabaetlf_cre.tlf_semamt3 end  AS fc_cue_importe_compra_usd
	,CASE WHEN LENGTH(trim(wabaetlf_cre.tlf_fiid))=0 OR trim(tlf_fiid)='0000' THEN NULL ELSE trim(wabaetlf_cre.tlf_fiid) END AS cod_cue_comercio_camp
	,CASE WHEN wabaetlf_cre.tlf_auth_code=0 THEN NULL ELSE wabaetlf_cre.tlf_auth_code END AS cod_cue_autorizacion
	,CASE WHEN LENGTH(trim(wabaetlf_cre.tlf_sys_id))=0 THEN NULL ELSE trim(wabaetlf_cre.tlf_sys_id) END AS cod_cue_tipo_tarjeta_banelco
	,CASE WHEN LENGTH(trim(wabaetlf_cre.tlf_sys_id))=0 OR trim(wabaetlf_cre.tlf_fiid) IS NULL THEN 0 ELSE 1 END AS flag_cue_tarjeta_banelco
	,CASE WHEN wabaetlf_cre.tlf_trace_no=0 THEN NULL ELSE wabaetlf_cre.tlf_trace_no END AS cod_cue_trace_host
    ,concat(
			  CASE
	          WHEN LENGTH(CAST(wabaetlf_cre.tlf_semtrdat AS string)) = 3 THEN from_unixtime(unix_timestamp(concat(substring(partition_date, 1, 4),'0',CAST(wabaetlf_cre.tlf_semtrdat AS string)), 'yyyyMMdd'),'yyyy-MM-dd')
	          WHEN length(CAST(wabaetlf_cre.tlf_semtrdat AS string)) = 4 THEN from_unixtime(unix_timestamp(concat(substring(partition_date, 1, 4),CAST(wabaetlf_cre.tlf_semtrdat AS string)), 'yyyyMMdd'),'yyyy-MM-dd')
	          ELSE NULL
	          END,' ',substring(cast(wabaetlf_cre.tlf_semtrtim as string),1,2),':',substring(cast(wabaetlf_cre.tlf_semtrtim as string),3,2),':',substring(cast(wabaetlf_cre.tlf_semtrtim as string),5,2)
             ) AS  ts_cue_transaccion
	,CASE
          WHEN LENGTH(CAST(wabaetlf_cre.tlf_sempdat AS string)) = 3 THEN from_unixtime(unix_timestamp(concat(substring(partition_date, 1, 4),'0',CAST(wabaetlf_cre.tlf_sempdat AS string)), 'yyyyMMdd'),'yyyy-MM-dd')
          WHEN length(CAST(wabaetlf_cre.tlf_sempdat AS string)) = 4 THEN from_unixtime(unix_timestamp(concat(substring(partition_date, 1, 4),CAST(wabaetlf_cre.tlf_sempdat AS string)), 'yyyyMMdd'),'yyyy-MM-dd')
          ELSE NULL
          END  AS dt_cue_fecha_banco
     ,CASE WHEN LENGTH(trim(wabaetlf_cre.tlf_semcod_term))=0 THEN NULL ELSE trim(wabaetlf_cre.tlf_semcod_term) END AS cod_cue_terminal
     ,CASE WHEN LENGTH(trim(wabaetlf_cre.tlf_semcod_tarj))=0 THEN NULL ELSE trim(wabaetlf_cre.tlf_semcod_tarj) END AS id_cue_entidad_tarjeta
     ,CASE WHEN LENGTH(trim(wabaetlf_cre.tlf_semccode))=0 THEN NULL ELSE trim(wabaetlf_cre.tlf_semccode) END AS cod_cue_moneda
	,CASE WHEN trim(wabaetlf_cre.tlf_semccode)='032' THEN 'Pesos'
		  WHEN trim(wabaetlf_cre.tlf_semccode)='840' THEN 'Dólares'
  	   	  WHEN trim(wabaetlf_cre.tlf_semccode)='858' THEN 'Pesos Uruguayos'
	 ELSE NULL
	 END AS ds_cue_moneda
	,wabaetlf_cre.tlf_semrrev AS cod_cue_reverso
	,CASE WHEN wabaetlf_cre.tlf_semrrev=1 THEN 'Tiempo para completar la autorización excedida'
		  WHEN wabaetlf_cre.tlf_semrrev=2 THEN 'Respuesta rechazada por la terminal'
		  WHEN wabaetlf_cre.tlf_semrrev=3 THEN 'Destino (terminal) no disponible'
		  WHEN wabaetlf_cre.tlf_semrrev=8 THEN 'Cancelación del cliente'
		  WHEN wabaetlf_cre.tlf_semrrev=10 THEN 'Error de hardware. Incluye a los reversos parciales'
		  WHEN wabaetlf_cre.tlf_semrrev=20 THEN 'Transacción sospechosa. No se recibió la confirmación'
	ELSE NULL END ds_cue_reverso
    ,CASE WHEN LENGTH(trim(wabaetlf_cre.tlf_id_canal))=0 THEN NULL ELSE trim(wabaetlf_cre.tlf_id_canal) END AS cod_cue_canal
	,NULL AS ds_cue_canal
	,CASE WHEN trim(wabaetlf_cre.tlf_marca_iva)='1' THEN '1' ELSE 0 end AS flag_cue_consumidor_final
    ,CASE WHEN LENGTH(trim(wabaetlf_cre.tlf_semrcard))=0 THEN NULL ELSE trim(wabaetlf_cre.tlf_semrcard) END AS cod_cue_semrcard
    ,CASE WHEN LENGTH(trim(wabaetlf_cre.tlf_semtrnad))=0 THEN NULL ELSE trim(wabaetlf_cre.tlf_semtrnad) END AS cod_cue_semtrnad
	,CASE WHEN LENGTH(trim(wabaetlf_cre.tlf_semcity))=0 THEN NULL ELSE trim(wabaetlf_cre.tlf_semcity) END ds_cue_terminal_ciudad
	,CAST(substring(wabaetlf_cre.tlf_semfanum,0,3) AS int) AS cod_suc_sucursal_desde
	,CAST(substring(wabaetlf_cre.tlf_semfanum,3,12) AS bigint) AS cod_cue_cuenta_desde
	,CAST(substring(wabaetlf_cre.tlf_semtanum,0,3) AS int) AS cod_suc_sucursal_hasta
	,CAST(substring(wabaetlf_cre.tlf_semtanum,3,12) AS bigint) AS cod_suc_cuenta_hasta
	,CASE WHEN trim(wabaetlf_cre.tlf_semterm_country)='00' OR LENGTH(trim(wabaetlf_cre.tlf_semterm_country))=0  THEN NULL ELSE trim(wabaetlf_cre.tlf_semterm_country) END AS cod_cue_terminal_pais
	,pais.ds_cue_pais AS ds_cue_terminal_pais
	,CASE WHEN LENGTH(trim(wabaetlf_cre.tlf_sembanc))=0 THEN NULL ELSE trim(wabaetlf_cre.tlf_sembanc) END cod_cue_tarjeta_banco
	,CASE WHEN LENGTH(trim(wabaetlf_cre.tlf_semfiid))=0 THEN NULL ELSE trim(wabaetlf_cre.tlf_semfiid) END cod_cue_terminal_banco
	,CASE WHEN LENGTH(trim(wabaetlf_cre.tlf_cbu_destino))=0 THEN NULL ELSE trim(wabaetlf_cre.tlf_cbu_destino) END cod_cbu_destino
	,NULL AS ds_cue_establecimiento
	,CASE WHEN LENGTH(trim(wabaetlf_cre.tlf_semtcomer))=0 THEN NULL ELSE trim(wabaetlf_cre.tlf_semtcomer) END cod_cue_comercio
	,CASE WHEN LENGTH(trim(wabaetlf_cre.tlf_semtatmi))=0 THEN NULL ELSE trim(wabaetlf_cre.tlf_semtatmi) END cod_cue_atm
	,wabaetlf_cre.tlf_semtrenu AS cod_cue_comprobante
	,trim(wabaetlf_cre.tlf_nombre_origen) AS ds_cue_origen
	,CASE WHEN LENGTH(trim(wabaetlf_cre.tlf_tip_debin))=0 THEN NULL ELSE trim(wabaetlf_cre.tlf_tip_debin) END AS cod_cue_debin_tipo
	,NULL AS cod_cue_moneda_origen
	,NULL ds_cue_moneda_origen
	,NULL AS fc_cue_importe_origen
	,NULL AS cod_cue_cupon
	,NULL AS cod_cue_porc_devl_clte
	,NULL AS cod_cue_porc_devl_comer
	,NULL AS cod_cue_rubro
	,CASE WHEN trim(wabaetlf_cre.tlf_id_debin)='0000        000000' OR LENGTH(trim(wabaetlf_cre.tlf_id_debin))=0  THEN NULL ELSE trim(wabaetlf_cre.tlf_id_debin) END AS cod_cue_debin
	,CASE WHEN LENGTH(trim(wabaetlf_cre.tlf_cuit_bco_cdor))=0 THEN NULL ELSE trim(wabaetlf_cre.tlf_cuit_bco_cdor) END AS cod_cue_cuit_bco_cdor
	,CASE WHEN LENGTH(trim(wabaetlf_cre.tlf_cbu_bco_cdor))=0 THEN NULL ELSE trim(wabaetlf_cre.tlf_cbu_bco_cdor) END AS cod_cue_cbu_bco_cdor
	,trim(wabaetlf_cre.tlf_cuit_bco_vdor) AS cod_cue_cuit_bco_vdor
	,CASE WHEN LENGTH(trim(wabaetlf_cre.tlf_cbu_bco_vdor))=0 THEN NULL ELSE trim(wabaetlf_cre.tlf_cbu_bco_vdor) END AS cod_cue_cbu_bco_vdor
	,CASE
          WHEN LENGTH(CAST(wabaetlf_cre.tlf_fec_neg_coel AS string)) = 3 THEN concat(substring(partition_date, 1, 4),'0',CAST(wabaetlf_cre.tlf_fec_neg_coel AS string))
          WHEN length(CAST(wabaetlf_cre.tlf_fec_neg_coel AS string)) = 4 THEN concat(substring(partition_date, 1, 4),CAST(wabaetlf_cre.tlf_fec_neg_coel AS string))
          ELSE NULL
     END  AS dt_cue_neg_coel
	,wabaetlf_cre.tlf_scoring_debin AS cod_cue_debin_scoring
	,CASE WHEN wabaetlf_cre.tlf_cpto_debin=0 THEN NULL ELSE wabaetlf_cre.tlf_cpto_debin END AS cod_cue_debin_cpto
	,trim(wabaetlf_cre.tlf_desc_cpto_deb) AS ds_cue_cpto_deb
	,CASE WHEN LENGTH(trim(wabaetlf_cre.tlf_corresp_titu))=0 THEN NULL ELSE trim(wabaetlf_cre.tlf_corresp_titu) END AS cod_cue_corresp_titu
	,partition_date
FROM bi_corp_staging.abae_wabaetlf_cre wabaetlf_cre
LEFT JOIN abae_wabaetlfcre_reversos wabaetlfcre_reversos
ON 	wabaetlf_cre.tlf_semcacct=wabaetlfcre_reversos.tlf_semcacct
AND	wabaetlf_cre.tlf_semtrdat=wabaetlfcre_reversos.tlf_semtrdat
AND	wabaetlf_cre.tlf_semtrtim=wabaetlfcre_reversos.tlf_semtrtim
AND	wabaetlf_cre.tlf_semtrenu=wabaetlfcre_reversos.tlf_semtrenu
AND	wabaetlf_cre.tlf_semtatmi=wabaetlfcre_reversos.tlf_semtatmi
LEFT JOIN pais pais
on trim(wabaetlf_cre.tlf_semterm_country)=pais.cod_cue_pais
WHERE partition_date='{{ ti.xcom_pull(task_ids='InputConfig', key='partition_date', dag_id='LOAD_CMN_Cuentas_Abae') }}'
	and wabaetlfcre_reversos.tlf_semtrenu is null -- Saco los reversos
;