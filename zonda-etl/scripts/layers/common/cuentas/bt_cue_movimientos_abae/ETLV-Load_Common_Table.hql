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
		tlf_semcacct,
		tlf_semtrdat,
		tlf_semtrtim,
		tlf_semtrenu,
		tlf_semtatmi
from bi_corp_staging.abae_wabaetlx
where partition_date='{{ ti.xcom_pull(task_ids='InputConfig', key='partition_date', dag_id='LOAD_CMN_Cuentas_Abae') }}'
	  AND tlf_semtatmi=400;



DROP TABLE IF EXISTS abae_wabaetlf_cre_reversos;
CREATE TEMPORARY TABLE abae_wabaetlf_cre_reversos AS
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

DROP TABLE IF EXISTS tlf_visa_reversos;
CREATE TEMPORARY TABLE tlf_visa_reversos AS
select
		tlf_semcacct,
		tlf_semtrdat,
		tlf_semtrtim,
		tlf_semtrenu,
		tlf_semtatmi
from bi_corp_staging.abae_tlf_visa
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
WHERE cod_cue_pais is not NULL;


--Insertamos en la tabla final los movimientos sin reversos
insert overwrite table bi_corp_common.bt_cue_movimientos_abae
partition(partition_date)
SELECT
  'TLF' as cod_cue_origen
  ,case when length(cast(wabaetlf.tlf_semtrtim as string))=6
			then concat(
						  CASE
				          WHEN LENGTH(CAST(wabaetlf.tlf_semtrdat AS string)) = 3 THEN from_unixtime(unix_timestamp(concat(substring(wabaetlf.partition_date, 1, 4),'0',CAST(wabaetlf.tlf_semtrdat AS string)), 'yyyyMMdd'),'yyyy-MM-dd')
				          WHEN length(CAST(wabaetlf.tlf_semtrdat AS string)) = 4 THEN from_unixtime(unix_timestamp(concat(substring(wabaetlf.partition_date, 1, 4),CAST(wabaetlf.tlf_semtrdat AS string)), 'yyyyMMdd'),'yyyy-MM-dd')
				          ELSE NULL
				          END,' ',substring(cast(wabaetlf.tlf_semtrtim as string),1,2),':',substring(cast(wabaetlf.tlf_semtrtim as string),3,2),':',substring(cast(wabaetlf.tlf_semtrtim as string),5,2)
			            )
             else concat(
						  CASE
				          WHEN LENGTH(CAST(wabaetlf.tlf_semtrdat AS string)) = 3 THEN from_unixtime(unix_timestamp(concat(substring(wabaetlf.partition_date, 1, 4),'0',CAST(wabaetlf.tlf_semtrdat AS string)), 'yyyyMMdd'),'yyyy-MM-dd')
				          WHEN length(CAST(wabaetlf.tlf_semtrdat AS string)) = 4 THEN from_unixtime(unix_timestamp(concat(substring(wabaetlf.partition_date, 1, 4),CAST(wabaetlf.tlf_semtrdat AS string)), 'yyyyMMdd'),'yyyy-MM-dd')
				          ELSE NULL
				          END,' 0',substring(cast(wabaetlf.tlf_semtrtim as string),1,1),':',substring(cast(wabaetlf.tlf_semtrtim as string),2,2),':',substring(cast(wabaetlf.tlf_semtrtim as string),4,2)
			            )
             end as ts_cue_transaccion
  ,CASE
          WHEN LENGTH(CAST(wabaetlf.tlf_semtrdat AS string)) = 3 THEN from_unixtime(unix_timestamp(concat(substring(wabaetlf.partition_date, 1, 4),'0',CAST(wabaetlf.tlf_semtrdat AS string)), 'yyyyMMdd'),'yyyy-MM-dd')
          WHEN length(CAST(wabaetlf.tlf_semtrdat AS string)) = 4 THEN from_unixtime(unix_timestamp(concat(substring(wabaetlf.partition_date, 1, 4),CAST(wabaetlf.tlf_semtrdat AS string)), 'yyyyMMdd'),'yyyy-MM-dd')
          ELSE NULL
          END  AS dt_cue_transaccion
  ,CASE
          WHEN LENGTH(CAST(wabaetlf.tlf_sempdat AS string)) = 3 THEN from_unixtime(unix_timestamp(concat(substring(wabaetlf.partition_date, 1, 4),'0',CAST(wabaetlf.tlf_sempdat AS string)), 'yyyyMMdd'),'yyyy-MM-dd')
          WHEN length(CAST(wabaetlf.tlf_sempdat AS string)) = 4 THEN from_unixtime(unix_timestamp(concat(substring(wabaetlf.partition_date, 1, 4),CAST(wabaetlf.tlf_sempdat AS string)), 'yyyyMMdd'),'yyyy-MM-dd')
          ELSE NULL
          END  AS dt_cue_banco
  ,wabaetlf.tlf_stat AS cod_cue_estado
  ,CASE WHEN wabaetlf.tlf_stat=0 THEN 'OK'
		  WHEN wabaetlf.tlf_stat=11 THEN 'Destino no Disponible'
		  WHEN wabaetlf.tlf_stat=12 THEN 'Línea no Disponible'
		  WHEN wabaetlf.tlf_stat=20 THEN 'Originador no Disponible'
		  WHEN wabaetlf.tlf_stat=21 THEN 'Tipo de Mensaje Desconocido'
		  WHEN wabaetlf.tlf_stat=22 THEN 'Número de Tarjeta Desconocido'
		  ELSE NULL END AS ds_cue_estado
  ,case WHEN wabaetlf.tlf_product_ind='00' THEN 'ATM' else null end ds_cue_receptor_producto
  ,wabaetlf.tlf_release_number AS cod_cue_version_dpc
  ,wabaetlf.tlf_dpc_number AS ds_cue_num_dpc
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
  ,mov.ds_cue_tipo_movimiento
  ,case when wabaetlf.tlf_semtfacc in (1,2,11,12,13,20,31,32) then wabaetlf.tlf_semtfacc else null end AS  cod_cue_tipo_cuenta_desde
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
  ,case when wabaetlf.tlf_semttacc in (1,2,11,12,13,20,31,32) then wabaetlf.tlf_semttacc else null end AS  cod_cue_tipo_cuenta_hasta
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
  ,CASE WHEN wabaetlf.tlf_semamt3>0 AND trim(wabaetlf.tlf_semccode) = '840' THEN wabaetlf.tlf_semamt3 ELSE NULL end  AS fc_cue_importe_compra_usd
  ,CASE WHEN LENGTH(trim(wabaetlf.tlf_fiid))=0 OR trim(wabaetlf.tlf_fiid)='0000' THEN NULL ELSE trim(wabaetlf.tlf_fiid) END AS cod_cue_comercio_camp
  ,CASE WHEN wabaetlf.tlf_auth_code=0 THEN NULL ELSE wabaetlf.tlf_auth_code END AS cod_cue_autorizacion
  ,CASE WHEN LENGTH(trim(wabaetlf.tlf_sys_id))=0 THEN NULL ELSE trim(wabaetlf.tlf_sys_id) END AS cod_cue_tipo_tarjeta_banelco
  ,CASE WHEN LENGTH(trim(wabaetlf.tlf_sys_id))=0 OR trim(wabaetlf.tlf_fiid) IS NULL THEN 0 ELSE 1 END AS flag_cue_tarjeta_banelco
  ,CASE WHEN wabaetlf.tlf_trace_no=0 THEN NULL ELSE wabaetlf.tlf_trace_no END AS cod_cue_trace_host
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
  ,CASE WHEN trim(wabaetlf.tlf_marca_iva)='1' THEN 1 ELSE 0 end AS flag_cue_consumidor_final
  ,trim(wabaetlf.tlf_semrcard) AS cod_cue_semrcard
  ,trim(wabaetlf.tlf_semtrnad) AS cod_cue_semtrnad
  ,CASE WHEN trim(wabaetlf.tlf_semrcard) in ('0','5','7') and trim(wabaetlf.tlf_semtrnad) in ('00','01') then 1 else 0 end  as flag_cue_operacion_aceptada
  ,CASE WHEN LENGTH(trim(wabaetlf.tlf_semcity))=0 THEN NULL ELSE trim(wabaetlf.tlf_semcity) END ds_cue_terminal_ciudad
  ,case WHEN cast(wabaetlf.tlf_t72_dni_cuil as int)=0 or LENGTH(trim(wabaetlf.tlf_t72_dni_cuil))=0 THEN null else tlf_t72_dni_cuil END AS cod_cue_dni_cuil_cta_destino
  ,wabaetlf.tlf_t72_comision AS fc_cue_comision
  ,CASE WHEN LENGTH(trim(wabaetlf.tlf_seg_tipo_mov))=0 THEN NULL ELSE trim(wabaetlf.tlf_seg_tipo_mov) END AS cod_cue_seg_tipo_mov
  ,CASE WHEN LENGTH(trim(wabaetlf.tlf_seg_tipo_seguro))=0 THEN NULL ELSE trim(wabaetlf.tlf_seg_tipo_seguro) END AS cod_cue_seg_tipo_seguro
  ,CASE WHEN LENGTH(trim(wabaetlf.tlf_seg_identificacion))=0 THEN NULL ELSE trim(wabaetlf.tlf_seg_identificacion) END AS cod_cue_seg_identificacion
  ,wabaetlf.tlf_seg_costo_cobertura AS fc_cue_seg_costo_cobertura
  ,CASE WHEN LENGTH(trim(wabaetlf.tlf_sor_banelco))=0 THEN NULL ELSE trim(wabaetlf.tlf_sor_banelco) END AS cod_cue_sor_banelco
  ,wabaetlf.tlf_sor_banelco_importe AS fc_cue_sor_banelco_importe
  ,CASE WHEN LENGTH(trim(wabaetlf.tlf_sor_bancos))=0 THEN NULL ELSE trim(wabaetlf.tlf_sor_bancos) END AS cod_cue_sor_bancos
  ,wabaetlf.tlf_sor_bancos_importe AS fc_cue_sor_bancos_importe
  ,wabaetlf.tlf_semfalen AS cod_cue_longitud_cuenta_desde
  ,wabaetlf.tlf_semfanum_deb AS fc_cue_debito_desde
  ,wabaetlf.tlf_semfanum_cred AS fc_cue_credito_desde
  ,wabaetlf.tlf_semfanum_suc AS cod_suc_sucursal
  ,wabaetlf.tlf_semfanum_cta AS cod_cue_cuenta_desde
  ,wabaetlf.tlf_semtalen AS cod_cue_longitud_cuenta_hasta
  ,wabaetlf.tlf_semtanum_deb AS fc_cue_debito_hasta
  ,wabaetlf.tlf_semtanum_cred AS fc_cue_credito_hasta
  ,wabaetlf.tlf_semtanum_suc AS cod_suc_sucursal_hasta
  ,wabaetlf.tlf_semtanum_cta AS cod_cue_cuenta_hasta
  ,CASE WHEN trim(wabaetlf.tlf_semterm_country)='00' OR LENGTH(trim(wabaetlf.tlf_semterm_country))=0  THEN NULL ELSE trim(wabaetlf.tlf_semterm_country) END AS cod_cue_terminal_pais
  ,pais.ds_cue_pais AS ds_cue_terminal_pais
  ,CASE WHEN LENGTH(trim(wabaetlf.tlf_sembanc))=0 THEN NULL ELSE trim(wabaetlf.tlf_sembanc) END cod_cue_tarjeta_banco
  ,CASE WHEN LENGTH(trim(wabaetlf.tlf_semfiid))=0 THEN NULL ELSE trim(wabaetlf.tlf_semfiid) END cod_cue_terminal_banco
  ,CASE WHEN LENGTH(trim(wabaetlf.tlf_cbu_destino))=0 THEN NULL ELSE trim(wabaetlf.tlf_cbu_destino) END cod_cbu_destino
  ,CASE WHEN LENGTH(trim(wabaetlf.tlf_titularidad))=0 THEN NULL ELSE trim(wabaetlf.tlf_titularidad) END cod_cue_titularidad
  ,case WHEN tlf_titularidad='1' THEN 'Cta Cte propia'
  		WHEN tlf_titularidad='2' THEN 'Cta Cte no propia'
  		WHEN tlf_titularidad='3' THEN 'Otra cta propia'
 		WHEN tlf_titularidad='4' THEN 'Otra cta no propia'
 		else null end AS ds_cue_titularidad
  ,CASE WHEN LENGTH(trim(wabaetlf.tlf_tipo_oper))=0 THEN NULL ELSE trim(wabaetlf.tlf_tipo_oper) END AS cod_cue_tipo_oper
  ,CASE WHEN LENGTH(trim(wabaetlf.tlf_mand_doc_benef))=0 THEN NULL ELSE trim(wabaetlf.tlf_mand_doc_benef) END AS cod_cue_doc_benef
  ,CASE WHEN LENGTH(trim(wabaetlf.tlf_mand_num_benef))=0 THEN NULL ELSE trim(wabaetlf.tlf_mand_num_benef) END AS ds_cue_num_benef
  ,CASE WHEN LENGTH(trim(wabaetlf.tlf_mand_identif))=0 THEN NULL ELSE trim(wabaetlf.tlf_mand_identif) END AS cod_cue_mand_identif
  ,CASE WHEN LENGTH(trim(wabaetlf.tlf_semtcomer))=0 THEN NULL ELSE trim(wabaetlf.tlf_semtcomer) END cod_cue_comercio
  ,CASE WHEN LENGTH(trim(wabaetlf.tlf_semtatmi))=0 THEN NULL ELSE trim(wabaetlf.tlf_semtatmi) END cod_cue_atm
  ,wabaetlf.tlf_semtrenu AS cod_cue_comprobante
  ,tlf_sembanc_1 AS fc_cue_sembanc_1
  ,tlf_porcen_retencion  AS fc_cue_porcen_rentencion
  ,tlf_porcen_percepcion AS fc_cue_porcen_percepcion
  ,tlf_retencion_imp_pais  AS fc_cue_retencion_imp_pais
  ,case when tlf_tipo_extr='0' then null else tlf_tipo_extr end AS cod_cue_tipo_extraccion
  ,CASE WHEN LENGTH(trim(wabaetlf.tlf_semtidep))=0 THEN NULL ELSE trim(wabaetlf.tlf_semtidep) END AS cod_cue_tipo_deposito
  ,case when tlf_semtideb='0' then null else tlf_semtideb end AS cod_cue_tipo_debito
  ,case when tlf_semtipag='0' then null else tlf_semtipag end AS cod_cue_tipo_pago
  ,CASE WHEN LENGTH(trim(wabaetlf.tlf_semtitar))=0 THEN NULL ELSE trim(wabaetlf.tlf_semtitar) END AS cod_cue_tipo_tarjeta
  ,case WHEN trim(tlf_semtitar)='A' THEN 'Administrativa'
  		WHEN trim(tlf_semtitar)='B' THEN 'Restringida a Depositos'
  		WHEN trim(tlf_semtitar)='F' THEN 'Tarjeta con Cuenta Credito'
  		WHEN trim(tlf_semtitar)='G' THEN 'Baneldo Débito Adicional'
  		WHEN trim(tlf_semtitar)='J' THEN 'AFJP'
  		WHEN trim(tlf_semtitar)='D' THEN 'Diners Club'
  		WHEN trim(tlf_semtitar)='P' THEN 'Banelco Débito Titular'
  		WHEN trim(tlf_semtitar)='E' THEN 'Electron'
  		WHEN trim(tlf_semtitar)='M' THEN 'Mini Electron'
  		WHEN trim(tlf_semtitar)='I' THEN 'Electron Adicional'
  		WHEN trim(tlf_semtitar)='V' THEN 'Visa'
   else null end AS ds_cue_tipo_tarjeta
  ,trim(wabaetlf.tlf_nombre_origen) AS ds_cue_origen
  ,tlf_tip_debin AS cod_cue_tipo_debin
  ,CASE WHEN trim(wabaetlf.tlf_id_debin)='0000        000000' OR LENGTH(trim(wabaetlf.tlf_id_debin))=0  THEN NULL ELSE trim(wabaetlf.tlf_id_debin) END AS cod_cue_debin
  ,CASE WHEN LENGTH(trim(wabaetlf.tlf_cuit_bco_cdor))=0 THEN NULL ELSE trim(wabaetlf.tlf_cuit_bco_cdor) END AS cod_cue_cuit_bco_cdor
  ,CASE WHEN LENGTH(trim(wabaetlf.tlf_cbu_bco_cdor))=0 THEN NULL ELSE trim(wabaetlf.tlf_cbu_bco_cdor) END AS cod_cue_cbu_bco_cdor
  ,trim(wabaetlf.tlf_cuit_bco_vdor) AS cod_cue_cuit_bco_vdor
  ,CASE WHEN LENGTH(trim(wabaetlf.tlf_cbu_bco_vdor))=0 THEN NULL ELSE trim(wabaetlf.tlf_cbu_bco_vdor) END AS cod_cue_cbu_bco_vdor
  ,CASE
          WHEN LENGTH(CAST(wabaetlf.tlf_fec_neg_coel AS string)) = 3 THEN concat(substring(wabaetlf.partition_date, 1, 4),'0',CAST(wabaetlf.tlf_fec_neg_coel AS string))
          WHEN length(CAST(wabaetlf.tlf_fec_neg_coel AS string)) = 4 THEN concat(substring(wabaetlf.partition_date, 1, 4),CAST(wabaetlf.tlf_fec_neg_coel AS string))
          ELSE NULL
     END  AS dt_cue_neg_coel
  ,wabaetlf.tlf_scoring_debin AS cod_cue_debin_scoring
  ,CASE WHEN wabaetlf.tlf_cpto_debin=0 THEN NULL ELSE wabaetlf.tlf_cpto_debin END AS cod_cue_debin_cpto
  ,trim(wabaetlf.tlf_desc_cpto_deb) AS ds_cue_cpto_deb
  ,CASE WHEN LENGTH(trim(wabaetlf.tlf_corresp_titu))=0 THEN NULL ELSE trim(wabaetlf.tlf_corresp_titu) END AS cod_cue_corresp_titu
  ,tlf_moneda_origen AS cod_cue_moneda_origen
  ,CASE WHEN trim(wabaetlf.tlf_moneda_origen)='032' THEN 'Pesos'
		WHEN trim(wabaetlf.tlf_moneda_origen)='840' THEN 'Dólares'
  	   	WHEN trim(wabaetlf.tlf_moneda_origen)='858' THEN 'Pesos Uruguayos'
		ELSE NULL
   END AS ds_cue_moneda_origen
  ,tlf_semimpo_original AS fc_cue_importe_original
  ,tlf_semfefa AS ds_cue_fefa
  ,tlf_semcotco AS cod_cue_cotco
  ,tlf_semcotve AS cod_cue_cotve
  ,tlf_semamt5 AS fc_cue_semant5
  ,tlf_percepcion_imp_ganan AS fc_cue_imp_ganan
  ,tlf_nro_control AS ds_cue_num_control
  ,CASE WHEN LENGTH(trim(wabaetlf.tlf_trans_banco_receptor))=0 THEN NULL ELSE trim(wabaetlf.tlf_trans_banco_receptor) END AS ds_cue_banco_receptor
  ,CASE WHEN LENGTH(trim(wabaetlf.tlf_trans_codigo_concepto))=0 THEN NULL ELSE trim(wabaetlf.tlf_trans_codigo_concepto) END AS cod_cue_concepto
  ,CASE   WHEN tlf_trans_codigo_concepto='1'  THEN 'Alquileres'
		  WHEN tlf_trans_codigo_concepto='2'  THEN 'Cuota'
		  WHEN tlf_trans_codigo_concepto='3'  THEN 'Expensas'
		  WHEN tlf_trans_codigo_concepto='4'  THEN 'Factura'
		  WHEN tlf_trans_codigo_concepto='5'  THEN 'Prestamo'
		  WHEN tlf_trans_codigo_concepto='6'  THEN 'Seguro'
		  WHEN tlf_trans_codigo_concepto='7'  THEN 'Honorarios'
		  WHEN tlf_trans_codigo_concepto='8'  THEN 'Varios'
		  WHEN tlf_trans_codigo_concepto='9'  THEN 'Haberes'
		  WHEN tlf_trans_codigo_concepto='A'  THEN 'Operaciones Inmobiliarias'
		  WHEN tlf_trans_codigo_concepto='B'  THEN 'Operaciones Inmobiliarias Habitualista'
		  WHEN tlf_trans_codigo_concepto='C'  THEN 'Bienes Registrables Habitualistas'
		  WHEN tlf_trans_codigo_concepto='D'  THEN 'Bienes Registrables No Habitualistas'
		  WHEN tlf_trans_codigo_concepto='E'  THEN 'Suscripción Obligaciones Negociables'
		  WHEN tlf_trans_codigo_concepto='F'  THEN 'Reintegros de Obras Sociales y Prepagas'
		  WHEN tlf_trans_codigo_concepto='G'  THEN 'Siniestros de Seguros'
		  WHEN tlf_trans_codigo_concepto='H'  THEN 'Aportes de Capital'
		  WHEN tlf_trans_codigo_concepto='I'  THEN 'Estado Expropiaciones u Otros'
		  WHEN tlf_trans_codigo_concepto='J'  THEN 'Compra PEI'
		  WHEN tlf_trans_codigo_concepto='K'  THEN 'Pago PEI'
		  WHEN tlf_trans_codigo_concepto='L'  THEN 'Devolución PEI'
   ELSE null END AS ds_cue_concepto
   ,CASE WHEN LENGTH(trim(wabaetlf.tlf_trans_referencia))=0 THEN NULL ELSE trim(wabaetlf.tlf_trans_referencia) END AS ds_cue_trans_referencia
   ,CASE WHEN LENGTH(trim(wabaetlf.tlf_semnro_cargo))=0 THEN NULL ELSE trim(wabaetlf.tlf_semnro_cargo) END AS cod_cue_num_cargo
   ,CASE WHEN LENGTH(trim(wabaetlf.tlf_porc_devl_clte))=0 THEN NULL ELSE trim(wabaetlf.tlf_porc_devl_clte) END AS fc_cue_porcen_devl_clte
   ,CASE WHEN LENGTH(trim(wabaetlf.tlf_porc_devl_comer))=0 THEN NULL ELSE trim(wabaetlf.tlf_porc_devl_comer) END AS fc_cue_porcen_devl_comer
  ,tlf_rubro as cod_cue_rubros
  ,tlf_balanceo as cod_cue_balanceo
  ,CASE WHEN LENGTH(trim(wabaetlf.tlf_tipo_cajero))=0 THEN NULL ELSE trim(wabaetlf.tlf_tipo_cajero) END AS cod_cue_tipo_cajero
  ,stk_debi.cod_per_nup_tarjeta
  ,stk_debi.cod_per_nup_titular
  ,wabaetlf.partition_date
FROM   bi_corp_staging.abae_wabaetlf wabaetlf
left join bi_corp_common.dim_cue_tipo_movimiento_abae mov
on wabaetlf.tlf_semtcode=mov.cod_cue_tipo_movimiento
LEFT JOIN  bi_corp_common.stk_cue_tarjetas_debito stk_debi
ON cast(wabaetlf.tlf_semcacct as bigint) = stk_debi.cod_cue_tarjeta
AND stk_debi.partition_date = '{{ ti.xcom_pull(task_ids='InputConfig', key='max_partition_stk_cue_tarjetas_debito', dag_id='LOAD_CMN_Cuentas_Abae') }}'
LEFT JOIN abae_wabaetlf_reversos wabaetlf_reversos
ON 	wabaetlf.tlf_semcacct=wabaetlf_reversos.tlf_semcacct
AND	wabaetlf.tlf_semtrdat=wabaetlf_reversos.tlf_semtrdat
AND	wabaetlf.tlf_semtrtim=wabaetlf_reversos.tlf_semtrtim
AND	wabaetlf.tlf_semtrenu=wabaetlf_reversos.tlf_semtrenu
AND	wabaetlf.tlf_semtatmi=wabaetlf_reversos.tlf_semtatmi
LEFT JOIN pais pais
on trim(wabaetlf.tlf_semterm_country)=pais.cod_cue_pais
WHERE wabaetlf.partition_date='{{ ti.xcom_pull(task_ids='InputConfig', key='partition_date', dag_id='LOAD_CMN_Cuentas_Abae') }}'
	and wabaetlf_reversos.tlf_semtrenu is null -- Saco los reversos

UNION ALL

SELECT
  'TLF_CRE' as cod_cue_origen
  ,case when length(cast(wabaetlf_cre.tlf_semtrtim as string))=6
			then concat(
						  CASE
				          WHEN LENGTH(CAST(wabaetlf_cre.tlf_semtrdat AS string)) = 3 THEN from_unixtime(unix_timestamp(concat(substring(wabaetlf_cre.partition_date, 1, 4),'0',CAST(wabaetlf_cre.tlf_semtrdat AS string)), 'yyyyMMdd'),'yyyy-MM-dd')
				          WHEN length(CAST(wabaetlf_cre.tlf_semtrdat AS string)) = 4 THEN from_unixtime(unix_timestamp(concat(substring(wabaetlf_cre.partition_date, 1, 4),CAST(wabaetlf_cre.tlf_semtrdat AS string)), 'yyyyMMdd'),'yyyy-MM-dd')
				          ELSE NULL
				          END,' ',substring(cast(wabaetlf_cre.tlf_semtrtim as string),1,2),':',substring(cast(wabaetlf_cre.tlf_semtrtim as string),3,2),':',substring(cast(wabaetlf_cre.tlf_semtrtim as string),5,2)
			            )
             else concat(
						  CASE
				          WHEN LENGTH(CAST(wabaetlf_cre.tlf_semtrdat AS string)) = 3 THEN from_unixtime(unix_timestamp(concat(substring(wabaetlf_cre.partition_date, 1, 4),'0',CAST(wabaetlf_cre.tlf_semtrdat AS string)), 'yyyyMMdd'),'yyyy-MM-dd')
				          WHEN length(CAST(wabaetlf_cre.tlf_semtrdat AS string)) = 4 THEN from_unixtime(unix_timestamp(concat(substring(wabaetlf_cre.partition_date, 1, 4),CAST(wabaetlf_cre.tlf_semtrdat AS string)), 'yyyyMMdd'),'yyyy-MM-dd')
				          ELSE NULL
				          END,' 0',substring(cast(wabaetlf_cre.tlf_semtrtim as string),1,1),':',substring(cast(wabaetlf_cre.tlf_semtrtim as string),2,2),':',substring(cast(wabaetlf_cre.tlf_semtrtim as string),4,2)
			            )
             end as ts_cue_transaccion
  ,CASE
          WHEN LENGTH(CAST(wabaetlf_cre.tlf_semtrdat AS string)) = 3 THEN from_unixtime(unix_timestamp(concat(substring(wabaetlf_cre.partition_date, 1, 4),'0',CAST(wabaetlf_cre.tlf_semtrdat AS string)), 'yyyyMMdd'),'yyyy-MM-dd')
          WHEN length(CAST(wabaetlf_cre.tlf_semtrdat AS string)) = 4 THEN from_unixtime(unix_timestamp(concat(substring(wabaetlf_cre.partition_date, 1, 4),CAST(wabaetlf_cre.tlf_semtrdat AS string)), 'yyyyMMdd'),'yyyy-MM-dd')
          ELSE NULL
          END  AS dt_cue_transaccion
  ,CASE
          WHEN LENGTH(CAST(wabaetlf_cre.tlf_sempdat AS string)) = 3 THEN from_unixtime(unix_timestamp(concat(substring(wabaetlf_cre.partition_date, 1, 4),'0',CAST(wabaetlf_cre.tlf_sempdat AS string)), 'yyyyMMdd'),'yyyy-MM-dd')
          WHEN length(CAST(wabaetlf_cre.tlf_sempdat AS string)) = 4 THEN from_unixtime(unix_timestamp(concat(substring(wabaetlf_cre.partition_date, 1, 4),CAST(wabaetlf_cre.tlf_sempdat AS string)), 'yyyyMMdd'),'yyyy-MM-dd')
          ELSE NULL
          END  AS dt_cue_banco
  ,wabaetlf_cre.tlf_stat AS cod_cue_estado
  ,CASE WHEN wabaetlf_cre.tlf_stat=0 THEN 'OK'
		  WHEN wabaetlf_cre.tlf_stat=11 THEN 'Destino no Disponible'
		  WHEN wabaetlf_cre.tlf_stat=12 THEN 'Línea no Disponible'
		  WHEN wabaetlf_cre.tlf_stat=20 THEN 'Originador no Disponible'
		  WHEN wabaetlf_cre.tlf_stat=21 THEN 'Tipo de Mensaje Desconocido'
		  WHEN wabaetlf_cre.tlf_stat=22 THEN 'Número de Tarjeta Desconocido'
		  ELSE NULL END AS ds_cue_estado
  ,case WHEN wabaetlf_cre.tlf_product_ind='00' THEN 'ATM' else null end ds_cue_receptor_producto
  ,wabaetlf_cre.tlf_release_number AS cod_cue_version_dpc
  ,wabaetlf_cre.tlf_dpc_number AS ds_cue_num_dpc
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
  ,mov.ds_cue_tipo_movimiento
  ,case when wabaetlf_cre.tlf_semtfacc in (1,2,11,12,13,20,31,32) then wabaetlf_cre.tlf_semtfacc else null end AS  cod_cue_tipo_cuenta_desde
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
  ,case when wabaetlf_cre.tlf_semttacc in (1,2,11,12,13,20,31,32) then wabaetlf_cre.tlf_semttacc else null end AS  cod_cue_tipo_cuenta_hasta
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
  ,CASE WHEN wabaetlf_cre.tlf_semamt3>0 AND trim(wabaetlf_cre.tlf_semccode) = '840' THEN wabaetlf_cre.tlf_semamt3 ELSE NULL end  AS fc_cue_importe_compra_usd
  ,CASE WHEN LENGTH(trim(wabaetlf_cre.tlf_fiid))=0 OR trim(wabaetlf_cre.tlf_fiid)='0000' THEN NULL ELSE trim(wabaetlf_cre.tlf_fiid) END AS cod_cue_comercio_camp
  ,CASE WHEN wabaetlf_cre.tlf_auth_code=0 THEN NULL ELSE wabaetlf_cre.tlf_auth_code END AS cod_cue_autorizacion
  ,CASE WHEN LENGTH(trim(wabaetlf_cre.tlf_sys_id))=0 THEN NULL ELSE trim(wabaetlf_cre.tlf_sys_id) END AS cod_cue_tipo_tarjeta_banelco
  ,CASE WHEN LENGTH(trim(wabaetlf_cre.tlf_sys_id))=0 OR trim(wabaetlf_cre.tlf_fiid) IS NULL THEN 0 ELSE 1 END AS flag_cue_tarjeta_banelco
  ,CASE WHEN wabaetlf_cre.tlf_trace_no=0 THEN NULL ELSE wabaetlf_cre.tlf_trace_no END AS cod_cue_trace_host
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
  ,CASE WHEN trim(wabaetlf_cre.tlf_marca_iva)='1' THEN 1 ELSE 0 end AS flag_cue_consumidor_final
  ,trim(wabaetlf_cre.tlf_semrcard) AS cod_cue_semrcard
  ,trim(wabaetlf_cre.tlf_semtrnad) AS cod_cue_semtrnad
  ,CASE WHEN trim(wabaetlf_cre.tlf_semrcard) in ('0','5','7') and trim(wabaetlf_cre.tlf_semtrnad) in ('00','01') then 1 else 0 end  as flag_cue_operacion_aceptada
  ,CASE WHEN LENGTH(trim(wabaetlf_cre.tlf_semcity))=0 THEN NULL ELSE trim(wabaetlf_cre.tlf_semcity) END ds_cue_terminal_ciudad
  ,case WHEN cast(wabaetlf_cre.tlf_t72_dni_cuil as int)=0 or LENGTH(trim(wabaetlf_cre.tlf_t72_dni_cuil))=0 THEN null else tlf_t72_dni_cuil END AS cod_cue_dni_cuil_cta_destino
  ,wabaetlf_cre.tlf_t72_comision AS fc_cue_comision
  ,CASE WHEN LENGTH(trim(wabaetlf_cre.tlf_seg_tipo_mov))=0 THEN NULL ELSE trim(wabaetlf_cre.tlf_seg_tipo_mov) END AS cod_cue_seg_tipo_mov
  ,CASE WHEN LENGTH(trim(wabaetlf_cre.tlf_seg_tipo_seguro))=0 THEN NULL ELSE trim(wabaetlf_cre.tlf_seg_tipo_seguro) END AS cod_cue_seg_tipo_seguro
  ,CASE WHEN LENGTH(trim(wabaetlf_cre.tlf_seg_identificacion))=0 THEN NULL ELSE trim(wabaetlf_cre.tlf_seg_identificacion) END AS cod_cue_seg_identificacion
  ,wabaetlf_cre.tlf_seg_costo_cobertura AS fc_cue_seg_costo_cobertura
  ,CASE WHEN LENGTH(trim(wabaetlf_cre.tlf_sor_banelco))=0 THEN NULL ELSE trim(wabaetlf_cre.tlf_sor_banelco) END AS cod_cue_sor_banelco
  ,wabaetlf_cre.tlf_sor_banelco_importe AS fc_cue_sor_banelco_importe
  ,CASE WHEN LENGTH(trim(wabaetlf_cre.tlf_sor_bancos))=0 THEN NULL ELSE trim(wabaetlf_cre.tlf_sor_bancos) END AS cod_cue_sor_bancos
  ,wabaetlf_cre.tlf_sor_bancos_importe AS fc_cue_sor_bancos_importe
  ,wabaetlf_cre.tlf_semfalen AS cod_cue_longitud_cuenta_desde
  ,wabaetlf_cre.tlf_semfanum_deb AS fc_cue_debito_desde
  ,wabaetlf_cre.tlf_semfanum_cred AS fc_cue_credito_desde
  ,wabaetlf_cre.tlf_semfanum_suc AS cod_suc_sucursal
  ,wabaetlf_cre.tlf_semfanum_cta AS cod_cue_cuenta_desde
  ,wabaetlf_cre.tlf_semtalen AS cod_cue_longitud_cuenta_hasta
  ,wabaetlf_cre.tlf_semtanum_deb AS fc_cue_debito_hasta
  ,wabaetlf_cre.tlf_semtanum_cred AS fc_cue_credito_hasta
  ,wabaetlf_cre.tlf_semtanum_suc AS cod_suc_sucursal_hasta
  ,wabaetlf_cre.tlf_semtanum_cta AS cod_cue_cuenta_hasta
  ,CASE WHEN trim(wabaetlf_cre.tlf_semterm_country)='00' OR LENGTH(trim(wabaetlf_cre.tlf_semterm_country))=0  THEN NULL ELSE trim(wabaetlf_cre.tlf_semterm_country) END AS cod_cue_terminal_pais
  ,pais.ds_cue_pais AS ds_cue_terminal_pais
  ,CASE WHEN LENGTH(trim(wabaetlf_cre.tlf_sembanc))=0 THEN NULL ELSE trim(wabaetlf_cre.tlf_sembanc) END cod_cue_tarjeta_banco
  ,CASE WHEN LENGTH(trim(wabaetlf_cre.tlf_semfiid))=0 THEN NULL ELSE trim(wabaetlf_cre.tlf_semfiid) END cod_cue_terminal_banco
  ,CASE WHEN LENGTH(trim(wabaetlf_cre.tlf_cbu_destino))=0 THEN NULL ELSE trim(wabaetlf_cre.tlf_cbu_destino) END cod_cbu_destino
  ,CASE WHEN LENGTH(trim(wabaetlf_cre.tlf_titularidad))=0 THEN NULL ELSE trim(wabaetlf_cre.tlf_titularidad) END cod_cue_titularidad
  ,case WHEN tlf_titularidad='1' THEN 'Cta Cte propia'
  		WHEN tlf_titularidad='2' THEN 'Cta Cte no propia'
  		WHEN tlf_titularidad='3' THEN 'Otra cta propia'
 		WHEN tlf_titularidad='4' THEN 'Otra cta no propia'
 		else null end AS ds_cue_titularidad
  ,CASE WHEN LENGTH(trim(wabaetlf_cre.tlf_tipo_oper))=0 THEN NULL ELSE trim(wabaetlf_cre.tlf_tipo_oper) END AS cod_cue_tipo_oper
  ,CASE WHEN LENGTH(trim(wabaetlf_cre.tlf_mand_doc_benef))=0 THEN NULL ELSE trim(wabaetlf_cre.tlf_mand_doc_benef) END AS cod_cue_doc_benef
  ,CASE WHEN LENGTH(trim(wabaetlf_cre.tlf_mand_num_benef))=0 THEN NULL ELSE trim(wabaetlf_cre.tlf_mand_num_benef) END AS ds_cue_num_benef
  ,CASE WHEN LENGTH(trim(wabaetlf_cre.tlf_mand_identif))=0 THEN NULL ELSE trim(wabaetlf_cre.tlf_mand_identif) END AS cod_cue_mand_identif
  ,CASE WHEN LENGTH(trim(wabaetlf_cre.tlf_semtcomer))=0 THEN NULL ELSE trim(wabaetlf_cre.tlf_semtcomer) END cod_cue_comercio
  ,CASE WHEN LENGTH(trim(wabaetlf_cre.tlf_semtatmi))=0 THEN NULL ELSE trim(wabaetlf_cre.tlf_semtatmi) END cod_cue_atm
  ,wabaetlf_cre.tlf_semtrenu AS cod_cue_comprobante
  ,tlf_sembanc_1 AS fc_cue_sembanc_1
  ,tlf_porcen_retencion  AS fc_cue_porcen_rentencion
  ,tlf_porcen_percepcion AS fc_cue_porcen_percepcion
  ,tlf_retencion_imp_pais  AS fc_cue_retencion_imp_pais
  ,case when tlf_tipo_extr='0' then null else tlf_tipo_extr end AS cod_cue_tipo_extraccion
  ,CASE WHEN LENGTH(trim(wabaetlf_cre.tlf_semtidep))=0 THEN NULL ELSE trim(wabaetlf_cre.tlf_semtidep) END AS cod_cue_tipo_deposito
  ,case when tlf_semtideb='0' then null else tlf_semtideb end AS cod_cue_tipo_debito
  ,case when tlf_semtipag='0' then null else tlf_semtipag end AS cod_cue_tipo_pago
  ,CASE WHEN LENGTH(trim(wabaetlf_cre.tlf_semtitar))=0 THEN NULL ELSE trim(wabaetlf_cre.tlf_semtitar) END AS cod_cue_tipo_tarjeta
  ,case WHEN trim(tlf_semtitar)='A' THEN 'Administrativa'
  		WHEN trim(tlf_semtitar)='B' THEN 'Restringida a Depositos'
  		WHEN trim(tlf_semtitar)='F' THEN 'Tarjeta con Cuenta Credito'
  		WHEN trim(tlf_semtitar)='G' THEN 'Baneldo Débito Adicional'
  		WHEN trim(tlf_semtitar)='J' THEN 'AFJP'
  		WHEN trim(tlf_semtitar)='D' THEN 'Diners Club'
  		WHEN trim(tlf_semtitar)='P' THEN 'Banelco Débito Titular'
  		WHEN trim(tlf_semtitar)='E' THEN 'Electron'
  		WHEN trim(tlf_semtitar)='M' THEN 'Mini Electron'
  		WHEN trim(tlf_semtitar)='I' THEN 'Electron Adicional'
  		WHEN trim(tlf_semtitar)='V' THEN 'Visa'
   else null end AS ds_cue_tipo_tarjeta
  ,trim(wabaetlf_cre.tlf_nombre_origen) AS ds_cue_origen
  ,tlf_tip_debin AS cod_cue_tipo_debin
  ,CASE WHEN trim(wabaetlf_cre.tlf_id_debin)='0000        000000' OR LENGTH(trim(wabaetlf_cre.tlf_id_debin))=0  THEN NULL ELSE trim(wabaetlf_cre.tlf_id_debin) END AS cod_cue_debin
  ,CASE WHEN LENGTH(trim(wabaetlf_cre.tlf_cuit_bco_cdor))=0 THEN NULL ELSE trim(wabaetlf_cre.tlf_cuit_bco_cdor) END AS cod_cue_cuit_bco_cdor
  ,CASE WHEN LENGTH(trim(wabaetlf_cre.tlf_cbu_bco_cdor))=0 THEN NULL ELSE trim(wabaetlf_cre.tlf_cbu_bco_cdor) END AS cod_cue_cbu_bco_cdor
  ,trim(wabaetlf_cre.tlf_cuit_bco_vdor) AS cod_cue_cuit_bco_vdor
  ,CASE WHEN LENGTH(trim(wabaetlf_cre.tlf_cbu_bco_vdor))=0 THEN NULL ELSE trim(wabaetlf_cre.tlf_cbu_bco_vdor) END AS cod_cue_cbu_bco_vdor
  ,CASE
          WHEN LENGTH(CAST(wabaetlf_cre.tlf_fec_neg_coel AS string)) = 3 THEN concat(substring(wabaetlf_cre.partition_date, 1, 4),'0',CAST(wabaetlf_cre.tlf_fec_neg_coel AS string))
          WHEN length(CAST(wabaetlf_cre.tlf_fec_neg_coel AS string)) = 4 THEN concat(substring(wabaetlf_cre.partition_date, 1, 4),CAST(wabaetlf_cre.tlf_fec_neg_coel AS string))
          ELSE NULL
     END  AS dt_cue_neg_coel
  ,wabaetlf_cre.tlf_scoring_debin AS cod_cue_debin_scoring
  ,CASE WHEN wabaetlf_cre.tlf_cpto_debin=0 THEN NULL ELSE wabaetlf_cre.tlf_cpto_debin END AS cod_cue_debin_cpto
  ,trim(wabaetlf_cre.tlf_desc_cpto_deb) AS ds_cue_cpto_deb
  ,CASE WHEN LENGTH(trim(wabaetlf_cre.tlf_corresp_titu))=0 THEN NULL ELSE trim(wabaetlf_cre.tlf_corresp_titu) END AS cod_cue_corresp_titu
  ,tlf_moneda_origen AS cod_cue_moneda_origen
  ,CASE WHEN trim(wabaetlf_cre.tlf_moneda_origen)='032' THEN 'Pesos'
		WHEN trim(wabaetlf_cre.tlf_moneda_origen)='840' THEN 'Dólares'
  	   	WHEN trim(wabaetlf_cre.tlf_moneda_origen)='858' THEN 'Pesos Uruguayos'
		ELSE NULL
   END AS ds_cue_moneda_origen
  ,tlf_semimpo_original AS fc_cue_importe_original
  ,tlf_semfefa AS ds_cue_fefa
  ,tlf_semcotco AS cod_cue_cotco
  ,tlf_semcotve AS cod_cue_cotve
  ,tlf_semamt5 AS fc_cue_semant5
  ,tlf_percepcion_imp_ganan AS fc_cue_imp_ganan
  ,tlf_nro_control AS ds_cue_num_control
  ,CASE WHEN LENGTH(trim(wabaetlf_cre.tlf_trans_banco_receptor))=0 THEN NULL ELSE trim(wabaetlf_cre.tlf_trans_banco_receptor) END AS ds_cue_banco_receptor
  ,CASE WHEN LENGTH(trim(wabaetlf_cre.tlf_trans_codigo_concepto))=0 THEN NULL ELSE trim(wabaetlf_cre.tlf_trans_codigo_concepto) END AS cod_cue_concepto
  ,CASE   WHEN tlf_trans_codigo_concepto='1'  THEN 'Alquileres'
		  WHEN tlf_trans_codigo_concepto='2'  THEN 'Cuota'
		  WHEN tlf_trans_codigo_concepto='3'  THEN 'Expensas'
		  WHEN tlf_trans_codigo_concepto='4'  THEN 'Factura'
		  WHEN tlf_trans_codigo_concepto='5'  THEN 'Prestamo'
		  WHEN tlf_trans_codigo_concepto='6'  THEN 'Seguro'
		  WHEN tlf_trans_codigo_concepto='7'  THEN 'Honorarios'
		  WHEN tlf_trans_codigo_concepto='8'  THEN 'Varios'
		  WHEN tlf_trans_codigo_concepto='9'  THEN 'Haberes'
		  WHEN tlf_trans_codigo_concepto='A'  THEN 'Operaciones Inmobiliarias'
		  WHEN tlf_trans_codigo_concepto='B'  THEN 'Operaciones Inmobiliarias Habitualista'
		  WHEN tlf_trans_codigo_concepto='C'  THEN 'Bienes Registrables Habitualistas'
		  WHEN tlf_trans_codigo_concepto='D'  THEN 'Bienes Registrables No Habitualistas'
		  WHEN tlf_trans_codigo_concepto='E'  THEN 'Suscripción Obligaciones Negociables'
		  WHEN tlf_trans_codigo_concepto='F'  THEN 'Reintegros de Obras Sociales y Prepagas'
		  WHEN tlf_trans_codigo_concepto='G'  THEN 'Siniestros de Seguros'
		  WHEN tlf_trans_codigo_concepto='H'  THEN 'Aportes de Capital'
		  WHEN tlf_trans_codigo_concepto='I'  THEN 'Estado Expropiaciones u Otros'
		  WHEN tlf_trans_codigo_concepto='J'  THEN 'Compra PEI'
		  WHEN tlf_trans_codigo_concepto='K'  THEN 'Pago PEI'
		  WHEN tlf_trans_codigo_concepto='L'  THEN 'Devolución PEI'
   ELSE null END AS ds_cue_concepto
   ,CASE WHEN LENGTH(trim(wabaetlf_cre.tlf_trans_referencia))=0 THEN NULL ELSE trim(wabaetlf_cre.tlf_trans_referencia) END AS ds_cue_trans_referencia
   ,CASE WHEN LENGTH(trim(wabaetlf_cre.tlf_semnro_cargo))=0 THEN NULL ELSE trim(wabaetlf_cre.tlf_semnro_cargo) END AS cod_cue_num_cargo
   ,CASE WHEN LENGTH(trim(wabaetlf_cre.tlf_porc_devl_clte))=0 THEN NULL ELSE trim(wabaetlf_cre.tlf_porc_devl_clte) END AS fc_cue_porcen_devl_clte
   ,CASE WHEN LENGTH(trim(wabaetlf_cre.tlf_porc_devl_comer))=0 THEN NULL ELSE trim(wabaetlf_cre.tlf_porc_devl_comer) END AS fc_cue_porcen_devl_comer
  ,tlf_rubro as cod_cue_rubros
  ,tlf_balanceo as cod_cue_balanceo
  ,CASE WHEN LENGTH(trim(wabaetlf_cre.tlf_tipo_cajero))=0 THEN NULL ELSE trim(wabaetlf_cre.tlf_tipo_cajero) END AS cod_cue_tipo_cajero
  ,stk_debi.cod_per_nup_tarjeta
  ,stk_debi.cod_per_nup_titular
  ,wabaetlf_cre.partition_date
FROM   bi_corp_staging.abae_wabaetlf_cre wabaetlf_cre
left join bi_corp_common.dim_cue_tipo_movimiento_abae mov
on wabaetlf_cre.tlf_semtcode=mov.cod_cue_tipo_movimiento
LEFT JOIN  bi_corp_common.stk_cue_tarjetas_debito stk_debi
ON cast(wabaetlf_cre.tlf_semcacct as bigint) = stk_debi.cod_cue_tarjeta
AND stk_debi.partition_date = '{{ ti.xcom_pull(task_ids='InputConfig', key='max_partition_stk_cue_tarjetas_debito', dag_id='LOAD_CMN_Cuentas_Abae') }}'
LEFT JOIN abae_wabaetlf_cre_reversos wabaetlf_cre_reversos
ON 	wabaetlf_cre.tlf_semcacct=wabaetlf_cre_reversos.tlf_semcacct
AND	wabaetlf_cre.tlf_semtrdat=wabaetlf_cre_reversos.tlf_semtrdat
AND	wabaetlf_cre.tlf_semtrtim=wabaetlf_cre_reversos.tlf_semtrtim
AND	wabaetlf_cre.tlf_semtrenu=wabaetlf_cre_reversos.tlf_semtrenu
AND	wabaetlf_cre.tlf_semtatmi=wabaetlf_cre_reversos.tlf_semtatmi
LEFT JOIN pais pais
on trim(wabaetlf_cre.tlf_semterm_country)=pais.cod_cue_pais
WHERE wabaetlf_cre.partition_date='{{ ti.xcom_pull(task_ids='InputConfig', key='partition_date', dag_id='LOAD_CMN_Cuentas_Abae') }}'
	and wabaetlf_cre_reversos.tlf_semtrenu is null -- Saco los reversos

union all

SELECT
  'TLF_VISA' as cod_cue_origen
  ,case when length(cast(tlf_visa.tlf_semtrtim as string))=6
			then concat(
						  CASE
				          WHEN LENGTH(CAST(tlf_visa.tlf_semtrdat AS string)) = 3 THEN from_unixtime(unix_timestamp(concat(substring(tlf_visa.partition_date, 1, 4),'0',CAST(tlf_visa.tlf_semtrdat AS string)), 'yyyyMMdd'),'yyyy-MM-dd')
				          WHEN length(CAST(tlf_visa.tlf_semtrdat AS string)) = 4 THEN from_unixtime(unix_timestamp(concat(substring(tlf_visa.partition_date, 1, 4),CAST(tlf_visa.tlf_semtrdat AS string)), 'yyyyMMdd'),'yyyy-MM-dd')
				          ELSE NULL
				          END,' ',substring(cast(tlf_visa.tlf_semtrtim as string),1,2),':',substring(cast(tlf_visa.tlf_semtrtim as string),3,2),':',substring(cast(tlf_visa.tlf_semtrtim as string),5,2)
			            )
             else concat(
						  CASE
				          WHEN LENGTH(CAST(tlf_visa.tlf_semtrdat AS string)) = 3 THEN from_unixtime(unix_timestamp(concat(substring(tlf_visa.partition_date, 1, 4),'0',CAST(tlf_visa.tlf_semtrdat AS string)), 'yyyyMMdd'),'yyyy-MM-dd')
				          WHEN length(CAST(tlf_visa.tlf_semtrdat AS string)) = 4 THEN from_unixtime(unix_timestamp(concat(substring(tlf_visa.partition_date, 1, 4),CAST(tlf_visa.tlf_semtrdat AS string)), 'yyyyMMdd'),'yyyy-MM-dd')
				          ELSE NULL
				          END,' 0',substring(cast(tlf_visa.tlf_semtrtim as string),1,1),':',substring(cast(tlf_visa.tlf_semtrtim as string),2,2),':',substring(cast(tlf_visa.tlf_semtrtim as string),4,2)
			            )
             end as ts_cue_transaccion
  ,CASE
          WHEN LENGTH(CAST(tlf_visa.tlf_semtrdat AS string)) = 3 THEN from_unixtime(unix_timestamp(concat(substring(tlf_visa.partition_date, 1, 4),'0',CAST(tlf_visa.tlf_semtrdat AS string)), 'yyyyMMdd'),'yyyy-MM-dd')
          WHEN length(CAST(tlf_visa.tlf_semtrdat AS string)) = 4 THEN from_unixtime(unix_timestamp(concat(substring(tlf_visa.partition_date, 1, 4),CAST(tlf_visa.tlf_semtrdat AS string)), 'yyyyMMdd'),'yyyy-MM-dd')
          ELSE NULL
          END  AS dt_cue_transaccion
  ,CASE
          WHEN LENGTH(CAST(tlf_visa.tlf_sempdat AS string)) = 3 THEN from_unixtime(unix_timestamp(concat(substring(tlf_visa.partition_date, 1, 4),'0',CAST(tlf_visa.tlf_sempdat AS string)), 'yyyyMMdd'),'yyyy-MM-dd')
          WHEN length(CAST(tlf_visa.tlf_sempdat AS string)) = 4 THEN from_unixtime(unix_timestamp(concat(substring(tlf_visa.partition_date, 1, 4),CAST(tlf_visa.tlf_sempdat AS string)), 'yyyyMMdd'),'yyyy-MM-dd')
          ELSE NULL
          END  AS dt_cue_banco
  ,tlf_visa.tlf_stat AS cod_cue_estado
  ,CASE WHEN tlf_visa.tlf_stat=0 THEN 'OK'
		  WHEN tlf_visa.tlf_stat=11 THEN 'Destino no Disponible'
		  WHEN tlf_visa.tlf_stat=12 THEN 'Línea no Disponible'
		  WHEN tlf_visa.tlf_stat=20 THEN 'Originador no Disponible'
		  WHEN tlf_visa.tlf_stat=21 THEN 'Tipo de Mensaje Desconocido'
		  WHEN tlf_visa.tlf_stat=22 THEN 'Número de Tarjeta Desconocido'
		  ELSE NULL END AS ds_cue_estado
  ,case WHEN tlf_visa.tlf_product_ind='00' THEN 'ATM' else null end ds_cue_receptor_producto
  ,tlf_visa.tlf_release_number AS cod_cue_version_dpc
  ,tlf_visa.tlf_dpc_number AS ds_cue_num_dpc
  ,CASE WHEN trim(tlf_visa.tlf_1er_uso)='P' THEN 1 ELSE 0 end   flag_cue_primer_uso
  ,tlf_visa.tlf_semsgtyp AS cod_cue_tipo_mensaje
  ,CASE WHEN tlf_visa.tlf_semsgtyp=0210 THEN 'Respuesta de Autorización (Aprobación o Denegación)'
	      WHEN tlf_visa.tlf_semsgtyp=0215 THEN 'Transacción de Consulta de Últimos Movimientos, de Consulta de Últimos Aportes y de Consulta de Servicios por Vencer.'
	      WHEN tlf_visa.tlf_semsgtyp=0220 THEN 'Aviso de Autorización de Transacción'
	      WHEN tlf_visa.tlf_semsgtyp=0400 THEN 'Reverso Contable de Operación Aprobada'
	      WHEN tlf_visa.tlf_semsgtyp=0420 THEN 'Aviso de Reverso de Transacción'
	      ELSE NULL
  END ds_cue_tipo_mensaje
  ,CASE WHEN LENGTH(trim(tlf_visa.tlf_semcacct))=0 THEN NULL ELSE trim(tlf_visa.tlf_semcacct) END AS cod_cue_tarjeta
  ,tlf_visa.tlf_semtcode AS cod_cue_tipo_movimiento
  ,mov.ds_cue_tipo_movimiento
  ,case when tlf_visa.tlf_semtfacc in (1,2,11,12,13,20,31,32) then tlf_visa.tlf_semtfacc else null end AS  cod_cue_tipo_cuenta_desde
  ,CASE WHEN tlf_visa.tlf_semtfacc=1 THEN 'Cuenta corriente en pesos'
		  WHEN tlf_visa.tlf_semtfacc=2 THEN 'Cuenta corriente en dólares'
		  WHEN tlf_visa.tlf_semtfacc=11 THEN 'Caja de ahorros en pesos'
		  WHEN tlf_visa.tlf_semtfacc=12 THEN 'Caja de ahorros en dólares'
		  WHEN tlf_visa.tlf_semtfacc=13 THEN 'Cuenta especial'
		  WHEN tlf_visa.tlf_semtfacc=20 THEN 'Cuenta de CBU'
		  WHEN tlf_visa.tlf_semtfacc=31 THEN 'Cuenta de crédito en pesos'
		  WHEN tlf_visa.tlf_semtfacc=32 THEN 'Cuenta de crédito en dólares'
		  ELSE NULL
  END AS  ds_cue_tipo_cuenta_desde
  ,case when tlf_visa.tlf_semttacc in (1,2,11,12,13,20,31,32) then tlf_visa.tlf_semttacc else null end AS  cod_cue_tipo_cuenta_hasta
  ,CASE WHEN tlf_visa.tlf_semttacc=1 THEN 'Cuenta corriente en pesos'
	  WHEN tlf_visa.tlf_semttacc=2 THEN 'Cuenta corriente en dólares'
	  WHEN tlf_visa.tlf_semttacc=11 THEN 'Caja de ahorros en pesos'
	  WHEN tlf_visa.tlf_semttacc=12 THEN 'Caja de ahorros en dólares'
	  WHEN tlf_visa.tlf_semttacc=13 THEN 'Cuenta especial'
	  WHEN tlf_visa.tlf_semttacc=20 THEN 'Cuenta de CBU'
	  WHEN tlf_visa.tlf_semttacc=31 THEN 'Cuenta de crédito en pesos'
	  WHEN tlf_visa.tlf_semttacc=32 THEN 'Cuenta de crédito en dólares'
	  ELSE NULL
  END AS  ds_cue_tipo_cuenta_hasta
  ,tlf_visa.tlf_semamt1 AS fc_cue_importe_cuenta
  ,tlf_visa.tlf_semamt2 AS fc_cue_importe_impuesto
  ,CASE WHEN tlf_visa.tlf_semamt3>0 AND trim(tlf_visa.tlf_semccode) = '840' THEN tlf_visa.tlf_semamt3 ELSE NULL end  AS fc_cue_importe_compra_usd
  ,CASE WHEN LENGTH(trim(tlf_visa.tlf_fiid))=0 OR trim(tlf_visa.tlf_fiid)='0000' THEN NULL ELSE trim(tlf_visa.tlf_fiid) END AS cod_cue_comercio_camp
  ,CASE WHEN tlf_visa.tlf_auth_code=0 THEN NULL ELSE tlf_visa.tlf_auth_code END AS cod_cue_autorizacion
  ,CASE WHEN LENGTH(trim(tlf_visa.tlf_sys_id))=0 THEN NULL ELSE trim(tlf_visa.tlf_sys_id) END AS cod_cue_tipo_tarjeta_banelco
  ,CASE WHEN LENGTH(trim(tlf_visa.tlf_sys_id))=0 OR trim(tlf_visa.tlf_fiid) IS NULL THEN 0 ELSE 1 END AS flag_cue_tarjeta_banelco
  ,CASE WHEN tlf_visa.tlf_trace_no=0 THEN NULL ELSE tlf_visa.tlf_trace_no END AS cod_cue_trace_host
  ,CASE WHEN LENGTH(trim(tlf_visa.tlf_semcod_term))=0 THEN NULL ELSE trim(tlf_visa.tlf_semcod_term) END AS cod_cue_terminal
  ,CASE WHEN LENGTH(trim(tlf_visa.tlf_semcod_tarj))=0 THEN NULL ELSE trim(tlf_visa.tlf_semcod_tarj) END AS id_cue_entidad_tarjeta
  ,CASE WHEN LENGTH(trim(tlf_visa.tlf_semccode))=0 THEN NULL ELSE trim(tlf_visa.tlf_semccode) END AS cod_cue_moneda
  ,CASE WHEN trim(tlf_visa.tlf_semccode)='032' THEN 'Pesos'
		WHEN trim(tlf_visa.tlf_semccode)='840' THEN 'Dólares'
  	   	WHEN trim(tlf_visa.tlf_semccode)='858' THEN 'Pesos Uruguayos'
		ELSE NULL
  END AS ds_cue_moneda
  ,tlf_visa.tlf_semrrev AS cod_cue_reverso
  ,CASE WHEN tlf_visa.tlf_semrrev=1 THEN 'Tiempo para completar la autorización excedida'
		  WHEN tlf_visa.tlf_semrrev=2 THEN 'Respuesta rechazada por la terminal'
		  WHEN tlf_visa.tlf_semrrev=3 THEN 'Destino (terminal) no disponible'
		  WHEN tlf_visa.tlf_semrrev=8 THEN 'Cancelación del cliente'
		  WHEN tlf_visa.tlf_semrrev=10 THEN 'Error de hardware. Incluye a los reversos parciales'
		  WHEN tlf_visa.tlf_semrrev=20 THEN 'Transacción sospechosa. No se recibió la confirmación'
        ELSE NULL END ds_cue_reverso
  ,CASE WHEN LENGTH(trim(tlf_visa.tlf_id_canal))=0 THEN NULL ELSE trim(tlf_visa.tlf_id_canal) END AS cod_cue_canal
  ,NULL AS ds_cue_canal
  ,CASE WHEN trim(tlf_visa.tlf_marca_iva)='1' THEN 1 ELSE 0 end AS flag_cue_consumidor_final
  ,trim(tlf_visa.tlf_semrcard) AS cod_cue_semrcard
  ,trim(tlf_visa.tlf_semtrnad) AS cod_cue_semtrnad
  ,CASE WHEN trim(tlf_visa.tlf_semrcard) in ('0','5','7') and trim(tlf_visa.tlf_semtrnad) in ('00','01') then 1 else 0 end  as flag_cue_operacion_aceptada
  ,CASE WHEN LENGTH(trim(tlf_visa.tlf_semcity))=0 THEN NULL ELSE trim(tlf_visa.tlf_semcity) END ds_cue_terminal_ciudad
  ,case WHEN cast(tlf_visa.tlf_t72_dni_cuil as int)=0 or LENGTH(trim(tlf_visa.tlf_t72_dni_cuil))=0 THEN null else tlf_t72_dni_cuil END AS cod_cue_dni_cuil_cta_destino
  ,tlf_visa.tlf_t72_comision AS fc_cue_comision
  ,CASE WHEN LENGTH(trim(tlf_visa.tlf_seg_tipo_mov))=0 THEN NULL ELSE trim(tlf_visa.tlf_seg_tipo_mov) END AS cod_cue_seg_tipo_mov
  ,CASE WHEN LENGTH(trim(tlf_visa.tlf_seg_tipo_seguro))=0 THEN NULL ELSE trim(tlf_visa.tlf_seg_tipo_seguro) END AS cod_cue_seg_tipo_seguro
  ,CASE WHEN LENGTH(trim(tlf_visa.tlf_seg_identificacion))=0 THEN NULL ELSE trim(tlf_visa.tlf_seg_identificacion) END AS cod_cue_seg_identificacion
  ,tlf_visa.tlf_seg_costo_cobertura AS fc_cue_seg_costo_cobertura
  ,CASE WHEN LENGTH(trim(tlf_visa.tlf_sor_banelco))=0 THEN NULL ELSE trim(tlf_visa.tlf_sor_banelco) END AS cod_cue_sor_banelco
  ,tlf_visa.tlf_sor_banelco_importe AS fc_cue_sor_banelco_importe
  ,CASE WHEN LENGTH(trim(tlf_visa.tlf_sor_bancos))=0 THEN NULL ELSE trim(tlf_visa.tlf_sor_bancos) END AS cod_cue_sor_bancos
  ,tlf_visa.tlf_sor_bancos_importe AS fc_cue_sor_bancos_importe
  ,tlf_visa.tlf_semfalen AS cod_cue_longitud_cuenta_desde
  ,tlf_visa.tlf_semfanum_deb AS fc_cue_debito_desde
  ,tlf_visa.tlf_semfanum_cred AS fc_cue_credito_desde
  ,tlf_visa.tlf_semfanum_suc AS cod_suc_sucursal
  ,tlf_visa.tlf_semfanum_cta AS cod_cue_cuenta_desde
  ,tlf_visa.tlf_semtalen AS cod_cue_longitud_cuenta_hasta
  ,tlf_visa.tlf_semtanum_deb AS fc_cue_debito_hasta
  ,tlf_visa.tlf_semtanum_cred AS fc_cue_credito_hasta
  ,tlf_visa.tlf_semtanum_suc AS cod_suc_sucursal_hasta
  ,tlf_visa.tlf_semtanum_cta AS cod_cue_cuenta_hasta
  ,CASE WHEN trim(tlf_visa.tlf_semterm_country)='00' OR LENGTH(trim(tlf_visa.tlf_semterm_country))=0  THEN NULL ELSE trim(tlf_visa.tlf_semterm_country) END AS cod_cue_terminal_pais
  ,pais.ds_cue_pais AS ds_cue_terminal_pais
  ,CASE WHEN LENGTH(trim(tlf_visa.tlf_sembanc))=0 THEN NULL ELSE trim(tlf_visa.tlf_sembanc) END cod_cue_tarjeta_banco
  ,CASE WHEN LENGTH(trim(tlf_visa.tlf_semfiid))=0 THEN NULL ELSE trim(tlf_visa.tlf_semfiid) END cod_cue_terminal_banco
  ,CASE WHEN LENGTH(trim(tlf_visa.tlf_cbu_destino))=0 THEN NULL ELSE trim(tlf_visa.tlf_cbu_destino) END cod_cbu_destino
  ,CASE WHEN LENGTH(trim(tlf_visa.tlf_titularidad))=0 THEN NULL ELSE trim(tlf_visa.tlf_titularidad) END cod_cue_titularidad
  ,case WHEN tlf_titularidad='1' THEN 'Cta Cte propia'
  		WHEN tlf_titularidad='2' THEN 'Cta Cte no propia'
  		WHEN tlf_titularidad='3' THEN 'Otra cta propia'
 		WHEN tlf_titularidad='4' THEN 'Otra cta no propia'
 		else null end AS ds_cue_titularidad
  ,CASE WHEN LENGTH(trim(tlf_visa.tlf_tipo_oper))=0 THEN NULL ELSE trim(tlf_visa.tlf_tipo_oper) END AS cod_cue_tipo_oper
  ,CASE WHEN LENGTH(trim(tlf_visa.tlf_mand_doc_benef))=0 THEN NULL ELSE trim(tlf_visa.tlf_mand_doc_benef) END AS cod_cue_doc_benef
  ,CASE WHEN LENGTH(trim(tlf_visa.tlf_mand_num_benef))=0 THEN NULL ELSE trim(tlf_visa.tlf_mand_num_benef) END AS ds_cue_num_benef
  ,CASE WHEN LENGTH(trim(tlf_visa.tlf_mand_identif))=0 THEN NULL ELSE trim(tlf_visa.tlf_mand_identif) END AS cod_cue_mand_identif
  ,CASE WHEN LENGTH(trim(tlf_visa.tlf_semtcomer))=0 THEN NULL ELSE trim(tlf_visa.tlf_semtcomer) END cod_cue_comercio
  ,CASE WHEN LENGTH(trim(tlf_visa.tlf_semtatmi))=0 THEN NULL ELSE trim(tlf_visa.tlf_semtatmi) END cod_cue_atm
  ,tlf_visa.tlf_semtrenu AS cod_cue_comprobante
  ,tlf_sembanc_1 AS fc_cue_sembanc_1
  ,tlf_porcen_retencion  AS fc_cue_porcen_rentencion
  ,tlf_porcen_percepcion AS fc_cue_porcen_percepcion
  ,tlf_retencion_imp_pais  AS fc_cue_retencion_imp_pais
  ,case when tlf_tipo_extr='0' then null else tlf_tipo_extr end AS cod_cue_tipo_extraccion
  ,CASE WHEN LENGTH(trim(tlf_visa.tlf_semtidep))=0 THEN NULL ELSE trim(tlf_visa.tlf_semtidep) END AS cod_cue_tipo_deposito
  ,case when tlf_semtideb='0' then null else tlf_semtideb end AS cod_cue_tipo_debito
  ,case when tlf_semtipag='0' then null else tlf_semtipag end AS cod_cue_tipo_pago
  ,CASE WHEN LENGTH(trim(tlf_visa.tlf_semtitar))=0 THEN NULL ELSE trim(tlf_visa.tlf_semtitar) END AS cod_cue_tipo_tarjeta
  ,case WHEN trim(tlf_semtitar)='A' THEN 'Administrativa'
  		WHEN trim(tlf_semtitar)='B' THEN 'Restringida a Depositos'
  		WHEN trim(tlf_semtitar)='F' THEN 'Tarjeta con Cuenta Credito'
  		WHEN trim(tlf_semtitar)='G' THEN 'Baneldo Débito Adicional'
  		WHEN trim(tlf_semtitar)='J' THEN 'AFJP'
  		WHEN trim(tlf_semtitar)='D' THEN 'Diners Club'
  		WHEN trim(tlf_semtitar)='P' THEN 'Banelco Débito Titular'
  		WHEN trim(tlf_semtitar)='E' THEN 'Electron'
  		WHEN trim(tlf_semtitar)='M' THEN 'Mini Electron'
  		WHEN trim(tlf_semtitar)='I' THEN 'Electron Adicional'
  		WHEN trim(tlf_semtitar)='V' THEN 'Visa'
   else null end AS ds_cue_tipo_tarjeta
  ,trim(tlf_visa.tlf_nombre_origen) AS ds_cue_origen
  ,tlf_tip_debin AS cod_cue_tipo_debin
  ,CASE WHEN trim(tlf_visa.tlf_id_debin)='0000        000000' OR LENGTH(trim(tlf_visa.tlf_id_debin))=0  THEN NULL ELSE trim(tlf_visa.tlf_id_debin) END AS cod_cue_debin
  ,CASE WHEN LENGTH(trim(tlf_visa.tlf_cuit_bco_cdor))=0 THEN NULL ELSE trim(tlf_visa.tlf_cuit_bco_cdor) END AS cod_cue_cuit_bco_cdor
  ,CASE WHEN LENGTH(trim(tlf_visa.tlf_cbu_bco_cdor))=0 THEN NULL ELSE trim(tlf_visa.tlf_cbu_bco_cdor) END AS cod_cue_cbu_bco_cdor
  ,trim(tlf_visa.tlf_cuit_bco_vdor) AS cod_cue_cuit_bco_vdor
  ,CASE WHEN LENGTH(trim(tlf_visa.tlf_cbu_bco_vdor))=0 THEN NULL ELSE trim(tlf_visa.tlf_cbu_bco_vdor) END AS cod_cue_cbu_bco_vdor
  ,CASE
          WHEN LENGTH(CAST(tlf_visa.tlf_fec_neg_coel AS string)) = 3 THEN concat(substring(tlf_visa.partition_date, 1, 4),'0',CAST(tlf_visa.tlf_fec_neg_coel AS string))
          WHEN length(CAST(tlf_visa.tlf_fec_neg_coel AS string)) = 4 THEN concat(substring(tlf_visa.partition_date, 1, 4),CAST(tlf_visa.tlf_fec_neg_coel AS string))
          ELSE NULL
     END  AS dt_cue_neg_coel
  ,tlf_visa.tlf_scoring_debin AS cod_cue_debin_scoring
  ,CASE WHEN tlf_visa.tlf_cpto_debin=0 THEN NULL ELSE tlf_visa.tlf_cpto_debin END AS cod_cue_debin_cpto
  ,trim(tlf_visa.tlf_desc_cpto_deb) AS ds_cue_cpto_deb
  ,CASE WHEN LENGTH(trim(tlf_visa.tlf_corresp_titu))=0 THEN NULL ELSE trim(tlf_visa.tlf_corresp_titu) END AS cod_cue_corresp_titu
  ,tlf_moneda_origen AS cod_cue_moneda_origen
  ,CASE WHEN trim(tlf_visa.tlf_moneda_origen)='032' THEN 'Pesos'
		WHEN trim(tlf_visa.tlf_moneda_origen)='840' THEN 'Dólares'
  	   	WHEN trim(tlf_visa.tlf_moneda_origen)='858' THEN 'Pesos Uruguayos'
		ELSE NULL
   END AS ds_cue_moneda_origen
  ,tlf_semimpo_original AS fc_cue_importe_original
  ,tlf_semfefa AS ds_cue_fefa
  ,tlf_semcotco AS cod_cue_cotco
  ,tlf_semcotve AS cod_cue_cotve
  ,tlf_semamt5 AS fc_cue_semant5
  ,tlf_percepcion_imp_ganan AS fc_cue_imp_ganan
  ,tlf_nro_control AS ds_cue_num_control
  ,CASE WHEN LENGTH(trim(tlf_visa.tlf_trans_banco_receptor))=0 THEN NULL ELSE trim(tlf_visa.tlf_trans_banco_receptor) END AS ds_cue_banco_receptor
  ,CASE WHEN LENGTH(trim(tlf_visa.tlf_trans_codigo_concepto))=0 THEN NULL ELSE trim(tlf_visa.tlf_trans_codigo_concepto) END AS cod_cue_concepto
  ,CASE   WHEN tlf_trans_codigo_concepto='1'  THEN 'Alquileres'
		  WHEN tlf_trans_codigo_concepto='2'  THEN 'Cuota'
		  WHEN tlf_trans_codigo_concepto='3'  THEN 'Expensas'
		  WHEN tlf_trans_codigo_concepto='4'  THEN 'Factura'
		  WHEN tlf_trans_codigo_concepto='5'  THEN 'Prestamo'
		  WHEN tlf_trans_codigo_concepto='6'  THEN 'Seguro'
		  WHEN tlf_trans_codigo_concepto='7'  THEN 'Honorarios'
		  WHEN tlf_trans_codigo_concepto='8'  THEN 'Varios'
		  WHEN tlf_trans_codigo_concepto='9'  THEN 'Haberes'
		  WHEN tlf_trans_codigo_concepto='A'  THEN 'Operaciones Inmobiliarias'
		  WHEN tlf_trans_codigo_concepto='B'  THEN 'Operaciones Inmobiliarias Habitualista'
		  WHEN tlf_trans_codigo_concepto='C'  THEN 'Bienes Registrables Habitualistas'
		  WHEN tlf_trans_codigo_concepto='D'  THEN 'Bienes Registrables No Habitualistas'
		  WHEN tlf_trans_codigo_concepto='E'  THEN 'Suscripción Obligaciones Negociables'
		  WHEN tlf_trans_codigo_concepto='F'  THEN 'Reintegros de Obras Sociales y Prepagas'
		  WHEN tlf_trans_codigo_concepto='G'  THEN 'Siniestros de Seguros'
		  WHEN tlf_trans_codigo_concepto='H'  THEN 'Aportes de Capital'
		  WHEN tlf_trans_codigo_concepto='I'  THEN 'Estado Expropiaciones u Otros'
		  WHEN tlf_trans_codigo_concepto='J'  THEN 'Compra PEI'
		  WHEN tlf_trans_codigo_concepto='K'  THEN 'Pago PEI'
		  WHEN tlf_trans_codigo_concepto='L'  THEN 'Devolución PEI'
   ELSE null END AS ds_cue_concepto
   ,CASE WHEN LENGTH(trim(tlf_visa.tlf_trans_referencia))=0 THEN NULL ELSE trim(tlf_visa.tlf_trans_referencia) END AS ds_cue_trans_referencia
   ,CASE WHEN LENGTH(trim(tlf_visa.tlf_semnro_cargo))=0 THEN NULL ELSE trim(tlf_visa.tlf_semnro_cargo) END AS cod_cue_num_cargo
   ,CASE WHEN LENGTH(trim(tlf_visa.tlf_porc_devl_clte))=0 THEN NULL ELSE trim(tlf_visa.tlf_porc_devl_clte) END AS fc_cue_porcen_devl_clte
   ,CASE WHEN LENGTH(trim(tlf_visa.tlf_porc_devl_comer))=0 THEN NULL ELSE trim(tlf_visa.tlf_porc_devl_comer) END AS fc_cue_porcen_devl_comer
  ,tlf_rubro as cod_cue_rubros
  ,tlf_balanceo as cod_cue_balanceo
  ,CASE WHEN LENGTH(trim(tlf_visa.tlf_tipo_cajero))=0 THEN NULL ELSE trim(tlf_visa.tlf_tipo_cajero) END AS cod_cue_tipo_cajero
  ,stk_debi.cod_per_nup_tarjeta
  ,stk_debi.cod_per_nup_titular
  ,tlf_visa.partition_date
FROM   bi_corp_staging.abae_tlf_visa tlf_visa
left join bi_corp_common.dim_cue_tipo_movimiento_abae mov
on tlf_visa.tlf_semtcode=mov.cod_cue_tipo_movimiento
LEFT JOIN  bi_corp_common.stk_cue_tarjetas_debito stk_debi
ON cast(tlf_visa.tlf_semcacct as bigint) = stk_debi.cod_cue_tarjeta
AND stk_debi.partition_date = '{{ ti.xcom_pull(task_ids='InputConfig', key='max_partition_stk_cue_tarjetas_debito', dag_id='LOAD_CMN_Cuentas_Abae') }}'
LEFT JOIN tlf_visa_reversos tlf_visa_reversos
ON 	tlf_visa.tlf_semcacct=tlf_visa_reversos.tlf_semcacct
AND	tlf_visa.tlf_semtrdat=tlf_visa_reversos.tlf_semtrdat
AND	tlf_visa.tlf_semtrtim=tlf_visa_reversos.tlf_semtrtim
AND	tlf_visa.tlf_semtrenu=tlf_visa_reversos.tlf_semtrenu
AND	tlf_visa.tlf_semtatmi=tlf_visa_reversos.tlf_semtatmi
LEFT JOIN pais pais
on trim(tlf_visa.tlf_semterm_country)=pais.cod_cue_pais
WHERE tlf_visa.partition_date='{{ ti.xcom_pull(task_ids='InputConfig', key='partition_date', dag_id='LOAD_CMN_Cuentas_Abae') }}'
	and tlf_visa_reversos.tlf_semtrenu is null -- Saco los reversos

UNION ALL

SELECT
  'TLX' as cod_cue_origen
  ,case when length(cast(wabaetlx.tlf_semtrtim as string))=6
			then concat(
						  CASE
				          WHEN LENGTH(CAST(wabaetlx.tlf_semtrdat AS string)) = 3 THEN from_unixtime(unix_timestamp(concat(substring(wabaetlx.partition_date, 1, 4),'0',CAST(wabaetlx.tlf_semtrdat AS string)), 'yyyyMMdd'),'yyyy-MM-dd')
				          WHEN length(CAST(wabaetlx.tlf_semtrdat AS string)) = 4 THEN from_unixtime(unix_timestamp(concat(substring(wabaetlx.partition_date, 1, 4),CAST(wabaetlx.tlf_semtrdat AS string)), 'yyyyMMdd'),'yyyy-MM-dd')
				          ELSE NULL
				          END,' ',substring(cast(wabaetlx.tlf_semtrtim as string),1,2),':',substring(cast(wabaetlx.tlf_semtrtim as string),3,2),':',substring(cast(wabaetlx.tlf_semtrtim as string),5,2)
			            )
             else concat(
						  CASE
				          WHEN LENGTH(CAST(wabaetlx.tlf_semtrdat AS string)) = 3 THEN from_unixtime(unix_timestamp(concat(substring(wabaetlx.partition_date, 1, 4),'0',CAST(wabaetlx.tlf_semtrdat AS string)), 'yyyyMMdd'),'yyyy-MM-dd')
				          WHEN length(CAST(wabaetlx.tlf_semtrdat AS string)) = 4 THEN from_unixtime(unix_timestamp(concat(substring(wabaetlx.partition_date, 1, 4),CAST(wabaetlx.tlf_semtrdat AS string)), 'yyyyMMdd'),'yyyy-MM-dd')
				          ELSE NULL
				          END,' 0',substring(cast(wabaetlx.tlf_semtrtim as string),1,1),':',substring(cast(wabaetlx.tlf_semtrtim as string),2,2),':',substring(cast(wabaetlx.tlf_semtrtim as string),4,2)
			            )
             end as ts_cue_transaccion
  ,CASE
          WHEN LENGTH(CAST(wabaetlx.tlf_semtrdat AS string)) = 3 THEN from_unixtime(unix_timestamp(concat(substring(wabaetlx.partition_date, 1, 4),'0',CAST(wabaetlx.tlf_semtrdat AS string)), 'yyyyMMdd'),'yyyy-MM-dd')
          WHEN length(CAST(wabaetlx.tlf_semtrdat AS string)) = 4 THEN from_unixtime(unix_timestamp(concat(substring(wabaetlx.partition_date, 1, 4),CAST(wabaetlx.tlf_semtrdat AS string)), 'yyyyMMdd'),'yyyy-MM-dd')
          ELSE NULL
          END  AS dt_cue_transaccion
  ,CASE
          WHEN LENGTH(CAST(wabaetlx.tlf_sempdat AS string)) = 3 THEN from_unixtime(unix_timestamp(concat(substring(wabaetlx.partition_date, 1, 4),'0',CAST(wabaetlx.tlf_sempdat AS string)), 'yyyyMMdd'),'yyyy-MM-dd')
          WHEN length(CAST(wabaetlx.tlf_sempdat AS string)) = 4 THEN from_unixtime(unix_timestamp(concat(substring(wabaetlx.partition_date, 1, 4),CAST(wabaetlx.tlf_sempdat AS string)), 'yyyyMMdd'),'yyyy-MM-dd')
          ELSE NULL
          END  AS dt_cue_banco
  ,wabaetlx.tlf_stat AS cod_cue_estado
  ,CASE WHEN wabaetlx.tlf_stat=0 THEN 'OK'
		  WHEN wabaetlx.tlf_stat=11 THEN 'Destino no Disponible'
		  WHEN wabaetlx.tlf_stat=12 THEN 'Línea no Disponible'
		  WHEN wabaetlx.tlf_stat=20 THEN 'Originador no Disponible'
		  WHEN wabaetlx.tlf_stat=21 THEN 'Tipo de Mensaje Desconocido'
		  WHEN wabaetlx.tlf_stat=22 THEN 'Número de Tarjeta Desconocido'
		  ELSE NULL END AS ds_cue_estado
  ,case WHEN wabaetlx.tlf_product_ind='00' THEN 'ATM' else null end ds_cue_receptor_producto
  ,wabaetlx.tlf_release_number AS cod_cue_version_dpc
  ,wabaetlx.tlf_dpc_number AS ds_cue_num_dpc
  ,CASE WHEN trim(wabaetlx.tlf_1er_uso)='P' THEN 1 ELSE 0 end   flag_cue_primer_uso
  ,wabaetlx.tlf_semsgtyp AS cod_cue_tipo_mensaje
  ,CASE WHEN wabaetlx.tlf_semsgtyp=0210 THEN 'Respuesta de Autorización (Aprobación o Denegación)'
	      WHEN wabaetlx.tlf_semsgtyp=0215 THEN 'Transacción de Consulta de Últimos Movimientos, de Consulta de Últimos Aportes y de Consulta de Servicios por Vencer.'
	      WHEN wabaetlx.tlf_semsgtyp=0220 THEN 'Aviso de Autorización de Transacción'
	      WHEN wabaetlx.tlf_semsgtyp=0400 THEN 'Reverso Contable de Operación Aprobada'
	      WHEN wabaetlx.tlf_semsgtyp=0420 THEN 'Aviso de Reverso de Transacción'
	      ELSE NULL
  END ds_cue_tipo_mensaje
  ,CASE WHEN LENGTH(trim(wabaetlx.tlf_semcacct))=0 THEN NULL ELSE trim(wabaetlx.tlf_semcacct) END AS cod_cue_tarjeta
  ,wabaetlx.tlf_semtcode AS cod_cue_tipo_movimiento
  ,mov.ds_cue_tipo_movimiento
  ,case when wabaetlx.tlf_semtfacc in (1,2,11,12,13,20,31,32) then wabaetlx.tlf_semtfacc else null end AS  cod_cue_tipo_cuenta_desde
  ,CASE WHEN wabaetlx.tlf_semtfacc=1 THEN 'Cuenta corriente en pesos'
		  WHEN wabaetlx.tlf_semtfacc=2 THEN 'Cuenta corriente en dólares'
		  WHEN wabaetlx.tlf_semtfacc=11 THEN 'Caja de ahorros en pesos'
		  WHEN wabaetlx.tlf_semtfacc=12 THEN 'Caja de ahorros en dólares'
		  WHEN wabaetlx.tlf_semtfacc=13 THEN 'Cuenta especial'
		  WHEN wabaetlx.tlf_semtfacc=20 THEN 'Cuenta de CBU'
		  WHEN wabaetlx.tlf_semtfacc=31 THEN 'Cuenta de crédito en pesos'
		  WHEN wabaetlx.tlf_semtfacc=32 THEN 'Cuenta de crédito en dólares'
		  ELSE NULL
  END AS  ds_cue_tipo_cuenta_desde
  ,case when wabaetlx.tlf_semttacc in (1,2,11,12,13,20,31,32) then wabaetlx.tlf_semttacc else null end AS  cod_cue_tipo_cuenta_hasta
  ,CASE WHEN wabaetlx.tlf_semttacc=1 THEN 'Cuenta corriente en pesos'
	  WHEN wabaetlx.tlf_semttacc=2 THEN 'Cuenta corriente en dólares'
	  WHEN wabaetlx.tlf_semttacc=11 THEN 'Caja de ahorros en pesos'
	  WHEN wabaetlx.tlf_semttacc=12 THEN 'Caja de ahorros en dólares'
	  WHEN wabaetlx.tlf_semttacc=13 THEN 'Cuenta especial'
	  WHEN wabaetlx.tlf_semttacc=20 THEN 'Cuenta de CBU'
	  WHEN wabaetlx.tlf_semttacc=31 THEN 'Cuenta de crédito en pesos'
	  WHEN wabaetlx.tlf_semttacc=32 THEN 'Cuenta de crédito en dólares'
	  ELSE NULL
  END AS  ds_cue_tipo_cuenta_hasta
  ,wabaetlx.tlf_semamt1 AS fc_cue_importe_cuenta
  ,wabaetlx.tlf_semamt2 AS fc_cue_importe_impuesto
  ,CASE WHEN wabaetlx.tlf_semamt3>0 AND trim(wabaetlx.tlf_semccode) = '840' THEN wabaetlx.tlf_semamt3 ELSE NULL end  AS fc_cue_importe_compra_usd
  ,CASE WHEN LENGTH(trim(wabaetlx.tlf_fiid))=0 OR trim(wabaetlx.tlf_fiid)='0000' THEN NULL ELSE trim(wabaetlx.tlf_fiid) END AS cod_cue_comercio_camp
  ,CASE WHEN wabaetlx.tlf_auth_code=0 THEN NULL ELSE wabaetlx.tlf_auth_code END AS cod_cue_autorizacion
  ,CASE WHEN LENGTH(trim(wabaetlx.tlf_sys_id))=0 THEN NULL ELSE trim(wabaetlx.tlf_sys_id) END AS cod_cue_tipo_tarjeta_banelco
  ,CASE WHEN LENGTH(trim(wabaetlx.tlf_sys_id))=0 OR trim(wabaetlx.tlf_fiid) IS NULL THEN 0 ELSE 1 END AS flag_cue_tarjeta_banelco
  ,CASE WHEN wabaetlx.tlf_trace_no=0 THEN NULL ELSE wabaetlx.tlf_trace_no END AS cod_cue_trace_host
  ,CASE WHEN LENGTH(trim(wabaetlx.tlf_semcod_term))=0 THEN NULL ELSE trim(wabaetlx.tlf_semcod_term) END AS cod_cue_terminal
  ,CASE WHEN LENGTH(trim(wabaetlx.tlf_semcod_tarj))=0 THEN NULL ELSE trim(wabaetlx.tlf_semcod_tarj) END AS id_cue_entidad_tarjeta
  ,CASE WHEN LENGTH(trim(wabaetlx.tlf_semccode))=0 THEN NULL ELSE trim(wabaetlx.tlf_semccode) END AS cod_cue_moneda
  ,CASE WHEN trim(wabaetlx.tlf_semccode)='032' THEN 'Pesos'
		WHEN trim(wabaetlx.tlf_semccode)='840' THEN 'Dólares'
  	   	WHEN trim(wabaetlx.tlf_semccode)='858' THEN 'Pesos Uruguayos'
		ELSE NULL
  END AS ds_cue_moneda
  ,wabaetlx.tlf_semrrev AS cod_cue_reverso
  ,CASE WHEN wabaetlx.tlf_semrrev=1 THEN 'Tiempo para completar la autorización excedida'
		  WHEN wabaetlx.tlf_semrrev=2 THEN 'Respuesta rechazada por la terminal'
		  WHEN wabaetlx.tlf_semrrev=3 THEN 'Destino (terminal) no disponible'
		  WHEN wabaetlx.tlf_semrrev=8 THEN 'Cancelación del cliente'
		  WHEN wabaetlx.tlf_semrrev=10 THEN 'Error de hardware. Incluye a los reversos parciales'
		  WHEN wabaetlx.tlf_semrrev=20 THEN 'Transacción sospechosa. No se recibió la confirmación'
        ELSE NULL END ds_cue_reverso
  ,CASE WHEN LENGTH(trim(wabaetlx.tlf_id_canal))=0 THEN NULL ELSE trim(wabaetlx.tlf_id_canal) END AS cod_cue_canal
  ,NULL AS ds_cue_canal
  ,CASE WHEN trim(wabaetlx.tlf_marca_iva)='1' THEN 1 ELSE 0 end AS flag_cue_consumidor_final
  ,trim(wabaetlx.tlf_semrcard) AS cod_cue_semrcard
  ,trim(wabaetlx.tlf_semtrnad) AS cod_cue_semtrnad
  ,CASE WHEN trim(wabaetlx.tlf_semrcard) in ('0','5','7') and trim(wabaetlx.tlf_semtrnad) in ('00','01') then 1 else 0 end  as flag_cue_operacion_aceptada
  ,CASE WHEN LENGTH(trim(wabaetlx.tlf_semcity))=0 THEN NULL ELSE trim(wabaetlx.tlf_semcity) END ds_cue_terminal_ciudad
  ,case WHEN cast(wabaetlx.tlf_t72_dni_cuil as int)=0 or LENGTH(trim(wabaetlx.tlf_t72_dni_cuil))=0 THEN null else tlf_t72_dni_cuil END AS cod_cue_dni_cuil_cta_destino
  ,wabaetlx.tlf_t72_comision AS fc_cue_comision
  ,CASE WHEN LENGTH(trim(wabaetlx.tlf_seg_tipo_mov))=0 THEN NULL ELSE trim(wabaetlx.tlf_seg_tipo_mov) END AS cod_cue_seg_tipo_mov
  ,CASE WHEN LENGTH(trim(wabaetlx.tlf_seg_tipo_seguro))=0 THEN NULL ELSE trim(wabaetlx.tlf_seg_tipo_seguro) END AS cod_cue_seg_tipo_seguro
  ,CASE WHEN LENGTH(trim(wabaetlx.tlf_seg_identificacion))=0 THEN NULL ELSE trim(wabaetlx.tlf_seg_identificacion) END AS cod_cue_seg_identificacion
  ,wabaetlx.tlf_seg_costo_cobertura AS fc_cue_seg_costo_cobertura
  ,CASE WHEN LENGTH(trim(wabaetlx.tlf_sor_banelco))=0 THEN NULL ELSE trim(wabaetlx.tlf_sor_banelco) END AS cod_cue_sor_banelco
  ,wabaetlx.tlf_sor_banelco_importe AS fc_cue_sor_banelco_importe
  ,CASE WHEN LENGTH(trim(wabaetlx.tlf_sor_bancos))=0 THEN NULL ELSE trim(wabaetlx.tlf_sor_bancos) END AS cod_cue_sor_bancos
  ,wabaetlx.tlf_sor_bancos_importe AS fc_cue_sor_bancos_importe
  ,wabaetlx.tlf_semfalen AS cod_cue_longitud_cuenta_desde
  ,wabaetlx.tlf_semfanum_deb AS fc_cue_debito_desde
  ,wabaetlx.tlf_semfanum_cred AS fc_cue_credito_desde
  ,wabaetlx.tlf_semfanum_suc AS cod_suc_sucursal
  ,wabaetlx.tlf_semfanum_cta AS cod_cue_cuenta_desde
  ,wabaetlx.tlf_semtalen AS cod_cue_longitud_cuenta_hasta
  ,wabaetlx.tlf_semtanum_deb AS fc_cue_debito_hasta
  ,wabaetlx.tlf_semtanum_cred AS fc_cue_credito_hasta
  ,wabaetlx.tlf_semtanum_suc AS cod_suc_sucursal_hasta
  ,wabaetlx.tlf_semtanum_cta AS cod_cue_cuenta_hasta
  ,CASE WHEN trim(wabaetlx.tlf_semterm_country)='00' OR LENGTH(trim(wabaetlx.tlf_semterm_country))=0  THEN NULL ELSE trim(wabaetlx.tlf_semterm_country) END AS cod_cue_terminal_pais
  ,pais.ds_cue_pais AS ds_cue_terminal_pais
  ,CASE WHEN LENGTH(trim(wabaetlx.tlf_sembanc))=0 THEN NULL ELSE trim(wabaetlx.tlf_sembanc) END cod_cue_tarjeta_banco
  ,CASE WHEN LENGTH(trim(wabaetlx.tlf_semfiid))=0 THEN NULL ELSE trim(wabaetlx.tlf_semfiid) END cod_cue_terminal_banco
  ,CASE WHEN LENGTH(trim(wabaetlx.tlf_cbu_destino))=0 THEN NULL ELSE trim(wabaetlx.tlf_cbu_destino) END cod_cbu_destino
  ,CASE WHEN LENGTH(trim(wabaetlx.tlf_titularidad))=0 THEN NULL ELSE trim(wabaetlx.tlf_titularidad) END cod_cue_titularidad
  ,case WHEN tlf_titularidad='1' THEN 'Cta Cte propia'
  		WHEN tlf_titularidad='2' THEN 'Cta Cte no propia'
  		WHEN tlf_titularidad='3' THEN 'Otra cta propia'
 		WHEN tlf_titularidad='4' THEN 'Otra cta no propia'
 		else null end AS ds_cue_titularidad
  ,CASE WHEN LENGTH(trim(wabaetlx.tlf_tipo_oper))=0 THEN NULL ELSE trim(wabaetlx.tlf_tipo_oper) END AS cod_cue_tipo_oper
  ,CASE WHEN LENGTH(trim(wabaetlx.tlf_mand_doc_benef))=0 THEN NULL ELSE trim(wabaetlx.tlf_mand_doc_benef) END AS cod_cue_doc_benef
  ,CASE WHEN LENGTH(trim(wabaetlx.tlf_mand_num_benef))=0 THEN NULL ELSE trim(wabaetlx.tlf_mand_num_benef) END AS ds_cue_num_benef
  ,CASE WHEN LENGTH(trim(wabaetlx.tlf_mand_identif))=0 THEN NULL ELSE trim(wabaetlx.tlf_mand_identif) END AS cod_cue_mand_identif
  ,CASE WHEN LENGTH(trim(wabaetlx.tlf_semtcomer))=0 THEN NULL ELSE trim(wabaetlx.tlf_semtcomer) END cod_cue_comercio
  ,CASE WHEN LENGTH(trim(wabaetlx.tlf_semtatmi))=0 THEN NULL ELSE trim(wabaetlx.tlf_semtatmi) END cod_cue_atm
  ,wabaetlx.tlf_semtrenu AS cod_cue_comprobante
  ,tlf_sembanc_1 AS fc_cue_sembanc_1
  ,tlf_porcen_retencion  AS fc_cue_porcen_rentencion
  ,tlf_porcen_percepcion AS fc_cue_porcen_percepcion
  ,tlf_retencion_imp_pais  AS fc_cue_retencion_imp_pais
  ,case when tlf_tipo_extr='0' then null else tlf_tipo_extr end AS cod_cue_tipo_extraccion
  ,CASE WHEN LENGTH(trim(wabaetlx.tlf_semtidep))=0 THEN NULL ELSE trim(wabaetlx.tlf_semtidep) END AS cod_cue_tipo_deposito
  ,case when tlf_semtideb='0' then null else tlf_semtideb end AS cod_cue_tipo_debito
  ,case when tlf_semtipag='0' then null else tlf_semtipag end AS cod_cue_tipo_pago
  ,CASE WHEN LENGTH(trim(wabaetlx.tlf_semtitar))=0 THEN NULL ELSE trim(wabaetlx.tlf_semtitar) END AS cod_cue_tipo_tarjeta
  ,case WHEN trim(tlf_semtitar)='A' THEN 'Administrativa'
  		WHEN trim(tlf_semtitar)='B' THEN 'Restringida a Depositos'
  		WHEN trim(tlf_semtitar)='F' THEN 'Tarjeta con Cuenta Credito'
  		WHEN trim(tlf_semtitar)='G' THEN 'Baneldo Débito Adicional'
  		WHEN trim(tlf_semtitar)='J' THEN 'AFJP'
  		WHEN trim(tlf_semtitar)='D' THEN 'Diners Club'
  		WHEN trim(tlf_semtitar)='P' THEN 'Banelco Débito Titular'
  		WHEN trim(tlf_semtitar)='E' THEN 'Electron'
  		WHEN trim(tlf_semtitar)='M' THEN 'Mini Electron'
  		WHEN trim(tlf_semtitar)='I' THEN 'Electron Adicional'
  		WHEN trim(tlf_semtitar)='V' THEN 'Visa'
   else null end AS ds_cue_tipo_tarjeta
  ,trim(wabaetlx.tlf_nombre_origen) AS ds_cue_origen
  ,tlf_tip_debin AS cod_cue_tipo_debin
  ,CASE WHEN trim(wabaetlx.tlf_id_debin)='0000        000000' OR LENGTH(trim(wabaetlx.tlf_id_debin))=0  THEN NULL ELSE trim(wabaetlx.tlf_id_debin) END AS cod_cue_debin
  ,CASE WHEN LENGTH(trim(wabaetlx.tlf_cuit_bco_cdor))=0 THEN NULL ELSE trim(wabaetlx.tlf_cuit_bco_cdor) END AS cod_cue_cuit_bco_cdor
  ,CASE WHEN LENGTH(trim(wabaetlx.tlf_cbu_bco_cdor))=0 THEN NULL ELSE trim(wabaetlx.tlf_cbu_bco_cdor) END AS cod_cue_cbu_bco_cdor
  ,trim(wabaetlx.tlf_cuit_bco_vdor) AS cod_cue_cuit_bco_vdor
  ,CASE WHEN LENGTH(trim(wabaetlx.tlf_cbu_bco_vdor))=0 THEN NULL ELSE trim(wabaetlx.tlf_cbu_bco_vdor) END AS cod_cue_cbu_bco_vdor
  ,CASE
          WHEN LENGTH(CAST(wabaetlx.tlf_fec_neg_coel AS string)) = 3 THEN concat(substring(wabaetlx.partition_date, 1, 4),'0',CAST(wabaetlx.tlf_fec_neg_coel AS string))
          WHEN length(CAST(wabaetlx.tlf_fec_neg_coel AS string)) = 4 THEN concat(substring(wabaetlx.partition_date, 1, 4),CAST(wabaetlx.tlf_fec_neg_coel AS string))
          ELSE NULL
     END  AS dt_cue_neg_coel
  ,wabaetlx.tlf_scoring_debin AS cod_cue_debin_scoring
  ,CASE WHEN wabaetlx.tlf_cpto_debin=0 THEN NULL ELSE wabaetlx.tlf_cpto_debin END AS cod_cue_debin_cpto
  ,trim(wabaetlx.tlf_desc_cpto_deb) AS ds_cue_cpto_deb
  ,CASE WHEN LENGTH(trim(wabaetlx.tlf_corresp_titu))=0 THEN NULL ELSE trim(wabaetlx.tlf_corresp_titu) END AS cod_cue_corresp_titu
  ,tlf_moneda_origen AS cod_cue_moneda_origen
  ,CASE WHEN trim(wabaetlx.tlf_moneda_origen)='032' THEN 'Pesos'
		WHEN trim(wabaetlx.tlf_moneda_origen)='840' THEN 'Dólares'
  	   	WHEN trim(wabaetlx.tlf_moneda_origen)='858' THEN 'Pesos Uruguayos'
		ELSE NULL
   END AS ds_cue_moneda_origen
  ,tlf_semimpo_original AS fc_cue_importe_original
  ,tlf_semfefa AS ds_cue_fefa
  ,tlf_semcotco AS cod_cue_cotco
  ,tlf_semcotve AS cod_cue_cotve
  ,tlf_semamt5 AS fc_cue_semant5
  ,tlf_percepcion_imp_ganan AS fc_cue_imp_ganan
  ,tlf_nro_control AS ds_cue_num_control
  ,CASE WHEN LENGTH(trim(wabaetlx.tlf_trans_banco_receptor))=0 THEN NULL ELSE trim(wabaetlx.tlf_trans_banco_receptor) END AS ds_cue_banco_receptor
  ,CASE WHEN LENGTH(trim(wabaetlx.tlf_trans_codigo_concepto))=0 THEN NULL ELSE trim(wabaetlx.tlf_trans_codigo_concepto) END AS cod_cue_concepto
  ,CASE   WHEN tlf_trans_codigo_concepto='1'  THEN 'Alquileres'
		  WHEN tlf_trans_codigo_concepto='2'  THEN 'Cuota'
		  WHEN tlf_trans_codigo_concepto='3'  THEN 'Expensas'
		  WHEN tlf_trans_codigo_concepto='4'  THEN 'Factura'
		  WHEN tlf_trans_codigo_concepto='5'  THEN 'Prestamo'
		  WHEN tlf_trans_codigo_concepto='6'  THEN 'Seguro'
		  WHEN tlf_trans_codigo_concepto='7'  THEN 'Honorarios'
		  WHEN tlf_trans_codigo_concepto='8'  THEN 'Varios'
		  WHEN tlf_trans_codigo_concepto='9'  THEN 'Haberes'
		  WHEN tlf_trans_codigo_concepto='A'  THEN 'Operaciones Inmobiliarias'
		  WHEN tlf_trans_codigo_concepto='B'  THEN 'Operaciones Inmobiliarias Habitualista'
		  WHEN tlf_trans_codigo_concepto='C'  THEN 'Bienes Registrables Habitualistas'
		  WHEN tlf_trans_codigo_concepto='D'  THEN 'Bienes Registrables No Habitualistas'
		  WHEN tlf_trans_codigo_concepto='E'  THEN 'Suscripción Obligaciones Negociables'
		  WHEN tlf_trans_codigo_concepto='F'  THEN 'Reintegros de Obras Sociales y Prepagas'
		  WHEN tlf_trans_codigo_concepto='G'  THEN 'Siniestros de Seguros'
		  WHEN tlf_trans_codigo_concepto='H'  THEN 'Aportes de Capital'
		  WHEN tlf_trans_codigo_concepto='I'  THEN 'Estado Expropiaciones u Otros'
		  WHEN tlf_trans_codigo_concepto='J'  THEN 'Compra PEI'
		  WHEN tlf_trans_codigo_concepto='K'  THEN 'Pago PEI'
		  WHEN tlf_trans_codigo_concepto='L'  THEN 'Devolución PEI'
   ELSE null END AS ds_cue_concepto
   ,CASE WHEN LENGTH(trim(wabaetlx.tlf_trans_referencia))=0 THEN NULL ELSE trim(wabaetlx.tlf_trans_referencia) END AS ds_cue_trans_referencia
   ,CASE WHEN LENGTH(trim(wabaetlx.tlf_semnro_cargo))=0 THEN NULL ELSE trim(wabaetlx.tlf_semnro_cargo) END AS cod_cue_num_cargo
   ,CASE WHEN LENGTH(trim(wabaetlx.tlf_porc_devl_clte))=0 THEN NULL ELSE trim(wabaetlx.tlf_porc_devl_clte) END AS fc_cue_porcen_devl_clte
   ,CASE WHEN LENGTH(trim(wabaetlx.tlf_porc_devl_comer))=0 THEN NULL ELSE trim(wabaetlx.tlf_porc_devl_comer) END AS fc_cue_porcen_devl_comer
  ,tlf_rubro as cod_cue_rubros
  ,tlf_balanceo as cod_cue_balanceo
  ,CASE WHEN LENGTH(trim(wabaetlx.tlf_tipo_cajero))=0 THEN NULL ELSE trim(wabaetlx.tlf_tipo_cajero) END AS cod_cue_tipo_cajero
  ,stk_debi.cod_per_nup_tarjeta
  ,stk_debi.cod_per_nup_titular
  ,wabaetlx.partition_date
FROM   bi_corp_staging.abae_wabaetlx wabaetlx
left join bi_corp_common.dim_cue_tipo_movimiento_abae mov
on wabaetlx.tlf_semtcode=mov.cod_cue_tipo_movimiento
LEFT JOIN  bi_corp_common.stk_cue_tarjetas_debito stk_debi
ON cast(wabaetlx.tlf_semcacct as bigint) = stk_debi.cod_cue_tarjeta
AND stk_debi.partition_date = '{{ ti.xcom_pull(task_ids='InputConfig', key='max_partition_stk_cue_tarjetas_debito', dag_id='LOAD_CMN_Cuentas_Abae') }}'
LEFT JOIN abae_wabaetlx_reversos wabaetlx_reversos
ON 	wabaetlx.tlf_semcacct=wabaetlx_reversos.tlf_semcacct
AND	wabaetlx.tlf_semtrdat=wabaetlx_reversos.tlf_semtrdat
AND	wabaetlx.tlf_semtrtim=wabaetlx_reversos.tlf_semtrtim
AND	wabaetlx.tlf_semtrenu=wabaetlx_reversos.tlf_semtrenu
AND	wabaetlx.tlf_semtatmi=wabaetlx_reversos.tlf_semtatmi
LEFT JOIN pais pais
on trim(wabaetlx.tlf_semterm_country)=pais.cod_cue_pais
WHERE wabaetlx.partition_date='{{ ti.xcom_pull(task_ids='InputConfig', key='partition_date', dag_id='LOAD_CMN_Cuentas_Abae') }}'
	and wabaetlx_reversos.tlf_semtrenu is null -- Saco los reversos
;


