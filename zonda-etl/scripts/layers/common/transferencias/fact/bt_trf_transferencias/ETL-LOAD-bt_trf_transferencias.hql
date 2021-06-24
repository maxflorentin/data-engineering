set hive.merge.mapfiles=true;
set hive.merge.mapredfiles=true;
set hive.merge.size.per.task=4000000;
set hive.merge.smallfiles.avgsize=16000000;
set hive.exec.dynamic.partition.mode=nonstrict;
set mapred.job.queue.name=root.dataeng;

--armo tabla temporal PEDT008 con todos los contratos de cuentas para obtener nup por cuenta/sucursal sin duplicados
CREATE TEMPORARY TABLE IF NOT EXISTS TEMP_PEDT008_CTAS as
select p8.penumper,
       p8.penumcon,
       p8.pecodofi
from (
select
       p8a.penumper,
       p8a.penumcon,
       p8a.pecodofi,
       row_number() over (partition by p8a.penumcon, p8a.pecodofi order by coalesce(p8a.pefecbrb,'9999-12-31') desc, p8a.peordpar asc, p8a.penumper ) as row_num
from bi_corp_staging.malpe_pedt008 p8a
where p8a.partition_date = '{{ ti.xcom_pull(task_ids='InputConfig', key='partition_date', dag_id='LOAD_CMN_Transferencias') }}'
and p8a.pecalpar = 'TI'
and p8a.pecodpro IN ('02', '03', '05', '06', '07', '14', '98', '99')
) p8
where p8.row_num = 1;

--armo tabla temporal PEDT015 con todos los documentos sin duplicados
CREATE TEMPORARY TABLE IF NOT EXISTS TEMP_PEDT015C as
select p15.penumper,
	   p15.penumdoc
from (
select p15a.penumper,
	   p15a.penumdoc,
	   row_number() over (partition by p15a.penumdoc order by p15a.penumper ) as row_num
from bi_corp_staging.malpe_pedt015 p15a
where p15a.partition_date = '{{ ti.xcom_pull(task_ids='InputConfig', key='partition_date', dag_id='LOAD_CMN_Transferencias') }}'
and trim(p15a.petipdoc) in ('L','T','D','N')
) p15
where p15.row_num = 1;

--armo tabla temporal PEDT015 con todos los nups sin duplicados con prioridad por cuit/cuil
CREATE TEMPORARY TABLE IF NOT EXISTS TEMP_PEDT015N as
select p.penumper,
	   p.penumdoc
from (
select p15.penumper,
	   p15.penumdoc,
	   row_number() over (partition by p15.penumper order by p15.petipdoc,p15.penumdoc ) as row_num
from (
select p15a.penumper,
	   p15a.penumdoc,
	   case when trim(p15a.petipdoc) = 'T' then 1
	 	    when trim(p15a.petipdoc) = 'L' then 2
	 	    when trim(p15a.petipdoc) = 'D' then 3
	 	else
	 		4
		end petipdoc
from bi_corp_staging.malpe_pedt015 p15a
where p15a.partition_date = '{{ ti.xcom_pull(task_ids='InputConfig', key='partition_date', dag_id='LOAD_CMN_Transferencias') }}'
and trim(p15a.petipdoc) in ('L','T','D','N')
) p15
)p
where p.row_num = 1;

--armo tabla temporal PEDT001 con todas las razones sociales
CREATE TEMPORARY TABLE IF NOT EXISTS TEMP_PEDT001 as
select p1.penumper,
	   case when trim(p1.pepriape)=''
	   	then trim(p1.penomfan)
	   		else
	   			trim(concat(trim(p1.pepriape),' ',trim(p1.penomper)))
	   end penomper
from bi_corp_staging.malpe_pedt001 p1
where p1.partition_date = '{{ ti.xcom_pull(task_ids='InputConfig', key='partition_date', dag_id='LOAD_CMN_Transferencias') }}';

--armo table temporal con la maxima fecha anterior al partition date para barrer los fines de semana en ABAE
CREATE TEMPORARY TABLE IF NOT EXISTS TEMP_ABAE_LAST_DATE as
select max(partition_date) last_day
from bi_corp_common.bt_trf_transferencias
where partition_date >= date_sub('{{ ti.xcom_pull(task_ids='InputConfig', key='partition_date', dag_id='LOAD_CMN_Transferencias') }}',7)
and partition_date < '{{ ti.xcom_pull(task_ids='InputConfig', key='partition_date', dag_id='LOAD_CMN_Transferencias') }}';

--armo tabla temporal con la transaccionalidad de ATRC transformando y completando campos.
CREATE TEMPORARY TABLE IF NOT EXISTS TEMP_ATRC as
select
	  trim(trf.entidad_origen)                                  cod_trf_entidad_origen,
	  cast(trf.sucursal_origen as bigint)                       cod_trf_suc_origen,
      cast(trf.cuenta_origen as bigint)                         cod_trf_cta_origen,
      case when trim(trf.cuit_origen)='' or trim(trf.cuit_origen)='00000000000' then null
	        else trim(trf.cuit_origen)
	  end                                                       ds_per_cuit_origen,
      case when trim(trf.cbu_origen)='' or trim(trf.cbu_origen)='0000000000000000000000' then null
	        else trim(trf.cbu_origen)
	  end                                                         cod_trf_cbu_origen,
      case when trim(coalesce(p1o.penomper,upper(trf.razon_social_origen))) = '' then null
            else trim(coalesce(p1o.penomper,upper(trf.razon_social_origen)))
      end                                                       ds_per_nombre_origen,
      cast(coalesce (p8o.penumper, p15o.penumper) as bigint)    cod_per_nup_origen,
      trim(trf.entidad_destino)                                 cod_trf_entidad_destino,
      cast(trf.sucursal_destino as bigint)                      cod_trf_suc_destino,
   	  cast(trf.cuenta_destino as bigint)                        cod_trf_cta_destino,
      case when trim(trf.cuit_destino)='' or trim(trf.cuit_destino)='00000000000' then null
			else trim(trf.cuit_destino)
	  end                                                       ds_per_cuit_destino,
      case when trim(trf.cbu_destino)='' or trim(trf.cbu_destino)='0000000000000000000000' then null
	        else trim(trf.cbu_destino)
	  end                                                       cod_trf_cbu_destino,
      case when trim(coalesce(p1d.penomper,upper(trf.razon_social_destino))) = '' then null
            else trim(coalesce(p1d.penomper,upper(trf.razon_social_destino)))
      end                                                       ds_per_nombre_destino,
      cast(coalesce (p8d.penumper, p15d.penumper) as bigint)    cod_per_nup_destino,
      from_unixtime(UNIX_TIMESTAMP(concat(case when trim(trf.fecha_alta) in ('9999-12-31', '0001-01-01','0000-00-00') then null else trim(trf.fecha_alta) end,' ',trim(trf.hora_alta)) ,'yyyy-MM-dd HH:mm:ss')) ts_trf_fecha_alta,
      from_unixtime(UNIX_TIMESTAMP(concat(case when trim(trf.fecha_debito) in ('9999-12-31', '0001-01-01','0000-00-00') then null else trim(trf.fecha_debito) end,' ',trim(trf.hora_debito)) ,'yyyy-MM-dd HH:mm:ss')) ts_trf_fecha_debito,
      from_unixtime(UNIX_TIMESTAMP(concat(case when trim(trf.fecha_envio_riesgo) in ('9999-12-31', '0001-01-01','0000-00-00')  then null else trim(trf.fecha_envio_riesgo) end,' ',trim(trf.hora_envio_riesgo)) ,'yyyy-MM-dd HH:mm:ss'))  ts_trf_fecha_env_riesgo,
  	  from_unixtime(UNIX_TIMESTAMP(concat(case when trim(trf.fecha_credito) in ('9999-12-31', '0001-01-01','0000-00-00')  then null else trim(trf.fecha_credito) end,' ',trim(trf.hora_credito)) ,'yyyy-MM-dd HH:mm:ss'))         ts_trf_fecha_credito,
  	  from_unixtime(UNIX_TIMESTAMP(concat(case when trim(trf.fecha_envio_camara) in ('9999-12-31', '0001-01-01','0000-00-00')  then null else trim(trf.fecha_envio_camara) end,' ',trim(trf.hora_envio_camara)) ,'yyyy-MM-dd HH:mm:ss'))  ts_trf_fecha_env_camara,
	  from_unixtime(UNIX_TIMESTAMP(concat(case when trim(trf.fecha_rta_camara) in ('9999-12-31', '0001-01-01','0000-00-00')  then null else trim(trf.fecha_rta_camara) end,' ',trim(trf.hora_rta_camara)) ,'yyyy-MM-dd HH:mm:ss')) ts_trf_fecha_rta_camara,
	  case when trim(trf.concepto) = '' then null
	     else
	        trim(trf.concepto)
	  end                                                       cod_trf_concepto,
	  case when trim(trf.referencia) = '' then null
	     else
	        trim(trf.referencia)
	  end                                                       ds_trf_referencia,
	  (cast(trf.importe as bigint)/100)                         fc_trf_importe,
      trim(trf.moneda)                                          cod_trf_moneda,
      case when trim(trf.producto) = '' then null
         else
            trim(trf.producto)
      end                                                       cod_trf_producto,
      case when trim(trf.camara) = '' then null
         else
            trim(trf.camara)
      end                                                       cod_trf_camara,
      trim(trf.canal)                                           cod_trf_canal,
      trim(trf.tipo_transf)                                     ds_trf_tipo,
      case when trim(trf.id_transf_camara) = '' then null
         else
            trim(trf.id_transf_camara)
      end                                                       cod_trf_en_camara,
      trim(trf.estado)                                          cod_trf_estado,
      case when trim(trf.motivo_rechazo) = '' then null
         else
             trim(trf.motivo_rechazo)
      end                                                       cod_trf_mot_rechazo,
      'ATRC'                                                    cod_trf_origen,
      case when trf.entidad_origen = '0000'
      			then cast(trf.sucursal_origen as bigint)
      				else
      					null
      end ent_suc_origen,
      case when trf.entidad_destino = '0000'
      			then cast(trf.sucursal_destino as bigint)
      				else
      					null
      end ent_suc_destino,
      '{{ ti.xcom_pull(task_ids='InputConfig', key='partition_date', dag_id='LOAD_CMN_Transferencias') }}' partition_date
from bi_corp_staging.atrc_transferencias trf
left join TEMP_PEDT008_CTAS p8o
	on (concat('000',substr(trf.cuenta_origen,4)) = p8o.penumcon
		and trf.sucursal_origen = p8o.pecodofi
		and trim(trf.entidad_origen) = '0072')
left join TEMP_PEDT008_CTAS p8d
	on (concat('000',substr(trf.cuenta_destino,4)) = p8d.penumcon
		and trf.sucursal_destino = p8d.pecodofi
		and trim(trf.entidad_destino) = '0072')
left join TEMP_PEDT015C p15o
	on (trf.cuit_origen = p15o.penumdoc)
left join TEMP_PEDT015C p15d
	on (trf.cuit_destino = p15d.penumdoc)
left join TEMP_PEDT001 p1o
	on (coalesce (p8o.penumper, p15o.penumper)  = p1o.penumper
	    and trim(trf.entidad_origen) = '0072')
left join TEMP_PEDT001 p1d
	on (coalesce (p8d.penumper, p15d.penumper) = p1d.penumper
	    and trim(trf.entidad_destino) = '0072')
where trf.partition_date = '{{ ti.xcom_pull(task_ids='InputConfig', key='partition_date', dag_id='LOAD_CMN_Transferencias') }}';

--armo tabla temporal con la transaccionalidad de MALBGC transformando y completando campos.
CREATE TEMPORARY TABLE IF NOT EXISTS TEMP_MALBGC as
select
	  trim(trf.entidad_cargo)                                   cod_trf_entidad_origen,
	  cast(trf.suc_cargo as bigint)                             cod_trf_suc_origen,
      cast(trf.cuenta_cargo as bigint)                          cod_trf_cta_origen,
      case when trim(p15o.penumdoc)='' or trim(p15o.penumdoc)='00000000000' then null
	        else trim(p15o.penumdoc)
	  end                                                       ds_per_cuit_origen,
      case when trim(trf.cbu_origen)='' or trim(trf.cbu_origen)='0000000000000000000000' then null
	        else trim(trf.cbu_origen)
	  end                                                       cod_trf_cbu_origen,
      case when trim(p1o.penomper) = '' then null
            else trim(p1o.penomper)
      end                                                       ds_per_nombre_origen,
      cast(trf.nup as bigint)    								cod_per_nup_origen,
      trim(trf.entidad_abono)                                 	cod_trf_entidad_destino,
      cast(trf.suc_abono as bigint)                      		cod_trf_suc_destino,
   	  cast(trf.cuenta_abono as bigint)                        	cod_trf_cta_destino,
      case when trim(p15d.penumdoc)='' or trim(p15d.penumdoc)='00000000000' then null
	        else trim(p15d.penumdoc)
	  end                                                       ds_per_cuit_destino,
      case when trim(trf.cbu_destino)='' or trim(trf.cbu_destino)='0000000000000000000000' then null
	        else trim(trf.cbu_destino)
	  end                                                       cod_trf_cbu_destino,
      case when trim(p1d.penomper) = '' then null
            else trim(p1d.penomper)
      end                                                       ds_per_nombre_destino,
 	  cast(p8d.penumper as bigint) 								cod_per_nup_destino,
      from_unixtime(UNIX_TIMESTAMP(concat(case when trim(trf.fecha_hoy) in ('9999-12-31', '0001-01-01','0000-00-00')  then null else trim(trf.fecha_hoy) end,' ',trim(trf.hora_hoy)) ,'yyyy-MM-dd HH:mm:ss')) ts_trf_fecha_alta,
      from_unixtime(UNIX_TIMESTAMP(concat(case when trim(trf.fecha_valor) in ('9999-12-31', '0001-01-01','0000-00-00')  then null else trim(trf.fecha_valor) end,' ', '00:00:00'),'yyyy-MM-dd HH:mm:ss')) ts_trf_fecha_debito,
	  cast(null as string)  													ts_trf_fecha_env_riesgo,
      from_unixtime(UNIX_TIMESTAMP(concat(case when trim(trf.fecha_valor) in ('9999-12-31', '0001-01-01','0000-00-00')  then null else trim(trf.fecha_valor) end,' ', '00:00:00'),'yyyy-MM-dd HH:mm:ss')) ts_trf_fecha_credito,
	  cast(null as string) 														ts_trf_fecha_env_camara,
	  cast(null as string) 														ts_trf_fecha_rta_camara,
	  cast(null as string)                                        				cod_trf_concepto,
	  cast(null as string)                                      				ds_trf_referencia,
	  (cast(trf.importe_abono as bigint)/100)                   fc_trf_importe,
      trim(trf.divisa_cargo)                                    cod_trf_moneda,
      cast(null as string)                                      				cod_trf_producto,
      cast(null as string)                                          			cod_trf_camara,
      trim(trf.canal)                                           cod_trf_canal,
      'E'                                     					ds_trf_tipo,
      cast(null as string)                                						cod_trf_en_camara,
      trim(trf.estado)                                          cod_trf_estado,
      cast(null as string)                                 						cod_trf_mot_rechazo,
      'MALBGC'                                                  cod_trf_origen,
      cast(null as bigint)                                                      ent_suc_origen,
      cast(null as bigint)                                                      ent_suc_destino,
     '{{ ti.xcom_pull(task_ids='InputConfig', key='partition_date', dag_id='LOAD_CMN_Transferencias') }}' partition_date
from bi_corp_staging.malbgc_zbdttra trf
left join TEMP_PEDT008_CTAS p8d
	on (concat('000',substr(trf.cuenta_abono,4)) = p8d.penumcon
		and trf.suc_abono = p8d.pecodofi
		and trim(trf.entidad_abono) = '0072')
left join TEMP_PEDT015N p15o
	on (trf.nup = p15o.penumper)
left join TEMP_PEDT015N p15d
	on (p8d.penumper = p15d.penumper)
left join TEMP_PEDT001 p1o
	on (trf.nup = p1o.penumper
	    and trim(trf.entidad_cargo) = '0072')
left join TEMP_PEDT001 p1d
	on (p8d.penumper = p1d.penumper
	    and trim(trf.entidad_abono) = '0072')
where trf.partition_date = '{{ ti.xcom_pull(task_ids='InputConfig', key='partition_date', dag_id='LOAD_CMN_Transferencias') }}';

--armo tabla temporal con la transaccionalidad de ABAE transformando y completando campos.
CREATE TEMPORARY TABLE IF NOT EXISTS TEMP_ABAE as
select
	  trim(trf.entidad_origen)                                  cod_trf_entidad_origen,
	  cast(trf.sucursal_origen as bigint)                       cod_trf_suc_origen,
      cast(trf.cuenta_origen as bigint)                         cod_trf_cta_origen,
      case when trim(trf.cuit_origen)='' or trim(trf.cuit_origen)='00000000000' then null
	        else trim(trf.cuit_origen)
	  end                                                       ds_per_cuit_origen,
      case when trim(trf.cbu_origen)='' or trim(trf.cbu_origen)='0000000000000000000000' then null
	        else trim(trf.cbu_origen)
	  end                                                         cod_trf_cbu_origen,
      case when trim(coalesce(p1o.penomper,upper(trf.razon_social_origen))) = '' then null
            else trim(coalesce(p1o.penomper,upper(trf.razon_social_origen)))
      end                                                       ds_per_nombre_origen,
      cast(coalesce (p8o.penumper, p15o.penumper) as bigint)    cod_per_nup_origen,
      trim(trf.entidad_destino)                                 cod_trf_entidad_destino,
      cast(trf.sucursal_destino as bigint)                      cod_trf_suc_destino,
   	  cast(trf.cuenta_destino as bigint)                        cod_trf_cta_destino,
      case when trim(trf.cuit_destino)='' or trim(trf.cuit_destino)='00000000000' then null
	        else trim(trf.cuit_destino)
	  end                                                       ds_per_cuit_destino,
      case when trim(trf.cbu_destino)='' or trim(trf.cbu_destino)='0000000000000000000000' then null
	        else trim(trf.cbu_destino)
	  end                                                       cod_trf_cbu_destino,
      case when trim(coalesce(p1d.penomper,upper(trf.razon_social_destino))) = '' then null
            else trim(coalesce(p1d.penomper,upper(trf.razon_social_destino)))
      end                                                       ds_per_nombre_destino,
      cast(coalesce (p8d.penumper, p15d.penumper) as bigint)    cod_per_nup_destino,
      from_unixtime(UNIX_TIMESTAMP(concat(case when trim(trf.fecha_alta) in ('9999-12-31', '0001-01-01','0000-00-00','') then null else trim(trf.fecha_alta) end,' ',trim(trf.hora_alta)) ,'yyyy-MM-dd HH:mm:ss')) ts_trf_fecha_alta,
      from_unixtime(UNIX_TIMESTAMP(concat(case when trim(trf.fecha_debito) in ('9999-12-31', '0001-01-01','0000-00-00','') then null else trim(trf.fecha_debito) end,' ',trim(trf.hora_debito)) ,'yyyy-MM-dd HH:mm:ss')) ts_trf_fecha_debito,
      from_unixtime(UNIX_TIMESTAMP(concat(case when trim(trf.fecha_envio_riesgo) in ('9999-12-31', '0001-01-01','0000-00-00','')  then null else trim(trf.fecha_envio_riesgo) end,' ',trim(trf.hora_envio_riesgo)) ,'yyyy-MM-dd HH:mm:ss'))  ts_trf_fecha_env_riesgo,
  	  from_unixtime(UNIX_TIMESTAMP(concat(case when trim(trf.fecha_credito) in ('9999-12-31', '0001-01-01','0000-00-00','')  then null else trim(trf.fecha_credito) end,' ',trim(trf.hora_credito)) ,'yyyy-MM-dd HH:mm:ss'))         ts_trf_fecha_credito,
  	  from_unixtime(UNIX_TIMESTAMP(concat(case when trim(trf.fecha_envio_camara) in ('9999-12-31', '0001-01-01','0000-00-00','')  then null else trim(trf.fecha_envio_camara) end,' ',trim(trf.hora_envio_camara)) ,'yyyy-MM-dd HH:mm:ss'))  ts_trf_fecha_env_camara,
	  from_unixtime(UNIX_TIMESTAMP(concat(case when trim(trf.fecha_rta_camara) in ('9999-12-31', '0001-01-01','0000-00-00','')  then null else trim(trf.fecha_rta_camara) end,' ',trim(trf.hora_rta_camara)) ,'yyyy-MM-dd HH:mm:ss')) ts_trf_fecha_rta_camara,
	  case when trim(trf.concepto) = '' then null
	     else
	        trim(trf.concepto)
	  end                                                       cod_trf_concepto,
	  case when trim(trf.referencia) = '' then null
	     else
	        trim(trf.referencia)
	  end                                                       ds_trf_referencia,
	  (cast(trf.importe as bigint)/100)                         fc_trf_importe,
      trim(trf.moneda)                                          cod_trf_moneda,
      case when trim(trf.producto) = '' then null
         else
            trim(trf.producto)
      end                                                       cod_trf_producto,
      case when trim(trf.camara) = '' then null
         else
            trim(trf.camara)
      end                                                       cod_trf_camara,
      trim(trf.canal)                                           cod_trf_canal,
      trim(trf.tipo_transf)                                     ds_trf_tipo,
      case when trim(trf.id_transf_camara) = '' then null
         else
            trim(trf.id_transf_camara)
      end                                                       cod_trf_en_camara,
      trim(trf.estado)                                          cod_trf_estado,
      case when trim(trf.motivo_rechazo) = '' or trim(trf.motivo_rechazo) in ('000','001')  then null
         else
             trim(trf.motivo_rechazo)
      end                                                       cod_trf_mot_rechazo,
      'ABAE'                                                    cod_trf_origen,
      case when trf.entidad_origen = '0000'
      			then cast(trf.sucursal_origen as bigint)
      				else
      					null
      end ent_suc_origen,
      case when trf.entidad_destino = '0000'
      			then cast(trf.sucursal_destino as bigint)
      				else
      					null
      end ent_suc_destino,
      '{{ ti.xcom_pull(task_ids='InputConfig', key='partition_date', dag_id='LOAD_CMN_Transferencias') }}' partition_date
from bi_corp_staging.abae_transferencias trf
left join TEMP_PEDT008_CTAS p8o
	on (concat('000',substr(trf.cuenta_origen,4)) = p8o.penumcon
		and trf.sucursal_origen = p8o.pecodofi
		and trim(trf.entidad_origen) = '0072')
left join TEMP_PEDT008_CTAS p8d
	on (concat('000',substr(trf.cuenta_destino,4)) = p8d.penumcon
		and trf.sucursal_destino = p8d.pecodofi
		and trim(trf.entidad_destino) = '0072')
left join TEMP_PEDT015C p15o
	on (trf.cuit_origen = p15o.penumdoc)
left join TEMP_PEDT015C p15d
	on (trf.cuit_destino = p15d.penumdoc)
left join TEMP_PEDT001 p1o
	on (coalesce (p8o.penumper, p15o.penumper)  = p1o.penumper
	    and trim(trf.entidad_origen) = '0072')
left join TEMP_PEDT001 p1d
	on (coalesce (p8d.penumper, p15d.penumper) = p1d.penumper
	    and trim(trf.entidad_destino) = '0072'),
TEMP_ABAE_LAST_DATE tad
where trf.partition_date > tad.last_day
and   trf.partition_date <= '{{ ti.xcom_pull(task_ids='InputConfig', key='partition_date', dag_id='LOAD_CMN_Transferencias') }}';

--inserto fact ATRC - ABAE - CTAS completando dimensiones
INSERT OVERWRITE TABLE bi_corp_common.bt_trf_transferencias
PARTITION (partition_date)
select
    tatrc.cod_trf_entidad_origen,
    eto.cod_trf_tipo cod_trf_tipo_entidad_origen,
    eto.ds_trf_entidad ds_trf_entidad_origen,
    tatrc.cod_trf_suc_origen,
    tatrc.cod_trf_cta_origen,
    tatrc.ds_per_cuit_origen,
    tatrc.cod_trf_cbu_origen,
    tatrc.ds_per_nombre_origen,
    tatrc.cod_per_nup_origen,
    tatrc.cod_trf_entidad_destino,
    etd.cod_trf_tipo cod_trf_tipo_entidad_destino,
	etd.ds_trf_entidad ds_trf_entidad_destino,
    tatrc.cod_trf_suc_destino,
    tatrc.cod_trf_cta_destino,
    tatrc.ds_per_cuit_destino,
    tatrc.cod_trf_cbu_destino,
    tatrc.ds_per_nombre_destino,
    tatrc.cod_per_nup_destino,
    coalesce (tatrc.ts_trf_fecha_credito, tatrc.ts_trf_fecha_debito, tatrc.ts_trf_fecha_alta, tatrc.ts_trf_fecha_env_riesgo, tatrc.ts_trf_fecha_rta_camara) ts_trf_fecha_estado,
    tatrc.ts_trf_fecha_alta,
    tatrc.ts_trf_fecha_debito,
    tatrc.ts_trf_fecha_env_riesgo,
  	tatrc.ts_trf_fecha_credito,
  	tatrc.ts_trf_fecha_env_camara,
	tatrc.ts_trf_fecha_rta_camara,
  	tatrc.cod_trf_concepto,
  	coalesce(con.ds_trf_concepto_unificado,con.ds_trf_concepto) ds_trf_concepto,
	tatrc.ds_trf_referencia,
    tatrc.fc_trf_importe,
    tatrc.cod_trf_moneda,
    tatrc.cod_trf_producto,
    pro.ds_trf_producto,
    tatrc.cod_trf_camara,
    cam.ds_trf_camara,
    tatrc.cod_trf_canal,
    coalesce(can.ds_trf_canal_unificado,can.ds_trf_canal,'NO IDENTIFICADO') ds_trf_canal,
    tatrc.ds_trf_tipo,
    tatrc.cod_trf_en_camara,
    tatrc.cod_trf_estado,
    tatrc.cod_trf_mot_rechazo,
    rec.ds_trf_rechazo,
    tatrc.cod_trf_origen,
    tatrc.partition_date
from TEMP_ATRC tatrc
left join bi_corp_common.dim_trf_conceptos con
	on (tatrc.cod_trf_concepto = con.cod_trf_concepto
		and tatrc.cod_trf_origen = con.cod_trf_origen
		and tatrc.partition_date = con.partition_date)
left join bi_corp_common.dim_trf_productos pro
	on (tatrc.cod_trf_producto = pro.cod_trf_producto
		and tatrc.cod_trf_origen = pro.cod_trf_origen
		and tatrc.partition_date = pro.partition_date)
left join bi_corp_common.dim_trf_camaras cam
	on (tatrc.cod_trf_camara = cam.cod_trf_camara
		and tatrc.cod_trf_origen = cam.cod_trf_origen
		and tatrc.partition_date = cam.partition_date)
left join bi_corp_common.dim_trf_canales can
	on (tatrc.cod_trf_canal = can.cod_trf_canal
		and tatrc.cod_trf_origen = can.cod_trf_origen
		and tatrc.partition_date = can.partition_date)
left join bi_corp_common.dim_trf_rechazos rec
	on (tatrc.cod_trf_mot_rechazo = rec.cod_trf_rechazo
		and tatrc.cod_trf_origen = rec.cod_trf_origen
		and tatrc.partition_date = rec.partition_date)
left join bi_corp_common.dim_trf_entidades eto
	on (tatrc.cod_trf_entidad_origen = eto.cod_trf_entidad
		and nvl(tatrc.ent_suc_origen,0) = nvl(eto.cod_trf_entidad_psp,0)
		and tatrc.partition_date = eto.partition_date)
left join bi_corp_common.dim_trf_entidades etd
	on (tatrc.cod_trf_entidad_destino = etd.cod_trf_entidad
		and nvl(tatrc.ent_suc_destino,0) = nvl(etd.cod_trf_entidad_psp,0)
		and tatrc.partition_date = etd.partition_date)
union all
select
    tmal.cod_trf_entidad_origen,
    eto.cod_trf_tipo cod_trf_tipo_entidad_origen,
    eto.ds_trf_entidad ds_trf_entidad_origen,
    tmal.cod_trf_suc_origen,
    tmal.cod_trf_cta_origen,
    tmal.ds_per_cuit_origen,
    tmal.cod_trf_cbu_origen,
    tmal.ds_per_nombre_origen,
    tmal.cod_per_nup_origen,
    tmal.cod_trf_entidad_destino,
    etd.cod_trf_tipo cod_trf_tipo_entidad_destino,
    etd.ds_trf_entidad ds_trf_entidad_destino,
    tmal.cod_trf_suc_destino,
    tmal.cod_trf_cta_destino,
    tmal.ds_per_cuit_destino,
    tmal.cod_trf_cbu_destino,
    tmal.ds_per_nombre_destino,
    tmal.cod_per_nup_destino,
    tmal.ts_trf_fecha_alta ts_trf_fecha_estado,
    tmal.ts_trf_fecha_alta,
    tmal.ts_trf_fecha_debito,
    tmal.ts_trf_fecha_env_riesgo,
  	tmal.ts_trf_fecha_credito,
  	tmal.ts_trf_fecha_env_camara,
	tmal.ts_trf_fecha_rta_camara,
  	tmal.cod_trf_concepto,
  	null ds_trf_concepto,
	tmal.ds_trf_referencia,
    tmal.fc_trf_importe,
    tmal.cod_trf_moneda,
    tmal.cod_trf_producto,
    null ds_trf_producto,
    tmal.cod_trf_camara,
    null ds_trf_camara,
    tmal.cod_trf_canal,
    coalesce(can.ds_trf_canal_unificado,can.ds_trf_canal,'NO IDENTIFICADO') ds_trf_canal,
    tmal.ds_trf_tipo,
    tmal.cod_trf_en_camara,
    tmal.cod_trf_estado,
    tmal.cod_trf_mot_rechazo,
    null ds_trf_rechazo,
    tmal.cod_trf_origen,
    tmal.partition_date
from TEMP_MALBGC tmal
left join bi_corp_common.dim_trf_canales can
	on (tmal.cod_trf_canal = can.cod_trf_canal
		and tmal.cod_trf_origen = can.cod_trf_origen
		and tmal.partition_date = can.partition_date)
left join bi_corp_common.dim_trf_entidades eto
	on (tmal.cod_trf_entidad_origen = eto.cod_trf_entidad
		and nvl(tmal.ent_suc_origen,0) = nvl(eto.cod_trf_entidad_psp,0)
		and tmal.partition_date = eto.partition_date)
left join bi_corp_common.dim_trf_entidades etd
	on (tmal.cod_trf_entidad_destino = etd.cod_trf_entidad
		and nvl(tmal.ent_suc_destino,0) = nvl(etd.cod_trf_entidad_psp,0)
		and tmal.partition_date = etd.partition_date)
union all
select
    tabae.cod_trf_entidad_origen,
    eto.cod_trf_tipo cod_trf_tipo_entidad_origen,
    eto.ds_trf_entidad ds_trf_entidad_origen,
    tabae.cod_trf_suc_origen,
    tabae.cod_trf_cta_origen,
    tabae.ds_per_cuit_origen,
    tabae.cod_trf_cbu_origen,
    tabae.ds_per_nombre_origen,
    tabae.cod_per_nup_origen,
    tabae.cod_trf_entidad_destino,
    etd.cod_trf_tipo cod_trf_tipo_entidad_destino,
	etd.ds_trf_entidad ds_trf_entidad_destino,
    tabae.cod_trf_suc_destino,
    tabae.cod_trf_cta_destino,
    tabae.ds_per_cuit_destino,
    tabae.cod_trf_cbu_destino,
    tabae.ds_per_nombre_destino,
    tabae.cod_per_nup_destino,
    tabae.ts_trf_fecha_alta ts_trf_fecha_estado,
    tabae.ts_trf_fecha_alta,
    tabae.ts_trf_fecha_debito,
    tabae.ts_trf_fecha_env_riesgo,
  	tabae.ts_trf_fecha_credito,
  	tabae.ts_trf_fecha_env_camara,
	tabae.ts_trf_fecha_rta_camara,
  	tabae.cod_trf_concepto,
  	coalesce(con.ds_trf_concepto_unificado,con.ds_trf_concepto) ds_trf_concepto,
	tabae.ds_trf_referencia,
    tabae.fc_trf_importe,
    tabae.cod_trf_moneda,
    tabae.cod_trf_producto,
    null ds_trf_producto,
    tabae.cod_trf_camara,
    cam.ds_trf_camara,
    tabae.cod_trf_canal,
    coalesce(can.ds_trf_canal_unificado,can.ds_trf_canal,'NO IDENTIFICADO') ds_trf_canal,
    tabae.ds_trf_tipo,
    tabae.cod_trf_en_camara,
    tabae.cod_trf_estado,
    tabae.cod_trf_mot_rechazo,
    rec.ds_trf_rechazo,
    tabae.cod_trf_origen,
    tabae.partition_date
from TEMP_ABAE tabae
left join bi_corp_common.dim_trf_conceptos con
	on (tabae.cod_trf_concepto = con.cod_trf_concepto
		and tabae.cod_trf_origen = con.cod_trf_origen
		and tabae.partition_date = con.partition_date)
left join bi_corp_common.dim_trf_camaras cam
	on (tabae.cod_trf_camara = cam.cod_trf_camara
		and tabae.partition_date = cam.partition_date)
left join bi_corp_common.dim_trf_canales can
	on (tabae.cod_trf_canal = can.cod_trf_canal
		and tabae.cod_trf_origen = can.cod_trf_origen
		and tabae.partition_date = can.partition_date)
left join bi_corp_common.dim_trf_rechazos rec
	on (tabae.cod_trf_mot_rechazo = rec.cod_trf_rechazo
		and tabae.cod_trf_origen = rec.cod_trf_origen
		and tabae.partition_date = rec.partition_date)
left join bi_corp_common.dim_trf_entidades eto
	on (tabae.cod_trf_entidad_origen = eto.cod_trf_entidad
		and nvl(tabae.ent_suc_origen,0) = nvl(eto.cod_trf_entidad_psp,0)
		and tabae.partition_date = eto.partition_date)
left join bi_corp_common.dim_trf_entidades etd
	on (tabae.cod_trf_entidad_destino = etd.cod_trf_entidad
		and nvl(tabae.ent_suc_destino,0) = nvl(etd.cod_trf_entidad_psp,0)
		and tabae.partition_date = etd.partition_date);
