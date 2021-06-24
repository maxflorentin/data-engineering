set hive.merge.mapfiles=true;
set hive.merge.mapredfiles=true;
set hive.merge.size.per.task=4000000;
set hive.merge.smallfiles.avgsize=16000000;
set hive.exec.dynamic.partition.mode=nonstrict;
set mapred.job.queue.name=root.dataeng;

--inserto tabla parametrica con descripciones normalizadas para ATRC-ABAE-MALBGC

INSERT OVERWRITE TABLE bi_corp_common.dim_trf_param_canales
select distinct
    'ATRC'                        cod_trf_origen,
    trim(codigo)                  cod_trf_canal,
	case when upper(trim(descripcion))  = '' then null
	       else
	            upper(trim(descripcion))
	 end ds_trf_canal_orig,
    case
		when upper(trim(descripcion)) = 'SUCURSAL TR00' then 'SUCURSAL'
		when upper(trim(descripcion)) = 'ASIGNACIONES FAMILIARES' then 'ANSES'
		when upper(trim(descripcion)) = 'BANCA PRIVADA' then 'OBBP'
		when upper(trim(descripcion)) = 'BLACKBERRY' then 'MOBILE'
		when upper(trim(descripcion)) = 'MOBILE BANKING INDIVIDUOS' then 'MOBILE'
		when upper(trim(descripcion)) = 'MOBILE BANKING PYME' then 'MOBILE'
		when upper(trim(descripcion)) = 'DATANET' then 'INTERBANKING'
		when upper(trim(descripcion)) = 'TR00' then 'SUCURSAL'
		when upper(trim(descripcion)) = 'OBE AGENDADA' then 'OBE'
		when upper(trim(descripcion)) = 'OBP AGENDADA' then 'OBP'
		when upper(trim(descripcion)) = 'CONTINGENCIA PIRYP' then 'PIRYP'
		when upper(trim(descripcion)) = '' then null
		    else
				upper(trim(descripcion))
	end                              ds_trf_canal_unificado,
    '2021-03-10'                     dt_trf_fecha_ult_modif
from bi_corp_staging.atrc_canales
where partition_date = '2021-03-10'
union all
select distinct
    'MALBGC'                        cod_trf_origen,
	substr(gen_clave,1,2)           cod_trf_canal,
	trim(substr(gen_Datos,1,40))    ds_trf_canal_orig,
	case
	    when trim(substr(gen_Datos,1,40)) = 'DPL SUCURSALES' then 'SUCURSAL'
		when trim(substr(gen_Datos,1,40)) = 'SUPER LINEA I' then 'SUPERLINEA'
		when trim(substr(gen_Datos,1,40)) = 'SUPER LINEA II' then 'SUPERLINEA'
		when trim(substr(gen_Datos,1,40)) = 'AUTOSERVICIO' then 'TAS'
	    when trim(substr(gen_Datos,1,40)) = 'ONLINE BANKING' then 'OBP'
	    when trim(substr(gen_Datos,1,40)) = 'MOBILE DEPENDENCIA' then 'MOBILE'
	    when trim(substr(gen_Datos,1,40)) = 'ONLINE BANKING CASH MANAGEMENT' then 'OBCM'
	    when trim(substr(gen_Datos,1,40)) = 'BROKER GLOBAL' then 'BP'
	    when trim(substr(gen_Datos,1,40)) = 'ONLINE BANKING AGENDAMIENTO' then 'OBP'
	    when trim(substr(gen_Datos,1,40)) = 'API' then trim(substr(gen_Datos,1,40))
	    when trim(substr(gen_Datos,1,40)) = 'DATANET' then 'INTERBANKING'
	    when trim(substr(gen_Datos,1,40)) = 'BROKER LOCAL' then 'BP'
	    when trim(substr(gen_Datos,1,40)) = 'BLACKBERRY INDIVIDUO' then 'MOBILE'
	    when trim(substr(gen_Datos,1,40)) = 'BLACKBERRY PYME' then 'MOBILE'
	    when trim(substr(gen_Datos,1,40)) = 'BLACKBERRY CORPORATE' then 'MOBILE'
		when trim(substr(gen_Datos,1,40)) = 'OB BANCA PRIVADA' then 'OBBP'
		    else
			    trim(substr(gen_Datos,1,40))
	end 						  ds_trf_canal_unificado,
	'2021-03-10'		 		  dt_trf_fecha_ult_modif
from bi_corp_staging.tcdtgen
where partition_date = '2021-03-10'
and gentabla = '0222'
union all
select distinct
	'ABAE'							cod_trf_origen,
	trim(canal)                     cod_trf_canal,
	case when upper(trim(descripcion))  = '' then null
	       else
	            upper(trim(descripcion))
	 end ds_trf_canal_orig,
	case
		when upper(trim(descripcion)) = 'HOME BANKING' then 'OBP'
		when upper(trim(descripcion)) = 'CAJEROS BANELCO' then 'ATM'
		when upper(trim(descripcion)) = 'CAJEROS LINK' then 'ATM'
		when upper(trim(descripcion)) = 'TRANSF. BOTON DE PAGO S.A.' then 'BOTON DE PAGO SA'
		when upper(trim(descripcion)) = 'PAGOS HOMEBANKING' then 'NO IDENTIFICADO'
		when upper(trim(descripcion)) = 'TRANSFERENCIAS INMEDIATAS' then 'NO IDENTIFICADO'
		when upper(trim(descripcion)) = '' then null
			else
				upper(trim(descripcion))
		end                         ds_trf_canal_unificado,
    '2021-03-10'                    dt_trf_fecha_ult_modif
from bi_corp_staging.abae_canales
where partition_date = '2021-03-10';