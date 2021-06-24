"
SET mapred.job.queue.name=root.dataeng;
SET hive.exec.dynamic.partition.mode=nonstrict;

INSERT OVERWRITE TABLE bi_corp_common.bt_mya_correspondenciadigital
PARTITION (partition_date)

select
DISTINCT id_correspondencia															ID_MYA_CORRESPONDENCIA,
msg_req_id 																	COD_MYA_MENSAJE,
tipo_carta																	DS_MYA_TIPO_CARTA,
CASE WHEN nombre_apellido='null' then NULL else nombre_apellido end			DS_MYA_NOMBRE_APELLIDO,
CASE WHEN razon_social='null' then NULL else razon_social end				DS_MYA_RAZON_SOCIAL,
CASE WHEN telefono='null' then NULL else telefono end						DS_MYA_TELEFONO,
CASE WHEN tipo_documento='null' then NULL else tipo_documento end			COD_MYA_TIPO_DOCUMENTO,
CASE WHEN tipo_documento='1' THEN 'DNI'
     ELSE TRIM(ti.descripcion)    END                                       DS_MYA_TIPO_DOCUMENTO,
CAST(numero_documento as bigint)											COD_MYA_NUMDOC,
CASE WHEN valida_direccion='null' then NULL else valida_direccion end		DS_MYA_VALIDA_DIRECCION,
CASE WHEN calle='null' then NULL else calle end								DS_MYA_CALLE,
CASE WHEN numero_direccion='null' then NULL else numero_direccion end		DS_MYA_NUMEROD,
CASE WHEN piso='null' then NULL else piso end								DS_MYA_PISO,
CASE WHEN depto='null' then NULL else depto end								DS_MYA_DEPTO,
CASE WHEN barrio='null' then NULL else barrio end							DS_MYA_BARRIO,
CASE WHEN localidad='null' then NULL else localidad end						DS_MYA_LOCALIDAD,
CASE WHEN codigo_postal='null' then NULL else codigo_postal end				DS_MYA_CODIGOPOSTAL,
CASE WHEN provincia='null' then NULL else provincia end						COD_MYA_PROVINCIA,
TRIM(pro.descripcion)                                                       DS_MYA_PROVINCIA,
CASE WHEN pais='null' then NULL else pais end								COD_MYA_PAIS,
CASE WHEN pais='13' then 'ARGENTINA' else NULL end                          DS_MYA_PAIS,
CASE WHEN lugar_fecha='null' then NULL else lugar_fecha end					DS_MYA_LUGAR_FECHA,
CAST(nup AS BIGINT)															COD_PER_NUP,
CASE WHEN marca_tarjeta='null' then NULL else marca_tarjeta end				DS_MYA_MARCA_TARJETA,
CASE WHEN tipo_prestamo='null' then NULL else tipo_prestamo end				DS_MYA_TIPO_PRESTAMOS,
CASE WHEN tarjeta_cuenta='null' then NULL else tarjeta_cuenta end			DS_MYA_NUM_TARJETA,
CAST(numero_sucursal AS INT) 												COD_MYA_SUCURSAL,
CASE WHEN nombre_sucursal='null' then NULL else nombre_sucursal end			DS_MYA_SUCURSAL_NOMBRE,
CASE WHEN domicilio_sucursal='null' then NULL else domicilio_sucursal end	DS_MYA_SUCURSAL_DOMICILIO,
CAST(monto AS DECIMAL(19,4))												FC_MYA_MONTO,
CASE WHEN moneda='null' then NULL else moneda end						   	COD_MYA_DIVISA,
from_unixtime(unix_timestamp(fecha_cierre,'dd/MM/yyyy'), 'yyyy-MM-dd')  	DT_MYA_FECHA_CIERRE,
CAST(cantidad_producto as INT)												INT_MYA_CANTIDAD_PRODUCTO,
CASE WHEN producto1='null' then NULL else producto1 end						DS_MYA_PRODUCTO1,
CAST(monto1 AS DECIMAL(19,4))												FC_MYA_MONTO1,
CASE WHEN producto2='null' then NULL else producto2 end						DS_MYA_PRODUCTO2,
CAST(monto2 AS DECIMAL(19,4))												FC_MYA_MONTO2,
CASE WHEN producto3='null' then NULL else producto3 end						DS_MYA_PRODUCTO3,
CAST(monto3 AS DECIMAL(19,4))												FC_MYA_MONTO3,
SUBSTRING(create_date,1,19)													TS_MYA_FECHA_CREACION,
SUBSTRING(modify_date,1,19)													TS_MYA_FECHA_MODIFICACION,
CASE WHEN tt_sts='null' then NULL else tt_sts end						    DS_MYA_TT_STS,
CASE WHEN fecha_imposicion='null' then NULL else SUBSTRING(fecha_imposicion,1,19) end		TS_MYA_FECHA_IMPOSICION,
CASE WHEN ultimo_resultado='null' then NULL else ultimo_resultado end		DS_MYA_ULTIMO_RESULTADO,
CASE WHEN fecha_resultado='null' then NULL else SUBSTRING(fecha_resultado,1,19) end			TS_MYA_FECHA_RESULTADO,

di.PARTITION_DATE																PARTITION_DATE

from bi_corp_staging.rio74_correspondencia_digital di
left join bi_corp_staging.rio74_provincia pro on
(di.provincia= pro.codigo_alfa_provincia and
pro.partition_Date= IF("{{ ti.xcom_pull(task_ids='InputConfig', key='partition_date', dag_id='LOAD_CMN_MYA-Daily') }}" < "2020-09-02","2020-09-02", "{{ ti.xcom_pull(task_ids='InputConfig', key='partition_date', dag_id='LOAD_CMN_MYA-Daily') }}"))
left join
(SELECT *
 FROM bi_corp_staging.rio74_tipo_documento
  WHERE partition_Date=IF("{{ ti.xcom_pull(task_ids='InputConfig', key='partition_date', dag_id='LOAD_CMN_MYA-Daily') }}" < "2020-09-02","2020-09-02","{{ ti.xcom_pull(task_ids='InputConfig', key='partition_date', dag_id='LOAD_CMN_MYA-Daily') }}" )
  AND CODIGO_CORREO IN ('2','3','4')) ti on di.tipo_documento= ti.CODIGO_CORREO
where
di.partition_Date="{{ ti.xcom_pull(task_ids='InputConfig', key='partition_date', dag_id='LOAD_CMN_MYA-Daily') }}"





"