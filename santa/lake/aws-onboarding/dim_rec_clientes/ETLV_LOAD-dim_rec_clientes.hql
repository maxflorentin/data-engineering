set hive.execution.engine=spark;
set spark.yarn.queue=root.dataeng;
set hive.exec.dynamic.partition.mode=nonstrict;

INSERT
	OVERWRITE TABLE bi_corp_common.dim_rec_clientes
SELECT
    case when trim(id_cliente)='null' then null else trim(id_cliente) end as cod_rec_cliente,
    case when trim(nup_cliente)='null' then null else trim(nup_cliente) end  as cod_per_nup,
    case when trim(tipo_documento)='null' then null else trim(tipo_documento) end  as ds_rec_tipodoc,
    case when trim(nro_documento)='null' then null else trim(nro_documento) end  as ds_rec_numdoc,
    case when trim(sec_documento)='null' then null else trim(sec_documento) end  as cod_rec_secdocumento,
    to_date(fecha_nacimiento) as dt_rec_fechanacimiento,
    case when trim(sexo)='null' then null else trim(sexo) end  as cod_rec_sexo,
    case when trim(tipo_persona)='null' then null else trim(tipo_persona) end  as cod_rec_tipopersona,
    case when trim(nombre_razon_social)='null' then null else trim(nombre_razon_social) end  as ds_rec_nombre_razonsocial,
    case when trim(apellido_nom_fantasia)='null' then null else trim(apellido_nom_fantasia) end  as ds_rec_apellido_nom_fantasia,
    case when trim(email)='null' then null else trim(email) end  as ds_rec_mail,
    case when trim(celular_ddn)='null' then null else trim(celular_ddn) end  as ds_rec_celular_ddn,
    case when trim(celular_nro)='null' then null else trim(celular_nro) end  as ds_rec_nro_celular,
    case when trim(celular_empresa)='null' then null else trim(celular_empresa) end  as ds_rec_empresa_celular,
    case when trim(cod_segmento)='null' then null else trim(cod_segmento) end  as cod_rec_segmento,
    case when trim(email_2)='null' then null else trim(email_2) end  as ds_rec_mail2,
    case when trim(id_usuario_alta)='null' then null else trim(id_usuario_alta) end  as cod_rec_usuario,
    case when trim(fecha_alta)='null' then null else trim(fecha_alta) end  as ts_rec_alta,
    case when trim(id_usuario_modif)='null' then null else trim(id_usuario_modif) end  as cod_rec_usuario_modif,
    case when trim(fecha_modif)='null' then null else trim(fecha_modif)  end  as ts_rec_modif  ,
    case when trim(fecha_baja)='null' then null else trim(fecha_baja) end  as ts_rec_baja,
    case when trim(direccion)='null' then null else trim(direccion) end  as cod_rec_direccion,
    case when trim(cod_subsegmento)='null' then null else trim(cod_subsegmento) end  as cod_rec_subsegmento,
    case when trim(sucursal_administradora)='null' then null else trim(sucursal_administradora) end  as cod_suc_sucursaladministradora,
    partition_date
FROM
	bi_corp_staging.rio187_clientes clientes
WHERE partition_date = '{{ ti.xcom_pull(task_ids='InputConfig', key='max_partition_rio187_clientes', dag_id='LOAD_CMN_Cosmos') }}';