set hive.merge.mapfiles=true;
set hive.merge.mapredfiles=true;
set hive.merge.size.per.task=4000000;
set hive.merge.smallfiles.avgsize=16000000;
set hive.exec.dynamic.partition.mode=nonstrict;
SET mapred.job.queue.name=root.dataeng;

INSERT OVERWRITE TABLE bi_corp_common.stk_adm_empresas_feve_acta_revision
    PARTITION (partition_date='{{ ti.xcom_pull(task_ids='InputConfig', key='partition_date', dag_id='LOAD_CMN_Admision_Empresas-Daily') }}')
select
    cast(numero_acta as bigint) as cod_adm_numero_acta,
    nombre_acta as ds_adm_nombre_acta,
    cast(estado_acta as int) as cod_adm_estado_acta,
    codigo_banca as cod_adm_banca,
    date_format(fecha_realizacion,'yyyy-MM-dd') as dt_adm_realizacion,
    nombre_centro as ds_adm_nombre_centro,
    servicios_centrales_codigo as cod_adm_servicios_centrales,
    servicios_centrales_desc as ds_adm_servicios_centrales,
    cast(gestor as int) as cod_adm_gestor,
    cast(idsucursal as int) as id_adm_sucursal,
    cast(tipo_comite as int) as cod_adm_tipo_comite,
    segmento_tutela as cod_adm_segmento_tutela,
    codigo_instancia as cod_adm_instancia,
    orden_del_dia as ds_adm_orden_del_dia,
    observaciones_generales as ds_adm_observaciones_generales,
    comentarios_acta as ds_adm_comentarios_acta,
    cast(cant_empresas_analizadas as int) as int_adm_cant_empresas_analizadas,
    cast(total_dispuesto as decimal(17, 4)) as fc_adm_total_dispuesto,
    cast(num_tot_clientes_analizados as int) as int_adm_num_tot_clientes_analizados,
    cast(num_clientes_afianzar as int) as int_adm_num_clientes_afianzar,
    cast(num_clientes_seguir as int) as int_adm_num_clientes_seguir,
    cast(num_clientes_extinguir as int) as int_adm_num_clientes_extinguir,
    cast(num_clientes_reducir as int) as int_adm_num_clientes_reducir,
    cast(limites as int) as int_adm_limites,
    cast(porcentaje_revisados as int) as int_adm_porcentaje_revisados,
    cast(num_salidas as int) as int_adm_num_salidas,
    cast(num_cliente_mej_grado as int) as int_adm_num_cliente_mej_grado,
    cast(num_cleinte_mej_valoracion as int) as int_adm_num_cleinte_mej_valoracion,
    cast(num_cliente_deterioro as int) as int_adm_num_cliente_deterioro,
    date_format(fecha_creacion,'yyyy-MM-dd') as dt_adm_creacion,
    date_format(fecha_modificacion,'yyyy-MM-dd') as dt_adm_modificacion,
    usuario_creacion as cod_adm_usuario_creacion,
    usuario_modificacion as cod_adm_usuario_modificacion,
    numero_acta_gys as cod_adm_numero_acta_gys
from bi_corp_staging.sge_feve_acta_revision
WHERE partition_date = '{{ ti.xcom_pull(task_ids='InputConfig', key='partition_date', dag_id='LOAD_CMN_Admision_Empresas-Daily') }}';