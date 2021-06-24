CREATE EXTERNAL TABLE IF NOT EXISTS bi_corp_common.stk_adm_empresas_feve_acta_revision (
    cod_adm_numero_acta bigint,
    ds_adm_nombre_acta string,
    cod_adm_estado_acta int,
    cod_adm_banca string,
    dt_adm_realizacion string,
    ds_adm_nombre_centro string,
    cod_adm_servicios_centrales string,
    ds_adm_servicios_centrales string,
    cod_adm_gestor int,
    id_adm_sucursal int,
    cod_adm_tipo_comite int,
    cod_adm_segmento_tutela string,
    cod_adm_instancia string,
    ds_adm_orden_del_dia string,
    ds_adm_observaciones_generales string,
    ds_adm_comentarios_acta string,
    int_adm_cant_empresas_analizadas int,
    fc_adm_total_dispuesto decimal(17, 4),
    int_adm_num_tot_clientes_analizados int,
    int_adm_num_clientes_afianzar int,
    int_adm_num_clientes_seguir int,
    int_adm_num_clientes_extinguir int,
    int_adm_num_clientes_reducir int,
    int_adm_limites int,
    int_adm_porcentaje_revisados int,
    int_adm_num_salidas int,
    int_adm_num_cliente_mej_grado int,
    int_adm_num_cleinte_mej_valoracion int,
    int_adm_num_cliente_deterioro int,
    dt_adm_creacion string,
    dt_adm_modificacion string,
    cod_adm_usuario_creacion string,
    cod_adm_usuario_modificacion string,
    cod_adm_numero_acta_gys string
)
PARTITIONED BY (
  partition_date string)
STORED AS PARQUET
LOCATION
  '${DATA_LAKE_SERVER}/santander/bi-corp/common/admision/stk_adm_empresas_feve_acta_revision';