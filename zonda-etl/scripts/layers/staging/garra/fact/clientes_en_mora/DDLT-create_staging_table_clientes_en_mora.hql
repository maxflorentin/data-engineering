CREATE EXTERNAL TABLE IF NOT EXISTS bi_corp_staging.clientes_en_mora(
    fec_periodo string,
    num_persona string,
    cod_marca_local_cliente string,
    cod_marca_cliente string,
    cod_submarca_cliente string,
    fec_alta_marca_cliente string,
    fec_alta_submarca_cliente string,
    cod_gestor string,
    cod_sit_irregular string,
    cod_grado_feve string,
    imp_riesgo string,
    imp_sit_irregular string,
    cod_topn string,
    cod_kgl string,
    primer_apellido_cliente string,
    nom_nombre string,
    cod_tip_persona string,
    cod_tip_documento string,
    num_documento string,
    cod_actividad string,
    cod_segmento string,
    cod_subsegmento string,
    cod_marca_pyme string,
    cod_uc_banca string,
    cod_uc_division string,
    cod_uc_suc_admin string,
    cod_marca_empleado string,
    tip_sociedad string,
    domicilio string,
    localidad string,
    cod_provincia string,
    cod_pais_residencia string,
    cod_postal string,
    sexo_cliente string,
    fec_alta_cliente string,
    fec_nacimiento string,
    telefono string,
    cod_estado_civil_cliente string,
    nacionalidad_cliente string,
    imp_ingresos_brutos string,
    imp_ingr_grupo_fam string,
    imp_gastos string,
    imp_ingreso string,
    profesion_cliente string,
    cod_tip_actividad string,
    cod_cargo_empresa string,
    nvl_de_estudio string,
    cod_entidad_prevision string,
    cod_ramo_empresa string,
    ind_resp_iva string,
    ind_resp_gan string,
    ind_resp_ing string,
    fec_ult_acred string,
    tip_paquete string,
    ind_riesgo string,
    marca_bp string
)
PARTITIONED BY (
  partition_date string)
ROW FORMAT SERDE
  'org.apache.hadoop.hive.ql.io.parquet.serde.ParquetHiveSerDe'
STORED AS INPUTFORMAT
  'org.apache.hadoop.hive.ql.io.parquet.MapredParquetInputFormat'
OUTPUTFORMAT
  'org.apache.hadoop.hive.ql.io.parquet.MapredParquetOutputFormat'
LOCATION
  '${DATA_LAKE_SERVER}/santander/bi-corp/staging/garra/fact/clientes_en_mora'
