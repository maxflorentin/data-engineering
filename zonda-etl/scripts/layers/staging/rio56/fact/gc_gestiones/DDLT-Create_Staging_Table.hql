CREATE EXTERNAL TABLE IF NOT EXISTS bi_corp_staging.rio56_gc_gestiones (
    cod_entidad  string,
    ide_gestion_sector  string,
    ide_gestion_nro  string,
    cod_segm  string,
    semf_ingreso_crm  string,
    semf_renta_crm  string,
    semf_riesgo_crm  string,
    calle_dom_contc  string,
    nro_dom_contc  string,
    piso_dom_contc  string,
    dpto_dom_contc  string,
    cpost_dom_contc  string,
    loc_dom_contc  string,
    pcia_dom_contc  string,
    pais_dom_contc  string,
    ddn_tel_dom_contc  string,
    nro_tel_dom_contc  string,
    ddn_fax_contc  string,
    nro_fax_contc  string,
    hra_dom_contc  string,
    ddn_celular_contc  string,
    nro_celular_contc  string,
    ddn_tel_laboral  string,
    nro_tel_laboral  string,
    hra_tel_laboral  string,
    ddn_fax_laboral  string,
    nro_fax_laboral  string,
    aplic_cta  string,
    cod_suc_cta  string,
    nro_cta  string,
    nro_firm_cta  string,
    cod_mone_cta  string,
    cod_prod_cta  string,
    cod_subpro_cta  string,
    nro_tarjeta  string,
    link_resumen  string,
    dire_mail_contc  string,
    dire_mail2_contc  string,
    cartera  string,
    aplic_cta_gold  string,
    cod_suc_cta_gold  string,
    nro_cta_gold  string,
    nro_firm_cta_gold  string,
    cod_mone_cta_gold  string,
    cod_prod_cta_gold  string,
    cod_subpro_cta_gold  string,
    cod_ramo_cta  string,
    nro_certificado_cta  string,
    rentabilidad_promedio  string,
    color_semaforo  string,
    color_semaf_riesgo  string,
    indi_envios_mya  string,
    dire_mail_opcional  string,
    nro_paquete  string,
    cod_paquete  string,
    indi_plan_sueldo  string,
    nro_convenio_paquete  string,
    emp_celular_contc  string,
    tpo_msj_asoc_rta  string,
    monto_deuda_ref  string,
    no_enviar_notificaciones  string,
    id_ejecutivo  string
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
  '${DATA_LAKE_SERVER}/santander/bi-corp/staging/rio56/fact/gc_gestiones'