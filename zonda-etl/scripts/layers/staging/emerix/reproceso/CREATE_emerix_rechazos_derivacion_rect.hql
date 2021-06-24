CREATE EXTERNAL TABLE bi_corp_staging.emerix_rechazos_derivacion_rect (
  zona_suc_adm STRING,
  suc_adm STRING,
  sucursal_nombre STRING,
  posicion STRING,
  tipo_doc STRING,
  num_doc STRING,
  gestor STRING,
  nombre_gestor STRING,
  num_cliente STRING,
  apellido_cliente STRING,
  nombre_cliente STRING,
  banca STRING,
  califacion STRING,
  fecha_ingreso_emerix STRING,
  riesgo_total STRING,
  contrato STRING,
  estado_contable STRING,
  alta_cuenta STRING,
  marca STRING,
  sub_marca STRING,
  garantia STRING,
  fecha_vencimiento_cuenta STRING,
  deuda_venc STRING,
  codigo_estado STRING,
  nombre_estado STRING,
  codigo_escenario STRING,
  nombre_escenario STRING,
  dias_mora_contrato STRING,
  cantidad_dias_en_estado STRING,
  cantidad_dias_en_escenario STRING,
  prod STRING,
  segmento STRING,
  paquete STRING
)
PARTITIONED BY (
  partition_date STRING
)
STORED AS PARQUET
LOCATION '${DATA_LAKE_SERVER}/santander/bi-corp/staging/emerix/fact/emerix_rechazos_derivacion_rect'
;