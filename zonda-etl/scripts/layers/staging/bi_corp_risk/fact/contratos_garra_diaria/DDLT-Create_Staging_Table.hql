
CREATE EXTERNAL TABLE IF NOT EXISTS bi_corp_risk.contratos_garra_diaria(
    nup int ,
    sucursal int ,
    nro_cuenta double ,
    codigo_producto string ,
    codigo_subproducto string ,
    divisa string ,
    fecha_situacion_irregular string ,
    fecha_dudosidad string ,
    marca string ,
    submarca string ,
    importe_riesgo double ,
    importe_irregular double ,
    importe_castigo double ,
    importe_pendiente double ,
    importe_quita double ,
    num_posicion string ,
    fecha_alta_registro string ,
    fecha_marca_submit string ,
    fecha_alta_producto string ,
    importe_capital_vencido double ,
    importe_interes_vencido double ,
    importe_otros double ,
    importe_capital_a_vencer double ,
    fecha_vencimiento_producto string ,
    num_cuota_actual int ,
    num_cuota_total int ,
    codigo_amortizacion string ,
    destino_fondo int ,
    fecha_cambio_clasificacion_reestructuracion string ,
    motivo_riesgo_sub_estado string ,
    indica_arrastre string ,
    fecha_inicio_gracia string ,
    fecha_fin_gracia string ,
    plazo int ,
    dias_atraso int ,
    cuotas_pagas_consecutivas int ,
    clase_garantia string ,
    normalizado string ,
    intereses_ordinarios double ,
    tipo_restructuracion string ,
    marca_subjetiva string ,
    proceso_de_normalizacion string ,
    marca_de_incumplimiento string ,
    fecha_de_incumplimiento string ,
    nro_cuenta_substr double ,
    tabla_destino string ,
    clasificacion_reestructuracion string ,
    categoria_producto string ,
    descripcion_segmento string )
PARTITIONED BY (
  fecha_informacion string)
ROW FORMAT SERDE
  'org.apache.hadoop.hive.ql.io.parquet.serde.ParquetHiveSerDe'
STORED AS INPUTFORMAT
  'org.apache.hadoop.hive.ql.io.parquet.MapredParquetInputFormat'
OUTPUTFORMAT
  'org.apache.hadoop.hive.ql.io.parquet.MapredParquetOutputFormat'
LOCATION
  '${DATA_LAKE_SERVER}/santander/bi-corp/sandbox/rda/staging/contratos_garra_diaria'