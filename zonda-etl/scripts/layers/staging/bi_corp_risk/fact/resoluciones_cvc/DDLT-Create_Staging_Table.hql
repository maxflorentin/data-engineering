CREATE EXTERNAL TABLE IF NOT EXISTS bi_corp_risk.raw_resoluciones_cvc(
                    id string,
                    cod_sucursal string ,
                    nro_solicitud string ,
                    cod_legajo string ,
                    fecha_ingreso string ,
                    resolutor string ,
                    cod_estado_resolucion string ,
                    mot_resolucion_1 string ,
                    mot_resolucion_2 string ,
                    fecha_resolucion string ,
                    elevador string ,
                    cod_estado_elevacion string ,
                    mot_elevacion_1 string ,
                    mot_elevacion_2 string ,
                    fecha_elevacion string ,
                    lim_acte_cvc string ,
                    lim_avis_cvc string ,
                    lim_amas_cvc string ,
                    lim_amex_cvc string ,
                    lim_aptm_cvc string ,
                    lim_per_cvc string ,
                    observaciones_internas string ,
                    comentario_f string ,
                    estado_10 boolean ,
                    estado_12 boolean ,
                    estado_13 boolean ,
                    repetido boolean ,
                    tiempo_respuesta string,
                    comite string )
                  PARTITIONED BY (
                    fecha_importacion string)
                  ROW FORMAT SERDE
                    'org.apache.hadoop.hive.ql.io.parquet.serde.ParquetHiveSerDe'
                  STORED AS INPUTFORMAT
                    'org.apache.hadoop.hive.ql.io.parquet.MapredParquetInputFormat'
                  OUTPUTFORMAT
                    'org.apache.hadoop.hive.ql.io.parquet.MapredParquetOutputFormat'
                  LOCATION
                    '${DATA_LAKE_SERVER}/santander/bi-corp/sandbox/rda/staging/resoluciones_cvc'