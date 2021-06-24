 CREATE EXTERNAL TABLE IF NOT EXISTS bi_corp_risk.pendientes_sri(
                     id string,
                    cod_sucursal string ,
                    nro_solicitud string ,
                    cod_legajo string ,
                    fecha_analisis string ,
                    fecha_ingreso string ,
                    fecha_elevacion string ,
                    fecha_resolucion string ,
                    tiempo_horas string ,
                    tiempo_dias string ,
                    cod_usuario string )
                PARTITIONED BY (
                  fecha_importacion string)
                ROW FORMAT SERDE
                  'org.apache.hadoop.hive.ql.io.parquet.serde.ParquetHiveSerDe'
                STORED AS INPUTFORMAT
                  'org.apache.hadoop.hive.ql.io.parquet.MapredParquetInputFormat'
                OUTPUTFORMAT
                  'org.apache.hadoop.hive.ql.io.parquet.MapredParquetOutputFormat'
                LOCATION
                  '${DATA_LAKE_SERVER}/santander/bi-corp/sandbox/rda/staging/pendientes_sri'