CREATE EXTERNAL TABLE IF NOT EXISTS bi_corp_risk.raw_stock_solicitudes(
                cod_sucursal string ,
                nro_solicitud string ,
                actividad string ,
                mar_plan_sueldo string ,
                cod_prom_scor string ,
                cod_canal string ,
                valor_bien string ,
                monto_solicitado string ,
                cant_cuotas string ,
                importe_cuota string ,
                monto_ingreso_total_grupo string ,
                afectacion_minima string ,
                val_rel_cuota_ingreso string ,
                endeudamiento_revolving string ,
                marca string ,
                modelo string ,
                cod_concesionario string ,
                cod_estado_sw string ,
                cod_estado_srs string ,
                des_obs_scor string ,
                motivos_scoring string ,
                num_score string)
            PARTITIONED BY (
              fecha string)
            ROW FORMAT SERDE
              'org.apache.hadoop.hive.ql.io.parquet.serde.ParquetHiveSerDe'
            STORED AS INPUTFORMAT
              'org.apache.hadoop.hive.ql.io.parquet.MapredParquetInputFormat'
            OUTPUTFORMAT
              'org.apache.hadoop.hive.ql.io.parquet.MapredParquetOutputFormat'
            LOCATION
              '${DATA_LAKE_SERVER}/santander/bi-corp/sandbox/rda/staging/stock_solicitudes'