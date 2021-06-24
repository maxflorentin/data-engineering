 CREATE EXTERNAL TABLE IF NOT EXISTS bi_corp_risk.stock_participantes(
            cod_sucursal string ,
            nro_solicitud string ,
            nro_participante string ,
            nom_nombre string ,
            nom_apellido string ,
            tpo_doc string ,
            nro_doc string ,
            fec_nacimiento string ,
            mar_sexo string ,
            cod_nacionalidad string ,
            cod_estado_civil string ,
            cod_nivel_estudio string ,
            cod_rol_en_soli string ,
            cod_profesion string ,
            mar_cliente string ,
            indicador_riesgo string ,
            nup_empresa_asociada string ,
            cod_informe_veraz string ,
            ide_nup string )
          PARTITIONED BY (
            fecha string)
          ROW FORMAT SERDE
            'org.apache.hadoop.hive.ql.io.parquet.serde.ParquetHiveSerDe'
          STORED AS INPUTFORMAT
            'org.apache.hadoop.hive.ql.io.parquet.MapredParquetInputFormat'
          OUTPUTFORMAT
            'org.apache.hadoop.hive.ql.io.parquet.MapredParquetOutputFormat'
          LOCATION
            '${DATA_LAKE_SERVER}/santander/bi-corp/sandbox/rda/staging/stock_participantes'