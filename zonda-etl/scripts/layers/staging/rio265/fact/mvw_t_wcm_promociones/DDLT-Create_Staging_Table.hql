CREATE EXTERNAL TABLE IF NOT EXISTS bi_corp_staging.rio265_mvw_t_wcm_promociones(
            idpromo string ,
            idtipopromo string ,
            titulo string ,
            descripcion string ,
            perfil string ,
            nombreempresa string ,
            descripcionmarca string ,
            aplicapais string ,
            shopping string ,
            textomapa string ,
            comunicacionmarcas string ,
            beneficiomobile string ,
           --  fechadesde string ,
            fechahasta string ,
            imagen string ,
            textoimagen string ,
            descripcionmobile string ,
            descripcionhomemobile string ,
            destacadohomesr string ,
            destacadohomestr string ,
            destacadodasboard string ,
            destacadohomemobile string ,
            infobeneficiodestacadashometsr string ,
            infobeneficiodesthometsrlinea2 string ,
            infobeneficiodesthometsrlinea3 string ,
            infobenefdbycrossellinglinea1 string ,
            infobenefdbycrossellinglinea2 string ,
            infobeneficiolegalbox string ,
            infobenefdesthomesrlinea1 string ,
            infobenefdesthomesrlinea2 string ,
            infobenefdesthomesrlinea3 string ,
            descripcionsrhome string ,
            infobeneficiodiabox string ,
            fechapublicidad string ,
            fechacaducidad string ,
            rubros string ,
            sucursales string ,
            legales_por_promocion string ,
            beneficios string ,
            plan string ,
            fechaespectaculo string ,
            lugarespectaculo string ,
            textopreventa string ,
            direccionevento string ,
            latitud string ,
            longitud string ,
            calle string ,
            nro string ,
            barrio string ,
            localidad string ,
            provincia string ,
            pais string ,
            telefonolugarevento string ,
            horarioatencion string ,
            textourlevento string ,
            empresaticketera string ,
            punto_de_venta string ,
            tipo_de_oferta string ,
            ultima_modificacion string ,
            nombre string ,
            activa string ,
            idcampana string ,
            idwcm string
        )
    PARTITIONED BY (
      fechadesde string)
    ROW FORMAT SERDE
      'org.apache.hadoop.hive.ql.io.parquet.serde.ParquetHiveSerDe'
    STORED AS INPUTFORMAT
      'org.apache.hadoop.hive.ql.io.parquet.MapredParquetInputFormat'
    OUTPUTFORMAT
      'org.apache.hadoop.hive.ql.io.parquet.MapredParquetOutputFormat'
    LOCATION
      '${DATA_LAKE_SERVER}/santander/bi-corp/staging/rio265/fact/mvw_t_wcm_promociones'