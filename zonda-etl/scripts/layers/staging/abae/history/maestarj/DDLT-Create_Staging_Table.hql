CREATE EXTERNAL TABLE IF NOT EXISTS bi_corp_staging.tmae_blco_hist
    (
        nro_tarjeta string,
        sucursal string,
        estado string,
        tipo string,
        clase string,
        fealta string,
        febaja string,
        fevto string,
        nro_tar_ant string,
        cod_comercial string,
        ide_comercial string,
        entidad_ppal string,
        centro_ppal string,
        cuenta_ppal string,
        divisa_ppal string,
        firmante_ppal string,
        penumper_firmante string,
        penumper string,
        caf_ult string,
        caf_orig string,
        miembro string,
        numero string,
        producto string,
        subproducto string,
        cantidad string,
        ticu string,
        fec_orig string,
        fec_caf_ult string,
        fec_carga string,
        fecha_emboza string,
        marca_otp string,
        fec_otp_alta string,
        fec_otp_ant_modif string,
        fec_otp_ult_modif string,
        marca_coordenadas string,
        fec_coord_alta string,
        fec_coord_ant_modif string,
        fec_coord_ult_modif string,
        marca_clave_robusta string,
        fec_cl_robus_alta string,
        fec_cl_robus_ant_modif string,
        fec_cl_robus_ult_modif string,
        marca_tarjeta_con_chip string
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
      '${DATA_LAKE_SERVER}/santander/bi-corp/staging/abae/history/tmae_blco_hist/';
