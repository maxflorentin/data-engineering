
CREATE EXTERNAL TABLE IF NOT EXISTS bi_corp_common.bt_tcr_movimientos_liquidados
(
    cod_tcr_operacion         int,
    cod_tcr_cuenta            bigint,
    cod_nro_cuenta            bigint,
    cod_tcr_tarjeta           bigint,
    cod_tcr_tipo_cuenta       string,
    cod_per_nup_tarjeta       int,
    cod_per_nup_cuenta        int,
    cod_tcr_entidad           string,
    cod_suc_sucursal          int,
    cod_tcr_marca_tarjeta     string,
    cod_tcr_comprobante       int,
    cod_div_divisa            string,
    cod_tcr_grupo_afip        int,
    cod_tcr_cartera           int,
    cod_tcr_establecimiento   bigint,
    cod_tcr_origen            string,
    ds_tcr_mk_agro          string,
    ds_tcr_mk_final         string,
    fc_tcr_importe            decimal(15, 2),
    fc_tcr_importe_arp        decimal(15, 2),
    fc_tcr_importe_usd        decimal(15, 2),
    dt_tcr_fecha_presentacion string,
    dt_tcr_fecha_origen string
)
PARTITIONED BY (
    partition_date       string)
ROW FORMAT SERDE
    'org.apache.hadoop.hive.ql.io.parquet.serde.ParquetHiveSerDe'
STORED AS INPUTFORMAT
    'org.apache.hadoop.hive.ql.io.parquet.MapredParquetInputFormat'
OUTPUTFORMAT
    'org.apache.hadoop.hive.ql.io.parquet.MapredParquetOutputFormat'
LOCATION
    '${DATA_LAKE_SERVER}/santander/bi-corp/common/tarjetas/fact/bt_tcr_movimientos_liquidados'