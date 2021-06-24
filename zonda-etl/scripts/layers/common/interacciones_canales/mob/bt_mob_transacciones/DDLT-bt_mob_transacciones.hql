CREATE EXTERNAL TABLE IF NOT EXISTS BI_CORP_COMMON.BT_MOB_TRANSACCIONES (

ID_MOB_ESTADISTICA              BIGINT,
TS_MOB_FECHA                    STRING,
COD_MOB_TRANSACCION             STRING,
DS_MOB_TRANSACCION              STRING,
DS_MOB_MODULO                   STRING,
FLAG_MOB_HABILITADO             INT,
COD_PER_NUP                     BIGINT,
COD_PER_NUP_EMPRESA             BIGINT,
FC_MOB_IMPORTE_DEBITO           DECIMAL(19,4),
FC_MOB_IMPORTE_CREDITO          DECIMAL(19,4),
COD_MOB_DIVISA                   STRING,
DS_MOB_IP                       STRING,
COD_MOB_ID_TERMINAL             STRING,
COD_MOB_TOKEN                   STRING,
COD_MOB_DISPOSITIVO_IDRUNTIME               STRING,
DS_MOB_DISPOSITIVO_ID                      STRING,
DS_MOB_DISPOSITIVO_MODELO                    STRING,
TS_MOB_DISPOSITIVO_FECHA                    STRING,
DS_MOB_DISPOSITIVO_HW                        STRING,
DS_MOB_DISPOTIVO_MACADRESS               STRING,
DS_MOB_DISPOSITIVO_VERSIONSO               STRING,
DS_MOB_DISPOSITIVO_OSID                    STRING,
DS_MOB_COMPROBANTE              STRING,
COD_MOB_CUENTA_ORIGEN           BIGINT,
COD_MOB_CUENTA_DESTION          BIGINT,
COD_MOB_NRO_SOLICITUD           STRING,
DS_MOB_ESTADO_SOLICITUD         STRING,
TS_MOB_FECHA_INICIOTRN          STRING,
TS_MOB_FECHA_FINTRN             STRING,
DS_MOB_CUIT_EMPRESA             BIGINT,
DS_MOB_DOCUMENTO                BIGINT,
FC_MOB_IMPORTE_DEBITOUSD        DECIMAL(19,4),
DS_MOB_SERVIDOR                 STRING,
COD_MOB_RES                     STRING,
DS_MOB_LATITUD                  STRING,
DS_MOB_LONGITUD                 STRING,
DS_MOB_VERSION                  STRING,
DS_MOB_EMAIL_DEST               STRING,
DS_MOB_DOCUMENTO_DEST           BIGINT,
COD_MOB_DIVISA_DEBITO           STRING,
DS_MOB_COTIZACION               DECIMAL(9,4),
DS_MOB_EMPSERV                  STRING

)
PARTITIONED BY (PARTITION_DATE STRING)
ROW FORMAT SERDE 'org.apache.hadoop.hive.ql.io.parquet.serde.ParquetHiveSerDe'
STORED AS INPUTFORMAT 'org.apache.hadoop.hive.ql.io.parquet.MapredParquetInputFormat'
OUTPUTFORMAT 'org.apache.hadoop.hive.ql.io.parquet.MapredParquetOutputFormat'
LOCATION '${DATA_LAKE_SERVER}/santander/bi-corp/common/mob/fact/bt_mob_transacciones'