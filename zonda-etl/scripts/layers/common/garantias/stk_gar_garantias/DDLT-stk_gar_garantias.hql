CREATE EXTERNAL TABLE IF NOT EXISTS bi_corp_common.stk_gar_garantias(

cod_gar_entidad                         STRING,
cod_gar_num_garantia                    STRING,
cod_gar_garantia                        STRING,
ds_gar_garantia                         STRING,
cod_gar_cla_garantia                    STRING,
ds_gar_cla_garantia                     STRING,
cod_gar_tipo_cobertura                  STRING,
ds_gar_tipo_cobertura                   STRING,
cod_per_nup                             BIGINT,
cod_gar_relacion                        STRING,
cod_gar_sucursal                        INT,
cod_gar_num_cuenta                      BIGINT,
cod_gar_producto                        STRING,
cod_gar_subproducto                     STRING,
dt_gar_fecha_alta                       STRING,
dt_gar_fecha_vencimiento                STRING,
dt_gar_fecha_activacion                 STRING,
cod_gar_divisa                          STRING,
fc_gar_importe_limiteoriginal           DECIMAL(19,4),
fc_gar_importe_limiteactualizado        DECIMAL(19,4),
dt_gar_fecha_cambiolimite               STRING,
flag_gar_bancosor                       INT,
cod_gar_estado                          STRING,
ds_gar_estado                           STRING,
cod_gar_subestado                       STRING,
fc_gar_importe_disponible               DECIMAL(19,4),
fc_gar_importe_alzado                   DECIMAL(19,4),
fc_gar_importe_contable                 DECIMAL(19,4),
dt_gar_fecha_ultimacobertura            STRING,
flag_gar_reconstitucion                 INT,
fc_gar_contrato_valororiginal           DECIMAL(19,4),
cod_gar_contrato_divisaoriginal         STRING,
fc_gar_contrato_valoractualizado        DECIMAL(19,4),
ds_gar_contrato_porcentajecobertura     DECIMAL(19,4),
cod_gar_num_bien                        INT,
cod_gar_bien                            STRING,
ds_gar_bien_descripcion                 STRING,
dt_gar_fecha_inivali                    STRING,
dt_gar_fecha_finvali                    STRING,
fc_gar_bien_valororiginal               DECIMAL(19,4),
cod_gar_bien_divisaoriginal             STRING,
fc_gar_bien_valoractualizado            DECIMAL(19,4),
fc_gar_bien_cambioactualizado           DECIMAL(19,4),
dt_gar_bien_fechaactualizacion          STRING,
ds_gar_bien_porcentajecobertura         DECIMAL(19,4),
cod_gar_tipo_propiedadhip               STRING,
fc_gar_uva_importeoriginal              DECIMAL(19,4),
fc_gar_uva_valororigen                  DECIMAL(19,4),
fc_gar_uva_valoractualizado             DECIMAL(19,4),
dt_gar_uva_fechactualizacion            STRING,
fc_gar_uva_tasacionactualizado          DECIMAL(19,4),
fc_gar_uva_pesos                        DECIMAL(19,4),
cod_gar_divisa_tasacion                 STRING,
fc_gar_importe_tasacionoriginal         DECIMAL(19,4),
fc_gar_importe_tasacion                 DECIMAL(19,4),
cod_gar_divisa_escritura                STRING,
fc_gar_importe_escritura                DECIMAL(19,4),
fc_gar_importe_deuda                    DECIMAL(19,4),
fc_gar_LTV_origen                       DECIMAL(19,4),
fc_gar_LTV_actual                       DECIMAL(19,4),
fc_gar_garantia_porcentajedisponible    DECIMAL(19,4),
fc_gar_calificacion_empresa             DECIMAL(19,4),
fc_gar_calificacion_pais                DECIMAL(19,4),
ds_gar_ubicacion                        STRING

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
  '${DATA_LAKE_SERVER}/santander/bi-corp/common/garantias/fact/stk_gar_garantias'