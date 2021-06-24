CREATE EXTERNAL TABLE IF NOT EXISTS bi_corp_common.dim_deli_cp_clientes (
               cod_deli_cliente int
              ,cod_per_nup int
              ,ds_deli_nombres_cliente string
              ,ds_deli_apellidos_cliente string
              ,cod_deli_tipo_doc int
              ,cod_nro_documento_cliente int
              ,dt_deli_nacimiento_cliente TIMESTAMP
              ,cod_deli_genero_cliente int
              ,partition_date string

)
STORED AS PARQUET
LOCATION
  '${DATA_LAKE_SERVER}/santander/bi-corp/common/delivery/dim/dim_deli_cp_clientes'
;