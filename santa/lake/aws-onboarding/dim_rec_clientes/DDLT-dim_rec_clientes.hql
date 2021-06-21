CREATE EXTERNAL TABLE IF NOT EXISTS bi_corp_common.dim_rec_clientes (
                cod_rec_cliente string
                ,cod_per_nup int
                ,ds_rec_tipodoc string
                ,ds_rec_numdoc bigint
                ,cod_rec_secdocumento string
                ,dt_rec_fechanacimiento string
                ,cod_rec_sexo string
                ,cod_rec_tipopersona string
                ,ds_rec_nombre_razonsocial string
				,ds_rec_apellido_nom_fantasia string
				,ds_rec_mail string
				,ds_rec_celular_ddn string
				,ds_rec_nro_celular string
				,ds_rec_empresa_celular string
				,cod_rec_segmento string
				,ds_rec_mail2 string
				,cod_rec_usuario string
				,ts_rec_alta timestamp
				,cod_rec_usuario_modif string
				,ts_rec_modif timestamp
				,ts_rec_baja timestamp
				,cod_rec_direccion string
				,cod_rec_subsegmento string
				,cod_suc_sucursaladministradora int
				,partition_date string
)
STORED AS PARQUET
LOCATION
  '/santander/bi-corp/common/reclamos/dim_rec_clientes';