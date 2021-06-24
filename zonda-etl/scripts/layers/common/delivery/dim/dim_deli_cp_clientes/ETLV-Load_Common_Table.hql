set hive.merge.mapfiles=true;
set hive.merge.mapredfiles=true;
set hive.merge.size.per.task=4000000;
set hive.merge.smallfiles.avgsize=16000000;
set hive.exec.dynamic.partition.mode=nonstrict;
set mapred.job.queue.name=root.dataeng;

insert overwrite table bi_corp_common.dim_deli_cp_clientes
Select

              trim(cod_cliente) as cod_deli_cliente
              ,nup as cod_per_nup
              ,nombres as ds_deli_nombres_cliente
              ,apellidos as ds_deli_apellidos_cliente
              ,cod_tipo_doc as cod_deli_tipo_doc
              ,nro_documento as cod_nro_documento_cliente
              ,fecha_nacimiento as dt_deli_nacimiento_cliente
              ,cod_genero as cod_deli_genero_cliente
              ,'{{ ti.xcom_pull(task_ids='InputConfig', key='partition_date', dag_id='LOAD_CMN_Delivery-Daily') }}' as partition_date
From
        (
        select
                 A.cod_cliente
                ,A.nup
                ,A.nombres
                ,A.apellidos
                ,A.cod_tipo_doc
                ,A.nro_documento
                ,A.fecha_nacimiento
                ,A.cod_genero
                ,A.ultima_modif as ultima_modificacion

        from  bi_corp_staging.rio258_Cp_Clientes A
        inner join
					(
					 select
						 cod_cliente,
						 max(ultima_modif) ultima_modif
					 from  bi_corp_staging.rio258_Cp_Clientes
					 group by cod_cliente
					 ) B -- Fix por que el cod_cliente puede tener en su historia mas de un nup
		on A.cod_cliente= B.cod_cliente
		and A.ultima_modif=B.ultima_modif
		where A.ultima_modif <='{{ ti.xcom_pull(task_ids='InputConfig', key='partition_date', dag_id='LOAD_CMN_Delivery-Daily') }}'
        ) A
   ;