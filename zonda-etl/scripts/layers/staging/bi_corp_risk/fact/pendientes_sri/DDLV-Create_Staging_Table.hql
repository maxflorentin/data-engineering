SET mapred.job.queue.name=root.dataeng;
create view bi_corp_risk.pendientes_sri
as
            select cast(id as int) id,
                    cast(cod_sucursal as smallint) cod_sucursal ,
                    cast(nro_solicitud as bigint) nro_solicitud ,
                    cod_legajo  ,
                    fecha_analisis  ,
                    fecha_ingreso  ,
                    fecha_elevacion  ,
                    fecha_resolucion  ,
                    cast(tiempo_horas as int) tiempo_horas ,
                    cast(tiempo_dias as int) tiempo_dias ,
                    cod_usuario ,
                    fecha_importacion
                   from bi_corp_risk.raw_pendientes_sri;