SET mapred.job.queue.name=root.dataeng;
create view bi_corp_risk.pendientes_scoring
as
                select
                cast (id as int )id,
                cast(cod_sucursal as smallint) cod_sucursal ,
                cast(nro_solicitud as bigint) nro_solicitud ,
                fecha_analisis  ,
                fecha_ingreso  ,
                cast(tiempo_horas as int) tiempo_horas,
                cast(tiempo_dias as int ) tiempo_dias,
                fecha_importacion
                from bi_corp_risk.raw_pendientes_scoring
;