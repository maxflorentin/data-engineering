SET mapred.job.queue.name=root.dataeng;
create view bi_corp_risk.stock_solicitudes as
                select
                cast(cod_sucursal as int) cod_sucursal,
                cast(nro_solicitud as bigint) nro_solicitud ,
                actividad   ,
                mar_plan_sueldo   ,
                cod_prom_scor   ,
                cod_canal   ,
                cast(valor_bien as int) valor_bien ,
                cast(monto_solicitado as int ) monto_solicitado,
                cast(cant_cuotas as smallint) cant_cuotas ,
                cast(importe_cuota as int) importe_cuota ,
                cast(monto_ingreso_total_grupo as int ) monto_ingreso_total_grupo,
                cast(afectacion_minima as int) afectacion_minima ,
                cast(val_rel_cuota_ingreso as int) val_rel_cuota_ingreso ,
                cast(endeudamiento_revolving as int) endeudamiento_revolving ,
                marca   ,
                modelo   ,
                cod_concesionario   ,
                cod_estado_sw   ,
                cod_estado_srs   ,
                des_obs_scor   ,
                motivos_scoring   ,
                cast(num_score as smallint) num_score,
               fecha  
               from bi_corp_risk.raw_stock_solicitudes;