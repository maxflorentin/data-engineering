SET mapred.job.queue.name=root.dataeng;
create view bi_corp_risk.stock_participantes
as
        Select 
            cast(cod_sucursal as int) cod_sucursal,
            cast(nro_solicitud as bigint) nro_solicitud ,
            cast(nro_participante as smallint) nro_participante ,
            nom_nombre  ,
            nom_apellido  ,
            cast(tpo_doc as smallint) tpo_doc ,
            cast(nro_doc as bigint) nro_doc ,
            fec_nacimiento  ,
            mar_sexo  ,
            cast(cod_nacionalidad as smallint) cod_nacionalidad ,
            cod_estado_civil  ,
            cod_nivel_estudio  ,
            cod_rol_en_soli  ,
            cod_profesion  ,
            mar_cliente  ,
            indicador_riesgo  ,
            cast(nup_empresa_asociada as bigint) nup_empresa_asociada ,
            cod_informe_veraz  ,
            cast(ide_nup as bigint) ide_nup ,
            cast(fecha as string) fecha
    from bi_corp_risk.raw_stock_participantes  ;