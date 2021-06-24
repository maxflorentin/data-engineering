SET mapred.job.queue.name=root.dataeng;
Create view bi_corp_risk.entradas as
Select
            periodo   ,
            nup   ,
            sucursal   ,
            nro_cuenta   ,
            codigo_producto   ,
            codigo_subproducto   ,
            divisa   ,
            fecha_situacion_irregular   ,
            cast(dias_atraso as int)  dias_atraso,
            marca   ,
            submarca   ,
            importe_entrada   ,
            tipo_movimiento   ,
            codigo_segmento   ,
            descripcion_segmento   ,
            detalle_renta   ,
            tipo_producto   ,
            descripcion_producto   ,
            tipo_reestructuracion   ,
            contrato_citi   ,
          fecha_ejecucion_garra
From bi_corp_risk.raw_entradas;

