SELECT gestiones.ID_GESTION AS cod_rec_gestion
        ,gestiones.ID_CLIENTE AS cod_rec_nup
        ,gestiones.ID_CASUISTICA AS cod_rec_casuistica
        ,current_date AS ds_rec_partition_date
        ,gestiones.APLICACION_CONTRATO AS cod_rec_aplicacion_contrato
        ,gestiones.PRODUCTO_CONTRATO AS cod_rec_producto_contrato
        ,gestiones.SUBPRODUCTO_CONTRATO AS cod_rec_subproducto_contrato
        ,gestiones.SUCURSAL_CONTRATO AS cod_rec_sucursal_contrato
        ,gestiones.NRO_CONTRATO AS cod_rec_nro_contrato
        ,gestiones.DIVISA_CONTRATO AS cod_rec_divisa_contrato
        ,gestiones.MONTO_RECLAMADO AS fc_rec_monto_reclamado
        ,gestiones.MONTO_DEVUELTO AS fc_rec_monto_devuelto
        ,gestiones.COD_MOTIVO AS cod_rec_motivo
        ,gestiones.DESC_MOTIVO AS ds_rec_motivo
        ,gestiones.COMENTARIO AS ds_rec_comentario
        ,gestiones.ESTADO_ACTUAL AS cod_rec_estado_actual
        ,gestiones.ID_USUARIO_ALTA AS cod_rec_usuario_alta
        ,gestiones.FECHA_ALTA AS dt_rec_alta
        ,gestiones.ID_USUARIO_MODIF AS cod_rec_usuario_modif
        ,gestiones.FECHA_MODIF AS dt_rec_modif
        ,gestiones.ACTOR_ACTUAL AS cod_rec_actor_actual
        ,gestiones.PRODUCTO_PAQUETE AS cod_rec_producto_paquete
        ,gestiones.SUBPRODUCTO_PAQUETE AS cod_rec_subproducto_paquete
        ,gestiones.ID_MAIL_RESOLUCION AS cod_rec_mail_resolución
        ,gestiones.COD_GRUPO_MOTDES AS cod_rec_grupo_motdes
        ,gestiones.DIGITO_VERIFICADOR AS cod_rec_digito_verificador
        ,gestiones.ID_USUARIO_ACTUAL AS cod_rec_usuario_actual
        ,gestiones.TIENE_SORPRESA AS flag_rec_tiene_sorpresa
        ,gestiones.MONTO_RECLAMADO_USD AS fc_rec_monto_reclamado_usd
        ,gestiones.MONTO_DEVUELTO_USD AS fc_rec_monto_devuelto_usd
        ,gestiones.FECHA_RESOLUCION AS dt_rec_resolución
        ,gestiones.FECHA_ESTADO_ACTUAL AS dt_rec_estado_actual
        ,gestiones.FECHA_ESTADO_MAX AS dt_rec_estado_max
        ,gestiones.COD_RESPUESTA AS cod_rec_respuesta
        ,gestiones.COD_RESULTADO AS cod_rec_resultado
        ,gestiones.COMENTARIO_RESOLUTOR AS ds_rec_comentario_resolutor
        ,gestiones.FECHA_BAJA AS dt_rec_baja
        ,gestiones.ID_CANAL AS cod_rec_canal
        ,gestiones.IND_FAVORABILIDAD_MOT_DES AS cod_rec_favorabilidad_mot_des
        ,gestiones.IND_FAVORABILIDAD_RESOL AS cod_rec_favorabilidad_resol
        ,gestiones.INFORMA_BCRA AS flag_rec_informa_bcra
        ,gestiones.NRO_GESTION AS cod_rec_nro_gestion
        ,gestiones.COD_RESOLUCION AS cod_rec_resolucion
        ,gestiones.DESC_RESOLUCION AS ds_rec_resolucion
        ,gestiones.NRO_TICKET AS cod_rec_nro_ticket
        ,gestiones.PREFIJO_TICKET AS ds_rec_prefijo_ticket
        ,gestiones.NRO_RECLAMO_BCO AS cod_rec_nro_reclamo_bco
        ,gestiones.ID_IMPUESTO AS cod_rec_impuesto
        ,gestiones.IND_AJUSTE AS flag_rec_ajuste
        ,gestiones.IND_ANTICIPO AS flag_rec_anticipo
        ,gestiones.ORIGEN_RECLAMO AS cod_rec_origen_reclamo
        ,gestiones.ZONA_CONSUMO AS cod_rec_zona_consumo
        ,gestiones.ETAPA_RESOLUCION AS cod_rec_etapa_resolucion
        ,gestiones.COD_SEGMENTO AS cod_rec_segmento
        ,gestiones.COD_SUBSEGMENTO AS cod_rec_subsegmento
        ,gestiones.V_IND_AJUSTE_PROVISORIO AS flag_rec_ajuste_provisorio
        ,gestiones.EJECUTIVO_ON_LINE AS flag_rec_ejecutivo_online
        ,gestiones.MONTO_IVA AS fc_rec_monto_iva
        ,gestiones.COD_SUC_ALTA AS cod_rec_sucursal_alta
        ,gestiones.COD_OFIC_ALTA AS cod_rec_oficial_alta
        ,gestiones.ID_ACTOR_ALTA AS cod_rec_actor_alta
        ,gestiones.ID_REQUERIMIENTO_PREMIACION AS cod_rec_requerimiento_premi
        ,gestiones.CUENTA_PAQUETE AS cod_rec_cuenta_paquete
        ,gestiones.NRO_PAQUETE AS cod_rec_nro_paquete
        ,gestiones.SUCURSAL_PAQUETE AS cod_rec_sucursal_paquete
        ,gestiones.TIPO_PERSONA AS cod_rec_tipo_persona
        ,gestiones.CLIENTE_VIP AS flag_rec_cliente_vip
        ,gestiones.CANT_CHEQUE_RECLAMADO AS fc_rec_cantidad_cheque_rec
        ,gestiones.FECHA_HORA_OPERACION AS dt_rec_operacion
        ,gestiones.COD_SUC_RESOL AS cod_rec_sucursal_resol
        ,gestiones.COD_ZONA_RESOL AS cod_rec_zona_resol
        ,gestiones.COD_BCO_EMISOR AS cod_rec_banco_emisor
        ,actor_actual.DESC_ACTOR AS ds_rec_actor_actual
        ,actor_alta.TIPO_ACTOR AS ds_rec_tipo_actor_alta
        ,actor_alta.DESC_ACTOR AS ds_rec_actor_alta
        ,casuisticas.DESC_CASUISTICA AS ds_rec_casuisticas
        ,casuisticas.ID_TIPO_GESTION AS cod_rec_tipo_gestion
        ,casuisticas.ID_TIPOLOGIA AS cod_rec_tipologia
        ,casuisticas.CANT_MESES_PRIMER_RECLAMO AS fc_rec_cantid_meses_pri_rec
        ,casuisticas.CANT_DIAS_RESOLUCION AS fc_rec_cantidad_dias_resol
        ,casuisticas.TIPO_PRODUCTO AS cod_rec_tipo_producto
        ,casuisticas.ID_SECTOR_SOPORTE AS cod_rec_sector_soporte
        ,Tipos_Gestion.DESC_TIPO_GESTION AS ds_rec_tipo_gestion
        ,Tipos_Gestion.INFORMA_BCRA AS flag_rec_informa_bcra
        ,Tipos_Gestion.PREFIJO_TICKET AS ds_rec_prefijo_ticket
        ,Tipos_Gestion.ID_TIPOS_GESTION_SGC AS cod_rec_tipos_gestion_sgc
        ,Canales.DESC_CANAL AS ds_rec_canales
        ,Usuarios_Alta.NOMBRE AS ds_rec_usuario_alta_nombre
        ,Usuarios_Alta.APELLIDO AS ds_rec_usuario_alta_apellido
        ,Centro_Costos_Alta.DESCRIPCION AS ds_rec_centro_costo_usu_alta
        ,Usuarios_Modif.NOMBRE AS ds_rec_usuario_modif_nombre
        ,Usuarios_Modif.APELLIDO AS ds_rec_usuario_modif_apellido
        ,Centro_Costos_Alta.DESCRIPCION AS ds_rec_centro_costo_usu_modif
        ,Usuarios_Alta.NOMBRE AS ds_rec_usuario_actual_nombre
        ,Usuarios_Alta.APELLIDO AS ds_rec_usuario_actual_apellido
        ,Centro_Costos_Alta.DESCRIPCION AS ds_rec_centro_costo_usu_actual
        /*,Gest_Resol_Acciones.ID_ACCION AS cod_rec_accion_resol
        ,Gest_Resol_Acciones.ID_ACCION AS cod_rec_accion_motdes*/
        ,Motdes_Arbol_Resultados.DESC_RESULTADO AS ds_rec_resultado_arbol_resul
        ,Motdes_Arbol_Resultados.TEXTO_MENSAJE AS ds_rec_texto_msj_arbol_resul
        ,Motdes_Arbol_Resultados.IND_FAVORABILIDAD as flag_rec_favorab_arbol_resul
        ,'Calculo a Realizar' AS fc_rec_cant_minutos_resolucion    

FROM bi_corp_staging.cosmos_gestiones gestiones --CSM.Gestiones gestiones
    
LEFT JOIN bi_corp_staging.cosmos_actores actor_actual   --CSM.actores actor_actual
ON gestiones.ac  gestiones.ACTOR_ACTUAL=actor_actual.ID_ACTOR
    
LEFT JOIN CSM.actores actor_alta
ON gestiones.ID_ACTOR_ALTA=actor_alta.ID_ACTOR
    
INNER JOIN CSM.casuisticas casuisticas
ON gestiones.ID_CASUISTICA=casuisticas.ID_CASUISTICA

LEFT JOIN CSM.Tipos_Gestion Tipos_Gestion
ON casuisticas.ID_TIPO_GESTION=Tipos_Gestion.ID_TIPO_GESTION

LEFT JOIN CSM.Canales Canales
ON gestiones.ID_CANAL=Canales.ID_CANAL

LEFT JOIN CSM.Usuarios Usuarios_Alta
ON gestiones.ID_USUARIO_ALTA=Usuarios_Alta.ID_USUARIO

LEFT JOIN CSM.Usuarios Usuarios_Modif
ON gestiones.ID_USUARIO_MODIF=Usuarios_Modif.ID_USUARIO

LEFT JOIN CSM.Usuarios Usuarios_Actual
ON gestiones.ID_USUARIO_ACTUAL=Usuarios_Actual.ID_USUARIO

LEFT JOIN CSM.Centro_Costos Centro_Costos_Alta
on Usuarios_Alta.COD_OFICINA=Centro_Costos_Alta.COD_OFICINA
and Usuarios_Alta.COD_SUCURSAL=Centro_Costos_Alta.COD_SUCURSAL

LEFT JOIN CSM.Centro_Costos Centro_Costos_Modif
on Usuarios_Modif.COD_OFICINA=Centro_Costos_Modif.COD_OFICINA
and Usuarios_Modif.COD_SUCURSAL=Centro_Costos_Modif.COD_SUCURSAL


LEFT JOIN CSM.Centro_Costos Centro_Costos_Actual
on Usuarios_Actual.COD_OFICINA=Centro_Costos_Actual.COD_OFICINA
and Usuarios_Actual.COD_SUCURSAL=Centro_Costos_Actual.COD_SUCURSAL


LEFT JOIN CSM.Motdes_Arbol_Resultados Motdes_Arbol_Resultados
ON gestiones.ID_CASUISTICA=Motdes_Arbol_Resultados.ID_CASUISTICA
AND gestiones.COD_RESULTADO=Motdes_Arbol_Resultados.COD_RESULTADO




/* Tablas carga inicial
actores 
canales 
casuistica_motivos 
casuisticas 
centro_costos 
codigos_operativos_cuenta 
codigos_operativos_tarjeta 
motdes_arbol_resultados 
tipos_gestion 

SELECT * FROM bi_corp_staging.cosmos_usuarios LIMIT 10; --OK
SELECT * FROM bi_corp_staging.cosmos_cliente_domicilios LIMIT 10; --OK
SELECT * FROM bi_corp_staging.cosmos_clientes LIMIT 10; --OK
SELECT * FROM bi_corp_staging.cosmos_gest_estados LIMIT 10; --OK
SELECT * FROM bi_corp_staging.cosmos_gest_motdes_acciones LIMIT 10; --OK
SELECT * FROM bi_corp_staging.cosmos_gest_movimientos_cuenta LIMIT 10; --OK
SELECT * FROM bi_corp_staging.cosmos_gest_movimientos_tarjeta LIMIT 10; --OK
SELECT * FROM bi_corp_staging.cosmos_gest_resol_acciones LIMIT 10; --OK
SELECT * FROM bi_corp_staging.cosmos_gestiones LIMIT 10; --OK
SELECT * FROM bi_corp_staging.cosmos_get_mails LIMIT 10; --OK
*/