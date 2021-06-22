CREATE OR REPLACE PACKAGE BODY GEC02."PKG_GC_PROCESOS" as

  v_paso                      number := 0;
  v_err_sql                   varchar2(2000);
  v_cantidad_error            number;
  v_fecha_desde               date;
  v_fecha_hasta               date;
  v_modulo                    varchar2(250);
  out_file                    UTL_FILE.FILE_TYPE;
  v_separador                 varchar2(1) := '|';

  --Cursor que levanta los procesos que estan listos para ejecutar (Estado = 0)
  cursor procesos_ejecutar is
    select modulo, fecha_desde, fecha_hasta, max(proceso_key) proceso_key
      from dw_procesos
     where estado_key = 0
  group by modulo, fecha_desde, fecha_hasta
  order by 4;

  --Cursor que levanta los procesos que estan con error para nueva ejecucion (Estado = 6)
  cursor procesos_ejecutar_error is
    select modulo, fecha_desde, fecha_hasta, max(proceso_key) proceso_key
      from dw_procesos
     where estado_key = 6
  group by modulo, fecha_desde, fecha_hasta
  order by 4;

/******************************************************************************/
-------------------------------- SP_EJECUTAR -----------------------------------
/******************************************************************************/
  procedure sp_ejecutar as
  begin
    select count(*)
      into v_cantidad_error
      from dw_procesos
     where estado_key = 6;

    v_paso := 1;

    if v_cantidad_error > 0 then
      v_paso := 2;
      for y in procesos_ejecutar_error loop

        v_modulo := null;
        v_modulo := y.modulo;

        case y.modulo
          when 'SP_FECHA' then pkg_gc_dimensiones.sp_fecha(y.proceso_key, to_char(sysdate,'YYYY'), y.fecha_desde, y.fecha_hasta);
          when 'SP_PRIORIDAD' then pkg_gc_dimensiones.sp_prioridad(y.proceso_key, y.fecha_desde, y.fecha_hasta);
          when 'SP_PAQUETES' then pkg_gc_dimensiones.sp_paquetes(y.proceso_key, y.fecha_desde, y.fecha_hasta);
          when 'SP_SEGMENTOS' then pkg_gc_dimensiones.sp_segmentos(y.proceso_key, y.fecha_desde, y.fecha_hasta);
          when 'SP_FAVORABILIDAD' then pkg_gc_dimensiones.sp_favorabilidad(y.proceso_key, y.fecha_desde, y.fecha_hasta);
          when 'SP_ESTADOS' then pkg_gc_dimensiones.sp_estados(y.proceso_key, y.fecha_desde, y.fecha_hasta);
          when 'SP_SECTORES' then pkg_gc_dimensiones.sp_sectores(y.proceso_key, y.fecha_desde, y.fecha_hasta);
          when 'SP_CLIENTES' then pkg_gc_dimensiones.sp_clientes(y.proceso_key, y.fecha_desde, y.fecha_hasta);
          when 'SP_CIRCUITOS' then pkg_gc_dimensiones.sp_circuitos(y.proceso_key, y.fecha_desde, y.fecha_hasta);
          when 'SP_INFO_REQUERIDA' then pkg_gc_dimensiones.sp_info_requerida(y.proceso_key, y.fecha_desde, y.fecha_hasta);
          when 'SP_INFO_REQUERIDA_VALORES' then pkg_gc_dimensiones.sp_info_requerida_valores(y.proceso_key, y.fecha_desde, y.fecha_hasta);
          when 'SP_GPO_INFO_REQUERIDA' then pkg_gc_dimensiones.sp_gpo_info_requerida(y.proceso_key, y.fecha_desde, y.fecha_hasta);
          when 'SP_DOC_REQUERIDA' then pkg_gc_dimensiones.sp_doc_requerida(y.proceso_key, y.fecha_desde, y.fecha_hasta);
          when 'SP_ETAPAS' then pkg_gc_dimensiones.sp_etapas(y.proceso_key, y.fecha_desde, y.fecha_hasta);
          when 'SP_TIPO_MEDIOS' then pkg_gc_dimensiones.sp_tipo_medios(y.proceso_key, y.fecha_desde, y.fecha_hasta);
          when 'SP_TIPO_MAIL' then pkg_gc_dimensiones.sp_tipo_mail(y.proceso_key, y.fecha_desde, y.fecha_hasta);
          when 'SP_MAILS_GESTIONES' then pkg_gc_dimensiones.sp_mails_gestiones(y.proceso_key, y.fecha_desde, y.fecha_hasta);
          when 'SP_MODELO_CARTAS' then pkg_gc_dimensiones.sp_modelo_cartas(y.proceso_key, y.fecha_desde, y.fecha_hasta);
          when 'SP_RESPONSABILIDAD' then pkg_gc_dimensiones.sp_responsabilidad(y.proceso_key, y.fecha_desde, y.fecha_hasta);
          when 'SP_CENTRO_COSTOS' then pkg_gc_dimensiones.sp_centro_costos(y.proceso_key, y.fecha_desde, y.fecha_hasta);
          when 'SP_USUARIOS' then pkg_gc_dimensiones.sp_usuarios(y.proceso_key, y.fecha_desde, y.fecha_hasta);
          when 'SP_TIPO_ENCUESTAS' then pkg_gc_dimensiones.sp_tipo_encuestas(y.proceso_key, y.fecha_desde, y.fecha_hasta);
          when 'SP_PREGUNTAS_ENCUESTAS' then pkg_gc_dimensiones.sp_preguntas_encuestas(y.proceso_key, y.fecha_desde, y.fecha_hasta);
          when 'SP_RESULTADO_ENCUESTAS' then pkg_gc_dimensiones.sp_resultado_encuestas(y.proceso_key, y.fecha_desde, y.fecha_hasta);
          when 'SP_AUTORIZANTES' then pkg_gc_dimensiones.sp_autorizantes(y.proceso_key, y.fecha_desde, y.fecha_hasta);
          when 'SP_REGULADOR' then pkg_gc_dimensiones.sp_regulador(y.proceso_key, y.fecha_desde, y.fecha_hasta);
          when 'SP_GESTION_REGULADOR' then pkg_gc_dimensiones.sp_gestion_regulador(y.proceso_key, y.fecha_desde, y.fecha_hasta);
          when 'SP_TIPO_RECLAMO_BCRA' then pkg_gc_dimensiones.sp_tipo_reclamo_bcra(y.proceso_key, y.fecha_desde, y.fecha_hasta);
          when 'SP_CONCEPTOS_BCRA' then pkg_gc_dimensiones.sp_conceptos_bcra(y.proceso_key, y.fecha_desde, y.fecha_hasta);
          when 'SP_TIPO_RECLAMO_SAC' then pkg_gc_dimensiones.sp_tipo_reclamo_sac(y.proceso_key, y.fecha_desde, y.fecha_hasta);
          when 'SP_CONCEPTOS_SAC' then pkg_gc_dimensiones.sp_conceptos_sac(y.proceso_key, y.fecha_desde, y.fecha_hasta);
          when 'SP_TIPO_RECLAMO_ESPANA' then pkg_gc_dimensiones.sp_tipo_reclamo_espana(y.proceso_key, y.fecha_desde, y.fecha_hasta);
          when 'SP_CONCEPTOS_ESPANA' then pkg_gc_dimensiones.sp_conceptos_espana(y.proceso_key, y.fecha_desde, y.fecha_hasta);
          when 'SP_INFO_ADJUNTA' then pkg_gc_dimensiones.sp_info_adjunta(y.proceso_key, y.fecha_desde, y.fecha_hasta);
          when 'SP_COMENTARIOS_CLIENTE' then pkg_gc_dimensiones.sp_comentarios_cliente(y.proceso_key, y.fecha_desde, y.fecha_hasta);
          when 'SP_CARTAS_PENDIENTES' then pkg_gc_dimensiones.sp_cartas_pendientes(y.proceso_key, y.fecha_desde, y.fecha_hasta);
          when 'SP_MENSAJES_MYA' then pkg_gc_dimensiones.sp_mensajes_mya(y.proceso_key, y.fecha_desde, y.fecha_hasta);
          when 'SP_VISA' then pkg_gc_dimensiones.sp_visa(y.proceso_key, y.fecha_desde, y.fecha_hasta);
          when 'SP_FACT' then pkg_gc_fact.sp_fact(y.proceso_key, y.fecha_desde, y.fecha_hasta);
          when 'SP_FACT_TRAC_PROM' then pkg_gc_fact.sp_fact_trac_prom(y.proceso_key, y.fecha_desde, y.fecha_hasta);
          when 'SP_COMPLETAR_IDS' then pkg_gc_indicadores.sp_completar_ids(y.proceso_key, y.fecha_desde, y.fecha_hasta,0);
          when 'SP_ULTIMO_ESTADO' then pkg_gc_indicadores.sp_ultimo_estado(y.proceso_key, y.fecha_desde, y.fecha_hasta,0);
          when 'SP_ULTIMO_ESTADO_MES' then pkg_gc_indicadores.sp_ultimo_estado_mes(y.proceso_key, y.fecha_desde, y.fecha_hasta);
          when 'SP_GESTIONES_VISA' then pkg_gc_indicadores.sp_gestiones_visa(y.proceso_key, y.fecha_desde, y.fecha_hasta);
          when 'SP_DEVOLUCIONES' then pkg_gc_indicadores.sp_devoluciones(y.proceso_key, y.fecha_desde, y.fecha_hasta,0);
          when 'SP_5_DIAS_HABILES' then pkg_gc_indicadores.sp_5_dias_habiles(y.proceso_key, y.fecha_desde, y.fecha_hasta);
          when 'SP_CUMPLIMIENTO_SLA' then pkg_gc_indicadores.sp_cumplimiento_sla(y.proceso_key, y.fecha_desde, y.fecha_hasta);
          when 'SP_CERRADAS_X_DEVOLUCION' then pkg_gc_indicadores.sp_cerradas_x_devolucion(y.proceso_key, y.fecha_desde, y.fecha_hasta,0);
          when 'SP_TRAC' then pkg_gc_indicadores.sp_trac(y.proceso_key, y.fecha_desde, y.fecha_hasta);
          when 'SP_DIAS_DE_GESTION' then pkg_gc_indicadores.sp_dias_de_gestion(y.proceso_key, y.fecha_desde, y.fecha_hasta,0,0);
          when 'SP_FAV_FALTA_DOC' then pkg_gc_indicadores.sp_fav_falta_doc(y.proceso_key, y.fecha_desde, y.fecha_hasta);
          when 'SP_REGULARIZAR_CIRCUITOS' then pkg_gc_indicadores.sp_regularizar_circuitos(y.proceso_key, y.fecha_desde, y.fecha_hasta);
          when 'SP_REGULARIZACION_4' then pkg_gc_indicadores.sp_regularizacion(y.proceso_key, y.fecha_desde, y.fecha_hasta, 4,0);
          when 'SP_REGULARIZACION_10' then pkg_gc_indicadores.sp_regularizacion(y.proceso_key, y.fecha_desde, y.fecha_hasta,10,0);
          when 'SP_REGULARIZACION_11' then pkg_gc_indicadores.sp_regularizacion(y.proceso_key, y.fecha_desde, y.fecha_hasta,11,0);
          when 'SP_REGULARIZACION_12' then pkg_gc_indicadores.sp_regularizacion(y.proceso_key, y.fecha_desde, y.fecha_hasta,12,0);
          when 'SP_REGULARIZACION_13' then pkg_gc_indicadores.sp_regularizacion(y.proceso_key, y.fecha_desde, y.fecha_hasta,13,0);
          when 'SP_REGULARIZACION_14' then pkg_gc_indicadores.sp_regularizacion(y.proceso_key, y.fecha_desde, y.fecha_hasta,14,0);
          when 'SP_REPORTE_INGRESADAS' then pkg_gc_reportes.sp_reporte_ingresadas(y.proceso_key, y.fecha_desde, y.fecha_hasta);
          when 'SP_REPORTE_CERRADAS' then pkg_gc_reportes.sp_reporte_cerradas(y.proceso_key, y.fecha_desde, y.fecha_hasta);
          when 'SP_REPORTE_REAPERTURAS' then pkg_gc_reportes.sp_reporte_reaperturas(y.proceso_key, y.fecha_desde, y.fecha_hasta);
          when 'SP_INSERT_FILES' then pkg_gc_indicadores.sp_insert_files(y.proceso_key, y.fecha_desde, y.fecha_hasta);
          when 'SP_CONTROL_INGRESADAS' then pkg_gc_indicadores.sp_control_cantidades(y.proceso_key, y.fecha_desde, y.fecha_hasta);
          when 'SP_VALIDACION_FINAL' then pkg_gc_indicadores.sp_validacion_final(y.proceso_key, y.fecha_desde, y.fecha_hasta);
          when 'SP_GESTION_CARACTERISTICAS' then pkg_gc_dimensiones.sp_gestion_caracteristicas(y.proceso_key, y.fecha_desde, y.fecha_hasta);
          when 'SP_RTA_RESOL_PREDEF' then pkg_gc_dimensiones.sp_rta_resol_predef(y.proceso_key, y.fecha_desde, y.fecha_hasta);

        else sp_inconsistencias(y.proceso_key, sysdate, 2, 'No existe SP: ' || y.modulo);
        end case;
      end loop;
    else
      v_paso := 3;
      for x in procesos_ejecutar loop

        v_modulo := null;
        v_modulo := x.modulo;

        case x.modulo
          when 'SP_FECHA' then pkg_gc_dimensiones.sp_fecha(x.proceso_key, to_char(sysdate,'YYYY'), x.fecha_desde, x.fecha_hasta);
          when 'SP_PRIORIDAD' then pkg_gc_dimensiones.sp_prioridad(x.proceso_key, x.fecha_desde, x.fecha_hasta);
          when 'SP_PAQUETES' then pkg_gc_dimensiones.sp_paquetes(x.proceso_key, x.fecha_desde, x.fecha_hasta);
          when 'SP_SEGMENTOS' then pkg_gc_dimensiones.sp_segmentos(x.proceso_key, x.fecha_desde, x.fecha_hasta);
          when 'SP_FAVORABILIDAD' then pkg_gc_dimensiones.sp_favorabilidad(x.proceso_key, x.fecha_desde, x.fecha_hasta);
          when 'SP_ESTADOS' then pkg_gc_dimensiones.sp_estados(x.proceso_key, x.fecha_desde, x.fecha_hasta);
          when 'SP_SECTORES' then pkg_gc_dimensiones.sp_sectores(x.proceso_key, x.fecha_desde, x.fecha_hasta);
          when 'SP_CLIENTES' then pkg_gc_dimensiones.sp_clientes(x.proceso_key, x.fecha_desde, x.fecha_hasta);
          when 'SP_CIRCUITOS' then pkg_gc_dimensiones.sp_circuitos(x.proceso_key, x.fecha_desde, x.fecha_hasta);
          when 'SP_INFO_REQUERIDA' then pkg_gc_dimensiones.sp_info_requerida(x.proceso_key, x.fecha_desde, x.fecha_hasta);
          when 'SP_INFO_REQUERIDA_VALORES' then pkg_gc_dimensiones.sp_info_requerida_valores(x.proceso_key, x.fecha_desde, x.fecha_hasta);
          when 'SP_GPO_INFO_REQUERIDA' then pkg_gc_dimensiones.sp_gpo_info_requerida(x.proceso_key, x.fecha_desde, x.fecha_hasta);
          when 'SP_DOC_REQUERIDA' then pkg_gc_dimensiones.sp_doc_requerida(x.proceso_key, x.fecha_desde, x.fecha_hasta);
          when 'SP_ETAPAS' then pkg_gc_dimensiones.sp_etapas(x.proceso_key, x.fecha_desde, x.fecha_hasta);
          when 'SP_TIPO_MEDIOS' then pkg_gc_dimensiones.sp_tipo_medios(x.proceso_key, x.fecha_desde, x.fecha_hasta);
          when 'SP_TIPO_MAIL' then pkg_gc_dimensiones.sp_tipo_mail(x.proceso_key, x.fecha_desde, x.fecha_hasta);
          when 'SP_MAILS_GESTIONES' then pkg_gc_dimensiones.sp_mails_gestiones(x.proceso_key, x.fecha_desde, x.fecha_hasta);
          when 'SP_MODELO_CARTAS' then pkg_gc_dimensiones.sp_modelo_cartas(x.proceso_key, x.fecha_desde, x.fecha_hasta);
          when 'SP_RESPONSABILIDAD' then pkg_gc_dimensiones.sp_responsabilidad(x.proceso_key, x.fecha_desde, x.fecha_hasta);
          when 'SP_CENTRO_COSTOS' then pkg_gc_dimensiones.sp_centro_costos(x.proceso_key, x.fecha_desde, x.fecha_hasta);
          when 'SP_USUARIOS' then pkg_gc_dimensiones.sp_usuarios(x.proceso_key, x.fecha_desde, x.fecha_hasta);
          when 'SP_TIPO_ENCUESTAS' then pkg_gc_dimensiones.sp_tipo_encuestas(x.proceso_key, x.fecha_desde, x.fecha_hasta);
          when 'SP_PREGUNTAS_ENCUESTAS' then pkg_gc_dimensiones.sp_preguntas_encuestas(x.proceso_key, x.fecha_desde, x.fecha_hasta);
          when 'SP_RESULTADO_ENCUESTAS' then pkg_gc_dimensiones.sp_resultado_encuestas(x.proceso_key, x.fecha_desde, x.fecha_hasta);
          when 'SP_AUTORIZANTES' then pkg_gc_dimensiones.sp_autorizantes(x.proceso_key, x.fecha_desde, x.fecha_hasta);
          when 'SP_REGULADOR' then pkg_gc_dimensiones.sp_regulador(x.proceso_key, x.fecha_desde, x.fecha_hasta);
          when 'SP_GESTION_REGULADOR' then pkg_gc_dimensiones.sp_gestion_regulador(x.proceso_key, x.fecha_desde, x.fecha_hasta);
          when 'SP_TIPO_RECLAMO_BCRA' then pkg_gc_dimensiones.sp_tipo_reclamo_bcra(x.proceso_key, x.fecha_desde, x.fecha_hasta);
          when 'SP_CONCEPTOS_BCRA' then pkg_gc_dimensiones.sp_conceptos_bcra(x.proceso_key, x.fecha_desde, x.fecha_hasta);
          when 'SP_TIPO_RECLAMO_SAC' then pkg_gc_dimensiones.sp_tipo_reclamo_sac(x.proceso_key, x.fecha_desde, x.fecha_hasta);
          when 'SP_TIPO_RECLAMO_ESPANA' then pkg_gc_dimensiones.sp_tipo_reclamo_espana(x.proceso_key, x.fecha_desde, x.fecha_hasta);
          when 'SP_CONCEPTOS_ESPANA' then pkg_gc_dimensiones.sp_conceptos_espana(x.proceso_key, x.fecha_desde, x.fecha_hasta);
          when 'SP_CONCEPTOS_SAC' then pkg_gc_dimensiones.sp_conceptos_sac(x.proceso_key, x.fecha_desde, x.fecha_hasta);
          when 'SP_INFO_ADJUNTA' then pkg_gc_dimensiones.sp_info_adjunta(x.proceso_key, x.fecha_desde, x.fecha_hasta);
          when 'SP_COMENTARIOS_CLIENTE' then pkg_gc_dimensiones.sp_comentarios_cliente(x.proceso_key, x.fecha_desde, x.fecha_hasta);
          when 'SP_CARTAS_PENDIENTES' then pkg_gc_dimensiones.sp_cartas_pendientes(x.proceso_key, x.fecha_desde, x.fecha_hasta);
          when 'SP_MENSAJES_MYA' then pkg_gc_dimensiones.sp_mensajes_mya(x.proceso_key, x.fecha_desde, x.fecha_hasta);
          when 'SP_VISA' then pkg_gc_dimensiones.sp_visa(x.proceso_key, x.fecha_desde, x.fecha_hasta);
          when 'SP_FACT' then pkg_gc_fact.sp_fact(x.proceso_key, x.fecha_desde, x.fecha_hasta);
          when 'SP_FACT_TRAC_PROM' then pkg_gc_fact.sp_fact_trac_prom(x.proceso_key, x.fecha_desde, x.fecha_hasta);
          when 'SP_COMPLETAR_IDS' then pkg_gc_indicadores.sp_completar_ids(x.proceso_key, x.fecha_desde, x.fecha_hasta,0);
          when 'SP_ULTIMO_ESTADO' then pkg_gc_indicadores.sp_ultimo_estado(x.proceso_key, x.fecha_desde, x.fecha_hasta,0);
          when 'SP_ULTIMO_ESTADO_MES' then pkg_gc_indicadores.sp_ultimo_estado_mes(x.proceso_key, x.fecha_desde, x.fecha_hasta);
          when 'SP_GESTIONES_VISA' then pkg_gc_indicadores.sp_gestiones_visa(x.proceso_key, x.fecha_desde, x.fecha_hasta);
          when 'SP_DEVOLUCIONES' then pkg_gc_indicadores.sp_devoluciones(x.proceso_key, x.fecha_desde, x.fecha_hasta,0);
          when 'SP_5_DIAS_HABILES' then pkg_gc_indicadores.sp_5_dias_habiles(x.proceso_key, x.fecha_desde, x.fecha_hasta);
          when 'SP_CUMPLIMIENTO_SLA' then pkg_gc_indicadores.sp_cumplimiento_sla(x.proceso_key, x.fecha_desde, x.fecha_hasta);
          when 'SP_CERRADAS_X_DEVOLUCION' then pkg_gc_indicadores.sp_cerradas_x_devolucion(x.proceso_key, x.fecha_desde, x.fecha_hasta,0);
          when 'SP_TRAC' then pkg_gc_indicadores.sp_trac(x.proceso_key, x.fecha_desde, x.fecha_hasta);
          when 'SP_DIAS_DE_GESTION' then pkg_gc_indicadores.sp_dias_de_gestion(x.proceso_key, x.fecha_desde, x.fecha_hasta,0,0);
          when 'SP_FAV_FALTA_DOC' then pkg_gc_indicadores.sp_fav_falta_doc(x.proceso_key, x.fecha_desde, x.fecha_hasta);
          when 'SP_REGULARIZAR_CIRCUITOS' then pkg_gc_indicadores.sp_regularizar_circuitos(x.proceso_key, x.fecha_desde, x.fecha_hasta);
          when 'SP_REGULARIZACION_4' then pkg_gc_indicadores.sp_regularizacion(x.proceso_key, x.fecha_desde, x.fecha_hasta, 4, 0);
          when 'SP_REGULARIZACION_10' then pkg_gc_indicadores.sp_regularizacion(x.proceso_key, x.fecha_desde, x.fecha_hasta,10,0);
          when 'SP_REGULARIZACION_11' then pkg_gc_indicadores.sp_regularizacion(x.proceso_key, x.fecha_desde, x.fecha_hasta,11,0);
          when 'SP_REGULARIZACION_12' then pkg_gc_indicadores.sp_regularizacion(x.proceso_key, x.fecha_desde, x.fecha_hasta,12,0);
          when 'SP_REGULARIZACION_13' then pkg_gc_indicadores.sp_regularizacion(x.proceso_key, x.fecha_desde, x.fecha_hasta,13,0);
          when 'SP_REGULARIZACION_14' then pkg_gc_indicadores.sp_regularizacion(x.proceso_key, x.fecha_desde, x.fecha_hasta,14,0);
          when 'SP_REPORTE_INGRESADAS' then pkg_gc_reportes.sp_reporte_ingresadas(x.proceso_key, x.fecha_desde, x.fecha_hasta);
          when 'SP_REPORTE_CERRADAS' then pkg_gc_reportes.sp_reporte_cerradas(x.proceso_key, x.fecha_desde, x.fecha_hasta);
          when 'SP_REPORTE_REAPERTURAS' then pkg_gc_reportes.sp_reporte_reaperturas(x.proceso_key, x.fecha_desde, x.fecha_hasta);
          when 'SP_INSERT_FILES' then pkg_gc_indicadores.sp_insert_files(x.proceso_key, x.fecha_desde, x.fecha_hasta);
          when 'SP_CONTROL_INGRESADAS' then pkg_gc_indicadores.sp_control_cantidades(x.proceso_key, x.fecha_desde, x.fecha_hasta);
          when 'SP_VALIDACION_FINAL' then pkg_gc_indicadores.sp_validacion_final(x.proceso_key, x.fecha_desde, x.fecha_hasta);
          when 'SP_GESTION_CARACTERISTICAS' then pkg_gc_dimensiones.sp_gestion_caracteristicas(x.proceso_key, x.fecha_desde, x.fecha_hasta);
          when 'SP_RTA_RESOL_PREDEF' then pkg_gc_dimensiones.sp_rta_resol_predef(x.proceso_key, x.fecha_desde, x.fecha_hasta);
        else sp_inconsistencias(x.proceso_key, sysdate, 3, 'No existe SP: ' || x.modulo);
        end case;
      end loop;
    end if;

    v_paso := 4;
    --Se actualiza la fecha de ultima ejecicion del proceso ETL
    update dw_parametros
       set valor_fecha = sysdate
     where variable = 'fecha_ult_ejecucion';
    commit;
  exception
    when others then
      v_err_sql := substr (sqlerrm (sqlcode), 1, 250);
      sp_inconsistencias(00000, sysdate, v_paso, 'Error SP_EJECUTAR: ' || v_err_sql || ' - ' || v_modulo);
  end sp_ejecutar;

/******************************************************************************/
-------------------------- SP_INCONSISTENCIAS ----------------------------------
/******************************************************************************/
  procedure sp_inconsistencias (id_proceso in number, fecha in date, paso in number, er in varchar2) as
  begin
    insert into dw_inconsistencias values (id_proceso, fecha, paso, er);
    commit;
  end sp_inconsistencias;

/******************************************************************************/
------------------------------- SP_PROCESOS ------------------------------------
/******************************************************************************/
  procedure sp_procesos(indicador in number, estado in number, fec_ini in date, fec_fin in date, id_proceso in number) as
  begin
    --indicador = 1, cuando es el update OK de inicio de ejecucion.
    --indicador = 2, cuando es el update OK de fin de ejecucion.
    --indicador = 3, cuando es el update ERROR de fin de ejecucion.
    if indicador = 1 then
      update dw_procesos
         set estado_key = estado,
             fecha_inicio = fec_ini,
             fecha_fin = null
       where proceso_key = id_proceso;
    elsif indicador = 2 then
      update dw_procesos
         set estado_key = estado,
             fecha_fin = fec_fin
       where proceso_key = id_proceso
         and fecha_inicio is not null;
    elsif indicador = 3 then
      update dw_procesos
         set estado_key = estado,
             fecha_inicio = null,
             fecha_fin = null
       where proceso_key = id_proceso;
    end if;
    commit;
  end sp_procesos;

/******************************************************************************/
--------------------------- SP_NUEVA_EJECUCION ---------------------------------
/******************************************************************************/
  procedure sp_nueva_ejecucion as
  begin

    v_cantidad_error := 0;
    v_paso := 1;
    for x in (select modulo, max(proceso_key) proceso
                from dw_procesos
                /*Se agrega para que inserte solo registros para nueva ejecucion mensual para los SP del filtro*/
                --where modulo in ('SP_CLIENTES','SP_FACT','SP_FACT_TRAC_PROM','SP_MAIN_INDICADORES')
                /**/
            group by modulo
            order by proceso asc) loop

      select fecha_desde, fecha_hasta
        into v_fecha_desde, v_fecha_hasta
        from dw_procesos
       where proceso_key = x.proceso;

      insert into dw_procesos
           values (seq_dw_procesos.nextval,
                   0,
                   2,
                   x.modulo,
                   null,
                   null,
                   /*** INICIO EJECUCION DIARIA ***/
                   v_fecha_desde + 1,
                   v_fecha_hasta + 1,
                   /*** FIN EJECUCION DIARIA ***/
                   /*** INICIO EJECUCION MENSUAL ***/
                   --add_months(v_fecha_desde,1),
                   --add_months(v_fecha_hasta,1),
                   /*** FIN EJECUCION MENSUAL ***/
                   null);
      end loop;
      commit;
  end sp_nueva_ejecucion;

/******************************************************************************/
----------------------- SP_BUSCA_PROCESOS_ERROR --------------------------------
/******************************************************************************/
  procedure sp_busca_procesos_error(errores out number) as
  begin
    v_paso := 1;
    v_cantidad_error := 0;

    select count(*)
      into v_cantidad_error
      from dw_procesos
     where estado_key = 6;

    errores := v_cantidad_error;

  end sp_busca_procesos_error;

/******************************************************************************/
----------------------------- SP_DEPURACION ------------------------------------
/******************************************************************************/
  procedure sp_depuracion(fecha_desde in number, fecha_hasta in number) as
  begin
    v_paso := 1;
    insert into gec02.fact_gestiones_hist (fact_nro_gestion,fact_cod_circuito,fact_cod_prioridad,fact_cod_favorabilidad,fact_cod_segmento,fact_cod_paquete,fact_cod_enc,fact_cod_cliente,fact_cod_tipo_medio,
                fact_cod_estados,fact_cod_usuario,fact_cod_sector,fact_cod_responsabilidad,fact_cod_modelo_cartas,fact_fecha_estado,fact_orden_estado,fact_fec_max_resol,fact_ind_dias_gest,
                fact_ind_importe_reclamado,fact_ind_moneda_reclamado,fact_ind_importe_concedido,fact_ind_moneda_concedido,fact_ind_reapertura,fact_ultimo_estado,fact_fec_alta,
                fact_fec_bandeja_actual,fact_sla,fact_marca_cerrado,fact_sector_alta,fact_cerradas_dev,fact_marca_devolucion,fact_fecha_circuito,fact_marca_visa,fact_ult_est_mes,
                fact_comentario_est,fact_fec_res_resol_1,fact_fec_res_resol_2,fact_fec_res_resol_3,fact_fec_resp,fact_responsable_resol,fact_desc_sec_resol_1,fact_desc_sec_resol_2,
                fact_desc_sec_resol_3,fact_desc_sec_resp,fact_cod_suc_cta,fact_cod_crm,fact_nro_cta,fact_bandeja_actual,fact_cod_ult_estado,fact_desc_ult_estado,fact_fec_cierre,fact_ingreso_epa,
                fact_resp_de_autoriz,fact_no_autorizado,fact_validacion_rci,fact_fecha_siguiente,fact_ind_dias_x_estado,fact_mes_anio_cierre,fact_resp_de_rta,fact_gestion_inicial,
                fact_gestion_anterior,fact_cant_reaperturas,fact_plazo_legal,fact_cod_rta_resol_predef,fact_nro_ticket,fact_prefijo_ticket)
         select fact_nro_gestion,fact_cod_circuito,fact_cod_prioridad,fact_cod_favorabilidad,fact_cod_segmento,fact_cod_paquete,fact_cod_enc,fact_cod_cliente,fact_cod_tipo_medio,
                fact_cod_estados,fact_cod_usuario,fact_cod_sector,fact_cod_responsabilidad,fact_cod_modelo_cartas,fact_fecha_estado,fact_orden_estado,fact_fec_max_resol,fact_ind_dias_gest,
                fact_ind_importe_reclamado,fact_ind_moneda_reclamado,fact_ind_importe_concedido,fact_ind_moneda_concedido,fact_ind_reapertura,fact_ultimo_estado,fact_fec_alta,
                fact_fec_bandeja_actual,fact_sla,fact_marca_cerrado,fact_sector_alta,fact_cerradas_dev,fact_marca_devolucion,fact_fecha_circuito,fact_marca_visa,fact_ult_est_mes,
                fact_comentario_est,fact_fec_res_resol_1,fact_fec_res_resol_2,fact_fec_res_resol_3,fact_fec_resp,fact_responsable_resol,fact_desc_sec_resol_1,fact_desc_sec_resol_2,
                fact_desc_sec_resol_3,fact_desc_sec_resp,fact_cod_suc_cta,fact_cod_crm,fact_nro_cta,fact_bandeja_actual,fact_cod_ult_estado,fact_desc_ult_estado,fact_fec_cierre,fact_ingreso_epa,
                fact_resp_de_autoriz,fact_no_autorizado,fact_validacion_rci,fact_fecha_siguiente,fact_ind_dias_x_estado,fact_mes_anio_cierre,fact_resp_de_rta,fact_gestion_inicial,
                fact_gestion_anterior,fact_cant_reaperturas,fact_plazo_legal,fact_cod_rta_resol_predef,fact_nro_ticket,fact_prefijo_ticket
           from gec02.fact_gestiones
          where fact_fecha_estado between fecha_desde and fecha_hasta;

    sp_inconsistencias(00000, sysdate, v_paso, 'Se pasa a la tabla gec02.fact_gestiones_hist desde: ' || fecha_desde || ' hasta: ' || fecha_hasta);

    v_paso := 2;
    delete from gec02.fact_gestiones where fact_fecha_estado between fecha_desde and fecha_hasta;

    sp_inconsistencias(00000, sysdate, v_paso, 'Se eliminan datos de la tabla gec02.fact_gestiones desde: ' || fecha_desde || ' hasta: ' || fecha_hasta);

    commit;

  exception
    when others then
      v_err_sql := substr (sqlerrm (sqlcode), 1, 250);
      sp_inconsistencias(00000, sysdate, v_paso, 'Error SP_DEPURACION: ' || v_err_sql);
  end sp_depuracion;

end pkg_gc_procesos;
/
