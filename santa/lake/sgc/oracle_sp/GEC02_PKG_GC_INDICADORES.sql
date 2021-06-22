CREATE OR REPLACE PACKAGE BODY GEC02."PKG_GC_INDICADORES" as

  v_paso                  number;
  v_indicador             number;
  v_cantidad_error        number;
  v_modulo_error          varchar2(50);
  v_fecha_inicio          date;
  v_fecha_fin             date;
  v_err_sql               varchar2(2000);
  v_fecha_alta            number;
  v_fecha_max_resol       number;
  v_dias_gestion          number;
  v_dias_vencimiento      number;
  v_dias_resol            number;
  v_tiempo                number;
  v_tiempo_actor          number;
  v_desvio_status         varchar2(1);
  v_tiempo_desvio         number;
  v_cerradas_trac         number;
  v_favorabilidad_1       number;
  v_favorabilidad_2       number;
  v_favorabilidad_3       number;
  v_fec_desde             number;
  v_fec_hasta             number;
  v_cod_tipo_medio        number;
  v_orden_estado          number;
  v_regularizacion_4      number;
  v_cod_favorabilidad     number;
  v_cantidad_regularizar  number;
  v_plazo_legal           number;
  v_cant_abiertas         number;
  v_max                   number:=32767;
  v_cod_segmentto         gec02.fact_gestiones.fact_cod_segmento%type;
  v_cir_alta              gec02.fact_gestiones.fact_cod_circuito%TYPE;
  v_mes_alta              gec02.fact_gestiones.fact_fecha_estado%TYPE;
  v_cir_no_alta           gec02.fact_gestiones.fact_cod_circuito%TYPE;
  v_mes_no_alta           gec02.fact_gestiones.fact_fecha_estado%TYPE;
  --
  v_path_utl           varchar2(15) := 'RIO56_INTERFACE';
  out_file             UTL_FILE.FILE_TYPE;
  out_file_1           UTL_FILE.FILE_TYPE;
  out_file_2           UTL_FILE.FILE_TYPE;
  v_separador          varchar2(1) := '|';
  --
  TYPE t_fact_gestiones_tab IS TABLE OF fact_gestiones%ROWTYPE;
  TYPE t_dim_circuitos_tab IS TABLE OF dim_circuitos%ROWTYPE;
  --
  l_ultimo_estado   t_fact_gestiones_tab := t_fact_gestiones_tab ();
  l_completar_ids   t_fact_gestiones_tab := t_fact_gestiones_tab ();
  l_fav_falta_doc   t_fact_gestiones_tab := t_fact_gestiones_tab ();
  l_comentarios     t_fact_gestiones_tab := t_fact_gestiones_tab ();
  l_5_dias_habiles  t_dim_circuitos_tab := t_dim_circuitos_tab();

/******************************************************************************/
---------------------------- SP_VALIDACION_FINAL -------------------------------
/******************************************************************************/
  procedure sp_validacion_final(id_proceso in number, fec_desde in date, fec_hasta in date) as
  begin
    v_fecha_inicio := sysdate;
    pkg_gc_procesos.sp_procesos(1, 1, v_fecha_inicio, null, id_proceso);

    v_paso := 1;
    --Verifica si hay procesos con error
    pkg_gc_procesos.sp_busca_procesos_error(v_cantidad_error);

    if v_cantidad_error = 0 then
      v_paso := 2;
      --Se generan registros para la nueva ejecucion ya que no hay ejecuciones con error
      v_fecha_fin := sysdate;
      pkg_gc_procesos.sp_procesos(2, 2, v_fecha_inicio, v_fecha_fin, id_proceso);
      pkg_gc_procesos.sp_nueva_ejecucion;
    else
      v_paso := 3;
      --No se genera registros para la nueva ejecucion ya que existen procesos con error
      v_fecha_fin := sysdate;
      pkg_gc_procesos.sp_procesos(3, 0, v_fecha_inicio, v_fecha_fin, id_proceso);
      pkg_gc_procesos.sp_inconsistencias(id_proceso, sysdate, v_paso, 'Existen procesos con error que impiden insertar registros para una nueva ejecuciÃ³n.');
    end if;
  end sp_validacion_final;

/******************************************************************************/
------------------------------- SP_ULTIMO_ESTADO -------------------------------
/******************************************************************************/
  procedure sp_ultimo_estado(id_proceso in number, fec_desde in date, fec_hasta in date, gestiones in number) as
  begin
    --Verifica si hay procesos con error
    pkg_gc_procesos.sp_busca_procesos_error(v_cantidad_error);

    if v_cantidad_error = 0 then
      v_fecha_inicio := sysdate;
      pkg_gc_procesos.sp_procesos(1, 1, v_fecha_inicio, null, id_proceso);

      if gestiones = 0 then
        v_paso := 1;
        --Se carga cursor con los datos a los cuales se les debe actualizar la marca de ultimo estado
        for x in (select distinct fact_nro_gestion, max(fact_orden_estado) ultimo
                    from fact_gestiones
                   where fact_fecha_estado between to_char(fec_desde,'YYYYMMDD')
                                               and to_char(fec_hasta,'YYYYMMDD')
                   group by fact_nro_gestion) loop

          l_ultimo_estado.extend;

          l_ultimo_estado(l_ultimo_estado.last).fact_nro_gestion := x.fact_nro_gestion;
          l_ultimo_estado(l_ultimo_estado.last).fact_orden_estado := x.ultimo;
        end loop;
      elsif gestiones = 1 then
      v_paso := 2;
      --Se carga cursor con los datos a los cuales se les debe actualizar la marca de ultimo estado
        for x in (select distinct fact_nro_gestion, max(fact_orden_estado) ultimo
                    from fact_gestiones, dim_gestiones_regularizar
                   where fact_nro_gestion = reg_nro_gestion
                   group by fact_nro_gestion) loop

          l_ultimo_estado.extend;

          l_ultimo_estado(l_ultimo_estado.last).fact_nro_gestion := x.fact_nro_gestion;
          l_ultimo_estado(l_ultimo_estado.last).fact_orden_estado := x.ultimo;
        end loop;
      end if;

      v_paso := 3;
      --Se modifica la marca de ultimo estado para los estados que dejan de ser ultimo
      forall x in l_ultimo_estado.first .. l_ultimo_estado.last
        update fact_gestiones
           set fact_ultimo_estado = 0
         where fact_nro_gestion  = l_ultimo_estado(x).fact_nro_gestion
           and fact_orden_estado <> l_ultimo_estado(x).fact_orden_estado;

     v_paso := 4;
      --Se marca el ultimo estado de la gestion
      forall x in l_ultimo_estado.first .. l_ultimo_estado.last
        update fact_gestiones
           set fact_ultimo_estado = 1
         where fact_nro_gestion  = l_ultimo_estado(x).fact_nro_gestion
           and fact_orden_estado = l_ultimo_estado(x).fact_orden_estado;

      commit;

      v_paso := 5;
      --Se agrega para regularizar inmediatamente las gestiones que queden con doble marca de ultimo estado
      delete from dim_gestiones_regularizar;

      v_paso := 6;
      insert into dim_gestiones_regularizar
        select distinct fact_nro_gestion, sysdate, min(fact_orden_estado), null
          from fact_gestiones
         where fact_ultimo_estado = 1
        having count(fact_ultimo_estado) > 1
         group by fact_nro_gestion;

      v_paso := 7;
      update fact_gestiones
         set fact_ultimo_estado = 0
       where (fact_nro_gestion, fact_orden_estado) in (select reg_nro_gestion, reg_numero
                                                         from dim_gestiones_regularizar);
      commit;

      v_paso := 8;
      v_fecha_fin := sysdate;
      pkg_gc_procesos.sp_procesos(2, 2, v_fecha_inicio, v_fecha_fin, id_proceso);
    else
      pkg_gc_procesos.sp_procesos(3, 0, v_fecha_inicio, v_fecha_fin, id_proceso);
      pkg_gc_procesos.sp_inconsistencias(id_proceso, sysdate, v_paso, 'Existen procesos con error que impiden la ejecuciÃ³n.');
    end if;
  exception
     when others then
      v_fecha_fin := sysdate;
      pkg_gc_procesos.sp_procesos(2, 6, v_fecha_inicio, v_fecha_fin, id_proceso);
      v_err_sql := substr (sqlerrm (sqlcode), 1, 250);
      pkg_gc_procesos.sp_inconsistencias(id_proceso, sysdate, v_paso, v_err_sql);
  end sp_ultimo_estado;

/******************************************************************************/
------------------------------- SP_COMPLETAR_IDS -------------------------------
/******************************************************************************/
  procedure sp_completar_ids(id_proceso in number, fec_desde in date, fec_hasta in date, gestiones in number) as
  begin
    --Verifica si hay procesos con error
    pkg_gc_procesos.sp_busca_procesos_error(v_cantidad_error);

    if v_cantidad_error = 0 then
      v_fecha_inicio := sysdate;
      pkg_gc_procesos.sp_procesos(1, 1, v_fecha_inicio, null, id_proceso);

      if gestiones = 0 then
        v_paso := 1;
        --Se cargan las gestiones a las cuales se les debe comletar el cod_tipo_gestion para que este en todos sus registros
        for x in (select distinct fact_nro_gestion, decode(fact_cod_tipo_medio,null,0,fact_cod_tipo_medio) tipo_medio
                    from fact_gestiones
                   where fact_nro_gestion in (select distinct fact_nro_gestion
                                                from fact_gestiones
                                               where fact_fecha_estado between to_char(fec_desde,'YYYYMMDD') and to_char(fec_hasta,'YYYYMMDD'))
                     and fact_cod_estados = 100
                   order by 1) loop

          l_completar_ids.extend;

          l_completar_ids(l_completar_ids.last).fact_nro_gestion := x.fact_nro_gestion;
          l_completar_ids(l_completar_ids.last).fact_cod_tipo_medio := x.tipo_medio;
        end loop;
      elsif gestiones = 1 then
        v_paso := 2;
        --Se cargan las gestiones a las cuales se les debe comletar el cod_tipo_gestion para que este en todos sus registros
        for x in (select distinct fact_nro_gestion, decode(fact_cod_tipo_medio,null,0,fact_cod_tipo_medio) tipo_medio
                    from fact_gestiones, dim_gestiones_regularizar
                   where fact_nro_gestion = reg_nro_gestion
                     and fact_cod_estados = 100
                   order by 1) loop

          l_completar_ids.extend;

          l_completar_ids(l_completar_ids.last).fact_nro_gestion := x.fact_nro_gestion;
          l_completar_ids(l_completar_ids.last).fact_cod_tipo_medio := x.tipo_medio;
        end loop;
      end if;

      v_paso := 3;
      --Se coloca el tipo de madio para todos los registros de la gestion
      forall x in l_completar_ids.first .. l_completar_ids.last
        update fact_gestiones
           set fact_cod_tipo_medio = l_completar_ids(x).fact_cod_tipo_medio
         where fact_nro_gestion  = l_completar_ids(x).fact_nro_gestion;

      commit;

      v_paso := 4;
      v_fecha_fin := sysdate;
      pkg_gc_procesos.sp_procesos(2, 2, v_fecha_inicio, v_fecha_fin, id_proceso);
    else
      pkg_gc_procesos.sp_procesos(3, 0, v_fecha_inicio, v_fecha_fin, id_proceso);
      pkg_gc_procesos.sp_inconsistencias(id_proceso, sysdate, v_paso, 'Existen procesos con error que impiden la ejecuciÃ³n.');
    end if;
  exception
     when others then
      v_fecha_fin := sysdate;
      pkg_gc_procesos.sp_procesos(2, 6, v_fecha_inicio, v_fecha_fin, id_proceso);
      v_err_sql := substr (sqlerrm (sqlcode), 1, 250);
      pkg_gc_procesos.sp_inconsistencias(id_proceso, sysdate, v_paso, v_err_sql);
  end sp_completar_ids;

/******************************************************************************/
----------------------------- SP_DIAS_DE_GESTION -------------------------------
/******************************************************************************/
  procedure sp_dias_de_gestion(id_proceso in number, fec_desde in date, fec_hasta in date, gestiones in number, dias in number) as
  begin
    --Verifica si hay procesos con error
    pkg_gc_procesos.sp_busca_procesos_error(v_cantidad_error);

    if v_cantidad_error = 0 then
      v_fecha_inicio := sysdate;
      pkg_gc_procesos.sp_procesos(1, 1, v_fecha_inicio, null, id_proceso);

      if gestiones = 0 then
        v_paso := 1;
        --Se recorre las gestiones para las cuales su ultimo estado es <> a cerrado y/o anulado
        for x in (select distinct fact_nro_gestion,
                         fact_fec_alta,
                         fact_fec_max_resol,
                         fact_fec_bandeja_actual,
                         fact_marca_cerrado
                    from fact_gestiones
                   where fact_marca_cerrado <> 1
                     and fact_ultimo_estado = 1
                     and fact_marca_visa <> 1
                   union all
                  select distinct fact_nro_gestion,
                         fact_fec_alta,
                         fact_fec_max_resol,
                         fact_fec_bandeja_actual,
                         fact_marca_cerrado
                    from fact_gestiones
                   where fact_marca_cerrado = 1
                     and fact_ultimo_estado = 1
                     and fact_marca_visa <> 1
                     and fact_fecha_estado between to_char(fec_desde,'YYYYMMDD') and to_char(fec_hasta,'YYYYMMDD')
                   order by 1) loop

        v_dias_gestion := 0;

        if x.fact_marca_cerrado = 1 then
          v_dias_gestion := fn_dias_habiles(x.fact_fec_alta,x.fact_fec_bandeja_actual,dias,x.fact_nro_gestion);
        else
          v_dias_gestion := fn_dias_habiles(x.fact_fec_alta,fec_hasta,dias,x.fact_nro_gestion);
        end if;

        --Calcula marca para plazo legal -- 0 No supera plazo legal y 1 Si supera plazo legal
        --Plazo legal < 21
        v_plazo_legal := 0;
        if v_dias_gestion >= 21 then
          v_plazo_legal := 1;
        end if;

          update fact_gestiones
             set fact_ind_dias_gest = v_dias_gestion,
                 fact_plazo_legal = v_plazo_legal
           where fact_nro_gestion = x.fact_nro_gestion;
        end loop;

        v_paso := 2;
        --Calculo de tiempo por sector/estado
        for x in (select fact_fec_bandeja_actual,
                       lead(fact_fec_bandeja_actual,1) over (partition by fact_sector_alta,fact_nro_gestion order by fact_orden_estado) fact_fecha_siguiente,
                       fact_cod_estados,
                       fact_orden_estado,
                       fact_nro_gestion
                  from fact_gestiones
                 where fact_fecha_estado between to_char(fec_desde,'YYYYMMDD') and to_char(fec_hasta,'YYYYMMDD')
                   and fact_marca_visa <> 1) loop

          v_dias_gestion := 0;

          if x.fact_fecha_siguiente is not null then
            v_dias_gestion := fn_dias_habiles(x.fact_fec_bandeja_actual, x.fact_fecha_siguiente,0, x.fact_nro_gestion);
          end if;

          update fact_gestiones
             set fact_ind_dias_x_estado = v_dias_gestion
           where fact_nro_gestion = x.fact_nro_gestion
             and fact_cod_estados = x.fact_cod_estados
             and fact_orden_estado = x.fact_orden_estado;

        end loop;
      elsif gestiones = 1 then
        v_paso := 3;
        --Se recorre las gestiones para las cuales su ultimo estado es <> a cerrado y/o anulado
        for x in (select distinct fact_nro_gestion,
                         fact_fec_alta,
                         fact_fec_max_resol,
                         fact_fec_bandeja_actual,
                         fact_marca_cerrado
                    from fact_gestiones, dim_gestiones_regularizar
                   where fact_nro_gestion = reg_nro_gestion
                     and fact_ultimo_estado = 1
                     and fact_marca_cerrado <> 1
                     and fact_marca_visa <> 1
                   union all
                  select distinct fact_nro_gestion,
                         fact_fec_alta,
                         fact_fec_max_resol,
                         fact_fec_bandeja_actual,
                         fact_marca_cerrado
                    from fact_gestiones, dim_gestiones_regularizar
                   where fact_nro_gestion = reg_nro_gestion
                     and fact_ultimo_estado = 1
                     and fact_marca_cerrado = 1
                     and fact_marca_visa <> 1
                   order by 1) loop

          v_dias_gestion := 0;

          if x.fact_marca_cerrado = 1 then
            v_dias_gestion := fn_dias_habiles(x.fact_fec_alta,x.fact_fec_bandeja_actual,dias,x.fact_nro_gestion);
          else
            v_dias_gestion := fn_dias_habiles(x.fact_fec_alta,fec_hasta,dias,x.fact_nro_gestion);
          end if;

          --Calcula marca para plazo legal -- 0 No supera plazo legal y 1 Si supera plazo legal
          --Plazo legal < 21
          v_plazo_legal := 0;
          if v_dias_gestion >= 21 then
            v_plazo_legal := 1;
          end if;

          update fact_gestiones
             set fact_ind_dias_gest = v_dias_gestion,
                 fact_plazo_legal = v_plazo_legal
           where fact_nro_gestion = x.fact_nro_gestion;
        end loop;

        v_paso := 4;
        --Calculo de tiempo por sector/estado
        for x in (select fact_fec_bandeja_actual,
                       lead(fact_fec_bandeja_actual,1) over (partition by fact_sector_alta,fact_nro_gestion order by fact_orden_estado) fact_fecha_siguiente,
                       fact_cod_estados,
                       fact_orden_estado,
                       fact_nro_gestion
                  from fact_gestiones, dim_gestiones_regularizar
                 where fact_nro_gestion = reg_nro_gestion
                   and fact_marca_visa <> 1) loop

          v_dias_gestion := 0;

          if x.fact_fecha_siguiente is not null then
            v_dias_gestion := fn_dias_habiles(x.fact_fec_bandeja_actual, x.fact_fecha_siguiente,0, x.fact_nro_gestion);
          end if;

          update fact_gestiones
             set fact_ind_dias_x_estado = v_dias_gestion
           where fact_nro_gestion = x.fact_nro_gestion
             and fact_cod_estados = x.fact_cod_estados
             and fact_orden_estado = x.fact_orden_estado;

        end loop;
      end if;
      commit;

      v_paso := 3;
      v_fecha_fin := sysdate;
      pkg_gc_procesos.sp_procesos(2, 2, v_fecha_inicio, v_fecha_fin, id_proceso);
    else
      pkg_gc_procesos.sp_procesos(3, 0, v_fecha_inicio, v_fecha_fin, id_proceso);
      pkg_gc_procesos.sp_inconsistencias(id_proceso, sysdate, v_paso, 'Existen procesos con error que impiden la ejecuciÃ³n.');
    end if;
  exception
     when others then
      v_fecha_fin := sysdate;
      pkg_gc_procesos.sp_procesos(2, 6, v_fecha_inicio, v_fecha_fin, id_proceso);
      v_err_sql := substr (sqlerrm (sqlcode), 1, 250);
      pkg_gc_procesos.sp_inconsistencias(id_proceso, sysdate, v_paso, v_err_sql);
  end sp_dias_de_gestion;

/******************************************************************************/
------------------------------- SP_INSERT_FILES --------------------------------
/******************************************************************************/
  procedure sp_insert_files(id_proceso in number, fec_desde in date, fec_hasta in date) as
  begin
    --Verifica si hay procesos con error
    pkg_gc_procesos.sp_busca_procesos_error(v_cantidad_error);

    if v_cantidad_error = 0 then
      v_fecha_inicio := sysdate;
      pkg_gc_procesos.sp_procesos(1, 1, v_fecha_inicio, null, id_proceso);
      v_paso := 1;
      out_file := null;
      out_file_1 := null;
      out_file_2 := null;

      v_paso := 21;
      out_file := utl_file.fopen (v_path_utl, 'DIM_GESTIONES_VISA.txt', 'w', v_max);
      v_paso := 22;
      out_file_1 := utl_file.fopen (v_path_utl, 'DIM_INFORMACION_ADJUNTA.txt', 'w', v_max);
       v_paso := 23;
      out_file_2 := utl_file.fopen (v_path_utl, 'FACT_GESTIONES.txt', 'w', v_max);
      for gest in (select visa_nro_gestion,visa_fec_act,visa_procesada,visa_fecha_proceso
                     from gec02.dim_gestiones_visa
                    where visa_fecha_proceso between fec_desde and fec_hasta) loop
        v_paso := 3;
        utl_file.put_line (out_file, gest.visa_nro_gestion || v_separador || gest.visa_fec_act || v_separador || gest.visa_procesada || v_separador || gest.visa_fecha_proceso);
      end loop;

      v_paso := 4;
      utl_file.fclose(out_file);

      for info in (select adj_nro_gestion,
                          adj_sector_gestion,
                          adj_actor,
                          adj_nro_orden,
                          adj_tipo_info,
                          adj_cod_tipo_doc,
                          adj_secuencia,
                          adj_dato_info,
                          adj_link_info,
                          adj_fecha_info,
                          adj_user_info,
                          adj_sector_info,
                          adj_nom_archivo_oroginal,
                          adj_nom_archivo,
                          adj_directorio_archivo,
                          adj_importe
                     from gec02.dim_informacion_adjunta
                    where adj_fecha_info between fec_desde and fec_hasta) loop
        v_paso := 5;
        utl_file.put_line (out_file_1, info.adj_nro_gestion || v_separador ||
                                      info.adj_sector_gestion || v_separador ||
                                      info.adj_actor || v_separador ||
                                      info.adj_nro_orden || v_separador ||
                                      info.adj_tipo_info || v_separador ||
                                      info.adj_cod_tipo_doc || v_separador ||
                                      info.adj_secuencia || v_separador ||
                                      info.adj_dato_info || v_separador ||
                                      info.adj_link_info || v_separador ||
                                      info.adj_fecha_info || v_separador ||
                                      info.adj_user_info || v_separador ||
                                      info.adj_sector_info || v_separador ||
                                      info.adj_nom_archivo_oroginal || v_separador ||
                                      info.adj_nom_archivo || v_separador ||
                                      info.adj_directorio_archivo || v_separador ||
                                      info.adj_importe);
      end loop;

      v_paso := 6;
      utl_file.fclose(out_file_1);

      for fg in (select fact_nro_gestion,fact_cod_circuito,fact_cod_prioridad,fact_cod_favorabilidad,fact_cod_segmento,fact_cod_paquete,fact_cod_enc,fact_cod_cliente,fact_cod_tipo_medio,fact_cod_estados,fact_cod_usuario,
                        fact_cod_sector,fact_cod_responsabilidad,fact_cod_modelo_cartas,fact_fecha_estado,fact_orden_estado,fact_fec_max_resol,fact_ind_dias_gest,fact_ind_importe_reclamado,fact_ind_moneda_reclamado,
                        fact_ind_importe_concedido,fact_ind_moneda_concedido,fact_ind_reapertura,fact_ultimo_estado,fact_fec_alta,fact_fec_bandeja_actual,fact_sla,fact_marca_cerrado,fact_sector_alta,fact_cerradas_dev,
                        fact_marca_devolucion,fact_fecha_circuito,fact_marca_visa,fact_ult_est_mes,fact_comentario_est,fact_fec_res_resol_1,fact_fec_res_resol_2,fact_fec_res_resol_3,fact_fec_resp,fact_responsable_resol,
                        fact_desc_sec_resol_1,fact_desc_sec_resol_2,fact_desc_sec_resol_3,fact_desc_sec_resp,fact_cod_suc_cta,fact_cod_crm,fact_nro_cta,fact_bandeja_actual,fact_cod_ult_estado,fact_desc_ult_estado,fact_fec_cierre,
                        fact_ingreso_epa,fact_resp_de_autoriz,fact_no_autorizado,fact_validacion_rci,fact_fecha_siguiente,fact_ind_dias_x_estado,fact_mes_anio_cierre,fact_resp_de_rta,fact_gestion_inicial,fact_gestion_anterior,
                        fact_cant_reaperturas,fact_plazo_legal,fact_cod_rta_resol_predef,fact_nro_ticket,fact_prefijo_ticket
                   from gec02.fact_gestiones
                  where fact_fecha_estado between to_char(fec_desde,'YYYYMMDD') and to_char(fec_hasta,'YYYYMMDD')) loop
        v_paso := 7;
        utl_file.put_line (out_file_2, fg.fact_nro_gestion
                          || v_separador || fg.fact_cod_circuito
                          || v_separador || fg.fact_cod_prioridad
                          || v_separador || fg.fact_cod_favorabilidad
                          || v_separador || fg.fact_cod_segmento
                          || v_separador || fg.fact_cod_paquete
                          || v_separador || fg.fact_fec_max_resol
                          || v_separador || fg.fact_ind_dias_gest
                          || v_separador || fg.fact_ind_importe_reclamado
                          || v_separador || fg.fact_ind_moneda_reclamado
                          || v_separador || fg.fact_ind_importe_concedido
                          || v_separador || fg.fact_ind_moneda_concedido
                          || v_separador || fg.fact_cod_enc
                          || v_separador || fg.fact_cod_cliente
                          || v_separador || fg.fact_ind_reapertura
                          || v_separador || fg.fact_ultimo_estado
                          || v_separador || fg.fact_cod_estados
                          || v_separador || fg.fact_cod_tipo_medio
                          || v_separador || fg.fact_cod_usuario
                          || v_separador || fg.fact_cod_sector
                          || v_separador || fg.fact_cod_responsabilidad
                          || v_separador || fg.fact_cod_modelo_cartas
                          || v_separador || fg.fact_orden_estado
                          || v_separador || fg.fact_fecha_estado
                          || v_separador || fg.fact_fec_alta
                          || v_separador || fg.fact_fec_bandeja_actual
                          || v_separador || fg.fact_sla
                          || v_separador || fg.fact_marca_cerrado
                          || v_separador || fg.fact_sector_alta
                          || v_separador || fg.fact_cerradas_dev
                          || v_separador || fg.fact_marca_devolucion
                          || v_separador || fg.fact_fecha_circuito
                          || v_separador || fg.fact_marca_visa
                          || v_separador || fg.fact_ult_est_mes
                          || v_separador || replace(replace(fg.fact_comentario_est,chr(10),''),chr(13),'')
                          || v_separador || fg.fact_fec_res_resol_1
                          || v_separador || fg.fact_fec_res_resol_2
                          || v_separador || fg.fact_fec_res_resol_3
                          || v_separador || fg.fact_fec_resp
                          || v_separador || fg.fact_responsable_resol
                          || v_separador || fg.fact_desc_sec_resol_1
                          || v_separador || fg.fact_desc_sec_resol_2
                          || v_separador || fg.fact_desc_sec_resol_3
                          || v_separador || fg.fact_desc_sec_resp
                          || v_separador || fg.fact_cod_suc_cta
                          || v_separador || fg.fact_cod_crm
                          || v_separador || fg.fact_nro_cta
                          || v_separador || fg.fact_bandeja_actual
                          || v_separador || fg.fact_cod_ult_estado
                          || v_separador || fg.fact_desc_ult_estado
                          || v_separador || fg.fact_fec_cierre
                          || v_separador || fg.fact_ingreso_epa
                          || v_separador || fg.fact_resp_de_autoriz
                          || v_separador || fg.fact_no_autorizado
                          || v_separador || fg.fact_validacion_rci
                          || v_separador || fg.fact_fecha_siguiente
                          || v_separador || fg.fact_ind_dias_x_estado
                          || v_separador || fg.fact_mes_anio_cierre
                          || v_separador || fg.fact_resp_de_rta
                          || v_separador || fg.fact_gestion_inicial
                          || v_separador || fg.fact_gestion_anterior
                          || v_separador || fg.fact_cant_reaperturas
                          || v_separador || fg.fact_plazo_legal
                          || v_separador || fg.fact_cod_rta_resol_predef
                          || v_separador || fg.fact_nro_ticket
                          || v_separador || fg.fact_prefijo_ticket);
      end loop;
      v_paso := 8;
      utl_file.fclose(out_file_2);
      commit;

      v_paso := 9;
      v_fecha_fin := sysdate;
      pkg_gc_procesos.sp_procesos(2, 2, v_fecha_inicio, v_fecha_fin, id_proceso);
    else
      pkg_gc_procesos.sp_procesos(3, 0, v_fecha_inicio, v_fecha_fin, id_proceso);
      pkg_gc_procesos.sp_inconsistencias(id_proceso, sysdate, v_paso, 'Existen procesos con error que impiden la ejecuciÃ³n.');
    end if;
  exception
     when others then
      v_fecha_fin := sysdate;
      pkg_gc_procesos.sp_procesos(2, 6, v_fecha_inicio, v_fecha_fin, id_proceso);
      v_err_sql := substr (sqlerrm (sqlcode), 1, 250);
      pkg_gc_procesos.sp_inconsistencias(id_proceso, sysdate, v_paso, v_err_sql);
  end sp_insert_files;

/******************************************************************************/
---------------------------- SP_CUMPLIMIENTO_SLA -------------------------------
/******************************************************************************/
  procedure sp_cumplimiento_sla(id_proceso in number, fec_desde in date, fec_hasta in date) as
  begin
    --Verifica si hay procesos con error
    pkg_gc_procesos.sp_busca_procesos_error(v_cantidad_error);

    if v_cantidad_error = 0 then
      v_fecha_inicio := sysdate;
      pkg_gc_procesos.sp_procesos(1, 1, v_fecha_inicio, null, id_proceso);
      v_paso := 1;
      /*insert into ig_gestion_estados (cod_entidad,ide_gestion_sector,ide_gestion_nro,cod_est_gest,fec_est_gest,tpo_medio,cod_rta_resol_predef,cod_contc,cod_niv_conf,indi_impre_masiva,imp_gest_estado,cod_mone_imp,cod_carta,cod_sect_asign_espec,tmp_est_gest,user_estado,cod_sect_estado,cod_bandeja,orden_estado,cod_responsab_est,actor_nro_orden,comentario,ide_gestion_sec,ind_modf_fec_resol)
           select cod_entidad,ide_gestion_sector,ide_gestion_nro,cod_est_gest,fec_est_gest,tpo_medio,cod_rta_resol_predef,cod_contc,cod_niv_conf,indi_impre_masiva,imp_gest_estado,cod_mone_imp,cod_carta,cod_sect_asign_espec,tmp_est_gest,user_estado,cod_sect_estado,cod_bandeja,orden_estado,cod_responsab_est,actor_nro_orden,comentario,ide_gestion_sec,ind_modf_fec_resol
             from gec01.gestion_estados
            where fec_est_gest between fec_desde and fec_hasta;

      v_paso := 2;
      insert into ig_gestiones (cod_entidad,ide_gestion_sector,ide_gestion_nro,ide_circuito,indi_tipo_circ,indi_decision_comer,indi_replteo,indi_rta_inmed,indi_impre_cart,imp_autz_solicitado,cod_mone_solicitado,imp_autz_autorizado,cod_mone_autorizado,imp_autz_resolucion,cod_mone_resolucion,tpo_pers,cod_crm,comen_cliente,ide_gest_sector_relac,ide_gest_nro_relac,fec_gestion_alta,cod_bandeja_actual,fec_bandeja_actual,cod_sector_actual,inic_user_alta,indi_mail,indi_prioridad,fec_max_resol,cod_conforme,cod_user_actual,cod_grupo_empresa,cod_tipo_favorabilidad,fec_aviso_venc)
           select distinct ges.cod_entidad,ges.ide_gestion_sector,ges.ide_gestion_nro,ges.ide_circuito,ges.indi_tipo_circ,ges.indi_decision_comer,ges.indi_replteo,ges.indi_rta_inmed,ges.indi_impre_cart,ges.imp_autz_solicitado,ges.cod_mone_solicitado,ges.imp_autz_autorizado,ges.cod_mone_autorizado,ges.imp_autz_resolucion,ges.cod_mone_resolucion,ges.tpo_pers,ges.cod_crm,ges.comen_cliente,ges.ide_gest_sector_relac,ges.ide_gest_nro_relac,ges.fec_gestion_alta,ges.cod_bandeja_actual,ges.fec_bandeja_actual,ges.cod_sector_actual,ges.inic_user_alta,ges.indi_mail,ges.indi_prioridad,ges.fec_max_resol,ges.cod_conforme,ges.cod_user_actual,ges.cod_grupo_empresa,ges.cod_tipo_favorabilidad,ges.fec_aviso_venc
             from gec01.gestiones ges, ig_gestion_estados est
            where ges.cod_entidad = est.cod_entidad
              and ges.ide_gestion_sector = est.ide_gestion_sector
              and ges.ide_gestion_nro = est.ide_gestion_nro
              and est.cod_est_gest = 100;

      v_paso := 3;
      insert into ig_circuito (cod_entidad,ide_circuito,cod_circuito,fec_vig_dde_circ,fec_vig_hta_circ,desc_circ,desc_detall_circ,tpo_gestion,cod_prod,cod_subprod,cod_cpto,cod_subcpto,tmp_asign_especial,tmp_pedido_info,tmp_circ,indi_gest_pend,indi_mail_demora,indi_cierr_autm,cod_recibo,est_circ,user_alt_circ,fec_alt_circ,user_modf_circ,fec_modf_circ,indi_modif_datos,indi_recep_resp,indi_sucursales,indi_dejar_pend,cod_tpo_resol,und_tiempo,tpo_vis_gest,indi_suma_tmp_autz,cod_conforme,indi_jerarquia_autz,indi_secuencia_autz,indi_asig_paralelo,indi_autoriz_devol,indi_modf_fec,circuito_predecesor,indi_autoriza_item,indi_crit,indi_info_multivaluada,indi_enviar_mail_recep,indi_enviar_sms_recep,indi_enviar_mail_resp,indi_enviar_sms_resp,indi_enviar_mail_demora,cod_modelo_msj,id_parrafo_cuerpo_mail,indi_uso_monto,id_clasif_select,indi_enviar_mail_prov,cod_comprobante_cliente,indi_enviar_carta_resp,indi_reapertura,long_comentario_recep,long_comentario_cliente,aviso_venc_gest)
           select distinct cir.cod_entidad,cir.ide_circuito,cir.cod_circuito,cir.fec_vig_dde_circ,cir.fec_vig_hta_circ,cir.desc_circ,cir.desc_detall_circ,cir.tpo_gestion,cir.cod_prod,cir.cod_subprod,cir.cod_cpto,cir.cod_subcpto,cir.tmp_asign_especial,cir.tmp_pedido_info,cir.tmp_circ,cir.indi_gest_pend,cir.indi_mail_demora,cir.indi_cierr_autm,cir.cod_recibo,cir.est_circ,cir.user_alt_circ,cir.fec_alt_circ,cir.user_modf_circ,cir.fec_modf_circ,cir.indi_modif_datos,cir.indi_recep_resp,cir.indi_sucursales,cir.indi_dejar_pend,cir.cod_tpo_resol,cir.und_tiempo,cir.tpo_vis_gest,cir.indi_suma_tmp_autz,cir.cod_conforme,cir.indi_jerarquia_autz,cir.indi_secuencia_autz,cir.indi_asig_paralelo,cir.indi_autoriz_devol,cir.indi_modf_fec,cir.circuito_predecesor,cir.indi_autoriza_item,cir.indi_crit,cir.indi_info_multivaluada,cir.indi_enviar_mail_recep,cir.indi_enviar_sms_recep,cir.indi_enviar_mail_resp,cir.indi_enviar_sms_resp,cir.indi_enviar_mail_demora,cir.cod_modelo_msj,cir.id_parrafo_cuerpo_mail,cir.indi_uso_monto,cir.id_clasif_select,cir.indi_enviar_mail_prov,cir.cod_comprobante_cliente,cir.indi_enviar_carta_resp,cir.indi_reapertura,cir.long_comentario_recep,cir.long_comentario_cliente,cir.aviso_venc_gest
             from gec01.circuito cir, ig_gestiones ges
            where cir.cod_entidad = ges.cod_entidad
              and cir.ide_circuito = ges.ide_circuito;

      v_paso := 4;
      for gestion in (select distinct ge.cod_entidad,
                             ge.ide_gestion_sector,
                             ge.ide_gestion_nro,
                             ci.ide_circuito,
                             gs.cod_sect_estado,
                             trunc (gs.cod_est_gest/100) cod_actor,
                             nvl(gs.actor_nro_orden,1)   actor_nro_orden,
                             case trunc (gs.cod_est_gest/100)
                              when 5 then
                                (select gs2.fec_est_gest
                                   from ig_gestion_estados gs2
                                  where gs2.cod_entidad = gs.cod_entidad
                                    and gs2.ide_gestion_sector = gs.ide_gestion_sector
                                    and gs2.ide_gestion_nro = gs.ide_gestion_nro
                                    and gs2.ide_gestion_sec = gs.ide_gestion_sec
                                    and gs2.orden_estado = gs.orden_estado - 2)
                              when 6 then
                                (select gs2.fec_est_gest
                                   from ig_gestion_estados gs2
                                  where gs2.cod_entidad = gs.cod_entidad
                                    and gs2.ide_gestion_sector = gs.ide_gestion_sector
                                    and gs2.ide_gestion_nro = gs.ide_gestion_nro
                                    and gs2.ide_gestion_sec = gs.ide_gestion_sec
                                    and gs2.orden_estado = gs.orden_estado - 2)
                              else null end fecha_ingreso,
                              case trunc (gs.cod_est_gest/100)
                                when 5 then
                                  gs.fec_est_gest
                                when 6 then
                                  gs.fec_est_gest
                                else null end fecha_egreso,
                              case trunc (gs.cod_est_gest/100)
                                when 1 then
                                  (select max(gs2.orden_estado)
                                     from ig_gestion_estados gs2
                                    where gs2.cod_entidad = gs.cod_entidad
                                      and gs2.ide_gestion_sector = gs.ide_gestion_sector
                                      and gs2.ide_gestion_nro = gs.ide_gestion_nro
                                      and gs2.ide_gestion_sec = gs.ide_gestion_sec
                                      and gs2.cod_est_gest between 100 and 199
                                      and gs2.fec_est_gest between fec_desde and fec_hasta)
                                when 2 then
                                  (select max(gs2.orden_estado)
                                     from ig_gestion_estados gs2
                                    where gs2.cod_entidad = gs.cod_entidad
                                      and gs2.ide_gestion_sector = gs.ide_gestion_sector
                                      and gs2.ide_gestion_nro = gs.ide_gestion_nro
                                      and gs2.ide_gestion_sec = gs.ide_gestion_sec
                                      and gs2.actor_nro_orden = gs.actor_nro_orden
                                      and gs2.cod_est_gest between 200 and 299
                                      and gs2.fec_est_gest between fec_desde and fec_hasta)
                                when 3 then
                                  (select max(gs2.orden_estado)
                                     from ig_gestion_estados gs2
                                    where gs2.cod_entidad = gs.cod_entidad
                                      and gs2.ide_gestion_sector = gs.ide_gestion_sector
                                      and gs2.ide_gestion_nro = gs.ide_gestion_nro
                                      and gs2.ide_gestion_sec = gs.ide_gestion_sec
                                      and gs2.actor_nro_orden = gs.actor_nro_orden
                                      and gs2.cod_est_gest between 300 and 399
                                      and gs2.fec_est_gest between fec_desde and fec_hasta)
                                when 4 then
                                  (select max(gs2.orden_estado)
                                     from ig_gestion_estados gs2
                                    where gs2.cod_entidad = gs.cod_entidad
                                      and gs2.ide_gestion_sector = gs.ide_gestion_sector
                                      and gs2.ide_gestion_nro = gs.ide_gestion_nro
                                      and gs2.ide_gestion_sec = gs.ide_gestion_sec
                                      and gs2.cod_est_gest between 400 and 499
                                      and gs2.fec_est_gest between fec_desde and fec_hasta)
                                when 5 then
                                  gs.orden_estado
                                when 6 then
                                  gs.orden_estado
                                end orden_estado
                        from ig_gestiones ge,
                             ig_circuito ci,
                             ig_gestion_estados gs
                       where ge.cod_entidad = ci.cod_entidad
                         and ge.ide_circuito = ci.ide_circuito
                         and ci.tpo_gestion in (2,3)  -- reclamos, requerimientos
                         and gs.fec_est_gest between fec_desde and fec_hasta
                         and ge.cod_entidad = gs.cod_entidad
                         and ge.ide_gestion_sector = gs.ide_gestion_sector
                         and ge.ide_gestion_nro = gs.ide_gestion_nro
                         and gs.ide_gestion_sec = 0
                    order by ci.ide_circuito) loop

        --obtengo el tiempo estandar para el actor
        v_tiempo := gec01.pkg_gc_repo_de_pendientes.obtener_tiempo_std(gestion.cod_entidad,
                                                                       gestion.ide_gestion_sector,
                                                                       gestion.ide_gestion_nro,
                                                                       gestion.ide_circuito,
                                                                       gestion.cod_actor,
                                                                       gestion.actor_nro_orden);
        --obtengo el tiempo real que consumiÃ³ el actor
        v_tiempo_actor := gec01.pkg_gc_repo_de_pendientes.obtengo_tiempo_x_actor (gestion.cod_entidad,
                                                                                  gestion.ide_gestion_sector,
                                                                                  gestion.ide_gestion_nro,
                                                                                  gestion.cod_sect_estado,
                                                                                  gestion.cod_actor,
                                                                                  gestion.actor_nro_orden,
                                                                                  gestion.fecha_ingreso,
                                                                                  gestion.fecha_egreso);

        -- define el desvio o no del sector
        if(v_tiempo_actor > v_tiempo) then
          v_desvio_status := 'S';
          --v_tiempo_desvio := v_tiempo_actor - v_tiempo;
        else
          v_desvio_status := 'N';
          --v_tiempo_desvio := 0;
        end if;

        -- se marca la gestion - sector - orden estado
        update fact_gestiones
           set fact_sla = v_desvio_status
         where fact_nro_gestion = gestion.ide_gestion_nro
           and fact_cod_sector = gestion.cod_sect_estado
           and fact_orden_estado = gestion.orden_estado;
      end loop;*/
      commit;

      v_paso := 5;
      v_fecha_fin := sysdate;
      pkg_gc_procesos.sp_procesos(2, 2, v_fecha_inicio, v_fecha_fin, id_proceso);
    else
      pkg_gc_procesos.sp_procesos(3, 0, v_fecha_inicio, v_fecha_fin, id_proceso);
      pkg_gc_procesos.sp_inconsistencias(id_proceso, sysdate, v_paso, 'Existen procesos con error que impiden la ejecuciÃ³n.');
    end if;
  exception
     when others then
      v_fecha_fin := sysdate;
      pkg_gc_procesos.sp_procesos(2, 6, v_fecha_inicio, v_fecha_fin, id_proceso);
      v_err_sql := substr (sqlerrm (sqlcode), 1, 250);
      pkg_gc_procesos.sp_inconsistencias(id_proceso, sysdate, v_paso, v_err_sql);
  end sp_cumplimiento_sla;

/******************************************************************************/
------------------------ SP_CERRADAS_X_DEVOLUCION ------------------------------
/******************************************************************************/
  procedure sp_cerradas_x_devolucion(id_proceso in number, fec_desde in date, fec_hasta in date, gestiones in number) as
  begin
    --Verifica si hay procesos con error
    pkg_gc_procesos.sp_busca_procesos_error(v_cantidad_error);

    if v_cantidad_error = 0 then
      v_fecha_inicio := sysdate;
      pkg_gc_procesos.sp_procesos(1, 1, v_fecha_inicio, null, id_proceso);

      if gestiones = 0 then
        v_paso := 1;
          update fact_gestiones
             set fact_cerradas_dev = 1
           where fact_nro_gestion in (select distinct fact_nro_gestion
                                        from fact_gestiones
                                       where fact_fecha_estado between to_char(fec_desde,'YYYYMMDD')
                                                                   and to_char(fec_hasta,'YYYYMMDD')
                                         and fact_ultimo_estado = 1
                                         and fact_cod_estados = 110)
             and substr(fact_fecha_estado,1,6) = to_char(fec_hasta,'YYYYMM');

      elsif gestiones = 1 then
        v_paso := 2;
          update fact_gestiones
             set fact_cerradas_dev = 1
           where fact_nro_gestion in (select distinct fact_nro_gestion
                                        from fact_gestiones, dim_gestiones_regularizar
                                       where fact_nro_gestion = reg_nro_gestion
                                         and fact_ultimo_estado = 1
                                         and fact_cod_estados = 110)
             and substr(fact_fecha_estado,1,6) = to_char(fec_hasta,'YYYYMM');
      end if;
      commit;

      v_paso := 3;
      v_fecha_fin := sysdate;
      pkg_gc_procesos.sp_procesos(2, 2, v_fecha_inicio, v_fecha_fin, id_proceso);
    else
      pkg_gc_procesos.sp_procesos(3, 0, v_fecha_inicio, v_fecha_fin, id_proceso);
      pkg_gc_procesos.sp_inconsistencias(id_proceso, sysdate, v_paso, 'Existen procesos con error que impiden la ejecuciÃ³n.');
    end if;
  exception
     when others then
      v_fecha_fin := sysdate;
      pkg_gc_procesos.sp_procesos(2, 6, v_fecha_inicio, v_fecha_fin, id_proceso);
      v_err_sql := substr (sqlerrm (sqlcode), 1, 250);
      pkg_gc_procesos.sp_inconsistencias(id_proceso, sysdate, v_paso, v_err_sql);
  end sp_cerradas_x_devolucion;

/******************************************************************************/
------------------------ SP_5_DIAS_HABILES ------------------------------
/******************************************************************************/
  procedure sp_5_dias_habiles(id_proceso in number, fec_desde in date, fec_hasta in date) as
  begin
    --Verifica si hay procesos con error
    pkg_gc_procesos.sp_busca_procesos_error(v_cantidad_error);

    if v_cantidad_error = 0 then
      v_fecha_inicio := sysdate;
      pkg_gc_procesos.sp_procesos(1, 1, v_fecha_inicio, null, id_proceso);
      /*v_paso := 1;
      for x in (select distinct cir_cod_prod, cir_cod_subprod, cir_cod_cpto, cir_cod_subcpto
                  from dim_circuitos
                 where cir_cod_prod = 4
                   and cir_cod_subprod in (22,369,20,329,359,8,342,368,169)
                   and cir_cod_cpto = 65
                   and cir_cod_subcpto in (1402,1494,1589,1590,1577,1580,1578,1536,1537,1559,510,1069,509,1416,1548,1591,1581,1068,1414,1535,1579,1415,1421,1077,714,1526,1560,1592,1053,855,1561)
                   and cir_5_dias <> 1
                 union all
                select distinct cir_cod_prod, cir_cod_subprod, cir_cod_cpto, cir_cod_subcpto
                  from dim_circuitos
                 where cir_cod_prod = 10
                   and cir_cod_subprod = 381
                   and cir_cod_cpto = 65
                   and cir_cod_subcpto = 1053
                   and cir_5_dias <> 1
                 union all
                select distinct cir_cod_prod, cir_cod_subprod, cir_cod_cpto, cir_cod_subcpto
                  from dim_circuitos
                 where cir_cod_prod = 19
                   and cir_cod_subprod in (2,92,208)
                   and cir_cod_cpto = 5
                   and cir_cod_subcpto in (1552,1574,1533,1550,1554,1573,1522,1536,1551,1525,1583,1523,1527,1528,1553,1556,1558,1572,1415,1524,1571,1526,1549,1555,1557,1529,1584)
                   and cir_5_dias <> 1) loop

        l_5_dias_habiles.extend;

        l_5_dias_habiles(l_5_dias_habiles.last).cir_cod_prod := x.cir_cod_prod;
        l_5_dias_habiles(l_5_dias_habiles.last).cir_cod_subprod := x.cir_cod_subprod;
        l_5_dias_habiles(l_5_dias_habiles.last).cir_cod_cpto := x.cir_cod_cpto;
        l_5_dias_habiles(l_5_dias_habiles.last).cir_cod_subcpto := x.cir_cod_subcpto;
      end loop;

      v_paso := 2;
      --Se modifica la marca de ultimo estado para los estados que dejan de ser ultimo
      forall x in l_5_dias_habiles.first .. l_5_dias_habiles.last
        update dim_circuitos
           set cir_5_dias = 1
         where cir_cod_prod = l_5_dias_habiles(x).cir_cod_prod
           and cir_cod_subprod = l_5_dias_habiles(x).cir_cod_subprod
           and cir_cod_cpto = l_5_dias_habiles(x).cir_cod_cpto
           and cir_cod_subcpto = l_5_dias_habiles(x).cir_cod_subcpto;
      commit;
      */
      v_paso := 3;
      v_fecha_fin := sysdate;
      pkg_gc_procesos.sp_procesos(2, 2, v_fecha_inicio, v_fecha_fin, id_proceso);
    else
      pkg_gc_procesos.sp_procesos(3, 0, v_fecha_inicio, v_fecha_fin, id_proceso);
      pkg_gc_procesos.sp_inconsistencias(id_proceso, sysdate, v_paso, 'Existen procesos con error que impiden la ejecuciÃ³n.');
    end if;
  exception
     when others then
      v_fecha_fin := sysdate;
      pkg_gc_procesos.sp_procesos(2, 6, v_fecha_inicio, v_fecha_fin, id_proceso);
      v_err_sql := substr (sqlerrm (sqlcode), 1, 250);
      pkg_gc_procesos.sp_inconsistencias(id_proceso, sysdate, v_paso, v_err_sql);
  end sp_5_dias_habiles;

/******************************************************************************/
--------------------------------- SP_DEVOLUCIONES ------------------------------
/******************************************************************************/
  procedure sp_devoluciones(id_proceso in number, fec_desde in date, fec_hasta in date, gestiones in number) as
  begin
    --Verifica si hay procesos con error
    pkg_gc_procesos.sp_busca_procesos_error(v_cantidad_error);

    if v_cantidad_error = 0 then
      v_fecha_inicio := sysdate;
      pkg_gc_procesos.sp_procesos(1, 1, v_fecha_inicio, null, id_proceso);

      if gestiones = 0 then
        v_paso := 1;
          update fact_gestiones
             set fact_marca_devolucion = 1
           where fact_nro_gestion in (select distinct fact_nro_gestion
                                        from fact_gestiones
                                       where fact_fecha_estado between to_char(fec_desde,'YYYYMMDD')
                                                                   and to_char(fec_hasta,'YYYYMMDD')
                                         and fact_cod_estados = 116)
             and fact_marca_devolucion = 0;
        commit;

        v_paso := 2;
        --Se agrega UPDATE para regularizar los casos en las que la gestion con devolucion tiene movimientos al mes siguiente
        update fact_gestiones
           set fact_marca_devolucion = 1
         where fact_nro_gestion in (select distinct fact_nro_gestion
                                      from fact_gestiones
                                    having count(distinct fact_marca_devolucion) > 1
                                     group by fact_nro_gestion)
           and fact_marca_devolucion = 0;
      elsif gestiones = 1 then
        v_paso := 3;
          update fact_gestiones
             set fact_marca_devolucion = 1
           where fact_nro_gestion in (select distinct fact_nro_gestion
                                        from fact_gestiones, dim_gestiones_regularizar
                                       where fact_nro_gestion = reg_nro_gestion
                                         and fact_cod_estados = 116)
             and fact_marca_devolucion = 0;
        commit;

        v_paso := 4;
        --Se agrega UPDATE para regularizar los casos en las que la gestion con devolucion tiene movimientos al mes siguiente
        update fact_gestiones
           set fact_marca_devolucion = 1
         where fact_nro_gestion in (select distinct fact_nro_gestion
                                      from fact_gestiones, dim_gestiones_regularizar
                                     where fact_nro_gestion = reg_nro_gestion
                                    having count(distinct fact_marca_devolucion) > 1
                                     group by fact_nro_gestion)
           and fact_marca_devolucion = 0;
      end if;
      commit;

      v_paso := 5;
      v_fecha_fin := sysdate;
      pkg_gc_procesos.sp_procesos(2, 2, v_fecha_inicio, v_fecha_fin, id_proceso);
    else
      pkg_gc_procesos.sp_procesos(3, 0, v_fecha_inicio, v_fecha_fin, id_proceso);
      pkg_gc_procesos.sp_inconsistencias(id_proceso, sysdate, v_paso, 'Existen procesos con error que impiden la ejecuciÃ³n.');
    end if;
  exception
     when others then
      v_fecha_fin := sysdate;
      pkg_gc_procesos.sp_procesos(2, 6, v_fecha_inicio, v_fecha_fin, id_proceso);
      v_err_sql := substr (sqlerrm (sqlcode), 1, 250);
      pkg_gc_procesos.sp_inconsistencias(id_proceso, sysdate, v_paso, v_err_sql);
  end sp_devoluciones;

/******************************************************************************/
--------------------------------- SP_TRAC -----------------------------
/******************************************************************************/
  procedure sp_trac(id_proceso in number, fec_desde in date, fec_hasta in date) as
  begin
    --Verifica si hay procesos con error
    pkg_gc_procesos.sp_busca_procesos_error(v_cantidad_error);

    if v_cantidad_error = 0 then
      v_fecha_inicio := sysdate;
      pkg_gc_procesos.sp_procesos(1, 1, v_fecha_inicio, null, id_proceso);
      v_paso := 1;
      --Logica para verificar que la gestion padre se cierre ya que sus hijas estan todas cerradas
      for x in (select distinct fact_nro_gestion, fact_marca_cerrado
                  from fact_gestiones
                 where fact_fecha_estado between to_char(fec_desde,'YYYYMMDD') and to_char(fec_hasta,'YYYYMMDD')
                   and fact_sector_alta in ('TRAC','PROM')
                   and fact_ultimo_estado = 1) loop

        select count(*)
          into v_cerradas_trac
          from dim_gestiones_trac
         where trac_gestion_padre = x.fact_nro_gestion
           and upper(trac_bandeja_actual) <> 'CERRADO';

        if v_cerradas_trac = 0 then
          update fact_gestiones
             set fact_marca_cerrado = 1
           where fact_nro_gestion = x.fact_nro_gestion
             and fact_ultimo_estado = 1;

          update fact_gestiones
             set fact_marca_cerrado = 0
           where fact_nro_gestion = x.fact_nro_gestion
             and fact_ultimo_estado <> 1;

          select count(*)
            into v_favorabilidad_1
            from dim_gestiones_trac
           where trac_gestion_padre = x.fact_nro_gestion
             and trac_favorabilidad = 1;

          select count(*)
            into v_favorabilidad_2
            from dim_gestiones_trac
           where trac_gestion_padre = x.fact_nro_gestion
             and trac_favorabilidad = 2;

          select count(*)
            into v_favorabilidad_3
            from dim_gestiones_trac
           where trac_gestion_padre = x.fact_nro_gestion
             and trac_favorabilidad = 3;

          if v_favorabilidad_3 = 0 and v_favorabilidad_2 = 0 and v_favorabilidad_1 > 0 then
            update fact_gestiones
               set fact_cod_favorabilidad = 1
             where fact_nro_gestion = x.fact_nro_gestion;
          elsif v_favorabilidad_2 > 0 and v_favorabilidad_1 = 0 and v_favorabilidad_3 = 0 then
            update fact_gestiones
               set fact_cod_favorabilidad = 2
             where fact_nro_gestion = x.fact_nro_gestion;
          elsif v_favorabilidad_2 = 0 and v_favorabilidad_1 = 0 and v_favorabilidad_3 = 0 then
            update fact_gestiones
               set fact_cod_favorabilidad = 0
             where fact_nro_gestion = x.fact_nro_gestion;
          else
            update fact_gestiones
               set fact_cod_favorabilidad = 3
             where fact_nro_gestion = x.fact_nro_gestion;
          end if;
        end if;

      end loop;
      commit;

      v_paso := 2;
      v_fecha_fin := sysdate;
      pkg_gc_procesos.sp_procesos(2, 2, v_fecha_inicio, v_fecha_fin, id_proceso);
    else
      pkg_gc_procesos.sp_procesos(3, 0, v_fecha_inicio, v_fecha_fin, id_proceso);
      pkg_gc_procesos.sp_inconsistencias(id_proceso, sysdate, v_paso, 'Existen procesos con error que impiden la ejecuciÃ³n.');
    end if;
  exception
     when others then
      v_fecha_fin := sysdate;
      pkg_gc_procesos.sp_procesos(2, 6, v_fecha_inicio, v_fecha_fin, id_proceso);
      v_err_sql := substr (sqlerrm (sqlcode), 1, 250);
      pkg_gc_procesos.sp_inconsistencias(id_proceso, sysdate, v_paso, v_err_sql);
  end sp_trac;

/******************************************************************************/
------------------------------ SP_GESTIONES_VISA -------------------------------
/******************************************************************************/
  procedure sp_gestiones_visa(id_proceso in number, fec_desde in date, fec_hasta in date) as
  begin
    --Verifica si hay procesos con error
    pkg_gc_procesos.sp_busca_procesos_error(v_cantidad_error);

    if v_cantidad_error = 0 then
      v_fecha_inicio := sysdate;
      pkg_gc_procesos.sp_procesos(1, 1, v_fecha_inicio, null, id_proceso);
      v_paso := 1;
      --Se marca las gestiones que ingresaron por VISA para excluir en los reportes de SAP BO
        update fact_gestiones
           set fact_marca_visa = 1
         where fact_nro_gestion in (select distinct fact_nro_gestion
                                      from dim_gestiones_visa, fact_gestiones
                                     where visa_nro_gestion = fact_nro_gestion
                                       and fact_fecha_estado between to_char(fec_desde,'YYYYMMDD')
                                                                 and to_char(fec_hasta,'YYYYMMDD')
                                       and fact_ultimo_estado = 1);

        v_paso := 2;
        --Se marca como procesadas las gestiones que ingresaron por VISA
        update dim_gestiones_visa
           set visa_procesada = 'S',
               visa_fecha_proceso = sysdate
         where visa_procesada = 'N'
           and visa_nro_gestion in (select distinct fact_nro_gestion
                                      from dim_gestiones_visa, fact_gestiones
                                     where visa_nro_gestion = fact_nro_gestion
                                       and fact_fecha_estado between to_char(fec_desde,'YYYYMMDD')
                                                                 and to_char(fec_hasta,'YYYYMMDD')
                                       and fact_marca_cerrado = 1
                                       and fact_ultimo_estado = 1);
      commit;

      v_paso := 3;
      v_fecha_fin := sysdate;
      pkg_gc_procesos.sp_procesos(2, 2, v_fecha_inicio, v_fecha_fin, id_proceso);
    else
      pkg_gc_procesos.sp_procesos(3, 0, v_fecha_inicio, v_fecha_fin, id_proceso);
      pkg_gc_procesos.sp_inconsistencias(id_proceso, sysdate, v_paso, 'Existen procesos con error que impiden la ejecuciÃ³n.');
    end if;
  exception
     when others then
      v_fecha_fin := sysdate;
      pkg_gc_procesos.sp_procesos(2, 6, v_fecha_inicio, v_fecha_fin, id_proceso);
      v_err_sql := substr (sqlerrm (sqlcode), 1, 250);
      pkg_gc_procesos.sp_inconsistencias(id_proceso, sysdate, v_paso, v_err_sql);
  end sp_gestiones_visa;

/******************************************************************************/
--------------------------- SP_ULTIMO_ESTADO_MES -------------------------------
/******************************************************************************/
  procedure sp_ultimo_estado_mes(id_proceso in number, fec_desde in date, fec_hasta in date) as
  begin
    --Verifica si hay procesos con error
    pkg_gc_procesos.sp_busca_procesos_error(v_cantidad_error);

    if v_cantidad_error = 0 then
      v_fecha_inicio := sysdate;
      pkg_gc_procesos.sp_procesos(1, 1, v_fecha_inicio, null, id_proceso);
      v_paso := 1;
      --Se blanquea la columna FACT_ULT_EST_MES de la tabla FACT_GESTIONES para el mes en el que se esta ejecutando el ETL
      update fact_gestiones
         set fact_ult_est_mes = 0
       where substr(fact_fecha_estado,1,6) = to_char(fec_desde,'YYYYMM')
         and fact_nro_gestion in (select distinct fact_nro_gestion
                                    from fact_gestiones
                                   where fact_fecha_estado between to_char(fec_desde,'YYYYMMDD') and to_char(fec_hasta,'YYYYMMDD')
                                     and fact_ultimo_estado = 1
                                     and fact_ult_est_mes = 0);

      v_paso := 2;
      --Se carga las gestiones con su respectivo numero de orden maximo para marcar como ultimo del mes
        update fact_gestiones
           set fact_ult_est_mes = to_char(fec_desde,'MM')
         where (fact_nro_gestion,
                fact_fecha_estado,
                fact_orden_estado) in (select fact_nro_gestion,
                                              max(fact_fecha_estado),
                                              max(fact_orden_estado)
                                         from fact_gestiones
                                        where fact_ult_est_mes = 0
                                          and fact_fecha_estado between to_char(fec_desde,'YYYYMMDD') and to_char(fec_hasta,'YYYYMMDD')
                                        group by fact_nro_gestion);

      commit;

      v_paso := 3;
      v_fecha_fin := sysdate;
      pkg_gc_procesos.sp_procesos(2, 2, v_fecha_inicio, v_fecha_fin, id_proceso);
    else
      pkg_gc_procesos.sp_procesos(3, 0, v_fecha_inicio, v_fecha_fin, id_proceso);
      pkg_gc_procesos.sp_inconsistencias(id_proceso, sysdate, v_paso, 'Existen procesos con error que impiden la ejecuciÃ³n.');
    end if;
  exception
     when others then
      v_fecha_fin := sysdate;
      pkg_gc_procesos.sp_procesos(2, 6, v_fecha_inicio, v_fecha_fin, id_proceso);
      v_err_sql := substr (sqlerrm (sqlcode), 1, 250);
      pkg_gc_procesos.sp_inconsistencias(id_proceso, sysdate, v_paso, v_err_sql);
  end sp_ultimo_estado_mes;

/******************************************************************************/
---------------------------- SP_REGULARIZACION ---------------------------------
/******************************************************************************/
  procedure sp_regularizacion(id_proceso in number, fec_desde in date, fec_hasta in date, regularizacion in number, dias in number) as
  begin
    v_paso := 0;
    --Verifica si hay procesos con error
    pkg_gc_procesos.sp_busca_procesos_error(v_cantidad_error);

    if v_cantidad_error = 0 then

      if regularizacion = 1 then

        v_fecha_inicio := sysdate;
        pkg_gc_procesos.sp_procesos(1, 1, v_fecha_inicio, null, id_proceso);

        v_paso := 1;
        --RegularizaciÃ³n devoluciones
        update fact_gestiones
           set fact_marca_devolucion = 1
         where fact_nro_gestion in (select distinct fact_nro_gestion
                                      from fact_gestiones
                                     where fact_fecha_estado between to_char(fec_desde,'YYYYMMDD') and to_char(fec_hasta,'YYYYMMDD')
                                    having count(distinct fact_marca_devolucion) > 1
                                     group by fact_nro_gestion)
             and fact_marca_devolucion = 0;

        v_fecha_fin := sysdate;
        pkg_gc_procesos.sp_procesos(2, 2, v_fecha_inicio, v_fecha_fin, id_proceso);

        commit;
      end if;

      if regularizacion = 2 then

        v_fecha_inicio := sysdate;
        pkg_gc_procesos.sp_procesos(1, 1, v_fecha_inicio, null, id_proceso);

        v_paso := 2;
        --RegularizaciÃ³n dÃas de gestiÃ³n < 0
        for x in (select fact_nro_gestion, fact_fec_alta, fact_fec_max_resol, fact_fec_bandeja_actual, fact_marca_cerrado
                    from fact_gestiones
                   where fact_ind_dias_gest < 0
                     and fact_fecha_estado between to_char(fec_desde,'YYYYMMDD') and to_char(fec_hasta,'YYYYMMDD')
                     and fact_ultimo_estado = 1) loop

          if x.fact_marca_cerrado = 1 then
              v_dias_gestion := 0;
              v_dias_gestion := fn_dias_habiles(x.fact_fec_alta, x.fact_fec_bandeja_actual,dias, x.fact_nro_gestion);

              update fact_gestiones
                 set fact_ind_dias_gest = v_dias_gestion
               where fact_nro_gestion = x.fact_nro_gestion;
            else
              v_dias_gestion := 0;
              v_dias_gestion := fn_dias_habiles(x.fact_fec_alta, fec_hasta,dias, x.fact_nro_gestion);

              update fact_gestiones
                 set fact_ind_dias_gest = v_dias_gestion
               where fact_nro_gestion = x.fact_nro_gestion;
            end if;
        end loop;

        v_fecha_fin := sysdate;
        pkg_gc_procesos.sp_procesos(2, 2, v_fecha_inicio, v_fecha_fin, id_proceso);

        commit;
      end if;

      if regularizacion = 3 then

        v_fecha_inicio := sysdate;
        pkg_gc_procesos.sp_procesos(1, 1, v_fecha_inicio, null, id_proceso);

        v_paso := 3;
        --RegularizaciÃ³n ultimo estado del mes
        for x in (select distinct fact_nro_gestion, substr(fact_fecha_estado,1,6) fecha, substr(fact_fecha_estado,5,2) mes, count(fact_ult_est_mes)
                    from fact_gestiones
                   where fact_ult_est_mes <> 0
                     and fact_fecha_estado between to_char(fec_desde,'YYYYMMDD') and to_char(fec_hasta,'YYYYMMDD')
                  having count(fact_ult_est_mes) > 1
                   group by fact_nro_gestion, substr(fact_fecha_Estado,1,6), substr(fact_fecha_estado,5,2)) loop

          update fact_gestiones
             set fact_ult_est_mes = 0
           where fact_nro_gestion = x.fact_nro_gestion
             and substr(fact_fecha_estado,1,6) = x.fecha
             and fact_ult_est_mes <> 0;

          select max(fact_orden_estado)
            into v_orden_estado
            from fact_gestiones
           where fact_nro_gestion = x.fact_nro_gestion
             and substr(fact_fecha_estado,1,6) = x.fecha;

          update fact_gestiones
             set fact_ult_est_mes = x.mes
           where fact_nro_gestion = x.fact_nro_gestion
             and fact_orden_estado = v_orden_estado;
        end loop;

        v_fecha_fin := sysdate;
        pkg_gc_procesos.sp_procesos(2, 2, v_fecha_inicio, v_fecha_fin, id_proceso);

        commit;
      end if;

      if regularizacion = 4 then

        v_fecha_inicio := sysdate;
        pkg_gc_procesos.sp_procesos(1, 1, v_fecha_inicio, null, id_proceso);

        v_paso := 4;
        --regularizacion de gestiones TRAC PROM "Hijas" que estan en la FACT
        for x in (select distinct fact_nro_gestion
                    from fact_gestiones, dim_circuitos, dim_gestiones_trac
                   where fact_fecha_estado between to_char(fec_desde,'YYYYMMDD') and to_char(fec_hasta,'YYYYMMDD')
                     and fact_marca_visa <> 1
                     and fact_cerradas_dev <> 1
                     and fact_cod_circuito = cir_cod
                     and cir_cod_tpo_ges in (1,2,5)
                     and fact_ultimo_estado = 1
                     and fact_marca_cerrado = 1
                     and fact_sector_alta in ('TRAC','PROM')
                     and fact_nro_gestion not in (select trac_gestion_padre from dim_gestiones_trac)) loop

          delete from fact_gestiones where fact_nro_gestion = x.fact_nro_gestion;
        end loop;

        v_fecha_fin := sysdate;
        pkg_gc_procesos.sp_procesos(2, 2, v_fecha_inicio, v_fecha_fin, id_proceso);

        commit;
      end if;

      if regularizacion = 5 then

        v_fecha_inicio := sysdate;
        pkg_gc_procesos.sp_procesos(1, 1, v_fecha_inicio, null, id_proceso);

        v_paso := 5;
        --RegularizaciÃ³n comentario por sectores
        insert into ig_gestion_estados (cod_entidad,ide_gestion_sector,ide_gestion_nro,cod_est_gest,fec_est_gest,tpo_medio,cod_rta_resol_predef,cod_contc,cod_niv_conf,indi_impre_masiva,imp_gest_estado,cod_mone_imp,cod_carta,cod_sect_asign_espec,tmp_est_gest,user_estado,cod_sect_estado,cod_bandeja,orden_estado,cod_responsab_est,actor_nro_orden,comentario,ide_gestion_sec,ind_modf_fec_resol)
               select est.cod_entidad,est.ide_gestion_sector,est.ide_gestion_nro,est.cod_est_gest,est.fec_est_gest,est.tpo_medio,est.cod_rta_resol_predef,est.cod_contc,est.cod_niv_conf,est.indi_impre_masiva,est.imp_gest_estado,est.cod_mone_imp,est.cod_carta,est.cod_sect_asign_espec,est.tmp_est_gest,est.user_estado,est.cod_sect_estado,est.cod_bandeja,est.orden_estado,est.cod_responsab_est,est.actor_nro_orden,est.comentario,est.ide_gestion_sec,est.ind_modf_fec_resol
                 from gec01.gestion_estados est, gec01.gestiones ges
                where est.cod_entidad = ges.cod_entidad
                  and est.ide_gestion_sector = ges.ide_gestion_sector
                  and est.ide_gestion_nro = ges.ide_gestion_nro
                  and est.fec_est_gest between fec_desde and fec_hasta
                  and est.comentario is not null;

        --Se cargan las gestiones con los comentarios por cada sector en el que pasaron
          /*for x in (select distinct ide_gestion_nro, orden_estado, dbms_lob.substr(comentario,20000,1) comentario
                      from ig_gestion_estados
                     order by 1) loop

            l_comentarios.extend;

            --l_comentarios(l_comentarios.last).fact_nro_gestion := x.ide_gestion_nro;
            --l_comentarios(l_comentarios.last).fact_orden_estado := x.orden_estado;
            --l_comentarios(l_comentarios.last).fact_comentario_est := x.comentario;
          end loop;*/

          --Se agrega el comentario para cada gestion / sector
          forall x in l_comentarios.first .. l_comentarios.last
            update fact_gestiones
               set fact_comentario_est = l_comentarios(x).fact_comentario_est
             where fact_nro_gestion  = l_comentarios(x).fact_nro_gestion
               and fact_orden_estado = l_comentarios(x).fact_orden_estado;

        v_fecha_fin := sysdate;
        pkg_gc_procesos.sp_procesos(2, 2, v_fecha_inicio, v_fecha_fin, id_proceso);

        commit;
      end if;

      if regularizacion = 6 then

        v_fecha_inicio := sysdate;
        pkg_gc_procesos.sp_procesos(1, 1, v_fecha_inicio, null, id_proceso);

        v_paso := 6;
        --RegularizaciÃ³n para gestiones con marca cerrado duplicada
        for x in (select distinct fact_nro_gestion, min(fact_orden_estado) minimo, max(fact_orden_estado) maximo
                    from fact_gestiones
                   where fact_marca_visa <> 1
                     and fact_marca_cerrado = 1
                  having count(fact_marca_cerrado) > 1
                   group by fact_nro_gestion) loop

          update fact_gestiones
             set fact_marca_cerrado = 0
           where fact_nro_gestion = x.fact_nro_gestion
             and fact_orden_estado = x.minimo;

        end loop;

      v_fecha_fin := sysdate;
      pkg_gc_procesos.sp_procesos(2, 2, v_fecha_inicio, v_fecha_fin, id_proceso);

      commit;
      end if;

      if regularizacion = 7 then

        v_fecha_inicio := sysdate;
        pkg_gc_procesos.sp_procesos(1, 1, v_fecha_inicio, null, id_proceso);

        v_paso := 7;
        --RegularizaciÃ³n para completar los importes de la informacion adjunta
        update dim_informacion_adjunta
           set adj_importe = to_number(adj_dato_info,'9999999.99')
         where adj_fecha_info between fec_desde and fec_hasta
           and adj_cod_tipo_doc in (538,2739,2756,2726);

        v_fecha_fin := sysdate;
        pkg_gc_procesos.sp_procesos(2, 2, v_fecha_inicio, v_fecha_fin, id_proceso);

      commit;
      end if;

      if regularizacion = 8 then

        v_fecha_inicio := sysdate;
        pkg_gc_procesos.sp_procesos(1, 1, v_fecha_inicio, null, id_proceso);

        v_paso := 8;
        insert into ig_gc_gestiones (cod_entidad,ide_gestion_sector,ide_gestion_nro,cod_segm,semf_ingreso_crm,semf_renta_crm,semf_riesgo_crm,calle_dom_contc,nro_dom_contc,piso_dom_contc,dpto_dom_contc,cpost_dom_contc,loc_dom_contc,pcia_dom_contc,pais_dom_contc,ddn_tel_dom_contc,nro_tel_dom_contc,ddn_fax_contc,nro_fax_contc,hra_dom_contc,ddn_celular_contc,nro_celular_contc,ddn_tel_laboral,nro_tel_laboral,hra_tel_laboral,ddn_fax_laboral,nro_fax_laboral,aplic_cta,cod_suc_cta,nro_cta,nro_firm_cta,cod_mone_cta,cod_prod_cta,cod_subpro_cta,nro_tarjeta,link_resumen,dire_mail_contc,dire_mail2_contc,cartera,aplic_cta_gold,cod_suc_cta_gold,nro_cta_gold,nro_firm_cta_gold,cod_mone_cta_gold,cod_prod_cta_gold,cod_subpro_cta_gold,cod_ramo_cta,nro_certificado_cta,rentabilidad_promedio,color_semaforo,color_semaf_riesgo,indi_envios_mya,dire_mail_opcional,nro_paquete,cod_paquete,indi_plan_sueldo,nro_convenio_paquete,emp_celular_contc,tpo_msj_asoc_rta,monto_deuda_ref,no_enviar_notificaciones)
          select distinct gc.cod_entidad,gc.ide_gestion_sector,gc.ide_gestion_nro,gc.cod_segm,gc.semf_ingreso_crm,gc.semf_renta_crm,gc.semf_riesgo_crm,gc.calle_dom_contc,gc.nro_dom_contc,gc.piso_dom_contc,gc.dpto_dom_contc,gc.cpost_dom_contc,gc.loc_dom_contc,gc.pcia_dom_contc,gc.pais_dom_contc,gc.ddn_tel_dom_contc,gc.nro_tel_dom_contc,gc.ddn_fax_contc,gc.nro_fax_contc,gc.hra_dom_contc,gc.ddn_celular_contc,gc.nro_celular_contc,gc.ddn_tel_laboral,gc.nro_tel_laboral,gc.hra_tel_laboral,gc.ddn_fax_laboral,gc.nro_fax_laboral,gc.aplic_cta,gc.cod_suc_cta,gc.nro_cta,gc.nro_firm_cta,gc.cod_mone_cta,gc.cod_prod_cta,gc.cod_subpro_cta,gc.nro_tarjeta,gc.link_resumen,gc.dire_mail_contc,gc.dire_mail2_contc,gc.cartera,gc.aplic_cta_gold,gc.cod_suc_cta_gold,gc.nro_cta_gold,gc.nro_firm_cta_gold,gc.cod_mone_cta_gold,gc.cod_prod_cta_gold,gc.cod_subpro_cta_gold,gc.cod_ramo_cta,gc.nro_certificado_cta,gc.rentabilidad_promedio,gc.color_semaforo,gc.color_semaf_riesgo,gc.indi_envios_mya,gc.dire_mail_opcional,gc.nro_paquete,gc.cod_paquete,gc.indi_plan_sueldo,gc.nro_convenio_paquete,gc.emp_celular_contc,gc.tpo_msj_asoc_rta,gc.monto_deuda_ref,gc.no_enviar_notificaciones
            from gec01.gestion_estados est, gec01.gc_gestiones gc
           where est.fec_est_gest between fec_desde and fec_hasta
             and est.cod_entidad = gc.cod_entidad
             and est.ide_gestion_sector = gc.ide_gestion_sector
             and est.ide_gestion_nro = gc.ide_gestion_nro;

        v_paso := 9;
        insert into ig_gestiones (cod_entidad,ide_gestion_sector,ide_gestion_nro,ide_circuito,indi_tipo_circ,indi_decision_comer,indi_replteo,indi_rta_inmed,indi_impre_cart,imp_autz_solicitado,cod_mone_solicitado,imp_autz_autorizado,cod_mone_autorizado,imp_autz_resolucion,cod_mone_resolucion,tpo_pers,cod_crm,comen_cliente,ide_gest_sector_relac,ide_gest_nro_relac,fec_gestion_alta,cod_bandeja_actual,fec_bandeja_actual,cod_sector_actual,inic_user_alta,indi_mail,indi_prioridad,fec_max_resol,cod_conforme,cod_user_actual,cod_grupo_empresa,cod_tipo_favorabilidad,fec_aviso_venc)
          select distinct ges.cod_entidad,ges.ide_gestion_sector,ges.ide_gestion_nro,ges.ide_circuito,ges.indi_tipo_circ,ges.indi_decision_comer,ges.indi_replteo,ges.indi_rta_inmed,ges.indi_impre_cart,ges.imp_autz_solicitado,ges.cod_mone_solicitado,ges.imp_autz_autorizado,ges.cod_mone_autorizado,ges.imp_autz_resolucion,ges.cod_mone_resolucion,ges.tpo_pers,ges.cod_crm,ges.comen_cliente,ges.ide_gest_sector_relac,ges.ide_gest_nro_relac,ges.fec_gestion_alta,ges.cod_bandeja_actual,ges.fec_bandeja_actual,ges.cod_sector_actual,ges.inic_user_alta,ges.indi_mail,ges.indi_prioridad,ges.fec_max_resol,ges.cod_conforme,ges.cod_user_actual,ges.cod_grupo_empresa,ges.cod_tipo_favorabilidad,ges.fec_aviso_venc
            from gec01.gestiones ges, gec01.gestion_estados est
           where est.fec_est_gest between fec_desde and fec_hasta
             and ges.cod_entidad = est.cod_entidad
             and ges.ide_gestion_sector = est.ide_gestion_sector
             and ges.ide_gestion_nro = est.ide_gestion_nro;

        v_paso := 10;
        --RegularizaciÃ³n para completar campo FACT_COD_SUC_CTA de la tabla FACT_GESTIONES para periodos anteriores
        for x in (select distinct gc.ide_gestion_nro, gc.cod_suc_cta, g.cod_crm, gc.nro_cta, gc.semf_ingreso_crm
                    from ig_gc_gestiones gc, ig_gestiones g
                   where gc.cod_entidad = g.cod_entidad
                     and gc.ide_gestion_sector = g.ide_gestion_sector
                     and gc.ide_gestion_nro = g.ide_gestion_nro) loop

          update fact_gestiones
             set fact_cod_suc_cta = x.cod_suc_cta,
                 fact_cod_crm = x.cod_crm,
                 fact_nro_cta = x.nro_cta,
                 fact_ingreso_epa = x.semf_ingreso_crm
           where fact_nro_gestion = x.ide_gestion_nro;

        end loop;

        v_fecha_fin := sysdate;
        pkg_gc_procesos.sp_procesos(2, 2, v_fecha_inicio, v_fecha_fin, id_proceso);

      commit;
      end if;

      if regularizacion = 9 then
        v_paso := 11;
        --Regularizacion para completar el campo FACT_FECHA_SIGUIENTE
        for x in (select fact_fec_bandeja_actual,
                         lead(fact_fec_bandeja_actual,1) over (partition by fact_sector_alta,fact_nro_gestion order by fact_orden_estado) fact_fecha_siguiente,
                         fact_cod_estados,
                         fact_orden_estado,
                         fact_nro_gestion
                    from fact_gestiones
                   where fact_nro_gestion in (select distinct fact_nro_gestion
                                                from fact_gestiones
                                               where fact_fecha_estado between to_char(fec_desde,'YYYYMMDD') and to_char(fec_hasta,'YYYYMMDD')
                                                 and fact_ultimo_estado = 1
                                                 and fact_marca_cerrado = 1)
                     and fact_cod_estados in (100,116,200,207,300,316,317,318,400,416,417,500,600)) loop

          v_dias_gestion := 0;

          if x.fact_fecha_siguiente is not null then
            v_dias_gestion := fn_dias_habiles(x.fact_fec_bandeja_actual, x.fact_fecha_siguiente,dias, x.fact_nro_gestion);
          else
            v_dias_gestion := fn_dias_habiles(x.fact_fec_bandeja_actual, x.fact_fec_bandeja_actual,dias, x.fact_nro_gestion);
          end if;

          update fact_gestiones
             set fact_ind_dias_x_estado = v_dias_gestion,
                 fact_fecha_siguiente = x.fact_fecha_siguiente
           where fact_nro_gestion = x.fact_nro_gestion
             and fact_cod_estados = x.fact_cod_estados
             and fact_orden_estado = x.fact_orden_estado;

        end loop;

        v_fecha_fin := sysdate;
        pkg_gc_procesos.sp_procesos(2, 2, v_fecha_inicio, v_fecha_fin, id_proceso);

      commit;
      end if;

      if regularizacion = 10 then

        v_fecha_inicio := sysdate;
        pkg_gc_procesos.sp_procesos(1, 1, v_fecha_inicio, null, id_proceso);

        v_paso := 12;
        --Regularizacion para los casos en que la gestion no se le completÃ³ el cod_favorabilidad en todos sus estados
        for x in (select distinct fact_nro_gestion, count(distinct fact_cod_favorabilidad)
                    from gec02.fact_gestiones
                   where fact_nro_gestion in (select distinct fact_nro_gestion
                                                from gec02.fact_gestiones
                                               where fact_fecha_estado between to_char(fec_desde,'YYYYMMDD')
                                                                           and to_char(fec_hasta,'YYYYMMDD')
                                                 and fact_marca_cerrado = 1
                                                 and fact_ultimo_estado = 1)
                  having count(distinct fact_cod_favorabilidad) > 1
                   group by fact_nro_gestion
                   order by 2 desc) loop

          select distinct fact_cod_favorabilidad
            into v_cod_favorabilidad
            from fact_gestiones
           where fact_nro_gestion = x.fact_nro_gestion
             and fact_ultimo_estado = 1
             and fact_marca_cerrado = 1;

          update fact_gestiones
             set fact_cod_favorabilidad = v_cod_favorabilidad
           where fact_nro_gestion = x.fact_nro_gestion;

        end loop;

        v_fecha_fin := sysdate;
        pkg_gc_procesos.sp_procesos(2, 2, v_fecha_inicio, v_fecha_fin, id_proceso);

      commit;
      end if;

      if regularizacion = 11 then

        v_fecha_inicio := sysdate;
        pkg_gc_procesos.sp_procesos(1, 1, v_fecha_inicio, null, id_proceso);

        v_paso := 13;
        --Regularizacion para las gestiones ANULADAS que no tienen ninguna favorabilidad
        for x in (select distinct fact_nro_gestion
                    from gec02.fact_gestiones, gec02.dim_estados
                   where fact_fecha_estado between to_char(fec_desde,'YYYYMMDD') and to_char(fec_hasta,'YYYYMMDD')
                     and fact_marca_cerrado = 1
                     and fact_ultimo_estado = 1
                     and fact_cod_estados = est_cod
                     and upper(est_bandeja) = 'ANULADO'
                     and fact_cod_favorabilidad = 0
                   union all
                  select distinct fact_nro_gestion
                    from gec02.fact_gestiones
                   where fact_fecha_estado between to_char(fec_desde,'YYYYMMDD') and to_char(fec_hasta,'YYYYMMDD')
                     and fact_marca_cerrado = 1
                     and fact_ultimo_estado = 1
                     and fact_cerradas_dev = 1
                     and fact_cod_favorabilidad = 0
                   union all
                  select distinct fact_nro_gestion
                    from gec02.fact_gestiones
                   where fact_nro_gestion in (select distinct fact_nro_gestion
                                                from gec02.fact_gestiones
                                               where fact_fecha_estado between to_char(fec_desde,'YYYYMMDD') and to_char(fec_hasta,'YYYYMMDD')
                                                 and fact_marca_cerrado = 1
                                                 and fact_ultimo_estado = 1
                                                 and fact_cerradas_dev <> 1
                                                 and fact_cod_favorabilidad = 0)
                     and fact_cod_estados = 203) loop

          update fact_gestiones
             set fact_cod_favorabilidad = 2
           where fact_nro_gestion = x.fact_nro_gestion;

        end loop;

        v_fecha_fin := sysdate;
        pkg_gc_procesos.sp_procesos(2, 2, v_fecha_inicio, v_fecha_fin, id_proceso);

      commit;
      end if;

      if regularizacion = 12 then

        v_fecha_inicio := sysdate;
        pkg_gc_procesos.sp_procesos(1, 1, v_fecha_inicio, null, id_proceso);

        v_paso := 14;
        --Regularizacion para casos con fact_marca_cerrado = 1 y fact_ultimo_estado <> 1
        delete from dim_gestiones_regularizar;

        insert into dim_gestiones_regularizar
             select distinct fact_nro_gestion, sysdate, max(fact_orden_estado), null
               from gec02.fact_gestiones
              where fact_fecha_estado between to_char(fec_desde,'YYYYMMDD') and to_char(fec_hasta,'YYYYMMDD')
                and fact_marca_cerrado = 1
                and fact_ultimo_estado <> 1
              group by fact_nro_gestion;

        update fact_gestiones
           set fact_ultimo_estado = 0
         where fact_nro_gestion in (select reg_nro_gestion from dim_gestiones_regularizar);

        for x in (select reg_nro_gestion, reg_numero
                    from dim_gestiones_regularizar) loop

          update fact_gestiones
             set fact_ultimo_estado = 1
           where fact_nro_gestion = x.reg_nro_gestion
             and fact_orden_estado = x.reg_numero;

        end loop;

        pkg_gc_indicadores.sp_dias_de_gestion(id_proceso,fec_desde,fec_hasta,1,0);

        v_fecha_fin := sysdate;
        pkg_gc_procesos.sp_procesos(2, 2, v_fecha_inicio, v_fecha_fin, id_proceso);

      commit;
      end if;

      if regularizacion = 13 then

        v_fecha_inicio := sysdate;
        pkg_gc_procesos.sp_procesos(1, 1, v_fecha_inicio, null, id_proceso);

        v_paso := 15;
        --Regularizacion para casos cerrados TRAC y PROM que no tienen marca cerrado = 1 y no tienen gestiones hijas abiertas
        for x in (select fact_nro_gestion
                    from fact_gestiones, dim_estados
                   where fact_cod_estados = est_cod
                     and fact_fecha_estado between to_char(fec_desde,'YYYYMMDD') and to_char(fec_hasta,'YYYYMMDD')
                     and upper(est_bandeja) = 'CERRADO'
                     and fact_ultimo_estado = 1
                     and fact_marca_cerrado <> 1) loop

          select count(*)
            into v_cant_abiertas
            from dim_gestiones_trac
           where trac_gestion_padre = x.fact_nro_gestion
             and upper(trac_bandeja_actual) <> 'CERRADO';

          if v_cant_abiertas = 0 then
            update fact_gestiones
               set fact_marca_cerrado = 1
             where fact_nro_gestion = x.fact_nro_gestion
               and fact_ultimo_estado = 1;
          end if;
        end loop;

        v_fecha_fin := sysdate;
        pkg_gc_procesos.sp_procesos(2, 2, v_fecha_inicio, v_fecha_fin, id_proceso);

      commit;
      end if;

      if regularizacion = 14 then

        v_fecha_inicio := sysdate;
        pkg_gc_procesos.sp_procesos(1, 1, v_fecha_inicio, null, id_proceso);

        v_paso := 16;
        --Regularizacion para gestiones hijas insertadas en fact_gestiones
        delete from fact_gestiones
              where fact_nro_gestion in (select distinct a.fact_nro_gestion
                                           from fact_gestiones a, dim_gestiones_trac
                                          where a.fact_fecha_estado between to_char(fec_desde,'YYYYMMDD') and to_char(fec_hasta,'YYYYMMDD')
                                            and a.fact_marca_cerrado = 1
                                            and a.fact_marca_visa <> 1
                                            and a.fact_ultimo_estado = 1
                                            and a.fact_sector_alta in ('TRAC','PROM')
                                            and a.fact_cod_favorabilidad = 0
                                            and not exists (select *
                                                              from fact_gestiones b
                                                             where b.fact_nro_gestion = a.fact_nro_gestion
                                                               and b.fact_cod_estados = 100)
                                            and a.fact_nro_gestion = trac_gestion_hijo
                                            and a.fact_nro_gestion not in (select trac_gestion_padre
                                                                             from dim_gestiones_trac
                                                                            where trac_gestion_padre = a.fact_nro_gestion));

        v_fecha_fin := sysdate;
        pkg_gc_procesos.sp_procesos(2, 2, v_fecha_inicio, v_fecha_fin, id_proceso);

      commit;
      end if;
    else
      pkg_gc_procesos.sp_procesos(3, 0, v_fecha_inicio, v_fecha_fin, id_proceso);
      pkg_gc_procesos.sp_inconsistencias(id_proceso, sysdate, v_paso, 'Existen procesos con error que impiden la ejecuciÃ³n.');
    end if;
  exception
     when others then
      v_fecha_fin := sysdate;
      pkg_gc_procesos.sp_procesos(2, 6, v_fecha_inicio, v_fecha_fin, id_proceso);
      v_err_sql := substr (sqlerrm (sqlcode), 1, 250);
      pkg_gc_procesos.sp_inconsistencias(id_proceso, sysdate, v_paso, v_err_sql);
  end sp_regularizacion;

/******************************************************************************/
------------------------------- SP_FAV_FALTA_DOC -------------------------------
/******************************************************************************/
  procedure sp_fav_falta_doc(id_proceso in number, fec_desde in date, fec_hasta in date) as
  begin
    --Verifica si hay procesos con error
    pkg_gc_procesos.sp_busca_procesos_error(v_cantidad_error);

    if v_cantidad_error = 0 then
      v_fecha_inicio := sysdate;
      pkg_gc_procesos.sp_procesos(1, 1, v_fecha_inicio, null, id_proceso);
      v_paso := 1;
      --Se carga cursor con los datos a los cuales se les debe actualizar el codigo de favorabilidad
      for x in (select distinct fact_nro_gestion
                  from fact_gestiones
                 where fact_fecha_estado between to_char(fec_desde,'YYYYMMDD') and to_char(fec_hasta,'YYYYMMDD')
                   and fact_ultimo_estado = 1
                   and fact_cod_estados in (122,123,325,110)
                   and fact_marca_cerrado = 1
                   and fact_cod_favorabilidad = 0
                 order by 1) loop

        l_fav_falta_doc.extend;

        l_fav_falta_doc(l_fav_falta_doc.last).fact_nro_gestion := x.fact_nro_gestion;
      end loop;

      v_paso := 2;
      --Se modifica el codigo de favorabilidad para las gestiones cerradas por falta de documentacion
      forall x in l_fav_falta_doc.first .. l_fav_falta_doc.last
        update fact_gestiones
           set fact_cod_favorabilidad = 2
         where fact_nro_gestion  = l_fav_falta_doc(x).fact_nro_gestion;

     commit;

      v_paso := 3;
      v_fecha_fin := sysdate;
      pkg_gc_procesos.sp_procesos(2, 2, v_fecha_inicio, v_fecha_fin, id_proceso);
    else
      pkg_gc_procesos.sp_procesos(3, 0, v_fecha_inicio, v_fecha_fin, id_proceso);
      pkg_gc_procesos.sp_inconsistencias(id_proceso, sysdate, v_paso, 'Existen procesos con error que impiden la ejecuciÃ³n.');
    end if;
  exception
     when others then
      v_fecha_fin := sysdate;
      pkg_gc_procesos.sp_procesos(2, 6, v_fecha_inicio, v_fecha_fin, id_proceso);
      v_err_sql := substr (sqlerrm (sqlcode), 1, 250);
      pkg_gc_procesos.sp_inconsistencias(id_proceso, sysdate, v_paso, v_err_sql);
  end sp_fav_falta_doc;

/******************************************************************************/
--------------------------- SP_REGULARIZAR_CIRCUITOS ---------------------------
/******************************************************************************/
  procedure sp_regularizar_circuitos(id_proceso in number, fec_desde in date, fec_hasta in date) as
  begin
    --Verifica si hay procesos con error
    pkg_gc_procesos.sp_busca_procesos_error(v_cantidad_error);

    if v_cantidad_error = 0 then
      v_fecha_inicio := sysdate;
      pkg_gc_procesos.sp_procesos(1, 1, v_fecha_inicio, null, id_proceso);

      v_paso := 1;
      --Se carga cursor con los datos a los cuales se les debe actualizar el codigo de favorabilidad
      for x in (select fact_nro_gestion, count(distinct fact_cod_circuito)
                  from fact_gestiones
                 where fact_nro_gestion in (select distinct fact_nro_gestion
                                                       from fact_gestiones
                                                      where fact_fecha_estado between to_char(fec_desde,'YYYYMMDD')
                                                                                  and to_char(fec_hasta,'YYYYMMDD'))
                having count(distinct fact_cod_circuito) > 1
                 group by fact_nro_gestion
                 order by 2 desc) loop

        begin
          select distinct(fact_cod_circuito), substr(fact_fecha_estado,1,6)
            into v_cir_alta, v_mes_alta
            from fact_gestiones
           where fact_nro_gestion = x.fact_nro_gestion
             and fact_cod_estados = 100;

          select distinct(fact_cod_circuito), substr(fact_fecha_estado,1,6)
            into v_cir_no_alta, v_mes_no_alta
            from fact_gestiones
           where fact_nro_gestion = x.fact_nro_gestion
             and fact_cod_estados <> 100
             and fact_ultimo_estado = 1;

          select distinct fact_cod_favorabilidad, fact_cod_segmento
            into v_cod_favorabilidad, v_cod_segmentto
            from fact_gestiones
           where fact_nro_gestion = x.fact_nro_gestion
             and fact_ultimo_estado = 1;

          if v_mes_alta = v_mes_no_alta then
            update fact_gestiones
               set fact_cod_circuito = v_cir_no_alta,
                   fact_cod_favorabilidad = v_cod_favorabilidad,
                   fact_cod_segmento = v_cod_segmentto
             where fact_nro_gestion = x.fact_nro_gestion;
          else
            update fact_gestiones
               set fact_cod_favorabilidad = v_cod_favorabilidad
             where fact_nro_gestion = x.fact_nro_gestion;
          end if;

        exception
          when too_many_rows then
            pkg_gc_procesos.sp_inconsistencias(id_proceso, sysdate, v_paso, 'Mas de una fila para la gestiÃ³n: ' || x.fact_nro_gestion);
        end;
      end loop;
     commit;

      v_paso := 2;
      v_fecha_fin := sysdate;
      pkg_gc_procesos.sp_procesos(2, 2, v_fecha_inicio, v_fecha_fin, id_proceso);
    else
      pkg_gc_procesos.sp_procesos(3, 0, v_fecha_inicio, v_fecha_fin, id_proceso);
      pkg_gc_procesos.sp_inconsistencias(id_proceso, sysdate, v_paso, 'Existen procesos con error que impiden la ejecuciÃ³n.');
    end if;
  exception
     when others then
      v_fecha_fin := sysdate;
      pkg_gc_procesos.sp_procesos(2, 6, v_fecha_inicio, v_fecha_fin, id_proceso);
      v_err_sql := substr (sqlerrm (sqlcode), 1, 250);
      pkg_gc_procesos.sp_inconsistencias(id_proceso, sysdate, v_paso, v_err_sql);
  end sp_regularizar_circuitos;

/******************************************************************************/
--------------------------- SP_CONTROL_CANTIDADES ------------------------------
/******************************************************************************/
  procedure sp_control_cantidades(id_proceso in number, fec_desde in date, fec_hasta in date) as
  begin
    --Verifica si hay procesos con error
    pkg_gc_procesos.sp_busca_procesos_error(v_cantidad_error);

    if v_cantidad_error = 0 then
      v_fecha_inicio := sysdate;
      pkg_gc_procesos.sp_procesos(1, 1, v_fecha_inicio, null, id_proceso);

      v_paso := 1;
      --Se cuentan las gestiones ingresadas excluyendo las gestiones ingresadas por TRAC y PROM y las ingresadas desde VISA
      for x in (select to_number(to_char(ges.fec_gestion_alta,'YYYY')) anio,
                       to_number(to_char(ges.fec_gestion_alta,'MM')) mes_num,
                       trim(to_char(ges.fec_gestion_alta,'Month','nls_date_language=spanish')) mes,
                       to_number(to_char(ges.fec_gestion_alta,'DD')) dia_num,
                       to_char(ges.fec_gestion_alta,'Day','nls_date_language=spanish') dia,
                       'GEC01' ambiente,
                       count(distinct ges.ide_gestion_nro) cantidad
                  from gec01.gestiones ges, gec01.circuito cir
                 where ges.fec_gestion_alta between fec_desde and fec_hasta
                   and ges.ide_gestion_sector not in ('TRAC','PROM')
                   and ges.ide_gestion_nro not in (select distinct ide_gestion_nro
                                                     from gec01.informacion_adjunta adj
                                                    where adj.fec_inf_adjunta between fec_desde and fec_hasta
                                                      and adj.cod_info_doc_reque = 2905
                                                      and adj.dato_info_doc_reque = 'V')
                   and ges.cod_entidad = cir.cod_entidad
                   and ges.ide_circuito = cir.ide_circuito
                   and cir.tpo_gestion in (1,2,5)
                 group by to_number(to_char(ges.fec_gestion_alta,'YYYY')),
                          to_number(to_char(ges.fec_gestion_alta,'MM')),
                          to_char(ges.fec_gestion_alta,'Month','nls_date_language=spanish'),
                          to_number(to_char(ges.fec_gestion_alta,'DD')),
                          to_char(ges.fec_gestion_alta,'Day','nls_date_language=spanish')
                 union all
                select fec_anio anio,
                       fec_mes_num mes_num,
                       fec_mes mes,
                       fec_dia dia_num,
                       fec_dia_nombre dia,
                       'GEC02' ambiente,
                       count(fact_nro_gestion) cantidad
                  from fact_gestiones, dim_circuitos, dim_fecha
                 where fact_fecha_estado between to_char(fec_desde,'YYYYMMDD') and to_char(fec_hasta,'YYYYMMDD')
                   and fact_marca_visa <> 1
                   and fact_cod_circuito = cir_cod
                   and cir_cod_tpo_ges in (1,2,5)
                   and fact_fecha_estado = fec_cod
                   and fact_cod_estados = 100
                   and fact_sector_alta not in ('TRAC','PROM')
                 group by fec_anio,
                          fec_mes_num,
                          fec_mes,
                          fec_dia,
                          fec_dia_nombre) loop

         if x.ambiente = 'GEC01' then
            update dim_control_cantidades
              set can_cantidad_gec01 = x.cantidad
            where can_anio = x.anio
              and can_mes_num = x.mes_num
              and can_dia_num = x.dia_num;

            if sql%notfound then
              insert into dim_control_cantidades
                 values (x.anio,
                         x.mes_num,
                         x.mes,
                         x.dia_num,
                         x.dia,
                         x.cantidad,
                         0);
            end if;
         else
            update dim_control_cantidades
              set can_cantidad_gec02 = x.cantidad
            where can_anio = x.anio
              and can_mes_num = x.mes_num
              and can_dia_num = x.dia_num;

            if sql%notfound then
              insert into dim_control_cantidades
                 values (x.anio,
                         x.mes_num,
                         x.mes,
                         x.dia_num,
                         x.dia,
                         0,
                         x.cantidad);
            end if;
         end if;
      end loop;

      commit;

      v_paso := 2;
      v_fecha_fin := sysdate;
      pkg_gc_procesos.sp_procesos(2, 2, v_fecha_inicio, v_fecha_fin, id_proceso);
    else
      pkg_gc_procesos.sp_procesos(3, 0, v_fecha_inicio, v_fecha_fin, id_proceso);
      pkg_gc_procesos.sp_inconsistencias(id_proceso, sysdate, v_paso, 'Existen procesos con error que impiden la ejecuciÃ³n.');
    end if;
  exception
     when others then
      v_fecha_fin := sysdate;
      pkg_gc_procesos.sp_procesos(2, 6, v_fecha_inicio, v_fecha_fin, id_proceso);
      v_err_sql := substr (sqlerrm (sqlcode), 1, 250);
      pkg_gc_procesos.sp_inconsistencias(id_proceso, sysdate, v_paso, v_err_sql);
  end sp_control_cantidades;

/******************************************************************************/
-------------------------------- FN_DIAS_HABILES -------------------------------
/******************************************************************************/
  function fn_dias_habiles(fec_desde in date, fec_hasta in date, detalle in number, gestion in number)
    return number is
      v_cant_dias               number;
      v_cant_feriados           number;
      v_desde_feriado           number;
      v_hasta_feriado           number;
      v_fec_desde               date;
      v_fec_hasta               date;
      v_cant_feriados_nuevo     number;
      v_cant_habiles_feriados   number;
      v_fec_min                 date;
      v_fec_max                 date;
  begin
      v_paso := 1;
    --verifico cantidad de feriados entre desde y hasta.
    select count(*)
      into v_cant_feriados
      from dim_fecha
     where trunc(fec_fecha) between trunc(fec_desde) and trunc(fec_hasta)
       and fec_feriado = 'S';

    --Loquea un detalle de cada paso para las gestiones a regularizar
    if detalle = 1 then
      pkg_gc_procesos.sp_inconsistencias(1, sysdate, v_paso, 'v_cant_feriados: ' || v_cant_feriados || ' gestiÃ³n: ' || gestion);
    end if;

    v_paso := 2;
    --verifico si desde es feriado.
    select count(*)
      into v_desde_feriado
      from dim_fecha
     where trunc(fec_fecha) = trunc(fec_desde)
       and fec_feriado = 'S';

    --Loquea un detalle de cada paso para las gestiones a regularizar
    if detalle = 1 then
      pkg_gc_procesos.sp_inconsistencias(1, sysdate, v_paso, 'v_desde_feriado: ' || v_desde_feriado || ' gestiÃ³n: ' || gestion);
    end if;

    v_paso := 3;
    --verifico si hasta es feriado.
    select count(*)
      into v_hasta_feriado
      from dim_fecha
     where trunc(fec_fecha) = trunc(fec_hasta)
       and fec_feriado = 'S';

    --Loquea un detalle de cada paso para las gestiones a regularizar
    if detalle = 1 then
      pkg_gc_procesos.sp_inconsistencias(1, sysdate, v_paso, 'v_hasta_feriado: ' || v_hasta_feriado || ' gestiÃ³n: ' || gestion);
    end if;

    v_paso := 4;
    --Si las fechas son iguales devulve el valor absoluto entre las mismas o si las fechas son feriados
    if trunc(fec_desde) = trunc(fec_hasta) then
      v_cant_dias := fec_hasta - fec_desde;

      --Loquea un detalle de cada paso para las gestiones a regularizar
      if detalle = 1 then
        pkg_gc_procesos.sp_inconsistencias(1, sysdate, v_paso, 'v_cant_dias: ' || v_cant_dias || ' gestiÃ³n: ' || gestion);
      end if;

    end if;

    v_paso := 5;
    --Si desde es feriado y hasta no lo es, se obtiene el dia habil mas proximo para adelante
    if v_desde_feriado > 0 and v_hasta_feriado = 0 then
      select fec_fecha
        into v_fec_desde
        from dim_fecha
       where trunc(fec_fecha) between trunc(fec_desde) and trunc(fec_hasta)
         and fec_feriado = 'N'
         and rownum = 1
       order by fec_cod asc;


      v_paso := 6;
      select count(*)
        into v_cant_feriados_nuevo
        from dim_fecha
       where trunc(fec_fecha) between trunc(v_fec_desde) and trunc(fec_hasta)
         and fec_feriado = 'S';

      v_cant_dias := fec_hasta - v_fec_desde - v_cant_feriados_nuevo;

      --Loquea un detalle de cada paso para las gestiones a regularizar
      if detalle = 1 then
        pkg_gc_procesos.sp_inconsistencias(1, sysdate, v_paso, 'v_cant_dias: ' || v_cant_dias || ' gestiÃ³n: ' || gestion);
      end if;
    end if;

    v_paso := 7;
    --Si desde no es feriado y hasta si lo es, se obtiene el dia habil mas proximo para atras
    if v_desde_feriado = 0 and v_hasta_feriado > 0 then
      select fec_fecha + 0.99999
        into v_fec_hasta
        from dim_fecha
       where trunc(fec_fecha) between trunc(fec_desde) and trunc(fec_hasta)
         and fec_feriado = 'N'
         and rownum = 1
       order by fec_cod desc;

      v_paso := 8;
      select count(*)
        into v_cant_feriados_nuevo
        from dim_fecha
       where trunc(fec_fecha) between trunc(fec_desde) and trunc(v_fec_hasta)
         and fec_feriado = 'S';

      v_cant_dias := v_fec_hasta - fec_desde - v_cant_feriados_nuevo;

      --Loquea un detalle de cada paso para las gestiones a regularizar
      if detalle = 1 then
        pkg_gc_procesos.sp_inconsistencias(1, sysdate, v_paso, 'v_cant_dias: ' || v_cant_dias || ' gestiÃ³n: ' || gestion);
      end if;
    end if;

    v_paso := 9;
    --Si desde y hasta son feriados
    if v_desde_feriado > 0 and v_hasta_feriado > 0 then
      --Busca los dias habiles entre dos fechas feriados
      select count(*)
        into v_cant_habiles_feriados
        from dim_fecha
       where trunc(fec_fecha) between trunc(fec_desde) and trunc(fec_hasta)
         and fec_feriado = 'N';

      --Loquea un detalle de cada paso para las gestiones a regularizar
      if detalle = 1 then
        pkg_gc_procesos.sp_inconsistencias(1, sysdate, v_paso, 'v_cant_habiles_feriados: ' || v_cant_habiles_feriados || ' gestiÃ³n: ' || gestion);
      end if;

      v_paso := 10;
      if v_cant_habiles_feriados > 0 then
        begin
          select min(fec_fecha), max(fec_fecha)
            into v_fec_min, v_fec_max
            from dim_fecha
           where trunc(fec_fecha) between trunc(fec_desde) and trunc(fec_hasta)
             and fec_feriado = 'N';

          v_cant_dias := (v_fec_max + 1) - v_fec_min;

          --Loquea un detalle de cada paso para las gestiones a regularizar
          if detalle = 1 then
            pkg_gc_procesos.sp_inconsistencias(1, sysdate, v_paso, 'v_cant_dias: ' || v_cant_dias || ' gestiÃ³n: ' || gestion);
          end if;

        exception
          when no_data_found then
            v_paso := 11;
            v_cant_dias := 0;
        end;
      else
        v_paso := 12;
        v_cant_dias := 0;
      end if;
    end if;

    v_paso := 13;
    --Si desde y hasta no son feriados
    if v_desde_feriado = 0 and v_hasta_feriado = 0 then
      v_cant_dias := fec_hasta - fec_desde - v_cant_feriados;

      --Loquea un detalle de cada paso para las gestiones a regularizar
      if detalle = 1 then
        pkg_gc_procesos.sp_inconsistencias(1, sysdate, v_paso, 'v_cant_dias: ' || v_cant_dias || ' gestiÃ³n: ' || gestion);
      end if;
    end if;

    v_paso := 14;
    return round(v_cant_dias,2);

  exception
    when others then
      v_err_sql := substr (sqlerrm (sqlcode), 1, 250);
      pkg_gc_procesos.sp_inconsistencias(0, sysdate, v_paso, 'Error calculando dÃas hÃ¡biles (fn_dias_habiles, gestiÃ³n: ' || gestion || '): ' || v_err_sql);
  end fn_dias_habiles;

end pkg_gc_indicadores;
/
