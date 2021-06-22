/* --CREATE OR REPLACE package body GEC02.pkg_gc_dimensiones as

  v_fecha              date;
  v_ultimo_dia_anio    number := 0;
  v_cantidad           number := 0;
  v_fecha_inicio       date;
  v_fecha_fin          date;
  v_err_sql            varchar2(2000);
  v_paso               number := 0;
  v_cantidad_error     number := 0;
  v_registros_cargados number := 0;
  v_max                number := 32000;
  v_line_max           number := 32767;

  v_path_utl           varchar2(15) := 'RIO56_INTERFACE';
  out_file             UTL_FILE.FILE_TYPE;
  out_file_1           UTL_FILE.FILE_TYPE;
  v_separador          varchar2(1) := '|';
*/

/*SP_FECHA 

  procedure sp_fecha (id_proceso in number, anio in number, fec_desde in date, fec_hasta in date) as
  begin
    v_fecha_inicio := sysdate;
    pkg_gc_procesos.sp_procesos(1, 1, v_fecha_inicio, null, id_proceso);

    v_paso := 1;
    --Verifica si se encuentra cargado el año en curso en la dimension fecha
    select count(*)
      into v_cantidad
      from dim_fecha
     where fec_anio = anio;

    v_paso := 2;
    if not(v_cantidad = 365 or v_cantidad = 366) then
      begin
        v_paso := 3;
        --Se busca a que numero representa el ultimo dia del año
        select to_char(to_date('31/12/' || anio),'DDD') - 1
          into v_ultimo_dia_anio
          from dual;

        v_paso := 4;
        for x in 0 .. v_ultimo_dia_anio loop
          --Se calcula la fecha en base al primer dia del año ingresado
          select to_date('01/01/' || anio) + x
            into v_fecha
            from dual;

          insert into dim_fecha
          select to_char(v_fecha,'YYYY') || to_char(v_fecha,'MM') || to_char(v_fecha,'DD'),
                 to_char(v_fecha,'YYYY'),
                 decode(to_char(v_fecha,'MM'),'01','Cuatrimestre 1',
                                              '02','Cuatrimestre 1',
                                              '03','Cuatrimestre 1',
                                              '04','Cuatrimestre 1',
                                              '05','Cuatrimestre 2',
                                              '06','Cuatrimestre 2',
                                              '07','Cuatrimestre 2',
                                              '08','Cuatrimestre 2',
                                              '09','Cuatrimestre 3',
                                              '10','Cuatrimestre 3',
                                              '11','Cuatrimestre 3',
                                              'Cuatrimestre 3'),
                decode(to_char(v_fecha,'Q'),1,'Trimestre 1',
                                            2,'Trimestre 2',
                                            3,'Trimestre 3',
                                            'Trimestre 4'),
                trim(to_char(v_fecha,'Month')),
                to_char(v_fecha,'MM'),
                to_char(v_fecha,'DD'),
                trim(to_char(v_fecha,'Day')),
                decode(trim(to_char(v_fecha,'Day')),'Sábado','S','Domingo','S','N'),
                v_fecha,
                trunc(v_fecha,'MM'),
                last_day(v_fecha)
           from dual;
        end loop;

        v_paso := 5;
        --Se carga la tabla IG_FERIADOS para marcar las fechas en la dimension DIM_FECHA
        insert into ig_feriados (cod_entidad,fecha,desc_fecha,indi_fer_nac,indi_fer_pcial,indi_fer_suc)
             select cod_entidad,fecha,desc_fecha,indi_fer_nac,indi_fer_pcial,indi_fer_suc
               from gec01.feriados
              where to_char(fecha,'YYYY') = anio;

        v_paso := 6;
        for y in (select fecha from ig_feriados) loop
          update dim_fecha
             set fec_feriado = 'S'
           where fec_fecha = y.fecha;
        end loop;
        commit;
      end;
    end if;

    v_paso := 7;
    v_fecha_fin := sysdate;
    pkg_gc_procesos.sp_procesos(2, 2, v_fecha_inicio, v_fecha_fin, id_proceso);
  exception
    when others then
      v_fecha_fin := sysdate;
      pkg_gc_procesos.sp_procesos(3, 6, v_fecha_inicio, v_fecha_fin, id_proceso);
      v_err_sql := substr (sqlerrm (sqlcode), 1, 250);
      pkg_gc_procesos.sp_inconsistencias(id_proceso, sysdate, v_paso, v_err_sql);
  end sp_fecha;
*/

/*SP_PRIORIDAD 
  procedure sp_prioridad (id_proceso in number, fec_desde in date, fec_hasta in date) as
  begin
  
    v_fecha_inicio := sysdate;
    pkg_gc_procesos.sp_procesos(1, 1, v_fecha_inicio, null, id_proceso);

    v_paso := 1;
    --Se replican los datos en la tabla  IG_PRIORIDAD desde las tablas de origen en el esquema GEC01
    insert into ig_prioridad (cod_entidad,cod_prioridad,desc_prior,est_prior,user_alt_prior,fec_alt_prior,user_modf_prior,fec_modf_prior,sector_owner)
         select cod_entidad,cod_prioridad,desc_prior,est_prior,user_alt_prior,fec_alt_prior,user_modf_prior,fec_modf_prior,sector_owner
           from gec01.prioridad
          where fec_modf_prior between fec_desde and fec_hasta;

    v_paso := 2;
    --Se almacena la informacion nueva en la tabla de interfaz INT_PRIORIDAD
    insert into int_prioridad
         select cod_prioridad, desc_prior, est_prior from ig_prioridad;

    v_paso := 3;
    out_file := null;
    out_file := utl_file.fopen (v_path_utl, 'DIM_PRIORIDAD.txt', 'w',v_line_max);

    for x in (select * from int_prioridad) loop
      v_paso := 4;
      --Modifica la prioridad en caso de que exista
      update dim_prioridad
         set pri_desc = x.pri_desc,
             pri_estado = x.pri_estado
       where pri_cod = x.pri_cod;

      if sql%notfound then
        v_paso := 5;
        --Inserta la prioridad en caso de que no exista
        insert into dim_prioridad
             values (x.pri_cod, x.pri_desc, x.pri_estado);
      end if;

      v_paso := 6;
      utl_file.put_line (out_file, x.pri_cod || v_separador || x.pri_desc || v_separador || x.pri_estado);
    end loop;

    v_paso := 7;
    utl_file.fclose(out_file);
    commit;

    v_paso := 8;
    v_fecha_fin := sysdate;
    pkg_gc_procesos.sp_procesos(2, 2, v_fecha_inicio, v_fecha_fin, id_proceso);
  exception
    when others then
      v_fecha_fin := sysdate;
      pkg_gc_procesos.sp_procesos(2, 6, v_fecha_inicio, v_fecha_fin, id_proceso);
      v_err_sql := substr (sqlerrm (sqlcode), 1, 250);
      pkg_gc_procesos.sp_inconsistencias(id_proceso, sysdate, v_paso, v_err_sql);
  end sp_prioridad;
*/

/*SP_PAQUETES 
  procedure sp_paquetes (id_proceso in number, fec_desde in date, fec_hasta in date) as
  begin
    v_fecha_inicio := sysdate;
    pkg_gc_procesos.sp_procesos(1, 1, v_fecha_inicio, null, id_proceso);

    v_paso := 1;
    --Se replican los datos en la tabla  IG_PAQUETES y IG_TIPO_PAQUETES
      insert into ig_paquetes (cod_paquete,tpo_paquete,cod_subprod_cont,desc_paquete,est_paquete,ind_destino_downgrade,user_alt_paquete,fec_alt_paquete,user_modf_paquete,fec_modf_paquete)
           select cod_paquete,tpo_paquete,cod_subprod_cont,desc_paquete,est_paquete,ind_destino_downgrade,user_alt_paquete,fec_alt_paquete,user_modf_paquete,fec_modf_paquete
             from gec01.gc_paquetes
            where fec_modf_paquete between fec_desde and fec_hasta;

      insert into ig_tipo_paquetes (tpo_paquete,desc_tpo_paquete,est_tpo_paquete)
           select tpo_paquete,desc_tpo_paquete,est_tpo_paquete
             from gec01.gc_tipo_paquetes;

    v_paso := 2;
    --Se almacena la informacion definitiva en la tabla de interfaz INT_PAQUETES
    insert into int_paquetes
         select paq.cod_paquete,
                paq.desc_paquete,
                paq.tpo_paquete,
                decode(tipo.desc_tpo_paquete,null,'NO DEFINIDO',tipo.desc_tpo_paquete),
                paq.est_paquete
           from ig_paquetes paq,
                ig_tipo_paquetes tipo
          where paq.tpo_paquete = tipo.tpo_paquete(+);

    v_paso := 3;
    out_file := null;
    out_file := utl_file.fopen (v_path_utl, 'DIM_PAQUETES.txt', 'w', v_line_max);

    for x in (select * from int_paquetes) loop
      v_paso := 4;
      --Modifica el paquete en caso de que exista
      update dim_paquetes
         set paq_desc = x.paq_desc,
             paq_tipo = x.paq_tipo,
             paq_desc_tipo = x.paq_desc_tipo,
             paq_estado = x.paq_estado
       where paq_cod = x.paq_cod;

      if sql%notfound then
        v_paso := 5;
        --Inserta el paquete en caso de que no exista
        insert into dim_paquetes
          values (x.paq_cod, x.paq_desc, x.paq_tipo, x.paq_desc_tipo, x.paq_estado);
      end if;

      v_paso := 6;
      utl_file.put_line (out_file, x.paq_cod || v_separador || x.paq_desc || v_separador || x.paq_tipo || v_separador || x.paq_desc_tipo || v_separador || x.paq_estado);

    end loop;

    v_paso := 7;
    utl_file.fclose(out_file);

    commit;

    v_paso := 8;
    v_fecha_fin := sysdate;
    pkg_gc_procesos.sp_procesos(2, 2, v_fecha_inicio, v_fecha_fin, id_proceso);
  exception
    when others then
      v_fecha_fin := sysdate;
      pkg_gc_procesos.sp_procesos(2, 6, v_fecha_inicio, v_fecha_fin, id_proceso);
      v_err_sql := substr (sqlerrm (sqlcode), 1, 250);
      pkg_gc_procesos.sp_inconsistencias(id_proceso, sysdate, v_paso, v_err_sql);
  end sp_paquetes;
*/

/*SP_SEGMENTOS 
  procedure sp_segmentos  (id_proceso in number, fec_desde in date, fec_hasta in date) as
  begin
    v_fecha_inicio := sysdate;
    pkg_gc_procesos.sp_procesos(1, 1, v_fecha_inicio, null, id_proceso);

    v_paso := 1;
    --Se replican los datos en la tabla temporal IG_SEGMENTOS y IG_GRUPO_SEGMENTOS
      insert into ig_segmentos (cod_entidad,cod_segm,desc_segm,desc_detall_segm,cod_gpo_segm,tpo_pers,est_segm,user_alt_segm,fec_alt_segm,user_modf_segm,fec_modf_segm)
           select cod_entidad,cod_segm,desc_segm,desc_detall_segm,cod_gpo_segm,tpo_pers,est_segm,user_alt_segm,fec_alt_segm,user_modf_segm,fec_modf_segm
             from gec01.gc_segmentos
            where fec_modf_segm between fec_desde and fec_hasta;

      insert into ig_grupo_segmentos (cod_entidad,cod_gpo_segm,desc_gpo_segm,est_gpo_segm,user_alt_gpo_segm,fec_alt_gpo_segm,user_modf_gpo_segm,fec_modf_gpo_segm,sector_owner)
           select cod_entidad,cod_gpo_segm,desc_gpo_segm,est_gpo_segm,user_alt_gpo_segm,fec_alt_gpo_segm,user_modf_gpo_segm,fec_modf_gpo_segm,sector_owner
             from gec01.gc_grupo_segmento;

    v_paso := 2;
    --Se almacena la informacion definitiva en la tabla de interfaz INT_SEGMENTOS
    insert into int_segmentos
         select seg.cod_segm, seg.desc_segm, seg.desc_detall_segm, gru.desc_gpo_segm, seg.est_segm
           from ig_segmentos seg, ig_grupo_segmentos gru
          where seg.cod_entidad = gru.cod_entidad
            and seg.cod_gpo_segm = gru.cod_gpo_segm;

    v_paso := 3;
    out_file := null;
    out_file := utl_file.fopen (v_path_utl, 'DIM_SEGMENTOS.txt', 'w',v_line_max);

    for x in (select * from int_segmentos) loop
      v_paso := 4;
      --Modifica el segmento en caso de que exista
      update dim_segmentos
         set seg_desc = x.seg_desc,
             seg_detalle = x.seg_detalle,
             seg_grupo = x.seg_grupo,
             seg_estado = x.seg_estado
       where seg_cod = x.seg_cod;

      if sql%notfound then
        v_paso := 5;
        --Inserta el segmento en caso de que no exista
        insert into dim_segmentos
          values (x.seg_cod, x.seg_desc, x.seg_detalle, x.seg_grupo, x.seg_estado);
      end if;

      v_paso := 6;
      utl_file.put_line (out_file, x.seg_cod || v_separador || x.seg_desc || v_separador || x.seg_detalle || v_separador || x.seg_grupo || v_separador || x.seg_estado);

    end loop;

    v_paso := 7;
    utl_file.fclose(out_file);

    commit;

    v_paso := 4;
    v_fecha_fin := sysdate;
    pkg_gc_procesos.sp_procesos(2, 2, v_fecha_inicio, v_fecha_fin, id_proceso);
  exception
    when others then
      v_fecha_fin := sysdate;
      pkg_gc_procesos.sp_procesos(2, 6, v_fecha_inicio, v_fecha_fin, id_proceso);
      v_err_sql := substr (sqlerrm (sqlcode), 1, 250);
      pkg_gc_procesos.sp_inconsistencias(id_proceso, sysdate, v_paso, v_err_sql);
  end sp_segmentos;
*/

/*SP_FAVORABILIDAD 
  procedure sp_favorabilidad (id_proceso in number, fec_desde in date, fec_hasta in date) as
  begin
    v_fecha_inicio := sysdate;
    pkg_gc_procesos.sp_procesos(1, 1, v_fecha_inicio, null, id_proceso);

    v_paso := 1;
    --Se replican los datos en la tabla temporal IG_FAVORABILIDAD sin filtro
      insert into ig_tipo_favorabilidad (cod_entidad,cod_tipo_favorabilidad,desc_tipo_favorabilidad,est_tipo_favorabilidad,user_alt_tpo_favorabilidad,fec_alt_tpo_favorabilidad,user_modf_tpo_favorabilidad,fec_modf_tpo_favorabilidad,sector_owner)
           select cod_entidad,cod_tipo_favorabilidad,desc_tipo_favorabilidad,est_tipo_favorabilidad,user_alt_tpo_favorabilidad,fec_alt_tpo_favorabilidad,user_modf_tpo_favorabilidad,fec_modf_tpo_favorabilidad,sector_owner
             from gec01.tipo_favorabilidad
            where fec_modf_tpo_favorabilidad between fec_desde and fec_hasta;

    v_paso := 2;
    --Se almacena la informacion definitiva en la tabla de interfaz INT_FAVORABILIDAD
    insert into int_favorabilidad
      select cod_tipo_favorabilidad , desc_tipo_favorabilidad , est_tipo_favorabilidad from ig_tipo_favorabilidad;

    v_paso := 3;
    out_file := null;
    out_file := utl_file.fopen (v_path_utl, 'DIM_FAVORABILIDAD.txt', 'w',v_line_max);

    for x in (select * from int_favorabilidad) loop
      v_paso := 4;
      --Modificala favorabilidad en caso de que exista
      update dim_favorabilidad
         set fav_desc = x.fav_desc,
             fav_estado = x.fav_estado
       where fav_cod = x.fav_cod;

      if sql%notfound then
        v_paso := 5;
        --Inserta la favorabilidad en caso de que no exista
        insert into dim_favorabilidad
             values (x.fav_cod, x.fav_desc, x.fav_estado);
      end if;

      v_paso := 6;
      utl_file.put_line (out_file, x.fav_cod || v_separador || x.fav_desc || v_separador || x.fav_estado);

    end loop;

    v_paso := 7;
    utl_file.fclose(out_file);

    commit;

    v_paso := 4;
    v_fecha_fin := sysdate;
    pkg_gc_procesos.sp_procesos(2, 2, v_fecha_inicio, v_fecha_fin, id_proceso);
  exception
    when others then
      v_fecha_fin := sysdate;
      pkg_gc_procesos.sp_procesos(2, 6, v_fecha_inicio, v_fecha_fin, id_proceso);
      v_err_sql := substr (sqlerrm (sqlcode), 1, 250);
      pkg_gc_procesos.sp_inconsistencias(id_proceso, sysdate, v_paso, v_err_sql);
  end sp_favorabilidad;
*/

/*SP_ESTADOS 
  procedure sp_estados (id_proceso in number, fec_desde in date, fec_hasta in date) as
  begin
    v_fecha_inicio := sysdate;
    pkg_gc_procesos.sp_procesos(1, 1, v_fecha_inicio, null, id_proceso);

    v_paso := 1;
    --Se replican los datos en la tabla temporal IG_ESTADOS
    insert into ig_estados (cod_entidad,cod_estado,desc_estado,cod_etapa,cod_bandeja,indi_est_autm,cod_est_contin,cant_dia_est_autm,indi_time_out,indi_anula_time_out,est_actv,user_alt_estado,fec_est_alt,user_modif_estado,fec_modf_estado)
         select cod_entidad,cod_estado,desc_estado,cod_etapa,cod_bandeja,indi_est_autm,cod_est_contin,cant_dia_est_autm,indi_time_out,indi_anula_time_out,est_actv,user_alt_estado,fec_est_alt,user_modif_estado,fec_modf_estado
           from gec01.estados
          where fec_modf_estado between fec_desde and fec_hasta;

    v_paso := 2;
    --Se almacena la informacion definitiva en la tabla de interfaz INT_ESTADOS
    insert into int_estados
      select cod_estado, desc_estado, est_actv, cod_bandeja, cod_etapa from ig_estados;

    v_paso := 3;
    out_file := null;
    out_file := utl_file.fopen (v_path_utl, 'DIM_ESTADOS.txt', 'w',v_line_max);

    for x in (select * from int_estados) loop
      v_paso := 4;
      --Modifica el estados en caso de que exista
      update dim_estados
         set est_desc = x.est_desc ,
             est_estado = x.est_estado,
             est_bandeja = x.est_bandeja,
             est_etapa = x.est_etapa
       where est_cod = x.est_cod;

      if sql%notfound then
        v_paso := 5;
        --Inserta el estados en caso de que no exista
        insert into dim_estados
             values (x.est_cod,x.est_desc, x.est_estado, x.est_bandeja, x.est_etapa);
      end if;

      v_paso := 6;
      utl_file.put_line (out_file, x.est_desc || v_separador || x.est_estado || v_separador || x.est_bandeja || v_separador || x.est_etapa);

    end loop;

    v_paso := 7;
    utl_file.fclose(out_file);

    commit;

    v_paso := 4;
    v_fecha_fin := sysdate;
    pkg_gc_procesos.sp_procesos(2, 2, v_fecha_inicio, v_fecha_fin, id_proceso);
   exception
    when others then
      v_fecha_fin := sysdate;
      pkg_gc_procesos.sp_procesos(2, 6, v_fecha_inicio, v_fecha_fin, id_proceso);
      v_err_sql := substr (sqlerrm (sqlcode), 1, 250);
      pkg_gc_procesos.sp_inconsistencias(id_proceso, sysdate, v_paso, v_err_sql);
  end sp_estados;
*/

/*SP_SECTORES
  procedure sp_sectores (id_proceso in number, fec_desde in date, fec_hasta in date) as
  begin
    v_fecha_inicio := sysdate;
    pkg_gc_procesos.sp_procesos(1, 1, v_fecha_inicio, null, id_proceso);

    v_paso := 1;
    --Se replican los datos en la tabla temporal  temporales IG_*** involucradas en el calculo de la dimension DIM_SECTORES
      insert into ig_sectores (cod_entidad,cod_sector,desc_sect,cod_grupo,indi_actral,cod_pcia,mail_sector,est_sect,user_alt_sect,fec_alt_sect,user_modf_sect,fec_modf_sect,enviar_mail,indi_info_asig_esp,cod_pais,indi_resol_info_adj,cod_ctro_costos,indi_mail_bandeja,indi_uso_carta,sector_owner,cod_grupo_empresa,indi_admin,gest_x_pag_bandeja,cod_sucursal,cod_zona,tipo_sector,class_pdf,dias_gestiones_ant,cod_sector_gen,indi_compromiso_gold,indi_enviar_mail_resp,indi_enviar_sms_resp,indi_no_enviar_mya,indi_instancias_post)
           select cod_entidad,cod_sector,desc_sect,cod_grupo,indi_actral,cod_pcia,mail_sector,est_sect,user_alt_sect,fec_alt_sect,user_modf_sect,fec_modf_sect,enviar_mail,indi_info_asig_esp,cod_pais,indi_resol_info_adj,cod_ctro_costos,indi_mail_bandeja,indi_uso_carta,sector_owner,cod_grupo_empresa,indi_admin,gest_x_pag_bandeja,cod_sucursal,cod_zona,tipo_sector,class_pdf,dias_gestiones_ant,cod_sector_gen,indi_compromiso_gold,indi_enviar_mail_resp,indi_enviar_sms_resp,indi_no_enviar_mya,indi_instancias_post
             from gec01.sectores
            where fec_modf_sect between fec_desde and fec_hasta;

      insert into ig_grupos (cod_entidad,cod_grupo,desc_grupo,est_grupo,user_alt_gpo,fec_alt_gpo,user_modf_gpo,fec_modf_gpo,sector_owner,cod_grupo_empresa)
           select cod_entidad,cod_grupo,desc_grupo,est_grupo,user_alt_gpo,fec_alt_gpo,user_modf_gpo,fec_modf_gpo,sector_owner,cod_grupo_empresa
             from gec01.grupos;

      insert into ig_provincias (cod_pais,cod_pcia,desc_pcia)
           select cod_pais,cod_pcia,desc_pcia
             from gec01.provincias;

      insert into ig_tipo_sector (cod_entidad,cod_tipo_sector,desc_tipo_sector)
           select cod_entidad,cod_tipo_sector,desc_tipo_sector
             from gec01.tipo_sector;

      insert into ig_sucursal (cod_entidad,cod_sucursal,desc_sucursal,indi_actv_suc,cod_zona_suc,user_alt_sucursal,fec_alt_sucursal,user_modf_sucursal,fec_modf_sucursal,sector_owner,indi_rec_trasp_cta,indi_rec_trasp_cta_select)
           select cod_entidad,cod_sucursal,desc_sucursal,indi_actv_suc,cod_zona_suc,user_alt_sucursal,fec_alt_sucursal,user_modf_sucursal,fec_modf_sucursal,sector_owner,indi_rec_trasp_cta,indi_rec_trasp_cta_select
             from gec01.sucursal;

      insert into ig_zona (cod_entidad,cod_zona,desc_zona,est_zona,user_alt_zona,fec_alt_zona,user_modf_zona,fec_modf_zona,sector_owner)
           select cod_entidad,cod_zona,desc_zona,est_zona,user_alt_zona,fec_alt_zona,user_modf_zona,fec_modf_zona,sector_owner
             from gec01.zona;

    v_paso := 2;
    --Se almacena la informacion definitiva en la tabla de interfaz INT_SECTORES
    insert into int_sectores
         select sec.cod_sector,
                sec.desc_sect,
                gru.desc_grupo,
                pro.desc_pcia,
                sec.mail_sector,
                sec.est_sect,
                tipo.desc_tipo_sector,
                decode(suc.desc_sucursal,null,'NO DEFINIDO',suc.desc_sucursal),
                decode(zona.desc_zona,null,'NO DEFINIDO',zona.desc_zona),
                decode(sec.cod_ctro_costos,null,'999-999',sec.cod_ctro_costos),
                sec.cod_grupo,
                sec.indi_actral
           from ig_sectores sec,
                ig_grupos gru,
                ig_provincias pro,
                ig_tipo_sector tipo,
                ig_sucursal suc,
                ig_zona zona
          where sec.cod_entidad = gru.cod_entidad
            and sec.cod_grupo = gru.cod_grupo
            and sec.cod_pcia = pro.cod_pcia
            and sec.cod_entidad = tipo.cod_entidad
            and sec.tipo_sector = tipo.cod_tipo_sector
            and sec.cod_entidad = suc.cod_entidad(+)
            and sec.cod_sucursal = suc.cod_sucursal(+)
            and suc.cod_entidad = zona.cod_entidad(+)
            and suc.cod_zona_suc = zona.cod_zona(+);

    v_paso := 3;
    out_file := null;
    out_file := utl_file.fopen (v_path_utl, 'DIM_SECTORES.txt', 'w',v_line_max);

    for x in (select * from int_sectores) loop
      v_paso := 4;
      --Modifica los sectores  en caso de que exista
      update dim_sectores
         set sec_desc = x.sec_desc,
             sec_grupo_sector = x.sec_grupo_sector,
             sec_provincia = x.sec_provincia,
             sec_mail = x.sec_mail,
             sec_estado= x.sec_estado,
             sec_tipo_sector = x.sec_tipo_sector,
             sec_sucursal = x.sec_sucursal,
             sec_zona = x.sec_zona,
             sec_ccostos = x.sec_ccostos,
             sec_cod_grupo = x.sec_cod_grupo,
             sec_indi_actral = x.sec_indi_actral
       where sec_cod = x.sec_cod;

      if sql%notfound then
        v_paso := 5;
        --Inserta los sectores en caso de que no exista
        insert into dim_sectores
             values (x.sec_cod,
                     x.sec_desc,
                     x.sec_grupo_sector,
                     x.sec_provincia,
                     x.sec_mail,
                     x.sec_estado,
                     x.sec_tipo_sector,
                     x.sec_sucursal,
                     x.sec_zona,
                     x.sec_ccostos,
                     x.sec_cod_grupo,
                     x.sec_indi_actral);
      end if;

      v_paso := 6;
      utl_file.put_line (out_file, x.sec_cod || v_separador ||
                                   x.sec_desc || v_separador ||
                                   x.sec_grupo_sector || v_separador ||
                                   x.sec_provincia || v_separador ||
                                   x.sec_mail || v_separador ||
                                   x.sec_estado || v_separador ||
                                   x.sec_tipo_sector || v_separador ||
                                   x.sec_sucursal || v_separador ||
                                   x.sec_zona || v_separador ||
                                   x.sec_ccostos || v_separador ||
                                   x.sec_cod_grupo || v_separador ||
                                   x.sec_indi_actral);

    end loop;

    v_paso := 7;
    utl_file.fclose(out_file);

    commit;

    v_paso := 8;
    v_fecha_fin := sysdate;
    pkg_gc_procesos.sp_procesos(2, 2, v_fecha_inicio, v_fecha_fin, id_proceso);
  exception
     when others then
      v_fecha_fin := sysdate;
      pkg_gc_procesos.sp_procesos(2, 6, v_fecha_inicio, v_fecha_fin, id_proceso);
      v_err_sql := substr (sqlerrm (sqlcode), 1, 250);
      pkg_gc_procesos.sp_inconsistencias(id_proceso, sysdate, v_paso, v_err_sql);
  end sp_sectores;
*/

/*SP_CLIENTES 
  procedure sp_clientes (id_proceso in number, fec_desde in date, fec_hasta in date) as
  begin
    v_fecha_inicio := sysdate;
    pkg_gc_procesos.sp_procesos(1, 1, v_fecha_inicio, null, id_proceso);

    v_paso := 1;
    --Se replican los datos en las tablas involucradas en el calculo de la dimension de Clientes
    insert into ig_gestiones (cod_entidad,ide_gestion_sector,ide_gestion_nro,ide_circuito,indi_tipo_circ,indi_decision_comer,indi_replteo,indi_rta_inmed,indi_impre_cart,imp_autz_solicitado,cod_mone_solicitado,imp_autz_autorizado,cod_mone_autorizado,imp_autz_resolucion,cod_mone_resolucion,tpo_pers,cod_crm,comen_cliente,ide_gest_sector_relac,ide_gest_nro_relac,fec_gestion_alta,cod_bandeja_actual,fec_bandeja_actual,cod_sector_actual,inic_user_alta,indi_mail,indi_prioridad,fec_max_resol,cod_conforme,cod_user_actual,cod_grupo_empresa,cod_tipo_favorabilidad,fec_aviso_venc)
         select cod_entidad,ide_gestion_sector,ide_gestion_nro,ide_circuito,indi_tipo_circ,indi_decision_comer,indi_replteo,indi_rta_inmed,indi_impre_cart,imp_autz_solicitado,cod_mone_solicitado,imp_autz_autorizado,cod_mone_autorizado,imp_autz_resolucion,cod_mone_resolucion,tpo_pers,cod_crm,comen_cliente,ide_gest_sector_relac,ide_gest_nro_relac,fec_gestion_alta,cod_bandeja_actual,fec_bandeja_actual,cod_sector_actual,inic_user_alta,indi_mail,indi_prioridad,fec_max_resol,cod_conforme,cod_user_actual,cod_grupo_empresa,cod_tipo_favorabilidad,fec_aviso_venc
           from gec01.gestiones
          where fec_gestion_alta between fec_desde and fec_hasta;

    insert into ig_gc_individuo_gestiones (cod_entidad,ide_gestion_sector,ide_gestion_nro,tpo_doc_indi,nro_doc_indi,fec_naci_indi,mar_sex_indi,ide_nup_indi)
         select cod_entidad,ide_gestion_sector,ide_gestion_nro,tpo_doc_indi,nro_doc_indi,fec_naci_indi,mar_sex_indi, 1
           from gec01.gc_individuo_gestiones ges
          where ges.ide_gestion_nro in (select ide_gestion_nro
                                          from ig_gestiones);

    insert into ig_gc_individuos (cod_entidad,tpo_doc_indi,nro_doc_indi,fec_naci_indi,mar_sex_indi,ape_indi,nom_indi,ide_nup_indi,cod_suc_indi,cod_segm_indi,cod_crm_indi,semf_ingreso_crm,semf_renta_crm,semf_riesgo_crm,rentabilidad_promedio,color_semaforo,color_semaf_riesgo)
         select distinct ind.cod_entidad,ind.tpo_doc_indi,ind.nro_doc_indi,ind.fec_naci_indi,ind.mar_sex_indi,ind.ape_indi,ind.nom_indi,ind.ide_nup_indi,ind.cod_suc_indi,ind.cod_segm_indi,ind.cod_crm_indi,ind.semf_ingreso_crm,ind.semf_renta_crm,ind.semf_riesgo_crm,ind.rentabilidad_promedio,ind.color_semaforo,ind.color_semaf_riesgo
           from gec01.gc_individuos ind,
                ig_gc_individuo_gestiones ges
          where ind.cod_entidad = ges.cod_entidad
            and ind.tpo_doc_indi = ges.tpo_doc_indi
            and ind.nro_doc_indi = ges.nro_doc_indi
            and ind.fec_naci_indi = ges.fec_naci_indi;
            --and ind.mar_sex_indi = ges.mar_sex_indi;

    insert into ig_gc_empresa_gestiones (cod_entidad,ide_gestion_sector,ide_gestion_nro,tpo_doc_empr,nro_doc_empr,sec_doc_empr,ide_nup_empr)
         select cod_entidad,ide_gestion_sector,ide_gestion_nro,tpo_doc_empr,nro_doc_empr,sec_doc_empr, 1
           from gec01.gc_empresa_gestiones ges
          where ges.ide_gestion_nro in (select ide_gestion_nro
                                          from ig_gestiones);

    insert into ig_gc_empresas (cod_entidad,tpo_doc_empr,nro_doc_empr,sec_doc_empr,tpo_empr,fec_ini_activ_empr,cod_activ_empr,nom_comer_empr,razon_social_empr,ide_nup_empr,ape_resp_empr,nom_resp_empr,tpo_doc2_empr,nro_doc2_empr,cod_suc_empr,cod_segm_empr,cod_crm_empr,semf_ingreso_crm,semf_renta_crm,semf_riesgo_crm,rentabilidad_promedio,color_semaforo,color_semaf_riesgo)
         select distinct emp.cod_entidad,emp.tpo_doc_empr,emp.nro_doc_empr,emp.sec_doc_empr,tpo_empr,emp.fec_ini_activ_empr,emp.cod_activ_empr,emp.nom_comer_empr,emp.razon_social_empr,emp.ide_nup_empr,emp.ape_resp_empr,emp.nom_resp_empr,emp.tpo_doc2_empr,emp.nro_doc2_empr,emp.cod_suc_empr,emp.cod_segm_empr,emp.cod_crm_empr,emp.semf_ingreso_crm,emp.semf_renta_crm,emp.semf_riesgo_crm,emp.rentabilidad_promedio,emp.color_semaforo,emp.color_semaf_riesgo
           from gec01.gc_empresas emp,
                ig_gc_empresa_gestiones ges
          where emp.cod_entidad = ges.cod_entidad
            and emp.tpo_doc_empr = ges.tpo_doc_empr
            and emp.nro_doc_empr = ges.nro_doc_empr
            and emp.sec_doc_empr = ges.sec_doc_empr;

    insert into ig_tipo_documentos (cod_entidad,tpo_doc,desc_tpo_doc,indi_doc_x_defecto)
         select cod_entidad,tpo_doc,desc_tpo_doc,indi_doc_x_defecto
           from gec01.tipo_documentos;

    insert into ig_clasif_crm (cod_entidad,tpo_pers,cod_crm,desc_crm,id_clasif_select)
         select cod_entidad,tpo_pers,cod_crm,desc_crm,id_clasif_select
           from gec01.clasif_crm;

    insert into ig_tipo_persona (cod_entidad,tpo_pers,desc_tpo_pers,est_tpo_pers,user_alt_tpo_pers,fec_alt_tpo_pers,user_modf_tpo_pers,fec_modf_tpo_pers)
         select cod_entidad,tpo_pers,desc_tpo_pers,est_tpo_pers,user_alt_tpo_pers,fec_alt_tpo_pers,user_modf_tpo_pers,fec_modf_tpo_pers
           from gec01.tipo_persona;

    insert into ig_clasif_select (id_clasif_select,tipo,orden)
         select id_clasif_select,tipo,orden
           from gec01.clasif_select;

    insert into ig_gc_activ_empresa (cod_entidad,cod_activ_empr,desc_activ_empr)
         select cod_entidad,cod_activ_empr,desc_activ_empr
           from gec01.gc_activ_empresa;

    v_paso := 2;
    --se ingresan los datos a la tabla de interfaz INT_CLIENTES
    insert into int_clientes
      select distinct 'I' tipo_cliente,
             tipo_d.desc_tpo_doc tipo_doc,
             ind.nro_doc_indi num_doc,
             ind.ide_nup_indi num_nup,
             gest.cod_crm cod_crm,
             crm.desc_crm desc_crm,
             tipo_p.desc_tpo_pers tipo_persona,
             sel.tipo indicador_select,
             ind.mar_sex_indi sexo_individuo,
             ind.ape_indi apellidos,
             ind.nom_indi nombres,
             ind.fec_naci_indi fecha_naci_inicio,
             'NO APLICA' emp_nombre_comercial,
             'NO APLICA' emp_razon_social,
             'NO APLICA' emp_apellido_respon,
             'NO APLICA' emp_nombre_respon,
             'NO APLICA' emp_tipo,
             'NO APLICA' emp_actividad,
             ind.nro_doc_indi||ind.ide_nup_indi codigo,
             ind.cod_suc_indi,
             ind.cod_segm_indi,
             ind.tpo_doc_indi
        from ig_gc_individuo_gestiones ges,
             ig_gestiones gest,
             ig_gc_individuos ind,
             ig_tipo_documentos tipo_d,
             ig_tipo_persona tipo_p,
             ig_clasif_crm crm,
             ig_clasif_select sel
       where ges.cod_entidad = gest.cod_entidad(+)
         and ges.ide_gestion_sector = gest.ide_gestion_sector(+)
         and ges.ide_gestion_nro = gest.ide_gestion_nro(+)
         and ges.cod_entidad = ind.cod_entidad
         and ges.tpo_doc_indi = ind.tpo_doc_indi
         and ges.nro_doc_indi = ind.nro_doc_indi
         and ges.fec_naci_indi = ind.fec_naci_indi
         and ges.mar_sex_indi = ind.mar_sex_indi
         and ges.cod_entidad = tipo_d.cod_entidad
         and ges.tpo_doc_indi = tipo_d.tpo_doc
         and gest.cod_entidad = tipo_p.cod_entidad
         and gest.tpo_pers = tipo_p.tpo_pers
         and gest.cod_entidad = crm.cod_entidad
         and gest.cod_crm = crm.cod_crm
         and gest.tpo_pers = crm.tpo_pers
         and crm.id_clasif_select = sel.id_clasif_select
      union all
      select distinct 'E' tipo_cliente,
             tipo_d.desc_tpo_doc tipo_doc,
             ges.nro_doc_empr num_doc,
             emp.ide_nup_empr num_nup,
             gest.cod_crm cod_crm,
             crm.desc_crm desc_crm,
             tipo_p.desc_tpo_pers tipo_persona,
             'NO APLICA' indicador_select,
             'NO APLICA' sexo_individuo,
             'NO APLICA' apellidos,
             'NO APLICA' nombres,
             emp.fec_ini_activ_empr fecha_naci_inicio,
             emp.nom_comer_empr emp_nombre_comercial,
             emp.razon_social_empr emp_razon_social,
             emp.ape_resp_empr emp_apellido_respon,
             emp.nom_resp_empr emp_nombre_respon,
             emp.tpo_empr emp_tipo,
             act.desc_activ_empr emp_actividad,
             ges.nro_doc_empr||emp.ide_nup_empr codigo,
             emp.cod_suc_empr,
             emp.cod_segm_empr,
             emp.tpo_doc_empr
        from ig_gc_empresa_gestiones ges,
             ig_gestiones gest,
             ig_gc_empresas emp,
             ig_tipo_documentos tipo_d,
             ig_clasif_crm crm,
             ig_gc_activ_empresa act,
             ig_tipo_persona tipo_p
       where ges.cod_entidad = gest.cod_entidad
         and ges.ide_gestion_sector = gest.ide_gestion_sector
         and ges.ide_gestion_nro = gest.ide_gestion_nro
         and ges.cod_entidad = emp.cod_entidad
         and ges.tpo_doc_empr = emp.tpo_doc_empr
         and ges.nro_doc_empr = emp.nro_doc_empr
         and ges.sec_doc_empr = emp.sec_doc_empr
         and ges.cod_entidad = tipo_d.cod_entidad
         and ges.tpo_doc_empr = tipo_d.tpo_doc
         and gest.cod_entidad = crm.cod_entidad
         and gest.cod_crm = crm.cod_crm
         and gest.tpo_pers = crm.tpo_pers
         and emp.cod_entidad = act.cod_entidad
         and emp.cod_activ_empr = act.cod_activ_empr
         and gest.cod_entidad = tipo_p.cod_entidad
         and gest.tpo_pers = tipo_p.tpo_pers;

    v_paso := 3;
    out_file := null;
    out_file := utl_file.fopen (v_path_utl, 'DIM_CLIENTES.txt', 'w',v_line_max);

    for x in (select * from int_clientes) loop
      v_paso := 4;
      --Modificala el cliente en caso de que exista
      update dim_clientes
         set cli_cod_crm = x.cli_cod_crm,
             cli_desc_crm = x.cli_desc_crm,
             cli_tipo_persona = x.cli_tipo_persona,
             cli_ind_select = x.cli_ind_select,
             cli_ind_sex = x.cli_ind_sex,
             cli_ind_apellidos = x.cli_ind_apellidos,
             cli_ind_nombres = x.cli_ind_nombres,
             cli_fec_nac_inicio = x.cli_fec_nac_inicio,
             cli_emp_nom_comercial = x.cli_emp_nom_comercial,
             cli_emp_razon_social = x.cli_emp_razon_social,
             cli_emp_ape_respon = x.cli_emp_ape_respon,
             cli_emp_nom_respon = x.cli_emp_nom_respon,
             cli_emp_tipo = x.cli_emp_tipo,
             cli_emp_actividad = x.cli_emp_actividad,
             cli_tipo_cliente = x.cli_tipo_cliente,
             cli_tipo_doc = x.cli_tipo_doc,
             cli_num_doc = x.cli_num_doc,
             cli_num_nup = x.cli_num_nup,
             cli_cod_suc = x.cli_cod_suc,
             cli_cod_segm = x.cli_cod_segm,
             cli_cod_tipo_doc = x.cli_cod_tipo_doc
       where cli_cod_cliente = x.cli_cod_cliente;

      if sql%notfound then
        v_paso := 5;
        --Inserta el cliente en caso de que no exista
        insert into dim_clientes values (x.cli_tipo_cliente,
                                         x.cli_tipo_doc,
                                         x.cli_num_doc,
                                         x.cli_num_nup,
                                         x.cli_cod_crm,
                                         x.cli_desc_crm,
                                         x.cli_tipo_persona,
                                         x.cli_ind_select,
                                         x.cli_ind_sex,
                                         x.cli_ind_apellidos,
                                         x.cli_ind_nombres,
                                         x.cli_fec_nac_inicio,
                                         x.cli_emp_nom_comercial,
                                         x.cli_emp_razon_social,
                                         x.cli_emp_ape_respon,
                                         x.cli_emp_nom_respon,
                                         x.cli_emp_tipo,
                                         x.cli_emp_actividad,
                                         x.cli_cod_cliente,
                                         x.cli_cod_suc,
                                         x.cli_cod_segm,
                                         x.cli_cod_tipo_doc);
      end if;

    v_paso := 6;
    utl_file.put_line (out_file, x.cli_tipo_cliente || v_separador ||
                                 x.cli_tipo_doc || v_separador ||
                                 x.cli_num_doc || v_separador ||
                                 x.cli_num_nup || v_separador ||
                                 x.cli_cod_crm || v_separador ||
                                 x.cli_desc_crm || v_separador ||
                                 x.cli_tipo_persona || v_separador ||
                                 x.cli_ind_select || v_separador ||
                                 x.cli_ind_sex || v_separador ||
                                 x.cli_ind_apellidos || v_separador ||
                                 x.cli_ind_nombres || v_separador ||
                                 x.cli_fec_nac_inicio || v_separador ||
                                 x.cli_emp_nom_comercial || v_separador ||
                                 x.cli_emp_razon_social || v_separador ||
                                 x.cli_emp_ape_respon || v_separador ||
                                 x.cli_emp_nom_respon || v_separador ||
                                 x.cli_emp_tipo || v_separador ||
                                 x.cli_emp_actividad || v_separador ||
                                 x.cli_cod_cliente || v_separador ||
                                 x.cli_cod_suc || v_separador ||
                                 x.cli_cod_segm || v_separador ||
                                 x.cli_cod_tipo_doc);
    end loop;

    v_paso := 7;
    utl_file.fclose(out_file);

    commit;

    v_paso := 8;
    v_fecha_fin := sysdate;
    pkg_gc_procesos.sp_procesos(2, 2, v_fecha_inicio, v_fecha_fin, id_proceso);
  exception
    when others then
      v_fecha_fin := sysdate;
      pkg_gc_procesos.sp_procesos(2, 6, v_fecha_inicio, v_fecha_fin, id_proceso);
      v_err_sql := substr (sqlerrm (sqlcode), 1, 250);
      pkg_gc_procesos.sp_inconsistencias(id_proceso, sysdate, v_paso, v_err_sql);
  end sp_clientes;
*/

/*SP_CIRCUITOS 
  procedure sp_circuitos (id_proceso in number, fec_desde in date, fec_hasta in date) as
  begin
    v_fecha_inicio := sysdate;
    pkg_gc_procesos.sp_procesos(1, 1, v_fecha_inicio, null, id_proceso);

    v_paso := 1;
    --Se replican los datos en la tabla temporal IG_CIRCUITOS
    insert into ig_circuito (cod_entidad,ide_circuito,cod_circuito,fec_vig_dde_circ,fec_vig_hta_circ,desc_circ,desc_detall_circ,tpo_gestion,cod_prod,cod_subprod,cod_cpto,cod_subcpto,tmp_asign_especial,tmp_pedido_info,tmp_circ,indi_gest_pend,indi_mail_demora,indi_cierr_autm,cod_recibo,est_circ,user_alt_circ,fec_alt_circ,user_modf_circ,fec_modf_circ,indi_modif_datos,indi_recep_resp,indi_sucursales,indi_dejar_pend,cod_tpo_resol,und_tiempo,tpo_vis_gest,indi_suma_tmp_autz,cod_conforme,indi_jerarquia_autz,indi_secuencia_autz,indi_asig_paralelo,indi_autoriz_devol,indi_modf_fec,circuito_predecesor,indi_autoriza_item,indi_crit,indi_info_multivaluada,indi_enviar_mail_recep,indi_enviar_sms_recep,indi_enviar_mail_resp,indi_enviar_sms_resp,indi_enviar_mail_demora,cod_modelo_msj,id_parrafo_cuerpo_mail,indi_uso_monto,id_clasif_select,indi_enviar_mail_prov,cod_comprobante_cliente,indi_enviar_carta_resp,indi_reapertura,long_comentario_recep,long_comentario_cliente,aviso_venc_gest)
         select cod_entidad,ide_circuito,cod_circuito,fec_vig_dde_circ,fec_vig_hta_circ,desc_circ,desc_detall_circ,tpo_gestion,cod_prod,cod_subprod,cod_cpto,cod_subcpto,tmp_asign_especial,tmp_pedido_info,tmp_circ,indi_gest_pend,indi_mail_demora,indi_cierr_autm,cod_recibo,est_circ,user_alt_circ,fec_alt_circ,user_modf_circ,fec_modf_circ,indi_modif_datos,indi_recep_resp,indi_sucursales,indi_dejar_pend,cod_tpo_resol,und_tiempo,tpo_vis_gest,indi_suma_tmp_autz,cod_conforme,indi_jerarquia_autz,indi_secuencia_autz,indi_asig_paralelo,indi_autoriz_devol,indi_modf_fec,circuito_predecesor,indi_autoriza_item,indi_crit,indi_info_multivaluada,indi_enviar_mail_recep,indi_enviar_sms_recep,indi_enviar_mail_resp,indi_enviar_sms_resp,indi_enviar_mail_demora,cod_modelo_msj,id_parrafo_cuerpo_mail,indi_uso_monto,id_clasif_select,indi_enviar_mail_prov,cod_comprobante_cliente,indi_enviar_carta_resp,indi_reapertura,long_comentario_recep,long_comentario_cliente,aviso_venc_gest
           from gec01.circuito
          where fec_modf_circ between fec_desde and fec_hasta;

    insert into ig_producto (cod_entidad,cod_prod,desc_prod,est_prod,user_alt_prod,fec_alt_prod,user_modf_prod,fec_modf_prod)
         select cod_entidad,cod_prod,desc_prod,est_prod,user_alt_prod,fec_alt_prod,user_modf_prod,fec_modf_prod
           from gec01.producto;

    insert into ig_subproducto (cod_entidad,cod_subprod,desc_subprod,est_subprod,user_alt_subprod,fec_alt_subprod,user_modf_subprod,fec_modf_subprod)
         select cod_entidad,cod_subprod,desc_subprod,est_subprod,user_alt_subprod,fec_alt_subprod,user_modf_subprod,fec_modf_subprod
           from gec01.subproducto;

    insert into ig_concepto (cod_entidad,cod_cpto,desc_cpto,desc_detall_cpto,est_cpto,user_alt_cpto,fec_alt_cpto,user_modf_cpto,fec_modf_cpto,sector_owner)
         select cod_entidad,cod_cpto,desc_cpto,desc_detall_cpto,est_cpto,user_alt_cpto,fec_alt_cpto,user_modf_cpto,fec_modf_cpto,sector_owner
           from gec01.concepto;

    insert into ig_subconcepto (cod_entidad,cod_subcpto,desc_subcpto,desc_detall_subcpto,est_subcpto,user_alt_subcpto,fec_alt_subcpto,user_modf_subcpto,fec_modf_subcpto,sector_owner)
         select cod_entidad,cod_subcpto,desc_subcpto,desc_detall_subcpto,est_subcpto,user_alt_subcpto,fec_alt_subcpto,user_modf_subcpto,fec_modf_subcpto,sector_owner
           from gec01.subconcepto;

    insert into ig_tipo_gestion (cod_entidad,tpo_gestion,desc_tpo_gest,desc_detall_tpo_gest,est_tpo_gest,user_alt_tpo_gest,fec_alt_tpo_gest,user_modf_tpo_gest,fec_modf_tpo_gest)
         select cod_entidad,tpo_gestion,desc_tpo_gest,desc_detall_tpo_gest,est_tpo_gest,user_alt_tpo_gest,fec_alt_tpo_gest,user_modf_tpo_gest,fec_modf_tpo_gest
           from gec01.tipo_gestion;

    insert into ig_gc_conceptos_bcra_circ (cod_entidad,cod_cpto,ide_circuito,user_alt,fec_alt,user_modf,fec_modf)
         select bcra.cod_entidad,bcra.cod_cpto,bcra.ide_circuito,bcra.user_alt,fec_alt,bcra.user_modf,bcra.fec_modf
           from gec01.gc_conceptos_bcra_circ bcra, gec01.circuito cir
          where bcra.cod_entidad = cir.cod_entidad
            and bcra.ide_circuito = cir.ide_circuito;

    insert into ig_gc_conceptos_sac_circ (cod_entidad,cod_cpto,ide_circuito,user_alt,fec_alt,user_modf,fec_modf)
         select sac.cod_entidad,sac.cod_cpto,sac.ide_circuito,sac.user_alt,sac.fec_alt,sac.user_modf,sac.fec_modf
           from gec01.gc_conceptos_sac_circ sac, gec01.circuito cir
          where sac.cod_entidad = cir.cod_entidad
            and sac.ide_circuito = cir.ide_circuito;

    insert into ig_gc_conceptos_espana_circ (cod_entidad,cod_cpto,ide_circuito,user_alt,fec_alt,user_modf,fec_modf)
         select esp.cod_entidad,esp.cod_cpto,esp.ide_circuito,esp.user_alt,esp.fec_alt,esp.user_modf,esp.fec_modf
           from gec01.gc_conceptos_espana_circ esp, gec01.circuito cir
          where esp.cod_entidad = cir.cod_entidad
            and esp.ide_circuito = cir.ide_circuito;

    insert into ig_circ_asociacion (cod_entidad,ide_circuito,cod_asoc,est_circ_asoc,user_alt_circ_asoc,fec_alt_circ_asoc,user_modf_circ_asoc,fec_modf_circ_asoc)
         select asoc.cod_entidad,asoc.ide_circuito,asoc.cod_asoc,asoc.est_circ_asoc,asoc.user_alt_circ_asoc,asoc.fec_alt_circ_asoc,asoc.user_modf_circ_asoc,asoc.fec_modf_circ_asoc
           from gec01.circ_asociacion asoc, gec01.circuito cir
          where asoc.cod_entidad = cir.cod_entidad
            and asoc.ide_circuito = cir.ide_circuito;

    v_paso := 2;
    --Se almacena la informacion definitiva en la tabla de interfaz INT_CIRCUITO
    insert into int_circuitos
         select cir.ide_circuito,
                cir.fec_vig_dde_circ,
                cir.fec_vig_hta_circ,
                cir.desc_circ,
                cir.desc_detall_circ,
                decode(cir.cod_prod,null,99999,cir.cod_prod),
                decode(prod.desc_prod,null,'NO DEFINIDO',prod.desc_prod),
                decode(cir.cod_subprod,null,99999,cir.cod_subprod),
                decode(sprod.desc_subprod,null,'NO DEFINIDO',sprod.desc_subprod),
                decode(cir.cod_cpto,null,99999,cir.cod_cpto),
                decode(con.desc_cpto,null,'NO DEFINIDO',con.desc_cpto),
                decode(cir.cod_subcpto,null,99999,cir.cod_subcpto),
                decode(scon.desc_subcpto,null,'NO DEFINIDO',scon.desc_subcpto),
                decode(cir.tpo_gestion,null,99999,cir.tpo_gestion),
                decode(ges.desc_tpo_gest,null,'NO DEFINIDO',ges.desc_tpo_gest),
                cir.est_circ,
                null,
                0 cir_5_dias,
                null,
                null,
                cir.tmp_circ,
                asoc.cod_asoc
           from ig_circuito cir
           left outer join ig_producto prod on cir.cod_prod = prod.cod_prod and cir.cod_entidad = prod.cod_entidad
           left outer join ig_subproducto sprod on cir.cod_subprod = sprod.cod_subprod and cir.cod_entidad = sprod.cod_entidad
           left outer join ig_concepto con on cir.cod_cpto = con.cod_cpto and cir.cod_entidad = con.cod_entidad
           left outer join ig_subconcepto scon on cir.cod_subcpto = scon.cod_subcpto and cir.cod_entidad = scon.cod_entidad
           left outer join ig_tipo_gestion ges on cir.tpo_gestion = ges.tpo_gestion and cir.cod_entidad = ges.cod_entidad
           left outer join ig_circ_asociacion asoc on cir.cod_entidad = asoc.cod_entidad and cir.ide_circuito = asoc.ide_circuito;

    v_paso := 3;
    out_file := null;
    out_file := utl_file.fopen (v_path_utl, 'DIM_CIRCUITOS.txt', 'w',v_line_max);

    for x in (select * from int_circuitos) loop
      v_paso := 4;
      --Modificala el circuito en caso de que exista
      update dim_circuitos
         set cir_fec_desde = x.cir_fec_desde,
             cir_fec_hasta = x.cir_fec_hasta,
             cir_desc = x.cir_desc,
             cir_detalle = x.cir_detalle,
             cir_cod_prod = x.cir_cod_prod,
             cir_desc_prod = x.cir_desc_prod,
             cir_cod_subprod = x.cir_cod_subprod,
             cir_desc_subprod = x.cir_desc_subprod,
             cir_cod_cpto = x.cir_cod_cpto,
             cir_desc_cpto = x.cir_desc_cpto,
             cir_cod_subcpto = x.cir_cod_subcpto,
             cir_desc_subcpto = x.cir_desc_subcpto,
             cir_cod_tpo_ges = x.cir_cod_tpo_ges,
             cir_desc_tpo_ges = x.cir_desc_tpo_ges,
             cir_estado = x.cir_estado,
             cir_cod_cpto_bcra = x.cir_cod_cpto_bcra,
             cir_5_dias = x.cir_5_dias,
             cir_cod_cpto_sac = x.cir_cod_cpto_sac,
             cir_cod_cpto_esp = x.cir_cod_cpto_esp,
             cir_tmp_std = x.cir_tmp_std,
             cir_cod_asoc = x.cir_cod_asoc
       where cir_cod = x.cir_cod;

      if sql%notfound then
        v_paso := 5;
        --Inserta el circuito en caso de que no exista
        insert into dim_circuitos values (x.cir_cod,
                                          x.cir_fec_desde,
                                          x.cir_fec_hasta,
                                          x.cir_desc,
                                          x.cir_detalle,
                                          x.cir_cod_prod,
                                          x.cir_desc_prod,
                                          x.cir_cod_subprod,
                                          x.cir_desc_subprod,
                                          x.cir_cod_cpto,
                                          x.cir_desc_cpto,
                                          x.cir_cod_subcpto,
                                          x.cir_desc_subcpto,
                                          x.cir_cod_tpo_ges,
                                          x.cir_desc_tpo_ges,
                                          x.cir_estado,
                                          x.cir_cod_cpto_bcra,
                                          x.cir_5_dias,
                                          x.cir_cod_cpto_sac,
                                          x.cir_cod_cpto_esp,
                                          x.cir_tmp_std,
                                          x.cir_cod_asoc);
      end if;
    end loop;

    v_paso := 6;
    for y in (select ide_circuito, cod_cpto, 1 indicador from ig_gc_conceptos_bcra_circ union all
              select ide_circuito, cod_cpto, 2 indicador from ig_gc_conceptos_sac_circ  union all
              select ide_circuito, cod_cpto, 3 indicador from ig_gc_conceptos_espana_circ) loop

      if y.indicador = 1 then
        v_paso := 7;
        update dim_circuitos
           set cir_cod_cpto_bcra = y.cod_cpto
         where cir_cod = y.ide_circuito;
      elsif y.indicador = 2 then
        v_paso := 8;
        update dim_circuitos
           set cir_cod_cpto_sac = y.cod_cpto
         where cir_cod = y.ide_circuito;
      elsif y.indicador = 3 then
        v_paso := 9;
        update dim_circuitos
           set cir_cod_cpto_esp = y.cod_cpto
         where cir_cod = y.ide_circuito;
      end if;
    end loop;

    for cir in (select cir_cod,cir_fec_desde,cir_fec_hasta,cir_desc,cir_detalle,cir_cod_prod,cir_desc_prod,cir_cod_subprod,cir_desc_subprod,cir_cod_cpto,cir_desc_cpto,cir_cod_subcpto,cir_desc_subcpto,
                       cir_cod_tpo_ges,cir_desc_tpo_ges,cir_estado,cir_cod_cpto_bcra,cir_5_dias,cir_cod_cpto_sac,cir_cod_cpto_esp,cir_tmp_std,cir_cod_asoc
                  from gec02.dim_circuitos
                 where cir_cod in (select distinct cir_cod from gec02.int_circuitos)) loop
      v_paso := 10;
      utl_file.put_line (out_file, cir.cir_cod || v_separador ||
                                  cir.cir_fec_desde || v_separador ||
                                  cir.cir_fec_hasta || v_separador ||
                                  replace(replace(cir.cir_desc,chr(10),''),chr(13),'') || v_separador ||
                                  replace(replace(cir.cir_detalle,chr(10),''),chr(13),'') || v_separador ||
                                  cir.cir_cod_prod || v_separador ||
                                  cir.cir_desc_prod || v_separador ||
                                  cir.cir_cod_subprod || v_separador ||
                                  cir.cir_desc_subprod || v_separador ||
                                  cir.cir_cod_cpto || v_separador ||
                                  cir.cir_desc_cpto || v_separador ||
                                  cir.cir_cod_subcpto || v_separador ||
                                  cir.cir_desc_subcpto || v_separador ||
                                  cir.cir_cod_tpo_ges || v_separador ||
                                  cir.cir_desc_tpo_ges || v_separador ||
                                  cir.cir_estado || v_separador ||
                                  cir.cir_cod_cpto_bcra || v_separador ||
                                  cir.cir_5_dias || v_separador ||
                                  cir.cir_cod_cpto_sac || v_separador ||
                                  cir.cir_cod_cpto_esp || v_separador ||
                                  cir.cir_tmp_std || v_separador ||
                                  cir.cir_cod_asoc);
    end loop;

    v_paso := 11;
    utl_file.fclose(out_file);

    commit;

    v_paso := 12;
    v_fecha_fin := sysdate;
    pkg_gc_procesos.sp_procesos(2, 2, v_fecha_inicio, v_fecha_fin, id_proceso);
  exception
    when others then
      v_fecha_fin := sysdate;
      pkg_gc_procesos.sp_procesos(2, 6, v_fecha_inicio, v_fecha_fin, id_proceso);
      v_err_sql := substr (sqlerrm (sqlcode), 1, 250);
      pkg_gc_procesos.sp_inconsistencias(id_proceso, sysdate, v_paso, v_err_sql);
  end sp_circuitos;
*/

/*SP_INFO_REQUERIDA */
  procedure sp_info_requerida (id_proceso in number, fec_desde in date, fec_hasta in date) as
  begin
    v_fecha_inicio := sysdate;
    pkg_gc_procesos.sp_procesos(1, 1, v_fecha_inicio, null, id_proceso);

    v_paso := 1;
    --Se replican los datos en la tabla temporaltemporales involucradas en el calculo de la dimension DIM_INFO_REQUERIDA
      insert into ig_info_requerida (cod_entidad,cod_info_reque,desc_info_reque,cod_tpo_dat,long_info_reque,cant_dec_info_reque,rang_dde_info_reque,rang_hta_info_reque,fec_dde_info_reque,fec_hta_info_reque,est_info_reque,user_alt_info_reque,user_modf_info_reque,fec_alt_info_reque,fec_modf_info_reque,cod_tpo_campo,cod_funcion_asoc,cod_info_relac,tpo_info_especial,sector_owner,texto_ayuda,cod_info_condic,cod_valor_condic)
           select cod_entidad,cod_info_reque,desc_info_reque,cod_tpo_dat,long_info_reque,cant_dec_info_reque,rang_dde_info_reque,rang_hta_info_reque,fec_dde_info_reque,fec_hta_info_reque,est_info_reque,user_alt_info_reque,user_modf_info_reque,fec_alt_info_reque,fec_modf_info_reque,cod_tpo_campo,cod_funcion_asoc,cod_info_relac,tpo_info_especial,sector_owner,texto_ayuda,cod_info_condic,cod_valor_condic
             from gec01.info_requerida
            where fec_modf_info_reque between fec_desde and fec_hasta;

    v_paso := 2;
    --Se almacena la informacion definitiva en la tabla de interfaz INT_INFO_REQUERIDA
    insert into int_info_requerida
      select null,
             null,
             info.cod_info_reque,
             info.desc_info_reque,
             null,
             null,
             info.est_info_reque
        from ig_info_requerida info;

    v_paso := 3;
    out_file := null;
    out_file := utl_file.fopen (v_path_utl, 'DIM_INFO_REQUERIDA.txt', 'w',v_line_max);

    for x in (select * from int_info_requerida) loop
      v_paso := 4;
      --Modificala la info requerida en caso de que exista
      update dim_info_requerida
         set inf_desc_info = x.inf_desc_info,
             inf_estado = x.inf_estado
       where inf_cod_info_req = x.inf_cod_info_req;

      if sql%notfound then
        v_paso := 5;
        --Inserta la info requerida en caso de que no exista
        insert into dim_info_requerida
             values (x.inf_cod_circuito,
                     x.inf_indi_info_gpo,
                     x.inf_cod_info_req,
                     x.inf_desc_info,
                     x.inf_cod_etapa,
                     x.inf_orden_etapa,
                     x.inf_estado);
      end if;

      v_paso := 6;
      utl_file.put_line (out_file, x.inf_cod_circuito || v_separador ||
                                   x.inf_indi_info_gpo || v_separador ||
                                   x.inf_cod_info_req || v_separador ||
                                   x.inf_desc_info || v_separador ||
                                   x.inf_cod_etapa || v_separador ||
                                   x.inf_orden_etapa || v_separador ||
                                   x.inf_estado);
    end loop;

    v_paso := 7;
    utl_file.fclose(out_file);

    commit;

    v_paso := 8;
    v_fecha_fin := sysdate;
    pkg_gc_procesos.sp_procesos(2, 2, v_fecha_inicio, v_fecha_fin, id_proceso);
  exception
    when others then
      v_fecha_fin := sysdate;
      pkg_gc_procesos.sp_procesos(2, 6, v_fecha_inicio, v_fecha_fin, id_proceso);
      v_err_sql := substr (sqlerrm (sqlcode), 1, 250);
      pkg_gc_procesos.sp_inconsistencias(id_proceso, sysdate, v_paso, v_err_sql);
  end sp_info_requerida;

/******************************************************************************/
--------------------------- SP_INFO_REQUERIDA_VALORES---------------------------
/******************************************************************************/
  procedure sp_info_requerida_valores (id_proceso in number, fec_desde in date, fec_hasta in date) as
  begin
    v_fecha_inicio := sysdate;
    pkg_gc_procesos.sp_procesos(1, 1, v_fecha_inicio, null, id_proceso);

    v_paso := 1;
    --Se replican los datos en la tabla temporaltemporales involucradas en el calculo de la dimension DIM_INFO_REQUERIDA_VALORES
      insert into ig_info_requerida_valores (cod_entidad,cod_info_reque,cod_valor,nro_valor,desc_valor,est_valor,user_alt_valor,fec_alt_valor,user_modf_valor,fec_modf_valor,cod_info_reque_valores)
           select cod_entidad,cod_info_reque,cod_valor,nro_valor,desc_valor,est_valor,user_alt_valor,fec_alt_valor,user_modf_valor,fec_modf_valor,cod_info_reque_valores
             from gec01.info_requerida_valores
            where fec_modf_valor between fec_desde and fec_hasta;

    v_paso := 2;
    --Se almacena la informacion definitiva en la tabla de interfaz INT_INFO_REQUERIDA_VALORES
    insert into int_info_requerida_valores
      select cod_info_reque,
             cod_valor,
             nro_valor,
             desc_valor,
             cod_info_reque_valores
        from ig_info_requerida_valores;

    v_paso := 3;
    out_file := null;
    out_file := utl_file.fopen (v_path_utl, 'DIM_INFO_REQUERIDA_VALORES.txt', 'w',v_line_max);

    for x in (select * from int_info_requerida_valores) loop
      v_paso := 4;
      --Modificala el valor de la info requerida en caso de que exista
      update dim_info_requerida_valores
         set val_nro_valor = x.val_nro_valor,
             val_valor = x.val_valor,
             val_info_reque_valores = x.val_info_reque_valores
       where val_cod_info_reque = x.val_cod_info_reque
         and val_cod_valor = x.val_cod_valor;

      if sql%notfound then
        v_paso := 5;
        --Inserta el valor de la info requerida en caso de que no exista
        insert into dim_info_requerida_valores
             values (x.val_cod_info_reque,
                     x.val_cod_valor,
                     x.val_nro_valor,
                     x.val_valor,
                     x.val_info_reque_valores);
      end if;

      v_paso := 6;
      utl_file.put_line (out_file, x.val_cod_info_reque || v_separador ||
                                   x.val_cod_valor || v_separador ||
                                   x.val_nro_valor || v_separador ||
                                   x.val_valor || v_separador ||
                                   x.val_info_reque_valores);
    end loop;

    v_paso := 7;
    utl_file.fclose(out_file);

    commit;

    v_paso := 8;
    v_fecha_fin := sysdate;
    pkg_gc_procesos.sp_procesos(2, 2, v_fecha_inicio, v_fecha_fin, id_proceso);
  exception
    when others then
      v_fecha_fin := sysdate;
      pkg_gc_procesos.sp_procesos(2, 6, v_fecha_inicio, v_fecha_fin, id_proceso);
      v_err_sql := substr (sqlerrm (sqlcode), 1, 250);
      pkg_gc_procesos.sp_inconsistencias(id_proceso, sysdate, v_paso, v_err_sql);
  end sp_info_requerida_valores;

/******************************************************************************/
-------------------------------- SP_GPO_INFO_REQUERIDA------------------------------
/******************************************************************************/
  procedure sp_gpo_info_requerida (id_proceso in number, fec_desde in date, fec_hasta in date) as
  begin
    v_fecha_inicio := sysdate;
    pkg_gc_procesos.sp_procesos(1, 1, v_fecha_inicio, null, id_proceso);

    v_paso := 1;
    --Se replican los datos en la tabla temporaltemporales involucradas en el calculo de la dimension DIM_GPO_INFO_REQUERIDA
      insert into ig_info_reque_grupo (cod_entidad,cod_gpo_info_reque,cod_info_reque,est_info_reque_gpo,user_alt_gpo_info,fec_alt_gpo_info,user_modf_gpo_info,fec_modf_gpo_info,orden_info)
           select cod_entidad,cod_gpo_info_reque,cod_info_reque,est_info_reque_gpo,user_alt_gpo_info,fec_alt_gpo_info,user_modf_gpo_info,fec_modf_gpo_info,orden_info
             from gec01.info_reque_grupo
            where fec_modf_gpo_info between fec_desde and fec_hasta;

      insert into ig_gpo_info_reque (cod_entidad,cod_gpo_info_reque,desc_gpo_info_reque,est_gpo_info_reque,user_alt_gpo_info_reque,fec_alt_gpo_info_reque,user_modf_gpo_info_reque,fec_modf_gpo_info_reque,sector_owner)
           select cod_entidad,cod_gpo_info_reque,desc_gpo_info_reque,est_gpo_info_reque,user_alt_gpo_info_reque,fec_alt_gpo_info_reque,user_modf_gpo_info_reque,fec_modf_gpo_info_reque,sector_owner
             from gec01.gpo_info_reque;

    v_paso := 2;
    --Se almacena la informacion definitiva en la tabla de interfaz INT_GPO_INFO_REQUERIDA
    insert into int_gpo_info_requerida
      select inf.cod_gpo_info_reque,
             gpo.desc_gpo_info_reque,
             inf.cod_info_reque,
             inf.est_info_reque_gpo
        from ig_info_reque_grupo inf,
             ig_gpo_info_reque gpo
       where inf.cod_entidad = gpo.cod_entidad(+)
         and inf.cod_gpo_info_reque = gpo.cod_gpo_info_reque(+);

    v_paso := 3;
    out_file := null;
    out_file := utl_file.fopen (v_path_utl, 'DIM_GPO_INFO_REQUERIDA.txt', 'w',v_line_max);

    for x in (select * from int_gpo_info_requerida) loop
      v_paso := 4;
      --Modificala el grupo de info requerida en caso de que exista
      update dim_gpo_info_requerida
         set gin_cod_info_reque = x.gin_cod_info_reque,
             gin_desc_grupo = x.gin_desc_grupo,
             gin_estado = x.gin_estado
       where gin_cod_grupo = x.gin_cod_grupo
         and gin_cod_info_reque = x.gin_cod_info_reque;

      if sql%notfound then
        v_paso := 6;
        --Inserta el grupo de info requerida en caso de que no exista
        insert into dim_gpo_info_requerida
             values (x.gin_cod_grupo,
                     x.gin_desc_grupo,
                     x.gin_cod_info_reque,
                     x.gin_estado);
      end if;

      v_paso := 6;
      utl_file.put_line (out_file, x.gin_cod_grupo || v_separador ||
                                   x.gin_desc_grupo || v_separador ||
                                   x.gin_cod_info_reque || v_separador ||
                                   x.gin_estado);
    end loop;

    v_paso := 7;
    utl_file.fclose(out_file);

    commit;

    v_paso := 4;
    v_fecha_fin := sysdate;
    pkg_gc_procesos.sp_procesos(2, 2, v_fecha_inicio, v_fecha_fin, id_proceso);
  exception
    when others then
      v_fecha_fin := sysdate;
      pkg_gc_procesos.sp_procesos(2, 6, v_fecha_inicio, v_fecha_fin, id_proceso);
      v_err_sql := substr (sqlerrm (sqlcode), 1, 250);
      pkg_gc_procesos.sp_inconsistencias(id_proceso, sysdate, v_paso, v_err_sql);
  end sp_gpo_info_requerida;

/******************************************************************************/
---------------------------- SP_DOC_REQUERIDA ----------------------------------
/******************************************************************************/
  procedure sp_doc_requerida (id_proceso in number, fec_desde in date, fec_hasta in date) as
  begin
    v_fecha_inicio := sysdate;
    pkg_gc_procesos.sp_procesos(1, 1, v_fecha_inicio, null, id_proceso);

    v_paso := 1;
    --Se replican los datos en las tablas temporales IG_DOC_REQUERIDA y IG_CIRC_DOC_REQ
    insert into ig_circ_doc_reque (cod_entidad,ide_circuito,cod_doc_req,indi_oblig_doc_reque,est_circ_doc_req,user_alt_circ_doc_req,fec_alt_circ_doc_req,user_modf_circ_doc_req,fec_modf_circ_doc_req,cod_etapa,orden_etapa,orden_doc)
         select cod_entidad,ide_circuito,cod_doc_req,indi_oblig_doc_reque,est_circ_doc_req,user_alt_circ_doc_req,fec_alt_circ_doc_req,user_modf_circ_doc_req,fec_modf_circ_doc_req,cod_etapa,orden_etapa,orden_doc
           from gec01.circ_doc_reque
          where fec_modf_circ_doc_req between fec_desde and fec_hasta;

    insert into ig_doc_requerida (cod_entidad,cod_doc_req,desc_doc_req,est_doc_req,user_alt_doc_req,fec_alt_doc_req,user_modf_doc_req,fec_modf_doc_req,sector_owner)
         select cod_entidad,cod_doc_req,desc_doc_req,est_doc_req,user_alt_doc_req,fec_alt_doc_req,user_modf_doc_req,fec_modf_doc_req,sector_owner
           from gec01.doc_requerida;

    v_paso := 2;
    --Se almacena la informacion definitiva en la tabla de interfaz INT_DOC_REQUERIDA
    insert into int_doc_requerida
      select distinct circ.ide_circuito,
                      decode(doc.cod_doc_req,null,99999,doc.cod_doc_req),
                      decode(doc.desc_doc_req,null,'NO DEFINIDO',doc.desc_doc_req),
                      circ.indi_oblig_doc_reque,
                      circ.cod_etapa,
                      circ.orden_etapa,
                      decode(doc.est_doc_req,null,'F',doc.est_doc_req)
        from ig_circ_doc_reque circ
        left outer join ig_doc_requerida doc on circ.cod_doc_req = doc.cod_doc_req;

    v_paso := 3;
    out_file := null;
    out_file := utl_file.fopen (v_path_utl, 'DIM_DOC_REQUERIDA.txt', 'w',v_line_max);

    for x in (select * from int_doc_requerida) loop
      v_paso := 4;
      --Modificala la documenteacion requerida en caso de que exista
      update dim_doc_requerida
         set doc_cod_document = x.doc_cod_document,
             doc_desc = x.doc_desc,
             doc_obligatorio = x.doc_obligatorio,
             doc_cod_etapa = x.doc_cod_etapa,
             doc_orden_etapa = x.doc_orden_etapa,
             doc_estado = x.doc_estado
       where doc_cod_circuito = x.doc_cod_circuito
         and doc_cod_document = x.doc_cod_document
         and doc_cod_etapa = x.doc_cod_etapa
         and doc_orden_etapa = x.doc_orden_etapa;

      if sql%notfound then
        v_paso := 5;
        --Inserta la documenteacion requerida en caso de que no exista
        insert into dim_doc_requerida
             values (x.doc_cod_circuito,
                    x.doc_cod_document,
                    x.doc_desc,
                    x.doc_obligatorio,
                    x.doc_cod_etapa,
                    x.doc_orden_etapa,
                    x.doc_estado);
      end if;

      v_paso := 6;
      utl_file.put_line (out_file, x.doc_cod_circuito || v_separador ||
                                   x.doc_cod_document || v_separador ||
                                   x.doc_desc || v_separador ||
                                   x.doc_obligatorio || v_separador ||
                                   x.doc_cod_etapa || v_separador ||
                                   x.doc_orden_etapa || v_separador ||
                                   x.doc_estado);
    end loop;

    v_paso := 7;
    utl_file.fclose(out_file);

    commit;

    v_paso := 8;
    v_fecha_fin := sysdate;
    pkg_gc_procesos.sp_procesos(2, 2, v_fecha_inicio, v_fecha_fin, id_proceso);
  exception
    when others then
      v_fecha_fin := sysdate;
      pkg_gc_procesos.sp_procesos(2, 6, v_fecha_inicio, v_fecha_fin, id_proceso);
      v_err_sql := substr (sqlerrm (sqlcode), 1, 250);
      pkg_gc_procesos.sp_inconsistencias(id_proceso, sysdate, v_paso, v_err_sql);
  end sp_doc_requerida;

/******************************************************************************/
--------------------------------- SP_ETAPAS ------------------------------------
/******************************************************************************/
  procedure sp_etapas (id_proceso in number, fec_desde in date, fec_hasta in date) as
  begin
    v_fecha_inicio := sysdate;
    pkg_gc_procesos.sp_procesos(1, 1, v_fecha_inicio, null, id_proceso);

    v_paso := 1;
    --Se replican los datos en la tabla temporal IG_ETAPAS
    insert into ig_etapas (cod_entidad,cod_etapa,desc_etapa,desc_etapa_2)
         select cod_entidad,cod_etapa,desc_etapa,desc_etapa_2
           from gec01.etapas;

    v_paso := 2;
    --Se almacena la informacion definitiva en la tabla de interfaz INT_ETAPAS
    insert into int_etapas
      select cod_etapa, desc_etapa, desc_etapa_2 from ig_etapas;

    v_paso := 3;
    out_file := null;
    out_file := utl_file.fopen (v_path_utl, 'DIM_ETAPAS.txt', 'w',v_line_max);

    for x in (select * from int_etapas) loop
      v_paso := 4;
      --Modifica las etapas en caso de que exista
      update dim_etapas
         set etp_desc = x.etp_desc,
             etp_desc_detalle = x.etp_desc_detalle
       where etp_cod = x.etp_cod;

      if sql%notfound then
        v_paso := 5;
        --Inserta las etapas en caso de que no exista
        insert into dim_etapas
             values (x.etp_cod, x.etp_desc, x.etp_desc_detalle);
      end if;

      v_paso := 6;
      utl_file.put_line (out_file, x.etp_cod || v_separador || x.etp_desc || v_separador || x.etp_desc_detalle);

    end loop;

    v_paso := 7;
    utl_file.fclose(out_file);

    commit;

    v_paso := 8;
    v_fecha_fin := sysdate;
    pkg_gc_procesos.sp_procesos(2, 2, v_fecha_inicio, v_fecha_fin, id_proceso);
  exception
    when others then
      v_fecha_fin := sysdate;
      pkg_gc_procesos.sp_procesos(2, 6, v_fecha_inicio, v_fecha_fin, id_proceso);
      v_err_sql := substr (sqlerrm (sqlcode), 1, 250);
      pkg_gc_procesos.sp_inconsistencias(id_proceso, sysdate, v_paso, v_err_sql);
  end sp_etapas;

/******************************************************************************/
------------------------------ SP_TIPO_MEDIOS ----------------------------------
/******************************************************************************/
  procedure sp_tipo_medios (id_proceso in number, fec_desde in date, fec_hasta in date) as
  begin
    v_fecha_inicio := sysdate;
    pkg_gc_procesos.sp_procesos(1, 1, v_fecha_inicio, null, id_proceso);

    v_paso := 1;
    --Se replican los datos en la tabla  IG_TIPO_MEDIOS sin filtros
      insert into ig_tipo_medios (cod_entidad,tpo_medio,desc_medio,desc_detall_medio,indi_tpo_medio,est_medio,user_alt_med,fec_alt_med,user_modf_med,fec_modf_med,sector_owner,indi_visible,indi_msj_asoc)
           select cod_entidad,tpo_medio,desc_medio,desc_detall_medio,indi_tpo_medio,est_medio,user_alt_med,fec_alt_med,user_modf_med,fec_modf_med,sector_owner,indi_visible,indi_msj_asoc
             from gec01.tipo_medios
            where fec_modf_med between fec_desde and fec_hasta;

    v_paso := 2;
    --Se almacena la informacion definitiva en la tabla de interfaz INT_TIPO_MEDIOS
    insert into int_tipo_medios
      select tpo_medio, desc_medio, desc_detall_medio, est_medio from ig_tipo_medios;

    v_paso := 3;
    out_file := null;
    out_file := utl_file.fopen (v_path_utl, 'DIM_TIPO_MEDIOS.txt', 'w',v_line_max);

    for x in (select * from int_tipo_medios) loop
      v_paso := 4;
      --Modifica los tipo medios en caso de que exista
      update dim_tipo_medios
         set tme_desc = x.tme_desc,
             tme_detalle = x.tme_detalle,
             tme_estado = x.tme_estado
       where tme_cod = x.tme_cod;

      if sql%notfound then
        v_paso := 5;
        --Inserta los tipo medios en caso de que no exista
        insert into dim_tipo_medios
             values (x.tme_cod, x.tme_desc, x.tme_detalle, x.tme_estado);
      end if;

      v_paso := 6;
      utl_file.put_line (out_file, x.tme_cod || v_separador || x.tme_desc || v_separador || x.tme_detalle || v_separador || x.tme_estado);

    end loop;

    v_paso := 7;
    utl_file.fclose(out_file);

    commit;

    v_paso := 8;
    v_fecha_fin := sysdate;
    pkg_gc_procesos.sp_procesos(2, 2, v_fecha_inicio, v_fecha_fin, id_proceso);
  exception
    when others then
      v_fecha_fin := sysdate;
      pkg_gc_procesos.sp_procesos(2, 6, v_fecha_inicio, v_fecha_fin, id_proceso);
      v_err_sql := substr (sqlerrm (sqlcode), 1, 250);
      pkg_gc_procesos.sp_inconsistencias(id_proceso, sysdate, v_paso, v_err_sql);
  end sp_tipo_medios;

/******************************************************************************/
-------------------------------- SP_TIPO_MAIL ----------------------------------
/******************************************************************************/
  procedure sp_tipo_mail (id_proceso in number, fec_desde in date, fec_hasta in date) as
  begin
    v_fecha_inicio := sysdate;
    pkg_gc_procesos.sp_procesos(1, 1, v_fecha_inicio, null, id_proceso);

    v_paso := 1;
    --Se replican los datos en la tabla temporal IG_TIPO_MAIL
    insert into ig_tipo_mail (cod_entidad,cod_tipo_mail,desc_tipo_mail)
         select cod_entidad,cod_tipo_mail,desc_tipo_mail
           from gec01.tipo_mail;

    v_paso := 2;
    --Se almacena la informacion definitiva en la tabla de interfaz INT_TIPO_MAIL
    insert into int_tipo_mail select cod_tipo_mail, desc_tipo_mail from ig_tipo_mail;

    v_paso := 3;
    out_file := null;
    out_file := utl_file.fopen (v_path_utl, 'DIM_TIPO_MAIL.txt', 'w',v_line_max);

    for x in (select * from int_tipo_mail) loop
      v_paso := 4;
      --Modifica el tipo de mail en caso de que exista
      update dim_tipo_mail
         set tma_desc = x.tma_desc
       where tma_cod = x.tma_cod;

      if sql%notfound then
        v_paso := 5;
        --Inserta el tipo de mail en caso de que no exista
        insert into dim_tipo_mail
             values (x.tma_cod, x.tma_desc);
      end if;

      v_paso := 6;
      utl_file.put_line (out_file, x.tma_cod || v_separador || x.tma_desc);

    end loop;

    v_paso := 7;
    utl_file.fclose(out_file);

    commit;

    v_paso := 8;
    v_fecha_fin := sysdate;
    pkg_gc_procesos.sp_procesos(2, 2, v_fecha_inicio, v_fecha_fin, id_proceso);
  exception
    when others then
      v_fecha_fin := sysdate;
      pkg_gc_procesos.sp_procesos(2, 6, v_fecha_inicio, v_fecha_fin, id_proceso);
      v_err_sql := substr (sqlerrm (sqlcode), 1, 250);
      pkg_gc_procesos.sp_inconsistencias(id_proceso, sysdate, v_paso, v_err_sql);
  end sp_tipo_mail;

/******************************************************************************/
---------------------------- SP_MAILS_GESTIONES --------------------------------
/******************************************************************************/
  procedure sp_mails_gestiones (id_proceso in number, fec_desde in date, fec_hasta in date) as
  begin
    v_fecha_inicio := sysdate;
    pkg_gc_procesos.sp_procesos(1, 1, v_fecha_inicio, null, id_proceso);

    v_paso := 1;
    --Se replican los datos en la tabla temporal IG_MAILS_GESTION
    insert into ig_mails_gestion (cod_entidad,ide_gestion_sector,ide_gestion_nro,sec_mail,tipo_mail,sec_mail_relac,ide_tarea_mail,user_envio,fec_envio,sector_envio,comen_mail,dire_mail_contc,ind_visible)
         select cod_entidad,ide_gestion_sector,ide_gestion_nro,sec_mail,tipo_mail,sec_mail_relac,ide_tarea_mail,user_envio,fec_envio,sector_envio,comen_mail,dire_mail_contc,ind_visible
           from gec01.mails_gestion
          where fec_envio between fec_desde and fec_hasta;

    v_paso := 2;
    --Se almacena la informacion definitiva en la tabla de interfaz INT_MAILS_GESTIONES
    insert into int_mails_gestiones
      select ide_gestion_nro,
             sec_mail,
             tipo_mail,
             fec_envio,
             sector_envio
        from ig_mails_gestion;

    v_paso := 3;
    out_file := null;
    out_file := utl_file.fopen (v_path_utl, 'DIM_MAILS_GESTIONES.txt', 'w',v_line_max);

    for x in (select * from int_mails_gestiones) loop
      v_paso := 4;
      --Modifica el mail en caso de que exista
      update dim_mails_gestiones
         set mge_cod_mail = x.mge_cod_mail,
             mge_fec_envio = x.mge_fec_envio,
             mge_sector_envio = x.mge_sector_envio
       where mge_nro_gestion = x.mge_nro_gestion
         and mge_sec_mail = x.mge_sec_mail;

      if sql%notfound then
        v_paso := 5;
        --Inserta el mail en caso de que no exista
        insert into dim_mails_gestiones
             values (x.mge_nro_gestion, x.mge_sec_mail, x.mge_cod_mail, x.mge_fec_envio, x.mge_sector_envio);
      end if;

      v_paso := 6;
      utl_file.put_line (out_file, x.mge_nro_gestion || v_separador || x.mge_sec_mail || v_separador || x.mge_cod_mail || v_separador || x.mge_fec_envio || v_separador || x.mge_sector_envio);

    end loop;

    v_paso := 7;
    utl_file.fclose(out_file);

    commit;

    v_paso := 8;
    v_fecha_fin := sysdate;
    pkg_gc_procesos.sp_procesos(2, 2, v_fecha_inicio, v_fecha_fin, id_proceso);
  exception
    when others then
      v_fecha_fin := sysdate;
      pkg_gc_procesos.sp_procesos(2, 6, v_fecha_inicio, v_fecha_fin, id_proceso);
      v_err_sql := substr (sqlerrm (sqlcode), 1, 250);
      pkg_gc_procesos.sp_inconsistencias(id_proceso, sysdate, v_paso, v_err_sql);
  end sp_mails_gestiones;

/******************************************************************************/
-------------------------------- SP_MODELO_CARTAS-------------------------------
/******************************************************************************/
  procedure sp_modelo_cartas (id_proceso in number, fec_desde in date, fec_hasta in date) as
  begin
    v_fecha_inicio := sysdate;
    pkg_gc_procesos.sp_procesos(1, 1, v_fecha_inicio, null, id_proceso);

    v_paso := 1;
    --Se replican los datos en la tabla  IG_MODELOS_CARTA_RECIBIDO
      insert into ig_modelos_carta_recibo (cod_entidad,cod_cart_rec,desc_cart_rec,desc_detall_cart_rec,indi_cart_rec,encab_cart_rec,crpo_cart_rec,pie_cart_rec,est_cart_rec,user_alt_cart_rec,fec_alt_cart_rec,user_modf_cart_rec,fec_modf_cart_rec,cod_actor,sector_owner)
           select cod_entidad,cod_cart_rec,desc_cart_rec,desc_detall_cart_rec,indi_cart_rec,encab_cart_rec,crpo_cart_rec,pie_cart_rec,est_cart_rec,user_alt_cart_rec,fec_alt_cart_rec,user_modf_cart_rec,fec_modf_cart_rec,cod_actor,sector_owner
             from gec01.modelos_carta_recibo
            where fec_modf_cart_rec between fec_desde and fec_hasta ;

    v_paso := 2;
    --Se almacena la informacion definitiva en la tabla de interfaz INT_MODELO_CARTAS
    insert into int_modelo_cartas
      select cod_cart_rec, desc_cart_rec, desc_detall_cart_rec, est_cart_rec from ig_modelos_carta_recibo;

    v_paso := 3;
    out_file := null;
    out_file := utl_file.fopen (v_path_utl, 'DIM_MODELO_CARTAS.txt', 'w',v_line_max);

    for x in (select * from int_modelo_cartas) loop
      v_paso := 4;
      --Modifica el modelo carta en caso de que exista
      update dim_modelo_cartas
         set mcar_des = x.mcar_des,
             mcar_detalle = x.mcar_detalle,
             mcar_estado = x.mcar_estado
       where mcar_cod = x.mcar_cod;

      if sql%notfound then
        v_paso := 5;
        --Inserta el modelo carta en caso de que no exista
        insert into dim_modelo_cartas
             values (x.mcar_cod, x.mcar_des, x.mcar_detalle, x.mcar_estado);
      end if;

      v_paso := 6;
      utl_file.put_line (out_file, x.mcar_cod || v_separador || x.mcar_des || v_separador || replace(replace(x.mcar_detalle,chr(10),''),chr(13),'')|| v_separador || x.mcar_estado);

    end loop;

    v_paso := 7;
    utl_file.fclose(out_file);

    commit;

    v_paso := 8;
    v_fecha_fin := sysdate;
    pkg_gc_procesos.sp_procesos(2, 2, v_fecha_inicio, v_fecha_fin, id_proceso);
  exception
    when others then
      v_fecha_fin := sysdate;
      pkg_gc_procesos.sp_procesos(2, 6, v_fecha_inicio, v_fecha_fin, id_proceso);
      v_err_sql := substr (sqlerrm (sqlcode), 1, 250);
      pkg_gc_procesos.sp_inconsistencias(id_proceso, sysdate, v_paso, v_err_sql);
  end sp_modelo_cartas;

/******************************************************************************/
---------------------------- SP_RESPONSABILIDAD --------------------------------
/******************************************************************************/
  procedure sp_responsabilidad (id_proceso in number, fec_desde in date, fec_hasta in date) as
  begin
    v_fecha_inicio := sysdate;
    pkg_gc_procesos.sp_procesos(1, 1, v_fecha_inicio, null, id_proceso);

    v_paso := 1;
    --Se replican los datos en la tabla IG_RESPONSABILIDAD
      insert into ig_responsabilidad (cod_entidad,cod_responsab,desc_responsab,est_responsab,user_alt_responsab,fec_alt_responsab,user_modf_responsab,fec_modf_responsab,sector_owner)
           select cod_entidad,cod_responsab,desc_responsab,est_responsab,user_alt_responsab,fec_alt_responsab,user_modf_responsab,fec_modf_responsab,sector_owner
             from gec01.responsabilidad
            where fec_modf_responsab between fec_desde and fec_hasta;

    v_paso := 2;
    --Se almacena la informacion definitiva en la tabla de interfaz INT_TIPO_MEDIOS
    insert into int_responsabilidad
      select cod_responsab, desc_responsab, est_responsab from ig_responsabilidad;

    v_paso := 3;
    out_file := null;
    out_file := utl_file.fopen (v_path_utl, 'DIM_RESPONSABILIDAD.txt', 'w',v_line_max);

    for x in (select * from int_responsabilidad) loop
      v_paso := 4;
      --Modifica la responsabilidad en caso de que exista
      update dim_responsabilidad
         set res_desc = x.res_desc,
             res_estado = x.res_estado
       where res_cod = x.res_cod;

      if sql%notfound then
        v_paso := 5;
        --Inserta la responsabilidad en caso de que no exista
        insert into dim_responsabilidad
             values (x.res_cod, x.res_desc, x.res_estado);
      end if;

      v_paso := 6;
      utl_file.put_line (out_file, x.res_cod || v_separador || x.res_desc || v_separador || x.res_estado);

    end loop;

    v_paso := 7;
    utl_file.fclose(out_file);

    commit;

    v_paso := 8;
    v_fecha_fin := sysdate;
    pkg_gc_procesos.sp_procesos(2, 2, v_fecha_inicio, v_fecha_fin, id_proceso);
  exception
    when others then
      v_fecha_fin := sysdate;
      pkg_gc_procesos.sp_procesos(2, 6, v_fecha_inicio, v_fecha_fin, id_proceso);
      v_err_sql := substr (sqlerrm (sqlcode), 1, 250);
      pkg_gc_procesos.sp_inconsistencias(id_proceso, sysdate, v_paso, v_err_sql);
  end sp_responsabilidad;

/******************************************************************************/
---------------------------- SP_CENTRO_COSTOS ----------------------------------
/******************************************************************************/
  procedure sp_centro_costos (id_proceso in number, fec_desde in date, fec_hasta in date) as
  begin
    v_fecha_inicio := sysdate;
    pkg_gc_procesos.sp_procesos(1, 1, v_fecha_inicio, null, id_proceso);

    v_paso := 1;
    --Se replican los datos en la tabla IG_CCOSTOS sin filtros
      insert into ig_ccostos (cod_entidad,cod_cc,desc_cc,cod_grupo_empresa,user_alt_ccostos,fec_alt_ccostos,user_modf_ccostos,fec_modf_ccostos)
           select cod_entidad,cod_cc,desc_cc,cod_grupo_empresa,user_alt_ccostos,fec_alt_ccostos,user_modf_ccostos,fec_modf_ccostos
             from gec01.ccostos
            where fec_modf_ccostos between fec_desde and fec_hasta ;

    v_paso := 2;
    --Se almacena la informacion definitiva en la tabla de interfaz INT_CCOSTOS
    insert into int_ccostos
      select cod_cc, desc_cc from ig_ccostos;

    v_paso := 3;
    out_file := null;
    out_file := utl_file.fopen (v_path_utl, 'DIM_CCOSTOS.txt', 'w',v_line_max);

    for x in (select * from int_ccostos) loop
      v_paso := 4;
      --Modifica del centro de costos en caso de que exista
      update dim_ccostos
         set cco_desc = x.cco_desc
       where cco_cod = x.cco_cod;

      if sql%notfound then
        v_paso := 5;
        --Insertadel centro de costos en caso de que no exista
        insert into dim_ccostos
             values (x.cco_cod, x.cco_desc);
      end if;

      v_paso := 6;
      utl_file.put_line (out_file, x.cco_cod || v_separador || x.cco_desc);

    end loop;

    v_paso := 7;
    utl_file.fclose(out_file);

    commit;

    v_paso := 8;
    v_fecha_fin := sysdate;
    pkg_gc_procesos.sp_procesos(2, 2, v_fecha_inicio, v_fecha_fin, id_proceso);
  exception
    when others then
      v_fecha_fin := sysdate;
      pkg_gc_procesos.sp_procesos(2, 6, v_fecha_inicio, v_fecha_fin, id_proceso);
      v_err_sql := substr (sqlerrm (sqlcode), 1, 250);
      pkg_gc_procesos.sp_inconsistencias(id_proceso, sysdate, v_paso, v_err_sql);
  end sp_centro_costos;

/******************************************************************************/
------------------------------- SP_AUTORIZANTES --------------------------------
/******************************************************************************/
  procedure sp_autorizantes (id_proceso in number, fec_desde in date, fec_hasta in date) as
  begin
    v_fecha_inicio := sysdate;
    pkg_gc_procesos.sp_procesos(1, 1, v_fecha_inicio, null, id_proceso);

    v_paso := 1;
    --Se replican los datos en las tablas involucradas en el calculo de la dimension autorizantes
    insert into ig_circ_autorizante (cod_entidad,ide_circuito,cod_sector,cod_perfil,nro_ord_autz,imp_min_autz,imp_max_autz,cod_prioridad,tmp_circ_autz,est_circ_autz,user_alt_circ_autz,fec_alt_circ_autz,user_modf_circ_autz,fec_modf_circ_autz,indi_selec_autz,indi_mail_autz)
         select cod_entidad,ide_circuito,cod_sector,cod_perfil,nro_ord_autz,imp_min_autz,imp_max_autz,cod_prioridad,tmp_circ_autz,est_circ_autz,user_alt_circ_autz,fec_alt_circ_autz,user_modf_circ_autz,fec_modf_circ_autz,indi_selec_autz,indi_mail_autz
           from gec01.circ_autorizante
          where fec_modf_circ_autz between fec_desde and fec_hasta;

    insert into ig_usuarios (cod_entidad,cod_user,ape_user,nom_user,cod_perfil,fec_dde_perfil,fec_hta_perfil,mail_user,fec_alt_user,fec_baj_user,user_modf_user,fec_modf_user,enviar_mail,inic_user)
         select cod_entidad,cod_user,ape_user,nom_user,cod_perfil,fec_dde_perfil,fec_hta_perfil,mail_user,fec_alt_user,fec_baj_user,user_modf_user,fec_modf_user,enviar_mail,inic_user
           from gec01.usuarios;

    insert into ig_perfiles (cod_entidad,cod_perfil,desc_perf,indi_impre_masiva,indi_rta_masiva,indi_impre_carta,est_perf,user_alt_perf,fec_alt_perf,user_modf_perf,fec_modf_perf,indi_auto_acm,indi_distribucion,indi_atencion_telef,nro_jerarquia,sector_owner)
         select cod_entidad,cod_perfil,desc_perf,indi_impre_masiva,indi_rta_masiva,indi_impre_carta,est_perf,user_alt_perf,fec_alt_perf,user_modf_perf,fec_modf_perf,indi_auto_acm,indi_distribucion,indi_atencion_telef,nro_jerarquia,sector_owner
           from gec01.perfiles;

    insert into ig_sector_usuario (cod_entidad,cod_sector,cod_user,indi_sector_x_defecto)
         select cod_entidad,cod_sector,cod_user,indi_sector_x_defecto
           from gec01.sector_usuario;

    v_paso := 2;
    --Se almacena la informacion definitiva en la tabla de interfaz INT_AUTORIZANTE
    insert into int_autorizantes
      select distinct decode(circ.ide_circuito,null,99999,circ.ide_circuito) circuito,
             decode(usu.cod_user,null,'NO DEFINIDO',usu.cod_user) cod_usuario,
             decode(usu.ape_user,null,'NO DEFINIDO',usu.ape_user) apellido_usuario,
             decode(usu.nom_user,null,'NO DEFINIDO',usu.nom_user) nombre_usuario,
             decode(perf.cod_perfil,null,'ND',perf.cod_perfil) cod_perfil,
             decode(perf.desc_perf,null,'NO DEFINIDO',perf.desc_perf) perfil,
             decode(usu.mail_user,null,'NO DEFINIDO',usu.mail_user) mail,
             decode(sec.cod_sector,null,'NO DEFINIDO',sec.cod_sector) sector,
             decode(usu.fec_baj_user, null, 'A', 'I') estado,
             circ.nro_ord_autz,
             circ.imp_min_autz
        from ig_circ_autorizante circ,
             ig_sector_usuario sec,
             ig_perfiles perf,
             ig_usuarios usu
       where circ.cod_entidad = sec.cod_entidad(+)
         and circ.cod_sector = sec.cod_sector(+)
         and circ.cod_entidad = perf.cod_entidad
         and circ.cod_perfil = perf.cod_perfil
         and sec.cod_entidad = usu.cod_entidad(+)
         and sec.cod_user = usu.cod_user(+);

    v_paso := 3;
    out_file := null;
    out_file := utl_file.fopen (v_path_utl, 'DIM_AUTORIZANTES.txt', 'w',v_line_max);

    for x in (select * from int_autorizantes) loop
      v_paso := 4;
      --Modifica del autorizante del circuito en caso de que exista
      update dim_autorizantes
         set aut_cod_user = x.aut_cod_user,
             aut_ape_user = x.aut_ape_user

       where aut_cod_circuito = x.aut_cod_circuito;

      if sql%notfound then
        v_paso := 5;
        --Inserta del autorizante del circuito en caso de que no exista
        insert into dim_autorizantes
             values (x.aut_cod_circuito,
                    x.aut_cod_user,
                    x.aut_ape_user,
                    x.aut_nom_user,
                    x.aut_cod_perfil,
                    x.aut_desc_perfil,
                    x.aut_mail,
                    x.aut_cod_sector,
                    x.aut_estado,
                    x.aut_nro_ord,
                    x.aut_min_autz);
      end if;

      v_paso := 6;
      utl_file.put_line (out_file, x.aut_cod_circuito || v_separador ||
                                    x.aut_cod_user || v_separador ||
                                    x.aut_ape_user || v_separador ||
                                    x.aut_nom_user || v_separador ||
                                    x.aut_cod_perfil || v_separador ||
                                    x.aut_desc_perfil || v_separador ||
                                    x.aut_mail || v_separador ||
                                    x.aut_cod_sector || v_separador ||
                                    x.aut_estado || v_separador ||
                                    x.aut_nro_ord || v_separador ||
                                    x.aut_min_autz);

    end loop;

    v_paso := 7;
    utl_file.fclose(out_file);

    commit;

    v_paso := 8;
    v_fecha_fin := sysdate;
    pkg_gc_procesos.sp_procesos(2, 2, v_fecha_inicio, v_fecha_fin, id_proceso);
  exception
    when others then
      v_fecha_fin := sysdate;
      pkg_gc_procesos.sp_procesos(2, 6, v_fecha_inicio, v_fecha_fin, id_proceso);
      v_err_sql := substr (sqlerrm (sqlcode), 1, 250);
      pkg_gc_procesos.sp_inconsistencias(id_proceso, sysdate, v_paso, v_err_sql);
  end sp_autorizantes;

/******************************************************************************/
---------------------------------- SP_USUARIOS ---------------------------------
/******************************************************************************/
  procedure sp_usuarios (id_proceso in number, fec_desde in date, fec_hasta in date) as
  begin
    v_fecha_inicio := sysdate;
    pkg_gc_procesos.sp_procesos(1, 1, v_fecha_inicio, null, id_proceso);

    v_paso := 1;
    --Se replican los datos en las tablas involucradas en el calculo de la dimension usuarios
    insert into ig_usuarios (cod_entidad,cod_user,ape_user,nom_user,cod_perfil,fec_dde_perfil,fec_hta_perfil,mail_user,fec_alt_user,fec_baj_user,user_modf_user,fec_modf_user,enviar_mail,inic_user)
         select cod_entidad,cod_user,ape_user,nom_user,cod_perfil,fec_dde_perfil,fec_hta_perfil,mail_user,fec_alt_user,fec_baj_user,user_modf_user,fec_modf_user,enviar_mail,inic_user
           from gec01.usuarios
          where fec_modf_user between fec_desde and fec_hasta;

    insert into ig_perfiles (cod_entidad,cod_perfil,desc_perf,indi_impre_masiva,indi_rta_masiva,indi_impre_carta,est_perf,user_alt_perf,fec_alt_perf,user_modf_perf,fec_modf_perf,indi_auto_acm,indi_distribucion,indi_atencion_telef,nro_jerarquia,sector_owner)
         select cod_entidad,cod_perfil,desc_perf,indi_impre_masiva,indi_rta_masiva,indi_impre_carta,est_perf,user_alt_perf,fec_alt_perf,user_modf_perf,fec_modf_perf,indi_auto_acm,indi_distribucion,indi_atencion_telef,nro_jerarquia,sector_owner
           from gec01.perfiles;

    v_paso := 2;
    --Se almacena la informacion definitiva en la tabla de interfaz INT_USUARIOS
    insert into int_usuarios
         select usu.cod_user,
                usu.ape_user,
                usu.nom_user,
                decode(perf.cod_perfil,null,'ND',perf.cod_perfil) cod_perfil,
                decode(perf.desc_perf,null,'NO DEFINIDO',perf.desc_perf) perfil,
                usu.mail_user,
                decode(usu.fec_baj_user, null, 'A', 'I') estado
           from ig_usuarios usu,
                ig_perfiles perf
          where usu.cod_entidad = perf.cod_entidad(+)
            and usu.cod_perfil = perf.cod_perfil(+);

    v_paso := 3;
    out_file := null;
    out_file := utl_file.fopen (v_path_utl, 'DIM_USUARIOS.txt', 'w',v_line_max);

    for x in (select * from int_usuarios) loop
      v_paso := 4;
      --Modifica el usuario en caso de que exista
      update dim_usuarios
         set usu_apellido = x.usu_apellido,
             usu_nombre = x.usu_nombre,
             usu_perfil = x.usu_perfil,
             usu_desc_perfil = x.usu_desc_perfil,
             usu_mail = x.usu_mail,
             usu_estado = x.usu_estado
       where usu_cod = x.usu_cod;

      if sql%notfound then
        v_paso := 5;
        --Inserta el usuario en caso de que no exista
        insert into dim_usuarios
             values (x.usu_cod,
                    x.usu_apellido,
                    x.usu_nombre,
                    x.usu_perfil,
                    x.usu_desc_perfil,
                    x.usu_mail,
                    x.usu_estado);
      end if;

      v_paso := 6;
      utl_file.put_line (out_file, x.usu_cod || v_separador ||
                                  x.usu_apellido || v_separador ||
                                  x.usu_nombre || v_separador ||
                                  x.usu_perfil || v_separador ||
                                  x.usu_desc_perfil || v_separador ||
                                  x.usu_mail || v_separador ||
                                  x.usu_estado);

    end loop;

    v_paso := 7;
    utl_file.fclose(out_file);

    commit;

    v_paso := 8;
    v_fecha_fin := sysdate;
    pkg_gc_procesos.sp_procesos(2, 2, v_fecha_inicio, v_fecha_fin, id_proceso);
  exception
    when others then
      v_fecha_fin := sysdate;
      pkg_gc_procesos.sp_procesos(2, 6, v_fecha_inicio, v_fecha_fin, id_proceso);
      v_err_sql := substr (sqlerrm (sqlcode), 1, 250);
      pkg_gc_procesos.sp_inconsistencias(id_proceso, sysdate, v_paso, v_err_sql);
  end sp_usuarios;

/******************************************************************************/
-------------------------- SP_RESULTADO_ENCUESTAS ------------------------------
/******************************************************************************/
  procedure sp_resultado_encuestas (id_proceso in number, fec_desde in date, fec_hasta in date) as
  begin
    v_fecha_inicio := sysdate;
    pkg_gc_procesos.sp_procesos(1, 1, v_fecha_inicio, null, id_proceso);

    v_paso := 1;
    --Se replican los datos en la tabla IG_INFORMACION_CONFORME
    insert into ig_informacion_conforme (cod_entidad,ide_gestion_sector,ide_gestion_nro,sec_mail,cod_info_conf,dato_info_conf,user_info_conf,fec_info_conf,orden_info_conf)
         select cod_entidad,ide_gestion_sector,ide_gestion_nro,sec_mail,cod_info_conf,dato_info_conf,user_info_conf,fec_info_conf,orden_info_conf
           from gec01.informacion_conforme
          where fec_info_conf between fec_desde and fec_hasta
            and dato_info_conf <> 'Ns/Nc';

    v_paso := 2;
    --Se replican los datos en la tabla IG_GESTIONES para relacionar las gestiones en los resultados con el id de circuito
    insert into ig_gestiones (cod_entidad,ide_gestion_sector,ide_gestion_nro,ide_circuito,indi_tipo_circ,indi_decision_comer,indi_replteo,indi_rta_inmed,indi_impre_cart,imp_autz_solicitado,cod_mone_solicitado,imp_autz_autorizado,cod_mone_autorizado,imp_autz_resolucion,cod_mone_resolucion,tpo_pers,cod_crm,comen_cliente,ide_gest_sector_relac,ide_gest_nro_relac,fec_gestion_alta,cod_bandeja_actual,fec_bandeja_actual,cod_sector_actual,inic_user_alta,indi_mail,indi_prioridad,fec_max_resol,cod_conforme,cod_user_actual,cod_grupo_empresa,cod_tipo_favorabilidad,fec_aviso_venc)
      select ges.cod_entidad,ges.ide_gestion_sector,ges.ide_gestion_nro,ges.ide_circuito,ges.indi_tipo_circ,ges.indi_decision_comer,ges.indi_replteo,ges.indi_rta_inmed,ges.indi_impre_cart,ges.imp_autz_solicitado,ges.cod_mone_solicitado,ges.imp_autz_autorizado,ges.cod_mone_autorizado,ges.imp_autz_resolucion,ges.cod_mone_resolucion,ges.tpo_pers,ges.cod_crm,ges.comen_cliente,ges.ide_gest_sector_relac,ges.ide_gest_nro_relac,ges.fec_gestion_alta,ges.cod_bandeja_actual,ges.fec_bandeja_actual,ges.cod_sector_actual,ges.inic_user_alta,ges.indi_mail,ges.indi_prioridad,ges.fec_max_resol,ges.cod_conforme,ges.cod_user_actual,ges.cod_grupo_empresa,ges.cod_tipo_favorabilidad,ges.fec_aviso_venc
        from ig_informacion_conforme inf, gec01.gestiones ges
       where inf.cod_entidad = ges.cod_entidad
         and inf.ide_gestion_nro = ges.ide_gestion_nro;

    v_paso := 3;
    --Se almacena la informacion definitiva en la tabla de interfaz INT_RESULTADO_ENCUESTAS
    insert into int_resultado_encuestas
      select inf.ide_gestion_nro,
             inf.cod_info_conf,
             inf.dato_info_conf,
             to_char(inf.fec_info_conf,'YYYYMMDD') || ges.ide_circuito,
             inf.sec_mail
        from ig_informacion_conforme inf, ig_gestiones ges
       where inf.cod_entidad = ges.cod_entidad
         and inf.ide_gestion_nro = ges.ide_gestion_nro;

    v_paso := 4;
    out_file := null;
    out_file := utl_file.fopen (v_path_utl, 'DIM_RESULTADOS_ENCUESTAS.txt', 'w',v_line_max);

    for x in (select * from int_resultado_encuestas) loop
      v_paso := 5;
      --Modifica el usuario en caso de que exista
      update dim_resultado_encuestas
         set enc_respuesta = x.enc_respuesta
       where enc_gestion_nro = x.enc_gestion_nro
         and enc_cod_pregunta = x.enc_cod_pregunta
         and enc_fecha_respuesta = x.enc_fecha_respuesta
         and enc_sec_mail = x.enc_sec_mail;

      if sql%notfound then
        v_paso := 6;
        --Inserta el usuario en caso de que no exista
        insert into dim_resultado_encuestas
             values (x.enc_gestion_nro,
                    x.enc_cod_pregunta,
                    x.enc_respuesta,
                    x.enc_fecha_respuesta,
                    x.enc_sec_mail);
      end if;

      v_paso := 7;
      utl_file.put_line (out_file, x.enc_gestion_nro || v_separador ||
                                  x.enc_cod_pregunta || v_separador ||
                                  x.enc_respuesta || v_separador ||
                                  x.enc_fecha_respuesta || v_separador ||
                                  x.enc_sec_mail);

    end loop;

    v_paso := 8;
    utl_file.fclose(out_file);

    commit;

    v_paso := 9;
    v_fecha_fin := sysdate;
    pkg_gc_procesos.sp_procesos(2, 2, v_fecha_inicio, v_fecha_fin, id_proceso);
  exception
    when others then
      v_fecha_fin := sysdate;
      pkg_gc_procesos.sp_procesos(2, 6, v_fecha_inicio, v_fecha_fin, id_proceso);
      v_err_sql := substr (sqlerrm (sqlcode), 1, 250);
      pkg_gc_procesos.sp_inconsistencias(id_proceso, sysdate, v_paso, v_err_sql);
  end sp_resultado_encuestas;

/******************************************************************************/
------------------------------- SP_TIPO_ENCUESTAS ------------------------------
/******************************************************************************/
  procedure sp_tipo_encuestas (id_proceso in number, fec_desde in date, fec_hasta in date) as
  begin
    v_fecha_inicio := sysdate;
    pkg_gc_procesos.sp_procesos(1, 1, v_fecha_inicio, null, id_proceso);

    v_paso := 1;
    --Se replican los datos en la tabla IG_MODELOS_CONFORME que contiene los tipos de encuestas sin filtro de fecha
    insert into ig_modelos_conforme (cod_entidad,cod_conforme,desc_conforme,desc_detall_conf,est_conforme,user_alt_conf,fec_alt_conf,user_modf_conf,fec_modf_conf)
         select cod_entidad,cod_conforme,desc_conforme,desc_detall_conf,est_conforme,user_alt_conf,fec_alt_conf,user_modf_conf,fec_modf_conf
           from gec01.modelos_conforme
          where fec_modf_conf between fec_desde and fec_hasta;

    v_paso := 2;
    --Se almacena la informacion definitiva en la tabla de interfaz INT_TIPO_ENCUESTAS
    insert into int_tipo_encuestas
      select cod_conforme, desc_conforme, desc_detall_conf, est_conforme from ig_modelos_conforme;

    v_paso := 3;
    out_file := null;
    out_file := utl_file.fopen (v_path_utl, 'DIM_TIPO_ENCUESTAS.txt', 'w',v_line_max);

    for x in (select * from int_tipo_encuestas) loop
      v_paso := 4;
      --Modifica el tipo de encuesta en caso de que exista
      update dim_tipo_encuestas
         set enc_desc = x.enc_desc,
             enc_detalle = x.enc_detalle,
             enc_estado = x.enc_estado
       where enc_cod = x.enc_cod;

      if sql%notfound then
        v_paso := 5;
        --Inserta  el tipo de encuesta en caso de que no exista
        insert into dim_tipo_encuestas
             values (x.enc_cod, x.enc_desc, x.enc_detalle, x.enc_estado);
      end if;

      v_paso := 6;
      utl_file.put_line (out_file, x.enc_cod || v_separador || x.enc_desc || v_separador || x.enc_detalle || v_separador || x.enc_estado);

    end loop;

    v_paso := 7;
    utl_file.fclose(out_file);

    commit;

    v_paso := 8;
    v_fecha_fin := sysdate;
    pkg_gc_procesos.sp_procesos(2, 2, v_fecha_inicio, v_fecha_fin, id_proceso);
  exception
    when others then
      v_fecha_fin := sysdate;
      pkg_gc_procesos.sp_procesos(2, 6, v_fecha_inicio, v_fecha_fin, id_proceso);
      v_err_sql := substr (sqlerrm (sqlcode), 1, 250);
      pkg_gc_procesos.sp_inconsistencias(id_proceso, sysdate, v_paso, v_err_sql);
  end sp_tipo_encuestas;

/******************************************************************************/
-------------------------- SP_PREGUNTAS_ENCUESTAS ------------------------------
/******************************************************************************/
  procedure sp_preguntas_encuestas (id_proceso in number, fec_desde in date, fec_hasta in date) as
  begin
    v_fecha_inicio := sysdate;
    pkg_gc_procesos.sp_procesos(1, 1, v_fecha_inicio, null, id_proceso);

    v_paso := 1;
    --Se replican los datos en la tabla IG_INFO_CONFORME que contiene las preguntas de la encuesta
    insert into ig_info_conforme (cod_entidad,cod_info_conf,desc_info_conf,cod_tpo_dat,cod_tpo_campo,long_info_conf,est_info_conf,user_alt_info_conf,fec_alt_info_conf,user_modf_info_conf,fec_modf_info_conf)
         select cod_entidad,cod_info_conf,desc_info_conf,cod_tpo_dat,cod_tpo_campo,long_info_conf,est_info_conf,user_alt_info_conf,fec_alt_info_conf,user_modf_info_conf,fec_modf_info_conf
           from gec01.info_conforme
          where fec_modf_info_conf between fec_desde and fec_hasta;

    v_paso := 2;
    --Se almacena la informacion definitiva en la tabla de interfaz INT_PREGUNTAS_ENCUESTAS
    insert into int_preguntas_encuestas
      select cod_info_conf, desc_info_conf, est_info_conf from ig_info_conforme;

    v_paso := 3;
    out_file := null;
    out_file := utl_file.fopen (v_path_utl, 'DIM_PREGUNTAS_ENCUESTAS.txt', 'w',v_line_max);

    for x in (select * from int_preguntas_encuestas) loop
      v_paso := 4;
      --Modifica las preguntas de las encuestas en caso de que exista
      update dim_preguntas_encuestas
         set pre_pregunta = x.pre_pregunta,
             pre_estado = x.pre_estado
       where pre_cod = x.pre_cod;

      if sql%notfound then
        v_paso := 5;
        --Inserta las preguntas de las encuestas en caso de que no exista
        insert into dim_preguntas_encuestas
             values (x.pre_cod, x.pre_pregunta, x.pre_estado);
      end if;

      v_paso := 6;
      utl_file.put_line (out_file, x.pre_cod || v_separador || x.pre_pregunta || v_separador || x.pre_estado);

    end loop;

    v_paso := 7;
    utl_file.fclose(out_file);

    commit;

    v_paso := 8;
    v_fecha_fin := sysdate;
    pkg_gc_procesos.sp_procesos(2, 2, v_fecha_inicio, v_fecha_fin, id_proceso);
  exception
    when others then
      v_fecha_fin := sysdate;
      pkg_gc_procesos.sp_procesos(2, 6, v_fecha_inicio, v_fecha_fin, id_proceso);
      v_err_sql := substr (sqlerrm (sqlcode), 1, 250);
      pkg_gc_procesos.sp_inconsistencias(id_proceso, sysdate, v_paso, v_err_sql);
  end sp_preguntas_encuestas;

/******************************************************************************/
---------------------------------- SP_REGULADOR --------------------------------
/******************************************************************************/
  procedure sp_regulador (id_proceso in number, fec_desde in date, fec_hasta in date) as
  begin
    v_fecha_inicio := sysdate;
    pkg_gc_procesos.sp_procesos(1, 1, v_fecha_inicio, null, id_proceso);

    v_paso := 1;
    --Se replican los datos en la tabla IG_TIPO_CARACTERISTICA
    insert into ig_tipo_caracteristica (ide_caracteristica,desc_caracteristica)
         select ide_caracteristica,desc_caracteristica
           from gec01.tipo_caracteristica;

    v_paso := 2;
    --Se almacena la informacion definitiva en la tabla de interfaz INT_REGULADOR
    insert into int_regulador
      select ide_caracteristica, desc_caracteristica from ig_tipo_caracteristica;

    v_paso := 3;
    out_file := null;
    out_file := utl_file.fopen (v_path_utl, 'DIM_REGULADOR.txt', 'w',v_line_max);

    for x in (select * from int_regulador) loop
      v_paso := 4;
      --Modifica el tipo de regulador en caso de que exista
      update dim_regulador
         set reg_desc = x.reg_desc
       where reg_cod = x.reg_cod;

      if sql%notfound then
        v_paso := 5;
        --Inserta  el tipo de regulador en caso de que no exista
        insert into dim_regulador
             values (x.reg_cod, x.reg_desc);
      end if;

      v_paso := 6;
      utl_file.put_line (out_file, x.reg_cod || v_separador || x.reg_desc);

    end loop;

    v_paso := 7;
    utl_file.fclose(out_file);

    commit;

    v_paso := 8;
    v_fecha_fin := sysdate;
    pkg_gc_procesos.sp_procesos(2, 2, v_fecha_inicio, v_fecha_fin, id_proceso);
  exception
    when others then
      v_fecha_fin := sysdate;
      pkg_gc_procesos.sp_procesos(2, 6, v_fecha_inicio, v_fecha_fin, id_proceso);
      v_err_sql := substr (sqlerrm (sqlcode), 1, 250);
      pkg_gc_procesos.sp_inconsistencias(id_proceso, sysdate, v_paso, v_err_sql);
  end sp_regulador;

/******************************************************************************/
---------------------------- SP_GESTION_REGULADOR ------------------------------
/******************************************************************************/
  procedure sp_gestion_regulador (id_proceso in number, fec_desde in date, fec_hasta in date) as
  begin
    v_fecha_inicio := sysdate;
    pkg_gc_procesos.sp_procesos(1, 1, v_fecha_inicio, null, id_proceso);

    v_paso := 1;
    --Se replican los datos en la tabla IG_GESTION_ESTADOS
    insert into ig_gestion_estados (cod_entidad,ide_gestion_sector,ide_gestion_nro,cod_est_gest,fec_est_gest,tpo_medio,cod_rta_resol_predef,cod_contc,cod_niv_conf,indi_impre_masiva,imp_gest_estado,cod_mone_imp,cod_carta,cod_sect_asign_espec,tmp_est_gest,user_estado,cod_sect_estado,cod_bandeja,orden_estado,cod_responsab_est,actor_nro_orden,comentario,ide_gestion_sec,ind_modf_fec_resol)
      select cod_entidad,ide_gestion_sector,ide_gestion_nro,cod_est_gest,fec_est_gest,tpo_medio,cod_rta_resol_predef,cod_contc,cod_niv_conf,indi_impre_masiva,imp_gest_estado,cod_mone_imp,cod_carta,cod_sect_asign_espec,tmp_est_gest,user_estado,cod_sect_estado,cod_bandeja,orden_estado,cod_responsab_est,actor_nro_orden,comentario,ide_gestion_sec,ind_modf_fec_resol
        from gec01.gestion_estados
       where cod_est_gest = 100
         and fec_est_gest between fec_desde and fec_hasta;

    v_paso := 2;
    --Se replican los datos en la tabla IG_GESTION_CARACTERISTICA
    /*insert into ig_gestion_caracteristica (cod_entidad,ide_gestion_sector,ide_gestion_nro,ide_caracteristica,desc_valor,fecha_estado,user_estado)
      select car.cod_entidad,car.ide_gestion_sector,car.ide_gestion_nro,car.ide_caracteristica,car.desc_valor,car.fecha_estado,car.user_estado
        from gec01.gestion_caracteristica car, ig_gestion_estados est
       where est.cod_entidad = car.cod_entidad
         and est.ide_gestion_sector = car.ide_gestion_sector
         and est.ide_gestion_nro = car.ide_gestion_nro;*/

    v_paso := 3;
    --Se insertan los datos necesarios en la tabla INT_GESTION_REGULADOR
    insert into int_gestion_regulador
      select distinct ide_gestion_nro, ide_caracteristica, fecha_estado from ig_gestion_caracteristica;

    v_paso := 4;
    --Se insertan los datos en la tabla DIM_GESTION_REGULADOR
    insert into dim_gestion_regulador
      select * from int_gestion_regulador;

    v_paso := 5;
    out_file := null;
    out_file := utl_file.fopen (v_path_utl, 'DIM_GESTION_REGULADOR.txt', 'w',v_line_max);

    for x in (select greg_gestion_nro,greg_cod_regulador,greg_fecha_regulador from int_gestion_regulador) loop
      v_paso := 6;
      utl_file.put_line (out_file, x.greg_gestion_nro || v_separador || x.greg_cod_regulador || v_separador || x.greg_fecha_regulador);
    end loop;

    v_paso := 6;
    utl_file.fclose(out_file);

    commit;

    v_paso := 8;
    v_fecha_fin := sysdate;
    pkg_gc_procesos.sp_procesos(2, 2, v_fecha_inicio, v_fecha_fin, id_proceso);
  exception
    when others then
      v_fecha_fin := sysdate;
      pkg_gc_procesos.sp_procesos(2, 6, v_fecha_inicio, v_fecha_fin, id_proceso);
      v_err_sql := substr (sqlerrm (sqlcode), 1, 250);
      pkg_gc_procesos.sp_inconsistencias(id_proceso, sysdate, v_paso, v_err_sql);
  end sp_gestion_regulador;

/******************************************************************************/
---------------------------- SP_RTA_RESOL_PREDEF ------------------------------
/******************************************************************************/
  procedure sp_rta_resol_predef (id_proceso in number, fec_desde in date, fec_hasta in date) as
  begin
    v_fecha_inicio := sysdate;
    pkg_gc_procesos.sp_procesos(1, 1, v_fecha_inicio, null, id_proceso);

    v_paso := 1;
    --Se replican los datos en la tabla IG_RTA_RESOL_PREDEF
    insert into ig_rta_resol_predef (cod_entidad,cod_rta_resol_predef,desc_rta_resol_predef,desc_detall_rta_resol,indi_tpo_rta,est_rta_resol_predef,user_alt_rta_resol_predef,fec_alt_rta_resol_predef,user_modf_rta_resol_predef,fec_modf_rta_resol_predef,cod_tipo_resolucion,indi_opcion_acm,sector_owner)
      select cod_entidad,cod_rta_resol_predef,desc_rta_resol_predef,desc_detall_rta_resol,indi_tpo_rta,est_rta_resol_predef,user_alt_rta_resol_predef,fec_alt_rta_resol_predef,user_modf_rta_resol_predef,fec_modf_rta_resol_predef,cod_tipo_resolucion,indi_opcion_acm,sector_owner
        from gec01.rta_resol_predef
       where fec_modf_rta_resol_predef between fec_desde and fec_hasta;

    v_paso := 2;
    --Se insertan los datos necesarios en la tabla INT_RTA_RESOL_PREDEF
    insert into int_rta_resol_predef
      select distinct cod_rta_resol_predef,desc_rta_resol_predef,desc_detall_rta_resol,indi_tpo_rta,fec_alt_rta_resol_predef,fec_modf_rta_resol_predef,cod_tipo_resolucion,indi_opcion_acm,sector_owner from ig_rta_resol_predef;

    v_paso := 3;
    --Se insertan los datos en la tabla DIM_RTA_RESOL_PREDEF
    insert into dim_rta_resol_predef
      select * from int_rta_resol_predef;

    v_paso := 4;
    out_file := null;
    out_file := utl_file.fopen (v_path_utl, 'DIM_RTA_RESOL_PREDEF.txt', 'w',v_line_max);

    for x in (select rta_cod,rta_desc,rta_detalle,rta_indi_tpo_rta,rta_fecha_alta,rta_fecha_modif,rta_cod_tpo_resol,rta_opcion_acm,rta_sector_owner from int_rta_resol_predef) loop
      v_paso := 5;
      utl_file.put_line (out_file, x.rta_cod || v_separador ||
                                   x.rta_desc || v_separador ||
                                   x.rta_detalle || v_separador ||
                                   x.rta_indi_tpo_rta || v_separador ||
                                   x.rta_fecha_alta || v_separador ||
                                   x.rta_fecha_modif || v_separador ||
                                   x.rta_cod_tpo_resol || v_separador ||
                                   x.rta_opcion_acm || v_separador ||
                                   x.rta_sector_owner);
    end loop;

    v_paso := 6;
    utl_file.fclose(out_file);

    commit;

    v_paso := 7;
    v_fecha_fin := sysdate;
    pkg_gc_procesos.sp_procesos(2, 2, v_fecha_inicio, v_fecha_fin, id_proceso);
  exception
    when others then
      v_fecha_fin := sysdate;
      pkg_gc_procesos.sp_procesos(2, 6, v_fecha_inicio, v_fecha_fin, id_proceso);
      v_err_sql := substr (sqlerrm (sqlcode), 1, 250);
      pkg_gc_procesos.sp_inconsistencias(id_proceso, sysdate, v_paso, v_err_sql);
  end sp_rta_resol_predef;

/******************************************************************************/
---------------------------- SP_TIPO_RECLAMO_BCRA ------------------------------
/******************************************************************************/
  procedure sp_tipo_reclamo_bcra (id_proceso in number, fec_desde in date, fec_hasta in date) as
  begin
    v_fecha_inicio := sysdate;
    pkg_gc_procesos.sp_procesos(1, 1, v_fecha_inicio, null, id_proceso);

    v_paso := 1;
    --Se replican los datos en la tabla IG_GC_TIPO_RECLAMO_BCRA
    insert into ig_gc_tipo_reclamo_bcra (cod_entidad,cod_tipo_reclamo,desc_tipo_reclamo,user_alt,fec_alt,user_modf,fec_modf)
      select cod_entidad,cod_tipo_reclamo,desc_tipo_reclamo,user_alt,fec_alt,user_modf,fec_modf
        from gec01.gc_tipo_reclamo_bcra
       where fec_modf between fec_desde and fec_hasta;

    v_paso := 2;
    --Se insertan los datos necesarios en la tabla INT_TIPO_RECLAMO_BCRA
    insert into int_tipo_reclamo_bcra
      select cod_tipo_reclamo, desc_tipo_reclamo from ig_gc_tipo_reclamo_bcra;

    v_paso := 3;
    out_file := null;
    out_file := utl_file.fopen (v_path_utl, 'DIM_TIPO_RECLAMO_BCRA.txt', 'w',v_line_max);

    for x in (select * from int_tipo_reclamo_bcra) loop
      v_paso := 4;
      --Modifica el tipo de reclamo bcra en caso de que exista
      update dim_tipo_reclamo_bcra
         set bcra_desc_tipo_reclamo = x.bcra_desc_tipo_reclamo
       where bcra_tipo_reclamo = x.bcra_tipo_reclamo;

      if sql%notfound then
        v_paso := 5;
        --Inserta  el tipo de reclamo bcra en caso de que no exista
        insert into dim_tipo_reclamo_bcra
             values (x.bcra_tipo_reclamo, x.bcra_desc_tipo_reclamo);
      end if;

      v_paso := 6;
      utl_file.put_line (out_file, x.bcra_tipo_reclamo || v_separador || x.bcra_desc_tipo_reclamo);

    end loop;

    v_paso := 7;
    utl_file.fclose(out_file);

    commit;

    v_paso := 8;
    v_fecha_fin := sysdate;
    pkg_gc_procesos.sp_procesos(2, 2, v_fecha_inicio, v_fecha_fin, id_proceso);
  exception
    when others then
      v_fecha_fin := sysdate;
      pkg_gc_procesos.sp_procesos(2, 6, v_fecha_inicio, v_fecha_fin, id_proceso);
      v_err_sql := substr (sqlerrm (sqlcode), 1, 250);
      pkg_gc_procesos.sp_inconsistencias(id_proceso, sysdate, v_paso, v_err_sql);
  end sp_tipo_reclamo_bcra;

/******************************************************************************/
---------------------------- SP_CONCEPTOS_BCRA ------------------------------
/******************************************************************************/
  procedure sp_conceptos_bcra (id_proceso in number, fec_desde in date, fec_hasta in date) as
  begin
    v_fecha_inicio := sysdate;
    pkg_gc_procesos.sp_procesos(1, 1, v_fecha_inicio, null, id_proceso);

    v_paso := 1;
    --Se replican los datos en la tabla IG_GC_CONCEPTOS_BCRA
    insert into ig_gc_conceptos_bcra (cod_entidad,cod_cpto,desc_cpto,cod_tipo_reclamo,user_alt,fec_alt,user_modf,fec_modf)
      select cod_entidad,cod_cpto,desc_cpto,cod_tipo_reclamo,user_alt,fec_alt,user_modf,fec_modf
        from gec01.gc_conceptos_bcra
       where fec_modf between fec_desde and fec_hasta;

    v_paso := 2;
    --Se insertan los datos necesarios en la tabla INT_CONCEPTOS_BCRA
    insert into int_conceptos_bcra
      select cod_cpto, desc_cpto, cod_tipo_reclamo from ig_gc_conceptos_bcra;

    v_paso := 3;
    out_file := null;
    out_file := utl_file.fopen (v_path_utl, 'DIM_CONCEPTOS_BCRA.txt', 'w',v_line_max);

    for x in (select * from int_conceptos_bcra) loop
      v_paso := 4;
      --Modifica el concepto de reclamo bcra en caso de que exista
      update dim_conceptos_bcra
         set cpto_desc_bcra = x.cpto_desc_bcra,
             cpto_cod_tipo_reclamo_bcra = x.cpto_cod_tipo_reclamo_bcra
       where cpto_cod_bcra = x.cpto_cod_bcra;

      if sql%notfound then
        v_paso := 5;
        --Inserta  el concepto de reclamo bcra en caso de que no exista
        insert into dim_conceptos_bcra
             values (x.cpto_cod_bcra, x.cpto_desc_bcra, x.cpto_cod_tipo_reclamo_bcra);
      end if;

      v_paso := 6;
      utl_file.put_line (out_file, x.cpto_cod_bcra || v_separador || x.cpto_desc_bcra || v_separador || x.cpto_cod_tipo_reclamo_bcra);

    end loop;

    v_paso := 7;
    utl_file.fclose(out_file);

    commit;

    v_paso := 8;
    v_fecha_fin := sysdate;
    pkg_gc_procesos.sp_procesos(2, 2, v_fecha_inicio, v_fecha_fin, id_proceso);
  exception
    when others then
      v_fecha_fin := sysdate;
      pkg_gc_procesos.sp_procesos(2, 6, v_fecha_inicio, v_fecha_fin, id_proceso);
      v_err_sql := substr (sqlerrm (sqlcode), 1, 250);
      pkg_gc_procesos.sp_inconsistencias(id_proceso, sysdate, v_paso, v_err_sql);
  end sp_conceptos_bcra;

/******************************************************************************/
---------------------------- SP_TIPO_RECLAMO_SAC ------------------------------
/******************************************************************************/
  procedure sp_tipo_reclamo_sac (id_proceso in number, fec_desde in date, fec_hasta in date) as
  begin
    v_fecha_inicio := sysdate;
    pkg_gc_procesos.sp_procesos(1, 1, v_fecha_inicio, null, id_proceso);

    v_paso := 1;
    --Se replican los datos en la tabla IG_GC_TIPO_RECLAMO_SAC
    insert into ig_gc_tipo_reclamo_sac (cod_entidad,cod_tipo_reclamo,desc_tipo_reclamo,user_alt,fec_alt,user_modf,fec_modf)
      select cod_entidad,cod_tipo_reclamo,desc_tipo_reclamo,user_alt,fec_alt,user_modf,fec_modf
        from gec01.gc_tipo_reclamo_sac
       where fec_modf between fec_desde and fec_hasta;

    v_paso := 2;
    --Se insertan los datos necesarios en la tabla INT_TIPO_RECLAMO_SAC
    insert into int_tipo_reclamo_sac
      select cod_tipo_reclamo, desc_tipo_reclamo from ig_gc_tipo_reclamo_sac;

    v_paso := 3;
    out_file := null;
    out_file := utl_file.fopen (v_path_utl, 'DIM_TIPO_RECLAMO_SAC.txt', 'w',v_line_max);

    for x in (select * from int_tipo_reclamo_sac) loop
      v_paso := 4;
      --Modifica el tipo de reclamo sac en caso de que exista
      update dim_tipo_reclamo_sac
         set sac_desc_tipo_reclamo = x.sac_desc_tipo_reclamo
       where sac_tipo_reclamo = x.sac_tipo_reclamo;

      if sql%notfound then
        v_paso := 5;
        --Inserta  el tipo de reclamo sac en caso de que no exista
        insert into dim_tipo_reclamo_sac
             values (x.sac_tipo_reclamo, x.sac_desc_tipo_reclamo);
      end if;

      v_paso := 6;
      utl_file.put_line (out_file, x.sac_tipo_reclamo || v_separador || x.sac_desc_tipo_reclamo);

    end loop;

    v_paso := 7;
    utl_file.fclose(out_file);

    commit;

    v_paso := 8;
    v_fecha_fin := sysdate;
    pkg_gc_procesos.sp_procesos(2, 2, v_fecha_inicio, v_fecha_fin, id_proceso);
  exception
    when others then
      v_fecha_fin := sysdate;
      pkg_gc_procesos.sp_procesos(2, 6, v_fecha_inicio, v_fecha_fin, id_proceso);
      v_err_sql := substr (sqlerrm (sqlcode), 1, 250);
      pkg_gc_procesos.sp_inconsistencias(id_proceso, sysdate, v_paso, v_err_sql);
  end sp_tipo_reclamo_sac;

/******************************************************************************/
---------------------------- SP_CONCEPTOS_SAC ------------------------------
/******************************************************************************/
  procedure sp_conceptos_sac (id_proceso in number, fec_desde in date, fec_hasta in date) as
  begin
    v_fecha_inicio := sysdate;
    pkg_gc_procesos.sp_procesos(1, 1, v_fecha_inicio, null, id_proceso);

    v_paso := 1;
    --Se replican los datos en la tabla IG_GC_CONCEPTOS_SAC
    insert into ig_gc_conceptos_sac (cod_entidad,cod_cpto,desc_cpto,cod_tipo_reclamo,user_alt,fec_alt,user_modf,fec_modf)
      select cod_entidad,cod_cpto,desc_cpto,cod_tipo_reclamo,user_alt,fec_alt,user_modf,fec_modf
        from gec01.gc_conceptos_sac
       where fec_modf between fec_desde and fec_hasta;

    v_paso := 2;
    --Se insertan los datos necesarios en la tabla INT_CONCEPTOS_SAC
    insert into int_conceptos_sac
      select cod_cpto, desc_cpto, cod_tipo_reclamo from ig_gc_conceptos_sac;

    v_paso := 3;
    out_file := null;
    out_file := utl_file.fopen (v_path_utl, 'DIM_CONCEPTOS_SAC.txt', 'w',v_line_max);

    for x in (select * from int_conceptos_sac) loop
      v_paso := 4;
      --Modifica el concepto de reclamo sac en caso de que exista
      update dim_conceptos_sac
         set cpto_desc_sac = x.cpto_desc_sac,
             cpto_cod_tipo_reclamo_sac = x.cpto_cod_tipo_reclamo_sac
       where cpto_cod_sac = x.cpto_cod_sac;

      if sql%notfound then
        v_paso := 5;
        --Inserta  el concepto de reclamo sac en caso de que no exista
        insert into dim_conceptos_sac
             values (x.cpto_cod_sac, x.cpto_desc_sac, x.cpto_cod_tipo_reclamo_sac);
      end if;

      v_paso := 6;
      utl_file.put_line (out_file, x.cpto_cod_sac || v_separador || x.cpto_desc_sac || v_separador || x.cpto_cod_tipo_reclamo_sac);

    end loop;

    v_paso := 7;
    utl_file.fclose(out_file);

    commit;

    v_paso := 8;
    v_fecha_fin := sysdate;
    pkg_gc_procesos.sp_procesos(2, 2, v_fecha_inicio, v_fecha_fin, id_proceso);
  exception
    when others then
      v_fecha_fin := sysdate;
      pkg_gc_procesos.sp_procesos(2, 6, v_fecha_inicio, v_fecha_fin, id_proceso);
      v_err_sql := substr (sqlerrm (sqlcode), 1, 250);
      pkg_gc_procesos.sp_inconsistencias(id_proceso, sysdate, v_paso, v_err_sql);
  end sp_conceptos_sac;

/******************************************************************************/
-------------------------- SP_TIPO_RECLAMO_ESPANA ------------------------------
/******************************************************************************/
  procedure sp_tipo_reclamo_espana (id_proceso in number, fec_desde in date, fec_hasta in date) as
  begin
    v_fecha_inicio := sysdate;
    pkg_gc_procesos.sp_procesos(1, 1, v_fecha_inicio, null, id_proceso);

    v_paso := 1;
    --Se replican los datos en la tabla IG_GC_TIPO_RECLAMO_ESPANA
    insert into ig_gc_tipo_reclamo_espana (cod_entidad,cod_tipo_reclamo,desc_tipo_reclamo,user_alt,fec_alt,user_modf,fec_modf)
      select cod_entidad,cod_tipo_reclamo,desc_tipo_reclamo,user_alt,fec_alt,user_modf,fec_modf
        from gec01.gc_tipo_reclamo_espana
       where fec_modf between fec_desde and fec_hasta;

    v_paso := 2;
    --Se insertan los datos necesarios en la tabla INT_TIPO_RECLAMO_ESPANA
    insert into int_tipo_reclamo_espana
      select cod_tipo_reclamo, desc_tipo_reclamo from ig_gc_tipo_reclamo_espana;

    v_paso := 3;
    out_file := null;
    out_file := utl_file.fopen (v_path_utl, 'DIM_TIPO_RECLAMO_ESPANA.txt', 'w',v_line_max);

    for x in (select * from int_tipo_reclamo_espana) loop
      v_paso := 4;
      --Modifica el tipo de reclamo espana en caso de que exista
      update dim_tipo_reclamo_espana
         set esp_desc_tipo_reclamo = x.esp_desc_tipo_reclamo
       where esp_tipo_reclamo = x.esp_tipo_reclamo;

      if sql%notfound then
        v_paso := 5;
        --Inserta  el tipo de reclamo espana en caso de que no exista
        insert into dim_tipo_reclamo_espana
             values (x.esp_tipo_reclamo, x.esp_desc_tipo_reclamo);
      end if;

      v_paso := 6;
      utl_file.put_line (out_file, x.esp_tipo_reclamo || v_separador || x.esp_desc_tipo_reclamo);

    end loop;

    v_paso := 7;
    utl_file.fclose(out_file);

    commit;

    v_paso := 8;
    v_fecha_fin := sysdate;
    pkg_gc_procesos.sp_procesos(2, 2, v_fecha_inicio, v_fecha_fin, id_proceso);
  exception
    when others then
      v_fecha_fin := sysdate;
      pkg_gc_procesos.sp_procesos(2, 6, v_fecha_inicio, v_fecha_fin, id_proceso);
      v_err_sql := substr (sqlerrm (sqlcode), 1, 250);
      pkg_gc_procesos.sp_inconsistencias(id_proceso, sysdate, v_paso, v_err_sql);
  end sp_tipo_reclamo_espana;

/******************************************************************************/
---------------------------- SP_CONCEPTOS_ESPANA -------------------------------
/******************************************************************************/
  procedure sp_conceptos_espana (id_proceso in number, fec_desde in date, fec_hasta in date) as
  begin
    v_fecha_inicio := sysdate;
    pkg_gc_procesos.sp_procesos(1, 1, v_fecha_inicio, null, id_proceso);

    v_paso := 1;
    --Se replican los datos en la tabla IG_GC_CONCEPTOS_ESPANA
    insert into ig_gc_conceptos_espana (cod_entidad,cod_cpto,desc_cpto,cod_tipo_reclamo,user_alt,fec_alt,user_modf,fec_modf)
      select cod_entidad,cod_cpto,desc_cpto,cod_tipo_reclamo,user_alt,fec_alt,user_modf,fec_modf
        from gec01.gc_conceptos_espana
       where fec_modf between fec_desde and fec_hasta;

    v_paso := 2;
    --Se insertan los datos necesarios en la tabla INT_CONCEPTOS_ESPANA
    insert into int_conceptos_espana
      select cod_cpto, desc_cpto, cod_tipo_reclamo from ig_gc_conceptos_espana;

    v_paso := 3;
    out_file := null;
    out_file := utl_file.fopen (v_path_utl, 'DIM_CONCEPTOS_ESPANA.txt', 'w',v_line_max);

    for x in (select * from int_conceptos_espana) loop
      v_paso := 4;
      --Modifica el concepto de reclamo espana en caso de que exista
      update dim_conceptos_espana
         set cpto_desc_esp = x.cpto_desc_esp,
             cpto_cod_tipo_reclamo_esp = x.cpto_cod_tipo_reclamo_esp
       where cpto_cod_esp = x.cpto_cod_esp;

      if sql%notfound then
        v_paso := 5;
        --Inserta  el concepto de reclamo espana en caso de que no exista
        insert into dim_conceptos_espana
             values (x.cpto_cod_esp, x.cpto_desc_esp, x.cpto_cod_tipo_reclamo_esp);
      end if;

      v_paso := 6;
      utl_file.put_line (out_file, x.cpto_cod_esp || v_separador || x.cpto_desc_esp || v_separador || x.cpto_cod_tipo_reclamo_esp);

    end loop;

    v_paso := 7;
    utl_file.fclose(out_file);

    commit;

    v_paso := 8;
    v_fecha_fin := sysdate;
    pkg_gc_procesos.sp_procesos(2, 2, v_fecha_inicio, v_fecha_fin, id_proceso);
  exception
    when others then
      v_fecha_fin := sysdate;
      pkg_gc_procesos.sp_procesos(2, 6, v_fecha_inicio, v_fecha_fin, id_proceso);
      v_err_sql := substr (sqlerrm (sqlcode), 1, 250);
      pkg_gc_procesos.sp_inconsistencias(id_proceso, sysdate, v_paso, v_err_sql);
  end sp_conceptos_espana;

/******************************************************************************/
------------------------------- SP_INFO_ADJUNTA --------------------------------
/******************************************************************************/
  procedure sp_info_adjunta (id_proceso in number, fec_desde in date, fec_hasta in date) as
  begin
    v_fecha_inicio := sysdate;
    pkg_gc_procesos.sp_procesos(1, 1, v_fecha_inicio, null, id_proceso);

    v_paso := 1;
    --Se modifica el parametro para que en la sesion se mantenga el formato numerico requerido
    execute immediate 'alter session set NLS_NUMERIC_CHARACTERS = ''.,''';

    v_paso := 2;
    --Se replican los datos en la tabla IG_INFORMACION_ADJUNTA
    insert into ig_informacion_adjunta (cod_entidad,ide_gestion_sector,ide_gestion_nro,cod_actor,nro_orden,indi_tpo_info,cod_info_doc_reque,dato_info_doc_reque,link_inf_adjunta,fec_inf_adjunta,user_inf_adjunta,cod_sect_inf_adjunta,nom_archivo_original,nom_archivo,nom_directorio,tamano,secuencia)
         select cod_entidad,ide_gestion_sector,ide_gestion_nro,cod_actor,nro_orden,indi_tpo_info,cod_info_doc_reque,dato_info_doc_reque,link_inf_adjunta,fec_inf_adjunta,user_inf_adjunta,cod_sect_inf_adjunta,nom_archivo_original,nom_archivo,nom_directorio,tamano,secuencia
           from gec01.informacion_adjunta
          where fec_inf_adjunta between fec_desde and fec_hasta;

    v_paso := 3;
    --Se insertan los datos necesarios en la tabla INT_INFORMACION_ADJUNTA
    insert into int_informacion_adjunta (adj_nro_gestion,adj_sector_gestion,adj_actor,adj_nro_orden,adj_tipo_info,adj_cod_tipo_doc,adj_secuencia,adj_dato_info,adj_link_info,adj_fecha_info,adj_user_info,adj_sector_info,adj_nom_archivo_oroginal,adj_nom_archivo,adj_directorio_archivo,adj_importe)
         select ide_gestion_nro,ide_gestion_sector,cod_actor,nro_orden,indi_tpo_info,cod_info_doc_reque,secuencia,dato_info_doc_reque,link_inf_adjunta,fec_inf_adjunta,user_inf_adjunta,cod_sect_inf_adjunta,nom_archivo_original,nom_archivo,nom_directorio,null
           from ig_informacion_adjunta;

    v_paso := 4;
    --out_file := null;
    --out_file := utl_file.fopen (v_path_utl, 'DIM_INFORMACION_ADJUNTA.txt', 'w');

    for x in (select * from int_informacion_adjunta) loop
      v_paso := 5;
      --Modifica la informacion adjunta en caso de que exista
        update dim_informacion_adjunta
           set adj_dato_info = x.adj_dato_info,
               adj_link_info = x.adj_link_info,
               adj_fecha_info = x.adj_fecha_info,
               adj_user_info = x.adj_user_info,
               adj_sector_info = x.adj_sector_info,
               adj_nom_archivo_oroginal = x.adj_nom_archivo_oroginal,
               adj_nom_archivo = x.adj_nom_archivo,
               adj_directorio_archivo = x.adj_directorio_archivo,
               adj_importe = x.adj_importe
         where adj_nro_gestion = x.adj_nro_gestion
           and adj_sector_gestion = x.adj_sector_gestion
           and adj_actor = x.adj_actor
           and adj_nro_orden = x.adj_nro_orden
           and adj_tipo_info = x.adj_tipo_info
           and adj_cod_tipo_doc = x.adj_cod_tipo_doc
           and adj_secuencia = x.adj_secuencia;

      if sql%notfound then
        v_paso:= 6;
        --Inserta la informacion adjunta que no exista en la tabla
          insert into dim_informacion_adjunta
               values (x.adj_nro_gestion,
                       x.adj_sector_gestion,
                       x.adj_actor,
                       x.adj_nro_orden,
                       x.adj_tipo_info,
                       x.adj_cod_tipo_doc,
                       x.adj_secuencia,
                       x.adj_dato_info,
                       x.adj_link_info,
                       x.adj_fecha_info,
                       x.adj_user_info,
                       x.adj_sector_info,
                       x.adj_nom_archivo_oroginal,
                       x.adj_nom_archivo,
                       x.adj_directorio_archivo,
                       x.adj_importe);
      end if;
    end loop;

    for y in (select ide_gestion_nro,ide_gestion_sector,cod_actor,nro_orden,indi_tpo_info,cod_info_doc_reque,secuencia,dato_info_doc_reque
                from ig_informacion_adjunta
               where cod_info_doc_reque in (538,2739,2756,2726)
               order by 1) loop
      begin
        v_paso := 7;
        update dim_informacion_adjunta
           set adj_importe = to_number(y.dato_info_doc_reque,'9999999.99')
         where adj_nro_gestion = y.ide_gestion_nro
           and adj_sector_gestion = y.ide_gestion_sector
           and adj_actor = y.cod_actor
           and adj_nro_orden = y.nro_orden
           and adj_tipo_info = y.indi_tpo_info
           and adj_cod_tipo_doc = y.cod_info_doc_reque
           and adj_secuencia = y.secuencia;
      exception
        when others then
          v_err_sql := substr (sqlerrm (sqlcode), 1, 250);
          pkg_gc_procesos.sp_inconsistencias(id_proceso,
                                             sysdate,
                                             499,
                                             v_err_sql               || '/' ||
                                             y.ide_gestion_nro       || '/' ||
                                             y.cod_info_doc_reque    || '/' ||
                                             y.dato_info_doc_reque   || '/' ||
                                             y.secuencia);
      end;
    end loop;
    /*
    for f in (select adj_nro_gestion,
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
                      adj_importe from dim_informacion_adjunta) loop
      v_paso := 8;
      utl_file.put_line (out_file, f.adj_nro_gestion || v_separador ||
                                    f.adj_sector_gestion || v_separador ||
                                    f.adj_actor || v_separador ||
                                    f.adj_nro_orden || v_separador ||
                                    f.adj_tipo_info || v_separador ||
                                    f.adj_cod_tipo_doc || v_separador ||
                                    f.adj_secuencia || v_separador ||
                                    f.adj_dato_info || v_separador ||
                                    f.adj_link_info || v_separador ||
                                    f.adj_fecha_info || v_separador ||
                                    f.adj_user_info || v_separador ||
                                    f.adj_sector_info || v_separador ||
                                    f.adj_nom_archivo_oroginal || v_separador ||
                                    f.adj_nom_archivo || v_separador ||
                                    f.adj_directorio_archivo || v_separador ||
                                    f.adj_importe);
    end loop;

    v_paso := 9;
    utl_file.fclose(out_file); */

    commit;

    v_paso := 10;
    v_fecha_fin := sysdate;
    pkg_gc_procesos.sp_procesos(2, 2, v_fecha_inicio, v_fecha_fin, id_proceso);

  exception
    when others then
      v_fecha_fin := sysdate;
      pkg_gc_procesos.sp_procesos(2, 6, v_fecha_inicio, v_fecha_fin, id_proceso);
      v_err_sql := substr (sqlerrm (sqlcode), 1, 250);
      pkg_gc_procesos.sp_inconsistencias(id_proceso, sysdate, v_paso, v_err_sql);
  end sp_info_adjunta;

/******************************************************************************/
------------------------ SP_COMENTARIOS_CLIENTE --------------------------------
/******************************************************************************/
  procedure sp_comentarios_cliente (id_proceso in number, fec_desde in date, fec_hasta in date) as
  begin
    v_fecha_inicio := sysdate;
    pkg_gc_procesos.sp_procesos(1, 1, v_fecha_inicio, null, id_proceso);

    v_paso := 1;
    --Se replican los datos en la tabla IG_GESTIONES
    insert into ig_gestiones (cod_entidad,ide_gestion_sector,ide_gestion_nro,ide_circuito,indi_tipo_circ,indi_decision_comer,indi_replteo,indi_rta_inmed,indi_impre_cart,imp_autz_solicitado,cod_mone_solicitado,imp_autz_autorizado,cod_mone_autorizado,imp_autz_resolucion,cod_mone_resolucion,tpo_pers,cod_crm,comen_cliente,ide_gest_sector_relac,ide_gest_nro_relac,fec_gestion_alta,cod_bandeja_actual,fec_bandeja_actual,cod_sector_actual,inic_user_alta,indi_mail,indi_prioridad,fec_max_resol,cod_conforme,cod_user_actual,cod_grupo_empresa,cod_tipo_favorabilidad,fec_aviso_venc)
      select cod_entidad,ide_gestion_sector,ide_gestion_nro,ide_circuito,indi_tipo_circ,indi_decision_comer,indi_replteo,indi_rta_inmed,indi_impre_cart,imp_autz_solicitado,cod_mone_solicitado,imp_autz_autorizado,cod_mone_autorizado,imp_autz_resolucion,cod_mone_resolucion,tpo_pers,cod_crm,comen_cliente,ide_gest_sector_relac,ide_gest_nro_relac,fec_gestion_alta,cod_bandeja_actual,fec_bandeja_actual,cod_sector_actual,inic_user_alta,indi_mail,indi_prioridad,fec_max_resol,cod_conforme,cod_user_actual,cod_grupo_empresa,cod_tipo_favorabilidad,fec_aviso_venc
        from gec01.gestiones
       where fec_gestion_alta between fec_desde and fec_hasta;

    v_paso := 2;
    --Se insertan los datos necesarios en la tabla INT_COMENTARIOS_CLIENTES
    insert into int_comentarios_clientes
      select ide_gestion_nro, comen_cliente
        from ig_gestiones;

    v_paso := 3;
    out_file := null;
    out_file := utl_file.fopen (v_path_utl, 'DIM_COMENTARIOS_CLIENTES.txt', 'w',v_line_max);

    for x in (select * from int_comentarios_clientes) loop
      v_paso := 4;
      --Modifica el comentario del cliente en caso de que ya exista uno
      update dim_comentarios_clientes
         set com_comentario = x.com_comentario
       where com_nro_gestion = x.com_nro_gestion;

      if sql%notfound then
        v_paso := 5;
        --Inserta el comentario del cliente
        insert into dim_comentarios_clientes
             values (x.com_nro_gestion,
                     x.com_comentario);
      end if;

      v_paso := 6;
      utl_file.put_line (out_file, x.com_nro_gestion || v_separador || x.com_comentario);

    end loop;

    v_paso := 7;
    utl_file.fclose(out_file);

    commit;

    v_paso := 8;
    v_fecha_fin := sysdate;
    pkg_gc_procesos.sp_procesos(2, 2, v_fecha_inicio, v_fecha_fin, id_proceso);
  exception
    when others then
      v_fecha_fin := sysdate;
      pkg_gc_procesos.sp_procesos(2, 6, v_fecha_inicio, v_fecha_fin, id_proceso);
      v_err_sql := substr (sqlerrm (sqlcode), 1, 250);
      pkg_gc_procesos.sp_inconsistencias(id_proceso, sysdate, v_paso, v_err_sql);
  end sp_comentarios_cliente;

/******************************************************************************/
---------------------------------- SP_VISA -------------------------------------
/******************************************************************************/
  procedure sp_visa (id_proceso in number, fec_desde in date, fec_hasta in date) as
  begin
    v_fecha_inicio := sysdate;
    pkg_gc_procesos.sp_procesos(1, 1, v_fecha_inicio, null, id_proceso);

    v_paso := 1;
    --Se replican los datos en la tabla IG_INFORMACION_ADJUNTA
    insert into ig_informacion_adjunta (cod_entidad,ide_gestion_sector,ide_gestion_nro,cod_actor,nro_orden,indi_tpo_info,cod_info_doc_reque,dato_info_doc_reque,link_inf_adjunta,fec_inf_adjunta,user_inf_adjunta,cod_sect_inf_adjunta,nom_archivo_original,nom_archivo,nom_directorio,tamano,secuencia)
      select cod_entidad,ide_gestion_sector,ide_gestion_nro,cod_actor,nro_orden,indi_tpo_info,cod_info_doc_reque,dato_info_doc_reque,link_inf_adjunta,fec_inf_adjunta,user_inf_adjunta,cod_sect_inf_adjunta,nom_archivo_original,nom_archivo,nom_directorio,tamano,secuencia
        from gec01.informacion_adjunta
       where fec_inf_adjunta between fec_desde and fec_hasta
         and cod_info_doc_reque = 2905
         and dato_info_doc_reque = 'V';

    v_paso := 2;
    --Se insertan los datos necesarios en la tabla INT_GESTIONES_VISA
    insert into int_gestiones_visa
      select ide_gestion_nro, fec_desde , 'N', null
        from ig_informacion_adjunta;

    v_paso := 3;
    --out_file := null;
    --out_file := utl_file.fopen (v_path_utl, 'DIM_GESTIONES_VISA.txt', 'w');

    for x in (select distinct visa_nro_gestion, visa_fec_act, visa_procesada, visa_fecha_proceso from int_gestiones_visa) loop
      v_paso := 4;
      --Modifica la gestion en caso de que ya exista uno
      update dim_gestiones_visa
         set visa_fec_act = x.visa_fec_act,
             visa_procesada = x.visa_procesada,
             visa_fecha_proceso = x.visa_fecha_proceso
       where visa_nro_gestion = x.visa_nro_gestion;

      if sql%notfound then
        v_paso := 5;
        --Inserta la gestion
        insert into dim_gestiones_visa
             values (x.visa_nro_gestion,
                     x.visa_fec_act,
                     x.visa_procesada,
                     x.visa_fecha_proceso);
      end if;

      v_paso := 6;
      --utl_file.put_line (out_file, x.visa_nro_gestion || v_separador || x.visa_fec_act || v_separador || x.visa_procesada || v_separador || x.visa_fecha_proceso);

    end loop;

    v_paso := 7;
    --utl_file.fclose(out_file);

    commit;

    v_paso := 8;
    v_fecha_fin := sysdate;
    pkg_gc_procesos.sp_procesos(2, 2, v_fecha_inicio, v_fecha_fin, id_proceso);
  exception
    when others then
      v_fecha_fin := sysdate;
      pkg_gc_procesos.sp_procesos(2, 6, v_fecha_inicio, v_fecha_fin, id_proceso);
      v_err_sql := substr (sqlerrm (sqlcode), 1, 250);
      pkg_gc_procesos.sp_inconsistencias(id_proceso, sysdate, v_paso, v_err_sql);
  end sp_visa;

/******************************************************************************/
------------------------- SP_CARTAS_PENDIENTES ---------------------------------
/******************************************************************************/
  procedure sp_cartas_pendientes (id_proceso in number, fec_desde in date, fec_hasta in date) as
  begin
    v_fecha_inicio := sysdate;
    pkg_gc_procesos.sp_procesos(1, 1, v_fecha_inicio, null, id_proceso);

    v_paso := 1;
    --Se replican los datos en la tabla IG_GC_CARTAS_PENDIENTES
    insert into ig_gc_cartas_pendientes (cod_entidad,ide_gestion_sector,ide_gestion_nro,cliente,estado_tarea,estado_fecha,cuerpo_carta)
      select cod_entidad,ide_gestion_sector,ide_gestion_nro,cliente,estado_tarea,estado_fecha,cuerpo_carta
        from gec01.gc_cartas_pendientes
       where estado_fecha between fec_desde and fec_hasta;

    v_paso := 2;
    --Se insertan los datos necesarios en la tabla INT_CARTAS_PENDIENTES
    insert into int_cartas_pendientes
      select ide_gestion_nro,cliente,estado_tarea,estado_fecha,cuerpo_carta
        from ig_gc_cartas_pendientes;

    v_paso := 3;
    out_file := null;
    out_file := utl_file.fopen (v_path_utl, 'DIM_CARTAS_PENDIENTES.txt', 'w',v_line_max);

    for x in (select distinct cpe_nro_gestion,cpe_cliente,cpe_estado_tarea,cpe_estado_fecha,cpe_cuerpo_carta
                         from int_cartas_pendientes) loop
      v_paso := 4;
      --Modifica la gestion en caso de que ya exista uno
      update dim_cartas_pendientes
         set cpe_cliente = x.cpe_cliente,
             cpe_estado_fecha = x.cpe_estado_fecha,
             cpe_cuerpo_carta = x.cpe_cuerpo_carta
       where cpe_nro_gestion = x.cpe_nro_gestion
         and cpe_estado_tarea = x.cpe_estado_tarea;

      if sql%notfound then
        v_paso := 5;
        --Inserta la gestion
        insert into dim_cartas_pendientes
             values (x.cpe_nro_gestion,
                     x.cpe_cliente,
                     x.cpe_estado_tarea,
                     x.cpe_estado_fecha,
                     x.cpe_cuerpo_carta);
      end if;

      v_paso := 6;
      utl_file.put_line (out_file, x.cpe_nro_gestion || v_separador || x.cpe_cliente || v_separador || x.cpe_estado_tarea || v_separador || x.cpe_estado_fecha || v_separador || x.cpe_cuerpo_carta);

    end loop;

    v_paso := 7;
    utl_file.fclose(out_file);

    commit;

    v_paso := 8;
    v_fecha_fin := sysdate;
    pkg_gc_procesos.sp_procesos(2, 2, v_fecha_inicio, v_fecha_fin, id_proceso);
  exception
    when others then
      v_fecha_fin := sysdate;
      pkg_gc_procesos.sp_procesos(2, 6, v_fecha_inicio, v_fecha_fin, id_proceso);
      v_err_sql := substr (sqlerrm (sqlcode), 1, 250);
      pkg_gc_procesos.sp_inconsistencias(id_proceso, sysdate, v_paso, v_err_sql);
  end sp_cartas_pendientes;

/******************************************************************************/
----------------------------- SP_MENSAJES_MYA ----------------------------------
/******************************************************************************/
  procedure sp_mensajes_mya (id_proceso in number, fec_desde in date, fec_hasta in date) as
  begin
    v_fecha_inicio := sysdate;
    pkg_gc_procesos.sp_procesos(1, 1, v_fecha_inicio, null, id_proceso);

    v_paso := 1;
    --Se replican los datos en la tabla IG_GC_MENSAJES_MYA
    insert into ig_gc_mensajes_mya (cod_entidad,ide_gestion_sector,ide_gestion_nro,tipo_msj,est_msj,fec_msj,id_mq_msj,destino_msj,cod_template_mail,mail_completo)
      select cod_entidad,ide_gestion_sector,ide_gestion_nro,tipo_msj,est_msj,fec_msj,id_mq_msj,destino_msj,cod_template_mail,mail_completo
        from gec01.gc_mensajes_mya
       where fec_msj between fec_desde and fec_hasta;

    v_paso := 2;
    --Se insertan los datos necesarios en la tabla INT_MENSAJES_MYA
    insert into int_mensajes_mya
      select ide_gestion_nro,fec_msj,destino_msj,mail_completo
        from ig_gc_mensajes_mya;

    v_paso := 3;
    out_file := null;
    out_file := utl_file.fopen (v_path_utl, 'DIM_MENSAJES_MYA.txt', 'w',v_line_max);

    for x in (select distinct mya_nro_gestion,mya_fec_msj,mya_destino_msj,mya_mail_completo from int_mensajes_mya) loop
      v_paso := 4;
      --Modifica la gestion en caso de que ya exista uno
      update dim_mensajes_mya
         set mya_fec_msj = x.mya_fec_msj,
             mya_destino_msj = x.mya_destino_msj,
             mya_mail_completo = x.mya_mail_completo
       where mya_nro_gestion = x.mya_nro_gestion;

      if sql%notfound then
        v_paso := 5;
        --Inserta la gestion
        insert into dim_mensajes_mya
             values (x.mya_nro_gestion,
                     x.mya_fec_msj,
                     x.mya_destino_msj,
                     x.mya_mail_completo);
      end if;

      v_paso := 6;
      utl_file.put_line (out_file, x.mya_nro_gestion || v_separador || x.mya_fec_msj || v_separador || x.mya_destino_msj || v_separador || replace(replace(x.mya_mail_completo,chr(10),''),chr(13),''));

    end loop;

    v_paso := 7;
    utl_file.fclose(out_file);

    commit;

    v_paso := 8;
    v_fecha_fin := sysdate;
    pkg_gc_procesos.sp_procesos(2, 2, v_fecha_inicio, v_fecha_fin, id_proceso);
  exception
    when others then
      v_fecha_fin := sysdate;
      pkg_gc_procesos.sp_procesos(2, 6, v_fecha_inicio, v_fecha_fin, id_proceso);
      v_err_sql := substr (sqlerrm (sqlcode), 1, 250);
      pkg_gc_procesos.sp_inconsistencias(id_proceso, sysdate, v_paso, v_err_sql);
  end sp_mensajes_mya;

/******************************************************************************/
------------------------- SP_GESTION_CARACTERISTICAS ---------------------------
/******************************************************************************/
  procedure sp_gestion_caracteristicas (id_proceso in number, fec_desde in date, fec_hasta in date) as
  begin
    v_fecha_inicio := sysdate;
    pkg_gc_procesos.sp_procesos(1, 1, v_fecha_inicio, null, id_proceso);

    v_paso := 1;
    --Se replican los datos en las tablas IG_TIPO_CARACTERISTICA y IG_GESTION_CARACTERISTICA
    insert into ig_tipo_caracteristica (ide_caracteristica,desc_caracteristica)
      select ide_caracteristica,desc_caracteristica
        from gec01.tipo_caracteristica;

    v_paso := 2;
    /*insert into ig_gestion_caracteristica (cod_entidad,ide_gestion_sector,ide_gestion_nro,ide_caracteristica,desc_valor,fecha_estado,user_estado)
      select cod_entidad,ide_gestion_sector,ide_gestion_nro,ide_caracteristica,desc_valor,fecha_estado,user_estado
        from gec01.gestion_caracteristica
       where fecha_estado between fec_desde and fec_hasta;*/
    out_file := null;
    out_file_1 := null;

    v_paso := 3;
    out_file := utl_file.fopen (v_path_utl, 'DIM_TIPO_CARACTERISTICA.txt', 'w',v_line_max);
    out_file_1 := utl_file.fopen (v_path_utl, 'DIM_GESTIONES_CARACTERISTICAS.txt', 'w',v_line_max);

    v_paso := 4;
    --Se insertan los datos necesarios en las tablas INT_TIPO_CARACTERISTICA y INT_GESTIONES_CARACTERISTICAS
    insert into int_tipo_caracteristica
      select ide_caracteristica,desc_caracteristica
        from ig_tipo_caracteristica;

    v_paso := 5;
    insert into int_gestiones_caracteristicas
      select ide_gestion_sector,ide_gestion_nro,ide_caracteristica,desc_valor,fecha_estado,user_estado
        from ig_gestion_caracteristica;

    for x in (select car_cod,car_desc from int_tipo_caracteristica) loop
      v_paso := 5;
      --Modifica la gestion en caso de que ya exista uno
      update dim_tipo_caracteristica
         set car_desc = x.car_desc
       where car_cod = x.car_cod;

      if sql%notfound then
        v_paso := 6;
        --Inserta la gestion
        insert into dim_tipo_caracteristica
             values (x.car_cod,
                     x.car_desc);
      end if;

      v_paso := 7;
      utl_file.put_line (out_file, x.car_cod || v_separador || x.car_desc);

    end loop;

    for x in (select gcar_sector,gcar_nro_gestion,gcar_cod_caracteristica,gcar_valor,gcar_fecha_estado,gcar_user_estado
                from int_gestiones_caracteristicas) loop

      v_paso := 8;
      --Inserta la gestion/caracteristica
      insert into dim_gestiones_caracteristicas
           values (x.gcar_sector,
                   x.gcar_nro_gestion,
                   x.gcar_cod_caracteristica,
                   x.gcar_valor,
                   x.gcar_fecha_estado,
                   x.gcar_user_estado);

      v_paso := 9;
      utl_file.put_line (out_file_1, x.gcar_sector || v_separador ||
                                     x.gcar_nro_gestion || v_separador ||
                                     x.gcar_cod_caracteristica || v_separador ||
                                     x.gcar_valor || v_separador ||
                                     x.gcar_fecha_estado || v_separador ||
                                     x.gcar_user_estado);

    end loop;

    v_paso := 10;
    utl_file.fclose(out_file);
    utl_file.fclose(out_file_1);

    commit;

    v_paso := 11;
    v_fecha_fin := sysdate;
    pkg_gc_procesos.sp_procesos(2, 2, v_fecha_inicio, v_fecha_fin, id_proceso);
  exception
    when others then
      v_fecha_fin := sysdate;
      pkg_gc_procesos.sp_procesos(2, 6, v_fecha_inicio, v_fecha_fin, id_proceso);
      v_err_sql := substr (sqlerrm (sqlcode), 1, 250);
      pkg_gc_procesos.sp_inconsistencias(id_proceso, sysdate, v_paso, v_err_sql);
  end sp_gestion_caracteristicas;

end pkg_gc_dimensiones;
/
