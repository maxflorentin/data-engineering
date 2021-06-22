CREATE OR REPLACE PACKAGE BODY GEC02.PKG_GC_FACT AS

  v_fecha_inicio        date;
  v_fecha_fin           date;
  v_err_sql             varchar2(2000);
  v_paso                number := 0;
  v_cantidad_error      number := 0;
  resul                 number := 0;
  v_cantidad_trac_padre number;
  v_favorabilidad       number;
  v_gestion_padre       gec01.gestiones.ide_gestion_nro%type;

/******************************************************************************/
---------------------------------- SP_FACT -------------------------------------
/******************************************************************************/
  procedure sp_fact (id_proceso in number, fec_desde in date, fec_hasta in date) as
  begin
    v_fecha_inicio := sysdate;
    pkg_gc_procesos.sp_procesos(1, 1, v_fecha_inicio, NULL, id_proceso);

    v_paso := 1;
    --Se busca si hay procesos con error ya que esto condiciona la ejecucion de la tabla de hechos
    pkg_gc_procesos.sp_busca_procesos_error(v_cantidad_error);

    if v_cantidad_error > 0 then
      v_paso := 2;
      v_fecha_fin := sysdate;
      pkg_gc_procesos.sp_procesos(3, 0, v_fecha_inicio, v_fecha_fin, id_proceso);
      pkg_gc_procesos.sp_inconsistencias(id_proceso, sysdate, v_paso, 'Existen procesos con error que impiden la ejecucion de la tabla de hechos.');
    else
      v_paso := 3;
      --Se replican los datos de las tablas involucradas en el calculo de la tabla de hechos
      insert into ig_gestion_estados (cod_entidad,ide_gestion_sector,ide_gestion_nro,cod_est_gest,fec_est_gest,tpo_medio,cod_rta_resol_predef,cod_contc,cod_niv_conf,indi_impre_masiva,imp_gest_estado,cod_mone_imp,cod_carta,cod_sect_asign_espec,tmp_est_gest,user_estado,cod_sect_estado,cod_bandeja,orden_estado,cod_responsab_est,actor_nro_orden,comentario,ide_gestion_sec,ind_modf_fec_resol)
           select est.cod_entidad,est.ide_gestion_sector,est.ide_gestion_nro,est.cod_est_gest,est.fec_est_gest,est.tpo_medio,est.cod_rta_resol_predef,est.cod_contc,est.cod_niv_conf,est.indi_impre_masiva,est.imp_gest_estado,est.cod_mone_imp,est.cod_carta,est.cod_sect_asign_espec,est.tmp_est_gest,est.user_estado,est.cod_sect_estado,est.cod_bandeja,est.orden_estado,est.cod_responsab_est,est.actor_nro_orden,est.comentario,est.ide_gestion_sec,est.ind_modf_fec_resol
             from gec01.gestion_estados est, gec01.gestiones ges
            where est.cod_entidad = ges.cod_entidad
              and est.ide_gestion_sector = ges.ide_gestion_sector
              and est.ide_gestion_nro = ges.ide_gestion_nro
              and est.fec_est_gest between fec_desde and fec_hasta
              and ges.ide_gestion_sector not in ('TRAC','PROM');

      insert into ig_gestiones (cod_entidad,ide_gestion_sector,ide_gestion_nro,ide_circuito,indi_tipo_circ,indi_decision_comer,indi_replteo,indi_rta_inmed,indi_impre_cart,imp_autz_solicitado,cod_mone_solicitado,imp_autz_autorizado,cod_mone_autorizado,imp_autz_resolucion,cod_mone_resolucion,tpo_pers,cod_crm,comen_cliente,ide_gest_sector_relac,ide_gest_nro_relac,fec_gestion_alta,cod_bandeja_actual,fec_bandeja_actual,cod_sector_actual,inic_user_alta,indi_mail,indi_prioridad,fec_max_resol,cod_conforme,cod_user_actual,cod_grupo_empresa,cod_tipo_favorabilidad,fec_aviso_venc)
           select distinct ges.cod_entidad,ges.ide_gestion_sector,ges.ide_gestion_nro,ges.ide_circuito,ges.indi_tipo_circ,ges.indi_decision_comer,ges.indi_replteo,ges.indi_rta_inmed,ges.indi_impre_cart,ges.imp_autz_solicitado,ges.cod_mone_solicitado,ges.imp_autz_autorizado,ges.cod_mone_autorizado,ges.imp_autz_resolucion,ges.cod_mone_resolucion,ges.tpo_pers,ges.cod_crm,ges.comen_cliente,ges.ide_gest_sector_relac,ges.ide_gest_nro_relac,ges.fec_gestion_alta,ges.cod_bandeja_actual,ges.fec_bandeja_actual,ges.cod_sector_actual,ges.inic_user_alta,ges.indi_mail,ges.indi_prioridad,ges.fec_max_resol,ges.cod_conforme,ges.cod_user_actual,ges.cod_grupo_empresa,ges.cod_tipo_favorabilidad,ges.fec_aviso_venc
             from gec01.gestiones ges, ig_gestion_estados est
            where ges.cod_entidad = est.cod_entidad
              and ges.ide_gestion_sector = est.ide_gestion_sector
              and ges.ide_gestion_nro = est.ide_gestion_nro;

      insert into ig_gc_gestiones (cod_entidad,ide_gestion_sector,ide_gestion_nro,cod_segm,semf_ingreso_crm,semf_renta_crm,semf_riesgo_crm,calle_dom_contc,nro_dom_contc,piso_dom_contc,dpto_dom_contc,cpost_dom_contc,loc_dom_contc,pcia_dom_contc,pais_dom_contc,ddn_tel_dom_contc,nro_tel_dom_contc,ddn_fax_contc,nro_fax_contc,hra_dom_contc,ddn_celular_contc,nro_celular_contc,ddn_tel_laboral,nro_tel_laboral,hra_tel_laboral,ddn_fax_laboral,nro_fax_laboral,aplic_cta,cod_suc_cta,nro_cta,nro_firm_cta,cod_mone_cta,cod_prod_cta,cod_subpro_cta,nro_tarjeta,link_resumen,dire_mail_contc,dire_mail2_contc,cartera,aplic_cta_gold,cod_suc_cta_gold,nro_cta_gold,nro_firm_cta_gold,cod_mone_cta_gold,cod_prod_cta_gold,cod_subpro_cta_gold,cod_ramo_cta,nro_certificado_cta,rentabilidad_promedio,color_semaforo,color_semaf_riesgo,indi_envios_mya,dire_mail_opcional,nro_paquete,cod_paquete,indi_plan_sueldo,nro_convenio_paquete,emp_celular_contc,tpo_msj_asoc_rta,monto_deuda_ref,no_enviar_notificaciones)
           select distinct gc.cod_entidad,gc.ide_gestion_sector,gc.ide_gestion_nro,gc.cod_segm,gc.semf_ingreso_crm,gc.semf_renta_crm,gc.semf_riesgo_crm,gc.calle_dom_contc,gc.nro_dom_contc,gc.piso_dom_contc,gc.dpto_dom_contc,gc.cpost_dom_contc,gc.loc_dom_contc,gc.pcia_dom_contc,gc.pais_dom_contc,gc.ddn_tel_dom_contc,gc.nro_tel_dom_contc,gc.ddn_fax_contc,gc.nro_fax_contc,gc.hra_dom_contc,gc.ddn_celular_contc,gc.nro_celular_contc,gc.ddn_tel_laboral,gc.nro_tel_laboral,gc.hra_tel_laboral,gc.ddn_fax_laboral,gc.nro_fax_laboral,gc.aplic_cta,gc.cod_suc_cta,gc.nro_cta,gc.nro_firm_cta,gc.cod_mone_cta,gc.cod_prod_cta,gc.cod_subpro_cta,gc.nro_tarjeta,gc.link_resumen,gc.dire_mail_contc,gc.dire_mail2_contc,gc.cartera,gc.aplic_cta_gold,gc.cod_suc_cta_gold,gc.nro_cta_gold,gc.nro_firm_cta_gold,gc.cod_mone_cta_gold,gc.cod_prod_cta_gold,gc.cod_subpro_cta_gold,gc.cod_ramo_cta,gc.nro_certificado_cta,gc.rentabilidad_promedio,gc.color_semaforo,gc.color_semaf_riesgo,gc.indi_envios_mya,gc.dire_mail_opcional,gc.nro_paquete,gc.cod_paquete,gc.indi_plan_sueldo,gc.nro_convenio_paquete,gc.emp_celular_contc,gc.tpo_msj_asoc_rta,gc.monto_deuda_ref,gc.no_enviar_notificaciones
             from gec01.gc_gestiones gc, ig_gestion_estados est
            where gc.cod_entidad = est.cod_entidad
              and gc.ide_gestion_sector = est.ide_gestion_sector
              and gc.ide_gestion_nro = est.ide_gestion_nro;

      insert into ig_gc_individuo_gestiones (cod_entidad,ide_gestion_sector,ide_gestion_nro,tpo_doc_indi,nro_doc_indi,fec_naci_indi,mar_sex_indi,ide_nup_indi)
           select distinct iges.cod_entidad,iges.ide_gestion_sector,iges.ide_gestion_nro,iges.tpo_doc_indi,iges.nro_doc_indi,iges.fec_naci_indi,iges.mar_sex_indi,ind.ide_nup_indi
             from gec01.gc_individuo_gestiones iges,
                  gec01.gc_individuos ind,
                  ig_gestiones ges
            where iges.cod_entidad = ges.cod_entidad
              and iges.ide_gestion_sector = ges.ide_gestion_sector
              and iges.ide_gestion_nro = ges.ide_gestion_nro
              and iges.cod_entidad = ind.cod_entidad
              and iges.tpo_doc_indi = ind.tpo_doc_indi
              and iges.nro_doc_indi = ind.nro_doc_indi
              and iges.fec_naci_indi = ind.fec_naci_indi
              and iges.mar_sex_indi = ind.mar_sex_indi;

      insert into ig_gc_empresa_gestiones (cod_entidad,ide_gestion_sector,ide_gestion_nro,tpo_doc_empr,nro_doc_empr,sec_doc_empr,ide_nup_empr)
           select distinct eges.cod_entidad,eges.ide_gestion_sector,eges.ide_gestion_nro,eges.tpo_doc_empr,eges.nro_doc_empr,eges.sec_doc_empr,emp.ide_nup_empr
             from gec01.gc_empresa_gestiones eges,
                  gec01.gc_empresas emp,
                  ig_gestiones ges
            where eges.cod_entidad = ges.cod_entidad
              and eges.ide_gestion_sector = ges.ide_gestion_sector
              and eges.ide_gestion_nro = ges.ide_gestion_nro
              and eges.cod_entidad = emp.cod_entidad
              and eges.tpo_doc_empr = emp.tpo_doc_empr
              and eges.nro_doc_empr = emp.nro_doc_empr
              and eges.sec_doc_empr = emp.sec_doc_empr;

      insert into ig_reaperturas (cod_entidad,ide_nro_reapertura,nro_orden,ide_gestion_sector,ide_gestion_nro)
        select reap.cod_entidad,reap.ide_nro_reapertura,reap.nro_orden,reap.ide_gestion_sector,reap.ide_gestion_nro
          from gec01.reaperturas reap, ig_gestiones ges
         where reap.cod_entidad = ges.cod_entidad
           and reap.ide_gestion_sector = ges.ide_gestion_sector
           and reap.ide_gestion_nro = ges.ide_gestion_nro;

      v_paso := 4;
      --Se almacena la informacion definitiva en la tabla de interfaz INT_FACT
      insert into int_fact
           select ges.ide_gestion_nro fact_nro_gestion,
                  ges.ide_circuito fact_cod_circuito,
                  ges.indi_prioridad fact_cod_prioridad,
                  decode(ges.cod_tipo_favorabilidad,null,0,ges.cod_tipo_favorabilidad) fact_cod_favorabilidad,
                  gc.cod_segm fact_cod_segmento,
                  decode(gc.cod_paquete,null,0,gc.cod_paquete) fact_cod_paquete,
                  ges.fec_max_resol fact_fec_max_resol,
                  0 fact_ind_dias_gest,
                  decode(ges.imp_autz_solicitado,null,0,ges.imp_autz_solicitado) fact_ind_importe_reclamado,
                  decode(ges.cod_mone_solicitado,null,'NO APLICA',ges.cod_mone_solicitado) fact_ind_moneda_reclamado,
                  decode(ges.imp_autz_autorizado,null,0,ges.imp_autz_autorizado) fact_ind_importe_concedido,
                  decode(ges.cod_mone_autorizado,null,'NO APLICA',ges.cod_mone_autorizado) fact_ind_moneda_concedido,
                  decode(ges.cod_conforme,null,0,ges.cod_conforme) fact_cod_enc,
                  decode(ind.ide_nup_indi,null,emp.nro_doc_empr||emp.ide_nup_empr,ind.nro_doc_indi||ind.ide_nup_indi) fact_cod_cliente,
                  case when reap.nro_orden = 0
                       then null
                       else decode(reap.ide_gestion_nro,null,null,1)
                  end fact_ind_reapertura,
                  0 fact_ultimo_estado,
                  est.cod_est_gest fact_cod_estados,
                  case when est.cod_est_gest = 100
                    then est.tpo_medio
                    else 0
                  end fact_cod_tipo_medio,
                  est.user_estado fact_cod_usuario,
                  est.cod_sect_estado sector,
                  decode(est.cod_responsab_est,null,0,est.cod_responsab_est) fact_cod_responsabilidad,
                  decode(est.cod_carta,null,99999,est.cod_carta) fact_cod_modelo_cartas,
                  est.orden_estado fact_orden_estado,
                  to_char(est.fec_est_gest,'YYYYMMDD') fact_fecha_estado,
                  ges.fec_gestion_alta fact_fec_alta,
                  est.fec_est_gest fact_fec_bandeja_actual,
                  null fact_sla,
                  decode(upper(est.cod_bandeja),'CERRADO',1,0) fact_marca_cerrado,
                  ges.ide_gestion_sector fact_sector_alta,
                  0 fact_cerradas_dev,
                  0 fact_marca_devolucion,
                  to_char(est.fec_est_gest,'YYYYMMDD') || ges.ide_circuito fact_fecha_circuito,
                  0 fact_marca_visa,
                  0 fact_ult_est_mes,
                  est.comentario fact_comentario,
                  null fact_fec_res_resol_1,
                  null fact_fec_res_resol_2,
                  null fact_fec_res_resol_3,
                  null fact_fec_resp,
                  null fact_responsable_resol,
                  null fact_desc_sec_resol_1,
                  null fact_desc_sec_resol_2,
                  null fact_desc_sec_resol_3,
                  null fact_desc_sec_resp,
                  gc.cod_suc_cta fact_cod_suc_cta,
                  ges.cod_crm fact_cod_crm,
                  gc.nro_cta fact_nro_cta,
                  null fact_bandeja_actual,
                  null fact_cod_ult_estado,
                  null fact_desc_ult_estado,
                  null fact_fec_cierre,
                  gc.semf_ingreso_crm fact_ingreso_epa,
                  null fact_resp_de_autoriz,
                  'No' fact_no_autorizado,
                  'No' fact_validacion_rci,
                  lead(est.fec_est_gest,1) over (partition by est.ide_gestion_sector,est.ide_gestion_nro order by est.orden_estado) fact_fecha_siguiente,
                  0 fact_ind_dias_x_estado,
                  null fact_mes_anio_cierre,
                  null fact_resp_de_rta,
                  'No es reapertura' fact_gestion_inicial,
                  'No es reapertura' fact_gestion_anterior,
                  0 fact_cant_reaperturas,
                  0 fact_plazo_legal,
                  est.cod_rta_resol_predef,
                  0 fact_nro_ticket,
                  0 fact_prefijo_ticket
             from ig_gestion_estados est,
                  ig_gestiones ges,
                  ig_gc_gestiones gc,
                  ig_gc_individuo_gestiones ind,
                  ig_gc_empresa_gestiones emp,
                  ig_reaperturas reap
            where est.cod_entidad = gc.cod_entidad
              and est.ide_gestion_nro = gc.ide_gestion_nro
              and est.cod_entidad = ind.cod_entidad(+)
              and est.ide_gestion_nro = ind.ide_gestion_nro(+)
              and est.cod_entidad = emp.cod_entidad(+)
              and est.ide_gestion_nro = emp.ide_gestion_nro(+)
              and est.cod_entidad = reap.cod_entidad(+)
              and est.ide_gestion_nro = reap.ide_gestion_nro(+)
              and est.cod_entidad = ges.cod_entidad
              and est.ide_gestion_sector = ges.ide_gestion_sector
              and est.ide_gestion_nro = ges.ide_gestion_nro
              --and ges.ide_gestion_sector not in ('TRAC','PROM')
            order by ges.ide_gestion_nro, est.orden_estado;

      v_paso := 5;
      --Se copia la informacion a la tabla FACT_GESTIONES visible desde el modelo de SAP BO
      insert into fact_gestiones
           select * from int_fact;
      commit;

      v_fecha_fin := sysdate;
      pkg_gc_procesos.sp_procesos(2, 2, v_fecha_inicio, v_fecha_fin, id_proceso);
    end if;
  exception
    when others then
      v_fecha_fin := sysdate;
      pkg_gc_procesos.sp_procesos(2, 6, v_fecha_inicio, v_fecha_fin, id_proceso);
      v_err_sql := substr (sqlerrm (sqlcode), 1, 250);
      pkg_gc_procesos.sp_inconsistencias(id_proceso, sysdate, v_paso, v_err_sql);
  end sp_fact;

/******************************************************************************/
------------------------ SP_FACT (TRAC - PROM) ---------------------------------
/******************************************************************************/
  procedure sp_fact_trac_prom (id_proceso in number, fec_desde in date, fec_hasta in date) as
  begin
    v_fecha_inicio := sysdate;
    pkg_gc_procesos.sp_procesos(1, 1, v_fecha_inicio, NULL, id_proceso);

    v_paso := 1;
    --Se busca si hay procesos con error ya que esto condiciona la ejecucion de la tabla de hechos
    pkg_gc_procesos.sp_busca_procesos_error(v_cantidad_error);

    if v_cantidad_error > 0 then
      v_paso := 2;
      v_fecha_fin := sysdate;
      pkg_gc_procesos.sp_procesos(3, 0, v_fecha_inicio, v_fecha_fin, id_proceso);
      pkg_gc_procesos.sp_inconsistencias(id_proceso, sysdate, v_paso, 'Existen procesos con error que impiden la ejecucion de la tabla de hechos (TRAC - PROM).');
    else
      v_paso := 3;
      --Se replican los datos de las tablas involucradas en el calculo de la tabla de hechos
      insert into ig_gestion_estados (cod_entidad,ide_gestion_sector,ide_gestion_nro,cod_est_gest,fec_est_gest,tpo_medio,cod_rta_resol_predef,cod_contc,cod_niv_conf,indi_impre_masiva,imp_gest_estado,cod_mone_imp,cod_carta,cod_sect_asign_espec,tmp_est_gest,user_estado,cod_sect_estado,cod_bandeja,orden_estado,cod_responsab_est,actor_nro_orden,comentario,ide_gestion_sec,ind_modf_fec_resol)
           select est.cod_entidad,est.ide_gestion_sector,est.ide_gestion_nro,est.cod_est_gest,est.fec_est_gest,est.tpo_medio,est.cod_rta_resol_predef,est.cod_contc,est.cod_niv_conf,est.indi_impre_masiva,est.imp_gest_estado,est.cod_mone_imp,est.cod_carta,est.cod_sect_asign_espec,est.tmp_est_gest,est.user_estado,est.cod_sect_estado,est.cod_bandeja,est.orden_estado,est.cod_responsab_est,est.actor_nro_orden,est.comentario,est.ide_gestion_sec,est.ind_modf_fec_resol
             from gec01.gestion_estados est, gec01.gestiones ges
            where est.cod_entidad = ges.cod_entidad
              and est.ide_gestion_sector = ges.ide_gestion_sector
              and est.ide_gestion_nro = ges.ide_gestion_nro
              and est.fec_est_gest between fec_desde and fec_hasta
              and ges.ide_gestion_sector in ('TRAC','PROM')
              and ges.ide_gestion_nro not in (select distinct visa_nro_gestion
                                                from dim_gestiones_visa
                                               where visa_fec_act between fec_desde and fec_hasta);

      insert into ig_gc_gestiones (cod_entidad,ide_gestion_sector,ide_gestion_nro,cod_segm,semf_ingreso_crm,semf_renta_crm,semf_riesgo_crm,calle_dom_contc,nro_dom_contc,piso_dom_contc,dpto_dom_contc,cpost_dom_contc,loc_dom_contc,pcia_dom_contc,pais_dom_contc,ddn_tel_dom_contc,nro_tel_dom_contc,ddn_fax_contc,nro_fax_contc,hra_dom_contc,ddn_celular_contc,nro_celular_contc,ddn_tel_laboral,nro_tel_laboral,hra_tel_laboral,ddn_fax_laboral,nro_fax_laboral,aplic_cta,cod_suc_cta,nro_cta,nro_firm_cta,cod_mone_cta,cod_prod_cta,cod_subpro_cta,nro_tarjeta,link_resumen,dire_mail_contc,dire_mail2_contc,cartera,aplic_cta_gold,cod_suc_cta_gold,nro_cta_gold,nro_firm_cta_gold,cod_mone_cta_gold,cod_prod_cta_gold,cod_subpro_cta_gold,cod_ramo_cta,nro_certificado_cta,rentabilidad_promedio,color_semaforo,color_semaf_riesgo,indi_envios_mya,dire_mail_opcional,nro_paquete,cod_paquete,indi_plan_sueldo,nro_convenio_paquete,emp_celular_contc,tpo_msj_asoc_rta,monto_deuda_ref,no_enviar_notificaciones)
           select distinct gc.cod_entidad,gc.ide_gestion_sector,gc.ide_gestion_nro,gc.cod_segm,gc.semf_ingreso_crm,gc.semf_renta_crm,gc.semf_riesgo_crm,gc.calle_dom_contc,gc.nro_dom_contc,gc.piso_dom_contc,gc.dpto_dom_contc,gc.cpost_dom_contc,gc.loc_dom_contc,gc.pcia_dom_contc,gc.pais_dom_contc,gc.ddn_tel_dom_contc,gc.nro_tel_dom_contc,gc.ddn_fax_contc,gc.nro_fax_contc,gc.hra_dom_contc,gc.ddn_celular_contc,gc.nro_celular_contc,gc.ddn_tel_laboral,gc.nro_tel_laboral,gc.hra_tel_laboral,gc.ddn_fax_laboral,gc.nro_fax_laboral,gc.aplic_cta,gc.cod_suc_cta,gc.nro_cta,gc.nro_firm_cta,gc.cod_mone_cta,gc.cod_prod_cta,gc.cod_subpro_cta,gc.nro_tarjeta,gc.link_resumen,gc.dire_mail_contc,gc.dire_mail2_contc,gc.cartera,gc.aplic_cta_gold,gc.cod_suc_cta_gold,gc.nro_cta_gold,gc.nro_firm_cta_gold,gc.cod_mone_cta_gold,gc.cod_prod_cta_gold,gc.cod_subpro_cta_gold,gc.cod_ramo_cta,gc.nro_certificado_cta,gc.rentabilidad_promedio,gc.color_semaforo,gc.color_semaf_riesgo,gc.indi_envios_mya,gc.dire_mail_opcional,gc.nro_paquete,gc.cod_paquete,gc.indi_plan_sueldo,gc.nro_convenio_paquete,gc.emp_celular_contc,gc.tpo_msj_asoc_rta,gc.monto_deuda_ref,gc.no_enviar_notificaciones
             from gec01.gc_gestiones gc, ig_gestion_estados est
            where gc.cod_entidad = est.cod_entidad
              and gc.ide_gestion_sector = est.ide_gestion_sector
              and gc.ide_gestion_nro = est.ide_gestion_nro;

      insert into ig_gestiones (cod_entidad,ide_gestion_sector,ide_gestion_nro,ide_circuito,indi_tipo_circ,indi_decision_comer,indi_replteo,indi_rta_inmed,indi_impre_cart,imp_autz_solicitado,cod_mone_solicitado,imp_autz_autorizado,cod_mone_autorizado,imp_autz_resolucion,cod_mone_resolucion,tpo_pers,cod_crm,comen_cliente,ide_gest_sector_relac,ide_gest_nro_relac,fec_gestion_alta,cod_bandeja_actual,fec_bandeja_actual,cod_sector_actual,inic_user_alta,indi_mail,indi_prioridad,fec_max_resol,cod_conforme,cod_user_actual,cod_grupo_empresa,cod_tipo_favorabilidad,fec_aviso_venc)
           select distinct ges.cod_entidad,ges.ide_gestion_sector,ges.ide_gestion_nro,ges.ide_circuito,ges.indi_tipo_circ,ges.indi_decision_comer,ges.indi_replteo,ges.indi_rta_inmed,ges.indi_impre_cart,ges.imp_autz_solicitado,ges.cod_mone_solicitado,ges.imp_autz_autorizado,ges.cod_mone_autorizado,ges.imp_autz_resolucion,ges.cod_mone_resolucion,ges.tpo_pers,ges.cod_crm,ges.comen_cliente,ges.ide_gest_sector_relac,ges.ide_gest_nro_relac,ges.fec_gestion_alta,ges.cod_bandeja_actual,ges.fec_bandeja_actual,ges.cod_sector_actual,ges.inic_user_alta,ges.indi_mail,ges.indi_prioridad,ges.fec_max_resol,ges.cod_conforme,ges.cod_user_actual,ges.cod_grupo_empresa,ges.cod_tipo_favorabilidad,ges.fec_aviso_venc
             from gec01.gestiones ges, ig_gestion_estados est
            where ges.cod_entidad = est.cod_entidad
              and ges.ide_gestion_sector = est.ide_gestion_sector
              and ges.ide_gestion_nro = est.ide_gestion_nro;

      insert into ig_gc_individuo_gestiones (cod_entidad,ide_gestion_sector,ide_gestion_nro,tpo_doc_indi,nro_doc_indi,fec_naci_indi,mar_sex_indi,ide_nup_indi)
           select distinct iges.cod_entidad,iges.ide_gestion_sector,iges.ide_gestion_nro,iges.tpo_doc_indi,iges.nro_doc_indi,iges.fec_naci_indi,iges.mar_sex_indi,ind.ide_nup_indi
             from gec01.gc_individuo_gestiones iges,
                  gec01.gc_individuos ind,
                  ig_gestiones ges
            where iges.cod_entidad = ges.cod_entidad
              and iges.ide_gestion_sector = ges.ide_gestion_sector
              and iges.ide_gestion_nro = ges.ide_gestion_nro
              and iges.cod_entidad = ind.cod_entidad
              and iges.tpo_doc_indi = ind.tpo_doc_indi
              and iges.nro_doc_indi = ind.nro_doc_indi
              and iges.fec_naci_indi = ind.fec_naci_indi
              and iges.mar_sex_indi = ind.mar_sex_indi;

      insert into ig_gc_empresa_gestiones (cod_entidad,ide_gestion_sector,ide_gestion_nro,tpo_doc_empr,nro_doc_empr,sec_doc_empr,ide_nup_empr)
           select distinct eges.cod_entidad,eges.ide_gestion_sector,eges.ide_gestion_nro,eges.tpo_doc_empr,eges.nro_doc_empr,eges.sec_doc_empr,emp.ide_nup_empr
             from gec01.gc_empresa_gestiones eges,
                  gec01.gc_empresas emp,
                  ig_gestiones ges
            where eges.cod_entidad = ges.cod_entidad
              and eges.ide_gestion_sector = ges.ide_gestion_sector
              and eges.ide_gestion_nro = ges.ide_gestion_nro
              and eges.cod_entidad = emp.cod_entidad
              and eges.tpo_doc_empr = emp.tpo_doc_empr
              and eges.nro_doc_empr = emp.nro_doc_empr
              and eges.sec_doc_empr = emp.sec_doc_empr;

      insert into ig_reaperturas (cod_entidad,ide_nro_reapertura,nro_orden,ide_gestion_sector,ide_gestion_nro)
        select reap.cod_entidad,reap.ide_nro_reapertura,reap.nro_orden,reap.ide_gestion_sector,reap.ide_gestion_nro
          from gec01.reaperturas reap, ig_gestiones ges
         where reap.cod_entidad = ges.cod_entidad
           and reap.ide_gestion_sector = ges.ide_gestion_sector
           and reap.ide_gestion_nro = ges.ide_gestion_nro;

      v_paso := 4;
      --Logica para relacionar gestiones "padre" con gestiones "hijo"
      for x in (select ind.ide_nup_indi nup,
                       ges.ide_gestion_sector sector,
                       ges.ide_gestion_nro gestion_hijo,
                       case when to_date(ges.fec_bandeja_actual,'DD/MM/YY') > to_date(fec_hasta,'DD/MM/YY')
                             and upper(ges.cod_bandeja_actual) = 'CERRADO'
                            then 'CerradoTMP'
                            else trim(ges.cod_bandeja_actual)
                       end bandeja_actual,
                       ges.fec_gestion_alta fecha_alta,
                       ges.cod_tipo_favorabilidad favorabilidad
                  from ig_gestiones ges,
                       ig_gc_individuo_gestiones ind
                 where ges.ide_gestion_sector in ('TRAC','PROM')
                   and ges.cod_entidad = ind.cod_entidad
                   and ges.ide_gestion_sector = ind.ide_gestion_sector
                   and ges.ide_gestion_nro = ind.ide_gestion_nro
                 order by 1, 2) loop

        --Se modifica la gestion para cambiar su estado y/o favorabilidad en caso de que ya este insertada
        update dim_gestiones_trac
           set trac_bandeja_actual = trim(x.bandeja_actual),
               trac_favorabilidad = x.favorabilidad,
               trac_fecha_alta = x.fecha_alta,
               trac_sector_ingreso = x.sector,
               trac_nup = x.nup
         where trac_gestion_hijo = x.gestion_hijo;

        --Se inserta la gestion ya que no se pudo modificar porque no existe
        if sql%notfound then
          --Se busca la gestion "padre"
          select min(ges.ide_gestion_nro)
            into v_gestion_padre
            from ig_gestiones ges,
                 ig_gc_individuo_gestiones ind
           where ges.ide_gestion_sector = x.sector
             and ges.cod_entidad = ind.cod_entidad
             and ges.ide_gestion_sector = ind.ide_gestion_sector
             and ges.ide_gestion_nro = ind.ide_gestion_nro
             and ind.ide_nup_indi = x.nup
             and to_date(ges.fec_gestion_alta,'DD/MM/YY') = to_date(x.fecha_alta,'DD/MM/YY')
           group by ges.ide_gestion_sector,
                    ind.ide_nup_indi,
                    to_date(ges.fec_gestion_alta,'DD/MM/YY');

          insert into dim_gestiones_trac
               values (v_gestion_padre,
                       x.gestion_hijo,
                       x.bandeja_actual,
                       x.fecha_alta,
                       x.favorabilidad,
                       x.sector,
                       x.nup);
        end if;
      end loop;

      v_paso := 5;
      --Logica para insertar unicamente una gestion del grupo de gestiones de transacciones cuestionadas
      for y in (select ind.ide_nup_indi nup,
                       to_date(ges.fec_gestion_alta,'DD/MM/YY'),
                       ges.ide_gestion_sector sector,
                       min(ges.ide_gestion_nro) gestion_padre
                  from ig_gestiones ges,
                       ig_gc_individuo_gestiones ind
                 where ges.ide_gestion_sector in ('TRAC','PROM')
                   and ges.cod_entidad = ind.cod_entidad
                   and ges.ide_gestion_sector = ind.ide_gestion_sector
                   and ges.ide_gestion_nro = ind.ide_gestion_nro
                 group by ind.ide_nup_indi,
                          to_date(ges.fec_gestion_alta,'DD/MM/YY'),
                          ges.ide_gestion_sector
                 order by 1, 2) loop

        --Se insertan los nuevos movimientos de la gestion "padre"
        insert into int_fact
             select ges.ide_gestion_nro fact_nro_gestion,
                    ges.ide_circuito fact_cod_circuito,
                    ges.indi_prioridad fact_cod_prioridad,
                    0 fact_cod_favorabilidad, --la favoribilidad depende de todas las gestiones relacionadas
                    gc.cod_segm fact_cod_segmento,
                    decode(gc.cod_paquete,null,0,gc.cod_paquete) fact_cod_paquete,
                    ges.fec_max_resol fact_fec_max_resol,
                    0 fact_ind_dias_gest,
                    decode(ges.imp_autz_solicitado,null,0,ges.imp_autz_solicitado) fact_ind_importe_reclamado,
                    decode(ges.cod_mone_solicitado,null,'NO APLICA',ges.cod_mone_solicitado) fact_ind_moneda_reclamado,
                    decode(ges.imp_autz_autorizado,null,0,ges.imp_autz_autorizado) fact_ind_importe_concedido,
                    decode(ges.cod_mone_autorizado,null,'NO APLICA',ges.cod_mone_autorizado) fact_ind_moneda_concedido,
                    decode(ges.cod_conforme,null,0,ges.cod_conforme) fact_cod_enc,
                    decode(ind.ide_nup_indi,null,emp.nro_doc_empr||emp.ide_nup_empr,ind.nro_doc_indi||ind.ide_nup_indi) fact_cod_cliente,
                    case when reap.nro_orden = 0
                         then null
                         else decode(reap.ide_gestion_nro,null,null,1)
                    end fact_ind_reapertura,
                    0 fact_ultimo_estado,
                    est.cod_est_gest fact_cod_estados,
                    case when est.cod_est_gest = 100
                      then est.tpo_medio
                      else 0
                    end fact_cod_tipo_medio,
                    est.user_estado fact_cod_usuario,
                    est.cod_sect_estado sector,
                    decode(est.cod_responsab_est,null,0,est.cod_responsab_est) fact_cod_responsabilidad,
                    decode(est.cod_carta,null,99999,est.cod_carta) fact_cod_modelo_cartas,
                    est.orden_estado fact_orden_estado,
                    to_char(est.fec_est_gest,'YYYYMMDD') fact_fecha_estado,
                    ges.fec_gestion_alta fact_fec_alta,
                    est.fec_est_gest fact_fec_bandeja_actual,
                    null fact_sla,
                    0 fact_marca_cerrado, --el estado cerrado depende de todas las gestiones relacionadas
                    ges.ide_gestion_sector fact_sector_alta,
                    0 fact_cerradas_dev,
                    0 fact_marca_devolucion,
                    to_char(est.fec_est_gest,'YYYYMMDD') || ges.ide_circuito fact_fecha_circuito,
                    0 fact_marca_visa,
                    0 fact_ult_est_mes,
                    est.comentario fact_comentario,
                    null fact_fec_res_resol_1,
                    null fact_fec_res_resol_2,
                    null fact_fec_res_resol_3,
                    null fact_fec_resp,
                    null fact_responsable_resol,
                    null fact_desc_sec_resol_1,
                    null fact_desc_sec_resol_2,
                    null fact_desc_sec_resol_3,
                    null fact_desc_sec_resp,
                    gc.cod_suc_cta fact_cod_suc_cta,
                    ges.cod_crm fact_cod_crm,
                    gc.nro_cta fact_nro_cta,
                    null fact_bandeja_actual,
                    null fact_cod_ult_estado,
                    null fact_desc_ult_estado,
                    null fact_fec_cierre,
                    gc.semf_ingreso_crm fact_ingreso_epa,
                    null fact_resp_de_autoriz,
                    'No' fact_no_autorizado,
                    'No' fact_validacion_rci,
                    lead(est.fec_est_gest,1) over (partition by est.ide_gestion_sector,est.ide_gestion_nro order by est.orden_estado) fact_fecha_siguiente,
                    0 fact_ind_dias_x_estado,
                    null fact_mes_anio_cierre,
                    null fact_resp_de_rta,
                    'No es reapertura' fact_gestion_inicial,
                    'No es reapertura' fact_gestion_anterior,
                    0 fact_cant_reaperturas,
                    0 fact_plazo_legal,
                    est.cod_rta_resol_predef,
                    0 fact_nro_ticket,
                    0 fact_prefijo_ticket
               from ig_gestion_estados est,
                    ig_gestiones ges,
                    ig_gc_gestiones gc,
                    ig_gc_individuo_gestiones ind,
                    ig_gc_empresa_gestiones emp,
                    ig_reaperturas reap
              where est.cod_entidad = gc.cod_entidad
                and est.ide_gestion_nro = gc.ide_gestion_nro
                and est.cod_entidad = ind.cod_entidad(+)
                and est.ide_gestion_nro = ind.ide_gestion_nro(+)
                and est.cod_entidad = emp.cod_entidad(+)
                and est.ide_gestion_nro = emp.ide_gestion_nro(+)
                and est.cod_entidad = reap.cod_entidad(+)
                and est.ide_gestion_nro = reap.ide_gestion_nro(+)
                and est.cod_entidad = ges.cod_entidad
                and est.ide_gestion_sector = ges.ide_gestion_sector
                and est.ide_gestion_nro = ges.ide_gestion_nro
                and est.ide_gestion_nro = y.gestion_padre
              order by ges.ide_gestion_nro, est.orden_estado;
      end loop;

      v_paso := 6;
      --Se copia la informacion a la tabla FACT_GESTIONES visible desde el modelo de SAP BO
      insert into fact_gestiones
           select * from int_fact;
      commit;

      v_fecha_fin := sysdate;
      pkg_gc_procesos.sp_procesos(2, 2, v_fecha_inicio, v_fecha_fin, id_proceso);
    end if;
  exception
    when others then
      v_fecha_fin := sysdate;
      pkg_gc_procesos.sp_procesos(2, 6, v_fecha_inicio, v_fecha_fin, id_proceso);
      v_err_sql := substr (sqlerrm (sqlcode), 1, 250);
      pkg_gc_procesos.sp_inconsistencias(id_proceso, sysdate, v_paso, v_err_sql);
  end sp_fact_trac_prom;

END PKG_GC_FACT;
/
