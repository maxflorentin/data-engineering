set hive.merge.mapfiles=true;
set hive.merge.mapredfiles=true;
set hive.merge.size.per.task=4000000;
set hive.merge.smallfiles.avgsize=16000000;
set hive.exec.dynamic.partition.mode=nonstrict;
SET mapred.job.queue.name=root.dataeng;


CREATE TEMPORARY TABLE testado_asol as
select *
from (
select cod_sucursal, nro_solicitud, cod_estado, fec_estado, hra_estado, cod_usuario, cod_sector_usu, nro_cta,
cast(trim(lim_visa) as decimal(17,2)) as fc_adm_limvisa,
cast(trim(lim_acte) as decimal(17,2)) as fc_adm_limacte,
cast(trim(lim_ppp) as decimal(17,2)) as fc_adm_limppp,
cast(trim(lim_amex) as decimal(17,2)) as fc_adm_limamex,
cast(trim(lim_cheq) as decimal(17,2)) as fc_adm_limcheques,
cast(trim(lim_visa) as decimal(17,2))+cast(trim(lim_acte) as decimal(17,2))+cast(trim(lim_ppp) as decimal(17,2))+cast(trim(lim_amex) as decimal(17,2))+cast(trim(lim_cheq) as decimal(17,2)) as fc_adm_limtotal,
ROW_NUMBER() over ( partition by cod_sucursal, nro_solicitud, cod_estado order by fec_estado, hra_estado desc) as rownum
from bi_corp_staging.alcen_testado_asol p
where p.partition_date = "{{ ti.xcom_pull(task_ids='InputConfig', key='partition_date', dag_id='LOAD_CMN_Admision_Ind-Daily') }}"
) x
where x.rownum = 1
;


CREATE TEMPORARY TABLE tmp_vestado_cvcsri as
select x.*,
CASE WHEN trim(cod_estado) = 'AP' THEN 'Aceptado'
WHEN trim(cod_estado) = 'DL' THEN 'Rechazado'
WHEN trim(cod_estado) = 'ER' THEN 'Devuelto'
WHEN trim(cod_estado) = 'IN' THEN 'Incompleto'
WHEN trim(cod_estado) = 'MD' THEN 'Documentacion Complementaria'
WHEN trim(cod_estado) = 'RS' THEN 'Resolución Condicional'
ELSE '' END AS des_estado
from (
select *, ROW_NUMBER () over ( partition by nro_solicitud , cod_sucursal, case when trim(cod_sector) = 'CVC' then trim(cod_sector) else 'SRI' end order by fec_ingreso desc ) as rownum
from bi_corp_staging.alcen_vestado_cvcsri
where partition_date = "{{ ti.xcom_pull(task_ids='InputConfig', key='partition_date', dag_id='LOAD_CMN_Admision_Ind-Daily') }}"
) x
where x.rownum = 1
;


CREATE TEMPORARY TABLE simulador_prendarios as
select trim(nro_solicitud) as nro_solicitud, trim(cod_sucursal) as cod_sucursal , trim(nro_solicitud_fe) as nro_solicitud_fe, row_number() over ( partition by cod_sucursal , nro_solicitud_fe order by cast(cod_sucursal as bigint), cast(nro_solicitud as bigint) desc) as int_adm_prendarioordensolicituddesc
from bi_corp_staging.alcen_tdatos_adicionales
where partition_date = '2021-04-13' and nro_solicitud_fe is not null and trim(nro_solicitud_fe) != '0'
;

CREATE TEMPORARY TABLE tramites_ptovta_tarjeta as
select
    MAX(CAST(td.cod_sucursal AS bigint)) as cod_sucursal,
    MAX(CAST(td.nro_solicitud AS bigint)) as nro_solicitud,
    trim(tpt.cod_sucursal) as cod_sucursal_join,
    trim(tpt.nro_solicitud) as nro_solicitud_join,
    trim(tpt.nro_tramite_fe) as nro_tramite_fe
from bi_corp_staging.alcen_tdatos_adicionales td
left join bi_corp_staging.alcen_tramites_ptovta_tarjeta tpt on cast(td.nro_solicitud_fe as bigint) = cast(tpt.nro_tramite_fe as bigint) and tpt.partition_date = "{{ ti.xcom_pull(task_ids='InputConfig', key='partition_date', dag_id='LOAD_CMN_Admision_Ind-Daily') }}"
where td.partition_date = "{{ ti.xcom_pull(task_ids='InputConfig', key='partition_date', dag_id='LOAD_CMN_Admision_Ind-Daily') }}"
and td.nro_solicitud_fe is not null and td.nro_solicitud_fe <> '0'
group by tpt.cod_sucursal, tpt.nro_solicitud, tpt.nro_tramite_fe;

INSERT OVERWRITE TABLE bi_corp_common.stk_adm_ind_solicitudes
PARTITION (partition_date="{{ ti.xcom_pull(task_ids='InputConfig', key='partition_date', dag_id='LOAD_CMN_Admision_Ind-Daily') }}")

SELECT x.*,
CASE when substring(x.cod_adm_estadoultresolriesgos,1,1)='A' THEN 'ACEPTADA'
when substring(x.cod_adm_estadoultresolriesgos,1,1)='D' THEN 'DECLINADA'
when x.cod_adm_estadoultresolriesgos IN ('MD','IN','ER','RV','OD','OV')  AND "{{ ti.xcom_pull(task_ids='InputConfig', key='partition_date', dag_id='LOAD_CMN_Admision_Ind-Daily') }}" <= date_add(to_date(trim(x.ts_adm_fecdesicionsw)), 15) THEN 'PENDIENTE'
when x.cod_adm_estadoultresolriesgos IN ('MD','IN','ER','RV','OD','OV')  AND "{{ ti.xcom_pull(task_ids='InputConfig', key='partition_date', dag_id='LOAD_CMN_Admision_Ind-Daily') }}" > date_add(to_date(trim(x.ts_adm_fecdesicionsw)), 15) THEN 'VENCIDA'
ELSE '' END AS ds_adm_estadoultresolriesgos
FROM (
select
concat(lpad(s.cod_sucursal, 3, '0') ,lpad(s.nro_solicitud, 17, '0')) as cod_adm_tramite,
cast(s.cod_sucursal as int) as cod_suc_sucursal,
cast(s.nro_solicitud as bigint) as cod_adm_nrosolicitud,

case when trim(t.fec_ingreso_rio2) < trim(t.fec_envio_sw1) and trim(t.fec_ingreso_rio2) < trim(t.fec_envio_sw2) then trim(t.fec_ingreso_rio2) else trim(t.fec_envio_sw2) end as ts_adm_fecingresoscoring,
trim(t.fec_ingreso_rio2) as ts_adm_fecingresorio2,
trim(t.fec_envio_sw1) as ts_adm_fecenviosw1,
trim(t.fec_envio_sw2) as ts_adm_fecenviosw2,
trim(t.cod_estado_sw) as cod_adm_estadosw,
trim(t.fec_desicion_sw) as ts_adm_fecdesicionsw,
trim(t.fec_ingreso_srs) as ts_adm_fecingresosrs,
trim(t.fec_asig_ana_srs) as ts_adm_fecasig_anasrs,
trim(t.fec_ini_resol_srs) as ts_adm_fecini_resolsrs,
trim(t.fec_fin_resol_srs) as ts_adm_fecfin_resolsrs,
trim(t.cod_estado_srs) as cod_adm_estadosrs,
trim(t.fec_resol_suc) as ts_adm_fecresolsuc,
trim(t.cod_resol_suc) as cod_adm_resolsuc,
trim(t.fec_resol_altas) as ts_adm_fecresolaltas,
trim(t.cod_resol_altas) as cod_adm_resolaltas,
trim(t.cod_estado_actual) as cod_adm_estadoactual,
trim(t.fec_estado_actual) as ts_adm_fec_estado_actual,
trim(t.cod_estado_asol) as cod_adm_estadoasol,
trim(t.fec_estado_asol) as ts_adm_fec_estado_asol,
trim(t.nom_sector_altas) as ds_adm_nomsectoraltas,
nvl(trim(t.fec_fin_resol_srs), trim(t.fec_desicion_sw)) as ts_adm_fecswsrs,
nvl(trim(t.cod_estado_srs), trim(t.cod_estado_sw)) as ds_adm_estadoswsrs,
case
when substring(nvl(trim(t.cod_estado_srs), trim(t.cod_estado_sw)), 1, 1) = 'A' then 'aceptada'
when substring(nvl(t.cod_estado_srs, t.cod_estado_sw),1,1) = 'D' then 'declinada'
when substring(nvl(t.cod_estado_srs, t.cod_estado_sw),1,1) = 'R' then 'pendiente zona gris'
when substring(nvl(t.cod_estado_srs, t.cod_estado_sw),1,1) = '' then 'Sin resolucion'
else 'pendiente'
end as ds_adm_estado,
CASE
WHEN cast(s.cod_dest_fond_aptm as bigint)<> 0 AND cast(s.cod_dest_fond_aptm as bigint) IS NOT NULL THEN s.cod_dest_fond_aptm
WHEN cast(s.cod_dest_fond_aptp as bigint)<> 0 AND cast(s.cod_dest_fond_aptp as bigint) IS NOT NULL THEN s.cod_dest_fond_aptp
WHEN cast(s.cod_dest_fond_afco as bigint)<> 0 AND cast(s.cod_dest_fond_aptp as bigint) IS NOT NULL THEN s.cod_dest_fond_afco
else null
end as cod_adm_destinofondos,
CASE
WHEN cast(s.val_bien_aptm as bigint)<> 0 AND cast(s.val_bien_aptm as bigint) IS NOT NULL THEN cast(s.val_bien_aptm as bigint)
WHEN cast(s.val_bien_aptp as bigint)<> 0 AND cast(s.val_bien_aptp as bigint) IS NOT NULL THEN cast(s.val_bien_aptp as bigint)
WHEN cast(s.val_bien_afco as bigint)<> 0 AND cast(s.val_bien_aptp as bigint) IS NOT NULL THEN cast(s.val_bien_afco as bigint)
else null
end as fc_adm_valorbien,
CASE
WHEN cast(s.mon_soli_aptm as bigint)<> 0 AND cast(s.mon_soli_aptm as bigint) IS NOT NULL THEN cast(s.mon_soli_aptm as bigint)
WHEN cast(s.mon_soli_aptp as bigint)<> 0 AND cast(s.mon_soli_aptp as bigint) IS NOT NULL THEN cast(s.mon_soli_aptp as bigint)
WHEN cast(s.mon_soli_afco as bigint)<> 0 AND cast(s.mon_soli_aptp as bigint) IS NOT NULL THEN cast(s.mon_soli_afco as bigint)
else null
end as fc_adm_montosolicitado,
CASE
WHEN cast(s.can_cuo_aptm as bigint)<> 0 AND cast(s.can_cuo_aptm as bigint) IS NOT NULL THEN cast(s.can_cuo_aptm as bigint)
WHEN cast(s.can_cuo_aptp as bigint)<> 0 AND cast(s.can_cuo_aptp as bigint) IS NOT NULL THEN cast(s.can_cuo_aptp as bigint)
WHEN cast(s.can_cuo_afco as bigint)<> 0 AND cast(s.can_cuo_aptp as bigint) IS NOT NULL THEN cast(s.can_cuo_afco as bigint)
else null
end as fc_adm_plazosolicitado,
CASE
WHEN cast(s.imp_cuo_aptm as bigint)<> 0 AND cast(s.imp_cuo_aptm as bigint) IS NOT NULL THEN cast(s.imp_cuo_aptm as bigint)
WHEN cast(s.imp_cuo_aptp as bigint)<> 0 AND cast(s.imp_cuo_aptp as bigint) IS NOT NULL THEN cast(s.imp_cuo_aptp as bigint)
WHEN cast(s.imp_cuo_afco as bigint)<> 0 AND cast(s.imp_cuo_aptp as bigint) IS NOT NULL THEN cast(s.imp_cuo_afco as bigint)
else null
end as fc_adm_importecuota,
cast(s.nro_score as bigint) as fc_adm_nroscore,
trim(s.mar_aprob_rech) as ds_adm_maraprobrech,
trim(s.des_obs_scor) as ds_adm_obsscor,
cast(s.can_participante as bigint) as int_adm_canparticipante,
trim(s.mar_plan_sueldo) as ds_adm_marplansueldo,
trim(s.fec_ingreso_sco) as ts_adm_fecingresosco,
trim(s.hra_ingreso_sco) as ts_adm_hraingresosco,
trim(s.cod_analista) as cod_adm_analista,
trim(s.cod_supervisor) as cod_adm_supervisor,
trim(s.cod_promotor) as cod_adm_promotor,
case
when trim(s.cod_prom_scor) is not null then trim(s.cod_prom_scor)
when trim(s.cod_prom_scor_accd) is not null then trim(s.cod_prom_scor_accd)
when trim(s.cod_prom_scor_acte) is not null then trim(s.cod_prom_scor_acte)
when trim(s.cod_prom_scor_afco) is not null then trim(s.cod_prom_scor_afco)
when trim(s.cod_prom_scor_amas) is not null then trim(s.cod_prom_scor_amas)
when trim(s.cod_prom_scor_amex) is not null then trim(s.cod_prom_scor_amex)
when trim(s.cod_prom_scor_aptm) is not null then trim(s.cod_prom_scor_aptm)
when trim(s.cod_prom_scor_aptp) is not null then trim(s.cod_prom_scor_aptp)
when trim(s.cod_prom_scor_avis) is not null then trim(s.cod_prom_scor_avis)
when trim(s.cod_prom_scor_paq) is not null then  trim(s.cod_prom_scor_paq)
else null end as cod_adm_promscor,
trim(s.mar_bp) as cod_adm_marbp,
trim(s.cod_emp_pas) as cod_adm_emp_pas,
trim(s.mar_limites_optimizados) as ds_adm_marlimitesoptimizados,
trim(s.cod_cola_asignada) as cod_adm_colaasignada,
trim(s.cod_estado_sco) as cod_adm_estadosco,
trim(s.cod_sector) as cod_adm_sector,
trim(s.cod_mot_zg) as cod_adm_motzg,
trim(s.mar_preembozado) as ds_adm_marpreembozado,
cast(s.lim_ppp_vigente as bigint) as fc_adm_limpppvigente,
cast(s.lim_acc_vigente as bigint) as fc_adm_limaccvigente,
trim(s.mar_aso1_auto) as ds_adm_mar_aso1_auto,
trim(s.des_obser) as ds_adm_obser,
spr.int_adm_prendarioordensolicituddesc as cod_adm_prendarioordensolicituddesc,
cast(sp.cod_sucursal_preev as bigint) as cod_adm_sucursal_prev,
cast(sp.nro_solicitud_preev as bigint) as cod_adm_nrosolicitudprev,
nvl(t2.fec_fin_resol_srs, t2.fec_desicion_sw) as dt_adm_fecresolucionpreevaluacion,
nvl(t2.cod_estado_srs, t2.cod_estado_sw) as ds_adm_resolucionpreevaluacion,
trim(tpt.nro_tramite_fe) as cod_adm_nrotramitefe_pdv,
concat(lpad(tpt.cod_sucursal, 3, '0') ,lpad(tpt.nro_solicitud, 17, '0')) as cod_adm_tramite2dollamado_pdv,
cast(p.nro_participante as bigint) as cod_adm_participante,
trim(p.mar_titulo) as ds_adm_martitulo,
trim(p.mar_prod_pas) as ds_adm_marprodpas,
trim(p.cod_bca) as cod_adm_bca,
trim(p.cod_divs) as cod_adm_divs,
cast(p.cod_tlead as bigint) as cod_adm_tlead,
cast(p.cod_ejec_cta as bigint) as cod_adm_ejeccta,
trim(p.mar_emp_inh) as ds_adm_marempinh,
cast(p.cod_bol_inh as bigint) as cod_adm_bolinh,
cast(p.nro_orden as bigint) as cod_adm_nroorden,
cast(p.cod_secc_inh as bigint) as cod_adm_seccinh,
cast(p.nro_banco as bigint) as cod_adm_nrobanco,
trim(p.fec_alt_inh) as dt_adm_fecaltinh,
trim(p.fec_vto_inh) as dt_adm_fecvtoinh,
cast(p.can_inh as bigint) as fc_adm_caninh,
trim(p.cod_caus_inh) as cod_adm_causinh,
trim(p.des_caus_inh) as ds_adm_causinh,
trim(p.nom_nombre) as ds_adm_nomnombre,
trim(p.nom_apellido) as ds_adm_nomapellido,
trim(p.tpo_doc) as ds_adm_tpodoc,
cast(p.nro_doc as bigint) as cod_adm_nrodoc,
trim(p.fec_nacimiento) as dt_adm_fecnacimiento,
trim(p.mar_sexo) as ds_adm_marsexo,
trim(p.cod_nacionalidad) as cod_adm_nacionalidad,
trim(p.cod_estado_civil) as cod_adm_estadocivil,
cast(p.can_pers_a_cargo as int) as int_adm_canpersacargo,
trim(p.cod_nivel_estudio) as cod_adm_nivelestudio,
trim(p.cod_rol_en_soli) as cod_adm_rolensoli,
trim(p.mar_aprb_rech_scor) as ds_adm_maraprbrechscor,
cast(p.can_consul as bigint) as int_adm_canconsul,
cast(p.can_antec_menor as bigint) as int_adm_canantecmenor,
cast(p.can_antec_mayr as bigint) as fc_adm_canantecmayr,
cast(p.can_antec_regu as bigint) as int_adm_canantecregu,
trim(p.fec_alta_prod_cb) as dt_adm_fecaltaprodcb,
cast(p.can_prod_actv_cb as bigint) as int_adm_canprodactvcb,
cast(p.can_prod_actv_bc as bigint) as int_adm_canprodactvbc,
trim(p.cod_exper_previa_sis) as cod_adm_experpreviasis,
trim(p.cod_informe_veraz) as cod_adm_informeveraz,
cast(p.mon_suel as bigint) as fc_adm_monsuel,
cast(p.mon_sac as bigint) as fc_adm_monsac,
cast(p.mon_comision as bigint) as fc_adm_moncomision,
cast(p.mon_gratif as bigint) as fc_adm_mongratif,
cast(p.mon_rentas as bigint) as fc_adm_monrentas,
cast(p.mon_otr_ingr as bigint) as fc_adm_monotringr,
trim(p.des_otr_ingr) as ds_adm_otringr,
cast(p.mon_alquiler as bigint) as fc_adm_monalquiler,
cast(p.mon_expensas as bigint) as fc_adm_monexpensas,
trim(p.mar_cotitular) as ds_adm_marcotitular,
trim(p.cod_profesion) as cod_adm_profesion,
trim(p.mar_cliente) as ds_adm_marcliente,
cast(p.nro_part_cony as bigint) as int_adm_nropartcony,
cast(p.can_tarjetas as bigint) as fc_adm_cantarjetas,
cast(p.can_antec_mayr_lev as bigint) as int_adm_canantecmayrlev,
cast(p.can_antec_mayr_gra as bigint) as int_adm_canantecmayrgra,
cast(p.cod_suc_veraz_reut as bigint) as cod_adm_sucverazreut,
cast(p.nro_solic_veraz_reut as bigint) as fc_adm_nrosolicverazreut,
cast(p.ide_nup as bigint) as cod_per_nup,
trim(p.cod_val_docu) as cod_adm_valdocu,
trim(p.tpo_ins) as ds_adm_tpoins,
cast(p.nro_ins as bigint) as fc_adm_nroins,
trim(p.mar_fraude) as ds_adm_marfraude,
trim(p.mar_experiencia) as ds_adm_marexperiencia,
cast(p.nro_score_veraz as bigint) as fc_adm_nroscoreveraz,
trim(p.cod_grupo_veraz) as cod_adm_grupoveraz,
trim(p.cod_poblac_veraz) as cod_adm_poblacveraz,
trim(p.indicador_riesgo) as ds_adm_indicadorriesgo,
cast(p.nup_empresa_asociada as bigint) as cod_adm_nupempresaasociada,
trim(p.mar_max_ir) as ds_adm_marmaxir,
cast(p.mon_ing_validos as bigint) as fc_adm_moningvalidos,
cast(p.lim_tope_renta as bigint) as fc_adm_limtoperenta,
trim(p.mar_pyme) as ds_adm_marpyme,
trim(p.fec_mar_juanito) as dt_adm_fecmarjuanito,
trim(p.email) as ds_adm_email,
trim(p.celular) as ds_adm_celular,
trim(p.documentacion) as ds_adm_documentacion,
cast(p.hijos as int) as int_adm_hijos,
cast(p.importe_colegio as bigint) as fc_adm_importecolegio,
trim(p.nro_lote_veraz_reut) as ds_adm_nroloteveraz_reut,
trim(p.mar_citi) as ds_adm_marciti,
trim(p.mar_rel_amex) as ds_adm_marrelamex,
trim(p.actividad_afip) as ds_adm_actividadafip,
trim(p.antig_act_afip) as ds_adm_antig_actafip,
trim(p.tipo_cliente) as ds_adm_tipocliente,
trim(p.cod_tipo_renta) as cod_adm_tiporenta,
cast(trim(p.cant_adel_eftvo_pesos) as bigint) as fc_adm_cantadeleftvo_pesos,
cast(trim(p.mon_adel_eftvo_pesos) as decimal(17,2)) as fc_adm_monadeleftvo_pesos,
cast(trim(p.cant_adel_eftvo_dolares) as bigint) as fc_adm_cantadeleftvo_dolares,
cast(trim(p.mon_adel_eftvo_dolares) as decimal(17,2)) as fc_adm_monadeleftvo_dolares,
trim(da.politicas) as ds_adm_politicas,
trim(da.universidad) as ds_adm_universidad,
trim(da.carrera) as ds_adm_carrera,
cast(trim(da.cant_mat_falt) as int) as int_adm_cantmatfalt,
trim(da.fec_ingreso_uni) as ts_adm_fecingresouni,
trim(da.fec_egreso_uni) as ts_adm_fecegresouni,
trim(da.tpo_solicitud) as ds_adm_tposolicitud,
cast(trim(da.nro_solicitud_fe) as bigint) as cod_adm_nrosolicitudfe,
trim(da.cod_canal) as cod_adm_canal,
trim(da.tpo_bien) as ds_adm_tpobien,
cast(trim(da.porc_ltv) as bigint) as fc_adm_porcltv,
trim(da.obs_cobertura) as ds_adm_obscobertura,
cast(trim(da.anio_bien) as int) as int_adm_aniobien,
cast(trim(da.val_rel_cuota_ingreso) as int) as int_adm_valrelcuotaingreso,
cast(trim(da.mon_ingreso_total_grupo) as int) as int_adm_moningresototalgrupo,
trim(da.tpo_uso) as ds_adm_tpo_uso,
trim(da.nom_aseguradora) as ds_adm_nom_aseguradora,
trim(da.periodicidad) as ds_adm_periodicidad,
trim(da.des_marca) as ds_adm_marca,
trim(da.des_modelo) as ds_adm_modelo,
trim(da.mar_veraz_renovado) as ds_adm_marverazrenovado,
trim(da.habilita_mejora) as ds_adm_habilitamejora,
cast(trim(da.pzo_prestamo) as bigint) as fc_adm_pzoprestamo,
cast(trim(da.mon_acred_hab) as int) as fc_admmonacredhab,
cast(trim(da.dias_acred_hab) as int) as int_admdiasacredhab,
cast(trim(da.tasa_a_12_meses) as decimal(17,2)) as fc_adm_tasa12meses,
cast(trim(da.tasa_a_24_meses) as decimal(17,2)) as fc_adm_tasa24meses,
cast(trim(da.tasa_a_36_meses) as decimal(17,2)) as fc_adm_tasa36meses,
cast(trim(da.tasa_a_48_meses) as decimal(17,2)) as fc_adm_tasa48meses,
cast(trim(da.tasa_a_60_meses) as decimal(17,2)) as fc_adm_tasa60meses,
cast(trim(da.alicuota_iva_prend) as decimal(17,2)) as fc_adm_alicuotaivaprend,
cast(trim(da.seguro_automotor_prend) as decimal(17,2)) as fc_adm_seguroautomotorprend,
cast(trim(da.seguro_vida_prend) as decimal(17,2)) as fc_adm_segurovida_prend,
cast(trim(da.tasa_a_12_meses_uva) as decimal(17,2)) as fc_adm_tasa12mesesuva,
cast(trim(da.tasa_a_24_meses_uva) as decimal(17,2)) as fc_adm_tasa24mesesuva,
cast(trim(da.tasa_a_36_meses_uva) as decimal(17,2)) as fc_adm_tasa36mesesuva,
cast(trim(da.tasa_a_48_meses_uva) as decimal(17,2)) as fc_adm_tasa48mesesuva,
cast(trim(da.tasa_a_60_meses_uva) as decimal(17,2)) as fc_adm_tasa60mesesuva,
trim(dpp.des_producto) as ds_adm_producto,
trim(dpp.des_producto_detalle) as ds_adm_productodetalle,
trim(dc.descripcion) as ds_adm_canal,
trim(ps.region) as ds_adm_sucursalregion,
trim(ps.nombre) as ds_adm_sucursalnombre,
trim(ps.zona_comercial) as ds_adm_sucursalzonacomer,
trim(cc.cod_concesionario) as cod_adm_concesionario,
trim(cc.nom_concesionario) as ds_adm_nomconcesionario,

case when trim(ta13.fec_estado) is not null then 1 else 0 end as flag_adm_maralta,
trim(ta31.fec_estado) as dt_adm_fecestado31,
trim(ta31.hra_estado) as ts_adm_estado_31,
trim(ta31.cod_sector_usu) as cod_adm_sectorusu31,
trim(ta31.cod_usuario) as cod_adm_usuario31,
trim(ta10.fec_estado) as dt_adm_fecestado10,
trim(ta10.hra_estado) as ts_adm_estado_10,
trim(ta10.cod_sector_usu) as cod_adm_sectorusu10,
trim(ta10.cod_usuario) as cod_adm_usuario10,
trim(ta13.fec_estado) as dt_adm_fecestado13,
trim(ta13.hra_estado) as ts_adm_estado_13,
trim(ta13.cod_sector_usu) as cod_adm_sectorusu13,
trim(ta13.cod_usuario) as cod_adm_usuario13,
trim(ta12.fec_estado) as dt_adm_fecestado12,
trim(ta12.hra_estado) as ts_adm_estado_12,
trim(ta12.cod_sector_usu) as cod_adm_sectorusu12,
trim(ta12.cod_usuario) as cod_adm_usuario12,
ta12.fc_adm_limvisa as fc_adm_limvisa,
ta12.fc_adm_limacte as fc_adm_limacte,
ta12.fc_adm_limppp as fc_adm_limppp,
ta12.fc_adm_limamex as fc_adm_limamex,
ta12.fc_adm_limcheques as fc_adm_limcheques,
ta12.fc_adm_limtotal as fc_adm_limtotal,
cast(trim(ta13.nro_cta) as decimal(17,2)) as cod_nro_cuenta,
trim(cvc.des_estado) as ds_adm_estadoresolucioncvc,
trim(cvc.fec_ingreso) as ts_adm_fecingresocvc,
trim(cvc.fec_ini_resol) as ts_adm_feciniresolcvc,
trim(cvc.fec_fin_resol) as ts_adm_fecfinresolcvc,
trim(cvc.cod_estado) as cod_adm_estadocvc,
trim(cvc.mot_resol1) as ds_adm_motresol1cvc,
trim(cvc.mot_resol2) as ds_adm_motresol2cvc,
trim(cvc.mot_resol3) as ds_adm_motresol3cvc,
trim(cvc.mot_resol4) as ds_adm_motresol4cvc,
trim(cvc.mot_resol5) as ds_adm_motresol5cvc,
trim(cvc.cod_analista) as cod_adm_analistacvc,
trim(sri.des_estado) as ds_adm_estadoresolucionsri,
trim(sri.fec_ingreso) as ts_adm_fecingresosri,
trim(sri.fec_ini_resol) as ts_adm_feciniresolsri,
trim(sri.fec_fin_resol) as ts_adm_fecfinresolsri,
trim(sri.cod_estado) as cod_adm_estadosri,
trim(sri.mot_resol1) as ds_adm_motresol1sri,
trim(sri.mot_resol2) as ds_adm_motresol2sri,
trim(sri.mot_resol3) as ds_adm_motresol3sri,
trim(sri.mot_resol4) as ds_adm_motresol4sri,
trim(sri.mot_resol5) as ds_adm_motresol5sri,
trim(sri.cod_analista) as cod_adm_analistasri,
trim(th.codigo_hash) as ds_adm_codigohash,
trim(th.tipo_tramite) as ds_adm_tipotramitehash,
cast(trim(th.valor) as int) as int_adm_valor,
trim(l.cod_ocupac) as cod_adm_ocupac,
trim(l.cod_emp_tpo) as cod_adm_emptpo,

CASE when UPPER(TRIM(sri.mot_resol1)) in ('A18','A04','A26','A22','A23','A25') OR UPPER(TRIM(sri.mot_resol2)) in ('A18','A04','A26','A22','A23','A25') OR UPPER(TRIM(sri.mot_resol3)) in ('A18','A04','A26','A22','A23','A25') OR UPPER(TRIM(sri.mot_resol4)) in ('A18','A04','A26','A22','A23','A25') THEN 1 ELSE 0 END as flag_adm_motivosupnoforzaje,
trim(dp.tramite) as ds_adm_tipotramite,
trim(dp.descripcion2) as ds_adm_productopromocion,

case when trim(ta13.fec_estado) < trim(ta12.fec_estado) then trim(ta13.fec_estado) else trim(ta12.fec_estado) end as dt_adm_fecaltaminima,
cod_per_segmentoduro as cod_per_segmentoduro,
ds_per_segmento as ds_per_segmento,
ds_per_subsegmento as ds_per_subsegmento,
cod_per_cuadrante as cod_per_cuadrante,

CASE
when trim(cvc.fec_fin_resol) IS NOT NULL AND trim(sri.cod_estado) in ('AP','DL')   AND trim(cvc.cod_estado) in ('AP','DL')
AND trim(cvc.fec_fin_resol) > nvl(trim(trim(t.fec_fin_resol_srs)), trim(t.fec_desicion_sw))
AND trim(cvc.fec_fin_resol) > trim(sri.fec_fin_resol)
THEN trim(cvc.fec_fin_resol)

when trim(cvc.fec_fin_resol) IS NOT NULL AND trim(sri.cod_estado) in ('MD','IN','ER','RS','RC') AND trim(cvc.cod_estado) in ('AP','DL')
AND trim(cvc.fec_fin_resol) > nvl(trim(trim(t.fec_fin_resol_srs)), trim(t.fec_desicion_sw))
AND trim(cvc.fec_fin_resol) < trim(sri.fec_fin_resol)
THEN trim(cvc.fec_fin_resol)

when trim(cvc.fec_fin_resol) IS NOT NULL AND trim(cvc.cod_estado) in ('MD','IN','ER','RS','RC') AND trim(sri.cod_estado) in ('AP','DL')
AND trim(cvc.fec_fin_resol) > nvl(trim(trim(t.fec_fin_resol_srs)), trim(t.fec_desicion_sw))
AND trim(cvc.fec_fin_resol) > trim(sri.fec_fin_resol)
THEN trim(sri.fec_fin_resol)

when trim(cvc.fec_fin_resol) IS NOT NULL AND trim(sri.cod_estado) in ('MD','IN','ER','RS','RC') AND trim(cvc.cod_estado) in ('MD','IN','ER','RS','RC')
AND trim(cvc.fec_fin_resol) > nvl(trim(trim(t.fec_fin_resol_srs)), trim(t.fec_desicion_sw))
AND trim(cvc.fec_fin_resol) < trim(sri.fec_fin_resol)
THEN nvl(trim(trim(t.fec_fin_resol_srs)), trim(t.fec_desicion_sw))

when trim(cvc.fec_fin_resol) IS NOT NULL AND trim(cvc.cod_estado) in ('MD','IN','ER','RS','RC')
AND trim(cvc.fec_fin_resol) > nvl(trim(trim(t.fec_fin_resol_srs)), trim(t.fec_desicion_sw))
AND trim(sri.fec_fin_resol) is null
THEN nvl(trim(trim(t.fec_fin_resol_srs)), trim(t.fec_desicion_sw))

when trim(sri.fec_fin_resol) IS NOT NULL AND trim(sri.cod_estado) in ('MD','IN','ER','RS','RC')
AND trim(sri.fec_fin_resol)> nvl(trim(trim(t.fec_fin_resol_srs)), trim(t.fec_desicion_sw))
AND trim(cvc.fec_fin_resol) is NULL
THEN nvl(trim(trim(t.fec_fin_resol_srs)), trim(t.fec_desicion_sw))

when trim(cvc.fec_fin_resol) IS NOT NULL AND trim(cvc.cod_estado) in ('MD','IN','ER','RS','RC')
AND trim(cvc.fec_fin_resol)> nvl(trim(trim(t.fec_fin_resol_srs)), trim(t.fec_desicion_sw))
AND trim(sri.fec_fin_resol) is NULL
THEN nvl(trim(trim(t.fec_fin_resol_srs)), trim(t.fec_desicion_sw))

when trim(cvc.fec_fin_resol) IS NOT NULL AND trim(sri.cod_estado) in ('MD','IN','ER','RS','RC') AND trim(cvc.cod_estado) in ('MD','IN','ER','RS','RC')
AND trim(cvc.fec_fin_resol) > nvl(trim(trim(t.fec_fin_resol_srs)), trim(t.fec_desicion_sw))
AND trim(cvc.fec_fin_resol) > trim(sri.fec_fin_resol)
THEN nvl(trim(trim(t.fec_fin_resol_srs)), trim(t.fec_desicion_sw))

when trim(sri.fec_fin_resol) IS NOT NULL AND trim(sri.cod_estado) in ('AP','DL')
AND trim(sri.fec_fin_resol) > nvl(trim(trim(t.fec_fin_resol_srs)), trim(t.fec_desicion_sw))
AND trim(cvc.fec_fin_resol) IS NULL
THEN trim(sri.fec_fin_resol)

when trim(sri.fec_fin_resol) IS NOT NULL and trim(cvc.fec_fin_resol) is not null AND trim(sri.cod_estado) in ('AP','DL')
AND trim(sri.fec_fin_resol) > nvl(trim(trim(t.fec_fin_resol_srs)), trim(t.fec_desicion_sw))
AND trim(cvc.fec_fin_resol) < trim(sri.fec_fin_resol)
THEN trim(sri.fec_fin_resol)

when trim(sri.fec_fin_resol) IS NOT NULL
AND trim(sri.fec_fin_resol) < nvl(trim(trim(t.fec_fin_resol_srs)), trim(t.fec_desicion_sw))
AND trim(cvc.fec_fin_resol) is null
THEN nvl(trim(trim(t.fec_fin_resol_srs)), trim(t.fec_desicion_sw))

when trim(sri.fec_fin_resol) IS null
AND trim(cvc.fec_fin_resol) < nvl(trim(trim(t.fec_fin_resol_srs)), trim(t.fec_desicion_sw))
AND trim(cvc.fec_fin_resol) is not null
THEN nvl(trim(trim(t.fec_fin_resol_srs)), trim(t.fec_desicion_sw))

when trim(sri.fec_fin_resol) IS not null
AND trim(cvc.fec_fin_resol) < nvl(trim(trim(t.fec_fin_resol_srs)), trim(t.fec_desicion_sw))
AND trim(sri.fec_fin_resol) < nvl(trim(trim(t.fec_fin_resol_srs)), trim(t.fec_desicion_sw))
AND trim(cvc.fec_fin_resol) is not null
THEN nvl(trim(trim(t.fec_fin_resol_srs)), trim(t.fec_desicion_sw))

when trim(cvc.fec_fin_resol) IS NOT NULL and trim(sri.fec_fin_resol) is not null AND trim(cvc.cod_estado) in ('AP','DL')
AND trim(cvc.fec_fin_resol) > nvl(trim(trim(t.fec_fin_resol_srs)), trim(t.fec_desicion_sw))
AND trim(cvc.fec_fin_resol) > trim(sri.fec_fin_resol)
THEN trim(cvc.fec_fin_resol)

when trim(cvc.fec_fin_resol) IS NOT NULL AND trim(cvc.cod_estado) in ('AP','DL')
AND trim(cvc.fec_fin_resol) > nvl(trim(trim(t.fec_fin_resol_srs)), trim(t.fec_desicion_sw))
AND trim(sri.fec_fin_resol) is null
THEN trim(cvc.fec_fin_resol)

when trim(cvc.fec_fin_resol) IS NULL AND trim(sri.fec_fin_resol) IS NULL
AND nvl(trim(trim(t.fec_fin_resol_srs)), trim(t.fec_desicion_sw)) IS NOT NULL
THEN nvl(trim(trim(t.fec_fin_resol_srs)), trim(t.fec_desicion_sw))
ELSE '' END AS dt_adm_fecultresolriesgos,

CASE
when trim(cvc.fec_fin_resol) IS NOT NULL AND trim(sri.cod_estado) in ('AP','DL')   AND trim(cvc.cod_estado) in ('AP','DL')
AND trim(cvc.fec_fin_resol) > nvl(trim(trim(t.fec_fin_resol_srs)), trim(t.fec_desicion_sw))
AND trim(cvc.fec_fin_resol) > trim(sri.fec_fin_resol)
THEN trim(cvc.cod_estado)

when trim(cvc.fec_fin_resol) IS NOT NULL AND trim(sri.cod_estado) in ('MD','IN','ER','RS','RC') AND trim(cvc.cod_estado) in ('AP','DL')
AND trim(cvc.fec_fin_resol) > nvl(trim(trim(t.fec_fin_resol_srs)), trim(t.fec_desicion_sw))
AND trim(cvc.fec_fin_resol) < trim(sri.fec_fin_resol)
THEN trim(cvc.cod_estado)

when trim(cvc.fec_fin_resol) IS NOT NULL AND trim(cvc.cod_estado) in ('MD','IN','ER','RS','RC') AND trim(sri.cod_estado) in ('AP','DL')
AND trim(cvc.fec_fin_resol) > nvl(trim(trim(t.fec_fin_resol_srs)), trim(t.fec_desicion_sw))
AND trim(cvc.fec_fin_resol) > trim(sri.fec_fin_resol)
THEN trim(sri.cod_estado)

when trim(cvc.fec_fin_resol) IS NOT NULL AND trim(sri.cod_estado) in ('MD','IN','ER','RS','RC') AND trim(cvc.cod_estado) in ('MD','IN','ER','RS','RC')
AND trim(cvc.fec_fin_resol) > nvl(trim(trim(t.fec_fin_resol_srs)), trim(t.fec_desicion_sw))
AND trim(cvc.fec_fin_resol) < trim(sri.fec_fin_resol)
THEN nvl(trim(t.cod_estado_srs), trim(t.cod_estado_sw))

when trim(cvc.fec_fin_resol) IS NOT NULL AND trim(cvc.cod_estado) in ('MD','IN','ER','RS','RC')
AND trim(cvc.fec_fin_resol) > nvl(trim(trim(t.fec_fin_resol_srs)), trim(t.fec_desicion_sw))
AND trim(sri.fec_fin_resol) is null
THEN nvl(trim(t.cod_estado_srs), trim(t.cod_estado_sw))

when trim(cvc.fec_fin_resol) IS NOT NULL AND trim(sri.cod_estado) in ('MD','IN','ER','RS','RC') AND trim(cvc.cod_estado) in ('MD','IN','ER','RS','RC')
AND trim(cvc.fec_fin_resol) > nvl(trim(trim(t.fec_fin_resol_srs)), trim(t.fec_desicion_sw))
AND trim(cvc.fec_fin_resol) > trim(sri.fec_fin_resol)
THEN nvl(trim(t.cod_estado_srs), trim(t.cod_estado_sw))

when trim(sri.fec_fin_resol) IS NOT NULL AND trim(sri.cod_estado) in ('MD','IN','ER','RS','RC')
AND trim(sri.fec_fin_resol) > nvl(trim(trim(t.fec_fin_resol_srs)), trim(t.fec_desicion_sw))
AND trim(cvc.fec_fin_resol) is null
THEN nvl(trim(t.cod_estado_srs), trim(t.cod_estado_sw))

when trim(sri.fec_fin_resol) IS NOT NULL
AND trim(sri.fec_fin_resol) < nvl(trim(trim(t.fec_fin_resol_srs)), trim(t.fec_desicion_sw))
AND trim(cvc.fec_fin_resol) is null
THEN nvl(trim(t.cod_estado_srs), trim(t.cod_estado_sw))

when trim(sri.fec_fin_resol) IS null
AND trim(cvc.fec_fin_resol) < nvl(trim(trim(t.fec_fin_resol_srs)), trim(t.fec_desicion_sw))
AND trim(cvc.fec_fin_resol) is not null
THEN nvl(trim(t.cod_estado_srs), trim(t.cod_estado_sw))

when trim(sri.fec_fin_resol) IS not null
AND trim(cvc.fec_fin_resol) < nvl(trim(trim(t.fec_fin_resol_srs)), trim(t.fec_desicion_sw))
AND trim(sri.fec_fin_resol) < nvl(trim(trim(t.fec_fin_resol_srs)), trim(t.fec_desicion_sw))
AND trim(cvc.fec_fin_resol) is not null
THEN nvl(trim(t.cod_estado_srs), trim(t.cod_estado_sw))

when trim(sri.fec_fin_resol) IS NOT NULL AND trim(sri.cod_estado) in ('AP','DL')
AND trim(sri.fec_fin_resol) > nvl(trim(trim(t.fec_fin_resol_srs)), trim(t.fec_desicion_sw))
AND trim(cvc.fec_fin_resol) < trim(sri.fec_fin_resol)
THEN trim(sri.cod_estado)

when trim(sri.fec_fin_resol) IS NOT NULL AND trim(sri.cod_estado) in ('AP','DL')
AND trim(sri.fec_fin_resol) > nvl(trim(trim(t.fec_fin_resol_srs)), trim(t.fec_desicion_sw))
AND trim(cvc.fec_fin_resol) is null
THEN trim(sri.cod_estado)

when trim(cvc.fec_fin_resol) IS NOT NULL and trim(sri.fec_fin_resol) is not null AND trim(cvc.cod_estado) in ('AP','DL')
AND trim(cvc.fec_fin_resol) > nvl(trim(trim(t.fec_fin_resol_srs)), trim(t.fec_desicion_sw))
AND trim(cvc.fec_fin_resol) > trim(sri.fec_fin_resol)
THEN trim(cvc.cod_estado)

when trim(cvc.fec_fin_resol) IS NOT NULL AND trim(cvc.cod_estado) in ('AP','DL')
AND trim(cvc.fec_fin_resol) > nvl(trim(trim(t.fec_fin_resol_srs)), trim(t.fec_desicion_sw))
AND trim(sri.fec_fin_resol) is null
THEN trim(cvc.cod_estado)

when trim(cvc.fec_fin_resol) IS NULL AND trim(sri.fec_fin_resol) IS NULL
AND nvl(trim(trim(t.fec_fin_resol_srs)), trim(t.fec_desicion_sw)) IS NOT NULL
THEN nvl(trim(t.cod_estado_srs), trim(t.cod_estado_sw))
ELSE '' END AS cod_adm_estadoultresolriesgos,

CASE
when trim(cvc.fec_fin_resol) is not null and trim(sri.cod_estado) in ('AP','DL') and trim(cvc.cod_estado) in ('AP','DL')
and trim(cvc.fec_fin_resol) > nvl(trim(trim(t.fec_fin_resol_srs)), trim(t.fec_desicion_sw))
and trim(cvc.fec_fin_resol) > trim(sri.fec_fin_resol)
THEN 'CVC'

when trim(cvc.fec_fin_resol) is not null and trim(sri.cod_estado) in ('MD','IN','ER','RS','RC') and trim(cvc.cod_estado) in ('AP','DL')
and trim(cvc.fec_fin_resol) > nvl(trim(trim(t.fec_fin_resol_srs)), trim(t.fec_desicion_sw))
and trim(cvc.fec_fin_resol) < trim(sri.fec_fin_resol)
THEN 'CVC'

when trim(cvc.fec_fin_resol) is not null and trim(sri.cod_estado) in ('MD','IN','ER','RS','RC') and trim(cvc.cod_estado) in ('MD','IN','ER','RS','RC')
and trim(cvc.fec_fin_resol) > nvl(trim(trim(t.fec_fin_resol_srs)), trim(t.fec_desicion_sw))
and trim(cvc.fec_fin_resol) < trim(sri.fec_fin_resol)
and trim(t.fec_fin_resol_srs) is not null
THEN 'ZONA GRIS'

when trim(cvc.fec_fin_resol) is not null and trim(sri.cod_estado) in ('MD','IN','ER','RS','RC') and trim(cvc.cod_estado) in ('MD','IN','ER','RS','RC')
and trim(cvc.fec_fin_resol) > nvl(trim(trim(t.fec_fin_resol_srs)), trim(t.fec_desicion_sw))
and trim(cvc.fec_fin_resol) < trim(sri.fec_fin_resol)
and trim(t.fec_fin_resol_srs) is null and t.fec_desicion_sw is not null
THEN 'SCORING'

when trim(cvc.fec_fin_resol) is not null and trim(sri.cod_estado) in ('MD','IN','ER','RS','RC') and trim(cvc.cod_estado) in ('MD','IN','ER','RS','RC')
and trim(cvc.fec_fin_resol) > nvl(trim(trim(t.fec_fin_resol_srs)), trim(t.fec_desicion_sw))
and trim(cvc.fec_fin_resol) > trim(sri.fec_fin_resol)
and trim(t.fec_fin_resol_srs) is not null
THEN 'ZONA GRIS'

when trim(sri.fec_fin_resol) is not null and trim(sri.cod_estado) in ('MD','IN','ER','RS','RC')
and trim(sri.fec_fin_resol)>nvl(trim(trim(t.fec_fin_resol_srs)), trim(t.fec_desicion_sw))
and trim(cvc.fec_fin_resol) is null
and trim(t.fec_fin_resol_srs) is null and t.fec_desicion_sw is not null
THEN 'SCORING'

when trim(cvc.fec_fin_resol) is not null and trim(cvc.cod_estado) in ('MD','IN','ER','RS','RC')
and trim(cvc.fec_fin_resol)>nvl(trim(trim(t.fec_fin_resol_srs)), trim(t.fec_desicion_sw))
and trim(sri.fec_fin_resol) is null
and trim(t.fec_fin_resol_srs) is null and t.fec_desicion_sw is not null
THEN 'SCORING'

when trim(sri.fec_fin_resol) is not null and trim(sri.cod_estado) in ('MD','IN','ER','RS','RC')
and trim(sri.fec_fin_resol)>nvl(trim(trim(t.fec_fin_resol_srs)), trim(t.fec_desicion_sw))
and trim(cvc.fec_fin_resol) is null
and trim(t.fec_fin_resol_srs) is NOT null
THEN 'ZONA GRIS'

when trim(cvc.fec_fin_resol) is not null and trim(cvc.cod_estado) in ('MD','IN','ER','RS','RC')
and trim(cvc.fec_fin_resol)>nvl(trim(trim(t.fec_fin_resol_srs)), trim(t.fec_desicion_sw))
and trim(sri.fec_fin_resol) is null
and trim(t.fec_fin_resol_srs) is NOT null
THEN 'ZONA GRIS'

when trim(sri.fec_fin_resol) is not null and trim(sri.cod_estado) in ('MD','IN','ER','RS','RC')
and trim(sri.fec_fin_resol)>nvl(trim(trim(t.fec_fin_resol_srs)), trim(t.fec_desicion_sw))
and trim(cvc.fec_fin_resol) is null
and trim(t.fec_fin_resol_srs) is not null
THEN 'ZONA GRIS'

when trim(cvc.fec_fin_resol) is not null and trim(cvc.cod_estado) in ('MD','IN','ER','RS','RC')
and trim(cvc.fec_fin_resol)>nvl(trim(trim(t.fec_fin_resol_srs)), trim(t.fec_desicion_sw))
and trim(sri.fec_fin_resol) is null
and trim(t.fec_fin_resol_srs) is not null
THEN 'ZONA GRIS'

when trim(cvc.fec_fin_resol) is not null and trim(sri.cod_estado) in ('MD','IN','ER','RS','RC') and trim(cvc.cod_estado) in ('MD','IN','ER','RS','RC')
and trim(cvc.fec_fin_resol)>nvl(trim(trim(t.fec_fin_resol_srs)), trim(t.fec_desicion_sw))
and trim(cvc.fec_fin_resol) > trim(sri.fec_fin_resol)
and trim(t.fec_fin_resol_srs) is null and t.fec_desicion_sw is not null
THEN 'SCORING'

when trim(sri.fec_fin_resol) is not null and trim(cvc.fec_fin_resol) is not null and trim(sri.cod_estado) in ('AP','DL')
and trim(sri.fec_fin_resol)>nvl(trim(trim(t.fec_fin_resol_srs)), trim(t.fec_desicion_sw))
and trim(cvc.fec_fin_resol) < trim(sri.fec_fin_resol)
THEN 'SRI'

when trim(sri.fec_fin_resol) is not null and trim(sri.cod_estado) in ('AP','DL')
and trim(sri.fec_fin_resol)>nvl(trim(trim(t.fec_fin_resol_srs)), trim(t.fec_desicion_sw))
and trim(cvc.fec_fin_resol) is null
THEN 'SRI'

when trim(CVC.fec_fin_resol) is not null and trim(CVC.cod_estado) in ('AP','DL')
and trim(CVC.fec_fin_resol)>nvl(trim(trim(t.fec_fin_resol_srs)), trim(t.fec_desicion_sw))
and trim(SRI.fec_fin_resol) is null
THEN 'CVC'

when trim(cvc.fec_fin_resol) is not null and trim(sri.fec_fin_resol) is not null and trim(cvc.cod_estado) in ('AP','DL')
and trim(cvc.fec_fin_resol)>nvl(trim(trim(t.fec_fin_resol_srs)), trim(t.fec_desicion_sw))
and trim(cvc.fec_fin_resol) > trim(sri.fec_fin_resol)
THEN 'CVC'

when trim(sri.fec_fin_resol) IS not null
AND trim(sri.fec_fin_resol) < nvl(trim(trim(t.fec_fin_resol_srs)), trim(t.fec_desicion_sw))
AND trim(cvc.fec_fin_resol) is null
and trim(t.fec_fin_resol_srs)>trim(t.fec_desicion_sw)
THEN 'ZONA GRIS'

when trim(sri.fec_fin_resol) IS not null
AND trim(sri.fec_fin_resol) < nvl(trim(trim(t.fec_fin_resol_srs)), trim(t.fec_desicion_sw))
AND trim(cvc.fec_fin_resol) is null
and trim(t.fec_fin_resol_srs)<trim(t.fec_desicion_sw)
THEN 'SCORING'

when trim(sri.fec_fin_resol) IS not null
AND trim(sri.fec_fin_resol) < nvl(trim(trim(t.fec_fin_resol_srs)), trim(t.fec_desicion_sw))
AND trim(cvc.fec_fin_resol) is null
and trim(t.fec_fin_resol_srs)>trim(t.fec_desicion_sw)
THEN 'ZONA GRIS'

when trim(sri.fec_fin_resol) IS not null
AND trim(sri.fec_fin_resol) < nvl(trim(trim(t.fec_fin_resol_srs)), trim(t.fec_desicion_sw))
AND trim(cvc.fec_fin_resol) is null
and trim(t.fec_fin_resol_srs)<trim(t.fec_desicion_sw)
THEN 'SCORING'

when trim(sri.fec_fin_resol) IS not null
AND trim(cvc.fec_fin_resol) < nvl(trim(trim(t.fec_fin_resol_srs)), trim(t.fec_desicion_sw))
AND trim(sri.fec_fin_resol) < nvl(trim(trim(t.fec_fin_resol_srs)), trim(t.fec_desicion_sw))
AND trim(cvc.fec_fin_resol) is not null
and trim(t.fec_fin_resol_srs)>trim(t.fec_desicion_sw)
THEN 'ZONA GRIS'

when trim(sri.fec_fin_resol) IS not null
AND trim(cvc.fec_fin_resol) < nvl(trim(trim(t.fec_fin_resol_srs)), trim(t.fec_desicion_sw))
AND trim(sri.fec_fin_resol) < nvl(trim(trim(t.fec_fin_resol_srs)), trim(t.fec_desicion_sw))
AND trim(cvc.fec_fin_resol) is not null
and trim(t.fec_fin_resol_srs)<trim(t.fec_desicion_sw)
THEN 'SCORING'

when trim(cvc.fec_fin_resol) is null and trim(sri.fec_fin_resol) is null
and nvl(trim(trim(t.fec_fin_resol_srs)), trim(t.fec_desicion_sw)) is not null
and trim(t.fec_fin_resol_srs) is not null
THEN 'ZONA GRIS'

when trim(cvc.fec_fin_resol) is null and trim(sri.fec_fin_resol) is null
and nvl(trim(trim(t.fec_fin_resol_srs)), trim(t.fec_desicion_sw)) is not null
and trim(t.fec_fin_resol_srs) is null and t.fec_desicion_sw is not null
THEN 'SCORING'
ELSE '' END AS ds_adm_sector_ultresolriesgos

from bi_corp_staging.alcen_solicitud s
left join bi_corp_staging.alcen_testado t on s.cod_sucursal = t.cod_sucursal and s.nro_solicitud = t.nro_solicitud  and t.partition_date = "{{ ti.xcom_pull(task_ids='InputConfig', key='partition_date', dag_id='LOAD_CMN_Admision_Ind-Daily') }}"
left join bi_corp_staging.alcen_solicitud_preevaluacion sp on s.cod_sucursal = sp.cod_sucursal and s.nro_solicitud = sp.nro_solicitud  and sp.partition_date = "{{ ti.xcom_pull(task_ids='InputConfig', key='partition_date', dag_id='LOAD_CMN_Admision_Ind-Daily') }}"
left join bi_corp_staging.alcen_testado t2 on sp.cod_sucursal_preev = t2.cod_sucursal and sp.nro_solicitud_preev = t2.nro_solicitud  and t2.partition_date = "{{ ti.xcom_pull(task_ids='InputConfig', key='partition_date', dag_id='LOAD_CMN_Admision_Ind-Daily') }}"
left join bi_corp_staging.alcen_participante p on s.cod_sucursal = p.cod_sucursal and s.nro_solicitud = p.nro_solicitud  and trim(p.nro_participante) = '1' and p.partition_date = "{{ ti.xcom_pull(task_ids='InputConfig', key='partition_date', dag_id='LOAD_CMN_Admision_Ind-Daily') }}"
left join bi_corp_staging.alcen_tdatos_adicionales da on s.cod_sucursal = da.cod_sucursal and s.nro_solicitud = da.nro_solicitud and da.partition_date = "{{ ti.xcom_pull(task_ids='InputConfig', key='partition_date', dag_id='LOAD_CMN_Admision_Ind-Daily') }}"
left join tmp_vestado_cvcsri cvc on s.cod_sucursal = cvc.cod_sucursal and s.nro_solicitud = cvc.nro_solicitud and trim(cvc.cod_sector) = 'CVC'
left join tmp_vestado_cvcsri sri on s.cod_sucursal = sri.cod_sucursal and s.nro_solicitud = sri.nro_solicitud and trim(sri.cod_sector) != 'CVC'
left join bi_corp_staging.alcen_ttramites_hash th on concat(lpad(s.cod_sucursal, 3, '0') ,lpad(s.nro_solicitud, 17, '0')) = th.cod_tramite and th.partition_date = "{{ ti.xcom_pull(task_ids='InputConfig', key='partition_date', dag_id='LOAD_CMN_Admision_Ind-Daily') }}"
left join bi_corp_staging.alcen_laboral l on s.cod_sucursal = l.cod_sucursal and s.nro_solicitud = l.nro_solicitud and trim(l.nro_participante) = '1' and trim(l.cod_emp_tpo) != '' and tpo_empleo = '0' and l.partition_date = "{{ ti.xcom_pull(task_ids='InputConfig', key='partition_date', dag_id='LOAD_CMN_Admision_Ind-Daily') }}"
left join bi_corp_staging.risksql5_admision_promociones dp on substring(trim(s.cod_prom_scor),3,2) = trim(dp.promocion) and trim(dp.promocion) != '' and dp.partition_date = "{{ ti.xcom_pull(task_ids='InputConfig', key='partition_date', dag_id='LOAD_CMN_Admision_Ind-Daily') }}"
left join bi_corp_staging.risksql5_admision_productos_promocion dpp on cast(substring(trim(s.cod_prom_scor),1,2) as int) = cast(trim(dpp.producto) as int) and dpp.partition_date = "{{ ti.xcom_pull(task_ids='InputConfig', key='partition_date', dag_id='LOAD_CMN_Admision_Ind-Daily') }}"
left join bi_corp_staging.risksql5_admision_canales_de_venta dc on trim(s.cod_canal) = trim(dc.codigo) and dc.partition_date = "{{ ti.xcom_pull(task_ids='InputConfig', key='partition_date', dag_id='LOAD_CMN_Admision_Ind-Daily') }}"
left join bi_corp_risk.parametria_sucursales ps on trim(s.cod_sucursal) = trim(ps.sucursal) and ps.partition_date = "{{ ti.xcom_pull(task_ids='InputConfig', key='partition_date', dag_id='LOAD_CMN_Admision_Ind-Daily') }}"
left join testado_asol ta31 on s.cod_sucursal = ta31.cod_sucursal and s.nro_solicitud = ta31.nro_solicitud and ta31.cod_estado = '31'
left join testado_asol ta10 on s.cod_sucursal = ta10.cod_sucursal and s.nro_solicitud = ta10.nro_solicitud and ta10.cod_estado = '10'
left join testado_asol ta12 on s.cod_sucursal = ta12.cod_sucursal and s.nro_solicitud = ta12.nro_solicitud and ta12.cod_estado = '12'
left join testado_asol ta13 on s.cod_sucursal = ta13.cod_sucursal and s.nro_solicitud = ta13.nro_solicitud and ta13.cod_estado = '13'
left join bi_corp_common.stk_per_personas pp on pp.cod_per_nup = cast(p.ide_nup as bigint) and pp.partition_date = "{{ ti.xcom_pull(task_ids='InputConfig', key='max_partition_stk_per_personas', dag_id='LOAD_CMN_Admision_Ind-Daily') }}"
left join bi_corp_staging.alcen_tconcesionario cc on trim(cc.cod_concesionario) = trim(da.cod_canal) and cc.partition_date = "{{ ti.xcom_pull(task_ids='InputConfig', key='partition_date', dag_id='LOAD_CMN_Admision_Ind-Daily') }}"
left join simulador_prendarios spr on spr.nro_solicitud = trim(s.nro_solicitud) and spr.cod_sucursal = trim(s.cod_sucursal)
left join tramites_ptovta_tarjeta tpt on trim(s.cod_sucursal) = tpt.cod_sucursal_join and trim(s.nro_solicitud) = tpt.nro_solicitud_join
where s.partition_date = "{{ ti.xcom_pull(task_ids='InputConfig', key='partition_date', dag_id='LOAD_CMN_Admision_Ind-Daily') }}"
) x
;