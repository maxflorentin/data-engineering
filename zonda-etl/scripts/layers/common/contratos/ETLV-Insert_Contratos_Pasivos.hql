set hive.merge.mapfiles=true;
set hive.merge.mapredfiles=true;
set hive.merge.size.per.task=4000000;
set hive.merge.smallfiles.avgsize=16000000;
set hive.exec.dynamic.partition.mode=nonstrict;

insert overwrite table bi_corp_common.contratos_pasivos
partition(partition_date)
SELECT concat(a.entidad,'_',a.centro_alta,'_',a.cuenta,'_',a.producto,'_',a.subprodu,'_',a.divisa) AS cod_contrato,
a.entidad,
a.centro_alta AS centro,
a.cuenta AS num_cuenta,
a.divisa,
producto ,
subprodu ,
producto_contab ,
subprodu_contab ,
a.entidad_contab ,
a.centro_contab ,
sector ,
cod_residencia ,
promocion ,
CASE WHEN ind_codbloq LIKE 'S' THEN 1 ELSE 0 END AS ind_codbloq,
CASE WHEN ind_inmovil LIKE 'S' THEN 1 ELSE 0 END AS ind_inmovil,
CASE WHEN ind_bloases LIKE 'S' THEN 1 ELSE 0 END AS ind_bloases,
CASE WHEN ind_avisos LIKE 'S' THEN 1 ELSE 0 END AS ind_avisos,
CASE WHEN ind_miscela LIKE 'S' THEN 1 ELSE 0 END AS ind_miscela,
ind_mancomu AS cta_mancomunada,
a.IND_PER_FISJUR AS tipo_persona,
CASE WHEN ind_comi_espe LIKE 'S' THEN 1 ELSE 0 END AS ind_comi_espe,
CASE WHEN ind_cta_alterna LIKE 'S' THEN 1 ELSE 0 END AS ind_cta_alterna,
ind_act_pas AS activo_pasivo,
CASE WHEN ind_lca LIKE 'S' THEN 1 ELSE 0 END AS ind_lca,
CASE WHEN ind_cobertura LIKE 'S' THEN 1 ELSE 0 END AS ind_cobertura,
ind_cta_mad AS cta_madre_hija,
CASE WHEN ind_lca_aso LIKE 'S' THEN 1 ELSE 0 END AS ind_lca_aso,
CASE WHEN ind_sobregiro LIKE 'S' THEN 1 ELSE 0 END AS ind_sobregiro,
estado_bloq_cup ,
CASE WHEN ind_bloq_inducido LIKE 'S' THEN 1 ELSE 0 END AS ind_bloq_inducido,
CASE WHEN ind_ctaesp LIKE 'S' THEN 1 ELSE 0 END AS ind_ctaesp,
ind_cobro_imp AS cobro_impuestos,
CASE WHEN ind_cta_dep LIKE 'S' THEN 1 ELSE 0 END AS ind_cta_dep,
CASE WHEN ind_embargada LIKE 'S' THEN 1 ELSE 0 END AS ind_embargada,
ind_cta_inactiva AS cta_inactiva,
CAST(NVL(numer_mov, '0.0') AS INT) AS numer_mov,
CAST(NVL(numer_ret, '0.0') AS INT) AS numer_ret,
CAST(NVL(cnt_gir_period, '0.0') AS INT) AS cnt_gir_period,
CAST(NVL(secuimp, '0.0') AS INT) AS secuimp,
CASE WHEN fecha_ultope NOT IN ('0001-01-01', '9999-12-31' )
     THEN to_date(from_unixtime(UNIX_TIMESTAMP(fecha_ultope,"yyyyMMdd")))
     ELSE NULL END AS fecha_ultope,
CASE WHEN fecha_ultopi NOT IN ('0001-01-01', '9999-12-31' )
     THEN to_date(from_unixtime(UNIX_TIMESTAMP(fecha_ultopi,"yyyyMMdd")))
     ELSE NULL END AS fecha_ultopi,
CASE WHEN fecha_ultope_inac NOT IN ('0001-01-01', '9999-12-31' )
     THEN to_date(from_unixtime(UNIX_TIMESTAMP(fecha_ultope_inac,"yyyyMMdd")))
     ELSE NULL END AS fecha_ultope_inac,
CASE WHEN fecha_penultre NOT IN ('0001-01-01', '9999-12-31' )
     THEN to_date(from_unixtime(UNIX_TIMESTAMP(fecha_penultre,"yyyyMMdd")))
     ELSE NULL END AS fecha_penultre,
CASE WHEN fecha_vtocamp NOT IN ('0001-01-01', '9999-12-31' )
     THEN to_date(from_unixtime(UNIX_TIMESTAMP(fecha_vtocamp,"yyyyMMdd")))
     ELSE NULL END AS fecha_vtocamp,
CASE WHEN fecha_vcto NOT IN ('0001-01-01', '9999-12-31' )
     THEN to_date(from_unixtime(UNIX_TIMESTAMP(fecha_vcto,"yyyyMMdd")))
     ELSE NULL END AS fecha_vcto,
CASE WHEN ind_libperd LIKE 'S' THEN 1 ELSE 0 END AS ind_libperd,
ind_soporte ,
CAST(NVL(linea, '0.0') AS INT) AS linea,
CAST(NVL(pagina, '0.0') AS INT) AS pagina,
plan_comi ,
CAST(concat(substr(a.porc_bonif_mant, 0, length(a.porc_bonif_mant)-5), '.',substr(a.porc_bonif_mant, length(a.porc_bonif_mant)-5), length(a.porc_bonif_mant)) AS DECIMAL(8,5)) AS porc_bonif_mant ,
CAST(concat(substr(a.porc_bonif_proc, 0, length(a.porc_bonif_proc)-5), '.',substr(a.porc_bonif_proc, length(a.porc_bonif_proc)-5), length(a.porc_bonif_proc)) AS DECIMAL(8,5)) AS porc_bonif_proc ,
cta_entidad ,
cta_centro ,
cta_cuenta ,
tipo_cambio ,
CAST(concat(substr(a.saldo_dispue, 0, length(a.saldo_dispue)-2), '.',substr(a.saldo_dispue, length(a.saldo_dispue)-2), length(a.saldo_dispue)) AS DECIMAL(15,2)) AS saldo_dispue,
CAST(concat(substr(a.saldo_impago, 0, length(a.saldo_impago)-2), '.',substr(a.saldo_impago, length(a.saldo_impago)-2), length(a.saldo_impago)) AS DECIMAL(15,2)) AS saldo_impago,
CAST(concat(substr(a.saldo_retenc, 0, length(a.saldo_retenc)-2), '.',substr(a.saldo_retenc, length(a.saldo_retenc)-2), length(a.saldo_retenc)) AS DECIMAL(15,2)) AS saldo_retenc ,
CAST(concat(substr(a.saldo_retenc_liq, 0, length(a.saldo_retenc_liq)-2), '.',substr(a.saldo_retenc_liq, length(a.saldo_retenc_liq)-2), length(a.saldo_retenc_liq)) AS DECIMAL(15,2)) AS saldo_retenc_liq ,
CAST(concat(substr(a.saldo_present, 0, length(a.saldo_present)-2), '.',substr(a.saldo_present, length(a.saldo_present)-2), length(a.saldo_present)) AS DECIMAL(15,2)) AS saldo_present ,
CAST(concat(substr(a.saldo_ret_prec, 0, length(a.saldo_ret_prec)-2), '.',substr(a.saldo_ret_prec, length(a.saldo_ret_prec)-2), length(a.saldo_ret_prec)) AS DECIMAL(15,2)) AS saldo_ret_prec ,
CAST(concat(substr(a.saldo_ret_48, 0, length(a.saldo_ret_48)-2), '.',substr(a.saldo_ret_48, length(a.saldo_ret_48)-2), length(a.saldo_ret_48)) AS DECIMAL(15,2)) AS saldo_ret_48 ,
CAST(concat(substr(a.saldo_ret_72, 0, length(a.saldo_ret_72)-2), '.',substr(a.saldo_ret_72, length(a.saldo_ret_72)-2), length(a.saldo_ret_72)) AS DECIMAL(15,2)) AS saldo_ret_72 ,
CAST(concat(substr(a.saldo_ret_hoy, 0, length(a.saldo_ret_hoy)-2), '.',substr(a.saldo_ret_hoy, length(a.saldo_ret_hoy)-2), length(a.saldo_ret_hoy)) AS DECIMAL(15,2)) AS saldo_ret_hoy ,
CAST(concat(substr(a.saldo_ret_hoy_ch, 0, length(a.saldo_ret_hoy_ch)-2), '.',substr(a.saldo_ret_hoy_ch, length(a.saldo_ret_hoy_ch)-2), length(a.saldo_ret_hoy_ch)) AS DECIMAL(15,2)) AS saldo_ret_hoy_ch ,
indesta AS cta_estado,
indsitu_ob AS cta_st_objetiva,
indsitu_su AS cta_st_subjetiva,
indsitu_in AS cta_st_inducida,
CAST(concat(substr(a.saldo_dispue_ca, 0, length(a.saldo_dispue_ca)-2), '.',substr(a.saldo_dispue_ca, length(a.saldo_dispue_ca)-2), length(a.saldo_dispue_ca)) AS DECIMAL(15,2)) AS saldo_dispue_ca ,
tipo_vto ,
num_vto ,
uni_negocio ,
periodo_liq ,
dianatu_liq ,
CASE WHEN fecha_ultliq NOT IN ('0001-01-01', '9999-12-31' )
     THEN to_date(from_unixtime(UNIX_TIMESTAMP(fecha_ultliq,"yyyyMMdd")))
     ELSE NULL END AS fecha_ultliq,
CASE WHEN fecha_proliq NOT IN ('0001-01-01', '9999-12-31' )
     THEN to_date(from_unixtime(UNIX_TIMESTAMP(fecha_proliq,"yyyyMMdd")))
     ELSE NULL END AS fecha_proliq,
franquic ,
tipo_liq ,
metod_liq ,
saldo_liq ,
dias_liq ,
clase_liq ,
tarifa ,
CASE WHEN fecha_ultcam NOT IN ('0001-01-01', '9999-12-31' )
     THEN to_date(from_unixtime(UNIX_TIMESTAMP(fecha_ultcam,"yyyyMMdd")))
     ELSE NULL END AS fecha_ultcam,
CAST(NVL(refer_liq, '0.0') AS INT) AS refer_liq,
clase_taf ,
CASE WHEN fecha_preape NOT IN ('0001-01-01', '9999-12-31' )
     THEN to_date(from_unixtime(UNIX_TIMESTAMP(fecha_preape,"yyyyMMdd")))
     ELSE NULL END AS fecha_preape,
CASE WHEN fecha_alta NOT IN ('0001-01-01', '9999-12-31' )
     THEN to_date(from_unixtime(UNIX_TIMESTAMP(fecha_alta,"yyyyMMdd")))
     ELSE NULL END AS fecha_alta,
CASE WHEN fecha_impaga NOT IN ('0001-01-01', '9999-12-31' )
     THEN to_date(from_unixtime(UNIX_TIMESTAMP(fecha_impaga,"yyyyMMdd")))
     ELSE NULL END AS fecha_impaga,
CASE WHEN fecha_bloqueo NOT IN ('0001-01-01', '9999-12-31' )
     THEN to_date(from_unixtime(UNIX_TIMESTAMP(fecha_bloqueo,"yyyyMMdd")))
     ELSE NULL END AS fecha_bloqueo,
CASE WHEN fecha_inmovil NOT IN ('0001-01-01', '9999-12-31' )
     THEN to_date(from_unixtime(UNIX_TIMESTAMP(fecha_inmovil,"yyyyMMdd")))
     ELSE NULL END AS fecha_inmovil,
CASE WHEN fecha_cancel NOT IN ('0001-01-01', '9999-12-31' )
     THEN to_date(from_unixtime(UNIX_TIMESTAMP(fecha_cancel,"yyyyMMdd")))
     ELSE NULL END AS fecha_cancel,
CASE WHEN fecha_cierre NOT IN ('0001-01-01', '9999-12-31' )
     THEN to_date(from_unixtime(UNIX_TIMESTAMP(fecha_cierre,"yyyyMMdd")))
     ELSE NULL END AS fecha_cierre,
CASE WHEN fecha_renova NOT IN ('0001-01-01', '9999-12-31' )
     THEN to_date(from_unixtime(UNIX_TIMESTAMP(fecha_renova,"yyyyMMdd")))
     ELSE NULL END AS fecha_renova,
CASE WHEN fecha_ultreaju NOT IN ('0001-01-01', '9999-12-31' )
     THEN to_date(from_unixtime(UNIX_TIMESTAMP(fecha_ultreaju,"yyyyMMdd")))
     ELSE NULL END AS fecha_ultreaju,
CASE WHEN fecha_proreaju NOT IN ('0001-01-01', '9999-12-31' )
     THEN to_date(from_unixtime(UNIX_TIMESTAMP(fecha_proreaju,"yyyyMMdd")))
     ELSE NULL END AS fecha_proreaju,
CASE WHEN fecha_per_rea NOT IN ('0001-01-01', '9999-12-31' )
     THEN to_date(from_unixtime(UNIX_TIMESTAMP(fecha_per_rea,"yyyyMMdd")))
     ELSE NULL END AS fecha_per_rea,
CASE WHEN fecha_iniciali NOT IN ('0001-01-01', '9999-12-31' )
     THEN to_date(from_unixtime(UNIX_TIMESTAMP(fecha_iniciali,"yyyyMMdd")))
     ELSE NULL END AS fecha_iniciali,
CASE WHEN fecha_proc_liq NOT IN ('0001-01-01', '9999-12-31' )
     THEN to_date(from_unixtime(UNIX_TIMESTAMP(fecha_proc_liq,"yyyyMMdd")))
     ELSE NULL END AS fecha_proc_liq,
CASE WHEN fecha_upro_liq NOT IN ('0001-01-01', '9999-12-31' )
     THEN to_date(from_unixtime(UNIX_TIMESTAMP(fecha_upro_liq,"yyyyMMdd")))
     ELSE NULL END AS fecha_upro_liq,
CASE WHEN fecha_cap_liq NOT IN ('0001-01-01', '9999-12-31' )
     THEN to_date(from_unixtime(UNIX_TIMESTAMP(fecha_cap_liq,"yyyyMMdd")))
     ELSE NULL END AS fecha_cap_liq,
CASE WHEN fecha_excedi NOT IN ('0001-01-01', '9999-12-31' )
     THEN to_date(from_unixtime(UNIX_TIMESTAMP(fecha_excedi,"yyyyMMdd")))
     ELSE NULL END AS fecha_excedi,
CASE WHEN fecha_pri_exce NOT IN ('0001-01-01', '9999-12-31' )
     THEN to_date(from_unixtime(UNIX_TIMESTAMP(fecha_pri_exce,"yyyyMMdd")))
     ELSE NULL END AS fecha_pri_exce,
CASE WHEN fecha_alt_acu NOT IN ('0001-01-01', '9999-12-31' )
     THEN to_date(from_unixtime(UNIX_TIMESTAMP(fecha_alt_acu,"yyyyMMdd")))
     ELSE NULL END AS fecha_alt_acu,
CASE WHEN fecha_ult_per NOT IN ('0001-01-01', '9999-12-31' )
     THEN to_date(from_unixtime(UNIX_TIMESTAMP(fecha_ult_per,"yyyyMMdd")))
     ELSE NULL END AS fecha_ult_per,
CASE WHEN fecha_ultso NOT IN ('0001-01-01', '9999-12-31' )
     THEN to_date(from_unixtime(UNIX_TIMESTAMP(fecha_ultso,"yyyyMMdd")))
     ELSE NULL END AS fecha_ultso,
CASE WHEN fecha_ultss NOT IN ('0001-01-01', '9999-12-31' )
     THEN to_date(from_unixtime(UNIX_TIMESTAMP(fecha_ultss,"yyyyMMdd")))
     ELSE NULL END AS fecha_ultss,
CASE WHEN fecha_ultsi NOT IN ('0001-01-01', '9999-12-31' )
     THEN to_date(from_unixtime(UNIX_TIMESTAMP(fecha_ultsi,"yyyyMMdd")))
     ELSE NULL END AS fecha_ultsi,
cnt_cartola,
pco_ecu_per ,
pco_ecu_sop ,
CASE WHEN pco_ecu_fhpr NOT IN ('0001-01-01', '9999-12-31' )
     THEN to_date(from_unixtime(UNIX_TIMESTAMP(pco_ecu_fhpr,"yyyyMMdd")))
     ELSE NULL END AS pco_ecu_fhpr,
pco_cop_ind ,
CAST(NVL(pco_cop_ulmo, '0.0') AS INT) AS pco_cop_ulmo,
CASE WHEN pco_cop_ulco NOT IN ('0001-01-01', '9999-12-31' )
     THEN to_date(from_unixtime(UNIX_TIMESTAMP(pco_cop_ulco,"yyyyMMdd")))
     ELSE NULL END AS pco_cop_ulco,
CAST(NVL(pco_cop_hoja, '0.0') AS INT) AS pco_cop_hoja,
CAST(concat(substr(pco_saldo_ulex, 0, length(pco_saldo_ulex)-2), '.',substr(pco_saldo_ulex, length(pco_saldo_ulex)-2), length(pco_saldo_ulex)) AS DECIMAL(15,2)) AS pco_saldo_ulex ,
pco_ind_destino,
CASE WHEN envcart_sinmov LIKE 'S' THEN 1 ELSE 0 END AS envcart_sinmov,
motivo_cancel ,
motivo_reaper ,
sucursal_fisica ,
CAST(concat(substr(saldo_uliq_va, 0, length(saldo_uliq_va)-2), '.',substr(saldo_uliq_va, length(saldo_uliq_va)-2), length(saldo_uliq_va)) AS DECIMAL(15,2)) AS saldo_uliq_va ,
per_abo_reaju ,
CAST(NVL(dias_desp_pro, '0.0') AS INT) AS dias_desp_pro,
CAST(NVL(dias_desp_cap, '0.0') AS INT) AS dias_desp_cap,
ind_tipodia AS tipo_dia_liq,
ind_paquete AS cta_paquete,
CASE WHEN ind_pivote LIKE 'S' THEN 1 ELSE 0 END AS ind_pivote,
CASE WHEN ind_pet_liq LIKE 'S' THEN 1 ELSE 0 END AS ind_pet_liq ,
CASE WHEN ind_bonifica LIKE 'S' THEN 1 ELSE 0 END AS ind_bonifica ,
CASE WHEN ind_excauto LIKE 'S' THEN 1 ELSE 0 END AS ind_excauto ,
CASE WHEN ind_renauto LIKE 'S' THEN 1 ELSE 0 END AS ind_renauto ,
CASE WHEN ind_sorteo LIKE 'S' THEN 1 ELSE 0 END AS ind_sorteo ,
CASE WHEN ind_fantasia LIKE 'S' THEN 1 ELSE 0 END AS ind_fantasia ,
CASE WHEN ind_seg_pr_camara LIKE 'S' THEN 1 ELSE 0 END AS ind_seg_pr_camara ,
CASE WHEN ind_acu_ahorro LIKE 'S' THEN 1 ELSE 0 END AS ind_acu_ahorro ,
CASE WHEN ind_bloq_entre LIKE 'S' THEN 1 ELSE 0 END AS ind_bloq_entre ,
motivo_bloq ,
CAST(NVL(stock_tln, '0.0') AS INT) AS stock_tln,
sucursal_ent ,
CAST(NVL(domicilio_ent, '0.0') AS INT) AS domicilio_ent,
tipo_tln_stndr ,
CAST(concat(substr(saldo_uliq_va_ca, 0, length(saldo_uliq_va_ca)-2), '.',substr(saldo_uliq_va_ca, length(saldo_uliq_va_ca)-2), length(saldo_uliq_va_ca)) AS DECIMAL(15,2)) AS saldo_uliq_va_ca ,
CAST(concat(substr(pco_saldo_ulex_ca, 0, length(pco_saldo_ulex_ca)-2), '.',substr(pco_saldo_ulex_ca, length(pco_saldo_ulex_ca)-2), length(pco_saldo_ulex_ca)) AS DECIMAL(15,2)) AS pco_saldo_ulex_ca,
a.partition_date
FROM bi_corp_staging.bgdtmae a
INNER JOIN bi_corp_staging.bgdtaux b on a.entidad = b.entidad AND a.centro_alta = b.centro_alta
AND a.cuenta = b.cuenta AND a.divisa = b.divisa AND a.partition_date = b.partition_date
WHERE a.partition_date=to_date(from_unixtime(UNIX_TIMESTAMP('{{ ti.xcom_pull(task_ids='InputConfig', key='partition_date', dag_id='LOAD_CMN_Contratos-Daily') }}',"yyyy-MM-dd"))) AND a.producto IN ('02', '03', '04', '14', '99');