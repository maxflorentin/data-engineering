set hive.merge.mapfiles=true;
set hive.merge.mapredfiles=true;
set hive.merge.size.per.task=4000000;
set hive.merge.smallfiles.avgsize=16000000;
set hive.exec.dynamic.partition.mode=nonstrict;
set mapred.job.queue.name=root.dataeng;

insert overwrite table bi_corp_common.stk_rcp_contratosmoria
partition(partition_date='{{ ti.xcom_pull(task_ids='InputConfig', key='partition_date', dag_id='LOAD_CMN_RECUPERACIONES_GarraMoria-Daily') }}')

SELECT
cast(m.mdec1610_cod_centro as int) as cod_suc_sucursal,
cast(m.mdec1610_num_contrato as bigint) as cod_nro_cuenta,
m.mdec1610_cod_producto as cod_prod_producto,
m.mdec1610_cod_subprodu as cod_prod_subproducto,
m.mdec1610_cod_divisa as cod_div_divisa,
m.mdec1610_fec_ingreso as dt_rcp_fechaingreso,
m.mdec1610_fec_inisitir as dt_rcp_fechainisituacionirregular,
m.mdec1610_cod_tipotasa as cod_rcp_tipotasa,
m.mdec1610_int_tasa as fc_int_tasa,
m.mdec1610_cod_tasa as cod_rcp_tasa,
m.mdec1610_cod_estado as cod_rcp_estado,
case when to_date(m.mdec1610_fec_castigo) in ('9999-12-31', '0001-01-01') then NULL else to_date(m.mdec1610_fec_castigo) end as dt_rcp_fechacastigo,
case when m.mdec1610_cod_origcast = '' then NULL else m.mdec1610_cod_origcast end as cod_rcp_origencastigo,
case when to_date(m.mdec1610_fec_abseguim) in ('9999-12-31', '0001-01-01') then NULL else to_date(m.mdec1610_fec_abseguim) end as dt_rcp_fechaabseguimiento,
case when to_date(m.mdec1610_fec_abtotal) in ('9999-12-31', '0001-01-01') then NULL else to_date(m.mdec1610_fec_abtotal) end as dt_rcp_fechaabtotal,
case when m.mdec1610_cod_garantia = '' then NULL else m.mdec1610_cod_garantia end as cod_rcp_garantia,
cast(m.mdec1610_num_persona as bigint) as cod_per_nup,
m.mdec1610_tip_documento as cod_rcp_tipodocumento,
m.mdec1610_num_documento as ds_rcp_numdocumento,
case when m.mdec1610_nucta = '' then NULL else m.mdec1610_nucta end as ds_rcp_nucta,
m.mdec1610_tip_persona as cod_rcp_tipopersona,
m.mdec1610_sex_persona as cod_rcp_sexo,
m.mdec1610_tot_capital as fc_rcp_importetotalcapital,
m.mdec1610_tot_ajucapug as fc_rcp_importetotalajustecapitalug,
m.mdec1610_tot_interes as fc_rcp_importetotalinteres,
m.mdec1610_sdo_intedocu as fc_rcp_importesaldointeresdocu,
m.mdec1610_tot_impuesto as fc_rcp_importetotalimpuestos,
case when m.mdec1610_cod_marcontr = '' then NULL else m.mdec1610_cod_marcontr end as cod_rcp_marca,
case when m.mdec1610_cod_smarcont = '' then NULL else m.mdec1610_cod_smarcont end as cod_rcp_submarca,
m.mdec1610_cod_motbajac as cod_rcp_marcabaja,
case when m.mdec1610_num_bufete = '' then NULL else m.mdec1610_num_bufete end as cod_rcp_bufete,
m.mdec1610_cod_banca as cod_rcp_banca,
m.mdec1610_cod_sector as cod_rcp_sector,
m.mdec1610_cod_oficont as cod_rcp_sucursalcontrato,
case when to_date(m.mdec1610_fec_apertura) in ('9999-12-31', '0001-01-01') then NULL else to_date(m.mdec1610_fec_apertura) end as dt_rcp_fechaapertura,
m.mdec1610_cod_clasepro as cod_rcp_claseproducto,
case when m.mdec1610_cod_subclpro = '' then NULL else m.mdec1610_cod_subclpro end as cod_rcp_subclaseproducto,
case when to_date(m.mdec1610_fec_ultlqint) in ('9999-12-31', '0001-01-01') then NULL else to_date(m.mdec1610_fec_ultlqint) end as dt_rcp_ultimaliquidacionintereses,
case when to_date(m.mdec1610_fec_ultlqesp) in ('9999-12-31', '0001-01-01') then NULL else to_date(m.mdec1610_fec_ultlqesp) end as dt_rcp_ultimaliquidacionespecial,
case when to_date(m.mdec1610_fec_ultamort) in ('9999-12-31', '0001-01-01') then NULL else to_date(m.mdec1610_fec_ultamort) end as dt_rcp_fechaultimaamortizacion,
case when to_date(m.mdec1610_fec_vto) in ('9999-12-31', '0001-01-01') then NULL else to_date(m.mdec1610_fec_vto) end as dt_rcp_fechavencimiento,
case when m.mdec1610_cod_clarevis = '' then NULL else m.mdec1610_cod_clarevis end as cod_rcp_clarevis,
m.mdec1610_dia_calendar as ds_rcp_diacalendario,
case when m.mdec1610_tip_amortiza = '' then NULL else m.mdec1610_tip_amortiza end as ds_rcp_tipoamortizacion,
case when m.mdec1610_tip_producto = '' then NULL else m.mdec1610_tip_producto end as ds_rcp_tipoproducto,
case when m.mdec1610_cod_instru = '' then NULL else m.mdec1610_cod_instru end as cod_rcp_instrumento,
case when m.mdec1610_cod_destfond = '' then NULL else m.mdec1610_cod_destfond end as cod_rcp_destinofondo,
m.mdec1610_imp_concedid as fc_rcp_importeconcedido,
case when m.mdec1610_cod_plazo = '' then NULL else m.mdec1610_cod_plazo end as cod_rcp_plazo,
case when m.mdec1610_per_amortcap = '' then NULL else m.mdec1610_per_amortcap end as ds_rcp_peramortcap,
case when m.mdec1610_per_interes = '' then NULL else m.mdec1610_per_interes end as ds_rcp_perinteres,
m.mdec1610_num_plazopen as ds_rcp_plazopendiente,
case when to_date(m.mdec1610_fec_ultfactu) in ('9999-12-31', '0001-01-01') then NULL else to_date(m.mdec1610_fec_ultfactu) end as dt_rcp_fechaultimafactura,
case when m.mdec1610_tip_premio = '' then NULL else m.mdec1610_tip_premio end as ds_rcp_tipopremio,
m.mdec1610_por_premio as fc_rcp_porcentajepremio,
case when m.mdec1610_cod_coefinde = '' then NULL else m.mdec1610_cod_coefinde end as cod_rcp_coeficienteindex,
case when m.mdec1610_cod_proceden = '' then NULL else m.mdec1610_cod_proceden end as cod_rcp_procedencia,
case when m.mdec1610_tip_titulari = '' then NULL else m.mdec1610_tip_titulari end as ds_rcp_tipotitular,
case when m.mdec1610_nup_plansldo = '' then NULL else m.mdec1610_nup_plansldo end as cod_rcp_nupplansueldo,
case when to_date(m.mdec1610_fec_prevision) in ('9999-12-31', '0001-01-01') then NULL else to_date(m.mdec1610_fec_prevision) end as dt_rcp_fechaprevision,
case when to_date(m.mdec1610_fec_infveraz) in ('9999-12-31', '0001-01-01') then NULL else to_date(m.mdec1610_fec_infveraz) end as dt_rcp_fechainformeveraz,
case when m.mdec1610_cod_refinanc = '' then NULL else m.mdec1610_cod_refinanc end as flag_rcp_refinanciacion,
m.mdec1610_ind_altair as flag_rcp_indaltair,
m.mdec1610_ind_conversi as flag_rcp_indconversi,
m.mdec1610_ind_quita as flag_rcp_indquita,
m.mdec1610_cod_tipadmin as cod_rcp_tipoadmin,
m.mdec1610_imp_castigo as fc_rcp_importecastigo,
m.mdec1610_imp_deudaing as fc_rcp_importedeudaingreso,
m.mdec1610_imp_deudaegr as fc_rcp_importedeudaegreso,
case when m.mdec1610_num_ctaint = '' then NULL else m.mdec1610_num_ctaint end as cod_rcp_numcuentainteres,
case when m.mdec1610_num_ctaaju = '' then NULL else m.mdec1610_num_ctaaju end as cod_rcp_numcuentaajuste,
case when to_date(m.mdec1610_fec_baja) in ('9999-12-31', '0001-01-01') then NULL else to_date(m.mdec1610_fec_baja) end as dt_rcp_fechabaja,
case s.segmento when 'INDIVIDUOS' then p.categoria_particulares else p.categoria_carterizados end as ds_rcp_categoriaproducto,
trim(s.segmento_control) as ds_rcp_descripcionsegmento,
trim(s.ramo) as cod_rcp_segmento,
trim(s.subsegmento) as cod_rcp_subsegmento
FROM bi_corp_staging.moria_contratos_md m
LEFT JOIN bi_corp_staging.malpe_ptb_pedt030 p30 on m.mdec1610_num_persona = p30.penumper and p30.partition_date = '{{ ti.xcom_pull(task_ids='InputConfig', key='partition_date', dag_id='LOAD_CMN_RECUPERACIONES_GarraMoria-Daily') }}'
LEFT JOIN bi_corp_staging.risksql5_segmentos s ON trim(p30.pesegcla) = trim(s.ramo) and s.fecha_informacion = '{{ ti.xcom_pull(task_ids='InputConfig', key='partition_date', dag_id='LOAD_CMN_RECUPERACIONES_GarraMoria-Daily') }}'
LEFT JOIN bi_corp_staging.risksql5_productos p ON trim(m.mdec1610_cod_producto) = trim(p.codigo_producto) and trim(m.mdec1610_cod_subprodu) = trim(p.codigo_subproducto)  and p.fecha_informacion = '{{ ti.xcom_pull(task_ids='InputConfig', key='partition_date', dag_id='LOAD_CMN_RECUPERACIONES_GarraMoria-Daily') }}'
WHERE m.partition_date = '{{ ti.xcom_pull(task_ids='InputConfig', key='partition_date', dag_id='LOAD_CMN_RECUPERACIONES_GarraMoria-Daily') }}'
;