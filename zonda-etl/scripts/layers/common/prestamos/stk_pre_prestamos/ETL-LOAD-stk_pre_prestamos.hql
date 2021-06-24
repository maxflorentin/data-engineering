set hive.merge.mapfiles=true;
set hive.merge.mapredfiles=true;
set hive.merge.size.per.task=4000000;
set hive.merge.smallfiles.avgsize=16000000;
set hive.exec.dynamic.partition.mode=nonstrict;
set mapred.job.queue.name=root.dataeng;

DROP TABLE IF EXISTS pedt008;
CREATE TEMPORARY TABLE pedt008 AS
select p.pecodofi, p.penumcon, p.pecodpro, p.pecodsub, p.penumper
from (
      select pecodofi, penumcon, pecodpro, pecodsub, penumper, ROW_NUMBER() OVER (PARTITION BY pecodofi, penumcon, pecodpro, pecodsub ORDER BY coalesce(pefecbrb,'9999-12-31') DESC, peordpar ASC) AS min_firmante
      from bi_corp_staging.malpe_ptb_pedt008
      where partition_date = '{{ ti.xcom_pull(task_ids='InputConfig', key='max_partition_malpe_ptb_pedt008', dag_id='LOAD_CMN_PRESTAMOS-Daily') }}'
      AND pecalpar = 'TI'
      AND pecodpro IN ('35', '36', '37', '38', '39', '71', '74')
     ) p
where p.min_firmante = 1;

DROP TABLE IF EXISTS pedt70;
CREATE TEMPORARY TABLE pedt70 AS
select pecodofi, penumcon, pecodpro, pecodsub
from bi_corp_staging.malpe_ptb_pedt008
where partition_date='{{ ti.xcom_pull(task_ids='InputConfig', key='max_partition_malpe_ptb_pedt008', dag_id='LOAD_CMN_PRESTAMOS-Daily') }}'
AND pecodpro = '70'
AND peestrel = 'A'
AND pecalpar = 'TI'
group by pecodofi, penumcon, pecodpro, pecodsub;

DROP TABLE IF EXISTS ultima_cuota;
CREATE TEMPORARY TABLE ultima_cuota AS
select distinct rec_entidad as entidad, rec_oficina as oficina, rec_cuenta as cuenta, fecha, monto
from(select rec.rec_entidad, rec.rec_oficina, rec.rec_cuenta,
FIRST_VALUE (mvr.mvr_fevalor) OVER (PARTITION BY rec.rec_entidad, rec.rec_oficina, rec.rec_cuenta ORDER BY mvr.mvr_fevalor DESC, rec.rec_numrec DESC ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING) AS fecha,
FIRST_VALUE (rec.REC_CAPINIRE + rec.REC_INTINIRE + rec.REC_GASINIRE + rec.REC_SEGINIRE + rec.REC_IMPINIRE + rec.REC_IMP_MORCAL + rec.REC_IMP_MORREC) OVER (PARTITION BY rec.rec_entidad, rec.rec_oficina, rec.rec_cuenta ORDER BY mvr.mvr_fevalor DESC, rec.rec_numrec DESC ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING) as monto
from bi_corp_staging.malug_ugdtrec rec
left join bi_corp_staging.malug_ugdtmvr mvr on mvr.mvr_entidad = rec.rec_entidad and mvr.mvr_cuenta=rec.rec_cuenta and mvr.mvr_oficina=rec.rec_oficina and rec.rec_feliq=mvr.mvr_feliq and rec.partition_date=mvr.partition_date
where rec.partition_date= '{{ ti.xcom_pull(task_ids='InputConfig', key='partition_date', dag_id='LOAD_CMN_PRESTAMOS-Daily') }}'
and mvr.partition_date= '{{ ti.xcom_pull(task_ids='InputConfig', key='partition_date', dag_id='LOAD_CMN_PRESTAMOS-Daily') }}'
and rec_userid_umo <> 'MORIA'
and not(rec_indpeco ='0' and to_date(rec_timestamp) <= add_months(trunc(TO_DATE(FROM_UNIXTIME(UNIX_TIMESTAMP())),'MM'),-12))
and not(rec_indpeco ='0' and to_date(rec_feliq) <= add_months(trunc(TO_DATE(FROM_UNIXTIME(UNIX_TIMESTAMP())),'MM'),-12))
and rec_indpeco ='0'
and mvr.mvr_indretro='N'
and mvr.mvr_fevalor >= add_months(trunc(TO_DATE(FROM_UNIXTIME(UNIX_TIMESTAMP())),'MM'),-12)) t;

DROP TABLE IF EXISTS ultimo_recibo;
CREATE TEMPORARY TABLE ultimo_recibo AS
select rec.rec_entidad,
rec.rec_oficina,
rec.rec_cuenta,
rec.rec_capinire,
rec.rec_intinire,
rec.rec_seginire,
rec.rec_cominire + rec.rec_gasinire + rec.rec_impsubve + rec.rec_impinire + rec.rec_imp_morcal + rec.rec_imp_cpscal as mon_cuo_otr,
rec.rec_feliq,
rec.partition_date
from bi_corp_staging.malug_ugdtrec rec
inner join (select rec.rec_entidad,
            rec.rec_oficina,
            rec.rec_cuenta,
            max(rec.rec_feliq) max_feliq
            from bi_corp_staging.malug_ugdtrec rec
            where rec.partition_date= '{{ ti.xcom_pull(task_ids='InputConfig', key='partition_date', dag_id='LOAD_CMN_PRESTAMOS-Daily') }}'
            and rec_indpeco ='0'
            group by rec.rec_entidad, rec.rec_oficina, rec.rec_cuenta) max_rec on max_rec.rec_entidad = rec.rec_entidad and max_rec.rec_cuenta=rec.rec_cuenta and max_rec.rec_oficina=rec.rec_oficina and rec.rec_feliq=max_rec.max_feliq
where rec.partition_date='{{ ti.xcom_pull(task_ids='InputConfig', key='partition_date', dag_id='LOAD_CMN_PRESTAMOS-Daily') }}'
and rec_indpeco ='0';

DROP TABLE IF EXISTS garantia;
CREATE TEMPORARY TABLE garantia AS
select distinct rgo.gttcrgo_rgo_contrato_rgo_cod_entcont entidad,rgo.gttcrgo_rgo_contrato_rgo_num_ctacont cuenta,rgo.gttcrgo_rgo_contrato_rgo_cod_oficont oficina,
cast(ma.auma_cd_marca as int) cod_auto_marca_prestamo,
ma.auma_de_marca auto_marca_prestamo,
cast(trim(mo.aumo_cd_modelo) as int) cod_auto_modelo_prestamo,
mo.aumo_de_modelo auto_modelo_prestamo,
veh.gttcveh_veh_anio_fabricac anio_auto_prestamo
from bi_corp_staging.gtdtrgo rgo
inner join bi_corp_staging.gtdtrgb rgb on rgb.gttcrgb_rgb_num_garantia= rgo.gttcrgo_rgo_num_garantia and rgb.partition_date=rgo.partition_date
inner join bi_corp_staging.gtdtveh veh on rgb.gttcrgb_rgb_num_bien=veh.gttcveh_veh_num_bien and veh.partition_date=rgb.partition_date
left join bi_corp_staging.gtdtmae mae on mae.gttcmae_mae_num_garantia=rgb.gttcrgb_rgb_num_garantia and mae.partition_date ='{{ ti.xcom_pull(task_ids='InputConfig', key='partition_date', dag_id='LOAD_CMN_PRESTAMOS-Daily') }}'
left join bi_corp_staging.gtdtgar gar on gar.gttcgar_gar_cod_garantia= mae.gttcmae_mae_cod_garantia and gar.partition_date ='{{ ti.xcom_pull(task_ids='InputConfig', key='partition_date', dag_id='LOAD_CMN_PRESTAMOS-Daily') }}'
left join bi_corp_staging.rio6_autt_marcas ma on cast(veh.gttcveh_veh_cod_marcaveh as int)=cast(ma.auma_cd_marca as int) and ma.partition_date ='{{ ti.xcom_pull(task_ids='InputConfig', key='partition_date', dag_id='LOAD_CMN_PRESTAMOS-Daily') }}'
left join bi_corp_staging.rio6_autt_modelos mo on cast(trim(mo.aumo_cd_modelo) as int)=cast(trim(veh.gttcveh_veh_cod_modelove) as int) and cast(trim(mo.aumo_auma_cd_marca) as int)=cast(trim(veh.gttcveh_veh_cod_marcaveh) as int) and mo.partition_date ='{{ ti.xcom_pull(task_ids='InputConfig', key='partition_date', dag_id='LOAD_CMN_PRESTAMOS-Daily') }}'
where rgo.partition_date = '{{ ti.xcom_pull(task_ids='InputConfig', key='partition_date', dag_id='LOAD_CMN_PRESTAMOS-Daily') }}'
and rgb.partition_date = '{{ ti.xcom_pull(task_ids='InputConfig', key='partition_date', dag_id='LOAD_CMN_PRESTAMOS-Daily') }}'
and veh.partition_date = '{{ ti.xcom_pull(task_ids='InputConfig', key='partition_date', dag_id='LOAD_CMN_PRESTAMOS-Daily') }}'
and rgo.gttcrgo_rgo_fec_altrelac <= '{{ ti.xcom_pull(task_ids='InputConfig', key='partition_date', dag_id='LOAD_CMN_PRESTAMOS-Daily') }}'
and rgo.gttcrgo_rgo_fec_bajrelac >= '{{ ti.xcom_pull(task_ids='InputConfig', key='partition_date', dag_id='LOAD_CMN_PRESTAMOS-Daily') }}'
and rgb.gttcrgb_rgb_fec_inivali <= '{{ ti.xcom_pull(task_ids='InputConfig', key='partition_date', dag_id='LOAD_CMN_PRESTAMOS-Daily') }}'
and rgb.gttcrgb_rgb_fec_finvali >= '{{ ti.xcom_pull(task_ids='InputConfig', key='partition_date', dag_id='LOAD_CMN_PRESTAMOS-Daily') }}'
and gar.gttcgar_gar_des_corta ='PRENDA';

DROP TABLE IF EXISTS solicitudes;
CREATE TEMPORARY TABLE solicitudes AS
select distinct cod_suc_sucursalcuenta,
cod_nro_cuenta,
dt_cys_fechalta,
cod_cys_nrosolicitud,
cod_suc_sucursal
from (select sol.cod_suc_sucursalcuenta,
sol.cod_nro_cuenta,
first_value(sol.dt_cys_fechalta) over (partition by sol.cod_suc_sucursalcuenta,sol.cod_nro_cuenta order by sol.dt_cys_fechalta desc, cod_cys_nrosolicitud desc ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING) dt_cys_fechalta,
first_value(sol.cod_cys_nrosolicitud) over (partition by sol.cod_suc_sucursalcuenta,sol.cod_nro_cuenta order by sol.dt_cys_fechalta desc, cod_cys_nrosolicitud desc ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING) cod_cys_nrosolicitud,
first_value(sol.cod_suc_sucursal) over (partition by sol.cod_suc_sucursalcuenta,sol.cod_nro_cuenta order by sol.dt_cys_fechalta desc, cod_cys_nrosolicitud desc ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING) cod_suc_sucursal
from bi_corp_staging.vmalug_ugdtmae mae
left join bi_corp_common.stk_cys_solicitudcontrato sol on cast(mae.cuenta as bigint)=sol.cod_nro_cuenta and cast(mae.oficina as int)=sol.cod_suc_sucursalcuenta and sol.cod_prod_producto = mae.producto and sol.cod_prod_subproducto=mae.subpro
where mae.partition_date='{{ ti.xcom_pull(task_ids='InputConfig', key='partition_date', dag_id='LOAD_CMN_PRESTAMOS-Daily') }}'
and mae.feforma >= sol.dt_cys_fechalta)t;


INSERT OVERWRITE TABLE bi_corp_common.stk_pre_prestamos
    PARTITION (partition_date='{{ ti.xcom_pull(task_ids='InputConfig', key='partition_date', dag_id='LOAD_CMN_PRESTAMOS-Daily') }}')
select
mae.ENTIDAD as cod_pre_entidad,
cast(mae.OFICINA as int) as cod_suc_sucursal,
cast(mae.CUENTA as bigint) as cod_nro_cuenta,
cast(pedt.penumper as int) as cod_per_nup,
mae.PRODUCTO as cod_prod_producto,
mae.SUBPRO as cod_prod_subproducto,
coalesce(p70.pecodpro, mae.PRODUCTO) as cod_pre_productoactual,
coalesce(p70.pecodsub,mae.SUBPRO)  as cod_pre_subproductoactual,
mae.DIVISA as cod_div_divisa,
cast(p42.pesucope as int) as cod_pre_sucursaloperativa,
case when trim(mae.ENTIDAD_VIN) = '' then null else trim(mae.ENTIDAD_VIN) end as cod_pre_entidadvinculada,
cast(mae.CENTRO_VIN as int) as cod_suc_sucursalvinculada,
cast(mae.CUENTA_VIN as bigint) as cod_pre_cuentavinculada,
case when trim(mae.PRODUCTO_VIN) = '' then null else trim(mae.PRODUCTO_VIN) end as cod_pre_productovinculado,
mae.UGOFIPRE as cod_pre_ugofipre,
mae.OFELEV as cod_pre_ofelev,
case when trim(mae.AGENTE)='' then null else trim(mae.AGENTE) end as cod_pre_agente,
drb.TIPOTASA as cod_pre_tipotasa,
mae.TIP_PRODUCTO as cod_pre_tipproducto,
mae.IND_CLASEPRO as cod_pre_indclaseproducto,
case when trim(mae.COD_PROCEDEN)='' then null else trim(mae.COD_PROCEDEN) end as cod_pre_proceden,
mae.CODIDES as cod_pre_destino,
case when mae.FESOLIC in ('0001-01-01','9999-12-31') then null else mae.FESOLIC end as dt_pre_fechasolicitud,
mae.SITPRES as cod_pre_situacionprestamo,
case when mae.FECSIT in ('0001-01-01','9999-12-31') then null else mae.FECSIT end as dt_pre_fechasit,
sol.cod_cys_nrosolicitud as cod_pre_solicitud, --RIO2
case when mae.FECAPROBA in ('0001-01-01','9999-12-31') then null else mae.FECAPROBA end as dt_pre_fechaaprobacion,
case when mae.FEFORMA in ('0001-01-01','9999-12-31') then null else mae.FEFORMA end as dt_pre_fechaformalizacion,
case when mae.FEC_ABONO in ('0001-01-01','9999-12-31') then null else mae.FEC_ABONO end as dt_pre_fechaabono,
case when mae.FEC_VCTO_PGR in ('0001-01-01','9999-12-31') then null else mae.FEC_VCTO_PGR end as dt_pre_fechavencprogramado,
case when mae.FECHA_PAGO in ('0001-01-01','9999-12-31') then null else mae.FECHA_PAGO end as dt_pre_fechapago,
mae.COD_CANAL as cod_pre_canalventa,
case when mae.FEPROLIQ_ORIG in ('0001-01-01','9999-12-31') then null else mae.FEPROLIQ_ORIG end as dt_pre_fechaproximaliquidacion,
case when drb.UGFULVTO in ('0001-01-01','9999-12-31') then null else drb.UGFULVTO end as dt_pre_fechaultimovencimiento,
case when mae.FEC_PASE_CAST in ('0001-01-01','9999-12-31') then null else mae.FEC_PASE_CAST end as dt_pre_fechafallido,
case when trim(mae.CODIGAR)='' then null else trim(mae.CODIGAR) end as cod_pre_garantia,
case when mae.FEULIQ in ('0001-01-01','9999-12-31') then null else mae.FEULIQ end as dt_pre_fechauliq,
case when mae.FEVENCIN_ORIG in ('0001-01-01','9999-12-31') then null else mae.FEVENCIN_ORIG end as dt_pre_fechavencimiento,
case when trim(mae.TIP_CONDALTE)='' then null else trim(mae.TIP_CONDALTE) end as cod_pre_tipocondalte,
case when trim(mae.COD_CONDALTE)='' then null else trim(mae.COD_CONDALTE) end as cod_pre_condalte,
case when trim(mae.COD_PROCEDEN)='' then null else trim(mae.COD_PROCEDEN) end as cod_pre_procedencia,
mae.SITDEUOB as cod_pre_situaciondeudaobjetiva,
mae.IND_COBRIMP as cod_pre_modalidadprestamo,
case when trim(mae.cod_motivcan)='' then null else trim(mae.cod_motivcan) end as cod_pre_motivobajacuenta, --RIO87
wt.wtri0010_cod_mot_baja_mora as cod_pre_motivobajamora, --RIO87
mae.feforma as dt_pre_fechaaltacuenta, --RIO87
case when mae.sitpres in ('0','2') then null else mae.fecsit end as dt_pre_fechabajacuenta, --RIO87
wt.wtri0010_fec_baja_mora as dt_pre_fechabajamora, --RIO87
case when ptcosq.ptcosq_fec_primer_imp in ('0001-01-01','9999-12-31') then null else ptcosq.ptcosq_fec_primer_imp end as dt_pre_fechainicioimpago, --RIO87
case when mae.UGFPAMOR in ('0001-01-01','9999-12-31') then null else mae.UGFPAMOR end as dt_pre_fechainiciomora,
case when u.fecha in ('0001-01-01','9999-12-31') then null else u.fecha end as dt_pre_fechaultimopago,
u.monto as fc_pre_importeultimopago,
r.rec_feliq as dt_pre_fechavencimientocuota, --RIO87
SUBSTRING(mae.PLAZO, 1, 1) as cod_pre_tipoplazo,  --RIO87
trim(bgdt.producto_contab) as cod_pre_paquete,
trim(bgdt.subprodu_contab) as cod_pre_tipopaquete,
case when mae.FECPRILIQ in ('0001-01-01','9999-12-31') then null else mae.FECPRILIQ end as dt_pre_fechaprimeracuota,
cast(drb.ugiquota_ini as decimal(15,2)) as fc_pre_importeprimeracuota,
sol.cod_suc_sucursal as cod_suc_sucursalsolicitud, --RIO2
contratos_di.waguxdex_cod_marca as cod_pre_marcagarra,
contratos_di.waguxdex_cod_smarcgsi as cod_pre_submarcagarra,
contratos_di.waguxdex_con_diaimpag as int_pre_diasatrasogarra,
cast(translate(drb.UGCPLINI,'.','') as int) as int_pre_plazoinicial,
cast(translate(drb.UGNPLINI,'.','') as int) as int_pre_plazoreal,
cast(translate(drb.UGCPLAZO,'.','') as int) as cod_pre_plazo,
cast(translate(drb.UGCDIPAG_INI,'.','') as int) as int_pre_diapago,
cast(translate(drb.UGCDIPAG_ACT,'.','') as int) as int_pre_diapagoactual,
cast(translate(drb.UGNPLVEN,'.','') as int) as int_pre_numcuota,
gar.cod_auto_marca_prestamo as cod_pre_automarcaprestamo,--prendarios
trim(gar.auto_marca_prestamo) as ds_pre_automarcaprestamo,--prendarios
gar.cod_auto_modelo_prestamo as cod_pre_automodeloprestamo,--prendarios
trim(gar.auto_modelo_prestamo) as ds_pre_automodeloprestamo,--prendarios
gar.anio_auto_prestamo as int_pre_anioautoprestamo,--prendarios
drb.DIASPERIODO as cod_pre_diasperiodo,
case when trim(mae.agente)='' then null else trim(mae.agente) end as ds_pre_prescriptor, -- prendarios
case when mae.IND_REESTRUC='S' then 1 else 0 end as flag_pre_reestructuracion,
case when mae.UGYPPAUT='S' then 1 else 0 end as flag_pre_prestamoindexado,
aux.coefici_index_act as fc_pre_coeficienteindexactualizacion,
mae.NUM_PROPUES as cod_pre_numeropropuesta,
cast(case when (mae.NUM_PROPUES like 'COV%' or mae.NUM_PROPUES like 'VD%' or mae.NUM_PROPUES like 'ID%' or mae.NUM_PROPUES like 'GD%') then SUBSTRING(mae.NUM_PROPUES, 4, 3) else null end as int) as cod_suc_sucursaloriginante,
cast(case when (mae.NUM_PROPUES like 'COV%' or mae.NUM_PROPUES like 'VD%' or mae.NUM_PROPUES like 'ID%' or mae.NUM_PROPUES like 'GD%') then SUBSTRING(mae.NUM_PROPUES, 7, 11) else null end as bigint) as cod_pre_cuentaoriginante,
cast(case when (mae.NUM_PROPUES like 'COV%' or mae.NUM_PROPUES like 'VD%' or mae.NUM_PROPUES like 'ID%' or mae.NUM_PROPUES like 'GD%') then SUBSTRING(mae.NUM_PROPUES, 18, 3) else null end as int) as cod_pre_nrocuotaimpagaprestamooriginante,
case when (mae.NUM_PROPUES like 'COV%' or mae.NUM_PROPUES like 'VD%' or mae.NUM_PROPUES like 'ID%' or mae.NUM_PROPUES like 'GD%') then 1 else 0 end as flag_pre_marcacovid,
drb.ugtfmini as cod_pre_tipoamortizacion,
cast(mae.TAE as decimal(15,6)) as fc_pre_tasaanualequivalente,
cast(drb.UGPININI as decimal(15,2)) as fc_pre_tasainteresinicial,
cast(drb.UGPINACT as decimal(15,2)) as fc_pre_tasainteresactual,
case when (ptcosq.ptcosq_fec_primer_imp is not null and ptcosq.ptcosq_fec_primer_imp not in ('0001-01-01','9999-12-31')) then (datediff(to_date('{{ ti.xcom_pull(task_ids='InputConfig', key='partition_date', dag_id='LOAD_CMN_PRESTAMOS-Daily') }}'), to_date(ptcosq.ptcosq_fec_primer_imp)) / 30) + 1 else 0 end as fc_pre_cantidadciclosimpagos, --RIO87
case when TRIM(mae.PRODUCTO)='74' then 0 else datediff(to_date(mae.fecpriamo), to_date(mae.feforma)) end as fc_pre_cantidaddiasdegracia, --RIO87
case when (ptcosq.ptcosq_fec_primer_imp is not null and ptcosq.ptcosq_fec_primer_imp not in ('0001-01-01','9999-12-31')) then datediff(to_date('{{ ti.xcom_pull(task_ids='InputConfig', key='partition_date', dag_id='LOAD_CMN_PRESTAMOS-Daily') }}'), to_date(ptcosq.ptcosq_fec_primer_imp)) + 1 else 0 end as fc_pre_cantidaddiasimpagos, --RIO87
cast(translate(drb.UGNPLPEN,'.','') as int) as fc_pre_cantidadcuotaspendientes,
cast(mae.CAPSOLI as decimal(15,2)) as fc_pre_capitalsolicitado,
cast(drb.FACTOR_MUTUO as decimal(15,2)) as fc_pre_lineacreditototal,
cast(mae.IMPCONCE as decimal(15,2)) as fc_pre_capitalconcedido,
cast(mae.SALTEOR as decimal(15,2)) as fc_pre_saldocapitalpendfactura,
cast(drb.UGIDPINI as decimal(15,2)) as fc_pre_importeinicialtomado,
cast(drb.UGIQUINT as decimal(15,2)) as fc_pre_interescompensatorio,
cast(drb.UGIQUOTA as decimal(15,2)) as fc_pre_montocuota,
cast(mae.SALREAL as decimal(15,2)) as fc_pre_saldoreal,
case when rpe.rpe_codconli is not null then rpe.rpe_imp_periodifi else ptcosq.ptcosq_interes_vencido end as fc_pre_interesdevengado, --RIO87
r.rec_capinire as fc_pre_montocuotacapital, --RIO87
r.rec_intinire as fc_pre_montocuotainteres, --RIO87
r.mon_cuo_otr as fc_pre_montocuotaotros, --RIO87
r.rec_seginire as fc_pre_montocuotaseguros, --RIO87
r.rec_capinire + r.rec_intinire + r.mon_cuo_otr + r.rec_seginire as fc_pre_montocuotatotal, --RIO87
0 as fc_pre_montodeudacapital, --RIO87
0 as fc_pre_montodeudaexigible, --RIO87
cast(mae.ACUIMPA as decimal(15,2)) as fc_pre_monto_impago,
adsf.isec4060_imp_prevision_cto as fc_pre_montoprovisioncuenta, --RIO87
cast(mae.SALREAL as decimal(15,2)) - cast(mae.SALTEOR as decimal(15,2)) as fc_pre_saldocapitalvencidoimpago, --RIO87
wt.wtri0010_sdo_origen_mora as fc_pre_saldoorigenmora, --RIO87
wt.wtri0010_sdo_capital as fc_pre_saldocapital, --RIO87
cast(mae.SALINTCAP as decimal(15,2)) as fc_pre_saldointeres,
wt.wtri0010_sdo_impuestos as fc_pre_saldoimpuestos, --RIO87
wt.wtri0010_sdo_ajustes as fc_pre_saldoajuste, --RIO87
wt.wtri0010_sdo_seguros as fc_pre_saldoseguros, --RIO87
wt.wtri0010_sdo_comisiones as fc_pre_saldocomisiones, --RIO87
wt.wtri0010_mon_pgo_capital as fc_pre_montopagocapital, --RIO87
wt.wtri0010_mon_pgo_otr_ccptos as fc_pre_montopagootrosconceptos, --RIO87
wt.wtri0010_sdo_tot_mora as fc_pre_saldototalmora, --RIO87
wt.wtri0010_mon_pgo_tot_mora as fc_pre_montopagototalmora, --RIO87
contratos_di.waguxdex_imp_riesmolo as fc_pre_importeriesgo,
contratos_di.waguxdex_imp_irremolo as fc_pre_importeirregular,
cast(drb.UGICMBAS as decimal(15,2)) as fc_pre_lineadisponible,
cast(mae.IMPDISPU as decimal(15,2)) as fc_pre_importedispuesto,
cast(drb.NUM_DIVIDENDO as decimal(15,2)) as fc_pre_numdividendo,
case when drb.UGFPPAGO in ('0001-01-01','9999-12-31') then null else drb.UGFPPAGO end as dt_pre_fechaprimerpago,
cast(ptmosq_MAE_OFTIT as int) as cod_pre_oftit,
trim(ptmosq_PTM1_CODTIAMO) as cod_pre_tiamo,
substr(ptmosq_MAE_PLAZO, 2, 3) as cod_pre_plzcontractual,
trim(ptmosq_PTM1_PERINT) as cod_pre_perint,
trim(ptmosq_PTM1_PERCAP) as cod_pre_percap,
case when trim(ptmosq_PTM1_IND_TASA_COMB) ='' then null  when trim(ptmosq_PTM1_IND_TASA_COMB) ='N' then 0 else 1 end as flag_pre_tasacomb,
case when trim(ptmosq_PTM1_IND_PERFIJO_TASA_COMB) ='' then null  when trim(ptmosq_PTM1_IND_PERFIJO_TASA_COMB) ='N' then 0 else 1 end as flag_pre_perfijotasacomb,
case when trim(ptmosq_PTM1_IND_TIPOAPLI)='V' then 1 else 0 end as flag_pre_tipoapli,
cast(ptmosq_PTM1_TIPOAPLI_HOY as decimal(15,2)) as fc_pre_tipoaplihoy,
trim(ptmosq_MAE_SITDEUCT) as cod_pre_maesitdeuct,
case when ptmosq_MAE_FECSITOB in ('0001-01-01','9999-12-31') then null else ptmosq_MAE_FECSITOB end as dt_pre_fechasitob,
case when ptmosq_MAE_FEVENCIN in ('0001-01-01','9999-12-31') then null else ptmosq_MAE_FEVENCIN end as dt_pre_fechavencin
from bi_corp_staging.vmalug_ugdtmae mae
left join bi_corp_staging.vmalug_ugdtdrb drb on drb.entidad=mae.entidad and drb.cuenta=mae.cuenta and drb.oficina=mae.oficina and drb.partition_date='{{ ti.xcom_pull(task_ids='InputConfig', key='partition_date', dag_id='LOAD_CMN_PRESTAMOS-Daily') }}'
left join bi_corp_staging.vmalug_ugdtaux aux on mae.entidad = aux.entidad and mae.oficina = aux.oficina and mae.cuenta = aux.cuenta and aux.partition_date = '{{ ti.xcom_pull(task_ids='InputConfig', key='partition_date', dag_id='LOAD_CMN_PRESTAMOS-Daily') }}'
left join pedt008 pedt on  mae.oficina =pedt.pecodofi and mae.cuenta = pedt.penumcon and mae.producto = pedt.pecodpro and mae.subpro = pedt.pecodsub
left join bi_corp_staging.garra_wagucdex contratos_di on contratos_di.waguxdex_cod_centro=mae.oficina and contratos_di.waguxdex_num_contrato=mae.cuenta and contratos_di.partition_date='{{ ti.xcom_pull(task_ids='InputConfig', key='partition_date', dag_id='LOAD_CMN_PRESTAMOS-Daily') }}'
left join pedt70 p70 on p70.pecodofi = mae.oficina and p70.penumcon = mae.cuenta
left join bi_corp_staging.malpe_ptb_pedt042 p42 on mae.oficina =p42.pecodofi and mae.cuenta = p42.penumcon and mae.producto = p42.pecodpro and mae.subpro = p42.pecodsub and p42.pecodpro in ('35','36','37','38','39','71','74','70') and p42.partition_date='{{ ti.xcom_pull(task_ids='InputConfig', key='partition_date', dag_id='LOAD_CMN_PRESTAMOS-Daily') }}'
left join ultima_cuota u on u.entidad = mae.entidad and u.oficina = mae.oficina and u.cuenta = mae.cuenta
left join bi_corp_staging.bgdtmae bgdt on bgdt.centro_alta=mae.centro_vin and bgdt.cuenta=mae.cuenta_vin and bgdt.divisa='ARS' and bgdt.partition_date ='{{ ti.xcom_pull(task_ids='InputConfig', key='partition_date', dag_id='LOAD_CMN_PRESTAMOS-Daily') }}'
left join ultimo_recibo r on mae.entidad = r.rec_entidad and mae.oficina = r.rec_oficina and mae.cuenta = r.rec_cuenta
left join bi_corp_staging.malug_ptcosq ptcosq on mae.entidad = ptcosq.ptcosq_entidad and mae.oficina = ptcosq.ptcosq_oficina and mae.cuenta = ptcosq.ptcosq_cuenta and ptcosq.partition_date ='{{ ti.xcom_pull(task_ids='InputConfig', key='partition_date', dag_id='LOAD_CMN_PRESTAMOS-Daily') }}'
left join bi_corp_staging.malug_rpe rpe on mae.entidad = rpe.rpe_entidad and mae.oficina = rpe.rpe_oficina and mae.cuenta = rpe.rpe_cuenta and rpe.rpe_codconli = '101' and rpe.partition_date ='{{ ti.xcom_pull(task_ids='InputConfig', key='max_partition_malug_rpe', dag_id='LOAD_CMN_PRESTAMOS-Daily') }}'
left join garantia gar on gar.entidad=mae.entidad and gar.cuenta=mae.cuenta and gar.oficina=mae.oficina
left join bi_corp_staging.moria_wtri0010 wt on wt.wtri0010_cod_suc=cast(mae.OFICINA as int) and wt.wtri0010_nro_cta=cast(mae.CUENTA as bigint) and wt.wtri0010_cod_prod=mae.PRODUCTO and wt.wtri0010_cod_subpr=mae.SUBPRO and wt.wtri0010_cod_mone=mae.DIVISA and wt.partition_date='{{ ti.xcom_pull(task_ids='InputConfig', key='max_partition_moria_wtri0010', dag_id='LOAD_CMN_PRESTAMOS-Daily') }}'
left join bi_corp_staging.adsf_isec4060 adsf on adsf.isec4060_centro=mae.OFICINA and adsf.isec4060_cuenta=mae.CUENTA and adsf.isec4060_producto=mae.PRODUCTO and adsf.isec4060_subproducto =mae.SUBPRO and adsf.isec4060_divisa=mae.DIVISA and adsf.partition_date='{{ ti.xcom_pull(task_ids='InputConfig', key='max_partition_adsf_isec4060', dag_id='LOAD_CMN_PRESTAMOS-Daily') }}'
left join bi_corp_staging.malug_ptmosq ptmosq on mae.cuenta=ptmosq.ptmosq_mae_cuenta and mae.oficina=ptmosq.ptmosq_mae_oficina and mae.entidad=ptmosq.ptmosq_mae_entidad and ptmosq.partition_date='{{ ti.xcom_pull(task_ids='InputConfig', key='partition_date', dag_id='LOAD_CMN_PRESTAMOS-Daily') }}'
left join solicitudes sol on sol.cod_suc_sucursalcuenta=cast(mae.oficina as int) and sol.cod_nro_cuenta=cast(mae.cuenta as bigint)
where mae.partition_date = '{{ ti.xcom_pull(task_ids='InputConfig', key='partition_date', dag_id='LOAD_CMN_PRESTAMOS-Daily') }}'
and contratos_di.waguxdex_cod_marca <> 'FA'
and contratos_di.waguxdex_cod_marca is not null
and mae.sitpres in ('0','3','P');