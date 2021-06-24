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
from
(select pecodofi, penumcon,
first_value(pecodpro) over(partition by pecodofi, penumcon order by pefecalt desc ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING) pecodpro,
first_value(pecodsub) over(partition by pecodofi, penumcon order by pefecalt desc ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING) pecodsub
from bi_corp_staging.malpe_ptb_pedt008
where partition_date = '{{ ti.xcom_pull(task_ids='InputConfig', key='max_partition_malpe_ptb_pedt008', dag_id='LOAD_CMN_PRESTAMOS-Daily') }}'
AND pecodpro IN ('35', '36', '37', '38', '39', '71', '74','70')
AND pecalpar = 'TI') t
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
and rec_indpeco ='0'
and mvr.mvr_indretro='N') t;


DROP TABLE IF EXISTS cancelaciones_fecha_valor;
CREATE TEMPORARY TABLE cancelaciones_fecha_valor as
select mov_cuenta, mov_oficina, mov_entidad, mov_feoper
from bi_corp_staging.malug_ugdtmov
where partition_date='{{ ti.xcom_pull(task_ids='InputConfig', key='partition_date', dag_id='LOAD_CMN_PRESTAMOS-Daily') }}'
and trim(mov_nom_campo)='SITPRES'
and trim(mov_valor_ant)='0'
and trim(mov_valor_nue)='1'
and mov_indretro='N';


INSERT OVERWRITE TABLE bi_corp_common.bt_pre_bajas
    PARTITION (partition_date='{{ ti.xcom_pull(task_ids='InputConfig', key='partition_date', dag_id='LOAD_CMN_PRESTAMOS-Daily') }}')
select
mae.ENTIDAD as cod_pre_entidad,
cast(mae.OFICINA as int) as cod_suc_sucursal,
cast(mae.CUENTA as bigint) as cod_nro_cuenta,
cast(pedt.penumper as int) as cod_per_nup,
mae.PRODUCTO as cod_prod_producto,
mae.SUBPRO as cod_prod_subproducto,
coalesce(p70.pecodpro, mae.PRODUCTO) as cod_pre_productocierre,
coalesce(p70.pecodsub,mae.SUBPRO)  as cod_pre_subproductocierre,
mae.DIVISA as cod_div_divisa,
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
case when mae.FECAPROBA in ('0001-01-01','9999-12-31') then null else mae.FECAPROBA end as dt_pre_fechaaprobacion,
case when mae.FEFORMA in ('0001-01-01','9999-12-31') then null else mae.FEFORMA end as dt_pre_fechaformalizacion,
case when mae.FEC_ABONO in ('0001-01-01','9999-12-31') then null else mae.FEC_ABONO end as dt_pre_fechaabono,
case when mae.FEC_VCTO_PGR in ('0001-01-01','9999-12-31') then null else mae.FEC_VCTO_PGR end as dt_pre_fechavencprogramado,
case when mae.FECHA_PAGO in ('0001-01-01','9999-12-31') then null else mae.FECHA_PAGO end as dt_pre_fechapago,
mae.COD_CANAL as cod_pre_canalventa,
case when mae.FEPROLIQ_ORIG in ('0001-01-01','9999-12-31') then null else mae.FEPROLIQ_ORIG end as dt_pre_fechaproximaliquidacion,
case when drb.UGFULVTO in ('0001-01-01','9999-12-31') then null else drb.UGFULVTO end as dt_pre_fechaultimovencimiento,
case when trim(mae.CODIGAR)='' then null else trim(mae.CODIGAR) end as cod_pre_garantia,
case when mae.FEULIQ in ('0001-01-01','9999-12-31') then null else mae.FEULIQ end as dt_pre_fechauliq,
case when mae.FEVENCIN_ORIG in ('0001-01-01','9999-12-31') then null else mae.FEVENCIN_ORIG end as dt_pre_fechavencimiento,
case when trim(mae.TIP_CONDALTE)='' then null else trim(mae.TIP_CONDALTE) end as cod_pre_tipocondalte,
case when trim(mae.COD_CONDALTE)='' then null else trim(mae.COD_CONDALTE) end as cod_pre_condalte,
case when trim(mae.COD_PROCEDEN)='' then null else trim(mae.COD_PROCEDEN) end as cod_pre_procedencia,
mae.SITDEUOB as cod_pre_situaciondeudaobjetiva,
mae.IND_COBRIMP as cod_pre_modalidadprestamo,
case when trim(mae.cod_motivcan)='' then null else trim(mae.cod_motivcan) end as cod_pre_motivobajacuenta,
mae.feforma as dt_pre_fechaaltacuenta,
case when mae.sitpres in ('0','2') then null else mae.fecsit end as dt_pre_fechabajacuenta,
case when u.fecha in ('0001-01-01','9999-12-31') then null else u.fecha end as dt_pre_fechaultimopago,
u.monto as fc_pre_importeultimopago,
SUBSTRING(mae.PLAZO, 1, 1) as cod_pre_tipoplazo,
trim(bgdt.producto_contab) as cod_pre_paquete,
trim(bgdt.subprodu_contab) as cod_pre_tipopaquete,
case when mae.FECPRILIQ in ('0001-01-01','9999-12-31') then null else mae.FECPRILIQ end as dt_pre_fechaprimeracuota,
cast(drb.ugiquota_ini as decimal(15,2)) as fc_pre_importeprimeracuota,
cast(translate(drb.UGCPLINI,'.','') as int) as int_pre_plazoinicial,
cast(translate(drb.UGNPLINI,'.','') as int) as int_pre_plazoreal,
cast(translate(drb.UGCPLAZO,'.','') as int) as cod_pre_plazo,
cast(translate(drb.UGCDIPAG_INI,'.','') as int) as int_pre_diapago,
cast(translate(drb.UGCDIPAG_ACT,'.','') as int) as int_pre_diapagoactual,
cast(translate(drb.UGNPLVEN,'.','') as int) as int_pre_numcuota,
drb.DIASPERIODO as cod_pre_diasperiodo,
case when trim(mae.agente)='' then null else trim(mae.agente) end as ds_pre_prescriptor,
case when mae.IND_REESTRUC='S' then 1 else 0 end as flag_pre_reestructuracion,
case when mae.UGYPPAUT='S' then 1 else 0 end as flag_pre_prestamoindexado,
aux.coefici_index_act as fc_pre_coeficienteindexactualizacion,
mae.NUM_PROPUES as cod_pre_numeropropuesta,
cast(case when mae.NUM_PROPUES like 'COV%' then SUBSTRING(mae.NUM_PROPUES, 4, 3) else null end as int) as cod_suc_sucursaloriginante,
cast(case when mae.NUM_PROPUES like 'COV%' then SUBSTRING(mae.NUM_PROPUES, 7, 11) else null end as bigint) as cod_pre_cuentaoriginante,
cast(case when mae.NUM_PROPUES like 'COV%' then SUBSTRING(mae.NUM_PROPUES, 18, 3) else null end as int) as cod_pre_nrocuotaimpagaprestamooriginante,
case when mae.NUM_PROPUES like 'COV%' then 1 else 0 end as flag_pre_marcacovid,
drb.ugtfmini as cod_pre_tipoamortizacion,
cast(mae.TAE as decimal(15,6)) as fc_pre_tasaanualequivalente,
cast(drb.UGPININI as decimal(15,2)) as fc_pre_tasainteresinicial,
cast(drb.UGPINACT as decimal(15,2)) as fc_pre_tasainteresactual,
cast(translate(drb.UGNPLPEN,'.','') as int) as fc_pre_cantidadcuotaspendientes,
cast(mae.CAPSOLI as decimal(15,2)) as fc_pre_capitalsolicitado,
cast(drb.FACTOR_MUTUO as decimal(15,2)) as fc_pre_lineacreditototal,
cast(mae.IMPCONCE as decimal(15,2)) as fc_pre_capitalconcedido,
cast(mae.SALTEOR as decimal(15,2)) as fc_pre_saldocapitalpendfactura,
cast(drb.UGIDPINI as decimal(15,2)) as fc_pre_importeinicialtomado,
cast(drb.UGIQUINT as decimal(15,2)) as fc_pre_interescompensatorio,
cast(drb.UGIQUOTA as decimal(15,2)) as fc_pre_montocuota,
cast(mae.SALREAL as decimal(15,2)) as fc_pre_saldoreal,
cast(mae.ACUIMPA as decimal(15,2)) as fc_pre_monto_impago,
cast(mae.SALREAL as decimal(15,2)) - cast(mae.SALTEOR as decimal(15,2)) as fc_pre_saldocapitalvencidoimpago,
cast(mae.SALINTCAP as decimal(15,2)) as fc_pre_saldointeres,
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
left join pedt70 p70 on p70.pecodofi = mae.oficina and p70.penumcon = mae.cuenta
left join ultima_cuota u on u.entidad = mae.entidad and u.oficina = mae.oficina and u.cuenta = mae.cuenta
left join bi_corp_staging.bgdtmae bgdt on bgdt.centro_alta=mae.centro_vin and bgdt.cuenta=mae.cuenta_vin and bgdt.divisa='ARS' and bgdt.partition_date ='{{ ti.xcom_pull(task_ids='InputConfig', key='partition_date', dag_id='LOAD_CMN_PRESTAMOS-Daily') }}'
left join cancelaciones_fecha_valor c on mae.entidad = c.mov_entidad and mae.oficina = c.mov_oficina and mae.cuenta = c.mov_cuenta
left join bi_corp_staging.malug_ptmosq ptmosq on mae.cuenta=ptmosq.ptmosq_mae_cuenta and mae.oficina=ptmosq.ptmosq_mae_oficina and mae.entidad=ptmosq.ptmosq_mae_entidad and ptmosq.partition_date='{{ ti.xcom_pull(task_ids='InputConfig', key='partition_date', dag_id='LOAD_CMN_PRESTAMOS-Daily') }}'
where mae.partition_date = '{{ ti.xcom_pull(task_ids='InputConfig', key='partition_date', dag_id='LOAD_CMN_PRESTAMOS-Daily') }}'
and mae.sitpres not in ('7','8','0','3','P')
and ((c.mov_cuenta is null and mae.fecsit > '{{ ti.xcom_pull(task_ids='InputConfig', key='previous_working_day', dag_id='LOAD_CMN_PRESTAMOS-Daily') }}') or
(c.mov_cuenta is not null and mae.fecsit <> '{{ ti.xcom_pull(task_ids='InputConfig', key='partition_date', dag_id='LOAD_CMN_PRESTAMOS-Daily') }}' and c.mov_feoper > '{{ ti.xcom_pull(task_ids='InputConfig', key='previous_working_day', dag_id='LOAD_CMN_PRESTAMOS-Daily') }}') or
(c.mov_cuenta is not null and mae.fecsit > '{{ ti.xcom_pull(task_ids='InputConfig', key='previous_working_day', dag_id='LOAD_CMN_PRESTAMOS-Daily') }}'));
