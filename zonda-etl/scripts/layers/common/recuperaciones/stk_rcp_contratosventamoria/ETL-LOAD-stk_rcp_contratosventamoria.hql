SET hive.merge.mapfiles=true;
SET hive.merge.mapredfiles=true;
SET hive.merge.size.per.task=4000000;
SET hive.merge.smallfiles.avgsize=16000000;
SET hive.exec.dynamic.partition.mode=nonstrict;
SET mapred.job.queue.name=root.dataeng;

INSERT OVERWRITE TABLE bi_corp_common.stk_rcp_contratosventamoria 
PARTITION(partition_date='{{ ti.xcom_pull(task_ids='InputConfig', key='partition_date', dag_id='LOAD_CMN_RECUPERACIONES-Daily') }}')

SELECT 
	CAST(idventa AS INT) idventa ,
	CAST(cod_centro AS INT) cod_centro ,
	num_contrato ,
	CAST(cod_producto AS INT) cod_producto ,
	TRIM(cod_subprodu) cod_subprodu ,
	TRIM(cod_divisa) cod_divisa ,
	CAST(penumper AS BIGINT) penumper ,
	TRIM(petipdoc) petipdoc ,
	CAST(penumdoc AS BIGINT) penumdoc ,
	IF(TRIM(penomfan) = '', NULL, TRIM(penomfan)) penomfan ,
	TRIM(pepriape) pepriape ,
	TRIM(penomper) penomper ,
	TRIM(petipdoc_i) petipdoc_i ,
	CAST(penumdoc_i AS BIGINT) penumdoc_i ,
	TRIM(pepriape_i) pepriape_i ,
	TRIM(penomper_i) penomper_i ,
	CAST(tot_capital AS INT) tot_capital ,
	CAST(tot_interes AS INT) tot_interes ,
	CAST(tot_comision AS INT) tot_comision ,
	CAST(tot_gastos AS INT) tot_gastos ,
	CAST(tot_seguros AS INT) tot_seguros ,
	CAST(tot_impuesto AS INT) tot_impuesto ,
	CAST(tot_ajuste AS INT) tot_ajuste ,
	CAST(saldo AS INT) saldo ,
	CAST(saldo_informado_i AS INT) saldo_informado_i ,
	CAST(imp_recuperar AS INT) imp_recuperar ,
	CAST(imp_totcanco AS INT) imp_totcanco ,
	CAST(imp_cancgasc AS INT) imp_cancgasc ,
	CAST(total_contable AS INT) total_contable ,
	CAST(total_no_contable AS INT) total_no_contable ,
	CAST(total_impuperc AS INT) total_impuperc ,
	CAST(cant_pagos AS INT) cant_pagos ,
	TRIM(cod_estado) cod_estado ,
	IF(TRIM(fec_castigo) IN ('9999-12-31','0001-01-01'), NULL, TRIM(fec_castigo) ) fec_castigo , 
	IF(TRIM(cod_origcast) = '', NULL, TRIM(cod_origcast)) cod_origcast ,
	IF(TRIM(fec_abseguim) IN ('9999-12-31','0001-01-01'), NULL, fec_abseguim ) fec_abseguim , 
	IF(TRIM(fec_abtotal) IN ('9999-12-31','0001-01-01'), NULL, fec_abtotal ) fec_abtotal , 
	IF(TRIM(fec_baja) IN ('9999-12-31','0001-01-01'), NULL, fec_baja ) fec_baja , 
	TRIM(cod_motbajac) cod_motbajac ,
	IF(TRIM(fec_inisitir) IN ('9999-12-31','0001-01-01'), NULL, fec_inisitir ) fec_inisitir ,
	IF(TRIM(cod_coefinde) = '', NULL, TRIM(cod_coefinde)) cod_coefinde ,
	IF(TRIM(cod_refinanc) = '', NULL, TRIM(cod_refinanc)) cod_refinanc ,
	IF(TRIM(ind_conversi) = 'S', 1, 0) ind_conversi ,
	TRIM(cod_tipadmin) cod_tipadmin ,
	IF(TRIM(cod_proceden) = '', NULL, TRIM(cod_proceden)) cod_proceden ,
	TRIM(nombre_eejj_i) nombre_eejj_i ,
	fecha_alta ,
	IF(TRIM(userid_alta) = '', NULL, TRIM(userid_alta)) userid_alta ,
	IF(TRIM(userid_umo) = '', NULL, TRIM(userid_umo)) userid_umo ,
	from_unixtime(unix_timestamp(timest_umo ,'dd-MM-yyyy HH:mm:ss'),'yyyy-MM-dd  HH:mm:ss') timest_umo ,
	TRIM(prod_emerix) prod_emerix ,
	TRIM(tip_cartera) tip_cartera ,
	TRIM(escenario) escenario ,
	CAST(nup_eejj AS INT) nup_eejj ,
	TRIM(segmento_comer) segmento_comer ,
	IF(TRIM(ind_rechazo) = 'S', 1, 0) ind_rechazo ,
	TRIM(cod_rechazo) cod_rechazo ,
	IF(TRIM(motivo_rechazo) = '', NULL, TRIM(motivo_rechazo)) motivo_rechazo ,
	partition_date
FROM bi_corp_staging.moria_vc_historico_ventas
WHERE partition_date = '{{ ti.xcom_pull(task_ids='InputConfig', key='partition_date', dag_id='LOAD_CMN_RECUPERACIONES-Daily') }}' ;