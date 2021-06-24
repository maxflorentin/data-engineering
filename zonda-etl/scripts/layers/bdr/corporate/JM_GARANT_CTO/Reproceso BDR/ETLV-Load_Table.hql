set hive.merge.mapfiles=true;
set hive.merge.mapredfiles=true;
set hive.merge.size.per.task=4000000;
set hive.merge.smallfiles.avgsize=16000000;
set hive.exec.dynamic.partition.mode=nonstrict;
set mapred.job.queue.name=root.dataeng;

INSERT OVERWRITE TABLE bi_corp_bdr.jm_garant_cto
PARTITION (partition_date='{{ ti.xcom_pull(task_ids='InputConfig', key='month_partition_bdr', dag_id='BDR_LOAD_Tables-Reproceso_BDR_Monthly_201912_202103') }}')
SELECT
  (concat(substring(gc.partition_date, 1, 8), cast(cal.num_dias_mes as STRING)) ) as G4124_FEOPERAC
, LPAD(NVL(cod_entidad_bdr,'00000'),5,'0') 			as G4124_S1EMP
, LPAD(NVL(id_cto_bdr, '0'),9,'0')		as G4124_CONTRA1
, LPAD(NVL(tip_gara_bdr,'99999'),5,'0') as G4124_TIP_GARA
,  lpad(nvl(
    CASE
	WHEN tip_gara_bdr = '001'
		THEN lpad(cast(garantias.num_persona_garante as string),9,'0')
	ELSE lpad(cast(gc.num_garantia as string),9,'0')
    END
           ,'0'),9,'0') as G4124_BIENGAR1
,	CASE
		WHEN tip_cobertur = '001' THEN LPAD(NVL(fec_alta,'0001-01-01'),10,'0')	-- especificas
		WHEN tip_cobertur = '002' THEN partition_date 							-- genericas
	END as G4124_FECINI
,	CASE
		WHEN tip_cobertur = '001' THEN LPAD(NVL(fec_bajrelac,'9999-12-31'),10,'9')	-- especificas
		WHEN tip_cobertur = '002' THEN '9999-12-31' 							-- genericas
	END as G4124_FECBAJA
,case
   when fec_vcto >= '{{ ti.xcom_pull(task_ids='InputConfig', key='last_working_day', dag_id='BDR_LOAD_Tables-Reproceso_BDR_Monthly_201912_202103') }}' then
     LPAD(NVL(fec_vcto,'9999-12-31'),10,'9')
   ELSE
     '9999-12-31'
 end G4124_FECVCTO
, LPAD(nvl(cod_garantia_bdr,'0'),5,'0') as G4124_COD_GAR
, LPAD(cod_garantia,5,'0') as G4124_COD_GAR2
, LPAD(tip_instrumentacion,5,'0') as G4124_TIPO_INS
, LPAD(NVL(TRIM(substr(pignoracion,1,1)),'N'),1,' ') as G4124_IND_PIGN
, LPAD(tip_aval,5,'0') as G4124_TIP_AVAL
, LPAD(tip_cobertur,5,'0') as G4124_TIP_COBE
, '00001' as G4124_EST_GARA
, lpad(regexp_replace(format_number(coalesce (cast(gc.porc_cobertura_actual as double), 0), 6),"\\,|\\.",""),9,"0")  as G4124_PORCOBER
, lpad(regexp_replace(format_number(coalesce (cast(gc.porc_cobertura_inicial as double), 0), 6),"\\,|\\.",""),9,"0")  as G4124_COB_INIC
, lpad(regexp_replace(format_number(coalesce (cast(nominal_cobertura_inicial as double), 0), 2),"\\,|\\.",""),17,"0")  as G4124_IMPT_NOM
, lpad(regexp_replace(format_number(coalesce (cast(nominal_cobertura_actual as double), 0), 2),"\\,|\\.",""),17,"0")  as G4124_IMP_ACTU
, '99999' as G4124_COMF_LET
, (concat(substring(gc.partition_date, 1, 8), cast(cal.num_dias_mes as STRING)) ) as G4124_FECULTMO
, '00000' as G4124_NUM_ASEG
, NVL(SUBSTR(TRIM(gar_cod_divisa),1,3),'999') as G4124_CODDIV
, LPAD(	num_persona, 9,'0') as G4124_IDNUMCLI
, 'N' as G4124_INDBLO
, 'N' as G4124_INDRGOSB
, 'N' as G4124_INDCOBPF
, 'S' as G4124_VALASEJU
, lpad(regexp_replace(format_number(coalesce (cast(gc.porc_reparto as double), 0), 6),"\\,|\\.",""),9,"0")  as G4124_REPAPORC
, LPAD(CAST(orden_prelacion as STRING),5,'0') as G4124_ORDAPGAR
, CASE WHEN cod_garantia in ('076','078') THEN 'S' ELSE 'N' END as G4124_RANGOHIP
, '00000' as G4124_N_IMPAGO
FROM bi_corp_bdr.aux_garant_garantias_contratos_div gc
    LEFT JOIN santander_business_risk_arda.calendario cal
        ON ( cal.data_date_part = TRANSLATE(gc.partition_date,'-','') AND cal.fec_yyyymmdd = translate(gc.partition_date,'-','') )
    LEFT OUTER JOIN
    (
        SELECT DISTINCT * 
        FROM (
            	select num_garantia , num_persona_garante 
				from bi_corp_bdr.aux_garant_garantias_especificas_div 
				where partition_date = '{{ ti.xcom_pull(task_ids='InputConfig', key='last_working_day', dag_id='BDR_LOAD_Tables-Reproceso_BDR_Monthly_201912_202103') }}'
            UNION ALL
            	select num_garantia , num_persona_garante 
				from bi_corp_bdr.aux_garant_garantias_genericas 
				where partition_date = '{{ ti.xcom_pull(task_ids='InputConfig', key='last_working_day', dag_id='BDR_LOAD_Tables-Reproceso_BDR_Monthly_201912_202103') }}'
            UNION ALL
    			select distinct ngm.id_cto_bdr as num_garantia, gmg.num_persona_garante   
                from 
                    (
                        select * 
                        from bi_corp_bdr.normalizacion_id_contratos 
                        where partition_date = '{{ ti.xcom_pull(task_ids='InputConfig', key='month_partition_bdr', dag_id='BDR_LOAD_Tables-Reproceso_BDR_Monthly_201912_202103') }}'--'2020-08'
                        and source = 'corresponsales-tactico'
                    ) ngm
                inner join 
                    (select *
                    from bi_corp_bdr.garant_miga gmg  
                    where gmg.fec_vcto >= '{{ ti.xcom_pull(task_ids='InputConfig', key='last_working_day', dag_id='BDR_LOAD_Tables-Reproceso_BDR_Monthly_201912_202103') }}'
                        and gmg.fec_alta <= '{{ ti.xcom_pull(task_ids='InputConfig', key='last_working_day', dag_id='BDR_LOAD_Tables-Reproceso_BDR_Monthly_201912_202103') }}'
                    ) gmg                      
                on trim(gmg.num_persona) = SUBSTRING(trim(id_cto_source),1,8) 
            ) t
    ) garantias
    ON (CAST(gc.num_garantia as INT) = garantias.num_garantia)
WHERE (partition_date='{{ ti.xcom_pull(task_ids='InputConfig', key='last_working_day', dag_id='BDR_LOAD_Tables-Reproceso_BDR_Monthly_201912_202103') }}')
and gc.nominal_cobertura_actual > 0;