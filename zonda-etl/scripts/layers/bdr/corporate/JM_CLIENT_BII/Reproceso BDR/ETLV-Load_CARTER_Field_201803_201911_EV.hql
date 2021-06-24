set hive.merge.mapfiles=true;
set hive.merge.mapredfiles=true;
set hive.merge.size.per.task=4000000;
set hive.merge.smallfiles.avgsize=16000000;
set hive.exec.dynamic.partition.mode=nonstrict;
set mapred.job.queue.name=root.dataeng;


with
imp_pos_cont_deuda
     as (
         select g4128_idnumcli, sum(cast(nvl(c.e0621_importh,'0') as bigint)) as deuda
           from bi_corp_bdr.jm_posic_contr  c
         inner join bi_corp_bdr.jm_interv_cto i
                 on i.g4128_tipintev = '00001'
                and i.partition_date = '{{ ti.xcom_pull(task_ids='InputConfig', key='month_partition_bdr', dag_id='BDR_LOAD_Tables-Reproceso_BDR_Monthly_EV_AD_CB') }}'
                and c.e0621_contra1 = i.g4128_contra1
          where c.partition_date = '{{ ti.xcom_pull(task_ids='InputConfig', key='month_partition_bdr', dag_id='BDR_LOAD_Tables-Reproceso_BDR_Monthly_EV_AD_CB') }}'
         group by i.g4128_idnumcli
        )
INSERT overwrite TABLE bi_corp_bdr.jm_client_bii
PARTITION(partition_date)
SELECT G4093_FEOPERAC
      ,G4093_S1EMP
      ,G4093_IDNUMCLI
      ,G4093_TIP_PERS
      ,G4093_APNOMPER
      ,G4093_DATEXTO1
      ,G4093_DATEXTO2
      ,G4093_IDENTPER
      ,G4093_CODIDPER
      ,G4093_CLIE_GLO
      ,G4093_IDSUCUR
      ,(CASE
          WHEN ra.num_persona is not null THEN
            '00009'
		  WHEN trim(bii.G4093_TIPSEGL2) in ('C1','S','A','B','C','P1','P2') THEN
		    '00002'
		  WHEN trim(bii.G4093_TIPSEGL2) in ('EM','G1','GL','LA','LO','MU','OT')
		    and (cast(nvl(juri.g5508_impfactm,'0') as bigint) / 100 / nvl(cotiz.imp_cambio_fijo_vigente,cast('1' as double)) < cast('50000000' as int ))
		    and (deuda / -100 / nvl(cotiz.imp_cambio_fijo_vigente,cast('1' as double)) < cast('1000000' as int))
		  then
		    '00002'
		  ELSE
		    '00001'
		  END) AS G4093_CARTER
      ,G4093_ID_PAIS
      ,G4093_COD_SECT
      ,G4093_COD_SEC2
      ,G4093_COD_SEC3
      ,G4093_CLISEGM
      ,G4093_CLISEGL1
      ,G4093_TIPSEGL1
      ,G4093_CLISEGL2
      ,G4093_TIPSEGL2
      ,G4093_FCHINI
      ,G4093_FCHFIN
      ,G4093_FECULTMO
      ,G4093_INDUSTRY
      ,G4093_EXCLCLI
      ,G4093_INDBCART
      ,G4093_FECNACIM
      ,G4093_PAISNEG
      ,G4093_CDPOSTAL
      ,G4093_TTO_ESPE
      ,G4093_GRA_VINC
      ,G4093_UTP_CLI
      ,G4093_FINIUTCL
      ,G4093_FFINUTCL
      ,bii.partition_date
FROM bi_corp_bdr.jm_client_bii bii
LEFT JOIN (
           select distinct g5508_idnumcli, g5508_impfactm
             from bi_corp_bdr.JM_CLIEN_JURI
            where partition_date = '{{ ti.xcom_pull(task_ids='InputConfig', key='month_partition_bdr', dag_id='BDR_LOAD_Tables-Reproceso_BDR_Monthly_EV_AD_CB') }}'
          ) juri on juri.g5508_idnumcli = bii.G4093_IDNUMCLI
left join bi_corp_bdr.perim_rating_aqua ra on lpad(trim(ra.num_persona), 9, '0') = bii.G4093_IDNUMCLI and ra.partition_date = '{{ ti.xcom_pull(task_ids='InputConfig', key='month_partition_bdr', dag_id='BDR_LOAD_Tables-Reproceso_BDR_Monthly_EV_AD_CB') }}'
left join imp_pos_cont_deuda pc on pc.g4128_idnumcli = bii.G4093_IDNUMCLI
LEFT JOIN (SELECT cod_divisa, data_date_part, cast((imp_cambio_fijo/100)as double) as imp_cambio_fijo_vigente, concat(substr(fec_cambio, 1,4), substr(fec_cambio, 6,2), substr(fec_cambio, 9,2)) as fec_cambio_yyyymmdd
     	     FROM santander_business_risk_arda.cotizacion
    	    WHERE data_date_part = '{{ ti.xcom_pull(task_ids='InputConfig', key='last_working_day_arda', dag_id='BDR_LOAD_Tables-Reproceso_BDR_Monthly_EV_AD_CB') }}'
    	 	  and ind_divisa = 'D'
    	 	  and ind_cotizacion = 'S'
    	 	  and cod_segmento = ''
    	      and cod_divisa = 'EUR'
              and fec_cambio = '{{ ti.xcom_pull(task_ids='InputConfig', key='last_working_day', dag_id='BDR_LOAD_Tables-Reproceso_BDR_Monthly_EV_AD_CB') }}'
          ) cotiz ON (cod_divisa ='EUR')
WHERE bii.partition_date = '{{ ti.xcom_pull(task_ids='InputConfig', key='month_partition_bdr', dag_id='BDR_LOAD_Tables-Reproceso_BDR_Monthly_EV_AD_CB') }}'