set hive.merge.mapfiles=true;
set hive.merge.mapredfiles=true;
set hive.merge.size.per.task=4000000;
set hive.merge.smallfiles.avgsize=16000000;
set hive.exec.dynamic.partition.mode=nonstrict;
set mapred.job.queue.name=root.dataeng;

with tabla as (
                SELECT lpad(trim(aq.num_persona), 9, '0') AS num_persona, 
                       juri.g5508_impfactm                AS impfactm,
                       juri.g5508_tdeudacl                AS tdeudacl,
                       cotiz.imp_cambio_fijo_vigente      AS imp_cambio_fijo_vigente,
                       msc.tipo_segmento_local_1          AS tipo_segmento_local_1,
                       msc.tipo_segmento_local_2          AS tipo_segmento_local_2
                FROM
                  (select distinct crp.nup,crp.sector_contable,crp.tipo_segmento_local_1,crp.tipo_segmento_local_2
                    from bi_corp_staging.corresponsales crp
                    where crp.partition_date = '{{ ti.xcom_pull(task_ids='InputConfig', key='max_partition_corresponsales', dag_id='BDR_LOAD_Carter') }}'
                  ) crp
                  LEFT JOIN bi_corp_bdr.baremos_local bl24 on bl24.cod_baremo_alfanumerico_local = crp.tipo_segmento_local_2 and bl24.cod_negocio_local = '24'
                  LEFT JOIN bi_corp_bdr.map_baremos_global_local mb24 on mb24.cod_baremo_local = bl24.cod_baremo_local and mb24.cod_negocio = 24
                  LEFT JOIN bi_corp_bdr.map_sector_contable msc on msc.tipo_segmento_local_1 = bl24.cod_baremo_alfanumerico_local
                  LEFT JOIN 
                    (select distinct num_persona 
                      from bi_corp_bdr.perim_rating_aqua 
                      where partition_date = '{{ ti.xcom_pull(task_ids='InputConfig', key='month_partition_bdr', dag_id='BDR_LOAD_Carter') }}'
                    ) aq 
                    ON aq.num_persona=crp.nup 
                  LEFT JOIN bi_corp_bdr.JM_CLIEN_JURI juri 
                    on juri.g5508_idnumcli = lpad(trim(crp.nup), 9, '0') 
                      and juri.partition_date = '{{ ti.xcom_pull(task_ids='InputConfig', key='month_partition_bdr', dag_id='BDR_LOAD_Carter') }}'
                  LEFT JOIN (SELECT cod_divisa, data_date_part, cast((imp_cambio_fijo/100)as double) as imp_cambio_fijo_vigente, concat(substr(fec_cambio, 1,4), substr(fec_cambio, 6,2), substr(fec_cambio, 9,2)) as fec_cambio_yyyymmdd
     	                        FROM santander_business_risk_arda.cotizacion
    	                        WHERE data_date_part = '{{ ti.xcom_pull(task_ids='InputConfig', key='last_working_day_arda', dag_id='BDR_LOAD_Carter') }}'
    	 	                            and ind_divisa = 'D' and ind_cotizacion = 'S' and cod_segmento = ''
    	                              and cod_divisa = 'EUR' 
                                    and fec_cambio = '{{ ti.xcom_pull(task_ids='InputConfig', key='last_working_day', dag_id='BDR_LOAD_Carter') }}') cotiz 
                    ON (cod_divisa ='EUR')
)

INSERT overwrite TABLE bi_corp_bdr.test_jm_client_bii
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
      ,(CASE WHEN t.num_persona is not null THEN '00009'
		         WHEN t.tipo_segmento_local_2 in ('C1','S','A','B','C','P1','P2') THEN '00002'
		         WHEN t.tipo_segmento_local_2 in ('EM','G1','GL','LA','LO','MU','OT')
		              and (cast(nvl(t.IMPFACTM,'0') as bigint) / 100 / nvl(t.imp_cambio_fijo_vigente,cast('1' as double)) < cast('50000000' as int ))
		              and (cast(nvl(t.TDEUDACL,'0') as bigint) / nvl(t.imp_cambio_fijo_vigente,cast('1' as double)) < cast('1000000' as int)) then
		              '00002'
		         ELSE '00001'
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
      ,partition_date
FROM bi_corp_bdr.test_jm_client_bii bii
  LEFT JOIN tabla t ON t.num_persona = bii.G4093_IDNUMCLI
WHERE partition_date = '{{ ti.xcom_pull(task_ids='InputConfig', key='month_partition_bdr', dag_id='BDR_LOAD_Carter') }}'