set hive.merge.mapfiles=true;
set hive.merge.mapredfiles=true;
set hive.merge.size.per.task=4000000;
set hive.merge.smallfiles.avgsize=16000000;
set hive.exec.dynamic.partition.mode=nonstrict;
set mapred.job.queue.name=root.dataeng;

insert overwrite table bi_corp_bdr.jm_cal_in_co
partition(partition_date= '2020-07'
select t.E9952_FEOPERAC,
       t.E9952_S1EMP,
       t.E9952_CONTRA1,
       t.E9952_FECCALI,
       t.E9952_TIPMODEL,
       t.E9952_TIPMODE2,
       t.E9952_TIPMODE2 as E9952_IDMODEL,
       t.E9952_TIPO,
       t.E9952_IDPUNSCO,
       t.E9952_FECCADUC,
       t.E9952_C1SPID,
       t.E9952_C1DIGCON,
       t.E9952_C1RESP,
       t.E9952_C1CIRC,
       t.E9952_FECULTMO,
       t.E9952_C1MOTEXC,
       t.E9952_TIPMODE3,
       t.E9952_SCOVINPR,
       t.E9952_FECINOFC
from (
select '2020-07-31' as E9952_FEOPERAC,
       '23100' as E9952_S1EMP,
       lpad(nc.id_cto_bdr,9,'0') as E9952_CONTRA1,
       sa.fec_ingreso_sco as E9952_FECCALI,
       case
          --admision
	     when cli.tipo_persona = 'NEG PF' --C1
		  and sa.cod_final_score is not null then
		   '00031'
		 when cli.tipo_persona = 'Pyme1 Pj' --P1
		  and sa.cod_final_score is not null then
		   '00032'
		 when cli.tipo_persona = 'Pyme2 Pj'  --P2
		  and sa.cod_final_score is not null then
		   '00034'
		 when cli.tipo_persona = 'individuos' --SABC
		  and sa.cod_final_score is not null then
		   '00035'
		 when cli.tipo_persona is not null then
		   '00000' --no informa
		 else
		   lpad('9' , 5, '9') --default
	   end as E9952_TIPMODEL,
	   case
         when cli.tipo_persona = 'Pyme2 Pj'  --P2
		  and sa.cod_final_score is not null then
		    '00501'
		 when cli.tipo_persona = 'individuos' --SABC
		  and sa.cod_final_score is not null then
		    '00404'
		 when cli.tipo_persona = 'NEG PF' --C1
		  and sa.cod_final_score is not null then
		    '00401'
		 when cli.tipo_persona = 'Pyme1 Pj' --P1
		  and sa.cod_final_score is not null then
		    '00402'
		 when cli.tipo_persona is not null then--no informa
		    '00000'
		 else  -- no aplica
		    '99999'
       end as E9952_TIPMODE2,
       nvl(sa.nro_scorecard_omdm, sa.nro_scorecard_sw) as E9952_IDMODEL,
       '00093' as E9952_TIPO,
       lpad(nvl(cast(sa.cod_final_score as string),'0'),17,'0') as E9952_IDPUNSCO,
       '9999-12-31' as E9952_FECCADUC,
       '00000' as E9952_C1SPID,
       '00000' as E9952_C1DIGCON,
       '00000' as E9952_C1RESP,
       '00000' as E9952_C1CIRC,
       '2020-07-31'as E9952_FECULTMO,
       '00000' as E9952_C1MOTEXC,
       '00000' as E9952_TIPMODE3,
       lpad('0',17,'0')as E9952_SCOVINPR,
       '0001-01-01'as E9952_FECINOFC
  from(select min(rsc.timestamp_umo) fecha_minima,
              concat_ws('_', rsc.cod_entidad, rsc.cod_centro, rsc.num_cuenta, rsc.cod_producto, rsc.cod_subprodu) as id_contrato,
              sa.cod_centro ,
              sa.num_solicitud
         from santander_business_risk_arda.scoring_admision sa
        inner join santander_business_risk_arda.rel_solicitud_contrato rsc
           on rsc.cod_centro_solic = sa.cod_centro
          and rsc.num_solicitud = sa.num_solicitud --concat_ws('_', rsc.cod_entidad, rsc.cod_centro, rsc.num_cuenta, rsc.cod_producto, rsc.cod_subprodu)
          and rsc.data_date_part = '20200731'
        where sa.data_date_part = '20200731'
        group by rsc.cod_entidad, rsc.cod_centro, rsc.num_cuenta, rsc.cod_producto, rsc.cod_subprodu, sa.cod_centro, sa.num_solicitud
      ) as core
  inner join bi_corp_bdr.normalizacion_id_contratos nc
          on nc.partition_date = '2020-07'--'{{ ti.xcom_pull(task_ids='InputConfig', key='month_partition_bdr', dag_id='BDR_LOAD_Tables-Monthly') }}'
         and nc.id_cto_source = core.id_contrato
  inner join santander_business_risk_arda.scoring_admision sa
     on sa.num_solicitud = core.num_solicitud
    and sa.cod_centro  = core.cod_centro
    and sa.data_date_part = '20200731'
  inner join santander_business_risk_arda.rel_solicitud_contrato rsc
     on rsc.timestamp_umo = core.fecha_minima
    and rsc.cod_centro = core.cod_centro
    and rsc.num_solicitud  = core.num_solicitud
    and rsc.data_date_part = '20200731'
    and concat_ws('_', rsc.cod_entidad, rsc.cod_centro, rsc.num_cuenta, rsc.cod_producto, rsc.cod_subprodu) = core.id_contrato
  inner join (select cli.*,
 				     case
		               when bl24.cod_baremo_alfanumerico_local in ('C1') THEN
		                 'NEG PF'
		               when bl24.cod_baremo_alfanumerico_local in ('P1') THEN
		                 'Pyme1 Pj'
		               when bl24.cod_baremo_alfanumerico_local in ('P2') THEN
		                 'Pyme2 Pj'
		               when rlike(bl24.cod_baremo_alfanumerico_local, '(\W|^)S|A|B|C') THEN
		                 'IND PF'
		               when bl24.cod_baremo_alfanumerico_local in ('EM','G1','IU','IP') THEN
		                 'empresas'
		               when bl24.cod_baremo_alfanumerico_local in ('GL','LA','LO','MU','PU','OT','FI','F1','FE','OF') then
		                 'Corp Aqua'
		               ELSE
		                 '99999'
		             end tipo_persona
		        from santander_business_risk_arda.clientes cli
		        LEFT JOIN bi_corp_bdr.baremos_local bl24
		          on bl24.cod_baremo_alfanumerico_local = cli.cod_segmento
		         and bl24.cod_negocio_local = '24'
		        LEFT JOIN bi_corp_bdr.map_baremos_global_local mb24
		          on mb24.cod_baremo_local = bl24.cod_baremo_local
		         and mb24.cod_negocio = 24
		       where cli.condicion_del_cliente = 'CLI'
		         and cli.data_date_part = '20200731'
		               ) cli
	on cli.num_persona =  sa.num_persona)t