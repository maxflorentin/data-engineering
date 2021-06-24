set hive.merge.mapfiles=true;
set hive.merge.mapredfiles=true;
set hive.merge.size.per.task=4000000;
set hive.merge.smallfiles.avgsize=16000000;
set hive.exec.dynamic.partition.mode=nonstrict;
set mapred.job.queue.name=root.dataeng;

DROP TABLE IF EXISTS pedt008_use;
CREATE TEMPORARY TABLE pedt008_use
AS
select distinct a.penumper,a.pecalpar,a.peordpar,a.penumcon,a.pecodpro,a.pecodsub
from
(
SELECT
		first_value(penumper) over(partition by pecalpar, peordpar, penumcon, pecodpro, pecodsub  order by peestrel asc, pefecbrb desc ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING) penumper,
        pecalpar,
		peordpar,
		penumcon,
		pecodpro,
		pecodsub
FROM bi_corp_staging.malpe_pedt008  p08
WHERE
        pecodpro IN ('40', '41', '42', '43')
        and partition_date = '{{ ti.xcom_pull(task_ids='InputConfig', key='partition_date', dag_id='LOAD_CMN_Beneficios_promociones-Daily_Hist') }}') a
				;

DROP TABLE IF EXISTS pedt008_use_titular;
CREATE TEMPORARY TABLE pedt008_use_titular AS
select distinct penumper,penumcon,pecodpro,pecodsub
from
(select
first_value(penumper) over(partition by penumcon, pecodpro, pecodsub  order by peestrel asc, pefecbrb desc ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING) penumper,
pecalpar,
peordpar,
penumcon,
pecodpro,
pecodsub
from bi_corp_staging.malpe_pedt008 p08
WHERE
 p08.pecodpro in ('40','41','42','43')
 AND p08.partition_date = '{{ ti.xcom_pull(task_ids='InputConfig', key='partition_date', dag_id='LOAD_CMN_Beneficios_promociones-Daily_Hist') }}'
AND p08.pecalpar = 'TI') t;


DROP TABLE IF EXISTS pedt008_intermedia;
CREATE TEMPORARY TABLE pedt008_intermedia AS
select p.penumper,p.pecalpar,p.peordpar,p.penumcon,p.pecodpro,p.pecodsub, tit.penumper penumper_titular
from pedt008_use p
left join pedt008_use_titular tit on tit.penumcon = p.penumcon and tit.pecodpro = p.pecodpro and tit.pecodsub = p.pecodsub;

DROP TABLE IF EXISTS pedt008_final;
CREATE TEMPORARY TABLE pedt008_final AS
select distinct penumper_tarjeta,penumper_titular,pecodpro,pecodsub,penumcon,peordpar
from (select
first_value(penumper) over(partition by pecodpro, penumcon, peordpar order by pecalpar asc ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING) penumper_tarjeta,
first_value(penumper_titular) over(partition by pecodpro, penumcon, peordpar order by pecalpar asc ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING) penumper_titular,
pecodpro,
pecodsub,
penumcon,
peordpar
from pedt008_intermedia) t;

DROP TABLE IF EXISTS wamas02_temp;
CREATE TEMPORARY TABLE wamas02_temp AS
select *
from bi_corp_staging.amas_wamas02
where partition_date = '{{ ti.xcom_pull(task_ids='InputConfig', key='partition_date', dag_id='LOAD_CMN_Beneficios_promociones-Daily_Hist') }}';

DROP TABLE IF EXISTS cuentasdebito;
CREATE TEMPORARY TABLE cuentasdebito AS
SELECT
distinct
cod_per_nup_titular,
cod_per_nup_tarjeta
FROM bi_corp_common.stk_cue_tarjetas_debito
WHERE partition_date = '{{ ti.xcom_pull(task_ids='InputConfig', key='partition_date', dag_id='LOAD_CMN_Beneficios_promociones-Daily_Hist') }}';

INSERT  overwrite TABLE bi_corp_common.bt_pro_movimientos_promocionados
    PARTITION (partition_date)

  SELECT

        cast(abae_use.tar_nup  as int)                              as cod_pro_nupconsumo,
        cast(cd.cod_per_nup_titular as int)                         as cod_pro_nuptitular,
        null                              				as cod_pro_cuenta,
        null                  							as cod_pro_tipo_cuenta,
<<<<<<< HEAD
        A.tlf_semcacct                             as cod_pro_tarjeta,
        lpad(substr (A.tlf_semfanum_suc, (-1)* 3),4,0)      as cod_pro_sucursal,
        CASE
          WHEN LENGTH(CAST(A.tlf_semtrdat AS string)) = 3 THEN from_unixtime(unix_timestamp(concat(substring(A.partition_date, 1, 4),'0',CAST(A.tlf_semtrdat AS string)), 'yyyyMMdd'),'yyyy-MM-dd')
          WHEN length(CAST(A.tlf_semtrdat AS string)) = 4 THEN from_unixtime(unix_timestamp(concat(substring(A.partition_date, 1, 4),CAST(A.tlf_semtrdat AS string)), 'yyyyMMdd'),'yyyy-MM-dd')
          ELSE NULL
          END  AS

                                            dt_pro_movimiento,
        case when length(cast(A.tlf_semtrtim as string))=6
			then concat(
						  CASE
				          WHEN LENGTH(CAST(A.tlf_semtrdat AS string)) = 3 THEN from_unixtime(unix_timestamp(concat(substring(A.partition_date, 1, 4),'0',CAST(A.tlf_semtrdat AS string)), 'yyyyMMdd'),'yyyy-MM-dd')
				          WHEN length(CAST(A.tlf_semtrdat AS string)) = 4 THEN from_unixtime(unix_timestamp(concat(substring(A.partition_date, 1, 4),CAST(A.tlf_semtrdat AS string)), 'yyyyMMdd'),'yyyy-MM-dd')
				          ELSE NULL
				          END,' ',substring(cast(A.tlf_semtrtim as string),1,2),':',substring(cast(A.tlf_semtrtim as string),3,2),':',substring(cast(A.tlf_semtrtim as string),5,2)
			            )
             else concat(
						  CASE
				          WHEN LENGTH(CAST(A.tlf_semtrdat AS string)) = 3 THEN from_unixtime(unix_timestamp(concat(substring(A.partition_date, 1, 4),'0',CAST(A.tlf_semtrdat AS string)), 'yyyyMMdd'),'yyyy-MM-dd')
				          WHEN length(CAST(A.tlf_semtrdat AS string)) = 4 THEN from_unixtime(unix_timestamp(concat(substring(A.partition_date, 1, 4),CAST(A.tlf_semtrdat AS string)), 'yyyyMMdd'),'yyyy-MM-dd')
				          ELSE NULL
				          END,' 0',substring(cast(A.tlf_semtrtim as string),1,1),':',substring(cast(A.tlf_semtrtim as string),2,2),':',substring(cast(A.tlf_semtrtim as string),4,2)
			            )
             end
                                  as ts_pro_movimiento,
        cast(A.tlf_semtatmi as string)                                  as cod_pro_establecimiento,
        A.tlf_cbu_destino                                  as ds_pro_establecimiento,
        A.tlf_semtcomer                                 as cod_pro_comercio,
=======
        A.tlx_semcacct                                  as cod_pro_tarjeta,
        lpad(substr (A.tlx_semfanum, (-1)* 3),4,0)      as cod_pro_sucursal,
        case
          when length(cast(A.tlx_semtrdat as string)) = 3 then concat(substring(A.partition_date, 1, 4),'0',cast(A.tlx_semtrdat as string))
          when length(cast(A.tlx_semtrdat as string)) = 4 then concat(substring(A.partition_date, 1, 4),cast(A.tlx_semtrdat as string))
          else null
          end                                           as dt_pro_movimiento,
        A.tlx_semtrtim                                  as ts_pro_movimiento,
        cast(A.tlx_semtatmi as string)                                  as cod_pro_establecimiento,
        A.tlx_semtnloc                                  as ds_pro_establecimiento,
        A.tlx_semtcomer                                 as cod_pro_comercio,
>>>>>>> 7b7284ad5e015de0b8430a3b453a859be379ac18
        null                                            as ds_pro_comercio,
        A.tlf_fiid                                      as cod_pro_camp,
        proa.desc_campana                               as ds_pro_campania,
<<<<<<< HEAD
        cast(A.tlf_rubro as string)                     as cod_pro_rubro,
        A.tlf_semimpo_original                          as fc_pro_importebonificado,
        A.tlf_semamt1                                   as fc_pro_importeconsumo,
=======
        cast(A.tlx_rubro as string)                     as cod_pro_rubro,
        A.tlx_semimpo_original                          as fc_pro_importebonificado,
        A.tlx_semamt1                                   as fc_pro_importeconsumo,
>>>>>>> 7b7284ad5e015de0b8430a3b453a859be379ac18
        'debito'                                        as ds_pro_mediopago,
        A.partition_date
from bi_corp_staging.abae_wabaetlx A
LEFT JOIN
    (
      Select
      MT.tar_numero_tarjeta,
      MT.tar_nup
      from bi_corp_staging.abae_maestarj MT
      WHERE MT.partition_date = '{{ ti.xcom_pull(task_ids='InputConfig', key='partition_date', dag_id='LOAD_CMN_Beneficios_promociones-Daily_Hist') }}'

    ) abae_use
    ON trim(abae_use.tar_numero_tarjeta) = trim(A.tlf_semcacct)
    LEFT JOIN
    (
      SELECT
      distinct cod_banco,
      desc_campana
      from bi_corp_staging.ops_promociones_ra ra
               Where ra.partition_date = '{{ ti.xcom_pull(task_ids='InputConfig', key='partition_date', dag_id='LOAD_CMN_Beneficios_promociones-Daily_Hist') }}'

     ) proa
     ON proa.cod_banco = A.tlf_fiid
     LEFT JOIN
cuentasdebito cd
on abae_use.tar_nup = cd.cod_per_nup_tarjeta


WHERE
     A.tlf_fiid <> ''
     and A.tlf_semtcode = 17
     and A.partition_date  = '{{ ti.xcom_pull(task_ids='InputConfig', key='partition_date', dag_id='LOAD_CMN_Beneficios_promociones-Daily_Hist') }}'
UNION ALL

SELECT

    cast(p08.penumper_tarjeta as int)                        as cod_pro_nupconsumo,
    cast(p08.penumper_titular as int)                   		 as cod_pro_nuptitular,
    s02.s02_cuenta_tc                        as cod_pro_cuenta,
    s02.s02_tipo_cuenta                      as cod_pro_tipo_cuenta,
    w600.600_tarjeta                        as cod_pro_tarjeta,
    lpad (w600.600_casadest,4,0)             as cod_pro_sucursal,
     date_format(
            from_unixtime(unix_timestamp(cast(w600.600_forig AS string), 'yyyyMMdd')), 'yyyy-MM-dd'
        )
         as dt_pro_movimiento,
    null                                     as ts_pro_movimiento,
    cast(w600.600_numest as string)          as cod_pro_establecimiento,
    w600.600_denest                          as ds_pro_establecimiento,
    w600.600_numcom                          as cod_pro_comercio,
    null                                     as ds_pro_comercio,
    w600.600_cod_campania_bco                as cod_pro_camp,
    prora.desc_campana                       as ds_pro_campania,
    cast (w600.600_rubro as string)          as cod_pro_rubro,
    w600.600_imp_dto_usu                     as fc_pro_importebonificado,
    w600.600_imporig                         as fc_pro_importeconsumo,
    'credito'                                as ds_pro_mediopago,
    w600.partition_date
    from bi_corp_staging.aftc_waftc600_presentados w600

    LEFT JOIN wamas02_temp s02 ON s02.s02_nro_tarjeta = substr(w600.600_tarjeta,1,16)
LEFT JOIN pedt008_final p08
    ON
    p08.penumcon = s02.s02_rel_nro_cta
    AND p08.peordpar = s02.s02_rel_nro_firm
    AND (CASE w600.600_bcodest
        WHEN "472" THEN "42"
        WHEN "172" THEN "41"
        WHEN "072" THEN "40"
        WHEN "72" THEN "40"
        END) = p08.pecodpro

    LEFT JOIN
       (
         SELECT
         distinct cod_banco,
         desc_campana
         from bi_corp_staging.ops_promociones_ra ra
                  Where ra.partition_date  = '{{ ti.xcom_pull(task_ids='InputConfig', key='partition_date', dag_id='LOAD_CMN_Beneficios_promociones-Daily_Hist') }}'

        ) prora
        ON prora.cod_banco = w600.600_cod_campania_bco
    where
    w600.600_cod_registro='30'
    and w600.600_id_archivo='P'
    and w600.600_cod_campania_bco <> ''
    and w600.partition_date  = '{{ ti.xcom_pull(task_ids='InputConfig', key='partition_date', dag_id='LOAD_CMN_Beneficios_promociones-Daily_Hist') }}';
