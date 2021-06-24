set mapred.job.queue.name=root.dataeng;
set hive.exec.dynamic.partition.mode=nonstrict;

CREATE
TEMPORARY TABLE cartera_ejecutivo_1 AS
SELECT partition_date,
cod_per_nup AS penumper,
       fc_per_productosatrasados AS mejor_produ,
       ds_per_segmento AS cus_subtype,
       (CASE
            WHEN ds_per_segmento = 'BEIA' THEN 'PYM'
            WHEN ds_per_segmento = 'INDIVIDUOS' THEN 'IND'
            WHEN ds_per_segmento = 'BMA' THEN 'COR'
            WHEN ds_per_segmento = 'PYME' THEN 'PYM'
        END) AS pesubseg,
       cod_per_cuadrante AS cuadrante,
       ds_per_subsegmento AS renta,
       flag_per_vip AS vip,
       flag_per_agro AS agro
FROM bi_corp_common.stk_per_personas
WHERE cod_per_condicioncliente='CLI'
AND partition_date='{{ ti.xcom_pull(task_ids='InputConfig', key='partition_date', dag_id='LOAD_CMN_Modelo_Atencion-Daily') }}';


CREATE
TEMPORARY TABLE cartera_ejecutivo_2 AS
SELECT b.*,
       a.pesucadm,
       a.pebancap,
       a.peejecue
FROM bi_corp_staging.malpe_pedt040 a
RIGHT JOIN cartera_ejecutivo_1 b ON a.penumper = b.penumper
WHERE a.partition_date ='{{ ti.xcom_pull(task_ids='InputConfig', key='partition_date', dag_id='LOAD_CMN_Modelo_Atencion-Daily') }}';

--------------------------

CREATE
TEMPORARY TABLE cartera_ejecutivo_3 AS
SELECT b.*,
       a.user_id AS suc_legajo
FROM bi_corp_staging.crm_tejec_carte a
RIGHT JOIN cartera_ejecutivo_2 b ON b.peejecue = a.peejecue
AND b.pesucadm = a.pesucadm
AND b.pebancap = a.pebancap
AND a.pesubseg = b.pesubseg
AND a.partition_date ='{{ ti.xcom_pull(task_ids='InputConfig', key='partition_date', dag_id='LOAD_CMN_Modelo_Atencion-Daily') }}';

-----------------------------------------------

CREATE
TEMPORARY TABLE cartera_ejecutivo_4 AS
SELECT b.*,
       initcap(trim(a.bup_nombre)) AS suc_nombre,
       initcap(trim(a.bup_apellido)) AS suc_apellido,
       a.bup_descripcion_puesto AS suc_puesto,
       null as er_legajo,
       null as er_nombre,
       null as er_apellido,
       null as er_puesto,
       null as er_grupo,
	   null as cartera_dual,
	   null as ce_legajo,
	   null as ce_nombre,
	   null as ce_apellido,
	   null as ce_puesto
FROM bi_corp_staging.personas_vw_lake a
RIGHT JOIN cartera_ejecutivo_3 b ON b.suc_legajo = a.bup_usuario
AND a.estado_laboral = 'A'
AND a.partition_date ='{{ ti.xcom_pull(task_ids='InputConfig', key='partition_date', dag_id='LOAD_CMN_Modelo_Atencion-Daily') }}';

-
CREATE
TEMPORARY TABLE tmp_eje_remoto AS
SELECT partition_date,
	   nup,
       user_id AS legajo
FROM bi_corp_staging.crm_carte_ejec_remoto
WHERE partition_date ='{{ ti.xcom_pull(task_ids='InputConfig', key='partition_date', dag_id='LOAD_CMN_Modelo_Atencion-Daily') }}'
ORDER BY nup;


INSERT OVERWRITE TABLE cartera_ejecutivo_4
SELECT a.partition_date,
	   a.penumper,
       a.mejor_produ,
       a.cus_subtype,
       a.pesubseg,
       a.cuadrante,
       a.renta,
       a.vip,
       a.agro,
       a.pesucadm,
       a.pebancap,
       a.peejecue,
       (CASE
            WHEN (a.suc_legajo IS NOT NULL
                  AND a.suc_nombre IS NULL
                  AND a.suc_apellido IS NULL
                  AND a.suc_puesto IS NULL) THEN NULL
            ELSE a.suc_legajo
        END) AS suc_legajo,
       a.suc_nombre,
       a.suc_apellido,
       a.suc_puesto,
       nvl(b.legajo,
           a.er_legajo) AS er_legajo,
       a.er_nombre,
       a.er_apellido,
       a.er_puesto,
       a.er_grupo
FROM cartera_ejecutivo_4 a
LEFT JOIN tmp_eje_remoto b ON a.penumper = b.nup;



INSERT OVERWRITE TABLE cartera_ejecutivo_4
SELECT a.partition_date,
       a.penumper,
       a.mejor_produ,
       a.cus_subtype,
       a.pesubseg,
       a.cuadrante,
       a.renta,
       a.vip,
       a.agro,
       a.pesucadm,
       a.pebancap,
       a.peejecue,
       a.suc_legajo,
       a.suc_nombre,
       a.suc_apellido,
       a.suc_puesto,
       a.er_legajo,
       (CASE
            WHEN b.existe=1 THEN initcap(trim(b.bup_nombre))
            ELSE a.er_nombre
        END) AS er_nombre,
       (CASE
            WHEN b.existe=1 THEN initcap(trim(b.bup_apellido))
            ELSE a.er_apellido
        END) AS er_apellido,
       (CASE
            WHEN b.existe=1 THEN b.bup_descripcion_puesto
            ELSE a.er_puesto
        END) AS er_puesto,
       a.er_grupo
FROM cartera_ejecutivo_4 a
LEFT JOIN
  (SELECT 1 AS existe,
          bup_usuario,
          bup_descripcion_puesto,
          bup_nombre,
          bup_apellido
   FROM bi_corp_staging.personas_vw_lake
   WHERE bup_descripcion_puesto IS NOT NULL
     AND partition_date ='{{ ti.xcom_pull(task_ids='InputConfig', key='partition_date', dag_id='LOAD_CMN_Modelo_Atencion-Daily') }}'
     AND estado_laboral = 'A') b ON a.er_legajo = b.bup_usuario;

create temporary TABLE tmp_eje_remoto_grupo as
SELECT
penumper,
       (CASE
            WHEN pevalind = 'V' THEN 'VIP'
            WHEN pevalind = 'S' THEN 'SELECT'
            WHEN pevalind = 'D' THEN 'DUO'
            WHEN pevalind = 'A' THEN 'AGRO'
            WHEN pevalind = 'P' THEN 'PYME'
            ELSE 'ERROR'
        END) AS er_grupo
FROM bi_corp_staging.malpe_pedt052
WHERE peindica = 'GES'
  AND partition_date='{{ ti.xcom_pull(task_ids='InputConfig', key='partition_date', dag_id='LOAD_CMN_Modelo_Atencion-Daily-Daily') }}'
ORDER BY 1 ;

INSERT OVERWRITE TABLE cartera_ejecutivo_4
SELECT a.partition_date,
       a.penumper,
       a.mejor_produ,
       a.cus_subtype,
       a.pesubseg,
       a.cuadrante,
       a.renta,
       a.vip,
       a.agro,
       a.pesucadm,
       a.pebancap,
       a.peejecue,
       a.suc_legajo,
       a.suc_nombre,
       a.suc_apellido,
       a.suc_puesto,
       a.er_legajo,
       a.er_nombre,
       a.er_apellido,
       a.er_puesto,
       (CASE
            WHEN b.existe=1 THEN b.er_grupo
            ELSE a.er_grupo
        END) AS er_grupo
FROM cartera_ejecutivo_4 a
LEFT JOIN
  (SELECT 1 AS existe,
          penumper,
          er_grupo
   FROM tmp_eje_remoto_grupo
   WHERE er_grupo IS NOT NULL ) b ON a.penumper = b.penumper ;


INSERT OVERWRITE TABLE cartera_ejecutivo_4
SELECT a.partition_date,
       a.penumper,
       a.mejor_produ,
       a.cus_subtype,
       a.pesubseg,
       a.cuadrante,
       a.renta,
       a.vip,
       a.agro,
       a.pesucadm,
       a.pebancap,
       a.peejecue,
       a.suc_legajo,
       a.suc_nombre,
       a.suc_apellido,
       a.suc_puesto,
       (CASE
            WHEN (a.er_legajo IS NOT NULL
                  AND a.er_nombre IS NULL
                  AND a.er_apellido IS NULL
                  AND a.er_puesto IS NULL) THEN NULL
            ELSE a.er_legajo
        END) AS er_legajo,
       a.er_nombre,
       a.er_apellido,
       a.er_puesto,
       a.er_grupo
FROM cartera_ejecutivo_4 a ;


INSERT OVERWRITE TABLE cartera_ejecutivo_4
SELECT a.partition_date,
       a.penumper,
       a.mejor_produ,
       a.cus_subtype,
       a.pesubseg,
       a.cuadrante,
       a.renta,
       a.vip,
       a.agro,
       a.pesucadm,
       a.pebancap,
       a.peejecue,
       a.suc_legajo,
       a.suc_nombre,
       a.suc_apellido,
       a.suc_puesto,
       a.er_legajo,
       a.er_nombre,
       a.er_apellido,
       a.er_puesto,
       a.er_grupo,
       (CASE
            WHEN b.existe=1 THEN concat(b.peindica, '-', b.pevalind)
            ELSE a.cartera_dual
        END) AS cartera_dual,
       a.ce_legajo,
       a.ce_nombre,
       a.ce_apellido,
       a.ce_puesto
FROM cartera_ejecutivo_4 a
LEFT JOIN
  (SELECT 1 AS existe,
          penumper,
          peindica,
          pevalind
   FROM bi_corp_staging.malpe_pedt052
   WHERE (peindica IS NOT NULL
          OR pevalind IS NOT NULL)
     AND partition_date ='{{ ti.xcom_pull(task_ids='InputConfig', key='partition_date', dag_id='LOAD_CMN_Modelo_Atencion-Daily') }}'
     AND peindica in ('ADE',
                      'ADP') ) b ON a.penumper = b.penumper ;

----------------------------------

INSERT OVERWRITE TABLE cartera_ejecutivo_4
SELECT a.partition_date,
       a.penumper,
       a.mejor_produ,
       a.cus_subtype,
       a.pesubseg,
       a.cuadrante,
       a.renta,
       a.vip,
       a.agro,
       a.pesucadm,
       a.pebancap,
       a.peejecue,
       a.suc_legajo,
       a.suc_nombre,
       a.suc_apellido,
       a.suc_puesto,
       a.er_legajo,
       a.er_nombre,
       a.er_apellido,
       a.er_puesto,
       a.er_grupo,
       a.cartera_dual,
       (CASE
            WHEN b.existe=1 THEN b.ejec_zona
            ELSE a.ce_legajo
        END) AS ce_legajo,
       a.ce_nombre,
       a.ce_apellido,
       a.ce_puesto
FROM cartera_ejecutivo_4 a
LEFT JOIN
  (SELECT 1 AS existe,
          nup,
          ejec_zona
   FROM bi_corp_staging.crm_tcab_pyc_masche
   WHERE ejec_zona IS NOT NULL
     AND partition_date ='{{ ti.xcom_pull(task_ids='InputConfig', key='partition_date', dag_id='LOAD_CMN_Modelo_Atencion-Daily') }}') b ON cast(a.penumper AS int) = b.nup




INSERT OVERWRITE TABLE cartera_ejecutivo_4
SELECT a.partition_date,
	   a.penumper,
       a.mejor_produ,
       a.cus_subtype,
       a.pesubseg,
       a.cuadrante,
       a.renta,
       a.vip,
       a.agro,
       a.pesucadm,
       a.pebancap,
       a.peejecue,
       a.suc_legajo,
       a.suc_nombre,
       a.suc_apellido,
       a.suc_puesto,
       a.er_legajo,
       a.er_nombre,
       a.er_apellido,
       a.er_puesto,
       a.er_grupo,
       a.cartera_dual,
       a.ce_legajo,
       (CASE
            WHEN b.existe=1 THEN initcap(trim(b.nombre))
            ELSE a.ce_nombre
        END) AS ce_nombre,
       (CASE
            WHEN b.existe=1 THEN initcap(trim(b.apellido))
            ELSE a.ce_apellido
        END) AS ce_apellido,
       (CASE
            WHEN b.existe=1 THEN b.desc_puesto
            ELSE a.ce_puesto
        END) AS ce_puesto
FROM cartera_ejecutivo_4 a
LEFT JOIN
  (SELECT 1 AS existe,
          bup_usuario AS usuario,
          bup_nombre AS nombre,
          bup_apellido AS apellido,
          bup_descripcion_puesto AS desc_puesto
   FROM bi_corp_staging.personas_vw_lake
   WHERE (bup_nombre IS NOT NULL
          OR bup_apellido IS NOT NULL
          OR bup_descripcion_puesto IS NOT NULL)
     AND partition_date ='{{ ti.xcom_pull(task_ids='InputConfig', key='partition_date', dag_id='LOAD_CMN_Modelo_Atencion-Daily') }}'
     AND estado_laboral = 'A') b ON b.usuario = a.ce_legajo;

INSERT OVERWRITE TABLE cartera_ejecutivo_4
SELECT a.partition_date,
	   a.penumper,
       a.mejor_produ,
       a.cus_subtype,
       a.pesubseg,
       a.cuadrante,
       a.renta,
       a.vip,
       a.agro,
       a.pesucadm,
       a.pebancap,
       a.peejecue,
       a.suc_legajo,
       a.suc_nombre,
       a.suc_apellido,
       a.suc_puesto,
       a.er_legajo,
       a.er_nombre,
       a.er_apellido,
       a.er_puesto,
       a.er_grupo,
       a.cartera_dual,
       (CASE
            WHEN (ce_legajo IS NOT NULL
                  AND ce_nombre IS NULL
                  AND ce_apellido IS NULL
                  AND ce_puesto IS NULL) THEN NULL
            ELSE a.ce_legajo
        END) AS ce_legajo,
       a.ce_nombre,
       a.ce_apellido,
       a.ce_puesto
FROM cartera_ejecutivo_4 a ;



INSERT OVERWRITE TABLE  bi_corp_common.stk_ate_cartera_ejecutivo
PARTITION(partition_date)
select

                                cod_ate_penumper as cod_per_nup,

                                case

                                    when er_grupo is not null then 'EJECUTIVO REMOTO'

                                    when er_grupo is null and ce_legajo is not null then 'CENTRO EMPRESAS / BUSINESS CENTER'

                                    when er_grupo is null and ce_legajo is null then 'SUCURSAL'

                                end as ds_ate_modelo_atencion,

                                case

                                    when er_grupo is not null  then er_grupo

                                    when er_grupo is null and ce_legajo is not null then cartera_dual

                                    when er_grupo is null and ce_legajo is null then pesucadm

                                end as ds_ate_oficina,

                                case

                                    when er_grupo is not null then er_legajo

                                    when er_grupo is null and ce_legajo is not null then ce_legajo

                                    when er_grupo is null and ce_legajo is null then suc_legajo

                                end as ds_ate_legajo,

                                case

                                    when er_grupo is not null then er_nombre

                                    when er_grupo is null and ce_legajo is not null then ce_nombre

                                    when er_grupo is null and ce_legajo is null and suc_legajo is not null then suc_nombre

                                end as ds_ate_nombre,

                                case

                                    when er_grupo is not null then er_apellido

                                    when er_grupo is null and ce_legajo is not null then ce_apellido

                                    when er_grupo is null and ce_legajo is null and suc_legajo is not null then suc_apellido

                                end as ds_ate_apellido,

                                case

                                    when er_grupo is not null then er_puesto

                                    when er_grupo is null and ce_legajo is not null then ce_puesto

                                    when er_grupo is null and ce_legajo is null and suc_legajo is not null then suc_puesto

                                end as ds_ate_puesto,

                                ds_ate_peejecue as ds_ate_ejecutivo,
                                partition_date

                        from cartera_ejecutivo_4;

