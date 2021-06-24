"
SET mapred.job.queue.name=root.dataeng;
set hive.exec.dynamic.partition.mode=nonstrict;

INSERT OVERWRITE TABLE bi_corp_common.dim_per_domicilios
PARTITION(partition_date)
SELECT malpe_pedt003.penumper cod_per_nup
, CAST(trim(malpe_pedt003.pesecdom) AS INT) ds_per_secuencia
, malpe_pedt003.petipdom ds_per_tipo_domicilio
, trim(malpe_pedt003.penomcal) ds_per_calle
, CAST(nvl(malpe_pedt003.penumblo, '0.0') AS INT) ds_per_nro_calle
, trim(substr(malpe_pedt003.peobserv, 29, 4)) ds_per_piso
, trim(substr(malpe_pedt003.peobserv, 34, 5)) ds_per_depto
, trim(substr(malpe_pedt003.peobserv, 6, 4)) ds_per_torre
, trim(substr(malpe_pedt003.peobserv, 1, 20)) ds_per_edificio
, trim(substr(malpe_pedt003.peobserv, 40, 30)) ds_per_entre_calle
, trim(substr(malpe_pedt003.peobserv, 70, 30)) ds_per_y_calle
, trim(substr(malpe_pedt003.peobserv, 125, 9)) ds_per_longitud
, trim(substr(malpe_pedt003.peobserv, 135, 9)) ds_per_latitud
, malpe_pedt003.pedesloc ds_per_localidad
, malpe_pedt003.pecodpro cod_per_cod_provincia
, b.provincia ds_per_provincia
, malpe_pedt003.pecodpos ds_per_cod_postal
, malpe_pedt003.pecodpai cod_per_pais
, c.pais ds_per_pais
, malpe_pedt003.petitdom flag_per_titular_domicilo
, malpe_pedt003.pemarnor flag_per_normalizacion
, malpe_pedt003.pemardom flag_per_domicilio_erroneo
, malpe_pedt003.pemarcor flag_per_correspondencia
, malpe_pedt003.pefecalt dt_per_fecha_alta
, to_date(malpe_pedt003.pehstamp) dt_per_fecha_modificacion
, malpe_pedt003.partition_date partition_date
FROM bi_corp_staging.malpe_pedt003
LEFT OUTER JOIN 
	(SELECT dom.gen_clave pecodpro, trim(substr(dom.gen_datos, 1, 30)) provincia
	FROM bi_corp_staging.tcdtgen dom WHERE dom.gentabla = '0009' AND dom.partition_date IN (SELECT max(partition_date)
	FROM bi_corp_staging.tcdtgen)
	) b ON trim(b.pecodpro) = trim(malpe_pedt003.pecodpro)
LEFT OUTER JOIN 
	(SELECT DISTINCT pais.gen_clave pecodpai, trim(substr(pais.gen_datos, 1, 30)) pais
	FROM bi_corp_staging.tcdtgen pais WHERE pais.gentabla = '0112' AND pais.partition_date IN (SELECT max(partition_date)
	FROM bi_corp_staging.tcdtgen)
	) c ON trim(c.pecodpai) = trim(malpe_pedt003.pecodpai) 
WHERE NOT malpe_pedt003.petipdom LIKE 'ELE'
AND malpe_pedt003.partition_date = '{{ ti.xcom_pull(task_ids='InputConfig', key='partition_date', dag_id='LOAD_STG_Malpe-Daily') }}'
"