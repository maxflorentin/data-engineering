CREATE VIEW bi_corp_common.vstk_rec_gest_resol_acciones AS
SELECT
	id_gestion cod_rec_gestion ,
	id_accion cod_rec_accion ,
	ind_origen flag_rec_origen ,
	ind_ejecucion flag_rec_ejecucion ,
	partition_date
FROM
	bi_corp_staging.cosmos_gest_resol_acciones
WHERE
	partition_date >= date_sub(to_date(from_unixtime(unix_timestamp())),1)