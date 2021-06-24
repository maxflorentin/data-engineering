CREATE VIEW bi_corp_common.vstk_rec_gest_motdes_acciones AS
SELECT
	id_gestion cod_rec_gestion ,
	id_accion cod_rec_accion ,
	valor_accion ds_rec_valor_accion ,
	cod_grupo cod_rec_grupo ,
	ind_origen flag_rec_origen ,
	partition_date
FROM
	bi_corp_staging.cosmos_gest_motdes_acciones
WHERE
	partition_date >= date_sub(to_date(from_unixtime(unix_timestamp())),1)