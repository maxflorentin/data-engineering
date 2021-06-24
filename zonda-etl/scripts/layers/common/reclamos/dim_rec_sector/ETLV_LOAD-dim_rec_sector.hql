-- Calculamos la maxima particion
DROP TABLE IF EXISTS max_partition_sector;
CREATE TEMPORARY TABLE max_partition_sector AS
SELECT cod_sector, max(partition_date) max_partition_date
FROM bi_corp_staging.rio56_sectores
where partition_date <= '{{ ti.xcom_pull(task_ids='InputConfig', key='partition_date', dag_id='LOAD_CMN_SGC-Daily_scheduled') }}'
group by cod_sector;

INSERT
	OVERWRITE TABLE bi_corp_common.dim_rec_sector
SELECT
	trim(cod_entidad) cod_rec_entidad ,
	trim(cod_sector) cod_rec_sector ,
	trim(desc_sect) ds_rec_sector ,
	trim(cod_grupo) cod_rec_grupo_sector ,
	trim(indi_actral) cod_rec_actral_sector ,
	trim(cod_pcia) cod_rec_pcia_sector ,
	case when trim(mail_sector)='null' then null else trim(mail_sector) end as ds_rec_mail_sector,
--	mail_sector ds_rec_mail_sector ,
	trim(est_sect) cod_rec_estado_sector ,
	case when enviar_mail='S' then 1 else 0 end  flag_rec_enviar_mail_sector ,
	case when indi_info_asig_esp='S' then 1 else 0 end  flag_rec_info_asig_esp_sector ,
	trim(cod_pais) cod_rec_pais_sector ,
	case when indi_resol_info_adj='S' then 1 else 0 end flag_rec_resol_info_adj_sector ,
	case when trim(cod_ctro_costos)='null' then null else trim(cod_ctro_costos) end as cod_rec_ctro_costos_sector,
--	cod_ctro_costos cod_rec_ctro_costos_sector ,
	case when indi_mail_bandeja='S' then 1 else 0 end flag_rec_mail_bandeja_sector ,
	case when indi_uso_carta='S' then 1 else 0 end flag_rec_uso_carta_sector ,
	trim(sector_owner) cod_rec_sector_owner_sector ,
	trim(cod_grupo_empresa) cod_rec_grupo_empresa_sector ,
	case when indi_admin='S' then 1 else 0 end  flag_rec_admin_sector ,
	trim(gest_x_pag_bandeja) fc_rec_gest_x_pag_bandeja_sector ,
	case when trim(cod_sucursal)='null' then null else trim(cod_sucursal) end as cod_suc_sucursal_sector,
--	cod_sucursal cod_suc_sucursal_sector ,
	case when trim(cod_zona)='null' then null else trim(cod_zona) end as cod_rec_zona_sector,
--	cod_zona cod_rec_zona_sector ,
	trim(tipo_sector) cod_rec_tipo_sector ,
	case when trim(class_pdf)='null' then null else trim(class_pdf) end as ds_rec_class_pdf_sector,
--	class_pdf ds_rec_class_pdf_sector ,
	case when trim(dias_gestiones_ant)='null' then null else trim(dias_gestiones_ant) end as fc_rec_cant_gestiones_ant_sector,
--	dias_gestiones_ant fc_rec_cant_gestiones_ant_sector ,
	trim(cod_sector_gen) cod_rec_sector_gen ,
	case when indi_compromiso_gold='S' then 1 else 0 end flag_rec_compromiso_gold_sector ,
	case when indi_enviar_mail_resp='S' then 1 else 0 end flag_rec_enviar_mail_resp_sector ,
	case when indi_enviar_sms_resp='S' then 1 else 0 end flag_rec_enviar_sms_resp_sector,
	'{{ ti.xcom_pull(task_ids='InputConfig', key='partition_date', dag_id='LOAD_CMN_SGC-Daily_scheduled') }}' partition_date
FROM
	(
	SELECT
		cod_entidad , a.cod_sector , desc_sect , cod_grupo , indi_actral , cod_pcia , mail_sector , est_sect , enviar_mail , indi_info_asig_esp , cod_pais , indi_resol_info_adj , cod_ctro_costos , indi_mail_bandeja , indi_uso_carta , sector_owner , cod_grupo_empresa , indi_admin , gest_x_pag_bandeja , cod_sucursal , cod_zona , tipo_sector , class_pdf , dias_gestiones_ant , cod_sector_gen , indi_compromiso_gold , indi_enviar_mail_resp , indi_enviar_sms_resp , max(partition_date) ult_partition_date
	FROM
		bi_corp_staging.rio56_sectores a
    inner join max_partition_sector b
    on a.cod_sector= b.cod_sector
    and a.partition_date=b.max_partition_date
	GROUP BY
		cod_entidad , a.cod_sector , desc_sect , cod_grupo , indi_actral , cod_pcia , mail_sector , est_sect , enviar_mail , indi_info_asig_esp , cod_pais , indi_resol_info_adj , cod_ctro_costos , indi_mail_bandeja , indi_uso_carta , sector_owner , cod_grupo_empresa , indi_admin , gest_x_pag_bandeja , cod_sucursal , cod_zona , tipo_sector , class_pdf , dias_gestiones_ant , cod_sector_gen , indi_compromiso_gold , indi_enviar_mail_resp , indi_enviar_sms_resp ) A;