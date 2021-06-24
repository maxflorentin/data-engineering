CREATE VIEW IF NOT EXISTS bi_corp_common.vstk_amuc_rechazados AS
SELECT
550_entidad as cod_chq_entidad,
cast(null as int) as cod_chq_cuentacheque,
550_comprobante as cod_chq_nrocheque,
550_sucursal as cod_suc_sucursal,
550_divisa as cod_chq_divisa,
550_cuenta as cod_chq_cuenta,
550_producto as cod_chq_producto,
550_subproducto as cod_chq_subproducto,
case when 550_fecha_rechazo in ('9999-12-31', '0001-01-01') then NULL else 550_fecha_rechazo end as dt_chq_fecha_rechazo,
case when 550_fec_pag in ('9999-12-31', '0001-01-01') then NULL else 550_fec_pag end as dt_chq_fecha_rescate,
550_persona_empresa as cod_chq_tipo_persona,
550_firmante_1 as ds_chq_firmante1,
550_firmante_2 as ds_chq_firmante2,
550_firmante_3 as ds_chq_firmante3,
550_firmante_4 as ds_chq_firmante4,
550_firmante_5 as ds_chq_firmante5,
550_motivo as cod_chq_motivo_rechazo,
550_subcodi as cod_chq_subrechazo,
550_indicador_computable as cod_chq_indi_computable,
550_codigo as cod_chq_rechazo,
coalesce(cast(trim(concat(substr(REGEXP_REPLACE(550_importe, "^0+", ''),1,length(REGEXP_REPLACE(550_importe, "^0+", ''))-2),'.',substr(REGEXP_REPLACE(550_importe, "^0+", ''),-2)))as decimal(19, 4)),0) as fc_chq_monto
FROM bi_corp_staging.amuc_amuc550 a
INNER JOIN ( select max(partition_date) as max from bi_corp_staging.amuc_amuc550 where partition_date > date_sub(to_date(from_unixtime(unix_timestamp())),7) ) x
ON a.partition_date = x.max
WHERE partition_date > date_sub(to_date(from_unixtime(unix_timestamp())),7)
;