CREATE VIEW `bi_corp_common.vbt_cue_altas` AS WITH max_partition_date AS (
    SELECT MAX(`bgdtaux`.`partition_date`) AS `partition_date` FROM `bi_corp_staging`.`bgdtaux`
)
                                              SELECT
                                                  concat_ws('_', `aux`.`entidad`, `aux`.`centro_alta`, `aux`.`cuenta`, COALESCE(`mae`.`producto`,'NULLVALUE'), COALESCE(`mae`.`subprodu`,'NULLVALUE'), `aux`.`divisa`) AS `cod_cue_contrato`,
                                                  'TBD' AS `NUP`,
                                                  `aux`.`centro_alta` AS `cod_suc_sucursal`,
                                                  `aux`.`cuenta` AS `cod_cue_cuenta`,
                                                  `mae`.`producto` AS `cod_cue_producto`,
                                                  `mae`.`subprodu` AS `cod_cue_subproducto`,
                                                  `aux`.`divisa` AS `cod_divisa`,
                                                  `mae`.`campania` AS `cod_cue_campania`,
                                                  `mae`.`num_convenio` AS `cod_cue_convenio`,
                                                  `mae`.`plan_comi` AS `cod_cue_plan_comision`,
                                                  `aux`.`fecha_alta` AS `dt_cue_alta`,
                                                  'TBD' AS `fc_monto_acuerdo`,
                                                  'TBD' AS `cod_cue_paquete`,
                                                  `aux`.`ind_paquete` AS `flag_cue_paquete`,
                                                  'TBD' AS `cod_cue_producto_paquete`,
                                                  'TBD' AS `cod_cue_subproducto_paquete`,
                                                  'TBD' AS `cod_cue_categoria_cuenta`,
                                                  'TBD' AS `dt_cue_upgrade`,
                                                  'TBD' AS `flag_cue_cliente_citi`,
                                                  'TBD' AS `cod_suc_sucursal_solicitud`,
                                                  'TBD' AS `cod_cue_solicitud`,
                                                  'TBD' AS `cod_cue_canal`,
                                                  'TBD' AS `cod_cue_legajo`
                                              FROM
                                                  `bi_corp_staging`.`bgdtaux` `aux`
                                                      JOIN
                                                  max_partition_date max ON
                                                          `max`.`partition_date` = `aux`.`partition_date`
                                                      LEFT JOIN
                                                  `bi_corp_staging`.`bgdtmae` `mae`
                                                  ON
                                                              `mae`.`entidad` = `aux`.`entidad` and
                                                              `mae`.`centro_alta` = `aux`.`centro_alta` and
                                                              `mae`.`cuenta` = `aux`.`cuenta` and
                                                              `mae`.`divisa` = `aux`.`divisa` and
                                                              `mae`.`partition_date` = `aux`.`partition_date`
