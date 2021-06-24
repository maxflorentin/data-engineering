CREATE VIEW `bi_corp_common.vbt_cue_movimientos` AS select
                                                        concat_ws('_', `mo1`.`mo1_entidad`, `mo1`.`mo1_centro_alta`, `mo1`.`mo1_cuenta`, `mo1`.`mo1_producto`, `mo1`.`mo1_subprodu`, `mo1`.`mo1_divisa`) AS `cod_cue_contrato`,
                                                        `mo1`.`mo1_entidad` as `cod_cue_entidad`,
                                                        `mo1`.`mo1_centro_alta` as `cod_suc_sucursal_alta`,
                                                        `mo1`.`mo1_cuenta` as `cod_cue_cuenta`,
                                                        `mo1`.`mo1_divisa` as `cod_cue_divisa`,
                                                        `mo1`.`mo1_producto` as `cod_cue_producto`,
                                                        `mo1`.`mo1_subprodu` as `cod_cue_subproducto`,
                                                        `mo1`.`mo1_cod_oper_ppal` as `cod_cue_operacion`,
                                                        `mo1`.`mo1_numer_mov` as `cod_cue_comprobante`,
                                                        `mo1`.`mo1_canal` as `cod_cue_canal`,
                                                        'TBD' as `cod_cue_signo_movimiento`,
                                                        `mo1`.`mo1_importe` as `fc_cue_importe`,
                                                        `mo1`.`mo1_ind_aom` as `flag_cue_automatico`,
                                                        `mae`.`producto_contab` as `cod_cue_producto_contable`,
                                                        `mae`.`subprodu_contab` as `cod_cue_subproducto_contable`,
                                                        `aux`.`ind_paquete` as `flag_cue_paquete`,
                                                        'TBD' as `cod__cuelook_resumen`,
                                                        'TBD' as `nup`,
                                                        `mo1`.`mo1_transaccion` as `cod_cue_transaccion`,
                                                        `mo1`.`mo1_ind_car_abo` as `cod_cue_cargo_abono`,
                                                        `mo1`.`mo1_ind_mov_genuino` as `flag_cue_geniuno`,
                                                        `mo1`.`mo1_centro_umo` as `cod_suc_sucursal_ultimo_movimiento`,
                                                        `mo1`.`mo1_centro_origen` as `cod_suc_sucursal_origen`,
                                                        `mo1`.`mo1_fecha_oper` as `partition_date`,
                                                        `mo1`.`mo1_hora_oper` as `ts_cue_operacion`,
                                                        `mo1`.`mo1_concepto` as `desc_cue_concepto`
                                                    from `bi_corp_staging`.`malbgc_bgdtmo1` `mo1`
                                                             left join `bi_corp_staging`.`bgdtmae` `mae`
                                                                       on  `mae`.`entidad` = `mo1`.`mo1_entidad` and
                                                                           `mae`.`centro_alta` = `mo1`.`mo1_centro_alta` and
                                                                           `mae`.`cuenta` = `mo1`.`mo1_cuenta` and
                                                                           `mae`.`divisa` = `mo1`.`mo1_divisa` and
                                                                           `mae`.`producto` = `mo1`.`mo1_producto` and
                                                                           `mae`.`subprodu` = `mo1`.`mo1_subprodu` and
                                                                           `mae`.`partition_date` = '2020-01-31'
                                                             left join `bi_corp_staging`.`bgdtaux` `aux`
                                                                       on `mo1`.`mo1_entidad` = `aux`.`entidad` and
                                                                          `mo1`.`mo1_centro_alta` = `aux`.`centro_alta` and
                                                                          `mo1`.`mo1_cuenta` = `aux`.`cuenta` and
                                                                          `mo1`.`mo1_divisa` = `aux`.`divisa` and
                                                                          `aux`.`partition_date` = '2020-01-31'
                                                    WHERE `mo1`.`partition_date` IN ('2020-01-27',
                                                                                     '2020-01-28',
                                                                                     '2020-01-29',
                                                                                     '2020-01-30',
                                                                                     '2020-01-31')

                                                    UNION ALL

                                                    select
                                                        concat_ws('_', `mo2`.`mo2_entidad`, `mo2`.`mo2_centro_alta`, `mo2`.`mo2_cuenta`, `mo2`.`mo2_producto`, `mo2`.`mo2_subprodu`, `mo2`.`mo2_divisa`) AS `cod_cue_contrato`,
                                                        `mo2`.`mo2_entidad` as `cod_cue_entidad`,
                                                        `mo2`.`mo2_centro_alta` as `cod_suc_sucursal_alta`,
                                                        `mo2`.`mo2_cuenta` as `cod_cue_cuenta`,
                                                        `mo2`.`mo2_divisa` as `cod_cue_divisa`,
                                                        `mo2`.`mo2_producto` as `cod_cue_producto`,
                                                        `mo2`.`mo2_subprodu` as `cod_cue_subproducto`,
                                                        `mo2`.`mo2_cod_oper_ppal` as `cod_cue_operacion`,
                                                        `mo2`.`mo2_numer_mov` as `cod_cue_comprobante`,
                                                        `mo2`.`mo2_canal` as `cod_cue_canal`,
                                                        'TBD' as `cod_cue_signo_movimiento`,
                                                        `mo2`.`mo2_importe` as `fc_cue_importe`,
                                                        `mo2`.`mo2_ind_aom` as `flag_cue_automatico`,
                                                        `mae`.`producto_contab` as `cod_cue_producto_contable`,
                                                        `mae`.`subprodu_contab` as `cod_cue_subproducto_contable`,
                                                        `aux`.`ind_paquete` as `flag_cue_paquete`,
                                                        'TBD' as `cod__cuelook_resumen`,
                                                        'TBD' as `nup`,
                                                        `mo2`.`mo2_transaccion` as `cod_cue_transaccion`,
                                                        `mo2`.`mo2_ind_car_abo` as `cod_cue_cargo_abono`,
                                                        `mo2`.`mo2_ind_mov_genuino` as `flag_cue_geniuno`,
                                                        `mo2`.`mo2_centro_umo` as `cod_suc_sucursal_ultimo_movimiento`,
                                                        `mo2`.`mo2_centro_origen` as `cod_suc_sucursal_origen`,
                                                        `mo2`.`mo2_fecha_oper` as `partition_date`,
                                                        `mo2`.`mo2_hora_oper` as `ts_cue_operacion`,
                                                        `mo2`.`mo2_concepto` as `desc_cue_concepto`
                                                    from `bi_corp_staging`.`malbgc_bgdtmo2` `mo2`
                                                             left join `bi_corp_staging`.`bgdtmae` `mae`
                                                                       on  `mae`.`entidad` = `mo2`.`mo2_entidad` and
                                                                           `mae`.`centro_alta` = `mo2`.`mo2_centro_alta` and
                                                                           `mae`.`cuenta` = `mo2`.`mo2_cuenta` and
                                                                           `mae`.`divisa` = `mo2`.`mo2_divisa` and
                                                                           `mae`.`producto` = `mo2`.`mo2_producto` and
                                                                           `mae`.`subprodu` = `mo2`.`mo2_subprodu` and
                                                                           `mae`.`partition_date` = '2020-01-31'
                                                             left join `bi_corp_staging`.`bgdtaux` `aux`
                                                                       on `mo2`.`mo2_entidad` = `aux`.`entidad` and
                                                                          `mo2`.`mo2_centro_alta` = `aux`.`centro_alta` and
                                                                          `mo2`.`mo2_cuenta` = `aux`.`cuenta` and
                                                                          `mo2`.`mo2_divisa` = `aux`.`divisa` and
                                                                          `aux`.`partition_date` = '2020-01-31'
                                                    WHERE `mo2`.`partition_date` IN ('2020-01-27',
                                                                                     '2020-01-28',
                                                                                     '2020-01-29',
                                                                                     '2020-01-30',
                                                                                     '2020-01-31')
