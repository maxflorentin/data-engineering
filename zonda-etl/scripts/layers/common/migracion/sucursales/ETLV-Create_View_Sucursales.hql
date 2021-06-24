CREATE VIEW IF NOT EXISTS bi_corp_common.vdim_sucursales AS
SELECT tcdt050.cod_entidad, tcdt050.cod_centro, tcdt050.desc_comp_centro_op desc_centro, tcdt050.direc_comp_centro_op direccion,
tcdt050.ind_cod_postal_unico cod_postal, tcdt050.cod_pais, tcdt050.cod_localidad, tcdt050.cod_regio_zona, tcdt050.tipo_centro,
tcdt050.ind_suc_amba, tcdt050.zona_sucursal, tcdt050.cod_deleg_hacienda, tcdt050.fec_apert_ofic fecha_aper_oficina,
tcdt050.fec_alta_centro fecha_alta, tcdt050.fec_baja_centro fecha_baja, tcdt050.partition_date FROM bi_corp_staging.tcdt050
