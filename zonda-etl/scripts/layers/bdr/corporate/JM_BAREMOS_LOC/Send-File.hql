SELECT DISTINCT CONCAT(
lpad(nvl(cast(empresa as string), '0'),5,'0'),
lpad(nvl(cod_negocio_local,'0'),5,'0'),
lpad(nvl(cod_baremo_local,'0'),5,'0'),
nvl(fecha_desde,'00001-01-01'),
nvl(fecha_hasta,'00001-01-01'),
rpad(nvl(SUBSTRING(trim(regexp_replace(regexp_replace(desc_baremo_local,'�', '?'),'"','')),1,40),' '),40,' '),
nvl(fecha_modificacion,' '))   AS line
FROM bi_corp_bdr.baremos_local;