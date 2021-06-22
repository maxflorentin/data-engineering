CREATE TABLE CNL_01.mc_bajada_generica tablespace users_data_temp nologging AS
SELECT
	to_date(fecha || ' ' || hora,
	'dd-mm-YY hh24miss') as_of_date ,
	a.TX_ID TRANSACCION ,
	A.nup penumper ,
	DECODE (orig_rev,
	'O',
	A.importe,
	'R',
	-A.importe) monto ,
	(CASE
		WHEN A.moneda IN ('ARS',
		'$') THEN 'P'
		WHEN A.moneda IN ('U$S',
		'USD') THEN 'D'
		WHEN A.moneda IN ('EUR') THEN 'E'
		ELSE 'P'
	END) moneda ,
	USUARIO legajo ,
	maq_id || '-' || to_date(fecha || ' ' || hora,
	'dd/mm/YY hh24miss') || '-' || hora || '-' || suc_nro || '-' || suc_maq || '-' || cantidad_cheques || '-' || tx_id || '-' || usuario || '-' || ente_id || '-' || nup || '-' || importe || '-' || moneda EXT_ID ,
	suc_nro || '-' || suc_maq || '-' || cantidad_cheques extra ,
	orig_rev ,
	DECODE (orig_rev,
	'O',
	1,
	'R',
	-1) AS trx
FROM
	cnl01.movimiento@cnl01_dblink a
WHERE
	fecha >= date'2020-10-01'
	AND fecha < date'2020-10-14'
	AND canal_id = 'CAJA'
	AND maq_id BETWEEN '8000' AND '8099';

CREATE TABLE CNL_01.mc_bajada_generica_group tablespace users_data_temp nologging AS (
SELECT
	as_of_Date,
	transaccion,
	penumper,
	monto,
	moneda,
	legajo,
	ext_id,
	extra
FROM
	(
	SELECT
		a.* ,
		lead(orig_rev) OVER( PARTITION BY penumper, transaccion, abs(monto), moneda, legajo, extra
	ORDER BY
		as_of_date ) AS aaa
	FROM
		mc_bajada_generica a )
WHERE
	nvl( aaa, '*' ) != 'R'
	AND orig_rev = 'O');
