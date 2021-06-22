--IF EXISTE_OBJETO('TAB_01', 'tmp_pedt030', 'table') = 1 THEN
--EXECUTE IMMEDIATE 'DROP TABLE tmp_pedt030';
--END IF;
--EXECUTE IMMEDIATE 'create table tmp_pedt030 tablespace users_data_temp nologging as
                           
SELECT penumper
, pesegcla segmento
, CASE WHEN SUBSTR(pesegcla,0,1) IN (''6'',''7'',''8'',''A'',''B'',''C'',''D'',''E'',''F'',''G'',''H'',''I'') THEN ''I''
	WHEN SUBSTR(pesegcla,0,1) IN (''J'',''K'') THEN ''P1''
	WHEN SUBSTR(pesegcla,0,1) IN (''L'',''M'') THEN ''P2''
	WHEN SUBSTR(pesegcla,0,1) IN (''5'',''4'') THEN ''P1'' -- PC1
	WHEN SUBSTR(pesegcla,0,1) IN (''N'',''O'') THEN ''EC'' -- EM
	WHEN SUBSTR(pesegcla,0,1) IN (''P'',''Q'') THEN ''EG'' -- G1
	WHEN SUBSTR(pesegcla,0,1) = ''0'' THEN ''IU''
	WHEN SUBSTR(pesegcla,0,1) IN (''3'',''9'') THEN ''IP''
	WHEN SUBSTR(pesegcla,0,1) IN (''R'',''S'',''T'',''U'',''V'',''W'',''X'',''Y'',''Z'',''1'',''2'') THEN ''C''
	END cus_subtype
, CASE WHEN SUBSTR(pesegcla,0,1) = ''6'' THEN ''S1''
	WHEN SUBSTR(pesegcla,0,1) = ''7'' THEN ''S2''
	WHEN SUBSTR(pesegcla,0,1) = ''8'' THEN ''S3''
	WHEN SUBSTR(pesegcla,0,1) = ''A'' THEN ''A1''
	WHEN SUBSTR(pesegcla,0,1) = ''B'' THEN ''A2''
	WHEN SUBSTR(pesegcla,0,1) = ''C'' THEN ''A3''
	WHEN SUBSTR(pesegcla,0,1) = ''D'' THEN ''B1''
	WHEN SUBSTR(pesegcla,0,1) = ''E'' THEN ''B2''
	WHEN SUBSTR(pesegcla,0,1) = ''F'' THEN ''B3''
	WHEN SUBSTR(pesegcla,0,1) = ''G'' THEN ''C1''
	WHEN SUBSTR(pesegcla,0,1) = ''H'' THEN ''C2''
	WHEN SUBSTR(pesegcla,0,1) = ''I'' THEN ''C3''
	WHEN SUBSTR(pesegcla,0,1) IN (''J'',''K'') THEN ''P1''
	WHEN SUBSTR(pesegcla,0,1) IN (''L'',''M'') THEN ''P2''
	WHEN SUBSTR(pesegcla,0,1) IN (''5'',''4'') THEN ''PC1''
	WHEN SUBSTR(pesegcla,0,1) IN (''N'',''O'') THEN ''EM''
	WHEN SUBSTR(pesegcla,0,1) IN (''P'',''Q'') THEN ''GE'' -- G1
	WHEN SUBSTR(pesegcla,0,1) = ''0'' THEN ''IU''
	WHEN SUBSTR(pesegcla,0,1) IN (''3'',''9'') THEN ''IP''
	WHEN SUBSTR(pesegcla,0,1) IN (''R'',''S'',''T'',''U'',''V'',''W'',''X'',''Y'',''Z'',''1'',''2'') THEN ''C''
	END cuadrante
, CASE WHEN SUBSTR(pesegcla,0,1) IN (''6'',''7'',''8'') THEN ''R. Select''
	WHEN SUBSTR(pesegcla,0,1) IN (''A'',''B'',''C'') THEN ''R. Alta''
	WHEN SUBSTR(pesegcla,0,1) IN (''D'',''E'',''F'') THEN ''R. Media''
	WHEN SUBSTR(pesegcla,0,1) IN (''G'',''H'',''I'') THEN ''R. Masiva''
	WHEN SUBSTR(pesegcla,0,1) IN (''J'',''K'') THEN ''Pyme1''
	WHEN SUBSTR(pesegcla,0,1) IN (''L'',''M'') THEN ''Pyme2''
	WHEN SUBSTR(pesegcla,0,1) IN (''5'',''4'') THEN ''Negocio''
	WHEN SUBSTR(pesegcla,0,1) IN (''N'',''O'') THEN ''Empresa Mediana''
	WHEN SUBSTR(pesegcla,0,1) IN (''P'',''Q'') THEN ''Empresa Grande''
	WHEN SUBSTR(pesegcla,0,1) IN (''0'',''3'',''9'') THEN ''Instituciones''
	WHEN SUBSTR(pesegcla,0,1) IN (''R'',''S'',''T'',''U'',''V'',''W'',''X'',''Y'',''Z'',''1'',''2'') THEN ''Corporate''
	END renta
from mviews.pedt030
where as_of_date = ''' || pfecha || '''';