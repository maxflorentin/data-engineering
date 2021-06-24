set mapred.job.queue.name=root.dataeng;
set hive.exec.dynamic.partition.mode=nonstrict;

--CREATE TEMPORARY TABLE MAIL AS
------- mail_mya.............
WITH mail_mya  AS (
select
row_number() over (order by penumper,email) id
, penumper
, email
, origen
, formato_error
, rebote_duro
, repetido
, vigente
, orden
, xmail
, partition_Date
from(
select
trim( id_subscriber )  as penumper,
trim( lower( destination ) ) email,
'MYA' as origen,
'N' as formato_error,
case when seqnum = '1' and id_status = '0' then 'S' else 'N' end as rebote_duro,
'N' as repetido,
'S' as vigente,
min(seqnum) as orden,
trim( lower( destination ) ) xmail,
partition_Date
from   bi_corp_staging.rio74_destination
where   id_device = 'MAIL'
and     trim( id_subscriber) is not null
and partition_Date='{{ ti.xcom_pull(task_ids='InputConfig', key='partition_date', dag_id='LOAD_CMN_CAMPANIAS') }}'
group by
trim(id_subscriber),
trim(lower(destination)),
case when seqnum = '1' and id_status = '0' then 'S' else 'N' end ,
partition_Date ) pr ),


tmp_rebote_duro  AS (
SELECT
id,
penumper,
mya.email,
origen,
formato_error,
case when reb.email is not null  then 'S' else mya.rebote_duro end rebote_duro,
repetido,
vigente,
orden,
xmail,
mya.partition_Date
FROM mail_mya mya
LEFT JOIN (
select  distinct lower(trim(email)) as email
from    bi_corp_staging.responsys_bounce
where   bounce_type = 'H' and lower(trim(email)) not in ('juanc.alesina@alesina.com.ar' )
AND partition_date > '{{ ti.xcom_pull(task_ids='InputConfig', key='previous_month-1_date_from', dag_id='LOAD_CMN_CAMPANIAS') }}' ) reb on reb.email=mya.email),


-- partition_date 60 dias para atras



tmp_rebote_baja  AS (
select
id,
penumper,
reb.email,
origen,
formato_error,
rebote_duro,
repetido,
case when opt.email is not null then 'S' else 'N' end negativo,
vigente,
orden,
xmail,
reb.partition_Date
from tmp_rebote_duro reb
left join(
select  email
from   bi_corp_staging.responsys_opt_out
) opt on opt.email= reb.xmail ),


validar_mail as (
SELECT
xmail,
  CASE WHEN instr(xmail, '.')=0
  		or instr(xmail, '.') = instr(xmail, '@') + 1
  		or instr(xmail, '.')= LENGTH(xmail)
        or  instr(xmail, '@')=0
        or  instr(xmail, '@')=1
        or  instr(xmail, '@')=LENGTH(xmail)
        or  instr(xmail, '@')!=(length(xmail) + 1 - instr(reverse(xmail), '@'))
        or  LENGTH(xmail) < 6
        or (length(xmail) + 1 - instr(reverse(xmail), '.')) >=LENGTH(xmail) -1 then -1
	  WHEN instr(UPPER(CONCAT('.TV.COM.EDU.GOV.GOB.NET.BIZ.ORG.COOP',
	'.AF.AL.DZ.As.AD.AO.AI.AQ.AG.AP.AR.AM.AW.AU.AT.AZ.BS.BH.BD.BB.BY','.BE.BZ.BJ.BM.BT.BO.BA.BW.BV.BR.IO.BN.BG.BF.MM.BI.KH.CM.CA.CV.KY',
	'.CF.TD.CL.CN.CX.CC.CO.KM.CG.CD.CK.CR.CI.HR.CU.CY.CZ.DK.DJ.DM.DO','.TP.EC.EG.SV.GQ.ER.EE.ET.FK.FO.FJ.FI.CS.SU.FR.FX.GF.PF.TF.GA.GM.GE.DE',
	'.GH.GI.GB.GR.GL.GD.GP.GU.GT.GN.GW.GY.HT.HM.HN.HK.HU.IS.IN.ID.IR.IQ','.IE.IL.IT.JM.JP.JO.KZ.KE.KI.KW.KG.LA.LV.LB.LS.LR.LY.LI.LT.LU.MO.MK.MG',
	'.MW.MY.MV.ML.MT.MH.MQ.MR.MU.YT.MX.FM.MD.MC.MN.MS.MA.MZ.NA','.NR.NP.NL.AN.NT.NC.NZ.NI.NE.NG.NU.NF.KP.MP.NO.OM.PK.PW.PA.PG.PY','.PE.PH.PN.PL.PT.PR.QA.RE.RO.RU.RW.GS.SH.KN.LC.PM.ST.VC.SM.SA.SN.SC',
	'.SL.SG.SK.SI.SB.SO.ZA.KR.ES.LK.SD.SR.SJ.SZ.SE.CH.SY.TJ.TW.TZ.TH.TG.TK','.TO.TT.TN.TR.TM.TC.TV.UG.UA.AE.UK.US.UY.UM.UZ.VU.VA.VE.VN.VG.VI',
	'.WF.WS.EH.YE.YU.ZR.ZM.ZW')),UPPER(SUBSTRING(xmail,LENGTH(xmail) -1)))>0 THEN IF(REGEXP_REPLACE(upper(substr( xmail, 1, instr(xmail, '@') - 1 )),concat(UPPER(substr(xmail,1,1)),'+$'),'') is not null, 1,-2)
	ELSE 0 END  validar

FROM tmp_rebote_baja
),



marcar_formato as (
select
distinct id,
penumper,
ba.email,
origen,
CASE WHEN validar!=1
	 	and formato_error = 'N'
	 	and rebote_duro = 'N'
	 	and  vigente = 'S' THEN 'S'
	 WHEN length(trim(translate(lower(ba.xmail), 'abcdefghijklmnopqrstuvwxyz@-.0123456789_', '                                        ' ) ) ) > 0
	 	and formato_error = 'N'
     	and rebote_duro = 'N'
	 	and  vigente = 'S' THEN 'S'
	 WHEN ( upper( ba.xmail ) like '%POSEE%' or upper( ba.xmail ) like '%TIENE%' or upper( ba.xmail ) like '%NOMAIL%' or upper( ba.xmail ) like '%NOTENGO%' )
		and     formato_error = 'N'
		and     rebote_duro = 'N'
		and     vigente = 'S' THEN 'S'
	WHEN err.email is not null
	 	and     formato_error = 'N'
		and     rebote_duro = 'N'
		and     vigente = 'S' THEN 'S'
	 WHEN ba.xmail like '%..%'
		and     formato_error = 'N'
		and     rebote_duro = 'N'
		and     vigente = 'S' THEN 'S'
	 WHEN (
		ba.xmail like '.%'
		or  substr( ba.xmail, 1, instr( ba.xmail, '@' ) - 1 ) = '.'
		or  substr( ba.xmail, 1, 1 ) = '@'
		)
		and     formato_error = 'N'
		and     rebote_duro = 'N'
		and     vigente = 'S' THEN 'S'
	  WHEN (
		ba.xmail like '%.'
		or  substr( ba.xmail, instr( ba.xmail, '@' ) ) rlike '%*_%'
		or  substr( ba.xmail, instr( ba.xmail, '@' ) ) not like '%.%'
		or  substr( ba.xmail, instr( ba.xmail, '@' ) + 1 ) = '.'
		or  substr( ba.xmail, instr( ba.xmail, '@' ) ) like '%.-%'
		)
		and     formato_error = 'N'
		and     rebote_duro = 'N'
		and     vigente = 'S'     THEN 'S'
	WHEN  not ba.xmail rlike '[^\-]?([[a-z0-9A-Z]\-\_\d\.])+[^\-]?@[^\-]?([[a-z0-9A-Z]\-\_\d\.])+[^\-]?'
	    and     formato_error = 'N'
		and     rebote_duro = 'N'
		and     vigente = 'S' THEN 'S'
		  WHEN substr( ba.xmail, instr(ba.xmail, '@' ) + 2, 1 ) = '.'
		and     formato_error = 'N'
		and     rebote_duro = 'N'
		and     vigente = 'S' THEN 'S'
	WHEN substr( ba.xmail, instr( ba.xmail, '@' ) + 1 ) in
			('noposee.com','notiene.com','noposee.com.ar','posee.com','notiene.com.ar',
			'nn.com','pp.com','qq.com','xxx.com','no.com.ar','notengo.com','nousa.com',
			'sinmail.com','nomail.com','nn.com.ar','noinforma.com','tengo.com','com.ar',
			'notengo.com.ar','no.no','asdasd.com','tiene.com','nono.com','xx.com',
			'nodeclara.com','nopose.com','noposeo.com','nousa.com.ar','noinforma.com.ar',
			'xxx.com.ar','jjj.com.ar','mmm.com','nono.com.ar','noinf.com',
			'notienemail.com','posee.com.ar','aaa.com',
			'n.com', 'no.com', 'a.com', 'notiene.com', 'x.com', 'noposee.com.ar', 'h.com',
			'n.com.ar', 'g.com', 'posee.com', 's.com', 'a.com.ar',
			'f.com', 'e.com', 'n.com', 'm.com', 'no.com.ar', 'no.no', 'd.com', 'o.com',
			 'w.com', 'a.c', 'c.com', 'h.c')
		and     formato_error = 'N'
		and     rebote_duro = 'N'
		and     vigente = 'S' THEN 'S'
	WHEN  ba.xmail like '% %'
		and     formato_error = 'N'
		and     rebote_duro = 'N'
		and     vigente = 'S' THEN 'S' else formato_error end  formato_error,

rebote_duro,
repetido,
negativo,
vigente,
orden,
ba.xmail,
substr(ba.email,instr(ba.email, '@' )+ 1 ) AS dominio,
ba.partition_Date
from tmp_rebote_baja ba
left join validar_mail va on va.xmail=ba.xmail
left join bi_corp_staging.rio35_tb_mails_erroneos err on lower(err.email)=ba.xmail),


tmp_act_mails as (
select
id,
orden,
penumper,
if(dominio_ok IS NULL,form.email, concat(substr(form.email, 1, instr(form.email, '@' )),dominio_ok )) xmail_new
from marcar_formato form
left join bi_corp_staging.rio35_tb_dominios_reemplazar ree on
(dominio_error= dominio)
where  vigente = 'S'),


tmp_act_mails_i as(
SELECT
distinct mail.id,
mail.penumper as penumper,
email,
origen,
formato_error,
rebote_duro,
repetido,
negativo,
vigente,
mail.orden,
xmail_new as xmail,
partition_Date
FROM marcar_formato mail
JOIN tmp_act_mails tmp on (mail.id=tmp.id and mail.orden=tmp.orden)
),

tmp_mails_repetidos as (
select xmail
from(
select xmail , count( distinct penumper )  can_penum
from tmp_act_mails_i
where   formato_error = 'N'
and     negativo = 'N'
and     rebote_duro = 'N'
and     vigente = 'S'
group by xmail) rep  where can_penum>5
),


repetidos_i as (
select
id,
mail.penumper,
email,
origen,
formato_error,
rebote_duro,
CASE WHEN rep.xmail is not null then 'S' else  repetido end repetido ,
negativo,
vigente,
orden,
mail.xmail,
partition_Date
from tmp_act_mails_i mail
left join tmp_mails_repetidos rep on rep.xmail=mail.xmail)


INSERT OVERWRITE TABLE  bi_corp_common.stk_cam_mejormail
PARTITION(partition_date)

select
DISTINCT CAST(penumper AS BIGINT)				cod_per_nup,
email											ds_cam_email,
origen											ds_cam_origen,
CASE WHEN formato_error='S' then 1 else 0 end   flag_cam_formato_error,
CASE WHEN rebote_duro='S' then 1 else 0 end 	flag_cam_rebote_duro,
CASE WHEN repetido ='S' then 1 else 0 end 		flag_cam_repetido,
CASE WHEN negativo='S' then 1 else 0 end 		flag_cam_negativo,
CASE WHEN vigente='S' then 1 else 0 end 		flag_cam_vigente,
CAST(orden AS INT)								ds_cam_prioridad,
xmail											ds_cam_xmail,
partition_date
from repetidos_i

