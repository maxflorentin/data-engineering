"set mapred.job.queue.name=root.dataeng;
select  from_unixtime(UNIX_TIMESTAMP(MAX(odate),'yyyy-MM-dd'),"yyyyMMdd"), count(*)
 from ( select max(a.partition_date) odate,
LPAD(a.fund_code, 4, '0') int_fondo, LPAD(a.person,7,'0') person, '         ' as dni,
'    ' as tipo,'                    ' as certificado ,LPAD(a.account,10, '0') cliente,
a.accounting_center centro_cont,a.accounting_account rubro,
LPAD(b.currency,3, ' ') divisa,p.pesucope ofi_comercial,
LPAD(cast(round(abs(sum(cast(a.commission as double))*100),0) as string), 17, '0') imp_mo,
'0072' entidad,'C' cre_deb,'02' gen_oper
from bi_corp_staging.fm_comisiones a
inner join bi_corp_staging.fm_maestro_fondos b on a.fund_code = b.fund_code
and a.partition_date = b.partition_date
left join bi_corp_staging.malpe_pedt042 p on p.partition_date = a.partition_date and
p.penumcon = LPAD(a.account, 12, '0') and p.pecodpro = '60' and p.pecodsub = '0000'
where a.partition_date between '{{ ti.xcom_pull(task_ids='InputConfig', key='first_working_day', dag_id='REPORT_CDG_COMISIONES_FONDOS') }}'
and '{{ ti.xcom_pull(task_ids='InputConfig', key='last_working_day', dag_id='REPORT_CDG_COMISIONES_FONDOS') }}'
group by  a.fund_code, a.person, a.account, a.accounting_center,
          a.accounting_account, b.currency,p.pesucope) as com
"