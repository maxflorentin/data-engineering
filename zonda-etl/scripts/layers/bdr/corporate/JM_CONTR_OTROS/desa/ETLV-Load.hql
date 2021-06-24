set hive.merge.mapfiles=true;
set hive.merge.mapredfiles=true;
set hive.merge.size.per.task=4000000;
set hive.merge.smallfiles.avgsize=16000000;
set hive.exec.dynamic.partition.mode=nonstrict;
set mapred.job.queue.name=root.dataeng;

CREATE TABLE bi_corp_bdr.contr_otros_test_fix
    (
        query      STRING,
        id_cto_bdr STRING,
        unnv       STRING,
        unnts      STRING
    );

with
 tabla as (
               select * from (
                              select row_number() over(partition by c.id_cto_bdr,rn.segmentation_code order by rn.global_value asc) as orden,
                                     c.id_cto_bdr , rn.segmentation_code , rn.global_value
                                from bi_corp_bdr.perim_contratos_bis c
                              inner join bi_corp_common.rosetta_nkey_hist nk
                                  on c.id_cto_bdr = nk.native_key
                                 and c.partition_date = '2020-10'
                                 and nk.partition_date= '2020-09-30'
                                 and nk.domain_code ='00004'
                              inner join bi_corp_common.rosetta_nkey_hist nk2
                                  on nk2.master_key = nk.master_key
                                 and nk2.partition_date= '2020-09-30'
                                 and nk2.domain_code = '00005'
                              inner join bi_corp_common.rosetta_rnkd_hist rn
                                  on rn.native_key = nk2.native_key
                                 and rn.partition_date=  '2020-09-30'
                                 and rn.domain_code = '00005'
                               where rn.segmentation_code in ( '00001', '00004')
                                 and trim(rn.global_value) != '998'
                              order by c.id_cto_bdr
                              ) a
                where orden = 1
            ),
 baremos as (
             select t.id_cto_bdr, t.segmentation_code, bl.cod_baremo_local, bgl.cod_baremo_global
               from tabla t
             left join bi_corp_bdr.baremos_local bl
                 on bl.cod_negocio_local in ('86','116')
                and t.global_value = bl.cod_baremo_alfanumerico_local
             left join bi_corp_bdr.map_baremos_global_local bgl
                 on bl.cod_negocio_local = cast(bgl.cod_negocio_local as string)
                and bl.cod_baremo_local = bgl.cod_baremo_local
             )
insert overwrite table bi_corp_bdr.contr_otros_test_fix
select 'actual',
       b1.id_cto_bdr,
       lpad(b4.cod_baremo_local,5,'0'),
       lpad(b1.cod_baremo_global,5,'0')
from baremos b1 inner join baremos b4
	on b1.id_cto_bdr = b4.id_cto_bdr
	    and b1.segmentation_code = '00001'
	    and b4.segmentation_code = '00004';


with
 tabla1 as (
               select * from (
                              select row_number() over(partition by c.id_cto_bdr,rn.segmentation_code order by rn.global_value asc) as orden,
                                     c.id_cto_bdr , rn.segmentation_code , rn.global_value
                                from bi_corp_bdr.perim_contratos_bis c
                              inner join bi_corp_common.rosetta_nkey_hist nk
                                  on c.id_cto_bdr = nk.native_key
                                 and c.partition_date = '2020-10'
                                 and nk.partition_date= '2020-09-30'
                                 and nk.domain_code ='00004'
                              inner join bi_corp_common.rosetta_nkey_hist nk2
                                  on nk2.master_key = nk.master_key
                                 and nk2.partition_date= '2020-09-30'
                                 and nk2.domain_code = '00005'
                              inner join bi_corp_common.rosetta_rnkd_hist rn
                                  on rn.native_key = nk2.native_key
                                 and rn.partition_date=  '2020-09-30'
                                 and rn.domain_code = '00005'
                                 and rn.global_value != 'AN04020000'
                               where rn.segmentation_code in ( '00001', '00004')
                                 and trim(rn.global_value) != '998'
                              order by c.id_cto_bdr
                              ) a
                where orden = 1
            ),
 baremos1 as (
             select t.id_cto_bdr, t.segmentation_code, bl.cod_baremo_local, bgl.cod_baremo_global
               from tabla1 t
             left join bi_corp_bdr.baremos_local bl
                 on bl.cod_negocio_local in ('86','116')
                and t.global_value = bl.cod_baremo_alfanumerico_local
             left join bi_corp_bdr.map_baremos_global_local bgl
                 on bl.cod_negocio_local = cast(bgl.cod_negocio_local as string)
                and bl.cod_baremo_local = bgl.cod_baremo_local
             )
insert into table bi_corp_bdr.contr_otros_test_fix
select 'nueva',
       b1.id_cto_bdr,
       lpad(b4.cod_baremo_local,5,'0'),
       lpad(b1.cod_baremo_global,5,'0')
from baremos1 b1 inner join baremos1 b4
	on b1.id_cto_bdr = b4.id_cto_bdr
	    and b1.segmentation_code = '00001'
	    and b4.segmentation_code = '00004';