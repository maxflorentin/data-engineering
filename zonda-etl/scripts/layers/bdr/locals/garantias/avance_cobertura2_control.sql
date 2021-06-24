INVALIDATE METADATA bi_corp_bdr.aux_garant_garantias_contratos_div;

/* CONTROL sobre una persona en particular */
SELECT  stage,num_persona ,id_cto_div,orden_contrato ,imp_deuda , orden_garantia ,tip_cobertur ,num_garantia ,orden_prelacion ,imp_total_garantia_pesos , nominal_cobertura_actual 
	, imp_deuda_remanente ,imp_total_garantia_pesos_remantente , porc_cobertura_actual,porc_reparto,nominal_cobertura_inicial,porc_cobertura_inicial
FROM bi_corp_bdr.aux_garant_garantias_contratos_div gc 
WHERE partition_date ='2020-06-30' and num_persona = '02765272'
ORDER BY num_persona ,orden_contrato asc, orden_garantia asc;

/* CONTROL DE CUANTAS GARANTIAS GENERICAS Y CTOS por PERSONA EXISTEN */
SELECT g_rema.num_persona, imp_gara, cant_gara,cant_cto, imp_deuda, IF((imp_deuda-imp_gara)<0,'cobertura_total' ,'deuda_remanente') as estado
FROM 
					(
					SELECT ge2.num_persona, SUM(imp_limite_pesos) as imp_gara, count(1) as cant_gara
					FROM bi_corp_bdr.aux_garant_garantias_genericas ge2
					WHERE ge2.partition_date = '2020-02-28'
					--and ge2.num_persona ='70516920'
					group by num_persona 
					) g_rema
			INNER JOIN 
				(
				SELECT num_persona,SUM(imp_deuda) as imp_deuda, count(1) as cant_cto
				FROM bi_corp_bdr.aux_garant_contratos_div  cto  
				--where num_persona ='70516920'
				group by num_persona
				) c_rema
		ON (g_rema.num_persona = c_rema.num_persona )
ORDER bY cant_cto desc;            


refresh bi_corp_bdr.aux_garant_garantias_contratos_div;	
INVALIDATE METADATA bi_corp_bdr.aux_garant_garantias_contratos_div;	
--COMPUTE INCREMENTAL STATS bi_corp_bdr.aux_garant_garantias_contratos_div PARTITION (partition_date='2020-00-00');	

/* CONTROL DE CUANTAS GARANTIAS Y CTOS REMANENTES VAN QUEDANDO A TRAVES DE LAS EJECUCIONES por GARANTIAS*/
SELECT g_rema.stage, g_rema.num_persona, cant_gara, garantia_remanente,cant_cto , DEUDA, if(garantia_remanente > deuda, 1,0) as cobertura_total
FROM 
	(
			/* GARANTIAS AUN REMANENTES : la primer garantia disponible de cada persona*/
					SELECT partition_date,num_persona,COUNT(DISTINCT num_garantia) cant_gara,   sum(imp_total_garantia_pesos_remantente) as garantia_remanente, max(max_stage) as stage
					FROM 
						(
						SELECT  gen_rema.*, rank() over (partition by num_persona order by orden_garantia asc) as ranking
						FROM (
							SELECT partition_date,stage,orden_contrato ,num_persona ,orden_garantia,num_garantia,imp_total_garantia, imp_total_garantia_pesos
										, gc.imp_total_garantia_pesos_remantente
										, max(orden_contrato ) over (partition by num_persona, num_garantia ) as max_orden_contrato
										, max(stage) over (PARTITION by partition_date ) as max_stage
							FROM bi_corp_bdr.aux_garant_garantias_contratos_div gc 
							WHERE partition_date ='2020-06-30' 
							and tip_cobertur ='002' --and imp_total_garantia_pesos_remantente >0
							--and num_persona = '02765272'			
							) gen_rema
						WHERE 1=1
							and imp_total_garantia_pesos_remantente >0
							and  max_orden_contrato = orden_contrato 
						) prox_gara
						GROUP BY partition_date,num_persona
	) g_rema
INNER JOIN 
		(
			/* PROXIMOS CONTRATOS AUN POR CUBRIR */
				SELECT  cto.partition_date,cto.num_persona,COUNT(DISTINCT cto.id_cto_div) cant_cto,   sum(CTO_REMA.deuda_remanente) as deuda
				FROM bi_corp_bdr.aux_garant_contratos_div  cto 
				LEFT OUTER JOIN 
						(
						SELECT partition_date,num_persona ,id_cto_div,max(orden_contrato ) as max_orden, MIN(imp_deuda_remanente ) as deuda_remanente
							FROM bi_corp_bdr.aux_garant_garantias_contratos_div gc 
							WHERE partition_date ='2020-06-30' 
							--and num_persona ='02765272'
							group by partition_date,num_persona,id_cto_div --having deuda_remanente > 0 			
							) CTO_REMA -- siguiente orden_cto del ultimo cargado con garantias_genericas
						ON (cto.partition_date = CTO_REMA.partition_date and cto.num_persona = CTO_REMA.num_persona and cto.id_cto_div = CTO_REMA.id_cto_div)
				WHERE cto.partition_date = '2020-06-30'
				and NVL(CTO_REMA.deuda_remanente,1) > 0
				--and cto.num_persona ='02765272'		
				group by  cto.partition_date,cto.num_persona
		) c_rema
ON (g_rema.num_persona = c_rema.num_persona )
--WHERE g_rema.orden_contrato = c_rema.actual_orden
ORDER BY stage desc, cant_gara desc;

/* CONTROL DE AVANCE DE COBERTURA en LOOP por STAGE*/
SELECT stage, count(1) as cant , sum(nominal_cobertura_actual) as nominal_cobertura_actual
from bi_corp_bdr.aux_garant_garantias_contratos_div 
where partition_date = '2020-06-30' and nominal_cobertura_actual>0
group by stage 
order by stage asc;