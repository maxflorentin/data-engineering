echo "---STARTING LOCAL TABLES---"
#echo "Loading marca_utp_clientes"
#airflow variables --import /aplicaciones/bi/zonda/repositories/zonda-etl/scripts/layers/bdr/locals/marca_utp_clientes/variables/MARCA_UTP_CLIENTES_$1.json
#echo "Loading normalizacion_id_clientes"
#airflow variables --import /aplicaciones/bi/zonda/repositories/zonda-etl/scripts/layers/bdr/locals/normalizacion_id_clientes/variables/NORMALIZACION_ID_CLIENTES_$1.json
#echo "Loading normalizacion_id_contratos"
#airflow variables --import /aplicaciones/bi/zonda/repositories/zonda-etl/scripts/layers/bdr/locals/normalizacion_id_contratos/variables/NORMALIZACION_ID_CONTRATOS_$1.json

echo "--STARTING CORPORATE TABLES--"
#echo "Loading JM_CONTR_BIS"
#airflow variables --import /aplicaciones/bi/zonda/repositories/zonda-etl/scripts/layers/bdr/corporate/JM_CONTR_BIS/variables/JM_CONTR_BIS_$1.json
#echo "Loading JM_CONTR_OTROS"
#airflow variables --import /aplicaciones/bi/zonda/repositories/zonda-etl/scripts/layers/bdr/corporate/JM_CONTR_OTROS/variables/JM_CONTR_OTROS_$1.json
echo "Loading JM_CTOS_CANCE"
airflow variables --import /aplicaciones/bi/zonda/repositories/zonda-etl/scripts/layers/bdr/corporate/JM_CTOS_CANCE/variables/JM_CTOS_CANCE_$1.json
#echo "Loading JM_INTERV_CTO"
#airflow variables --import /aplicaciones/bi/zonda/repositories/zonda-etl/scripts/layers/bdr/corporate/JM_INTERV_CTO/variables/JM_INTERV_CTO_$1.json
echo "Loading JM_POSIC_CONTR"
airflow variables --import /aplicaciones/bi/zonda/repositories/zonda-etl/scripts/layers/bdr/corporate/JM_POSIC_CONTR/variables/JM_POSIC_CONTR_$1.json
#airflow variables --import /aplicaciones/bi/zonda/repositories/zonda-etl/scripts/layers/bdr/corporate/JM_POSIC_CONTR/variables/JM_POSIC_CONTR_RECLA_$1.json
echo "Loading JM_TRZ_CONT_REN"
airflow variables --import /aplicaciones/bi/zonda/repositories/zonda-etl/scripts/layers/bdr/corporate/JM_TRZ_CONT_REN/variables/JM_TRZ_CONT_REN_$1.json
echo "Loading JM_CLIENT_BII"
airflow variables --import /aplicaciones/bi/zonda/repositories/zonda-etl/scripts/layers/bdr/corporate/JM_CLIENT_BII/variables/JM_CLIENT_BII_$1.json
#echo "Loading JM_FLUJOS"
#airflow variables --import /aplicaciones/bi/zonda/repositories/zonda-etl/scripts/layers/bdr/corporate/JM_FLUJOS/variables/JM_FLUJOS_$1.json
#echo "Loading JM_EXPOS_NO_CON"
#airflow variables --import /aplicaciones/bi/zonda/repositories/zonda-etl/scripts/layers/bdr/corporate/JM_EXPOS_NO_CON/variables/JM_EXPOS_NO_CON_$1.json
#echo "Loading JM_VTA_CARTER"
#airflow variables --import /aplicaciones/bi/zonda/repositories/zonda-etl/scripts/layers/bdr/corporate/JM_VTA_CARTER/variables/JM_VTA_CARTER_$1.json
#echo "Loading JM_CLIEN_JURI"
#airflow variables --import /aplicaciones/bi/zonda/repositories/zonda-etl/scripts/layers/bdr/corporate/JM_CLIEN_JURI/variables/JM_CLIEN_JURI_$1.json
#echo "Loading JM_GRUP_RELA"
#airflow variables --import /aplicaciones/bi/zonda/repositories/zonda-etl/scripts/layers/bdr/corporate/JM_GRUP_RELA/variables/JM_GRUP_RELA_$1.json
#echo "Loading JM_GRUP_ECONOs"
#airflow variables --import /aplicaciones/bi/zonda/repositories/zonda-etl/scripts/layers/bdr/corporate/JM_GRUP_ECONO/variables/JM_GRUP_ECONO_$1.json
echo "Loading JM_CAL_IN_CL"
airflow variables --import /aplicaciones/bi/zonda/repositories/zonda-etl/scripts/layers/bdr/corporate/JM_CAL_IN_CL/variables/JM_CAL_IN_CL_$1.json
#echo "Loading JM_INF_AD_CLI"
#airflow variables --import /aplicaciones/bi/zonda/repositories/zonda-etl/scripts/layers/bdr/corporate/JM_INF_AD_CLI/variables/JM_INF_AD_CLI_$1.json