CREATE TABLE RVD.RPT_D_LOCALIDAD (
	LOCALIDADSID NUMERIC(4,0),
	LOCALIDADCOD NUMERIC(5,0),
	PROVINCIASID NUMERIC(3,0),
	DESCRIPCION VARCHAR2(50),
	CODIGOPOSTAL NUMERIC(4,0),
	ACTIVO CHAR(1));
	
	
	
	
sqoop import -Dmapreduce.job.queuename=root.dataeng  --connect 'jdbc:oracle:thin:@dblxorabcatel01:7366/RIO44'  --username 'SRVCBI' --table "RVD.RPT_D_LOCALIDAD"  --bindir '/aplicaciones/bi/zonda/sqoop/rio44/RPT_D_LOCALIDAD' --target-dir  '/santander/bi-corp/landing/rio44/dim/rpt_d_localidad/partition_date=2020-12-10' --delete-target-dir --fields-terminated-by '^' --num-mappers 1 --password 'RNa3IixU2i3T2VY1gUkW'  --verbose


spark2-submit --master yarn --conf spark.yarn.appMasterEnv.partition_date=2020-12-10 --conf spark.sql.sources.partitionOverwriteMode=dynamic --files /aplicaciones/bi/zonda/repositories/zonda-etl/scripts/layers/staging/rio44/dim/rpt_d_localidad/rpt_d_localidad_schema.json --py-files /aplicaciones/bi/zonda/repositories/guyra/src/config_file.py --num-executors 8 --executor-cores 4 --executor-memory 4G --name STAGING_RPT_D_LOCALIDAD_Parquetize --verbose --queue root.dataeng --deploy-mode cluster /aplicaciones/bi/zonda/repositories/guyra/src/guyra.py rpt_d_localidad_schema.json
	