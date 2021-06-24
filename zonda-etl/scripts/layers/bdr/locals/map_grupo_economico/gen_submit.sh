spark2-submit \
--master yarn \
--deploy-mode cluster \
--queue root.dataeng \
--num-executors 20 \
--executor-cores 4 \
--executor-memory 16G \
--driver-memory 16G \
--conf spark.sql.sources.partitionOverwriteMode="dynamic" \
--conf spark.yarn.maxAppAttempts=1 \
--conf spark.rpc.message.maxSize=1024 \
--conf spark.driver.extraJavaOptions=" -XX:MaxPermSize=256M " \
--conf spark.executor.extraJavaOptions=" -XX:MaxPermSize=256M " \
/aplicaciones/bi/zonda/repositories/zonda-etl/scripts/layers/bdr/locals/map_grupo_economico/generador_map_grupo_economico.py