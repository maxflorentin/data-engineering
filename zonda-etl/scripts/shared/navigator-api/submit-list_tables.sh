spark2-submit \
--master yarn \
--executor-memory 5G \
--num-executors 2\
--executor-cores 2\
--name list_tables \
--queue root.dataeng \
--deploy-mode cluster \
--conf spark.yarn.maxAppAttempts=1 \
$ZONDA_DIR/repositories/zonda-etl/scripts/shared/navigator-api/list_tables.py


spark2-submit --master yarn --executor-memory 5G --num-executors 2 --executor-cores 2 --name list_tables --queue root.dataeng --deploy-mode cluster --conf spark.yarn.maxAppAttempts=1 $ZONDA_DIR/repositories/zonda-etl/scripts/shared/navigator-api/list_tables.py

spark2-submit \
--master yarn \
--name list_tables \
--executor-memory 5G \
--num-executors 2\
--executor-cores 2\
--queue root.dataeng \
--deploy-mode cluster \
--conf spark.yarn.maxAppAttempts=1 \
/aplicaciones/bi/zonda/tmp/navigator-api/list_tables_v2.py