from pyspark import SparkContext, SQLContext, SparkConf
from pyspark.sql import SparkSession, Row, HiveContext

class Spark_connector:
    
    def __init__(self):
        self.config = SparkContext._active_spark_context.getConf() \
                if SparkContext._active_spark_context is not None else None
        self.context = SparkContext._active_spark_context
        self.session = None
    
    def crear_conexion(self, master = 'yarn-client' ):
        if self.conexion_existente():
            
            self.terminar_conexion()
            
        self.config = SparkConf().set('spark.kryoserializer.buffer.max',400)\
                .set('spark.executor.memory', '8g')\
                .set('spark.rpc.message.maxSize',512)\
                .set('spark.yarn.queue', 'root.cdsw')

        self.context = SparkContext(master=master,conf=self.config).getOrCreate()
        
        self.session = SparkSession.builder\
        .config("spark.hadoop.hive.exec.dynamic.partition", "true") \
        .config("spark.hadoop.hive.exec.dynamic.partition.mode", "nonstrict") \
        .enableHiveSupport().getOrCreate()
        return self
        
    def conexion_existente(self):
        return self.context is not None
    
    def terminar_conexion(self):
        self.session.stop() if (self.session is not None) else None 
        self.context.stop()
        return self
    
    def exec_query(self, query, to_pandas=True):
        df = self.session.sql(query)
        if to_pandas:
            df = df.toPandas()
        
        return df
    
    
        
        