import pyspark
strconnection = "mongodb://ZONDA:Hdbur_db647D_c@10.40.0.249:32823,10.40.0.250:32823,10.40.0.251:32823/?replicaSet=rszonda"
conf = pyspark.SparkConf()
conf.set("spark.mongodb.input.uri", strconnection)
conf.set("spark.mongodb.output.uri", strconnection)
spark = pyspark.sql.SparkSession.builder.enableHiveSupport().appName('MongoLoader_testconnection').config(conf=conf).getOrCreate()
df = spark.read.format('com.mongodb.spark.sql.DefaultSource').option("database", "metrics").option("collection", "customer_behaviour_score").load()
df.printSchema()
spark.stop()