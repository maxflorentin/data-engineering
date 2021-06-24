CREATE EXTERNAL TABLE IF NOT EXISTS bi_corp_staging.si02_swift_msg_queue(
KEY               	STRING,
SWIFT_MSG         	STRING,
QUEUE             	STRING,
STATE             	STRING,
SEQ               	STRING,
DEST              	STRING,
URGENT            	STRING,
SUBMISSION_KEY    	STRING,
SPAIN_DATE        	STRING,
SWIFT_SESSION     	STRING,
SWIFT_ISN         	STRING,
UNDEL_CODE        	STRING,
VERSION           	STRING,
C_USER            	STRING,
M_USER            	STRING,
C_DATE            	STRING,
M_DATE            	STRING,
S_USER            	STRING,
S_DATE            	STRING,
L_USER            	STRING,
INCOMING_KEY      	STRING,
PRINTED           	STRING,
FKSWFMSGBX        	STRING,
FKDISTLIST        	STRING,
PK                	STRING,
FK_PARENT         	STRING,
FK_OWNER_OBJ      	STRING,
FK_EXTENSION      	STRING,
FKCHANNELX        	STRING,
GPI               	STRING,
GUID              	STRING,
FIELD_119         	STRING,
MT_FONDOS         	STRING
)
PARTITIONED BY (SWIFT_DATE string)
ROW FORMAT SERDE 'org.apache.hadoop.hive.ql.io.parquet.serde.ParquetHiveSerDe'
STORED AS INPUTFORMAT 'org.apache.hadoop.hive.ql.io.parquet.MapredParquetInputFormat'
OUTPUTFORMAT 'org.apache.hadoop.hive.ql.io.parquet.MapredParquetOutputFormat'
LOCATION '${DATA_LAKE_SERVER}/santander/bi-corp/staging/si02/fact/swift_msg_queue';