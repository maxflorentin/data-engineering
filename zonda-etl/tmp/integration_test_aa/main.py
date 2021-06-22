from util_conexiones import conectar_oracle, consulta_base_to_pandas
import json
import pandas as pd

#--------- CARGA PARAMETROS---------
with open("params.json", "r") as read_file:
    param = json.load(read_file)

user = param['params']['user']
password = param['params']['password']
servidor = param['params']['server']
puerto = param['params']['puerto']
base = param['base']

#---------CONEXION--------------------
con = conectar_oracle(user,password,servidor,puerto,base)

#---------CONSULTA A LA TABLA----
query = """SELECT * FROM MCANAL.DIALOGOS_WATSON where rownum < 10"""

df = consulta_base_to_pandas(query,con)
print(df.head())
