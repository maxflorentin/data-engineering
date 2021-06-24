import cx_Oracle
import pandas as pd

def conectar_oracle(base,param):
  
  user = param[base]['user']
  password = param[base]['password']
  server = param[base]['server']
  puerto = param[base]['puerto']
  
  conf = (str(user) + '/' + str(password) + str(server) + ':' +
          str(puerto) +  '/' + str(base))
  con = cx_Oracle.connect(conf)
  return con
  
def consulta_base_to_pandas(query,con):
  
  cur = con.cursor()  
  consulta = cur.execute(query)
  des = consulta.description
  datos = consulta.fetchall()
  df = pd.DataFrame.from_records(datos)
  df.columns = [i[0] for i in des]
  cur.close()
  return df