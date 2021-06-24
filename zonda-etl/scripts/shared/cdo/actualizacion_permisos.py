import pandas as pd
from datetime import date
import numpy as np
import glob
import os

ZONDA_DIR = os.getenv('ZONDA_DIR')

def delete_files(path, pattern):
    for f in glob.iglob(os.path.join(path, pattern)):
        try:
            os.remove(f)
        except OSError as exc:
            print(exc)

if __name__ == '__main__':
    url = ZONDA_DIR + '/outbound/'
    #Borro los ficheros anteriores
    delete_files(url, '[0-9]*_actualizacion_perfilado*')
    #Cargo la info
    d1 = date.today().strftime("%Y%m%d")
    tarea = "PLAN DE PRUEBAS:\n" + "\tSe verificará con los usuarios si tienen permiso de lectura sobre las bases indicadas\n\n" + "PLAN DE VUELTA ATRÁS:\n" +  "\tVolver atras los cambios solicitados. Cualquier duda contactar a CIFUENTES, JUAN CARLOS (int 16186)\n\n" + "PLAN DE INSTALACION:\n" + " \tTAREA1 - Asignación permisos Sentry (Seg.Inf UNIX-ORACLE)\n" + "\t\tA. Solicitamos agregar los siguientes permisos en Sentry en PROD de acuerdo al archivo " + d1 + "_actualizacion_perfilado.xlsx:\n"
    # Creare un Excel del Panda
    writer = pd.ExcelWriter(url+d1+"_actualizacion_perfilado.xlsx",engine='xlsxwriter')
    # Obtengo los datos
    columns = pd.read_table(url +'hive_metastore_report_'+d1+'.txt', header=0, sep="|")
    #Obtengo las vistas ya que no se puede aplicar el criterio de seguridad por columna
    views = columns.loc[ columns['_u1.tbl_type'].astype(str).str.contains('VIEW'),['_u1.name','_u1.tbl_name','_u1.column_name','_u1.motivo']]
    #Obtengo las columnas no sensibles
    columns = columns.loc[ (columns['_u1.motivo'] == 'otro') & (~columns['_u1.tbl_type'].astype(str).str.contains('VIEW')),['_u1.name','_u1.tbl_name','_u1.column_name']]
    columns = columns.rename(columns={"_u1.name": "nombre_schema", "_u1.tbl_name": "nombre_tabla", "_u1.column_name": "nombre_columnas"})
    columns['key'] = columns['nombre_schema'] + '.' + columns['nombre_tabla'] + '.' + columns['nombre_columnas']
    columns = columns[['key', 'nombre_schema', 'nombre_tabla', 'nombre_columnas']]
    #Escribo en un excel por pestaña las columnas que debo ejecutar
    perfiles ={'GRPDLBICORPSTAGING':'bi_corp_staging',
               'GRPDLBICORPCOMMON':'bi_corp_common',
               'GRPDLBICORPBUSINESS':'bi_corp_business',
               'GRPDLBICORPBDR':'bi_corp_bdr',
               'GRPDLBUSNIESSARDA':'santander_business_risk_arda',
               'GRPDLBUSNIESSIFRS9':'santander_business_risk_ifrs9',
               'GRPDLBCCARGABAL': 'bi_corp_nuevo_cargabal'
               }
    for key, value in perfiles.items():
        pd_perfil = columns.loc[columns['nombre_schema']==value]
        if len(pd_perfil.index) != 0:
            pd_perfil.to_excel(writer, index=False, sheet_name=key)
            tarea += "\t\t\t- Al grupo " +key +"  -> Pestaña " + key +'\n'
    # Close the Pandas Excel writer and output the Excel file.
    writer.save()
    #Proceso las vistas
    views['sensitive'] = np.where(views["_u1.motivo"]=='otro', 0, 1)
    resumen_views = views.groupby(by=["_u1.name","_u1.tbl_name"]).agg({'_u1.column_name':'count','sensitive':'sum'})
    #Valido las vistas que no son sensibles
    index_no_sensitive = list(resumen_views.loc[resumen_views['sensitive'] == 0].index)
    if(len(index_no_sensitive)>0):
        index_no_sensitive.sort(key=lambda tup: tup[1])
        tarea += "\t\tB. Solicitamos agregar los siguientes permisos sobre toda la vista en Sentry en PROD para los grupos AD:"
        perfil = ""
        for i in index_no_sensitive:
            if perfil != i[0]:
                perfil = i[0]
                group = [key for key, value in perfiles.items() if value == i[0]]
                tarea += "\n\t\t\t- Al grupo "+group[0]+":"
            tarea +="\n\t\t\t\t- "+i[0]+'.'+i[1]
    # Valido las vistas que son sensibles
    index_sensitive = list(resumen_views.loc[resumen_views['sensitive'] > 0].index)
    if (len(index_sensitive) > 0):
        index_sensitive.sort(key=lambda tup: tup[1])
        tarea += "\nVistas con datos sensibles"
        for i in index_sensitive:
            tarea += "\n\t- " + i[0] + '.' + i[1]
    #Escribo el texto para el CRQ
    f = open(url+d1+"_actualizacion_perfilado_instrucciones.txt", "a")
    f.write(tarea)
    f.close()