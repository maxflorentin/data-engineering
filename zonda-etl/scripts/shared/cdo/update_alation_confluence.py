import requests
import json
import pandas as pd
import os
import glob
from datetime import date

from math import floor
from bs4 import BeautifulSoup

ALATION_CLIENT_TOKEN = os.getenv('ALATION_CLIENT_TOKEN')
ALATION_HOST = os.getenv('ALATION_HOST', 'alation.ar.bsch')
CONFLUENCE_HOST = 'confluence.ar.bsch' #os.getenv('CONFLUENCE_HOST', 'confluence.ar.bsch')
ZONDA_DIR = os.getenv('ZONDA_DIR')

def delete_files(path, pattern):
    for f in glob.iglob(os.path.join(path, pattern)):
        try:
            os.remove(f)
        except OSError as exc:
            print(exc)

class Model:
    def __init__(self, n):
        self.name = n
        self.status = 0
        self.num_tables = 0
        self.num_tables_updates = 0
        self.num_tables_not_updates = 0
        self.num_tables_errors = 0
        self.num_tables_empty_field = 0
        self.num_tables_not_documentated = 0
        self.num_tables_not_existed = 0
        self.num_columns = 0
        self.num_columns_empty_field = 0
        self.num_columns_not_documentated = 0
        self.num_columns_not_existed = 0
        self.tables = []
        self.tables_not_exists=[]

    def calculate_statisc(self):
        for table in self.tables:
            if len(table.columns_not_documentated)>0:
                self.num_tables_not_documentated +=1
                self.num_columns_not_documentated += len(table.columns_not_documentated)
                self.status = 1
            if len(table.columns_not_exist)>0:
                self.num_tables_not_existed +=1
                self.num_columns_not_existed += len(table.columns_not_exist)
                self.status = 1

    def update_statisc_alatio(self, errores):
        self.num_tables_updates += errores['updated_objects']
        self.num_tables_not_updates += errores['num_error_objects']

    def create_report(self):
        json={
            'Dominio': self.name,
            'Error': self.status,
            'Tablas':self.num_tables,
            'Tablas no construidas': len(self.tables_not_exists),
            'Tablas mal documentadas': self.num_tables_errors,
            'Tablas actualizadas':self.num_tables_updates,
            'Tablas sin actualizar':self.num_tables_not_updates,
            'Tablas con campo sin documentación': self.num_tables_not_documentated,
            'Tablas con campos vacíos':self.num_tables_empty_field,
            'Tablas con campos sin construir':self.num_tables_not_existed,
            'Datos':self.num_columns,
            'Datos vacíos': self.num_columns_empty_field,
            'Datos sin documentación':self.num_columns_not_documentated,
            'Datos sin construir':self.num_columns_not_existed
        }
        return json

    def create_detail_report(self):
        json ={'Dominio': self.name,
               "Nombre":[],
               "Flag Lake": [],
               "Contiene errores": [],
               "Campos no documentados":[],
               "Campos vacíos":[],
               "Campos no existentes":[]}
        for table in self.tables:
            json["Nombre"].append(table.name)
            json["Flag Lake"].append(table.exist)
            json["Contiene errores"].append(table.error)
            json["Campos no documentados"].append('\n - '.join( [str(x) for x in table.columns_not_documentated]))
            json["Campos vacíos"].append('\n - '.join([str(x) for x in table.columns_empty]) )#Pendiente
            json["Campos no existentes"].append('\n - '.join([str(x) for x in table.columns_not_exist])) # Pendiente
        return json

    def add_tables(self, table):
        self.tables.append(table)
        self.num_tables += 1
        self.num_columns += len(table.columns_confluence)
        if table.error != "":
            self.num_tables_errors += 1
            self.status = 1
        if table.num_columns_empty > 0:
            self.num_tables_empty_field += 1
            self.num_columns_empty_field += table.num_columns_empty
            self.status = 1

class Table:
    def __init__(self):
        self.name ='Dummy'
        self.columns_confluence = []
        self.columns_alation = []
        self.columns_not_documentated =[]
        self.columns_empty = []
        self.columns_not_exist = []
        self.error=''
        self.exist = 0
        self.status = 0
        self.num_columns_empty = 0

    def load(self, nombre_descripcion, webui):
        self.key =  '4.' + nombre_descripcion[0].text.strip()
        self.name = nombre_descripcion[0].text.strip()
        self.title =nombre_descripcion[1].text.strip().split(".")[0].strip()
        self.description =nombre_descripcion[1].text.strip()
        self.CDO_Level_Governance = 'Intermediate'
        self.documentation= '<a href = "' + webui + '" rel = "noopener noreferrer" target = "_blank">Documentación Confluence</a>'

    def add_column_confluence(self, column):
        self.columns_confluence.append(column)
        if column.is_empty == 1:
            self.num_columns_empty += 1
            self.columns_empty.append(column.name)
            self.status = 1

    def comparate_columns(self):
        self.columns_not_documentated = self.columns_alation.copy()
        columns_json =[]
        for column_confluence in self.columns_confluence:
            if column_confluence.name in self.columns_not_documentated:
                self.columns_not_documentated.remove(column_confluence.name)
                column_confluence.existed=1
                columns_json.append(column_confluence.to_json_load())
            else:
                self.columns_not_exist.append(column_confluence.name)
                self.status = 1
        return '\n'.join([json.dumps(x) for x in columns_json])

    def to_json_load(self):
        json={
            'key':self.key,
            'title':self.title,
            'description':self.description,
            'CDO Level Governance':self.CDO_Level_Governance,
            'Documentation':self.documentation
        }
        return json

class Column:
    def __init__(self, key, nombre, descripcion, data_type, documentated):
        self.key = key + '.' + nombre
        self.name = nombre
        self.title = descripcion.split(".")[0].strip().split(":")[0].strip()
        self.description = descripcion
        self.data_type = data_type
        self.governance ='Intermediate'
        self.documentated=documentated
        self.is_empty = 1 if descripcion=="" else 0
        existed=0

    def to_json_load(self):
        json={
            'key':self.key,
            'title':self.title,
            'description':self.description,
            'CDO Level Governance':self.governance
        }
        return json

# Obtengo la información de las columnas de una tabla
def get_columns_alation(base_url, headers, table):
    table_info = []
    try:
        response = requests.get(base_url+"/integration/v1/column/?ds_id=4&deleted=0&table_name="+table , headers = headers)
        columns = json.loads(response.text)
        for column in columns:
            if column['table_name']==table:
                table_info.append(column['name'])
        return table_info
    except:
        print("An exception occurred getting information about columns of "+table)
        return []

def call_api_confluence(url):
    response={}
    try:
        base_url = 'https://'+CONFLUENCE_HOST+'/rest/api/content/' +url
        headers = {"Accept": "application/json"}
        response_api = requests.request("GET",base_url,headers=headers)
        response = json.loads(response_api.text)
    except:
        print("Error calling Confluence API")
    return response

def data_confluence():
    # Obtengo los datos del espacio de Confluence buscando los hijos, usando childer.page.childer.page
    modelos=[]
    modelos_confluence = call_api_confluence("11504225/child/page?limit=70")
    if bool(modelos_confluence):
        for modelo in  modelos_confluence['results']:
            if modelo['title'] !='Anexo':#in ('Cuentas','Caja de Seguridad','Garantías','Plazos Fijos'): #in ('Cuentas'):#
                modelo_analizar = Model(modelo['title'])
                modelo_confluence = call_api_confluence(modelo['id'] + "/child/page?expand=children.page.children.page&limit=70")
                analys_child(modelo_confluence['results'], modelo_analizar)
                modelos.append(modelo_analizar)
    return modelos


#Itera sobre cada hijo para ir viendo si es un dominio o debe ir a un segundo paso
def analys_child(children, modelo_analizar):
    #Itero por cada Hijo, si un hijo tiene mas hijo accedo al hijo siguiente y extraigo la información
    tables = []
    #La primera validara si tiene hijos directos, después vemos si anida
    for child in children:
        if 'children' in child and len(child['children']['page']['results'])>0:
            analys_child(child['children']['page']['results'], modelo_analizar)
        elif not(modelo_analizar.name =='Transferencias' and child['title']  in ('ATRC', 'ABAE', 'MALBGC','Diagrama funcional') ):
            tablas_confluence = call_api_confluence(child['id'] + '?expand=body.storage')
            # Parseo el HTML
            table = parse_html(tablas_confluence['body']['storage']['value'], 'https://'+CONFLUENCE_HOST+child['_links']['webui'])
            modelo_analizar.add_tables(table)

def parse_html(html_body, webui):
    # Analizo el AHTML
    soup = BeautifulSoup(html_body, 'html')
    encabezado = 1
    ##Obtengo el nombre y la descripcion validando que al menos tengo dos bullets, puede haber mas en las descripciones de los campos)
    nombre_descripcion = soup.findAll('ul')
    table_confluence = Table()
    if len(nombre_descripcion) > 1:
        table_confluence.load(nombre_descripcion, webui)
        # Debe tener dos tablas, una de origen y otra de la tabla
        table = soup.findAll("table")
        if len(table) in [1, 2]:
            #Valido si tiene tablas de predecesora
            if len(table) == 2:
                tr_table = table[1].findAll("tr")
            else:
                tr_table = table[0].findAll("tr")
            #Recorro las columnas de la tabla
            for row in tr_table:
                cells = row.findAll("td")
                if encabezado == 1:
                    encabezado = 0
                else:
                    #Si la tabla contiene separado la longitud o no
                    if len(cells) == 3 or len(cells) == 4:
                        indice = 2 if len(cells) == 3 else 3
                        # Validamos si tiene lista
                        if '<li>' in str(cells[indice]):
                            descripcion = str(cells[indice]).replace('<td>', '').replace('</td>', '').replace('<td colspan=\"1\">', '').strip()
                        else:
                            descripcion = cells[indice].text.strip()
                        column_confluence = Column(table_confluence.key, cells[0].text.strip(), descripcion, cells[1].text.strip(), 1)
                        table_confluence.add_column_confluence(column_confluence)
        else:
            table_confluence.error ='No respeta el format'
    else:
        table_confluence.error = 'No respeta el format'
    return table_confluence

def alation_api_upload( base_url, headers, table, data):
    json_table = table.to_json_load()
    resultado={'number_received': 0, 'updated_objects': 0, 'num_error_objects':0,'error_objects': [],
               'number_received_c': 0, 'updated_objects_c': 0, 'num_error_objects_c':0,'error_objects_c': []}
    try:
        response = requests.post(base_url + '/api/v1/bulk_metadata/custom_fields/default/table?replace_values=true', json=json_table, headers=headers)
        result = json.loads(response.text)
        resultado['number_received'] += result['number_received']
        resultado['updated_objects'] += result['updated_objects']
        resultado['num_error_objects'] += len(result['error_objects'])
        resultado['error_objects'].append(result['error_objects'])
        try:
            response = requests.post(base_url + '/api/v1/bulk_metadata/custom_fields/default/attribute?replace_values=true', data=data, headers=headers)
            result = json.loads(response.text)
            resultado['number_received_c'] += result['number_received']
            resultado['updated_objects_c'] += result['updated_objects']
            resultado['num_error_objects_c'] += len(result['error_objects'])
            resultado['error_objects_c'].append(result['error_objects'])
        except:
            print("An exception occurred setting column values in ")
    except:
        print("An exception occurred setting table values")
    return resultado


def load_alation(modelo_documentado):
    headers = {'Token': ALATION_CLIENT_TOKEN}
    base_url = "https://"+ALATION_HOST
    for modelo in modelo_documentado:
        for tabla in modelo.tables:
            if tabla.error == "":
                json_column = get_columns_alation(base_url, headers, tabla.name)
                if len(json_column) > 0:
                    tabla.columns_alation = json_column
                    tabla.exist = 1
                    columns_json = tabla.comparate_columns()
                    results = alation_api_upload( base_url, headers, tabla, columns_json)
                    modelo.update_statisc_alatio(results)
                else:
                    tabla.exist = 0
                    modelo.tables_not_exists.append(tabla.name)
        modelo.calculate_statisc()

def final_report(modelos):
    url ='/aplicaciones/bi/zonda/outbound/'
    file =  date.today().strftime("%Y%m%d")+'_resumen.xlsx'
    delete_files(url, file)
    writer = pd.ExcelWriter(url+file, engine='xlsxwriter')

    json = {
        'Dominio': [],
        'Error': [],
        'Tablas': [],
        'Tablas actualizadas': [],
        'Tablas sin actualizar': [],
        'Tablas no construidas': [],
        'Tablas mal documentadas': [],
        'Tablas con campo sin documentación': [],
        'Tablas con campos vacíos': [],
        'Tablas con campos sin construir': [],
        'Datos':[],
        'Datos vacíos': [],
        'Datos sin documentación': [],
        'Datos sin construir': []
    }

    for modelo in modelos:
        reporte_modelo = modelo.create_report()
        for key in json:
            json[key].append(reporte_modelo[key])

    df = pd.DataFrame.from_dict(json)
    df.to_excel(writer, encoding='utf-8-sig',sheet_name='Resumen')

    for modelo in modelos:
        df_modelo = pd.DataFrame.from_dict(modelo.create_detail_report())
        df_modelo.to_excel(writer, encoding='utf-8-sig', sheet_name=modelo.name)

    writer.save()

# Press the green button in the gutter to run the script.
if __name__ == '__main__':
    modelo_documentado = data_confluence()
    load_alation(modelo_documentado)
    final_report(modelo_documentado)