import datetime
import argparse
import json
import os

ZONDA_DIR = 'C:/Users/a309423/Documents/zonda-etl'  # os.getenv('ZONDA_DIR')

path_files = ZONDA_DIR +  "/scripts/shared/z_init_cm/config/"   # "/repositories/zonda-etl/scripts/shared/z_init_cm/config/"
for filename in os.listdir(path_files):
    if filename.endswith(".json"):
        json_file = path_files + filename

        try:
            with open(json_file, 'r') as f:
                zonda_dependencies_config = json.load(f)
                print("Json Config Readed: "+zonda_dependencies_config["name"])
                # print(zonda_dependencies_config)  # comentar!
        except:
            print("#############################################")
            print("Error al leer archivo")
            print("#############################################")
        continue

start_date = datetime.datetime.now().date()
#start_date = execution_date.strftime('%Y-%m-%d')

print(start_date)

'''
    #Funcion utilizada para verificar el resultado de ejecucion de un dag o una tarea en airflow
    #Los parametros de entrada requeridos son el dag_id, la fecha de ejecucion a controlar "execution_date" y el nombre de las tareas que se deben verificar
    @provide_session
    def dag_status(dag_id, date_from, tasks, start_date, session=None):

        #En el caso de que en la clave "task" se encuentre una lista se incorpora un linea sobre la clausela "where" para filtrar por las tareas indicadas
        #En el caso de que la clave "task" se encuentre con el valor "all" la instruccion anterior se omite y en consecuencia se validan todas las tareas del dag_id

        if tasks == ['all']:
            tasks_conditions = ''
        else:
            tasks_conditions = 'and TI.task_id in ('+str(tasks).replace("[","").replace("]","")+')'

        date_from_enc = date_from.strftime('%Y-%m-%d').encode('utf-8').hex()

        print("tasks: {}".format(tasks))
        tasks_query = "SELECT row_to_json(json) as row FROM (select TI.dag_id, TI.task_id, TI.execution_date, TI.state, TI.start_date from dag_run DR join task_instance TI on TI.dag_id = DR.dag_id and CAST(DR.execution_date as date) = CAST(TI.execution_date as date) where DR.dag_id = '{}' and ( encode(DR.conf, 'hex') like '%{}%' or cast(DR.start_date as date) = '{}') {} ) json"
        tasks_formatted = tasks_query.format(dag_id, date_from_enc, start_date, tasks_conditions)
        print("tasks query: {}".format(tasks_formatted))
        tasks_result = session.execute(tasks_formatted)

        #Se apenda sobre una lista el resultado de ejecucion de las tareas del dag
        status = []
        for row in tasks_result:
            state = row[0]['state']
            status.append(state)

        dag_query = "SELECT row_to_json(json) as row FROM (select state from dag_run where dag_id = '{}' and ( encode(conf, 'hex') LIKE '%{}%' or cast(start_date as date) = '{}' ) and state = 'success' limit 1) json"
        dag_formatted = dag_query.format(dag_id, date_from_enc, start_date)
        print("dag query: {}".format(dag_formatted))
        dag_result = session.execute(dag_formatted)

        #Se apenda sobre la lista de tareas, el resultado de ejecucion del dag completo
        if tasks == 'all':
        print("Se evalúa la ejecución del DAG en su totalidad")
        for row in dag_result:
            state = row[0]['state']
            if state == 'running':
            status.append(state)
        else:
        print("Se evalúa la ejecución de las tareas especificadas")

        #En el caso de que se encuentre alguna de las tareas en "failed" se llena el valor de la variable task_result en "failed"
        #Caso contrario se determinar que la ejecucion del dag/tareas fue correcta
        task_result = []
        if 'failed' in status:
        result = {
            "dag_id":dag_id,
            "tasks":tasks,
            "status": "failed"
        }
        task_result.append(result)
        elif 'running' in status:
        result = {
            "dag_id":dag_id,
            "tasks":tasks,
            "status": "running"
        }
        task_result.append(result)
        elif len(status) == 0 :
        result = {
            "dag_id":dag_id,
            "tasks":tasks,
            "status": "empty"
        }
        task_result.append(result)
        else:
            stat = 'success'
            for element in status:
                if element != 'success':
                    stat = 'not success'

            if stat == 'success':
                result = {
                "dag_id":dag_id,
                "tasks":tasks,
                "status": "success"
                }
                task_result.append(result)
            else:
                result = {
                "dag_id":dag_id,
                "tasks":tasks,
                "status": "undefined"
                }
                task_result.append(result)
        print("Evaluacion de condiciones: "+str(task_result))
        return task_result

    #Funcion utilizada para recuperar la ultima fecha de ejecucion de un DAG en Airflow
    #Parametros de entrada requeridos "dag_id"
    @provide_session
    def dag_last_execution(dag_id, date_from, session=None):
        date_from_enc = date_from.strftime('%Y-%m-%d').encode('utf-8').hex()
        query = "select row_to_json(a) as row from (select cast(start_date as date) as start_date, cast(execution_date as date) as execution_date, id from dag_run where dag_id = '{}' and  encode(conf, 'hex') LIKE '%{}%' limit 1) a"
        query_formatted = query.format(dag_id, date_from_enc)
        result_set = session.execute(query_formatted)

        last_execution = []
        if result_set is not None:
        for row in result_set:
            last_start_date = row[0]['start_date']
            last_start_date = datetime.datetime.strptime(last_start_date,'%Y-%m-%d').date()
            last_execution.append(last_start_date)

            last_execution_date = row[0]['execution_date']
            last_execution.append(last_execution_date)

            id = row[0]['id']
            #Se intenta recuperar "date_From" si no se envia ultima fecha de ejecucion
            try:
                conf = session.query(DagRun.conf).filter(DagRun.id == id).first()
                conf = conf[0]['date_from']
                print("conf (date from): {} dag_id: {}".format(conf, dag_id))
                last_execution.append(conf)
            except:
                conf = last_execution_date
                print("conf (execution_date): {} dag_id: {}".format(conf, dag_id))
                last_execution.append(conf)

            print("last_execution: {}".format(last_execution))

        return last_execution


    #se setea la fecha de ejecucion de acuerdo a lo indicado en la clave "shift"

    start_date = datetime.datetime.now().date()
    execution_date = datetime.datetime.now().date() + datetime.timedelta(days=zonda_dependencies_config["shift"])

    # execution_date = datetime.datetime.strptime(start_date,'%Y-%m-%d').date() + datetime.timedelta(days=zonda_dependencies_config["shift"])
    print("Fecha de ejecucion: "+str(execution_date))
    #se verifia el cumplimiento de las condiciones
    condition_status = []

    for condition in zonda_dependencies_config['conditions']:
        dag_id = condition["dag_id"]
        tasks = condition["tasks"]
        if not isinstance(tasks, list):
        tasks = [tasks]

        date_shift = execution_date
        if "shift" in condition:
        date_shift = execution_date + datetime.timedelta(days=condition["shift"])

        result = dag_status(dag_id, date_shift, tasks, start_date)
        condition_status.append(result[0])
        # date_from = dag_last_execution(dag_id)

    #Si se cumplen las condiciones de la iteracion se añaden los dag/tareas a ejecutar
    to_execute = []

    status = 'success'
    for value in condition_status:
        if value['status'] != 'success':
            status = 'failed'

    if status == 'success':
        for dag in zonda_dependencies_config['trigger']:
            dag_id = dag["dag_id"]
            date_shift = execution_date
            if "shift" in str(zonda_dependencies_config["shift"]):
            date_shift = execution_date + datetime.timedelta(days=zonda_dependencies_config["shift"])
            last_dates = dag_last_execution(dag_id, date_shift)
            if not last_dates:
            response = {
                "name": zonda_dependencies_config["name"],
                "dag_id":dag_id,
                "status":"Dag añadido a la lista de ejecucion"
            }
            to_execute.append(dag)
            print(response)

            else:
            last_execution = last_dates[2]
            print("last_dates: {}".format(date_shift))
            print("comparing: {} with: {}".format(last_execution, str(execution_date)))
            response = {
                "name": zonda_dependencies_config["name"],
                "dag_id":dag_id,
                "last_start_date":last_execution,
                "start_date": execution_date.strftime('%Y-%m-%d'),
                "status":"Fecha de ultima ejecucion mayor o igual a fecha de ejecucion proporcionada"
            }
            print(response)
            #Se incorporan los datos del DAG objetivo a la lista de enviados a ejecutar

    #Si no se cumplen las condiciones se devuelve respuesta indicando el motivo
    else:
        for dag in condition_status:
        if dag["status"] != "success":
            dag_failed = dag["dag_id"]

        response = {
            "name": zonda_dependencies_config["name"],
            "dag_id": dag_failed,
            "status":"No cumple con las condiciones proporcionadas"
        }
        print(response)

    #Lista final con dependencias que deben ser ejecutadas
    final_response = {
        "dependencias_a_ejecutar":to_execute
    }
    print(final_response)
    print("#############################################")

    # execution_date = execution_date.strftime('%Y-%m-%d')

    #Si la lista "to_execute" esta vacia se termina la iteracion
    if len(to_execute) == 0:
    print("No hay DAGS para ejecutar para la fecha evaluada ({})".format(str(execution_date)))

    else:
    #Si no esta vacia, se manda a ejecutar

    conf = {'date_from': str(execution_date)}
    print("ejecutando para: {}".format(conf))
    for objective in zonda_dependencies_config['trigger']:
        dag_id = objective["dag_id"]
        t = ZondaTriggerDagRunOperator(task_id='internal_task', trigger_dag_id=dag_id, conf=conf)
        t.execute(context=kwargs.get('context'))
        print(str(dag_id)+" ejecutado correctamente - date_from: "+str(execution_date))

final_response = 'Iteracion finalizada con exito'
return final_response'''
