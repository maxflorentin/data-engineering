import json
import os
from pyzonda.hadoop import HDFS
from slack import WebClient
from slack.errors import SlackApiError
import argparse
import datetime
import pandas as pd
import smtplib
from email.mime.multipart import MIMEMultipart
from email.mime.text import MIMEText

SLACK_API_TOKEN = os.getenv('SLACK_API_TOKEN')
ZONDA_DIR = os.getenv('ZONDA_DIR')
proxy = 'http://proxy.ar.bsch:8080'


def zonda_inbound_control_files(date_control):

    client = WebClient(token=SLACK_API_TOKEN, ssl=False, proxy=proxy)

    landing_path = '/santander/bi-corp/landing'

    with open(ZONDA_DIR+"/repositories/zonda-etl/scripts/shared/inbound/controlm_config.json", 'r') as f:
        jobs_control_m = json.load(f)
    print("Json Config Readed")

    missing_list_files=[]
    ZONDA_DIR_INB = ZONDA_DIR+"/inbound"

    for job in jobs_control_m['jobs']:

        if 'rio6/fact/cotizaciones' in job['path']:
            path_to_check = '{}/ivct_fe_operacion={}'.format(job['path'].replace(ZONDA_DIR_INB, landing_path),
                                                          date_control if job.get('today', False) is False
                                                          else (datetime.datetime.strptime(date_control, '%Y-%m-%d') + datetime.timedelta(days=1)).strftime('%Y-%m-%d'))
        else:
            path_to_check = '{}/partition_date={}'.format(job['path'].replace(ZONDA_DIR_INB, landing_path),
                                                          date_control if job.get('today', False) is False
                                                          else (datetime.datetime.strptime(date_control, '%Y-%m-%d') + datetime.timedelta(days=1)).strftime('%Y-%m-%d'))

        landing_dates = job.get('landing_dates')

        if job.get('control', False) is True:
            if isinstance(landing_dates, list) and int(datetime.datetime.now().strftime("%w")) in landing_dates:
                if datetime.datetime.now().hour > job.get('landing_time'):
                    if not HDFS.exists(path_to_check):
                        conf_controlm = {
                            "Application": job['application'],
                            "Sub-Application": job['sub-application'],
                            "Job Control-M": job['name'],
                            "Path": job['path'],
                            "Partition": date_control,
                            "File": job['file'].replace('${partition_date}', date_control.replace('-', ''))
                        }
                        print(path_to_check)
                        missing_list_files.append(conf_controlm)
            # El bloque se ejecuta solo si existe la clave "start_landing_dates"
            if "start_landing_dates" in job:
                print("Existe la clave start_landing_dates")
                start_dates = job.get('start_landing_dates')

                if job.get('today') == True:
                    today = datetime.datetime.strptime(date_control,
                                                       '%Y-%m-%d').date() + datetime.timedelta(days=1)
                else:
                    today = datetime.datetime.strptime(date_control, '%Y-%m-%d').date()

                control_week = []

                # Solo en el caso de que todas las fechas declaradas en "start_landing_dates" sean menores a hoy
                # es que se setea la variable "incrementar" en TRUE
                def control_fechas(lista_de_fechas):
                    incrementar = False
                    start_dates = lista_de_fechas

                    for date in start_dates:
                        date = datetime.datetime.strptime(date, '%Y-%m-%d').date()

                        if date < today:
                            control_week.append(True)
                        else:
                            control_week.append(False)

                    if False in control_week:
                        incrementar = False
                    else:
                        incrementar = True

                    return incrementar

                incrementar = control_fechas(start_dates)
                week_dates = []

                # Los valores de "start_landing_dates" se incrementan hasta que alguna de las fechas
                # sea igual o mayor a hoy
                while incrementar == True:
                    control_dates = []
                    for date in start_dates:
                        date = datetime.datetime.strptime(date, '%Y-%m-%d').date()
                        date = date + datetime.timedelta(days=7)
                        control_dates.append(date.strftime("%Y-%m-%d"))
                        start_dates = control_dates

                    incrementar = control_fechas(control_dates)
                    week_dates.append(control_dates)

                # Se contabiliza la cantidad de fechas declaradas en "start_landing_dates" para poder
                # verificar que la alerta se envia solo en el caso de haber controlado todos sus valores
                cantidad_fechas = len(start_dates)
                cantidad_fechas_recorridas = 0
                resultado = []
                print(start_dates)
                print("Fechas a controlar: "+str(start_dates))
                checked_paths = []
                for date in start_dates:

                    date = datetime.datetime.strptime(date, '%Y-%m-%d').date()
                    date_file = date.strftime("%Y%m%d")
                    path_to_check = '{}/partition_date={}/{}'.format(job['path'].replace(ZONDA_DIR_INB, landing_path), str(date),job['file'].replace("${partition_date}", date_file))
                    print(path_to_check)
                    checked_paths.append(path_to_check)
                    # Se recorre la lista de valores menores o igual a hoy verificando la existencia del PATH
                    # correspondiente sobre HDFS.
                    if date <= today:
                        if datetime.datetime.now().hour > job.get('landing_time'):
                            cantidad_fechas_recorridas = cantidad_fechas_recorridas + 1
                            if not HDFS.exists(path_to_check):
                                resultado.append('NE')
                            else:
                                resultado.append('E')
                    # En el caso de que el valor sea mayor se interrumpe la ejecucion del ciclo
                    else:
                        print('No se alcanzaron todas las fechas de control')

                print(cantidad_fechas, cantidad_fechas_recorridas)
                # Solo en el caso de que todas las fechas hayan sido recorridas y no se haya
                # detectado la existencia del PATH en algunos de los valores de la lsita
                # se añade al diccionario correspondiente la notificaion de SLACK
                if 'E' not in resultado and cantidad_fechas == cantidad_fechas_recorridas:

                    print('Se completa el diccionario para alerta en SLACK')
                    conf_controlm = {
                        "Application": job['application'],
                        "Sub-Application": job['sub-application'],
                        "Job Control-M": job['name'],
                        "Path": job['path'],
                        "Partition": start_dates,
                        "File": checked_paths
                    }
                    missing_list_files.append(conf_controlm)

                else:
                    print('Existe un PATH o no se completo el periodo de evaluacion - No se alerta')

            print(missing_list_files)

    if missing_list_files:
        try:
            text = '<!here|here> :grey_exclamation: This incident require your attention!                             '
            color_notification = '#FFCC00'
            callback_id = 'warning_notification'
            fallback = text.replace('*', '').replace('<!here|here>', '').strip()

            fields = [
                {
                    'title': 'Server',
                    'value': os.getenv('HOSTNAME'),
                    'short': True
                },
                {
                    'title': 'Priority',
                    'value': 'High',
                    'short': True
                }
            ]

            fields.append(
                {
                    'title': 'Missing Files',
                    'value': json.dumps(missing_list_files, indent=4),
                    'short': False
                }
            )

            attachments = [
                {
                    'text': text,
                    'author_name': 'Zonda Control Files',
                    'fallback': fallback,
                    'callback_id': callback_id,
                    'color': color_notification,
                    'attachment_type': 'default',
                    'fields': fields,
                    'footer': 'Execution date',
                }
            ]

            result = client.chat_postMessage(
                as_user='false',
                username='Zonda',
                channel='zonda-inbound-control',
                attachments=attachments
            )

            if not result.get('ok'):
                print("Error notifying to channel zonda-alerts: {}".format(result.get('error')))
            #
            df = pd.DataFrame(data=missing_list_files)

            # From y To del email
            origen = "zonda-alerts@santandertecnologia.com.ar"
            destino = "data-engineering@santandertecnologia.com.ar, data-viz@santandertecnologia.com.ar"

            # Armamos el mensaje
            msg = MIMEMultipart('alternative')
            msg['Subject'] = "[IMPORTANTE] - Missing Files at Inbound"
            msg['From'] = origen
            msg['To'] = destino
            html = df.to_html(classes=["table-bordered", "table-striped", "table-hover"])
            text = 'Prueba'
            part1 = MIMEText(text, 'plain')
            part2 = MIMEText(html, 'html')

            # Injectamos el texto en html
            msg.attach(part1)
            msg.attach(part2)

            # Seteamos servidor SMTP.
            s = smtplib.SMTP('relay.ar.bsch', 25)
            s.set_debuglevel(1)
            # Enviamos el email
            s.sendmail(origen, destino, msg.as_string())
            s.quit()

        except SlackApiError as e:
            assert e.response["error"]


if __name__ == "__main__":

    parser = argparse.ArgumentParser(description='Execute ZONDA HDFS Control Files')

    parser.add_argument('--date_control', metavar='date_control', required=True,
                        help='the path to workspace')
    args = parser.parse_args()

    zonda_inbound_control_files(date_control=args.date_control)