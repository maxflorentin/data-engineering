#!/usr/bin/env python
# coding: utf-8


import csv
from googleapiclient import errors
from googleapiclient import discovery
from httplib2 import Http
from oauth2client import file, client, tools
import os


def get_api_services():
    # define credentials and client secret file paths
    credentials_file_path = '.\credentials\credentials.json'
    clientsecret_file_path = '.\credentials\client_secret.json'

    # define scope
    SCOPE = 'https://www.googleapis.com/auth/drive'

    # define store
    store = file.Storage(credentials_file_path)
    credentials = store.get()

    if not credentials or credentials.invalid:
        flow = client.flow_from_clientsecrets(clientsecret_file_path, SCOPE)
        credentials = tools.run_flow(flow, store)

    # define API service
    http = credentials.authorize(Http())
    drive = discovery.build('drive', 'v3', http=http)
    sheets = discovery.build('sheets', 'v4', credentials=credentials)

    return drive, sheets


def download_sheet_to_csv(sheets_instance, spreadsheet_id, sheet_name):
    result = sheets_instance.spreadsheets().values().get(spreadsheetId=spreadsheet_id, range=sheet_name).execute()
    path_out = '{sheet_name}'
    output_file =path_out + '{sheet_name}.csv'

    with open(output_file, 'w+', newline='') as f:
        writer = csv.writer(f, delimiter=';', quoting=csv.QUOTE_MINIMAL)
        writer.writerows(result.get('values'))

    f.close()

    print(f'Successfully downloaded {sheet_name}.csv')


def get_spreadsheet_id(api_service, spreadsheet_name):
    results = []
    page_token = None

    while True:
        try:
            param = {'q': 'mimeType="application/vnd.google-apps.spreadsheet"'}

            if page_token:
                param['pageToken'] = page_token

            files = api_service.files().list(**param).execute()
            results.extend(files.get('files'))

            # Google Drive API shows our files in multiple pages when the number of files exceed 100
            page_token = files.get('nextPageToken')

            if not page_token:
                break

        except errors.HttpError as error:
            print(f'An error has occurred: {error}')
            break

    spreadsheet_id = [result.get('id') for result in results if result.get('name') == spreadsheet_name][0]

    return spreadsheet_id


def parse_args():
    parser = argparse.ArgumentParser(description="Function to download a specific sheet from a Google Spreadsheet")

    parser.add_argument("--spreadsheet-name", required=True, help="The name of the Google Spreadsheet")
    parser.add_argument("--sheet-name", required=True, help="The name of the sheet in spreadsheet to download as csv")

    return parser.parse_args()


if __name__ == '__main__':

    spreadsheet_name = "INPUT_RORWA_Parametricas"
    sheet_name = "param_rorwa_area_negocio"
    #sheet_name = ["param_rorwa_area_negocio", "param_rorwa_coste_income", "param_rorwa_rel_segmento_adn", "param_rorwa_riesgo_operativo", "param_rorwa_sucursales", "param_rorwa_riesgo_encaje"]

    drive, sheets = get_api_services() 

    spreadsheet_id = get_spreadsheet_id(drive, spreadsheet_name)
    download_sheet_to_csv(sheets, spreadsheet_id, sheet_name)


   ## q_sheets = len(sheet_name)
   ## for i in range(0, q_sheets):
   ##     print(sheet_name[i])
   ##     spreadsheet_id = get_spreadsheet_id(drive, spreadsheet_name)
   ##     download_sheet_to_csv(sheets, spreadsheet_id, sheet_name[i])

