import csv
from googleapiclient import errors
from googleapiclient import discovery
from httplib2 import Http
from oauth2client import file, client, tools
import os
import argparse
import pandas as pd

os.environ['https_proxy'] = 'https://proxy.ar.bsch:8080'

def get_api_services():
    # define credentials and client secret file paths
    credentials_file_path = '/aplicaciones/bi/zonda/repositories/zonda-etl/scripts/shared/gdrive/credentials/credentials.json'
    clientsecret_file_path = '/aplicaciones/bi/zonda/repositories/zonda-etl/scripts/shared/gdrive/credentials/client_secret.json'

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


def download_sheet_to_csv(sheets_instance, spreadsheet_id, sheet_name, partition_date_filter, partition_date):
    result = sheets_instance.spreadsheets().values().get(spreadsheetId=spreadsheet_id, range=sheet_name).execute()
    path_out = '/aplicaciones/bi/zonda/inbound/googledrive/dim/{}'.format(sheet_name)
    output_file = os.path.join(path_out, sheet_name + "_"+ partition_date_filter +".csv")

    items_values = list(result.values())[2][:]
    df = pd.DataFrame(items_values)

    df_filtered = df.loc[(df[len(df.columns) - 1] == partition_date)]

    list_filtered = df_filtered.values.tolist()

    list(result.values())[2][:] = list_filtered

    with open(output_file, 'w', newline='') as f:
        writer = csv.writer(f, delimiter=';', quoting=csv.QUOTE_MINIMAL)
        writer.writerows(result.get('values'))

    f.close()
  ##  print(f'Successfully downloaded {sheet_name}.csv')


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
            #print(f'An error has occurred: {error}')
            print('Error HTTP')
            break

    spreadsheet_id = [result.get('id') for result in results if result.get('name') == spreadsheet_name][0]

    return spreadsheet_id


if __name__ == '__main__':

    parser = argparse.ArgumentParser(description="Function to download a specific sheet from a Google Spreadsheet")

    parser.add_argument("--spreadsheet_name", required=True, help="The name of the Google Spreadsheet")
    parser.add_argument("--sheet_name", required=True, nargs='+', type=str, help="The name of the sheet in spreadsheet to download as csv")
    parser.add_argument("--partition_date_filter", required=True,  type=str, help="Partition Date Filter")
    parser.add_argument("--partition_date", required=True,  type=str, help="Partition Date ")

    args = parser.parse_args()

    drive, sheets = get_api_services()

    q_sheets = len(args.sheet_name)
    for i in range(0, q_sheets):
        ##print(args.sheet_name[i])
        spreadsheet_id = get_spreadsheet_id(drive, args.spreadsheet_name)
        download_sheet_to_csv(sheets, spreadsheet_id, args.sheet_name[i], args.partition_date_filter, args.partition_date)