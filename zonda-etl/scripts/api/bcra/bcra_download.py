# coding=utf-8

import requests
import json
import os
import pandas as pd
import argparse
import io

ZONDA_DIR = os.getenv('ZONDA_DIR')


def read_config(json_file):
    with open(json_file, 'r') as f:
        data = json.load(f)
    return data


def download_bcra_files(partition_date_filter):
    base_url = "http://www.bcra.gob.ar/Pdfs/PublicacionesEstadisticas/"
    config = read_config(ZONDA_DIR+"/repositories/zonda-etl/scripts/api/bcra/bcra_config.json")
    print("Config File Read")

    proxies = {
        "http": "http://proxy.ar.bsch:8080",
        "https": "http://proxy.ar.bsch:8080",
    }

    for bcra_name, bcra_data in config.items():
        url = base_url+bcra_data.get('item')+"."+bcra_data.get('source_file')
        print("-------------------------url--------------------- "+url)
        print("Start Download: {}".format(url))

        download_request_response = requests.get(url, verify=False, proxies=proxies)
        url_content = download_request_response.content

        inbound_path = ZONDA_DIR + "/inbound/bcra/{}/{}/".format(bcra_data.get('type'), bcra_data.get('item'))
        file_name = "bcra_{}_{}.csv".format(bcra_data.get('item'), partition_date_filter)

        if bcra_data.get('source_file') == 'txt':
            csv_file = open(inbound_path+file_name, 'wb')
            csv_file.write(url_content)
            csv_file.close()
        elif bcra_data.get('source_file') == 'xls':
            with io.BytesIO(url_content) as fh:
                data = pd.read_excel(fh, sheet_name="Tabla", skiprows=range(1, 27))
            df = pd.DataFrame(data)

            if bcra_data.get('item') == 'tabt0006':
                df.columns = ["Col1", "Col2", "codigo", "descripcion"]
                df = df.drop(["Col1", "Col2"], 1)
                df["descripcion"] = df["descripcion"].str.normalize('NFKD').str.encode('ascii', errors='ignore').str.decode(
                    'utf-8')
                df = df[df['codigo'].notna()]
                df["codigo"] = df["codigo"].astype(int)

            print(df)
            df.to_csv(inbound_path+file_name, index=False, header=False, sep=';')

        print("File: {} successfully generated".format(file_name))


if __name__ == "__main__":

    parser = argparse.ArgumentParser(description='Create dates schema')
    parser.add_argument('--partition_date_filter', metavar='partition_date_filter', required=True, help='partition_date_filter')
    args = parser.parse_args()

    download_bcra_files(partition_date_filter=args.partition_date_filter)