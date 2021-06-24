# coding=utf-8

import os
import argparse
from selenium import webdriver
from selenium.webdriver.chrome.options import Options
import subprocess
import os.path
import time

ZONDA_DIR = os.getenv('ZONDA_DIR')

def remove_files(download_directory):
    cmd = 'rm -f {}/* '.format(download_directory)
    return subprocess.check_output(cmd, shell=True).decode('utf-8').strip().split('\n')


def download_file(browser, file_download_url,download_directory):
    print("Start Download: {}".format(file_download_url))
    browser.get(file_download_url)
    dl_wait = True
    while dl_wait:
        time.sleep(10)
        dl_wait = False
        if all(file.endswith('.crdownload') for file in os.listdir(download_directory)):
            dl_wait = True
    return

def move_file_inbound(download_directory, inbound_directory):
    print("Moving file from {} to {}".format(download_directory, inbound_directory))
    cmd = 'mv {}/* {}'.format(download_directory, inbound_directory)
    return subprocess.check_output(cmd, shell=True).decode('utf-8').strip().split('\n')

def download_files(partition_date_filter):
    #Variables generales de descarga
    username = 'V0729684'
    password = 'Camino41'
    login_url= 'https://extranet.prismamediosdepago.com/extranet/login'
    file_download_url = "https://extranet.prismamediosdepago.com/extranet//downloadFileServlet?id=/downloadFileServlet&&path=/Bandeja%20de%20Entrada/INFORMACION_VARIA/B072_L%EDmitesDisponbles_" + partition_date_filter + '.txt'
    download_directory = 'r' + ZONDA_DIR + "/tmp/limites_disponibles_prisma"
    inbound_directory= ZONDA_DIR + "/cupones/limites_disponibles/B072_LimitesDisponibles_" + partition_date_filter + '.txt'
    proxy = "180.166.55.92:8080"

    chromeOptions = Options()
    chromeOptions.headless = True
    chromeOptions.add_argument('--proxy-server=%s' % proxy)

    chromeOptions.add_experimental_option("prefs", {
        "download.default_directory": download_directory,
        "download.prompt_for_download": False,
        "download.directory_upgrade": True,
        "safebrowsing.enabled": True
    })

    browser = webdriver.Chrome(executable_path="/opt/selenium/chromedriver", options=chromeOptions)
    browser.get(login_url)
    browser.find_element_by_css_selector("input#Usuario.form-control").send_keys(username)
    browser.find_element_by_css_selector("input#Password.form-control").send_keys(password)
    browser.find_element_by_css_selector("button#ingresar.btn.btn-primary").click()

    remove_files(download_directory)
    download_file(browser, file_download_url, download_directory)
    move_file_inbound(download_directory, inbound_directory)

if __name__ == "__main__":

    parser = argparse.ArgumentParser(description='Create a ArcHydro schema')
    parser.add_argument('--partition_date_filter', metavar='partition_date_filter', required=True, help='partition_date_filter')

    args = parser.parse_args()

    download_files(partition_date_filter=args.partition_date_filter)