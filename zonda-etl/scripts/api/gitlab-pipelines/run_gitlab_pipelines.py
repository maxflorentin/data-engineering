import requests
import zipfile
import json
import io, os
import sys
import urllib3
import subprocess

urllib3.disable_warnings(urllib3.exceptions.InsecureRequestWarning)


def run_gitlab_pipeline(url):
    # Setting user Parameters

    try:
        apiToken = "xyLFZNCapQNpVYMu9Lk1"
    except KeyError:
        print("set environment variable APIKEY")
        sys.exit(2)

    headers = {
        "content-type": "application/json",
        "private-token": apiToken,
    }
    #proxies = {
    #        "http": "http://proxy.ar.bsch:8080",
    #        "https": "http://proxy.ar.bsch:8080",
    #    }

    runJob = requests.request("POST", url, headers=headers, verify=False)
    print(runJob.json())

if __name__ == "__main__":
    import argparse

    parser = argparse.ArgumentParser(description='Create a ArcHydro schema')

    parser.add_argument('--url', metavar='url', required=True,
                        help='url')

    args = parser.parse_args()

    run_gitlab_pipeline(url=args.url)