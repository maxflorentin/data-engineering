import sys
from json.decoder import JSONDecodeError

import requests
import json
import logging as log
from requests.auth import HTTPBasicAuth
from pathlib import Path
from datetime import timedelta, datetime


def get_credential():
    f = open("/aplicaciones/bi/zonda/repositories/zonda-etl/scripts/shared/navigator-api/auth.txt", "r")
    lines = f.readlines()
    p = lines[0]
    f.close()
    return p[:-1]


def query_cloudera_api(max_results, date_to_process, offset, user_type):
    host = 'https://lxcdhsrv01.ar.bsch:7183'
    query = 'service!=scm;service!=sentry;service!=hbase;service!=hdfs;username!=srv*;username=={ut}'.format(ut=user_type)
    start_date = date_to_process + 'T00:00:00'
    end_date = date_to_process + 'T23:59:59'
    time_frame = 'startTime={date_from}&endTime={date_to}'.format(date_from=start_date, date_to=end_date)
    api_get = '{host}/api/v13/audits/?maxResults={max_results}&query={query}&{time_frame}&resultOffset={offset}' \
        .format(host=host,
                max_results=max_results,
                query=query,
                time_frame=time_frame,
                offset=offset)
    log.info(api_get)
    return requests.get(api_get, verify=False, auth=HTTPBasicAuth('srvcengineerbi', password))


def date_range(start_date, end_date):
    for n in range(int((end_date - start_date).days) + 1):
        yield start_date + timedelta(n)


def process_date(date_to_process):
    log.info("Date to process --> " + date_to_process)

    max_results = 10000
    path = "/aplicaciones/bi/zonda/tmp/navigator-api/partition_date={date_to_process}".format(date_to_process=date_to_process)
    Path(path).mkdir(parents=True, exist_ok=True)

    log.info(path)

    with open('{path}/cloudera_log_{date_to_process}.json'.format(path=path, date_to_process=date_to_process), 'w') as f:
        for user_type in ['a*', 'A*', 'b*', 'B*']:
            offset = 0
            items_flag = True
            while items_flag:
                log.info("Querying for: [date: {dt}, users: {ur}, offset: {of}]". format(
                    dt=date_to_process,
                    ur=user_type,
                    of=offset))
                response = query_cloudera_api(max_results, date_to_process, offset, user_type)
                log.info(response)
                try:
                    items = response.json().get("items")
                except JSONDecodeError:
                    items = []
                try:
                    log.info("Received: " + str(len(items)) + " log entries.\n")
                except TypeError:
                    continue
                if items:
                    for line in items:
                        json.dump(line, f)
                        f.write('\n')
                offset += max_results
                items_flag = False if (len(items) < max_results) else True


def main():
    global password
    password = get_credential()
    start = str(sys.argv[1])
    try:
        end = sys.argv[2]
    except IndexError:
        end = start

    log.info("Date from --> " + start)
    log.info("Date to --> " + end)

    start_loop = datetime.strptime(start, "%Y-%m-%d").date()
    end_loop = datetime.strptime(end, "%Y-%m-%d").date()
    for single_date in date_range(start_loop, end_loop):
        process_date(single_date.strftime("%Y-%m-%d"))


if __name__ == "__main__":
    FORMAT = '%(asctime)-15s navigator-unload: %(message)s'
    log.basicConfig(level=log.INFO, format=FORMAT)
    log.info('Started')
    main()
    log.info('Finished')
