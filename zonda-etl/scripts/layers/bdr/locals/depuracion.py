import os
import argparse
import datetime as dt
from dateutil.relativedelta import *

def depuracion(path, date_from, months_old, days_old):
    if(len(path.split('/'))==1 or  os.system('ls ' + path) != 0): 
        raise Exception('Incompatible path') 
    else:
        print("Today: " + date_from)
        date = dt.datetime.strptime(date_from, '%Y-%m-%d')
        date = date + relativedelta(months =- int(months_old))
        date = date + dt.timedelta(days =- int(days_old))
        date = date.strftime('%Y-%m-%d')
        print("Date of delete: " + date)
        #find /path ! -newermt "YYYY-MM-DD" | xargs rm -rf
        command = "find " + path + " ! -newermt " + date + " | xargs rm -rf" 
        os.system(command)
        print('Files were deleted - Older than: ' + date)

if __name__ == "__main__": 
    parser = argparse.ArgumentParser(description='Create dates schema')
    parser.add_argument('--path',       metavar='path',       required=True, help='path')
    parser.add_argument('--date_from',  metavar='date_from',  required=True, help='date_from')
    parser.add_argument('--months_old', metavar='months_old', required=True, help='months_old')
    parser.add_argument('--days_old',   metavar='days_old',   required=True, help='days_old')
    args = parser.parse_args()
    depuracion(path=args.path,date_from=args.date_from, months_old=args.months_old,days_old=args.days_old)