from datetime import timedelta
from calendar import mdays
import datetime as dt
import calendar
from dateutil.relativedelta import *


class date_input:

    def last_working_day(isdate): 

        date_ts_from = dt.datetime.strptime(isdate[-1] + '01', '%Y%m%d') 
        last = date_ts_from.replace(day=calendar.monthrange(date_ts_from.year,date_ts_from.month)[1])

        if last.weekday()<5:
            date_last = last
            return date_last.strftime("%Y%m%d")
        else:
            date_last = last-timedelta(days=1+last.weekday()-5)
            return date_last.strftime("%Y%m%d")

    def first_day_of_the_month(isdate): 

        date_ts_from = dt.datetime.now()
        first_day_of_the_month = date_ts_from.replace(day=1)

        return first_day_of_the_month.strftime("%Y%m%d")  

    def last_day_of_month(isdate):

        date_ts_from = dt.datetime.strptime(isdate[-1] + '01', '%Y%m%d')

        dt_last_day = date_ts_from.replace(day=28) + timedelta(days=4)

        last_day_of_month = dt_last_day + timedelta(days=-1*dt_last_day.day)

        return last_day_of_month.strftime("%Y%m%d")