import datetime
from calendar import monthrange
from datetime import datetime, timedelta
import math

date_ts_from = datetime.strptime('2016-01-01', '%Y-%m-%d')

def get_last_day_of_month(date_ts_from):
    tmp = date_ts_from.replace(day=28) + timedelta(days=4)
    return tmp + timedelta(days=-1*tmp.day)

def get_first_day_of_month(date_ts_from):
    """
    Get first day of month by date
    :param date_ts_from: from date [Datetime]
    :return: first_day
    """
    return date_ts_from.replace(day=1)


#last_calendar_day_previousyear =  get_last_day_of_month((date_ts_from - timedelta(days=365)).replace(day=31,month=12))
#last_calendar_day_previous2years =  get_last_day_of_month((date_ts_from - timedelta(days=365)*2).replace(day=31,month=12))

last_calendar_day_previousyear =  get_first_day_of_month(date_ts_from - timedelta(days=365)).replace(day=31,month=12)
last_calendar_day_previous2years =  get_first_day_of_month(date_ts_from - timedelta(days=365)*2).replace(day=31,month=12)

print(last_calendar_day_previousyear)
print(last_calendar_day_previous2years)