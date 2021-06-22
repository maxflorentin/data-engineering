#!/usr/bin/python3

import pytz
import sqlparse
import os
from calendar import monthrange
from datetime import datetime, timedelta
from dateutil.relativedelta import relativedelta
import socket
import math
import pandas as pd
import cx_Oracle

DATE_FORMAT = "%Y-%m-%d"
DATE_FORMAT_PARTITION = "%Y%m%d"
DATE_HOUR_FORMAT = "%Y-%m-%d H%H"
TS_FORMAT = "%Y-%m-%d %H:%M:%S"
ISO8601_FORMAT = "%Y-%m-%dT%H:%M:%S%z"
ISO8601_FORMAT_ALT = "%Y-%m-%dT%H:%M:%SZ"
DAY_FORMAT = "%d"
HOUR_FORMAT = "%H"
MINUTE_FORMAT = "%M"
SECOND_FORMAT = "%S"
EPOCH_FORMAT = "%s"
WEEK_FORMAT = "%Y%U"
MONTH_FORMAT = "%Y%m"
MONTH_FORMAT_PART = "%m"
MONTH_FORMAT_BDR = "%Y-%m"
YEAR_FORMAT = "%Y"
YYMMDD_FORMAT = "%y%m%d"

ZONDA_DIR = os.getenv("ZONDA_DIR")


def get_month_number_of_days(year, month):
    """
    Get number of days for a given month
    :return: number of days
    """
    return monthrange(year, month)[1]


def get_same_date_prior_n_weeks(date, deltaweek=1):
    """
    Get same date prior n weeks
    :param date: Date [Datetime]
    :param deltaweek: Number of weeks [Integer]
    :return: date shifted [Datetime]
    """
    dt = date
    i = 1

    while i <= abs(deltaweek):
        dt = dt - timedelta(days=7)
        i += 1

    return dt


def get_range_of_dates(date_ts_from, date_ts_to, mode='days'):
    """
    Generate dates range
    :param date_ts_from: from date [Datetime]
    :param date_ts_to: to date [Datetime]
    :param mode: salt mode, days or hours [String]
    :return: dates range [List<Datetime>]
    """
    l_dates = []

    # Deltas
    delta_by_mode = dict()
    delta_by_mode[mode] = 1

    delta_end = dict()
    if mode == 'days':
        delta_end['hour'] = 23
        delta_end['minute'] = 59
        delta_end['second'] = 59
    else:
        delta_end['minute'] = 59
        delta_end['second'] = 59

    while date_ts_from <= date_ts_to:
        dt_from = date_ts_from
        dt_to = date_ts_from.replace(**delta_end)
        l_dates.append([dt_from, dt_to])
        date_ts_from += timedelta(**delta_by_mode)

    return l_dates


def get_range_of_dates_v1(date_ts_from, date_ts_to, shift, mode, format, countries):
    """
    Generate dates range
    :param date_ts_from: from date [Datetime]
    :param date_ts_to: to date [Datetime]
    :param shift: number of days/hours to calculate date from/to by using the date provided
    :param mode:
    :param format:
    :param countries: list of countries [List<String>]
    :return: dates range [List<Datetime>]
    """
    l_dates = []

    if date_ts_from and not date_ts_to:
        date_ts_to = date_ts_from
    elif date_ts_to and not date_ts_from:
        date_ts_from = date_ts_to

    if not date_ts_from:
        # dates by country
        current_date_by_country = []
        for c in countries:
            dt = get_current_datetime_country(c).replace(tzinfo=None)
            current_date_by_country.append(dt)

        # calculating shift from date from/to values
        dt_timestamp = (min(current_date_by_country) + timedelta(days=-1)).replace(hour=0, minute=0, second=0,
                                                                                   microsecond=0)

        # delta
        delta = dict()
        delta[mode] = shift
        dt_shifted_timestamp = dt_timestamp + timedelta(**delta)

        if shift < 0:
            aux = dt_timestamp
            dt_timestamp = dt_shifted_timestamp
            dt_shifted_timestamp = aux

        date_ts_from = dt_timestamp
        date_ts_to = dt_shifted_timestamp.replace(hour=23, minute=59, second=59)

    # Deltas
    delta_by_mode = dict()
    delta_by_mode["hours"] = 1

    while date_ts_from <= date_ts_to:
        l_dates.append(date_ts_from)
        date_ts_from += timedelta(**delta_by_mode)

    return l_dates


def get_month_range(date, deltamonth=0):
    """
    Get month dates range
    :param date: base date [Datetime]
    :param deltamonth: months [Integer]
    :return: dates range [Tuple]
    """
    dt = date + relativedelta(months=1)
    dt = dt - timedelta(days=dt.day)
    i = 1

    while i <= abs(deltamonth):
        dt = dt - timedelta(days=dt.day)
        i += 1

    date_from = dt - timedelta(days=dt.day - 1, hours=dt.hour, minutes=dt.minute, seconds=dt.second)
    date_to = dt + timedelta(hours=23 - dt.hour, minutes=59 - dt.minute, seconds=59 - dt.second)

    return date_from, date_to


def get_week_range(date, deltaweek=0):
    """
    Get week dates range
    :param date: base date [Datetime]
    :param deltaweek: weeks [Integer]
    :return: dates range [Tuple]
    """
    dt = date
    i = 1

    while i <= abs(deltaweek):
        dt = dt - timedelta(days=7)
        i += 1

    week = dt.isocalendar()[1]
    found = False

    while not found:
        prior_dt = dt
        dt = dt + timedelta(days=1)
        if week != dt.isocalendar()[1]:
            found = True
            dt = prior_dt

    date_from = dt - timedelta(days=6, hours=dt.hour, minutes=dt.minute, seconds=dt.second)
    date_to = dt + timedelta(hours=23 - dt.hour, minutes=59 - dt.minute, seconds=59 - dt.second)

    return date_from, date_to


def str_to_datetime(date, format=None):
    """
    Convert string date to datetime format
    :param date: Date in string format
    :param format: Format to parse date. Default: calculated using function get_date_format() [String]
    :return: Date [Datetime]
    """
    if not format:
        format = get_date_format(date)
    date_formatted = datetime.strptime(date, format)
    return date_formatted


def get_date_format(date):
    """
    Get date format
    :param date: Date in string format [String]
    :return: Format [String]
    """
    format = None
    try:
        date_format = DATE_FORMAT
        datetime.strptime(date, date_format)
        format = date_format
    except:
        try:
            ts_format = ISO8601_FORMAT
            datetime.strptime(date, ts_format)
            format = ts_format
        except:
            try:
                ts_format = ISO8601_FORMAT_ALT
                datetime.strptime(date, ts_format)
                format = ts_format
            except:
                try:
                    ts_format = TS_FORMAT
                    datetime.strptime(date, ts_format)
                    format = ts_format
                except:
                    pass
                pass
            pass
        pass
    return format


def get_dates_difference(dt_from_timestamp, dt_to_timestamp, mode='days'):
    """
    Get difference in days or hours between to dates
    :param dt_from_timestamp: Date from value [Datetime]
    :param dt_to_timestamp: Date to value [Datetime]
    :param mode: Days or hours values allowed [String]
    :return: Difference in days or hours [Integer]
    """
    # deltas
    delta_by_mode = dict()
    delta_by_mode[mode] = 1

    # checking dates
    if dt_from_timestamp > dt_from_timestamp:
        aux = dt_from_timestamp
        dt_from_timestamp = dt_from_timestamp
        dt_to_timestamp = aux

    # calculating shift
    shift = 0
    while dt_from_timestamp <= dt_to_timestamp:
        shift += delta_by_mode[mode]
        dt_from_timestamp = dt_from_timestamp + timedelta(**delta_by_mode)

    return shift


def get_scripts(script_directory):
    scripts = []
    for file in os.listdir(script_directory):
        if file.endswith(".txt"):
            scripts.append(open(script_directory + "/" + file).read())
    return scripts


def parse_script(script):
    """
    Parse string
    :param script: string to parse [String]
    :return: list of scripts parsed [List<String>]
    """
    l_scripts = []
    scripts = sqlparse.format(script, strip_comments=True, reindent=True, indent_width=4)
    sql_statement_list = sqlparse.parse(scripts, encoding="UTF-8")

    count_empty_queries = 0
    for statement in sql_statement_list:
        if not (str(statement).strip() == ';' or len(str(statement).strip()) == 1):
            token = str(statement.token_first())
            if not ("--" in token or "/*" in token or token == "None"):
                l_scripts.append(str(statement))
        else:
            count_empty_queries += 1

    return l_scripts


def get_quarter(dt, delta=0):
    """
    Get quarter by date
    :param dt: date to evaluate
    :param delta: delta quarter
    :return: quarter
    """
    quarter = None
    year = dt.year
    factor = 1

    if delta < 0:
        factor = -1
        delta = abs(delta)

    try:
        q = int(math.ceil(float(dt.month) / 3))
        i = 1
        while i <= delta:
            if factor < 0:
                if q == 1:
                    q = 4
                    year -= 1
                else:
                    q -= 1
            else:
                if q == 4:
                    q = 1
                    year += 1
                else:
                    q += 1
            i += 1

        quarter = 'Q{} [{}]'.format(q, year)
    except Exception as _:
        pass

    return quarter


def get_quarter_range(quarter_year):
    """
    Get range of dates corresponding to a given quarter
    :param quarter_year: quarter of the year in format Q<quarter> [<year>] (Q1 [2019])
    :return: Tuple with from/to range of the quarter
    """
    range = []
    quarter_year = quarter_year.replace('[', '').replace(']', '')
    quarter, year = quarter_year.split()

    try:
        if quarter in ('Q1', 'Q2', 'Q3', 'Q4'):
            if quarter == 'Q1':
                range = [datetime(int(year), 1, 1, 0, 0, 0, 0), datetime(int(year), 3, 31, 23, 59, 59, 999)]
            elif quarter == 'Q2':
                range = [datetime(int(year), 4, 1, 0, 0, 0, 0), datetime(int(year), 6, 30, 23, 59, 59, 999)]
            elif quarter == 'Q3':
                range = [datetime(int(year), 7, 1, 0, 0, 0, 0), datetime(int(year), 11, 30, 23, 59, 59, 999)]
            elif quarter == 'Q4':
                range = [datetime(int(year), 10, 1, 0, 0, 0, 0), datetime(int(year), 12, 31, 23, 59, 59, 999)]
    except Exception as _:
        pass

    return range


def get_first_day_of_month(date_ts_from):
    """
    Get first day of month by date
    :param date_ts_from: from date [Datetime]
    :return: first_day
    """
    return date_ts_from.replace(day=1)


def get_last_day_of_month(date_ts_from):
    """
    Get last day of month by date
    :param date_ts_from: from date [Datetime]
    :return: last_day
    """
    tmp = date_ts_from.replace(day=28) + timedelta(days=4)
    return tmp + timedelta(days=-1*tmp.day)


def get_last_working_day(date_ts_from):
    """
    Get last working day of month by date
    :param date_ts_from: from date [Datetime]
    :return: quarter
    """
    days_of_month = {}
    #try:

    csv_holidays = pd.read_csv(ZONDA_DIR + r'/repositories/zonda-etl/services/airflow/plugins/zonda/params/tcdt020.csv', sep=';', names=["fhfestiv", "tpfestiv", "cdautono", "stcdpine", "cdempres", "cddiasem", "nbfhfest", "partition_date"], encoding='utf8')
    csv_holidays = csv_holidays['fhfestiv'].unique()
    aux_holidays = pd.DataFrame(csv_holidays, columns=['date'])

    #date_ts_from = datetime.strptime(date_ts_from, DATE_FORMAT)
    first_day = get_first_day_of_month(date_ts_from)
    last_day = get_last_day_of_month(date_ts_from)
    delta = last_day - first_day
    dm = [[(first_day + timedelta(days=i)).strftime(DATE_FORMAT), (first_day + timedelta(days=i)).weekday()] for i in range(delta.days + 1)]
    df_days = pd.DataFrame(dm, columns=['date', 'day_of_week'])

    days_of_month = df_days[(df_days['day_of_week'] != 5) & (df_days['day_of_week'] != 6)]
    filter_holidays = days_of_month.date.isin(aux_holidays.date)
    days_of_month = days_of_month[~filter_holidays]
    last_working_day = max(days_of_month.date)

    return datetime.strptime(last_working_day ,DATE_FORMAT)

    #except:
    #    return date_ts_from


def get_first_working_day(date_ts_from):
    """
    Get first working day of month by date
    :param date_ts_from: from date [Datetime]
    :return: quarter
    """
    days_of_month = {}
    #try:

    csv_holidays = pd.read_csv(ZONDA_DIR + r'/repositories/zonda-etl/services/airflow/plugins/zonda/params/tcdt020.csv', sep=';', names=["fhfestiv", "tpfestiv", "cdautono", "stcdpine", "cdempres", "cddiasem", "nbfhfest", "partition_date"], encoding='utf8')
    csv_holidays = csv_holidays['fhfestiv'].unique()
    aux_holidays = pd.DataFrame(csv_holidays, columns=['date'])

    #date_ts_from = datetime.strptime(date_ts_from, DATE_FORMAT)
    first_day = get_first_day_of_month(date_ts_from)
    last_day = get_last_day_of_month(date_ts_from)
    delta = last_day - first_day
    dm = [[(first_day + timedelta(days=i)).strftime(DATE_FORMAT), (first_day + timedelta(days=i)).weekday()] for i in range(delta.days + 1)]
    df_days = pd.DataFrame(dm, columns=['date', 'day_of_week'])

    days_of_month = df_days[(df_days['day_of_week'] != 5) & (df_days['day_of_week'] != 6)]
    filter_holidays = days_of_month.date.isin(aux_holidays.date)
    days_of_month = days_of_month[~filter_holidays]
    first_working_day = min(days_of_month.date)

    return datetime.strptime(first_working_day ,DATE_FORMAT)

    #except:
    #    return date_ts_from

def get_previous_working_day(date_ts_from):
    """
    Get previous working day by date
    :param date_ts_from: from date [Datetime]
    :return: quarter
    """
    last_days = {}
    #try:

    csv_holidays = pd.read_csv(ZONDA_DIR + r'/repositories/zonda-etl/services/airflow/plugins/zonda/params/tcdt020.csv', sep=';', names=["fhfestiv", "tpfestiv", "cdautono", "stcdpine", "cdempres", "cddiasem", "nbfhfest", "partition_date"], encoding='utf8')
    csv_holidays = csv_holidays['fhfestiv'].unique()
    aux_holidays = pd.DataFrame(csv_holidays, columns=['date'])

    #date_ts_from = datetime.strptime(date_ts_from, DATE_FORMAT)
    first_day = date_ts_from - timedelta(days=date_ts_from.day + 7)
    last_day = date_ts_from - timedelta(days=1)
    delta = last_day - first_day
    dw = [[(first_day + timedelta(days=i)).strftime(DATE_FORMAT), (first_day + timedelta(days=i)).weekday()] for i in range(delta.days + 1)]
    df_days = pd.DataFrame(dw, columns=['date', 'day_of_week'])

    last_days = df_days[(df_days['day_of_week'] != 5) & (df_days['day_of_week'] != 6)]
    filter_holidays = last_days.date.isin(aux_holidays.date)
    last_days = last_days[~filter_holidays]
    previous_working_day = max(last_days.date)

    return datetime.strptime(previous_working_day, DATE_FORMAT)


def get_working_day(date_ts_from):
    """
    Get last working day by date
    :param date_ts_from: from date [Datetime]
    :return: quarter
    """
    last_days = {}
    #try:

    csv_holidays = pd.read_csv(ZONDA_DIR + r'/repositories/zonda-etl/services/airflow/plugins/zonda/params/tcdt020.csv', sep=';', names=["fhfestiv", "tpfestiv", "cdautono", "stcdpine", "cdempres", "cddiasem", "nbfhfest", "partition_date"], encoding='utf8')
    csv_holidays = csv_holidays['fhfestiv'].unique()
    aux_holidays = pd.DataFrame(csv_holidays, columns=['date'])

    #date_ts_from = datetime.strptime(date_ts_from, DATE_FORMAT)
    first_day = date_ts_from - timedelta(days=date_ts_from.day + 7)
    last_day = date_ts_from
    delta = last_day - first_day
    dw = [[(first_day + timedelta(days=i)).strftime(DATE_FORMAT), (first_day + timedelta(days=i)).weekday()] for i in range(delta.days + 1)]
    df_days = pd.DataFrame(dw, columns=['date', 'day_of_week'])

    last_days = df_days[(df_days['day_of_week'] != 5) & (df_days['day_of_week'] != 6)]
    filter_holidays = last_days.date.isin(aux_holidays.date)
    last_days = last_days[~filter_holidays]
    working_day = max(last_days.date)

    return datetime.strptime(working_day, DATE_FORMAT)


def get_max_partition(table, date_from):

    date_from = date_from.replace('-', '')

    user = os.getenv('HIVE_METASTORE_USER')
    password = os.getenv('HIVE_METASTORE_PASS')
    server = os.getenv('HIVE_METASTORE_HOST')
    puerto = os.getenv('HIVE_METASTORE_PORT')
    base = os.getenv('HIVE_METASTORE_SID')

    query = 'select max(c."PART_KEY_VAL") from "TBLS" a \
             inner join "PARTITIONS" b \
             on a."TBL_ID" = b."TBL_ID" \
             inner join "PARTITION_KEY_VALS" c \
             on c."PART_ID" = b."PART_ID" \
             inner join "DBS" d \
             on d."DB_ID" = a."DB_ID" \
             where concat(concat(d."NAME", \'.\'), a."TBL_NAME") = \'{}\' \
             and replace(c."PART_KEY_VAL" , \'-\', \'\') <= \'{}\' and c."INTEGER_IDX" = 0'.format(table, date_from)

    query_bdr = 'select max(c."PART_KEY_VAL") from "TBLS" a \
                 inner join "PARTITIONS" b \
                 on a."TBL_ID" = b."TBL_ID" \
                 inner join "PARTITION_KEY_VALS" c \
                 on c."PART_ID" = b."PART_ID" \
                 inner join "DBS" d \
                 on d."DB_ID" = a."DB_ID" \
                 where concat(concat(d."NAME", \'.\'), a."TBL_NAME") = \'{}\' \
                 and substr(replace(c."PART_KEY_VAL", \'-\', \'\'), 1, 6) = substr(\'{}\', 1, 6) \
                 and c."INTEGER_IDX" = 0'.format(table, date_from)

    if 'lxbidu' in socket.gethostname():

        dsn_tns = cx_Oracle.makedsn(server, puerto, service_name=base)

        con = cx_Oracle.connect(user=user, password=password, dsn=dsn_tns)

        cur = con.cursor()

    else:

        import psycopg2
        user = 'hive'
        password = 'dnJb&!VB7-'
        server = 'sard1ae1sdbzonda0plat001.ceyuutfhmuk2.us-east-1.rds.amazonaws.com'
        puerto = '5432'
        base = 'hive_metastore'

        conn_string = "host=" + server + " port=" + puerto + " dbname=" + base + " user=" + user \
                      + " password=" + password
        conn = psycopg2.connect(conn_string)
        query = query + ';'
        query_bdr = query_bdr + ';'
        cur = conn.cursor()

    cur.execute(query)
    partition = cur.fetchall()
    max_partition = partition[0][0]

    cur.execute(query_bdr)
    partition_bdr = cur.fetchall()
    max_partition_bdr = partition_bdr[0][0] if partition_bdr else None

    return max_partition, max_partition_bdr


def get_max_partition_previous_month(table, date_from):

    date_from = date_from.replace('-', '')

    user = os.getenv('HIVE_METASTORE_USER')
    password = os.getenv('HIVE_METASTORE_PASS')
    server = os.getenv('HIVE_METASTORE_HOST')
    puerto = os.getenv('HIVE_METASTORE_PORT')
    base = os.getenv('HIVE_METASTORE_SID')

    query = 'select max(c."PART_KEY_VAL") from "TBLS" a \
             inner join "PARTITIONS" b \
             on a."TBL_ID" = b."TBL_ID" \
             inner join "PARTITION_KEY_VALS" c \
             on c."PART_ID" = b."PART_ID" \
             inner join "DBS" d \
             on d."DB_ID" = a."DB_ID" \
             where concat(concat(d."NAME", \'.\'), a."TBL_NAME") = \'{}\' \
             and replace(c."PART_KEY_VAL" , \'-\', \'\') <= TO_CHAR(LAST_DAY(TO_DATE(ADD_MONTHS(TO_DATE(\'{}\', \'yyyy/mm/dd\'),-1))), \'YYYYMMDD\') and c."INTEGER_IDX" = 0'.format(table, date_from)

    query_bdr = 'select max(c."PART_KEY_VAL") from "TBLS" a \
                 inner join "PARTITIONS" b \
                 on a."TBL_ID" = b."TBL_ID" \
                 inner join "PARTITION_KEY_VALS" c \
                 on c."PART_ID" = b."PART_ID" \
                 inner join "DBS" d \
                 on d."DB_ID" = a."DB_ID" \
                 where concat(concat(d."NAME", \'.\'), a."TBL_NAME") = \'{}\' \
                 and substr(replace(c."PART_KEY_VAL", \'-\', \'\'), 1, 6) = substr(TO_CHAR(LAST_DAY(TO_DATE(ADD_MONTHS(TO_DATE(\'{}\', \'yyyy/mm/dd\'),-1))), \'YYYYMMDD\'), 1, 6) \
                 and c."INTEGER_IDX" = 0'.format(table, date_from)

    dsn_tns = cx_Oracle.makedsn(server, puerto, service_name=base)

    con = cx_Oracle.connect(user=user, password=password, dsn=dsn_tns)

    cur = con.cursor()

    cur.execute(query)
    partition = cur.fetchall()
    max_partition_previous_month = partition[0][0]

    cur.execute(query_bdr)
    partition_bdr = cur.fetchall()
    max_partition_bdr_previous_month = partition_bdr[0][0] if partition_bdr else None

    return max_partition_bdr_previous_month
