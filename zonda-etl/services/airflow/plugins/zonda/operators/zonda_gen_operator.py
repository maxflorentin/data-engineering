#!/usr/bin/python3

import datetime as dt
import calendar
from calendar import mdays
from airflow.models import BaseOperator
from airflow.utils.decorators import apply_defaults
from zonda.helpers import *
from dateutil.relativedelta import relativedelta
import time
import socket

class ZondaGenOperator(BaseOperator):
    template_fields = ['input', 'date_from', 'date_to', 'shift']

    @apply_defaults
    def __init__(self, *args, **kwargs):
        """
        Constructor Init
        """
        super(ZondaGenOperator, self).__init__(*args, **kwargs)
        self.input = kwargs.get('input', {})
        self.date_from = self.input.get('date_from') or kwargs.get('date_from')
        self.date_to = self.input.get('date_to') or kwargs.get('date_to')
        self.mode = self.input.get('mode') or kwargs.get('mode')
        self.shift = self.input.get('shift') or kwargs.get('shift')
        self.tables = self.input.get('tables') or kwargs.get('tables')
        
    def execute(self, context):
        """
        Execute operator
        :param context: dag context
        :return: None
        """
        dag_run = context.get('dag_run')

        try:
            conf = dag_run.conf or dict()
        except AttributeError as _:
            conf = dict()
            pass

        # get date range
        self.date_from = conf.get('date_from') or self.date_from
        self.date_to = conf.get('date_to') or self.date_to
        self.mode = conf.get('mode') or self.mode or 'days'
        self.shift = conf.get('shift') or self.shift or 0
        self.tables = conf.get('tables') or self.tables or None
        try:
            self.shift = int(self.shift)
        except Exception as _:
            self.shift = 0
            pass

        if self.date_from and not self.date_to and not self.shift:
            self.date_to = self.date_from
        elif not self.date_from and self.date_to:
            self.date_from = self.date_to
            if self.shift:
                self.date_to = None
        elif not (self.date_from or self.date_to):
            # self.date_from = context.get('yesterday_ds')
            self.date_from = context.get('ds')
            if not self.shift:
                self.date_to = self.date_from

        return self.generate_variables()

    def generate_variables(self):
        """
        Generate set of variables
        :return: set of variables with value [Dictionary]
        """
        dt_vars = {}

        # parse dates
        date_ts_from = dt.datetime.strptime(self.date_from, '%Y-%m-%d')
        date_ts_to = dt.datetime.strptime(self.date_to, '%Y-%m-%d') if self.date_to else None

        if not date_ts_to:
            if self.shift > 0:
                date_ts_to = date_ts_from + dt.timedelta(days=self.shift)
            elif self.shift < 0:
                date_ts_to = date_ts_from
                date_ts_from = date_ts_to + dt.timedelta(days=self.shift)

        if date_ts_to < date_ts_from:
            aux = date_ts_to
            date_ts_to = date_ts_from
            date_ts_from = aux

        date_ts_to = date_ts_to + timedelta(days=1, milliseconds=-1, microseconds=0)

        

        # date variables
        dt_vars['process_date'] = dt.date.today().strftime(DATE_FORMAT_PARTITION)
        dt_vars['partition_process_date'] = dt.date.today().strftime(DATE_FORMAT)
        dt_vars['previous_process_date'] = (dt.date.today() - timedelta(days=1)).strftime(DATE_FORMAT)
        dt_vars['previous_non_partition_process_date'] = (dt.date.today() - timedelta(days=1)).strftime(DATE_FORMAT_PARTITION)
        dt_vars['next_date_from'] = (date_ts_from + timedelta(days=1)).strftime(DATE_FORMAT)
        dt_vars['date_ts_from'] = date_ts_from.strftime(TS_FORMAT)
        dt_vars['date_ts_to'] = date_ts_to.strftime(TS_FORMAT)
        dt_vars['previous_date_from'] = (date_ts_from - timedelta(days=1, milliseconds=-1, microseconds=0)).strftime(DATE_FORMAT)
        dt_vars['date_ts'] = date_ts_from.strftime(TS_FORMAT)
        dt_vars['date_to_process_ts'] = dt_vars['date_ts']
        dt_vars['date_from'] = date_ts_from.strftime(DATE_FORMAT)
        dt_vars['date_to'] = date_ts_to.strftime(DATE_FORMAT)
        dt_vars['date'] = date_ts_from.strftime(DATE_FORMAT)
        dt_vars['date_to_process'] = dt_vars['date']
        dt_vars['week_day'] = date_ts_from.weekday()
        dt_vars['week'] = date_ts_from.isocalendar()[1]
        dt_vars['month'] = date_ts_from.strftime(MONTH_FORMAT)
        dt_vars['month_partition_bdr'] = date_ts_from.strftime(MONTH_FORMAT_BDR)
        dt_vars['year'] = date_ts_from.strftime(YEAR_FORMAT)
        dt_vars['hour'] = date_ts_from.strftime(HOUR_FORMAT)
        dt_vars['minute'] = date_ts_from.strftime(MINUTE_FORMAT)
        dt_vars['second'] = date_ts_from.strftime(SECOND_FORMAT)
        dt_vars['previous_short_date'] = (dt.date.today() - timedelta(days=1)).strftime(YYMMDD_FORMAT)
        
        #ultimo dia del mes siguiente
        date_nm = date_ts_from  + relativedelta(months=+1)
        dt_vars['next_month_working_day'] = date_nm.replace(day= calendar.monthrange(date_nm.year, date_nm.month)[1]).strftime(DATE_FORMAT)

        #Primer dia del mes siguiente
        dt_vars['next_month_first_day'] = (date_ts_from + relativedelta(months=1, day=1)).strftime(DATE_FORMAT)


        dt_vars['quarter_from'] = get_quarter_range(get_quarter(date_ts_from))[0].strftime(DATE_FORMAT)
        dt_vars['quarter_to'] = get_quarter_range(get_quarter(date_ts_to))[1].strftime(DATE_FORMAT)

        dt_vars['previous_quarter_from'] = get_quarter_range(get_quarter(date_ts_from, delta=-1))[0].strftime(
            DATE_FORMAT)
        dt_vars['previous_quarter_to'] = get_quarter_range(get_quarter(date_ts_to, delta=-1))[1].strftime(
            DATE_FORMAT)

        from_cmfrom, from_cmto = get_month_range(date_ts_from)
        to_cmfrom, to_cmto = get_month_range(date_ts_to)

        dt_vars['month_from'] = from_cmfrom.strftime(DATE_FORMAT)
        dt_vars['month_to'] = to_cmto.strftime(DATE_FORMAT)

        from_cwfrom, from_cwto = get_week_range(date_ts_from)
        to_cwfrom, to_cwto = get_week_range(date_ts_to)

        dt_vars['current_week_from'] = from_cwfrom.strftime(DATE_FORMAT)
        dt_vars['current_week_to'] = from_cwto.strftime(DATE_FORMAT)

        dt_vars['week_from'] = from_cwfrom.strftime(DATE_FORMAT)
        dt_vars['week_to'] = to_cwto.strftime(DATE_FORMAT)

        dt_vars['previous_week_same_date_ts_from'] = get_same_date_prior_n_weeks(date_ts_from).strftime(
            TS_FORMAT)
        dt_vars['previous_week_same_date_ts_to'] = get_same_date_prior_n_weeks(date_ts_to).strftime(TS_FORMAT)
        dt_vars['previous_week_same_date_ts'] = get_same_date_prior_n_weeks(date_ts_from).strftime(TS_FORMAT)
        dt_vars['previous_week_same_date_from'] = get_same_date_prior_n_weeks(date_ts_from).strftime(
            DATE_FORMAT)
        dt_vars['previous_week_same_date_to'] = dt_vars['previous_week_same_date_from']
        dt_vars['previous_week_same_date'] = get_same_date_prior_n_weeks(date_ts_from).strftime(DATE_FORMAT)
        dt_vars['previous_week'] = (date_ts_from - timedelta(days=7)).isocalendar()[1]
        dt_vars['previous_month'] = (date_ts_from - timedelta(days=date_ts_from.day)).strftime(MONTH_FORMAT)
        dt_vars['previous_year'] = int(dt_vars['year']) - 1

        dt_vars['previous_month-1'] = ((date_ts_from - timedelta(days=date_ts_from.day)) - timedelta(
            days=(date_ts_from - timedelta(days=date_ts_from.day)).day)).strftime(MONTH_FORMAT)

        dt_vars['previous_month-1_bdr'] = ((date_ts_from - timedelta(days=date_ts_from.day)) - timedelta(
            days=(date_ts_from - timedelta(days=date_ts_from.day)).day)).strftime(MONTH_FORMAT_BDR)

        pmfrom, pmto = get_month_range(date_ts_from, 1)
        dt_vars['previous_month_from'] = pmfrom.strftime(DATE_FORMAT)
        dt_vars['previous_month_to'] = pmto.strftime(DATE_FORMAT)
        dt_vars['previous_month_from_arda'] = pmfrom.strftime(DATE_FORMAT_PARTITION)
        dt_vars['previous_month_to_arda'] = pmto.strftime(DATE_FORMAT_PARTITION)
        dt_vars['previous_month_partition_bdr'] = pmfrom.strftime(MONTH_FORMAT_BDR)

        pmfrom_1, pmto_1 = get_month_range(date_ts_from, 2)
        dt_vars['previous_month-1_date_from'] = pmfrom_1.strftime(DATE_FORMAT)
        dt_vars['previous_month-1_date_to'] = pmto_1.strftime(DATE_FORMAT)
        dt_vars['previous_month-1_date_from_arda'] = pmfrom_1.strftime(DATE_FORMAT_PARTITION)
        dt_vars['previous_month-1_date_to_arda'] = pmto_1.strftime(DATE_FORMAT_PARTITION)

        pwfrom, pwto = get_week_range(date_ts_from, 1)
        dt_vars['previous_week_from'] = pwfrom.strftime(DATE_FORMAT)
        dt_vars['previous_week_to'] = pwto.strftime(DATE_FORMAT)
        p_two_wfrom, p_two_wto = get_week_range(date_ts_from, 2)
        dt_vars['previous_week-1_from'] = p_two_wfrom.strftime(DATE_FORMAT)
        dt_vars['previous_week-1_to'] = p_two_wto.strftime(DATE_FORMAT)
        p_three_wfrom, p_three_wto = get_week_range(date_ts_from, 3)
        dt_vars['previous_week-2_from'] = p_three_wfrom.strftime(DATE_FORMAT)
        dt_vars['previous_week-2_to'] = p_three_wto.strftime(DATE_FORMAT)
        p_four_wfrom, p_four_wto = get_week_range(date_ts_from, 4)
        dt_vars['previous_week-3_from'] = p_four_wfrom.strftime(DATE_FORMAT)
        dt_vars['previous_week-3_to'] = p_four_wto.strftime(DATE_FORMAT)
        p_five_wfrom, p_five_wto = get_week_range(date_ts_from, 5)
        dt_vars['previous_week-4_from'] = p_five_wfrom.strftime(DATE_FORMAT)
        dt_vars['previous_week-4_to'] = p_five_wto.strftime(DATE_FORMAT)
        
        first_calendar_day = get_first_day_of_month(date_ts_from)
        # first_calendar_day = str(a.strftime(DATE_FORMAT))
        dt_vars['first_calendar_day'] = first_calendar_day.strftime(DATE_FORMAT)
        dt_vars['first_calendar_day_arda'] = first_calendar_day.strftime(DATE_FORMAT_PARTITION)

        last_calendar_day = get_last_day_of_month(date_ts_from)
        last_calendar_day_previousyear =  get_first_day_of_month(date_ts_from - timedelta(days=366)).replace(day=31,month=12)
        last_calendar_day_previous2years =  get_first_day_of_month(date_ts_from - timedelta(days=366)*2).replace(day=31,month=12)
        # last_calendar_day = str(last_calendar_day.strftime(DATE_FORMAT))
        dt_vars['last_calendar_day'] = last_calendar_day.strftime(DATE_FORMAT)
        dt_vars['last_calendar_day_arda'] = last_calendar_day.strftime(DATE_FORMAT_PARTITION)
        dt_vars['last_calendar_day_previous_year'] = last_calendar_day_previousyear.strftime(DATE_FORMAT)
        dt_vars['last_calendar_day_previous_2years'] = last_calendar_day_previous2years.strftime(DATE_FORMAT)
        
        first_working_day = get_first_working_day(date_ts_from)
        dt_vars['first_working_day'] = first_working_day.strftime(DATE_FORMAT)
        dt_vars['first_working_day_arda'] = first_working_day.strftime(DATE_FORMAT_PARTITION)

        last_working_day = get_last_working_day(date_ts_from)
        day_of_date_from = date_ts_from.day
        previous_month_last_working_day = get_last_working_day(date_ts_from - timedelta(days=day_of_date_from, milliseconds=0, microseconds=0))
        dt_vars['last_working_day'] = last_working_day.strftime(DATE_FORMAT)
        dt_vars['last_working_day_arda'] = last_working_day.strftime(DATE_FORMAT_PARTITION)
        dt_vars['previous_month_last_working_day'] = previous_month_last_working_day.strftime(DATE_FORMAT)
        dt_vars['previous_month_last_working_day_arda'] = previous_month_last_working_day.strftime(DATE_FORMAT_PARTITION)

        previous_month_last_working_day_1 = get_last_working_day(date_ts_from - timedelta(days=60) )
        dt_vars['previous_month-1_last_working_day'] = previous_month_last_working_day_1.strftime(DATE_FORMAT)
        dt_vars['previous_month-1_last_working_day_arda'] = previous_month_last_working_day_1.strftime(DATE_FORMAT_PARTITION)

        previous_working_day = get_previous_working_day(date_ts_from)
        working_day = get_working_day(date_ts_from)
        previous_month_working_day = get_previous_working_day(date_ts_from - timedelta(days=30, milliseconds=0, microseconds=0))
        previous_year_working_day = get_previous_working_day(date_ts_from - timedelta(days=365, milliseconds=0, microseconds=0))
        previous_year_last_working_day = get_previous_working_day((date_ts_from - timedelta(days=365)).replace(day=31,month=12))
        dt_vars['previous_working_day'] = previous_working_day.strftime(DATE_FORMAT)
        dt_vars['previous_month_working_day'] = previous_month_working_day.strftime(DATE_FORMAT)
        dt_vars['previous_year_working_day'] = previous_year_working_day.strftime(DATE_FORMAT)
        dt_vars['previous_year_last_working_day'] = previous_year_last_working_day.strftime(DATE_FORMAT)
        dt_vars['working_day'] = working_day.strftime(DATE_FORMAT)
        
        # Partition Value
        dt_vars['partition_date'] = date_ts_from.strftime(DATE_FORMAT)
        dt_vars['partition_date_filter'] = date_ts_from.strftime(DATE_FORMAT_PARTITION)
        dt_vars['previous_partition_date'] = (date_ts_from - timedelta(days=1, milliseconds=-1, microseconds=0)).strftime(DATE_FORMAT)
        dt_vars['previous_partition_date_filter'] = (date_ts_from - timedelta(days=1, milliseconds=-1, microseconds=0)).strftime(DATE_FORMAT_PARTITION)
        dt_vars['next_partition_date'] = (date_ts_from + timedelta(days=1, milliseconds=0, microseconds=0)).strftime(DATE_FORMAT)
        dt_vars['next_partition_date_filter'] = (date_ts_from + timedelta(days=1, milliseconds=0, microseconds=0)).strftime(DATE_FORMAT_PARTITION)
        dt_vars['partition_filter'] = self.build_partition_filter(date_ts_from, date_ts_to, self.mode)
        dt_vars['day_partition'] = str(int(date_ts_from.strftime(DAY_FORMAT)))
        dt_vars['month_partition'] = str(int(date_ts_from.strftime(MONTH_FORMAT_PART)))
        dt_vars['partition_date_short'] = date_ts_from.strftime(YYMMDD_FORMAT)
        dt_vars['previous_partition_date_short'] = (date_ts_from - timedelta(days=1, milliseconds=-1, microseconds=0)).strftime(YYMMDD_FORMAT)


        dtunix = dt.datetime(int(date_ts_from.strftime(YEAR_FORMAT)), int(date_ts_from.strftime(MONTH_FORMAT_PART)), int(date_ts_from.strftime(DAY_FORMAT)), int(date_ts_from.strftime(HOUR_FORMAT)), int(date_ts_from.strftime(MINUTE_FORMAT)))
        dt_vars['partition_date_unix'] = str(time.mktime(dtunix.timetuple()))

        if self.tables is not None:
            tables = [table_name.strip() for table_name in self.tables.split(',')]
            if tables:
                for table in tables:
                    dt_vars['max_partition_{}'.format(table.split('.')[1])], dt_vars['bdr_max_partition_{}'.format(table.split('.')[1])] = get_max_partition(table, self.date_from)
                    if 'lxbidu' in socket.gethostname():
                        dt_vars['max_partition_prevmonth_{}'.format(table.split('.')[1])] = get_max_partition_previous_month(table, self.date_from)

        return {k: v for k, v in dt_vars.items() if v}

    def build_partition_filter(self, date_ts_from, date_ts_to, mode):
        """
        Build spectrum partition filter condition
        :param date_ts_from:
        :param date_ts_to:
        :param mode:
        :return: spectrum partition filter [String]
        """
        # dates
        dates = []
        delta = {mode: 1}
        dt_from = date_ts_from
        dt_to = date_ts_to

        while dt_from < dt_to:
            dates.append(dt_from)
            dt_from = dt_from + relativedelta(**delta)
        final_dates = self.reduces_dates(dates, mode)

        # build filter
        l_days_final = []
        l_months_final = []
        l_hours_final = []
        l_years_final = []
        aux = ''
        for key in final_dates:
            max_index = len(final_dates[key]) - 1
            prior_year = None
            prior_year_month = None
            prior_year_month_day = None
            l_days = []
            l_months = []
            l_hours = []
            for i, dt in enumerate(final_dates[key]):
                last_element = i == max_index
                year = dt.year
                month = dt.month
                day = dt.day
                hour = dt.hour
                if key == 'years':
                    l_years_final.append('year={}'.format(year))
                if key == 'months':
                    if not prior_year:
                        aux = 'year={}'.format(year)
                        l_months = ['month={}'.format(month)]
                    elif prior_year == year:
                        l_months.append('month={}'.format(month))
                    elif prior_year != year:
                        if len(l_months) == 1:
                            elem = '({} AND {})'.format(aux, l_months[0])
                        else:
                            elem = "({} AND ({}))".format(aux, ' OR '.join(l_months))
                        l_months_final.append(elem)
                        aux = 'year={}'.format(year)
                        l_months = ['month={}'.format(month)]
                    if last_element:
                        if len(l_months) == 1:
                            elem = '({} AND {})'.format(aux, l_months[0])
                        else:
                            elem = "({} AND ({}))".format(aux, ' OR '.join(l_months))
                        l_months_final.append(elem)
                        aux = 'year={}'.format(year)
                        l_months = ['month={}'.format(month)]
                    prior_year = year
                if key == 'days':
                    if not prior_year_month:
                        aux = 'year={} AND month={}'.format(year, month)
                        l_days = ['day={}'.format(day)]
                    elif prior_year_month == int(str(year) + str(month)):
                        l_days.append('day={}'.format(day))
                    else:
                        if len(l_days) == 1:
                            elem = '({} AND {})'.format(aux, l_days[0])
                        else:
                            elem = "({} AND ({}))".format(aux, ' OR '.join(l_days))
                        l_months_final.append(elem)
                        aux = 'year={} AND month={}'.format(year, month)
                        l_days = ['day={}'.format(day)]
                    if last_element:
                        if len(l_days) == 1:
                            elem = '({} AND {})'.format(aux, l_days[0])
                        else:
                            elem = "({} AND ({}))".format(aux, ' OR '.join(l_days))
                        l_months_final.append(elem)
                        aux = 'year={} AND month={}'.format(year, month)
                        l_days = ['day={}'.format(day)]
                    prior_year_month = int(str(year) + str(month))
                if key == 'hours':
                    if not prior_year_month_day:
                        aux = 'year={} AND month={} AND day={}'.format(year, month, day)
                        l_hours = ['hour={}'.format(hour)]
                    elif prior_year_month_day == int(str(year) + str(month) + str(day)):
                        l_hours.append('hour={}'.format(hour))
                    else:
                        if len(l_hours) == 1:
                            elem = '({} AND {})'.format(aux, l_hours[0])
                        else:
                            elem = "({} AND ({}))".format(aux, ' OR '.join(l_hours))
                        l_months_final.append(elem)
                        aux = 'year={} AND month={} AND day={}'.format(year, month, day)
                        l_hours = ['hour={}'.format(hour)]
                    if last_element:
                        if len(l_hours) == 1:
                            elem = '({} AND {})'.format(aux, l_hours[0])
                        else:
                            elem = "({} AND ({}))".format(aux, ' OR '.join(l_hours))
                        l_months_final.append(elem)
                        aux = 'year={} AND month={} AND day={}'.format(year, month, day)
                        l_hours = ['hour={}'.format(hour)]
                    prior_year_month_day = int(str(year) + str(month) + str(day))
        l_filters = l_years_final + l_months_final + l_days_final + l_hours_final
        return '(' + ' OR '.join(l_filters) + ')'

    @staticmethod
    def reduces_dates(dates, level):
        """
        Reduce dates to the lower level of partition recursively
        :param dates: date [Datetime]
        :param level: level of reduction [String]
        :return: filter reduced and max level of reduction reached [Tuple]
        """
        l_dates = dict()
        l_dates[level] = dates
        max_reduced = False
        while not max_reduced:
            if level == 'hours':
                reduced = False
                iterdates = l_dates[level]
                new_dates = []
                new_level = 'days'
                delta = {new_level: 1, level: -1}
                last_element = False
                while not last_element:
                    max_index = len(iterdates) - 1
                    for idx, dt in enumerate(iterdates):
                        if idx == max_index:
                            last_element = True
                        if dt.hour == 0:
                            dt_limit = dt + relativedelta(**delta)
                            if dt_limit in iterdates:
                                reduced = True
                                limit_index = iterdates.index(dt_limit) + 1
                                index_range = range(idx, limit_index)
                                iterdates = [x for i, x in enumerate(iterdates) if i not in index_range]
                                new_dates += [dt]
                                if (limit_index - 1) == max_index:
                                    last_element = True
                                break
                if reduced:
                    l_dates[level] = iterdates
                    l_dates[new_level] = new_dates
                    level = new_level
                else:
                    max_reduced = True
            elif level == 'days':
                reduced = False
                iterdates = l_dates[level]
                new_dates = []
                new_level = 'months'
                delta = {new_level: 1, level: -1}
                last_element = False
                while not last_element:
                    max_index = len(iterdates) - 1
                    for idx, dt in enumerate(iterdates):
                        if idx == max_index:
                            last_element = True
                        if dt.day == 1:
                            dt_limit = dt + relativedelta(**delta)
                            if dt_limit in iterdates:
                                reduced = True
                                limit_index = iterdates.index(dt_limit) + 1
                                index_range = range(idx, limit_index)
                                iterdates = [x for i, x in enumerate(iterdates) if i not in index_range]
                                new_dates += [dt]
                                if (limit_index - 1) == max_index:
                                    last_element = True
                                break
                if reduced:
                    l_dates[level] = iterdates
                    l_dates[new_level] = new_dates
                    level = new_level
                else:
                    max_reduced = True
            elif level == 'months':
                reduced = False
                iterdates = l_dates[level]
                new_dates = []
                new_level = 'years'
                delta = {new_level: 1, level: -1}
                last_element = False
                while not last_element:
                    max_index = len(iterdates) - 1
                    for idx, dt in enumerate(iterdates):
                        if idx == max_index:
                            last_element = True
                        if dt.month == 1:
                            dt_limit = dt + relativedelta(**delta)
                            if dt_limit in iterdates:
                                reduced = True
                                limit_index = iterdates.index(dt_limit) + 1
                                index_range = range(idx, limit_index)
                                iterdates = [x for i, x in enumerate(iterdates) if i not in index_range]
                                new_dates += [dt]
                                if (limit_index - 1) == max_index:
                                    last_element = True
                                break
                if reduced:
                    l_dates[level] = iterdates
                    l_dates[new_level] = new_dates
                    level = new_level
                max_reduced = True
            else:
                max_reduced = True
        for key in ['hours', 'days', 'months', 'years']:
            if key in l_dates:
                if not l_dates[key]:
                    del l_dates[key]
        return l_dates

    def __repr__(self):
        """
        Object string representation
        :return: str representation of this object
        """
        return str(self)
