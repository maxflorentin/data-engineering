#!/usr/bin/python3

import glob
import os
import io
import subprocess
import urllib
from datetime import timedelta
from time import sleep
from airflow.exceptions import AirflowSensorTimeout, AirflowSkipException, AirflowRescheduleException
from airflow.models.taskreschedule import TaskReschedule
from airflow.sensors.base_sensor_operator import BaseSensorOperator
from airflow.utils import timezone
from airflow.utils.decorators import apply_defaults


class ZondaFileSensor(BaseSensorOperator):
    template_fields = ['path', 'change_owner', 'poke_interval', 'timeout', 'file_size_in_bytes', 'ignored_ext']

    @apply_defaults
    def __init__(self, path, change_owner=None, poke_interval=60, timeout=60 * 60 * 24, file_size_in_bytes=None, ignored_ext=None, *args, **kwargs):
        """
        Constructor
        """
        super(ZondaFileSensor, self).__init__(*args, **kwargs)
        self.is_dir = os.path.isdir(path)
        path_splitted = path.split(os.sep)
        self.path = os.sep.join(path_splitted + [''] if path_splitted[-1] != '' and self.is_dir else path_splitted)
        self.poke_interval = poke_interval
        self.timeout = timeout
        self.soft_fail = kwargs.get('soft_fail', False)
        self.file_size_in_bytes = file_size_in_bytes or 0
        self.ignored_ext = ignored_ext or []
        self.change_owner = change_owner
        self.files = []

    def execute(self, context):
        started_at = timezone.utcnow()
        if self.reschedule:
            # If reschedule, use first start date of current try
            task_reschedules = TaskReschedule.find_for_task_instance(context['ti'])
            if task_reschedules:
                started_at = task_reschedules[0].start_date
        while not self.poke(context):
            if (timezone.utcnow() - started_at).total_seconds() > self.timeout:
                # If sensor is in soft fail mode but will be retried then
                # give it a chance and fail with timeout.
                # This gives the ability to set up non-blocking AND soft-fail sensors.
                if self.soft_fail and not context['ti'].is_eligible_to_retry():
                    self._do_skip_downstream_tasks(context)
                    raise AirflowSkipException('Snap. Time is OUT.')
                else:
                    raise AirflowSensorTimeout('Snap. Time is OUT.')
            if self.reschedule:
                reschedule_date = timezone.utcnow() + timedelta(
                    seconds=self.poke_interval)
                raise AirflowRescheduleException(reschedule_date)
            else:
                sleep(self.poke_interval)

        # change owner
        final_files = []
        if self.change_owner:
            for file in self.files:
                # filename_formatted = '{}'.format(file) if len(file.split()) > 1 else file
                cmd = ["sudo", "chown", self.change_owner, file]
                # print("FILENAME: " + filename_formatted)
                # self.log.info("CMD: " + " ".join(cmd))
                # cmd = 'sudo chown {} {}'.format(self.change_owner, file).split()
                p = subprocess.Popen(cmd, stdout=subprocess.PIPE, stderr=subprocess.STDOUT)

                for line in io.TextIOWrapper(p.stdout, encoding="utf-8"):
                    self.log.info(line.strip())

                p.wait()
                if p.returncode != 0:
                    raise Exception()

        self.log.info("Success criteria met")
        self.log.info('')
        self.log.info('FILES:')
        i = 1
        for e in self.files:
            size_in_bytes = os.path.getsize(e)

            self.log.info("   >> File {}: {} [{}]".format(str(i).zfill(4), e, ZondaFileSensor.format_size_in_bytes(size_in_bytes)))
            i += 1

        self.log.info('')
        self.log.info("Exiting.")
        return self.files

    @staticmethod
    def format_size_in_bytes(value):
        """
        Format output of size in the corresponding measure
        :param value:
        :return:
        """
        factors = [
            {
                'Unit': 'PB',
                'Factor': 0.000000000000000888174399804592
            },
            {
                'Unit': 'TB',
                'Factor': 0.0000000000009094905853999023
            },
            {
                'Unit': 'GB',
                'Factor': 0.0000000009313183594495
            },
            {
                'Unit': 'MB',
                'Factor': 0.00000095367
            },
            {
                'Unit': 'KB',
                'Factor': 0.000976563
            },
            {
                'Unit': 'B',
                'Factor': 1
            },
        ]

        for f in factors:
            conversion = value * f['Factor']
            if conversion >= 1:
                return '{} {}'.format('%.2f' % conversion, f['Unit'])

        return '{} B'.format('%.2f' % value)

    def poke(self, context):
        """
        Poke for files
        :return:
        """
        try:
            files = self._get_files()
            self.log.info('Poking for file {}...'.format(self.path))
            if len(files):
                tmp_files = dict()
                sleep(10)

                for k, v in files.items():
                    tmp_size = os.path.getsize(k)
                    if tmp_size == v:
                        tmp_files[k] = tmp_size

                if len(tmp_files):
                    self.files = [k for k, v in tmp_files.items()]
                    return True
                else:
                    return False
            else:
                return False
        except Exception:
            return False

    def _get_files(self):
        """
        Get list of files filtered by size and extension
        :return:
        """
        files = dict()
        search_path = self.path + '**/*' if self.is_dir else self.path
        for filename in glob.iglob(search_path, recursive=True):
            if not os.path.isdir(filename) and os.path.getsize(filename) >= self.file_size_in_bytes and not any(filename.endswith(x) for x in self.ignored_ext):
                # print("FILENAME A:" + filename)
                files[filename] = os.path.getsize(filename)

        return files

    def __repr__(self):
        """
        Object string representation
        :return: str representation of this object
        """
        return str(self)
