#!/usr/bin/python3

from airflow.models import BaseOperator
from airflow.utils.decorators import apply_defaults
from pyzonda.hadoop import HDFS
import subprocess
import re
import os
import io
from datetime import datetime
from pathlib import Path

REGEX_HDFS_FILE = '^hdfs:/*||^/*'
REGEX_DATE_FILE = '(^.*([12]\d{3}(0[1-9]|1[0-2])(0[1-9]|[12]\d|3[01])).*$)'
REGEX_IS_FILE = '(\/.*?\.[\w:]+)'


class HDFSOperation:
    GET = 'get'
    PUT = 'put'
    RM = 'rm'
    MV = 'mv'
    CP = 'cp'
    LS = 'ls'
    COPYTOLOCAL = 'copytolocal'
    MOVETOLOCAL = 'movetolocal'
    COUNT = 'count'
    MKDIR = 'mkdir'


class ZondaHDFSOperator(BaseOperator):
    template_fields = ['operation', 'local_path', 'remote_path', 'hdfs_source_path', 'hdfs_destination_path', 'overwrite']

    @apply_defaults
    def __init__(self, operation=HDFSOperation.PUT, local_path=None, remote_path=None, hdfs_source_path=None, hdfs_destination_path=None, *args, **kwargs):
        """
        Constructor
        """
        super(ZondaHDFSOperator, self).__init__(*args, **kwargs)
        self.operation = operation
        self.local_path = local_path
        self.remote_path = remote_path
        self.hdfs_source_path = hdfs_source_path
        self.hdfs_destination_path = hdfs_destination_path
        self.skip_errors = kwargs.get("skip_errors", False)
        self.overwrite = kwargs.get("overwrite", True)
        self.kwargs = kwargs

    def execute(self, context):
        """
        Execute HiveOperator Task
        :param context:
        :return:
        """
        result = []
        if self.operation.lower() in [HDFSOperation.PUT, HDFSOperation.LS, HDFSOperation.CP, HDFSOperation.COPYTOLOCAL, HDFSOperation.MOVETOLOCAL]:
            if self.operation.lower() == HDFSOperation.LS:
                if not self.remote_path:
                    raise Exception('Missing argument remote_path')

                if not re.match(REGEX_HDFS_FILE, self.remote_path):
                    raise Exception("Malformed remote path '{}'".format(self.remote_path))
                cmd = 'hdfs dfs -{} {}'.format(self.operation.lower(), self.remote_path).split()
                self.log.info('Executing command ' + ' '.join(cmd))

                p = subprocess.Popen(cmd, stdout=subprocess.PIPE, stderr=subprocess.STDOUT)

                for line in io.TextIOWrapper(p.stdout, encoding="utf-8"):
                    self.log.info(line.strip())

                p.wait()
                if p.returncode != 0 and not self.skip_errors:
                    raise Exception()

            elif self.operation.lower() == HDFSOperation.PUT:
                if not self.local_path:
                    self.local_path = []

                if not isinstance(self.local_path, (str, list)):
                    raise Exception('Invalid data type for local_path argument. Only string and list data types are supported')
                elif isinstance(self.local_path, str):
                    self.local_path = self.local_path.strip("'] ['").split("', '")

                for local_path in self.local_path:
                    flag_error = False
                    cmd = ['hdfs', 'dfs', '-' + self.operation.lower()]
                    remote_path = self.remote_path

                    if remote_path:
                        if not re.match(REGEX_HDFS_FILE, remote_path):
                            raise Exception("Malformed remote path '{}'".format(remote_path))
                    else:
                        # prefix preparation
                        default_prefix = str(Path(local_path).parent)
                        prefix = self.kwargs.get('prefix')
                        replace_prefix = self.kwargs.get('replace_prefix')

                        if not prefix and not replace_prefix:
                            prefix = default_prefix
                            replace_prefix = default_prefix
                        elif not prefix and replace_prefix:
                            prefix = default_prefix
                        elif prefix and not replace_prefix:
                            replace_prefix = prefix

                        local_path = local_path.replace(r'\udcf3', '?')
                        # generate remote path
                        remote_path = str(Path(local_path.replace(prefix, replace_prefix)).parent)

                    if not re.match(REGEX_IS_FILE, remote_path):
                        # check partition by date option
                        if self.kwargs.get('partitioned_by_date'):
                            check = re.match(REGEX_DATE_FILE, local_path)
                            last_modification_date = datetime.fromtimestamp(os.path.getmtime(local_path))
                            if check:
                                last_modification_date = datetime.strptime(check[2], '%Y%m%d')

                            suffix = 'partition_date={}-{}-{}'.format(last_modification_date.year, str(last_modification_date.month).zfill(2), str(last_modification_date.day).zfill(2))
                            # suffix = '{}/{}/{}'.format(last_modification_date.year, str(last_modification_date.month).zfill(2), str(last_modification_date.day).zfill(2))

                            # miscorp fix
                            if remote_path.startswith('/santander/bi-corp/landing/control_gestion'):
                                remote_path = str(Path(remote_path).parent)

                            remote_path = os.path.join(remote_path, suffix)

                        # create dir if not exists
                        tmp = 'hdfs dfs -mkdir -p {}'.format(remote_path).split()

                        p = subprocess.Popen(tmp, stdout=subprocess.PIPE, stderr=subprocess.STDOUT)

                        for line in io.TextIOWrapper(p.stdout, encoding="utf-8"):
                            self.log.info(line.strip())

                        p.wait()
                        if p.returncode != 0 and not self.skip_errors:
                            raise Exception()

                    if self.kwargs.get('overwrite', True):
                        cmd.append('-f')

                    cmd = cmd + [local_path, remote_path]
                    self.log.info('Executing command ' + ' '.join(cmd))
                    p = subprocess.Popen(cmd, stdout=subprocess.PIPE, stderr=subprocess.STDOUT, shell=False)
                    p.wait()
                    if p.returncode != 0:
                        flag_error = True
                        if not self.skip_errors:
                            raise Exception()

                    # remove local file
                    if self.kwargs.get('remove_local_path', False):
                        try:
                            os.remove(local_path)
                        except Exception as e:
                            if not self.skip_errors:
                                raise e
                            else:
                                flag_error = True
                                self.log.error("Error removing file {}".format(local_path))
                                pass

                    if not flag_error:
                        result.append(os.path.join(remote_path, os.path.basename(local_path)))

            elif self.operation.lower() == HDFSOperation.CP:
                hdfs_destination = ""
                if re.match(REGEX_DATE_FILE, self.hdfs_source_path):
                    split_source = self.hdfs_source_path.split('/')[-1]
                    check = re.match(REGEX_DATE_FILE, split_source)
                    destination_name = split_source.replace(check[2], '')
                    hdfs_destination = self.hdfs_destination_path + destination_name

                if self.kwargs.get('partitioned_by_date'):
                    # prefix preparation
                    default_prefix = str(Path(self.hdfs_source_path).parent)
                    prefix = self.kwargs.get('prefix')
                    replace_prefix = self.kwargs.get('replace_prefix')

                    if not prefix and not replace_prefix:
                        prefix = default_prefix
                        replace_prefix = default_prefix
                    elif not prefix and replace_prefix:
                        prefix = default_prefix
                    elif prefix and not replace_prefix:
                        replace_prefix = prefix

                    # generate remote path
                    hdfs_destination = str(Path(self.hdfs_source_path.replace(prefix, replace_prefix)).parent)

                    if not HDFS.exists(hdfs_destination):
                        tmp = 'hdfs dfs -mkdir -p {}'.format(hdfs_destination).split()
                        p = subprocess.Popen(tmp, stdout=subprocess.PIPE, stderr=subprocess.STDOUT)
                        for line in io.TextIOWrapper(p.stdout, encoding="utf-8"):
                            self.log.info(line.strip())

                        p.wait()
                        if p.returncode != 0 and not self.skip_errors:
                            raise Exception()

                if not self.hdfs_source_path:
                    raise Exception('HDFS source cannot be null')

                if not HDFS.exists(self.hdfs_source_path):
                    raise Exception('Invalid HDFS Source Path: {}'.format(self.hdfs_source_path))

                if not self.hdfs_destination_path:
                    raise Exception('HDFS destination cannot be null')

                command = 'hdfs dfs -{} -f {} {}'.format(self.operation.lower(), self.hdfs_source_path, hdfs_destination).split()

                self.log.info('Executing command ' + ' '.join(command))
                p = subprocess.Popen(command, stdout=subprocess.PIPE, stderr=subprocess.STDOUT, shell=False)

                for line in io.TextIOWrapper(p.stdout, encoding="utf-8"):
                    self.log.info(line.strip())

                p.wait()
                if p.returncode != 0:
                    if not self.skip_errors:
                        raise Exception()

            elif self.operation.lower() in (HDFSOperation.MOVETOLOCAL, HDFSOperation.COPYTOLOCAL):
                if not self.remote_path:
                    raise Exception('Missing argument remote_path')

                if not self.local_path:
                    raise Exception('Missing argument local_path')

                if not HDFS.exists(self.remote_path):
                    raise Exception('Remote path {} does not exist'.format(self.remote_path))

                if not re.match(REGEX_HDFS_FILE, self.remote_path):
                    raise Exception("Malformed remote path '{}'".format(self.remote_path))

                if not Path(self.local_path).parent.exists():
                    raise Exception("Local path provided does not exist '{}'".format(self.local_path))

                # remove local file if exists
                if self.overwrite:
                    if Path(self.local_path).is_file():
                        os.remove(self.local_path)

                cmd = 'hdfs dfs -{} {} {}'.format('copyToLocal' if self.operation.lower() == HDFSOperation.COPYTOLOCAL else 'moveToLocal', self.remote_path, self.local_path).split()
                self.log.info('Executing command ' + ' '.join(cmd))

                p = subprocess.Popen(cmd, stdout=subprocess.PIPE, stderr=subprocess.STDOUT)

                for line in io.TextIOWrapper(p.stdout, encoding="utf-8"):
                    self.log.info(line.strip())

                p.wait()
                if p.returncode != 0 and not self.skip_errors:
                    raise Exception()

        else:
            if not self.skip_errors:
                raise Exception('Invalid HDFS operation or not supported ({})'.format(self.operation))

        return result

    def __repr__(self):
        """
        Object string representation
        :return: str representation of this object
        """
        return str(self)
