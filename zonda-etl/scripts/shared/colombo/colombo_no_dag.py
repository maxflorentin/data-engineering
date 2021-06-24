#!/usr/bin/python3

import datetime
import errno
import glob
import os
import re
import yaml
import json
import getpass
from cerberus import Validator

AIRFLOW_HOME = os.getenv('AIRFLOW_HOME')
SERVICE_USER = os.getenv('SERVICE_USER')
DEFAULT_SCHEMA_FILE = os.sep.join(os.path.abspath(__file__).split(os.sep)[:-1]) + os.sep + 'schema.json'
TESTING_PREFIXES = ['_test', '_testing', 'test_', 'testing_']
ENVIRONMENT = os.getenv('GIT_BRANCH', 'master').lower()
OPERATORS_ALLOWED_FIELDS = {
    'ZondaMoveItOperator': ['connection_id', 'path', 'table', 'shift', 'regexp_date_files', 'filewatch_ttl_min', 'model', 'hdfs_host', 'hdfs_port', 'hdfs_user_name', 'delta'],
    'ZondaHDFSOperator': ['operation', 'local_path', 'remote_path', 'prefix', 'replace_prefix', 'overwrite', 'partitioned_by_date', 'remove_local_path', 'skip_errors', 'hdfs_source_path', 'hdfs_destination_path'],
    'ZondaS3Operator': ['operation', 'local_path', 'remote_path', 'prefix', 'replace_prefix', 'overwrite','s3_bucket','source_s3_conn_id', 'partitioned_by_date', 'remove_local_path', 'skip_errors'],
    'ZondaFileSensor': ['path', 'poke_interval', 'change_owner', 'timeout', 'file_size_in_bytes', 'ignored_ext', 'soft_fail'],
    'ZondaDagSensor': ['external_dag_id', 'execution_date', 'timeout', 'soft_fail'],
    'ZondaHiveOperator': ['hql', 'is_hql_file', 'connection_id', 'schema', 'delimiter', 'output_file', 'show_header', 'output_format', 'empty_file'],
    'ZondaSlackOperator': ['connection_id', 'text', 'status', 'channels', 'users'],
    'ZondaGCSOperator': ['connection_id', 'bucket', 'prefix', 'destination', 'sql', 'is_sql_file'],
    'BashOperator': ['bash_command', 'file'],
    'SSHOperator': ['connection_id', 'command', 'timeout'],
    'HiveOperator': ['hql', 'is_hql_file', 'connection_id', 'schema', 'mapred_queue', 'mapred_queue', 'mapred_job_name', 'mapred_queue_priority'],
    'SparkSubmitOperator': ['name', 'application', 'java_class', 'application_args', 'executor_cores', 'executor_memory', 'num_executors', 'files', 'connection_id', 'verbose', 'spark_binary', 'py_files', 'conf', 'jars'],
    'SFTPOperator': ['connection_id', 'local_filepath', 'remote_filepath', 'operation', 'confirm', 'create_intermediate_dirs'],
    'TriggerDagRunOperator': ['trigger_dag_id'],
    'ZondaTriggerDagRunOperator': ['trigger_dag_id', 'conf'],
    'SqoopOperator': ['connection_id', 'cmd_type', 'table', 'query', 'target_dir', 'append', 'file_type', 'columns', 'num_mappers', 'split_by', 'where', 'export_dir'],
    'BranchPythonOperator': ['function_name', 'function_def', 'op_args', 'op_kwargs', 'provide_context'],
    'PythonOperator': ['function_name', 'function_def', 'op_args', 'op_kwargs', 'provide_context'],
    'ZondaGDOperator': ['hdfs_host', 'hdfs_port', 'hdfs_user_name', 'path', 'threshold'],
    'ZondaTableauUpdateOperator': ['url', 'content_url'],
    'ZondaControlmOperator': ['job_name'],
    'MySqlOperator': ['connection_id', 'sql']
}


class DAGBuilder(object):
    def __init__(self, filepath):
        """
        Constructor
        """
        self.filepath = filepath
        self.config = self._read_config()
        self.is_subdag = False
        self.validator = SchemaValidator()
        self.dag = self._build_dag()

    def get_config(self):
        """
        Get YAML configuration
        :return:
        """
        return yaml.dump(self.config)

    def is_active(self):
        """
        DAG status
        :return:
        """
        return self.dag['active']

    def get_name(self):
        """
        Return dag name
        :return:
        """
        return self.dag['name'] or 'UNKNOWN'

    def get_status(self):
        """
        Return status
        :return:
        """
        return self.dag['status']

    def set_status(self, status):
        """
        Set status
        :return:
        """
        self.dag['status'] = status

    def get_code(self, dag=None):
        """
        Return python code beautified
        :param dag: dag definition to be rendered
        :return:
        """
        dag = dag or self.dag
        code = []
        indent = " " * 4

        for i in dag['code']:
            section = dag['code'][i]
            blocks = section.get('blocks')
            min_indent_factor = min([x.get('indent', 0) for x in blocks])

            if len(blocks) and any(x['active'] for x in blocks):
                comments = section.get('comments')
                if comments:
                    code.append(indent * min_indent_factor + '# {}'.format(comments))

                for b in blocks:
                    indent_factor = b.get('indent', 0)
                    is_active = b.get('active')
                    if is_active:
                        lines_body = b.get('lines').split('\n')
                        for l in lines_body:
                            code.append(indent * indent_factor + os.path.expandvars(l))
        code.append('\r')

        return code

    def _read_config(self):
        """
        Read configuration file
        :return:
        """
        # validate schema
        v = SchemaValidator()
        check, errors = v.is_valid_schema(path=self.filepath)
        if not check:
            if errors.get('error', 'none') == 'Invalid YML format, check file indentation':
                raise Exception(errors.get('error'))
            else:
                raise Exception('INVALID_SCHEMA')

        # read configuration
        config = None
        with open(self.filepath, 'r') as stream:
            try:
                config = yaml.safe_load(stream)
            except yaml.YAMLError as ex:
                raise Exception('ERROR: ' + str(ex))

        return config

    @staticmethod
    def to_snake_case(name):
        name = name.replace('-', '_')

        s1 = re.sub('(.)([A-Z][a-z]+)', r'\1_\2', name)
        s2 = re.sub('([a-z0-9])([A-Z])', r'\1_\2', s1).lower()

        return re.sub('_+', '_', s2)

    def build_operators_args(self, **kwargs):
        """
        Build string of params to be used in an operator
        :param kwargs: dictionary of params
        :return: params stringify
        """
        args = ""
        for k, v in kwargs.items():
            tmp = ''
            args += '{}='.format(k)

            # list arg
            if isinstance(v, list):
                for e in v:
                    if isinstance(e, str):
                        e_splitted = e.split(self.loop_iterator_var)
                        e_final = []

                        for i in range(0, len(e_splitted)):
                            e_final.append(e_splitted[i])

                            if i < (len(e_splitted) - 1):
                                e_final.append('" + e + "')

                        tmp += '"{}", '.format(''.join(e_final))
                    else:
                        e_splitted = e.split(self.loop_iterator_var)
                        e_final = []

                        for i in range(0, len(e_splitted)):
                            e_final.append(e_splitted[i])

                            if i < (len(e_splitted) - 1):
                                e_final.append(' + e + ')

                        tmp += '{}, '.format(''.join(e_final))

                args += "[{}], ".format(tmp)

            # dict arg
            elif isinstance(v, dict):
                for x, y in v.items():
                    y_splitted = y.split(self.loop_iterator_var)
                    y_final = []

                    if isinstance(y, str):
                        for i in range(0, len(y_splitted)):
                            y_final.append(y_splitted[i])

                            if i < (len(y_splitted) - 1):
                                y_final.append('" + e + "')

                        tmp += '"{}": "{}", '.format(x, ''.join(y_final))
                    else:
                        for i in range(0, len(y_splitted)):
                            y_final.append(y_splitted[i])

                            if i < (len(y_splitted) - 1):
                                y_final.append(' + e + ')

                        tmp += '"{}": {}, '.format(x, ''.join(y_final))

                args += "{{{}}}, ".format(tmp)
            elif isinstance(v, int) or isinstance(v, float) or k in ('dag', 'subdag', 'python_callable'):
                args += "{}, ".format(v)
            elif any(re.match(x, v) for x in ['^open\((.*).*\$', 'start_date', '^datetime.*']):
                open_search = re.search('(^open\()(.*)(,.*)$', v, re.IGNORECASE)
                if open_search:
                    v_splitted = open_search.group(2).split(self.loop_iterator_var)
                    v_final = []

                    for i in range(0, len(v_splitted)):
                        v_final.append(v_splitted[i])

                        if i < (len(v_splitted) - 1):
                            v_final.append('" + e + "')
                    args += '{}{}{}, '.format(open_search.group(1), ''.join(v_final), open_search.group(3))

            else:
                v_splitted = v.split(self.loop_iterator_var)
                v_final = []

                for i in range(0, len(v_splitted)):
                    v_final.append(v_splitted[i])

                    if i < (len(v_splitted) - 1):
                        v_final.append('" + e + "')
                args += '"{}", '.format(''.join(v_final))

        return args[0:-2].replace(' + ""', "")

    def _build_subdag(self, task, dag_name, **kwargs):
        """
        Build subdag code
        :return:
        """
        dag = {'name': dag_name,
               'active': False,
               'status': 'NA',
               'code': dict()}

        parent_dag_name = 'subdag_{}'.format(self.to_snake_case(dag_name.split('.')[1]))
        dag_block = '{} = DAG(dag_id=dag_id, '.format(parent_dag_name) + \
                    'schedule_interval=schedule_interval, ' + \
                    'start_date=start_date, ' + \
                    'params=params' + \
                    ')\n'

        dag['code']['dag'] = {'comments': 'dag definition', 'blocks': [{'lines': dag_block, 'active': True}]}
        tasks = []
        task_block = ''
        counter = 0
        config = task.get('config')
        with_dependencies = True

        if task.get('operator').lower() == 'hiveoperator':
            """
            +---------------+
            | HIVE OPERATOR |
            +---------------+
            """
            operator = 'HiveOperator'

            config = ({k: v for k, v in config.items() if k in OPERATORS_ALLOWED_FIELDS[operator]})
            hql_config = config.get('hql')

            config["hive_cli_conn_id"] = config.pop("connection_id")

            if not config.get('mapred_queue'):
                config['mapred_queue'] = 'root.dataeng'

            for v in hql_config:
                variable_name = 'task_{}'.format(str(counter + 1).zfill(3))

                # update config
                config['task_id'] = 'Task{}'.format(str(counter + 1).zfill(3))
                config['dag'] = parent_dag_name
                config['hql'] = v if not kwargs.get('is_hql_file') else 'open("{}", "r").read()'.format(v)

                # build operator
                task_block = '{} = {}({})'.format(variable_name, operator, self.build_operators_args(**config))
                tasks.append({'lines': task_block, 'active': True})
                counter += 1

        if task.get('operator').lower() == 'zondahiveoperator':
            """
            +---------------------+
            | ZONDA HIVE OPERATOR |
            +---------------------+
            """
            operator = 'ZondaHiveOperator'

            config = ({k: v for k, v in config.items() if k in OPERATORS_ALLOWED_FIELDS[operator]})
            hql_config = config.get('hql')
            file_config = config.get('output_file')

            for v in hql_config:
                variable_name = 'task_{}'.format(str(counter + 1).zfill(3))

                # update config
                config['task_id'] = 'Task{}'.format(str(counter + 1).zfill(3))
                config['dag'] = parent_dag_name
                config['hql'] = v if not kwargs.get('is_hql_file') else 'open("{}", "r").read()'.format(v)

                if file_config:
                    config['output_format'] = config.get('output_format') or None
                    config['delimiter'] = config.get('delimiter') or ""
                    config['show_header'] = config.get('show_header') or 'true'
                    config['empty_file'] = config.get('empty_file') or True

                # build operator
                task_block = '{} = {}({})'.format(variable_name, operator, self.build_operators_args(**config))
                tasks.append({'lines': task_block, 'active': True})
                counter += 1

        elif task.get('operator').lower() == 'sshoperator':
            """
            +--------------+
            | SSH OPERATOR |
            +--------------+
            """
            operator = 'SSHOperator'

            config = ({k: v for k, v in config.items() if k in OPERATORS_ALLOWED_FIELDS[operator]})
            command_config = config.get('command')

            for cmd in command_config:
                variable_name = 'task_{}'.format(str(counter + 1).zfill(3))

                # update config
                config['task_id'] = 'Task{}'.format(str(counter + 1).zfill(3))
                config['dag'] = parent_dag_name
                config['command'] = cmd

                # build operator
                task_block = '{} = {}({})'.format(variable_name, operator, self.build_operators_args(**config))
                tasks.append({'lines': task_block, 'active': True})
                counter += 1

        elif task.get('operator').lower() == 'bashoperator':
            """
            +---------------+
            | BASH OPERATOR |
            +---------------+
            """
            operator = 'BashOperator'

            config = ({k: v for k, v in config.items() if k in OPERATORS_ALLOWED_FIELDS[operator]})
            bash_command_config = config.get('bash_command')

            for bash_command in bash_command_config:
                variable_name = 'task_{}'.format(str(counter + 1).zfill(3))

                # update config
                config['task_id'] = 'Task{}'.format(str(counter + 1).zfill(3))
                config['dag'] = parent_dag_name
                config['bash_command'] = bash_command + ' '

                # build operator
                task_block = '{} = {}({})'.format(variable_name, operator, self.build_operators_args(**config))
                tasks.append({'lines': task_block, 'active': True})
                counter += 1

        elif task.get('operator').lower() == 'zondafilesensor':
            """
            +-------------------+
            | ZONDA FILE SENSOR |
            +-------------------+
            """
            with_dependencies = False
            operator = 'ZondaFileSensor'

            config = ({k: v for k, v in config.items() if k in OPERATORS_ALLOWED_FIELDS[operator]})
            path_config = config.get('path')

            for path in path_config:
                variable_name = 'task_{}'.format(str(counter + 1).zfill(3))

                # update config
                config['task_id'] = 'Task{}'.format(str(counter + 1).zfill(3))
                config['dag'] = parent_dag_name
                config['path'] = path

                # build operator
                task_block = '{} = {}({})'.format(variable_name, operator, self.build_operators_args(**config))
                tasks.append({'lines': task_block, 'active': True})
                counter += 1

        tasks[-1]['lines'] = task_block + '\n'
        dag['code']['tasks'] = {'comments': 'tasks', 'blocks': tasks}

        # dependencies
        if with_dependencies:
            dependencies_block = ' >> '.join(['task_{}'.format(str(x).zfill(3)) for x in range(1, counter + 1)])
            dag['code']['dependencies'] = {'comments': 'dependencies',
                                           'blocks': [{'lines': dependencies_block + '\n', 'active': True}]}

        dag['code']['return'] = {'comments': None,
                                 'blocks': [{'lines': 'return {}'.format(parent_dag_name), 'active': True}]}
        dag['status'] = 'OK'

        return dag

    def _build_dag(self):
        """
        Build dag code
        :return:
        """
        dag = {'name': None,
               'active': False,
               'status': 'NA',
               'code': dict()}

        if not self.config:
            raise Exception("Could not load configuration file '{}'".format(self.filepath))

        """
        +-------------+
        | VALIDATIONS |
        +-------------+
        """
        if not self.config.get('name'):
            raise Exception("Missing property 'name' in configuration file '{}'".format(self.filepath))

        if not self.config.get('tasks'):
            raise Exception("Missing property 'tasks' in configuration file '{}'".format(self.filepath))

        dag_name = re.sub('\s', '_', self.config.get('name'))
        dag['name'] = dag_name
        dag['active'] = self.config.get('active', False)

        if not dag['active']:
            dag['status'] = 'INACTIVE_DAG'
            return dag

        """
        +----------------------+
        | SHEBANG AND COMMENTS |
        +----------------------+
        """
        shebang_block = '#!/usr/bin/python3\n'
        dag['code']['shebang'] = {'comments': None,
                                  'blocks': [{'lines': shebang_block, 'active': not self.is_subdag}]}

        comments_1 = '"""\n' + \
                     'Script autogenerated by Columbus\n\n' + \
                     'USER IDENTIFIER           {}\n'.format(getpass.getuser()) + \
                     'CREATED AT                {}\n'.format(datetime.datetime.utcnow()) + \
                     'CONFIGURATION FILE PATH   {}'.format(self.filepath)
        comments_2 = '"""\n'
        dag['code']['comments'] = {'comments': None,
                                   'blocks': [{'lines': comments_1, 'active': not self.is_subdag},
                                              {'lines': comments_2, 'active': not self.is_subdag}]}
        """
        +---------+
        | IMPORTS |
        +---------+
        """
        import_block = 'from airflow import DAG\n' + \
                       'from pendulum import timezone\n' + \
                       'from datetime import datetime, timedelta\n'

        dag['code']['import'] = {'comments': None, 'blocks': []}
        dag['code']['functions'] = {'comments': 'functions', 'blocks': []}

        """
        +---------------+
        | NOTIFICATIONS |
        +---------------+
        """
        notifications = self.config.get('notifications')
        notification_function_block = []
        alert_callback = []

        if notifications:
            channels = notifications.get('channels')
            users = notifications.get('users', None)
            emails = notifications.get('emails')
            notify_on_start = notifications.get('on_start', False)
            notify_on_finish = notifications.get('on_finish', False)
            notify_on_error = notifications.get('on_error', False)
            notify_on_success = notifications.get('on_success', False)

            if notify_on_success:
                notify_on_start = False
                notify_on_finish = False

            if notify_on_start or notify_on_finish:
                notify_on_success = True

            if (channels or emails) and (notify_on_error or notify_on_success):
                if channels:
                    # ONLY notification by Slack
                    if isinstance(channels, str):
                        channels = [channels]

                    dag['code']['notifications'] = {'comments': 'notifications',
                                                    'blocks': [{'lines': 'channels = {}'.format(str(channels)), 'active': True},
                                                               {'lines': 'users = {}\n'.format(str(users)), 'active': True}]}

                    if notify_on_error:
                        error_notifier_def_name = "on_error_notifier(context)"
                        notification_function_block.append({'lines': 'def {}:'.format(error_notifier_def_name), 'active': True})
                        notification_function_block.append({'lines': '"""\nSend error notification alerts\n"""', 'active': True, 'indent': 1})
                        notification_function_block.append({'lines': 'slack_alert_error = ZondaSlackOperator(task_id="slack_alert_error", status=1, channels=channels, users=users)', 'active': True, 'indent': 1})
                        notification_function_block.append({'lines': 'slack_response = slack_alert_error.execute(context=context)\n', 'active': True, 'indent': 1})
                        notification_function_block.append({'lines': 'return slack_response\n', 'active': True, 'indent': 1})

                        alert_callback.append('"on_failure_callback": on_error_notifier')

                    if notify_on_success:
                        success_notifier_def_name = "on_success_notifier(context)"
                        notification_function_block.append({'lines': 'def {}:'.format(success_notifier_def_name), 'active': True})

                        if notify_on_start and notify_on_finish:
                            notification_function_block.append({'lines': '"""\nSend success notification alerts only when the DAG starts and finishes\n"""', 'active': True, 'indent': 1})
                            notification_function_block.append({'lines': "slack_response = True", 'active': True, 'indent': 1})
                            notification_function_block.append({'lines': "init_last_tasks = [t.task_id for t in context.get('dag').tasks if not t.downstream_task_ids or not t.upstream_task_ids]\n", 'active': True, 'indent': 1})
                            notification_function_block.append({'lines': "if context.get('task').task_id in init_last_tasks:", 'active': True, 'indent': 1})
                            notification_function_block.append({'lines': 'slack_alert_success = ZondaSlackOperator(task_id="slack_alert_success", status=0, channels=channels)', 'active': True, 'indent': 2})
                            notification_function_block.append({'lines': 'slack_response = slack_alert_success.execute(context=context)\n', 'active': True, 'indent': 2})
                            notification_function_block.append({'lines': 'return slack_response\n', 'active': True, 'indent': 1})
                        elif notify_on_start:
                            notification_function_block.append({'lines': '"""\nSend success notification alerts only when the DAG starts\n"""', 'active': True, 'indent': 1})
                            notification_function_block.append({'lines': "slack_response = True", 'active': True, 'indent': 1})
                            notification_function_block.append({'lines': "init_tasks = [t.task_id for t in context.get('dag').tasks if not t.upstream_task_ids]\n", 'active': True, 'indent': 1})
                            notification_function_block.append({'lines': "if context.get('task').task_id in init_tasks:", 'active': True, 'indent': 1})
                            notification_function_block.append({'lines': 'slack_alert_success = ZondaSlackOperator(task_id="slack_alert_success", status=0, channels=channels)', 'active': True, 'indent': 2})
                            notification_function_block.append({'lines': 'slack_response = slack_alert_success.execute(context=context)\n', 'active': True, 'indent': 2})
                            notification_function_block.append({'lines': 'return slack_response\n', 'active': True, 'indent': 1})
                        elif notify_on_finish:
                            notification_function_block.append({'lines': '"""\nSend success notification alerts only when the DAG finishes\n"""', 'active': True, 'indent': 1})
                            notification_function_block.append({'lines': "slack_response = True", 'active': True, 'indent': 1})
                            notification_function_block.append({'lines': "last_tasks = [t.task_id for t in context.get('dag').tasks if not t.downstream_task_ids]\n", 'active': True, 'indent': 1})
                            notification_function_block.append({'lines': "if context.get('task').task_id in last_tasks:", 'active': True, 'indent': 1})
                            notification_function_block.append({'lines': 'slack_alert_success = ZondaSlackOperator(task_id="slack_alert_success", status=0, channels=channels)', 'active': True, 'indent': 2})
                            notification_function_block.append({'lines': 'slack_response = slack_alert_success.execute(context=context)\n', 'active': True, 'indent': 2})
                            notification_function_block.append({'lines': 'return slack_response\n', 'active': True, 'indent': 1})
                        else:
                            notification_function_block.append({'lines': '"""\nSend success notification alerts \n"""', 'active': True, 'indent': 1})
                            notification_function_block.append({'lines': 'slack_alert_success = ZondaSlackOperator(task_id="slack_alert_success", status=0, channels=channels)', 'active': True, 'indent': 1})
                            notification_function_block.append({'lines': 'slack_response = slack_alert_success.execute(context=context)\n', 'active': True, 'indent': 1})
                            notification_function_block.append({'lines': 'return slack_response\n', 'active': True, 'indent': 1})

                        alert_callback.append('"on_success_callback": on_success_notifier')

                    dag['code']['functions']['blocks'].extend(notification_function_block)

                    # update import block
                    import_op = 'from airflow.operators import ZondaSlackOperator\n'
                    if import_op not in import_block:
                        import_block += import_op

        """
        +-------------------+
        | DEFAULT ARGUMENTS |
        +-------------------+
        """
        # schedule interval
        schedule_interval = self.config.get('schedule_interval')
        if schedule_interval:
            schedule_interval = '"{}"'.format(schedule_interval)

        # start date
        start_date = datetime.datetime.strptime(self.config.get('start_date', '2019-01-01'), '%Y-%m-%d')
        utcnow = datetime.datetime.utcnow()
        # @TODO: update logic for different timezone than UTC
        tz = self.config.get('timezone', 'utc')

        # end date
        end_date = self.config.get('end_date', None)
        if end_date:
            end_date = datetime.datetime.strptime(end_date, '%Y-%m-%d')
            end_date_block = "end_date = datetime({}, {}, {}, {}, {}, {}, {}, tzinfo=timezone('{}'))".format(end_date.year, end_date.month, end_date.day, 0, 0, 0, 0, tz)
        else:
            end_date_block = "end_date = {}".format(end_date)
        utcnow = datetime.datetime.utcnow()
        # @TODO: update logic for different timezone than UTC
        tz = self.config.get('timezone', 'utc')



        # run as user
        user = self.config.get('run_as_user', SERVICE_USER)

        # max active runs
        max_active_runs = self.config.get('max_active_runs', 1)

        # catchup
        catchup = self.config.get('catchup')
        if not (catchup is not None and isinstance(catchup, bool)):
            catchup = False

        # input variables
        if self.config.get('input'):
            input_block = 'input_prompts = {'
            default_block = 'default_input = {'
            for e in self.config.get('input'):
                # validate input
                name = e.get('name')
                description = e.get('description', '')

                if not name:
                    raise Exception("Missing property 'name' in input configuration")

                default = e.get('default')
                if default:
                    default_block += '"{}": "{}", '.format(name, default)
                input_block += '"{}": "{}", '.format(name,
                                                     description if not default else description + ' [Default: {}]'.format(
                                                         default))

            input_block += '}'
            default_block += '}'
        else:
            input_block = 'input_prompts = {}'
            default_block = 'default_input = {}'

        # parameters
        if self.config.get('params'):
            params = dict()

            for p in self.config.get('params'):
                params[p['name']] = p['value']
            params_block = 'params = {}\n'.format(json.dumps(params))
        else:
            params_block = 'params = {}\n'

        # conf
        execution_date = datetime.datetime.now().date() - datetime.timedelta(1)
        default_conf = '{"date_from": "%s"}' % (execution_date)
        conf_line = "conf = dag_run.conf or {}".format(default_conf)
        conf_line_error = "conf = {}".format(default_conf)

        default_args_block = 'default_args = {' + \
                             '"owner": "{}", '.format(self.config.get('owner') or 'BI-Corp') + \
                             '"depends_on_past": {}, '.format(self.config.get('depends_on_past', 'False')) + \
                             '"wait_for_downstream": {}, '.format(self.config.get('wait_for_downstream', 'False')) + \
                             '"retries": {}, '.format(self.config.get('retries', 3)) + \
                             '"retry_delay": timedelta(seconds={}), '.format(str(self.config.get('retry_delay', 60))) + \
                             '"input": input_prompts, ' + \
                             '"provide_context": True, ' + \
                             '"start_date": start_date, ' + \
                             '"end_date": end_date, ' + \
                             '"run_as_user": "{}", '.format(user) + \
                             '{}'.format(', '.join(alert_callback)) + \
                             '}\n'

        dag['code']['default_args'] = {'comments': 'default args',
                                       'blocks': [{'lines': 'schedule_interval = {}'.format(schedule_interval), 'active': not self.is_subdag},
                                                  {'lines': "start_date = datetime({}, {}, {}, {}, {}, {}, {}, tzinfo=timezone('{}'))".format(start_date.year, start_date.month, start_date.day, 0, 0, 0, 0, tz), 'active': not self.is_subdag},
                                                  {'lines': end_date_block, 'active': not self.is_subdag},
                                                  {'lines': 'max_active_runs = {}'.format(max_active_runs), 'active': True},
                                                  {'lines': 'catchup = {}\n'.format(catchup), 'active': True},
                                                  {'lines': input_block, 'active': not self.is_subdag},
                                                  {'lines': default_block, 'active': not self.is_subdag},
                                                  {'lines': params_block, 'active': not self.is_subdag},
                                                  {'lines': default_args_block, 'active': True},
                                                  ]}

        """
        +-----+
        | DAG |
        +-----+
        """
        parent_dag_name = 'dag_{}'.format(self.to_snake_case(dag_name))

        dag_block = '{} = DAG(dag_id=dag_id, '.format(parent_dag_name) + \
                    'description="{}", '.format(self.config.get('description') or '') + \
                    'default_args=default_args, ' + \
                    'params=params, ' + \
                    'max_active_runs=max_active_runs, ' + \
                    'schedule_interval=schedule_interval, ' + \
                    'catchup=catchup' + \
                    ')\n'

        dag['code']['dag'] = {'comments': 'dag definition',
                              'blocks': [{'lines': 'dag_id = "{}"'.format(dag_name), 'active': not self.is_subdag},
                                         {'lines': dag_block, 'active': True}]}

        """
        +-----------+
        | ITERATION |
        +-----------+
        """
        loop_config = self.config.get('loop', {})
        loop_indent = 0 if not loop_config else 1
        loop_iterator = loop_config.get('iterator', 'iter_value')
        self.loop_iterator_var = "${{{}}}".format(loop_iterator)

        """
        +-------+
        | TASKS |
        +-------+
        """
        tasks = []
        is_there_input_task = '{}' not in input_block
        if is_there_input_task:
            input_function_block = [{'lines': 'def get_input_config(**kwargs):', 'active': not self.is_subdag},
                                    {'lines': default_block, 'active': not self.is_subdag, 'indent': 1},
                                    {'lines': "dag_run = kwargs.get('dag_run')\n", 'active': not self.is_subdag, 'indent': 1},
                                    {'lines': "try:", 'active': not self.is_subdag, 'indent': 1},
                                    {'lines': conf_line, 'active': not self.is_subdag, 'indent': 2},
                                    {'lines': "except AttributeError as _:", 'active': not self.is_subdag, 'indent': 1},
                                    {'lines': conf_line_error, 'active': not self.is_subdag, 'indent': 2},
                                    {'lines': "pass\n", 'active': not self.is_subdag, 'indent': 2},
                                    {'lines': "tmp = ZondaGenOperator(task_id='GenerateVariables', input=default_input)", 'active': not self.is_subdag, 'indent': 1},
                                    {'lines': "vars_generated = tmp.execute(context=kwargs)\n", 'active': not self.is_subdag, 'indent': 1},
                                    {'lines': "default_input.update({k: v for k, v in conf.items() if v})", 'active': not self.is_subdag, 'indent': 1},
                                    {'lines': "default_input.update({k: v for k, v in vars_generated.items() if v})\n", 'active': not self.is_subdag, 'indent': 1},
                                    {'lines': "for k, v in default_input.items():", 'active': not self.is_subdag, 'indent': 1},
                                    {'lines': "kwargs['ti'].xcom_push(key=k, value=v)\n", 'active': not self.is_subdag, 'indent': 2},
                                    {'lines': "kwargs['ti'].xcom_push(key='all', value=default_input)", 'active': not self.is_subdag, 'indent': 1},
                                    {'lines': "kwargs['ti'].xcom_push(key='context', value=str(kwargs))\n", 'active': not self.is_subdag, 'indent': 1}]

            dag['code']['functions']['blocks'].extend(input_function_block)

            # setup config
            config = dict()
            config['task_id'] = 'InputConfig'
            config['dag'] = parent_dag_name
            config['python_callable'] = 'get_input_config'
            config['provide_context'] = True

            task_block = 'input_task = PythonOperator({})'.format(self.build_operators_args(**config))
            tasks.append({'lines': task_block, 'active': True})

            # update import block
            import_op = 'from airflow.operators.python_operator import PythonOperator\n' \
                        'from airflow.operators import ZondaGenOperator\n'
            if import_op not in import_block:
                import_block += import_op

        is_there_dummy_task = self.config.get('include_dummy_task', True)
        if is_there_dummy_task:
            # setup config
            config = dict()
            config['task_id'] = 'StartExecution'
            config['dag'] = parent_dag_name

            task_block = 'dummy_task = DummyOperator({})'.format(self.build_operators_args(**config))
            tasks.append({'lines': task_block, 'active': True})

            # update import block
            import_op = 'from airflow.operators.dummy_operator import DummyOperator\n'
            if import_op not in import_block:
                import_block += import_op

        config_tasks = self.config.get('tasks')
        task_block = ''

        if loop_config:
            loop_block = {'lines': '\nfor {} in {}:'.format(loop_iterator, [loop_config['values']] if isinstance(loop_config['values'], str) else loop_config['values']), 'active': True}
            tasks.append(loop_block)

        for t in config_tasks:
            # common variables
            is_subdag = False
            subdag = None
            subdag_name = None
            config = dict()
            kwargs = dict()

            # validate task
            name = t.get('name')
            operator = t.get('operator')

            if not name:
                raise Exception("Missing property 'name' in step configuration")

            if not operator:
                raise Exception("Missing property 'operator' in step configuration '{}'".format(name))

            variable_name = self.to_snake_case(name)

            if operator.lower() == 'dummyoperator':
                """
                +----------------+
                | DUMMY OPERATOR |
                +----------------+
                """
                operator = 'DummyOperator'

                # setup config
                config['task_id'] = name
                config['dag'] = parent_dag_name

                # update import block
                import_op = 'from airflow.operators.dummy_operator import DummyOperator\n'
                if import_op not in import_block:
                    import_block += import_op

            elif operator.lower() == 'bashoperator':
                """
                +---------------+
                | BASH OPERATOR |
                +---------------+
                """
                operator = 'BashOperator'

                # validate and clean config
                config = t.get('config')
                if not config:
                    raise Exception("Missing property 'config' in task '{}'".format(name))

                config = ({k: v for k, v in config.items() if k in OPERATORS_ALLOWED_FIELDS[operator]})

                bash_command_config = config.get('bash_command')

                if not bash_command_config:
                    raise Exception("Missing property 'bash_command' in task configuration '{}'".format(name))

                if isinstance(bash_command_config, str):
                    config['bash_command'] += ' '

                if isinstance(bash_command_config, list) and len(bash_command_config) == 1:
                    config['bash_command'] = bash_command_config[0]
                    config['bash_command'] += ' '
                elif isinstance(bash_command_config, list):
                    operator = 'SubDagOperator'
                    is_subdag = True

                # update import block
                import_op = 'from airflow.operators.bash_operator import BashOperator\n'
                if import_op not in import_block:
                    import_block += import_op

            elif operator.lower() == 'zondafilesensor':
                """
                +-------------------+
                | ZONDA FILE SENSOR |
                +-------------------+
                """
                operator = 'ZondaFileSensor'

                # validate and clean config
                config = t.get('config')
                if not config:
                    raise Exception("Missing property 'config' in task '{}'".format(name))

                config = ({k: v for k, v in config.items() if k in OPERATORS_ALLOWED_FIELDS[operator]})

                path_config = config.get('path')

                if not path_config:
                    raise Exception("Missing property 'path' in task configuration '{}'".format(name))

                if isinstance(path_config, list) and len(path_config) == 1:
                    config['path'] = path_config[0]
                    config['bash_command'] += ' '
                elif isinstance(path_config, list):
                    operator = 'SubDagOperator'
                    is_subdag = True

                # update import block
                import_op = 'from airflow.sensors import ZondaFileSensor\n'
                if import_op not in import_block:
                    import_block += import_op

            elif operator.lower() == 'zondadagsensor':
                """
                +-------------------+
                |  ZONDA DAG SENSOR |
                +-------------------+
                """
                operator = 'ZondaDagSensor'

                # validate and clean config
                config = t.get('config')
                if not config:
                    raise Exception("Missing property 'config' in task '{}'".format(name))

                config = ({k: v for k, v in config.items() if k in OPERATORS_ALLOWED_FIELDS[operator]})

                external_dag_config = config.get('external_dag_id')
                execution_date_config = config.get('execution_date')

                if not external_dag_config:
                    raise Exception("Missing property 'external_dag_config' in task configuration '{}'".format(name))

                if not execution_date_config:
                    raise Exception("Missing property 'execution_date_config' in task configuration '{}'".format(name))

                config['external_dag_id'] = config.get('external_dag_id')
                config['execution_date'] = config.get('execution_date')

                # update import block
                import_op = 'from airflow.sensors import ZondaDagSensor\n'
                if import_op not in import_block:
                    import_block += import_op

            elif operator.lower() == 'sshoperator':
                """
                +--------------+
                | SSH OPERATOR |
                +--------------+
                """
                operator = 'SSHOperator'

                # validate and clean config
                config = t.get('config')
                if not config:
                    raise Exception("Missing property 'config' in task '{}'".format(name))

                config = ({k: v for k, v in config.items() if k in OPERATORS_ALLOWED_FIELDS[operator]})

                connection_config = config.get('connection_id')
                command_config = config.get('command')

                if not connection_config:
                    raise Exception("Missing property 'connection_id' in task configuration '{}'".format(name))

                if not command_config:
                    raise Exception("Missing property 'command' in task configuration '{}'".format(name))

                if isinstance(command_config, str):
                    is_subdag = False

                if isinstance(command_config, list) and len(command_config) == 1:
                    config['command'] = command_config[0]
                    is_subdag = False
                elif isinstance(command_config, list):
                    operator = 'SubDagOperator'
                    is_subdag = True

                config["ssh_conn_id"] = config.pop("connection_id")

                # update import block
                import_op = 'from airflow.contrib.operators.ssh_operator import SSHOperator\n'
                if import_op not in import_block:
                    import_block += import_op

            elif operator.lower() == 'hiveoperator':
                """
                +---------------+
                | HIVE OPERATOR |
                +---------------+
                """
                operator = 'HiveOperator'

                # validate and clean config
                config = t.get('config')
                if not config:
                    raise Exception("Missing property 'config' in task '{}'".format(name))

                config = ({k: v for k, v in config.items() if k in OPERATORS_ALLOWED_FIELDS[operator]})

                connection_config = config.get('connection_id')
                hql_config = config.get('hql')
                is_hql_file = config.get('is_hql_file', False)

                if not connection_config:
                    raise Exception("Missing property 'connection_id' in task configuration '{}'".format(name))

                if not hql_config:
                    raise Exception("Missing property 'hql' in task configuration '{}'".format(name))

                if isinstance(hql_config, str):
                    is_subdag = False

                if isinstance(hql_config, list) and len(hql_config) == 1:
                    hql_config = hql_config[0]
                    is_subdag = False
                elif isinstance(hql_config, list):
                    operator = 'SubDagOperator'
                    is_subdag = True

                config['hql'] = hql_config if not is_hql_file else 'open("{}", "r").read()'.format(hql_config)
                config["hive_cli_conn_id"] = config.pop("connection_id")

                if not config.get('mapred_queue'):
                    config['mapred_queue'] = 'root.dataeng'

                if config.get('is_hql_file'):
                    kwargs['is_hql_file'] = config.pop('is_hql_file')

                # update import block
                import_op = 'from airflow.operators.hive_operator import HiveOperator\n'
                if import_op not in import_block:
                    import_block += import_op

            elif operator.lower() == 'zondahiveoperator':
                """
                +---------------------+
                | ZONDA HIVE OPERATOR |
                +---------------------+
                """
                operator = 'ZondaHiveOperator'

                # validate and clean config
                config = t.get('config')
                if not config:
                    raise Exception("Missing property 'config' in task '{}'".format(name))

                config = ({k: v for k, v in config.items() if k in OPERATORS_ALLOWED_FIELDS[operator]})

                connection_config = config.get('connection_id')
                hql_config = config.get('hql')
                is_hql_file = config.get('is_hql_file', False)

                if not connection_config:
                    raise Exception("Missing property 'connection_id' in task configuration '{}'".format(name))

                if not hql_config:
                    raise Exception("Missing property 'hql' in task configuration '{}'".format(name))

                if isinstance(hql_config, str):
                    is_subdag = False

                if isinstance(hql_config, list) and len(hql_config) == 1:
                    hql_config = hql_config[0]
                    is_subdag = False
                elif isinstance(hql_config, list):
                    operator = 'SubDagOperator'
                    is_subdag = True

                config['hql'] = hql_config if not is_hql_file else 'open("{}", "r").read()'.format(hql_config)

                if config.get('is_hql_file'):
                    kwargs['is_hql_file'] = config.pop('is_hql_file')

                # update import block
                import_op = 'from airflow.operators import ZondaHiveOperator\n'
                if import_op not in import_block:
                    import_block += import_op

                """
                +------------------------------+
                | ZONDA TABLEU UPDATE OPERATOR |
                +------------------------------+
                """
            elif operator.lower() == 'zondatableauupdateoperator':

                operator = 'ZondaTableauUpdateOperator'

                # validate and clean config
                config = t.get('config')
                if not config:
                    raise Exception("Missing property 'config' in task '{}'".format(name))

                config = ({k: v for k, v in config.items() if k in OPERATORS_ALLOWED_FIELDS[operator]})

                url_config = config.get('url')
                content_url_config = config.get('content_url')

                if not url_config:
                    raise Exception("Missing property 'url_config' in task configuration '{}'".format(name))

                if not content_url_config:
                    raise Exception("Missing property 'content_url_config' in task configuration '{}'".format(name))

                # update import block
                import_op = 'from airflow.operators import ZondaTableauUpdateOperator\n'
                if import_op not in import_block:
                    import_block += import_op

                """
                +--------------------------+
                | ZONDA CONTROL M OPERATOR |
                +--------------------------+
                """
            elif operator.lower() == 'zondacontrolmoperator':

                operator = 'ZondaControlmOperator'

                # validate and clean config
                config = t.get('config')
                if not config:
                    raise Exception("Missing property 'config' in task '{}'".format(name))

                config = ({k: v for k, v in config.items() if k in OPERATORS_ALLOWED_FIELDS[operator]})

                job_name = config.get('job_name')

                if not job_name:
                    raise Exception("Missing property 'job_name' in task configuration '{}'".format(name))

                # update import block
                import_op = 'from airflow.operators import ZondaControlmOperator\n'
                if import_op not in import_block:
                    import_block += import_op

            elif operator.lower() == 'zondagcsoperator':
                """
                +---------------------+
                | ZONDA GCS OPERATOR |
                +---------------------+
                """
                operator = 'ZondaGCSOperator'

                # validate and clean config
                config = t.get('config')
                if not config:
                    raise Exception("Missing property 'config' in task '{}'".format(name))

                config = ({k: v for k, v in config.items() if k in OPERATORS_ALLOWED_FIELDS[operator]})

                connection_config = config.get('connection_id')
                bucket_config = config.get('bucket')
                prefix_config = config.get('prefix')
                destination_config = config.get('destination')
                sql_config = config.get('sql')
                is_sql_file = config.get('is_sql_file', False)

                if not connection_config:
                    raise Exception("Missing property 'connection_id' in task configuration '{}'".format(name))

                if not bucket_config:
                    raise Exception("Missing property 'bucket' in task configuration '{}'".format(name))

                if not prefix_config:
                    raise Exception("Missing property 'prefix' in task configuration '{}'".format(name))

                if not destination_config:
                    raise Exception("Missing property 'destination' in task configuration '{}'".format(name))

                if not sql_config:
                    raise Exception("Missing property 'sql' in task configuration '{}'".format(name))

                config['sql'] = sql_config if not is_sql_file else 'open("{}", "r").read()'.format(sql_config)

                if config.get('is_sql_file'):
                    kwargs['is_sql_file'] = config.pop('is_sql_file')

                # update import block
                import_op = 'from airflow.operators import ZondaGCSOperator\n'
                if import_op not in import_block:
                    import_block += import_op

            elif operator.lower() == 'zondagdoperator':
                """
                +---------------------+
                | ZONDA GD OPERATOR |
                +---------------------+
                """
                operator = 'ZondaGDOperator'

                # validate and clean config
                config = t.get('config')
                if not config:
                    raise Exception("Missing property 'config' in task '{}'".format(name))

                config = ({k: v for k, v in config.items() if k in OPERATORS_ALLOWED_FIELDS[operator]})

                hdfs_host_c = config.get('hdfs_host')
                hdfs_port_c = config.get('hdfs_port')
                hdfs_user_name_c = config.get('hdfs_user_name')
                path_c = config.get('path')
                threshold_c = config.get('threshold')

                if not path_c or not hdfs_host_c or not hdfs_user_name_c or not hdfs_port_c or not threshold_c:
                    raise Exception("Missing properties in task configuration '{}'".format(name))


                # update import block
                import_op = 'from airflow.operators import ZondaGDOperator\n'
                if import_op not in import_block:
                    import_block += import_op

            elif operator.lower() == 'sparksubmitoperator':
                """
                +-----------------------+
                | SPARK SUBMIT OPERATOR |
                +-----------------------+
                """
                operator = 'SparkSubmitOperator'

                # validate and clean config
                config = t.get('config')
                if not config:
                    raise Exception("Missing property 'config' in task '{}'".format(name))

                config = ({k: v for k, v in config.items() if k in OPERATORS_ALLOWED_FIELDS[operator]})

                connection_config = config.get('connection_id')
                application_config = config.get('application')
                py_config = config.get('py_files')

                if not connection_config:
                    raise Exception("Missing property 'connection_id' in task configuration '{}'".format(name))

                if not application_config:
                    raise Exception("Missing property 'application' in task configuration '{}'".format(name))

                # if not py_config and connection_config == 'cloudera_spark2':
                #     raise Exception("Missing property 'py-files' in task configuration '{}'".format(name))

                files_config = config.get('files')
                if isinstance(files_config, list):
                    if len(files_config) > 0:
                        config['files'] = ','.join(files_config)
                    else:
                        config.pop('files')

                config["conn_id"] = config.pop("connection_id")
                config["verbose"] = config.get("verbose") or True
                config["application_args"] = config.get('application_args', [])
                config["conf"] = config.get('conf', {})
                config["num_executors"] = config.get('num_executors') or 2
                config["jars"] = config.get("jars", '')

                # update import block
                import_op = 'from airflow.contrib.operators.spark_submit_operator import SparkSubmitOperator\n'
                if import_op not in import_block:
                    import_block += import_op

            elif operator.lower() == 'mysqloperator':
                """
                +-----------------------+
                |    MYSQL OPERATOR     |
                +-----------------------+
                """
                operator = 'MySqlOperator'

                # validate and clean config
                config = t.get('config')
                if not config:
                    raise Exception("Missing property 'config' in task '{}'".format(name))

                config = ({k: v for k, v in config.items() if k in OPERATORS_ALLOWED_FIELDS[operator]})

                connection_config = config.get('connection_id')
                sql_config = config.get('sql')

                if not connection_config:
                    raise Exception("Missing property 'connection_id' in task configuration '{}'".format(name))

                if not sql_config:
                    raise Exception("Missing property 'sql' in task configuration '{}'".format(name))

                config["mysql_conn_id"] = config.pop("connection_id")

                # update import block
                import_op = 'from airflow.operators.mysql_operator import MySqlOperator\n'
                if import_op not in import_block:
                    import_block += import_op

            elif operator.lower() == 'zondaslackoperator':
                """
                +----------------------+
                | ZONDA SLACK OPERATOR |
                +----------------------+
                """
                operator = 'ZondaSlackOperator'

                # validate and clean config
                config = t.get('config')
                if not config:
                    raise Exception("Missing property 'config' in task '{}'".format(name))

                config = ({k: v for k, v in config.items() if k in OPERATORS_ALLOWED_FIELDS[operator]})

                connection_config = config.get('connection_id')
                channels_config = config.get('channels')

                if connection_config:
                    config['slack_connection'] = config.pop('connection_id')

                if not channels_config:
                    raise Exception("Missing property 'channels' in task configuration '{}'".format(name))

                if isinstance(channels_config, str):
                    config['channels'] = [channels_config]

                # update import block
                import_op = 'from airflow.operators import ZondaSlackOperator\n'
                if import_op not in import_block:
                    import_block += import_op

            elif operator.lower() == 'sftpoperator':
                """
                +---------------+
                | SFTP OPERATOR |
                +---------------+
                """
                operator = 'SFTPOperator'

                # validate and clean config
                config = t.get('config')
                if not config:
                    raise Exception("Missing property 'config' in task '{}'".format(name))

                config = ({k: v for k, v in config.items() if k in OPERATORS_ALLOWED_FIELDS[operator]})

                connection_config = config.get('connection_id')
                local_path_config = config.get('local_filepath')
                remote_path_config = config.get('remote_filepath')

                if not connection_config:
                    raise Exception("Missing property 'connection_id' in task configuration '{}'".format(name))

                if not local_path_config:
                    raise Exception("Missing property 'local_path_config' in task configuration '{}'".format(name))

                if not remote_path_config:
                    raise Exception("Missing property 'remote_path_config' in task configuration '{}'".format(name))

                if config.get('operation'):
                    if not config.get('operation').lower() in ('put', 'get'):
                        raise Exception("Invalid SFTP operation '{}' (values allowed: put, get)".format(config.get('operation')))
                else:
                    config['operation'] = 'put'

                config['ssh_conn_id'] = config.pop('connection_id')

                # update import block
                import_op = 'from airflow.contrib.operators.sftp_operator import SFTPOperator\n'
                if import_op not in import_block:
                    import_block += import_op

            elif operator.lower() == 'triggerdagrunoperator':
                """
                +-------------------------+
                | TRIGGER DAGRUN OPERATOR |
                +-------------------------+
                """
                operator = 'TriggerDagRunOperator'

                # validate and clean config
                config = t.get('config')
                if not config:
                    raise Exception("Missing property 'config' in task '{}'".format(name))

                config = ({k: v for k, v in config.items() if k in OPERATORS_ALLOWED_FIELDS[operator]})

                trigger_config = config.get('trigger_dag_id')

                if not trigger_config:
                    raise Exception("Missing property 'trigger_dag_id' in task configuration '{}'".format(name))

                config['execution_date'] = "datetime.utcnow().replace(tzinfo=timezone('{}'))".format(tz)

                # update import block
                import_op = 'from airflow.operators.dagrun_operator import TriggerDagRunOperator\n'
                if import_op not in import_block:
                    import_block += import_op

            elif operator.lower() == 'zondatriggerdagrunoperator':
                """
                +-------------------------------+
                | ZONDA TRIGGER DAGRUN OPERATOR |
                +-------------------------------+
                """
                operator = 'ZondaTriggerDagRunOperator'

                # validate and clean config
                config = t.get('config')
                if not config:
                    raise Exception("Missing property 'config' in task '{}'".format(name))

                config = ({k: v for k, v in config.items() if k in OPERATORS_ALLOWED_FIELDS[operator]})

                trigger_config = config.get('trigger_dag_id')

                if not trigger_config:
                    raise Exception("Missing property 'trigger_dag_id' in task configuration '{}'".format(name))

                config['execution_date'] = "datetime.utcnow().replace(tzinfo=timezone('{}'))".format(tz)

                # update import block
                import_op = 'from airflow.operators import ZondaTriggerDagRunOperator\n'
                if import_op not in import_block:
                    import_block += import_op

            elif operator.lower() == 'sqoopoperator':
                """
                +---------------+
                | SQOOP OPERATOR |
                +---------------+
                """
                # @TODO: test and map all fields available for the operator
                operator = 'SqoopOperator'

                # validate and clean config
                config = t.get('config')
                if not config:
                    raise Exception("Missing property 'config' in task '{}'".format(name))

                config = ({k: v for k, v in config.items() if k in OPERATORS_ALLOWED_FIELDS[operator]})

                connection_config = config.get('connection_id')

                if not connection_config:
                    raise Exception("Missing property 'connection_id' in task configuration '{}'".format(name))

                config["conn_id"] = config.pop("connection_id")

                # update import block
                import_op = 'from airflow.contrib.operators.sqoop_operator import SqoopOperator\n'
                if import_op not in import_block:
                    import_block += import_op

            elif operator.lower() == 'zondahdfsoperator':
                """
                +---------------------+
                | ZONDA HDFS OPERATOR |
                +---------------------+
                """
                operator = 'ZondaHDFSOperator'

                # validate and clean config
                config = t.get('config')
                if not config:
                    raise Exception("Missing property 'config' in task '{}'".format(name))

                config = ({k: v for k, v in config.items() if k in OPERATORS_ALLOWED_FIELDS[operator]})

                operation_config = config.get('operation')

                if not operation_config:
                    raise Exception("Missing property 'operation' in task configuration '{}'".format(name))

                # update import block
                import_op = 'from airflow.operators import ZondaHDFSOperator\n'
                if import_op not in import_block:
                    import_block += import_op

            elif operator.lower() == 'zondas3operator':
                """
                +---------------------+
                | ZONDA S3 OPERATOR |
                +---------------------+
                """
                operator = 'ZondaS3Operator'

                # validate and clean config
                config = t.get('config')
                if not config:
                    raise Exception("Missing property 'config' in task '{}'".format(name))

                config = ({k: v for k, v in config.items() if k in OPERATORS_ALLOWED_FIELDS[operator]})

                operation_config = config.get('operation')

                if not operation_config:
                    raise Exception("Missing property 'operation' in task configuration '{}'".format(name))

                # update import block
                import_op = 'from airflow.operators import ZondaS3Operator\n'
                if import_op not in import_block:
                    import_block += import_op

            elif operator.lower() == 'branchpythonoperator':
                """
                +------------------------+
                | BRANCH PYTHON OPERATOR |
                +------------------------+
                """
                operator = 'BranchPythonOperator'

                # validate and clean config
                config = t.get('config')
                if not config:
                    raise Exception("Missing property 'config' in task '{}'".format(name))

                config = ({k: v for k, v in config.items() if k in OPERATORS_ALLOWED_FIELDS[operator]})

                function_name_config = config.get('function_name')
                if not function_name_config:
                    raise Exception("Missing property 'function_name' in task configuration '{}'".format(name))

                function_def_config = config.get('function_def')
                if not function_def_config:
                    raise Exception("Missing property 'function_def' in task configuration '{}'".format(name))

                op_args_config = config.get('op_args')
                if op_args_config and not isinstance(op_args_config, list):
                    raise Exception("Property 'op_args' in task configuration '{}' must be a list (positional arguments)".format(name))

                # add function definition to dag
                function_def_block = [{'lines': config.pop('function_def'), 'active': not self.is_subdag}]
                dag['code']['functions']['blocks'].extend(function_def_block)

                config['python_callable'] = config.pop('function_name')

                # update import block
                import_op = 'from airflow.operators.python_operator import BranchPythonOperator\n'
                if import_op not in import_block:
                    import_block += import_op

            elif operator.lower() == 'pythonoperator':
                """
                +-----------------+
                | PYTHON OPERATOR |
                +-----------------+
                """
                operator = 'PythonOperator'

                # validate and clean config
                config = t.get('config')
                if not config:
                    raise Exception("Missing property 'config' in task '{}'".format(name))

                config = ({k: v for k, v in config.items() if k in OPERATORS_ALLOWED_FIELDS[operator]})

                function_name_config = config.get('function_name')
                if not function_name_config:
                    raise Exception("Missing property 'function_name' in task configuration '{}'".format(name))

                function_def_config = config.get('function_def')
                if not function_def_config:
                    raise Exception("Missing property 'function_def' in task configuration '{}'".format(name))

                op_args_config = config.get('op_args')
                if op_args_config and not isinstance(op_args_config, list):
                    raise Exception("Property 'op_args' in task configuration '{}' must be a list (positional arguments)".format(name))

                # add function definition to dag
                function_def_block = [{'lines': config.pop('function_def'), 'active': not self.is_subdag}]
                dag['code']['functions']['blocks'].extend(function_def_block)

                config['python_callable'] = config.pop('function_name')

                # update import block
                import_op = 'from airflow.operators.python_operator import PythonOperator\n'
                if import_op not in import_block:
                    import_block += import_op

            else:
                raise Exception("Invalid operator '{}'".format(operator))

            # build subdag
            if not is_subdag:
                # setup config
                config['task_id'] = name + "_{}".format(self.loop_iterator_var) if loop_config else name
                config['dag'] = parent_dag_name
                task_block = '{} = {}({})'.format(variable_name, operator, self.build_operators_args(**config))
                tasks.append({'lines': task_block, 'active': True, 'indent': loop_indent})
            else:
                # build subdag
                subdag_name = '{}.{}'.format(dag_name, name)
                subdag = self._build_subdag(task=t, dag_name=subdag_name, **kwargs)

                subdag_def_name = 'build_{}(dag_id, start_date, schedule_interval, params)'.format(variable_name)
                subdag_function_block = []
                subdag_function_block.append({'lines': 'def {}:'.format(subdag_def_name), 'active': True})
                subdag_function_block.append({'lines': '\n'.join(self.get_code(dag=subdag)) + '\n', 'active': True, 'indent': 1})
                dag['code']['functions']['blocks'].extend(subdag_function_block)

                args = {
                    'task_id': name + "_{}".format(self.loop_iterator_var) if loop_config else name,
                    'dag': parent_dag_name,
                    'subdag': 'build_{}("{}", {}, {}, {})'.format(variable_name, subdag_name, 'start_date', 'None', 'params')
                }

                task_block = '{} = {}({})'.format(variable_name, operator, self.build_operators_args(**args))
                tasks.append({'lines': task_block, 'active': True, 'indent': loop_indent})

                # update import block
                import_op = 'from airflow.operators.subdag_operator import SubDagOperator\n'
                if import_op not in import_block:
                    import_block += import_op

        dag['code']['import']['blocks'].append({'lines': import_block, 'active': not self.is_subdag})

        tasks[-1]['lines'] = task_block + '\n'
        dag['code']['tasks'] = {'comments': 'tasks', 'blocks': tasks}

        """
        +--------------+
        | DEPENDENCIES |
        +--------------+
        """
        dependencies = self.config.get('dependencies', {}).copy()
        for k in dependencies:
            if isinstance(dependencies[k], str):
                dependencies[k] = [dependencies[k]]

        final_isolated_nodes = [x for x in config_tasks if x.get('name') not in dependencies]
        for n in final_isolated_nodes:
            dependencies[n.get('name')] = []

        isolated_nodes = [k for k in dependencies.keys() if k not in [y for x in dependencies.values() for y in x]]

        if is_there_input_task:
            if len(config_tasks) == 1:
                dependencies['input_task'] = [config_tasks[0]['name']]
            else:
                dependencies['input_task'] = isolated_nodes

        if is_there_dummy_task:
            if is_there_input_task:
                dependencies['dummy_task'] = ['input_task']
            else:
                if len(config_tasks) == 1:
                    dependencies['dummy_task'] = [config_tasks[0]['name']]
                else:
                    dependencies['dummy_task'] = isolated_nodes

        dependencies_block = ''
        for k, v in dependencies.items():
            if v:
                dependencies_block += '\n'.join(
                    [self.to_snake_case(k) + ' >> {}'.format(self.to_snake_case(x)) for x in v]) + '\n'

        dag['code']['dependencies'] = {'comments': 'dependencies',
                                       'blocks': [{'lines': dependencies_block, 'active': True, 'indent': loop_indent}]}

        dag['code']['return'] = {'comments': None,
                                 'blocks': [{'lines': 'return dag_{}'.format(dag_name), 'active': self.is_subdag}]}

        dag['status'] = 'OK'

        return dag


class SchemaValidator(object):
    def __init__(self, schema_file=DEFAULT_SCHEMA_FILE):
        """
        Constructor
        :param schema_file: path to JSON file that contains the valid schema
        """
        self.schema = self._read_schema(schema_file)
        self.validator = Validator(self.schema)

    @staticmethod
    def _read_schema(schema_file):
        """
        Read schema from JSON file
        :param schema_file:
        :return:
        """
        schema = None
        with open(schema_file, 'r') as f:
            schema = json.load(f)
        return schema

    def is_valid_schema(self, path):
        """
        Validate config against schema loaded
        :param path: configuration file to be validated
        :return:
        """
        check = False
        errors = {"error": "Invalid YML format, check file indentation"}
        with open(path, 'r') as stream:
            try:
                config = yaml.safe_load(stream)
                check = self.validator.validate(config, self.schema)
                errors = self.validator.errors
            except yaml.YAMLError:
                pass
        return check, errors


def discover_dags(**kwargs):
    """
    Discover configuration files and build DAGs
    :return: None
    """
    execution_status = True
    context_config = kwargs['dag_run'].conf if kwargs.get('dag_run') else {}
    try:
        airflow_home = context_config.get('airflow_home') or kwargs.get('airflow_home')
        if not airflow_home:
            airflow_home = AIRFLOW_HOME
        else:
            airflow_home = airflow_home if airflow_home[-1] != '/' else airflow_home[:-1]

        search_path = context_config.get('search_path') or kwargs.get('search_path')
        if not search_path:
            search_path = '{}/dags'.format(airflow_home)
            print('Using default search path {}...'.format(search_path))
        else:
            search_path = search_path if search_path[-1] != '/' else search_path[:-1]

        search_path = os.path.abspath(search_path)
        dags_folder = os.path.abspath(airflow_home + '/dags/autogenerated')

        print('Scanning folder {}...'.format(search_path))
        first_file = True

        for source_filename in glob.iglob(search_path + '**/**', recursive=True):
            ext = os.path.splitext(source_filename)[1]
            if os.path.isfile(source_filename) and ext in ['.yml', '.yaml'] and ((not any([x in source_filename.lower() for x in TESTING_PREFIXES]) and ENVIRONMENT != 'development') or ENVIRONMENT == 'development'):
                if first_file:
                    print(' |')
                    first_file = False

                dag_name = 'UNKNOWN'
                print(' |-+ Config file found -> {}'.format(source_filename))

                try:
                    dag = DAGBuilder(filepath=source_filename)
                    dag_name = dag.get_name() or dag_name
                    destination_filename = source_filename.replace(search_path, dags_folder).replace(ext, '.py')

                    if dag.get_status() == 'OK':
                        code = '\n'.join(dag.get_code())

                        # create file and directory
                        if not os.path.exists(os.path.dirname(destination_filename)):
                            try:
                                os.makedirs(os.path.dirname(destination_filename))
                            except OSError as exc:
                                if exc.errno != errno.EEXIST:
                                    raise

                        dag_modified = True
                        # @TODO: fix logic to detect same configuration
                        """
                        if os.path.exists(destination_filename):
                            old_dag = ''
                            with open(destination_filename, 'r') as f:
                                old_dag = f.read()
    
                            if dag.get_config() in old_dag:
                                dag.set_status(status='CONFIGURATION_NOT_MODIFIED')
                                print(' | +-- Status: {}'.format(dag.get_status()))
                                print(' | +-- Skipping file')
                                dag_modified = False
                        """

                        if dag_modified:
                            with open(destination_filename, "w") as f:
                                f.write(code)
                            print(' | +-- DAG Name: {}'.format(dag_name))
                            print(' | +-- Status: {}'.format(dag.get_status()))
                            print(' | +-- Tasks: {}'.format(len(dag.dag.get('code', {}).get('tasks', {}).get('blocks', []))))
                            print(' | +-- Python File: {}'.format(destination_filename))
                    else:
                        print(' | +-- DAG Name: {}'.format(dag_name))
                        print(' | +-- Status: {}'.format(dag.get_status()))
                        print(' | +-- Skipping file')

                except Exception as ex:
                    print(' | +-- DAG Name: {}'.format(dag_name))
                    print(' | +-- Status: EXCEPTION ({})'.format(str(ex)))
                    print(' | +-- Skipping file')
                    pass
                print(' |')
        print('Scan completed')
    except Exception as ex:
        print(str(ex))
        execution_status = False
        pass

    return execution_status


if __name__ == '__main__':
    config = {'airflow_home': os.getenv('AIRFLOW_HOME'),
              'search_path': os.getenv('SEARCH_PATH')}
    discover_dags(**config)
