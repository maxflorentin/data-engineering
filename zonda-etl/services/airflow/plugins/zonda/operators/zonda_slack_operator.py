#!/usr/bin/python3

import os
import slack
import json
from datetime import timedelta
from airflow.hooks.base_hook import BaseHook
from airflow.models import BaseOperator
from airflow.utils.decorators import apply_defaults
from pyzonda.utils import get_epoch_timestamp, clean_dict

CERT = os.getenv('WEBSOCKET_CLIENT_CA_BUNDLE')


class ZondaSlackOperator(BaseOperator):
    @apply_defaults
    def __init__(self, slack_connection='slack', *args, **kwargs):
        super(ZondaSlackOperator, self).__init__(*args, **kwargs)

        connection = BaseHook.get_connection(slack_connection)

        self.selected_channels = list(set(kwargs.get('channels')))
        self.selected_users = list(set(kwargs.get('users', [])))
        self.text = kwargs.get('text')
        self.conf = kwargs.get('conf')
        # self.display_conf = True if kwargs.get('display_conf') or self.conf else False
        self.display_conf = True
        self.slack_token = connection.password
        self.slack_client = None
        self.extras = connection.extra_dejson
        self.slack_proxy = self.extras.get("proxy")
        self.slack_channels = dict()
        self.slack_users = dict()
        self.status = kwargs.get('status', 0)
        self.channels_list = []

    def _get_channel_id(self, channel_name):
        """
        Get channel identifier for a given channel name
        :param channel_name: slack channel name
        :return:
        """
        channel_id = None
        if self.slack_client:
            # get channels list
            if not self.channels_list:
                try:
                    response = self.slack_client.conversations_list(types='private_channel', exclude_archived='true', limit=1000)
                    self.channels_list = response.get('channels')
                    cursor = response.get('response_metadata', {}).get('next_cursor', "")

                    while cursor:
                        response = self.slack_client.conversations_list(types='private_channel', exclude_archived='true', cursor=cursor, limit=1000)
                        self.channels_list += response.get('channels')
                        cursor = response.get('response_metadata', {}).get('next_cursor', "")

                except Exception as ex:
                    self.log.error(str(ex))
                    pass

            # search channel id
            for channel in self.channels_list:
                print(channel.get('name'))
                if channel.get('name').lower() == channel_name.lower():
                    channel_id = channel.get('id')
                    break
        return channel_id

    def _get_user_id(self, user_mail):
        """
        Get user identifier for a given user email
        :param user_mail: slack user email
        :return:
        """
        user_id = None
        if self.slack_client:
            try:
                response = self.slack_client.users_lookupByEmail(email=user_mail)
                user_id = response['user']['id']
                print("User: {}".format(user_id))
            except Exception as ex:
                self.log.error("Error in users: {}".format(str(ex)))
                pass

        return user_id

    def execute(self, context):
        """
        Execute operator
        :param context: dag context
        :return: None
        """
        # dag conf
        dag_conf = context.get('dag_run').conf if context.get('dag_run') else {}
        dag_conf = dag_conf if dag_conf is not None else { "date_from": context.get('dag_run').execution_date.strftime('%Y-%m-%d') }
        print("dag_conf: {}".format(dag_conf))
        conf = clean_dict(self.conf or dag_conf)

        # slack web client
        if self.slack_token:
            try:
                self.slack_client = slack.WebClient(token=self.slack_token, ssl=False, proxy=self.slack_proxy)
            except:
                pass

        # get channels identifiers
        for channel in self.selected_channels:
            self.slack_channels[channel] = self._get_channel_id(channel)

        if not self.slack_channels or not any([v for k, v in self.slack_channels.items()]):
            self.log.warning("No channels/users to notify")
            return False

        if self.selected_users:
            for user in self.selected_users:
                self.slack_channels[user] = self._get_user_id(user)

        if not self.slack_token:
            self.log.error("Notification by Slack could not be sent to {} (Missing api token)".format(
                ', '.join(self.slack_channels)))
            return False

        if self.status in (0, 1, 2):
            try:
                job_name = '{} [{}]'.format(context.get('task_instance').task_id, context.get('task_instance').dag_id)
                # execution_date = context.get('execution_date') - timedelta(hours=3)
                execution_date = context.get('execution_date')

                # error
                if self.status == 1:
                    text = self.text or '<!here|here> :heavy_multiplication_x: Task *{}* has failed!'.format(job_name)
                    color_notification = '#F44242'
                    callback_id = 'error_notification'
                    fallback = text.replace('*', '').replace('<!here|here>', '').strip()

                    fields = [
                        {
                            'title': 'Server',
                            'value': os.getenv('HOSTNAME'),
                            'short': True
                        },
                        {
                            'title': 'Priority',
                            'value': 'High',
                            'short': True
                        }
                    ]

                # warning
                elif self.status == 2:
                    text = self.text or '<!here|here> :grey_exclamation: Task *{}* requires your attention!'.format(job_name)
                    color_notification = '#FFCC00'
                    callback_id = 'warning_notification'
                    fallback = text.replace('*', '').replace('<!here|here>', '').strip()
                    fields = [
                        {
                            'title': 'Server',
                            'value': os.getenv('HOSTNAME'),
                            'short': True
                        },
                        {
                            'title': 'Priority',
                            'value': 'Medium',
                            'short': True
                        }
                    ]

                # successful
                else:
                    text = self.text or ':heavy_check_mark: Task *{}* executed successfully!'.format(job_name)
                    color_notification = '#2EB82E'
                    callback_id = 'successful_notification'
                    fallback = text.replace('*', '').replace('<!here|here>', '').strip()
                    fields = [
                        {
                            'title': 'Server',
                            'value': os.getenv('HOSTNAME'),
                            'short': True
                        },
                        {
                            'title': 'Priority',
                            'value': 'Low',
                            'short': True
                        }
                    ]
                if conf and self.display_conf:
                    fields.append(
                        {
                            'title': 'Configuration',
                            'value': json.dumps(conf, indent=4),
                            'short': True
                        }
                    )

                # actions
                actions = [
                    {
                        'type': 'button',
                        'name': 'see_logs',
                        'text': 'See logs',
                        'url': context.get('task_instance').log_url,
                        'style': 'default'
                    }
                ]

                # attachments
                attachments = [
                    {
                        'text': text,
                        'author_name': 'Airflow',
                        # 'author_link': 'https://{}/admin'.format(os.getenv('HOSTNAME')),
                        'fallback': fallback,
                        'callback_id': callback_id,
                        'color': color_notification,
                        'attachment_type': 'default',
                        'actions': actions,
                        'fields': fields,
                        'footer': 'Execution date',
                        'ts': get_epoch_timestamp(date=execution_date)
                    }
                ]

                # send notification
                channels_notified = []
                for k, v in self.slack_channels.items():
                    print("Channel: {}".format(v))
                    if v:
                        result = self.slack_client.chat_postMessage(
                            as_user='false',
                            username='Zonda',
                            channel=v,
                            attachments=attachments
                        )

                        if not result.get('ok'):
                            self.log.error("Error notifying to channel {} [{}]: {}".format(k, v, result.get('error')))
                            pass
                        else:
                            channels_notified.append(k)

                if len(self.slack_channels) == len(channels_notified):
                    self.log.info(
                        "Notification sent by Slack to channels/users {}".format(', '.join(self.slack_channels)))

                elif len(channels_notified) == 0:
                    self.log.warning(
                        "Notification by Slack could not be sent to {}".format(
                            ', '.join([x for x in self.slack_channels])))
                else:
                    self.log.info(
                        "Notification sent by Slack to channels/users {} (Could not send the notification to {}]".format(
                            ', '.join(channels_notified),
                            ', '.join([k for k, v in self.slack_channels.items() if k not in channels_notified])))
            except Exception as ex:
                self.log.error(str(ex))
                self.log.error("Notification by Slack could not be sent to {}".format(', '.join(self.slack_channels)))
                return False

    def __repr__(self):
        """
        Object string representation
        :return: str representation of this object
        """
        return str(self)
