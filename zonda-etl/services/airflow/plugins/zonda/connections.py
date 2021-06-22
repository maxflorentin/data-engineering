from airflow.models import Connection
from airflow.utils.db import provide_session
import logging
import json
import os

ZONDA_DIR = os.getenv("ZONDA_DIR")


@provide_session
def create_connections(session=None):
    json_config = ZONDA_DIR + '/repositories/zonda-etl/services/airflow/plugins/zonda/connection_list.json'

    with open(json_config, 'r') as f:
        connections = json.load(f)

    for source in connections['airflowConnections']:

        for connection in source['connections']:

            conn_type = source.get("type")
            host = connection.get("host", "")
            port = connection.get("port")
            db = connection.get("db", "")
            user = connection.get("user", "")
            password = connection.get("password", "")

            if conn_type == 'google_cloud_platform':
                extra = {
                    'extra__google_cloud_platform__scope': ",".join(connection.get("scopes", [])),
                    'extra__google_cloud_platform__project': connection.get("project_id", ""),
                    'extra__google_cloud_platform__key_path': connection.get("keyfile_path", "")
                }
                extra = json.dumps(extra)
            elif conn_type == 'jdbc':
                extra = {
                    'extra__jdbc__drv_path': connection.get("driver_path", ""),
                    'extra__jdbc__drv_clsname': connection.get("driver_class", "")
                }
                extra = json.dumps(extra)
            else:
                extra = json.dumps(connection.get("extra", ""))

            try:
                connection_query = session.query(Connection).filter(Connection.conn_id == connection['conn_id'], )
                connection_query_result = connection_query.one_or_none()
                if not connection_query_result:
                    conn = Connection(conn_id=connection['conn_id'], conn_type=conn_type, host=host, port=port,
                                            login=user, password=password, schema=db, extra=extra)
                    session.add(conn)
                    session.commit()
                else:
                    connection_query_result.host = host
                    connection_query_result.login = user
                    connection_query_result.schema = db
                    connection_query_result.port = port
                    connection_query_result.set_password(password)
                    connection_query_result.extra = extra
                    session.add(connection_query_result)
                    session.commit()
            except Exception as e:
                logging.info("Failed creating connection")
                logging.info(e)


if __name__ == '__main__':
    create_connections()