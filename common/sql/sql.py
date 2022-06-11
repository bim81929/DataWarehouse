from contextlib import contextmanager

import psycopg2

from config import config


def get_config():
    host = config.DB_HOST
    port = config.DB_PORT
    username = config.DB_USERNAME
    password = config.DB_PASSWORD
    database = config.DB_NAME

    return host, port, username, password, database


def get_connect():
    host, port, username, password, database = get_config()
    try:
        connect = psycopg2.connect(host=host, port=port, database=database,
                                   username=username, password=password)
    except Exception as e:
        print(e)
        connect = None
    return connect


def sql_close(connect):
    try:
        connect.commit()
        connect.close()
    except Exception as e:
        print(e)


def sql_insert(connect, table, df):
    if connect is not None:
        cursor = connect.cursor()
        if len(df) > 0:
            df_columns = list(df)
            columns = ",".join(df_columns)
            values = "VALUES({})".format(",".join(["%s" for _ in df_columns]))
            query = f"INSERT INTO {table} ({columns}) {values}"
            cursor.execute(query)
        sql_close(connect)
    else:
        print("ERROR CONNECT")


def sql_read_table(connect, table, l_columns):
    data = None
    if connect is not None:
        cursor = connect.cursor()
        columns = ",".join(l_columns)
        query = f"SELECT {columns} from {table}"
        cursor.execute(query)

        data = cursor.fetchall()
        sql_close(connect)
    else:
        print("ERROR CONNECT")
    return data


def sql_execute(connect, query):
    if connect is not None:
        cursor = connect.cursor()
        cursor.execute(query)
        sql_close(connect)
    else:
        print("ERROR CONNECT")
