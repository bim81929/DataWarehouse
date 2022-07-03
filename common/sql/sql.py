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
                                   user=username, password=password)
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
            try:
                df_columns = list(df)
                columns = ",".join(df_columns)
                list_values = [str(x) for x in list(set([tuple(x) for x in df.to_numpy()]))]
                values = "VALUES{}".format(",".join(list_values))
                query = f"INSERT INTO {table} ({columns}) {values}"
                cursor.execute(query)
            except Exception as e:
                pass
        sql_close(connect)
    else:
        print("ERROR CONNECT")


def sql_read_table(connect, table, l_columns, condition=None):
    data = None
    if connect is not None:
        cursor = connect.cursor()
        if l_columns[0] == "*":
            columns = "*"
        else:
            columns = ",".join(l_columns)
        if condition is None:
            query = f"SELECT {columns} from {table}"
        else:
            query = f"SELECT {columns} from {table} where {condition}"
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


if __name__ == '__main__':
    connect = get_connect()
    data = sql_read_table(connect, table="article", l_columns=["*"])
    for f in data[:5]:
        print(f)