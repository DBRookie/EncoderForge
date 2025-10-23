import numpy as np
import duckdb
import psycopg2
import pymonetdb
from clickhouse_driver import connect
import time
from psycopg2 import OperationalError
from pandas.api.types import is_integer_dtype, is_bool_dtype, is_string_dtype, is_float_dtype
import pymysql

from encoderforge.base.defs import DBDataType

# when the query is not null, the function only executes the query
def insert_db(db, table_name, cols, data, query=None):
    if db == 'duckdb':
        duckdb_file_path = '/volume/zhijh/my_database.duckdb'
        conn = duckdb.connect(database=duckdb_file_path, read_only=False)
        # create table
        cols_sql = ','.join([f'{col} {cols[col]}' for col in cols])
        create_sql = f'DROP TABLE IF EXISTS {table_name}; CREATE TABLE {table_name} ({cols_sql});'
        conn.execute(create_sql)
        # temp_tables.append(table_name)
        # insert data
        slot_sql = ','.join(['?'] * len(data[0]))
        insert_sql = f'INSERT INTO {table_name} VALUES ({slot_sql});'
        conn.executemany(insert_sql, data)
        conn.commit()
        conn.close()
    
    elif db == 'postgresql':
        # database parameters
        dbname = 'encoderforge'
        user = 'postgres'
        host = 'localhost'  
        port = 5432  

        # connect to pg
        try:
            connection = psycopg2.connect(
                dbname=dbname,
                user=user,
                host=host,
                port=port,
                client_encoding='utf8'
            )
            cursor = connection.cursor()
        except OperationalError as e:
            print(f"The error '{e}' occurred")

        # create table
        cols_sql = ','.join([f'{col} {cols[col]}' for col in cols])
        create_sql = f'DROP TABLE IF EXISTS {table_name}; CREATE TABLE {table_name} ({cols_sql});'
        try:
            cursor.execute("SET max_parallel_workers_per_gather = 3;")
            cursor.execute(create_sql)
            connection.commit()
        except OperationalError as e:
            print(f"The error '{e}' occurred")

        # insert data
        slot_sql = ','.join(['%s'] * len(data[0]))
        # if(table_name == "transaction_description_cat_c_cat"):
        #     pass
        insert_sql = f'INSERT INTO {table_name} VALUES ({slot_sql});'
        cursor.executemany(insert_sql, data)
        connection.commit()

        # close connect
        cursor.close()
        connection.close()
        
    elif db == 'monetdb':
        conn = pymonetdb.connect(database='encoderforge', username='monetdb', password='monetdb')
        cur = conn.cursor()
        # create table
        cols_sql = ','.join([f'{col} {cols[col]}' for col in cols])
        create_sql = f'DROP TABLE IF EXISTS {table_name}; CREATE TABLE {table_name} ({cols_sql});'
        cur.execute(create_sql)
        conn.commit()
        # temp_tables.append(table_name)
        # insert data
        slot_sql = ','.join(['%s'] * len(data[0]))
        insert_sql = f'INSERT INTO {table_name} VALUES ({slot_sql});'
        cur.executemany(insert_sql, data)
        conn.commit()
        conn.close()
    
    elif db =="clickhouse":
        conn = connect(host='localhost', user='default', password='',database = 'encoderforge')
        cur = conn.cursor()
        if query is None:
            cols_sql = ', '.join([f'{col} {cols[col]}' for col in cols])
            create_sql = f'DROP TABLE IF EXISTS {table_name};'
            cur.execute(create_sql)  
            create_sql = f'CREATE TABLE {table_name} ({cols_sql})ENGINE = MergeTree() ORDER BY tuple();'
            cur.execute(create_sql)  
            slot_sql = ', '.join(['%s'] * len(data[0]))
            # print(slot_sql)
            insert_sql = f'INSERT INTO {table_name} VALUES ({slot_sql});'
            insert_sql = f'INSERT INTO {table_name} VALUES'
            # print(data)
            cur.executemany(insert_sql, data)  
        else:
            cur.execute(query)
        conn.close()
    
    elif db == 'tidb':
        database = 'encoderforge'
        connection = pymysql.connect(
            host='49.52.27.18', 
            port=4000,      
            user='root',      
            password='root',  
            database=database,
            charset='utf8mb4',
            autocommit=True
        )
        cursor = connection.cursor()
        cols_sql = ','.join([f'{col} {cols[col]}' for col in cols])
        drop_table = f'DROP TABLE IF EXISTS {table_name};'
        create_table = f'CREATE TABLE {table_name} ({cols_sql});'
        try:
            cursor.execute("SET tidb_mem_quota_query = 8 << 30;")
            cursor.execute(drop_table)
            cursor.execute(create_table)
            cursor.execute(f'ALTER TABLE {database}.{table_name} SET TIFLASH REPLICA 1;')
            connection.commit()
        except OperationalError as e:
            print(f"The error '{e}' occurred")

        # insert data
        slot_sql = ','.join(['%s'] * len(data[0]))
        # if(table_name == "transaction_description_cat_c_cat"):
        #     pass
        insert_sql = f'INSERT INTO {table_name} VALUES ({slot_sql});'
        cursor.executemany(insert_sql, data)
        connection.commit()

        # close connect
        cursor.close()
        connection.close()

def df_type2db_type(df_type, db):
    if db in ('duckdb', 'postgresql','clickhouse','tidb'):
        if is_string_dtype(df_type):
            return DBDataType.VARCHAR.value
        elif df_type == np.int8:
            return DBDataType.SMALLINT.value
        elif is_integer_dtype(df_type):
            return DBDataType.INT.value
        elif is_float_dtype(df_type):
            return DBDataType.FLOAT.value
        elif is_bool_dtype(df_type):
            return DBDataType.BOOLEAN.value
    
    elif db == 'monetdb':
        if is_string_dtype(df_type):
            return DBDataType.VARCHAR512.value
        elif df_type == np.int8:
            return DBDataType.SMALLINT.value
        elif is_integer_dtype(df_type):
            return DBDataType.INT.value
        elif is_float_dtype(df_type):
            return DBDataType.FLOAT.value
        elif is_bool_dtype(df_type):
            return DBDataType.BOOLEAN.value
    
    return None


def merge_db(db, table_name, merge_table):
    if db == 'duckdb':
        duckdb_file_path = '/volume/zhijh/my_database.duckdb'
        conn = duckdb.connect(database=duckdb_file_path, read_only=False)
        join_sql = f"SELECT * FROM {' CROSS JOIN '.join(merge_table)}"
        conn.execute(f"DROP TABLE IF EXISTS {table_name}; CREATE TABLE {table_name} AS {join_sql}")
        conn.commit()
        conn.close()
    
    elif db == 'postgresql':
        # database parameters
        dbname = 'encoderforge'
        user = 'postgres'
        host = 'localhost'  
        port = 5432  

        # connect to pg
        try:
            connection = psycopg2.connect(
                dbname=dbname,
                user=user,
                host=host,
                port=port
            )
            cursor = connection.cursor()
        except OperationalError as e:
            print(f"The error '{e}' occurred")
        # merge_time
        join_sql = f"SELECT * FROM {' CROSS JOIN '.join(merge_table)}"
        table_name = table_name[:63]
        
        try:
            cursor.execute("SET max_parallel_workers_per_gather = 3;")
            cursor.execute("SET statement_timeout = 3600000;")
            cursor.execute("SET enable_mergejoin = off;")
            cursor.execute("SET enable_nestloop = off;")
            cursor.execute(f"DROP TABLE IF EXISTS {table_name};")
            t1 = time.time()
            cursor.execute(f"CREATE TABLE {table_name} AS {join_sql}")
            t2 = time.time()
            print(f'merge_{table_name}_time :{(t2 - t1):.4f} s', flush=True)    
            connection.commit()
        except OperationalError as e:
            print(f"The error '{e}' occurred")

        # close connect
        cursor.close()
        connection.close()
    elif db == 'tidb':
        conn = pymysql.connect(
            host='49.52.27.18', 
            port=4000,      
            user='root',      
            password='root',  
            database=database,
            charset='utf8mb4',
            autocommit=True
        )
        cur = conn.cursor()
        join_sql = f"SELECT * FROM {' CROSS JOIN '.join(merge_table)}"
        table_name = table_name[:63]
        
        try:
            cursor.execute(f"DROP TABLE IF EXISTS {table_name};")
            t1 = time.time()
            cursor.execute(f"CREATE TABLE {table_name} AS {join_sql}")
            t2 = time.time()
            print(f'merge_{table_name}_time :{(t2 - t1):.4f} s', flush=True)    
            conn.commit()
        except OperationalError as e:
            print(f"The error '{e}' occurred")

        # close connect
        cursor.close()
        conn.close()
        
    elif db == 'monetdb':
        conn = pymonetdb.connect(database='encoderforge', username='monetdb', password='monetdb')
        cur = conn.cursor()
        join_sql = f"SELECT * FROM {' CROSS JOIN '.join(merge_table)}"
        cur.execute(f"DROP TABLE IF EXISTS {table_name}; CREATE TABLE {table_name} AS {join_sql}")
        conn.commit()
        conn.close()
    
    elif db =="clickhouse":
        conn = connect(host='localhost', user='default', password='',database = 'encoderforge')
        cur = conn.cursor()
        join_sql = f"SELECT * FROM {' CROSS JOIN '.join(merge_table)}"
        cur.execute(f"DROP TABLE IF EXISTS {table_name}")
        create_sql = f"CREATE TABLE {table_name} ENGINE = MergeTree() ORDER BY tuple() AS {join_sql}"
        cur.execute(create_sql) 
        conn.close()