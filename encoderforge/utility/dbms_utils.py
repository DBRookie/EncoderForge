import duckdb
import psycopg2
import numpy as np
import pymonetdb
import pymysql

class DBMSUtils(object):
    available_dbms = ['postgresql', 'duckdb', 'monetdb','clickhouse','tidb']

    @staticmethod
    def check_dbms(name: str):
        assert isinstance(name, str), "Wrong data type for param 'name'."
        assert name in DBMSUtils.available_dbms, f"No dbms {name} found. Use one of {DBMSUtils.available_dbms}."

        return name

    @staticmethod
    def get_delimited_col(dbms: str, col: str):
        dbms = DBMSUtils.check_dbms(dbms)
        assert isinstance(col, str), "Wrong data type for param 'col'."
        
        if dbms == 'postgresql':
            return f'"{col}"'
        
        if dbms == 'duckdb':
            return f'"{col}"'
        
        if dbms == 'monetdb':
            return f'"{col}"'
    
        if dbms == 'clickhouse':
            return f'"{col}"'
        
        if dbms == 'tidb':
            return f'"{col}"'
        
    @staticmethod
    def fetch_data_as_numpy(dbms: str, table_name: str, column: str):
        dbms = DBMSUtils.check_dbms(dbms)
        assert isinstance(table_name, str), "Wrong data type for param 'table_name'."
        assert isinstance(column, str), "Wrong data type for param 'column'."

        if dbms == 'postgresql':
            pg_config = {
                'dbname': 'postgres',
                'user': 'postgres',
                'password': 'postgres',
                'host': '127.0.0.1',
                'port': '5432'
            }
            conn = psycopg2.connect(**pg_config)
            cur = conn.cursor()
            cur.execute(f'SELECT {column} FROM {table_name}')
            data = np.array(cur.fetchall())
            cur.close()
            conn.close()
            return data.flatten() if data.size > 0 else np.array([])

        elif dbms == 'duckdb':
            duckdb_file_path = '/root/volume/duckdb/mydb'
            conn = duckdb.connect(database=duckdb_file_path)
            return conn.execute(f'SELECT {column} FROM {table_name}').fetchnumpy()[column]

        elif dbms == 'monetdb':
            conn = pymonetdb.connect(username='monetdb', password='monetdb', port=9000,database='encoderforge')
            cur = conn.cursor()
            cur.execute(f'SELECT {column} FROM {table_name}')
            data = np.array(cur.fetchall())
            cur.close()
            conn.close()
            return data.flatten() if data.size > 0 else np.array([])

        elif dbms == 'tidb':
            conn = pymysql.connect(
                host='49.52.27.18', 
                port=4000,    
                user='root',      
                password='root',  
                database='encoderforge',
                charset='utf8mb4',
                autocommit=True
            )
            cur = conn.cursor()
            cur.execute(f'SELECT {column} FROM {table_name}')
            data = np.array(cur.fetchall())
            cur.close()
            conn.close()
            return data.flatten() if data.size > 0 else np.array([])
            
        
        