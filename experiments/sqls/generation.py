import time
from sympy import numer
import os, sys
CUR_DIR = os.path.dirname(__file__)
PROJECT_ROOT = os.path.abspath(os.path.join(CUR_DIR, "../../"))
sys.path.insert(0, PROJECT_ROOT)
from encoderforge.transformer_manager import TransformerManager
from encoderforge.utility.join_utils import insert_db
import signal
import time

class TimeoutException(Exception):
    pass

def handler(signum, frame):
    raise TimeoutException()


if __name__ == "__main__":
    manager = TransformerManager()
    
    BASE_DIR = os.path.abspath(os.path.join(os.path.dirname(__file__), "../"))
    file_names = ["ukair","job","adult","bank","cat","criteo","mushrooms","kdd"]
    for file_name in file_names:
        pipeline_file = BASE_DIR + "/pipelines/" + file_name + "/" + file_name + ".joblib"
        dbms = 'postgresql' # duckdb or postgresql or ...
        pre_sql = "SET max_parallel_workers_per_gather = 3; \
                    SET statement_timeout = 3600000; \
                    SET enable_mergejoin = off; \
                    SET enable_nestloop = off; \
                    EXPLAIN ANALYZE "
        # pre_sql = "SET max_parallel_workers_per_gather = 3; \
        #             SET statement_timeout = 3600000; \
        #             RESET enable_mergejoin; \
        #             RESET enable_nestloop; \
        #             EXPLAIN ANALYZE "   # blue        
        signal.signal(signal.SIGALRM, handler)
        for group in ('org', 'separate', 'blue_elephants', 'composite', 'sort', 'dp', 'enumerate'):
            dp_mode = ("disable", "origin") if group == "dp" else ("origin",)
            for dp_way in dp_mode:
                try:
                    signal.alarm(3600)  #  30 
                    t1 = time.time()
                    query = manager.generate_query(
                        pipeline_file,
                        file_name,
                        dbms,
                        pre_sql=pre_sql,
                        order_when=False,
                        group=group,
                        cost_model='encoderforge',
                        max_process_num=1,
                        dp_way=dp_way
                    )
                    t2 = time.time()
                    signal.alarm(0)  # 
                    print(f'{group}, time cost: {(t2-t1):.2f}s')

                    generated_file_path = os.path.join(BASE_DIR, "sqls", file_name, f"{dp_way}.sql") if dp_way == "disable" else os.path.join(BASE_DIR, "sqls", file_name, f"{group}.sql")
                    with open(generated_file_path, "w") as sql_file:
                        sql_file.write(query)

                except TimeoutException:
                    print(f'{group}, execution exceeded 60 minutes and was terminated.')
