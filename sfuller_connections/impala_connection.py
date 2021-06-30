#!/usr/bin/env python
from logging import error
import pandas as pd
from impala.dbapi import connect
from .impala import ImpalaConfig
import os
import time
from datetime import datetime

def time_since(start_time):
    current_time = time.time()
    time_taken = current_time - start_time
    if time_taken > 60:
        time_taken = round(time_taken/60, 1)
        units = "minutes"
    else:
        time_taken = round(time_taken, 0)
        units = "seconds"
    
    return current_time, f"{time_taken} {units}"

class ImpalaConnect:
    def __init__(self, query, config: ImpalaConfig):
        self.query = query
        self.config = config

    def get_impala_df(self, request_pool=os.getenv("REQUEST_POOL"), mem_limit="40g"):
        start_time = time.time()
        
        with connect(host=self.config.host,
                     port=self.config.port,
                     user=self.config.user,
                     password=self.config.password,
                    #  timeout=self.config.timeout,
                     timeout = 1,
                     use_ssl=True,
                     auth_mechanism=self.config.auth_mechanism) as conn:

            with conn.cursor() as cur:
                queries = [x for x in self.query.split(";") if x.replace('\n','').replace('\t', '').replace(' ', '') != ''] # removes any queries which are blank (only \n, \t and spaces)

                i = 1
                for q in queries:
                    try:
                        cur.execute(q, configuration={"REQUEST_POOL": request_pool, "MEM_LIMIT": mem_limit})
                    except Exception as e:
                        error_time, length_str = time_since(start_time)

                        print("\n")
                        print(type(e).__name__)
                        if e.args[0]:
                            print(e.args[0])
                        print(f"Start time: {datetime.utcfromtimestamp(int(start_time)).strftime('%Y-%m-%d %H:%M:%S')}")
                        print(f"Error hit at: {datetime.utcfromtimestamp(int(error_time)).strftime('%Y-%m-%d %H:%M:%S')} ({length_str})\n")
                        print("Offending query:\n")
                        print(q)
                        print("\n")
                        raise e

                    if len(queries) > 1:
                        print(f'executing query {i} of {len(queries)}')
                    try:
                        col = [desc[0] for desc in cur.description]
                        df = pd.DataFrame(cur.fetchall(), columns=col)
                    except TypeError:
                        df = None
                    i += 1
                
                if len(queries) > 1:
                    print(f'returning results for query {i - 1}')

                if df is None:
                    print('no dataframe was returned - impala connection returning None')

                return df


