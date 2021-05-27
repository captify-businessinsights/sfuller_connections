#!/usr/bin/env python
import pandas as pd
from impala.dbapi import connect
from .impala import ImpalaConfig
import os


class ImpalaConnect:
    def __init__(self, query, config: ImpalaConfig):
        self.query = query
        self.config = config

    def get_impala_df(self, request_pool=os.getenv("REQUEST_POOL"), mem_limit="40g"):
        with connect(host=self.config.host,
                     port=self.config.port,
                     user=self.config.user,
                     password=self.config.password,
                     timeout=self.config.timeout,
                     use_ssl=True,
                     auth_mechanism=self.config.auth_mechanism) as conn:

            with conn.cursor() as cur:
                queries = [x for x in self.query.split(";") if x.replace('\n','').replace(' ', '') != ''] # removes any queries which are blank (only \n and spaces)

                i = 1
                for q in queries:
                    cur.execute(q, configuration={"REQUEST_POOL": request_pool, "MEM_LIMIT": mem_limit})
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


