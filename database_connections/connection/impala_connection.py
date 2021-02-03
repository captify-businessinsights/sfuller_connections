#!/usr/bin/env python
import pandas as pd
from impala.dbapi import connect
from .configs import ImpalaConfig
import os


class ImpalaConnect:
    def __init__(self, query, config: ImpalaConfig):
        self.query = query
        self.config = config

    def get_impala_df(self):
        with connect(host=self.config.host,
                     port=self.config.port,
                     user=self.config.user,
                     password=self.config.password,
                     timeout=self.config.timeout,
                     use_ssl=True,
                     auth_mechanism=self.config.auth_mechanism) as conn:

            with conn.cursor() as cur:
                for q in self.query.split(";"):
                    cur.execute(q, configuration={"REQUEST_POOL": os.getenv("REQUEST_POOL"), "MEM_LIMIT": "40g"})
                    try:
                        col = [desc[0] for desc in cur.description]
                        df = pd.DataFrame(cur.fetchall(), columns=col)
                    except TypeError:
                        print('no dataframe was returned - impala connection returning None')
                        df = None
                    return df


