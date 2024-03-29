from .impala_connection import ImpalaConnect
from .s3_connection import S3Connect
from .impala import ImpalaConfigFromEnv
from .s3 import S3ConfigFromEnv
from .query_object import QueryObject
from pickle import dump as pickle_dump, load as pickle_load
import pandas as pd
import os
import time

def query_impala_basic(query, config=ImpalaConfigFromEnv, request_pool=os.getenv("REQUEST_POOL"), mem_limit="40g", floatify=True):
    con = ImpalaConnect(query=query, config=config)
    df = (ImpalaConnect.get_impala_df(con, request_pool=request_pool, mem_limit=mem_limit))
    try:
        df = df.reset_index(drop=True)

        if floatify:
            for col in df.select_dtypes(include='object').columns:
                try:
                    df[col] = df[col].astype('float')
                except:
                    pass

    except AttributeError:
        df = None
    return df


def query_impala(queryobj, config=ImpalaConfigFromEnv, request_pool=os.getenv("REQUEST_POOL"), mem_limit="40g", time_query=True, floatify=True):
    if time_query:
        start_time = time.time()
        
    if isinstance(queryobj, QueryObject):
        q = queryobj.query
    elif isinstance(queryobj, str):
        q = queryobj
    else:
        print("invalid type for queryobj. Must be QueryObject or Str")
        raise InvalidQueryType
   
    has_picked_data = False

    if os.getenv("TURN_PICKLE_ON") == "TRUE" and isinstance(queryobj, QueryObject):
        try:
            df = pickle_load(open(f"pickled_data/{queryobj.name}.sav", "rb"))
            print(f"loading from picked state - {queryobj.name}.sav")
            has_picked_data = True
        except:
            pass

    if not has_picked_data:
        df = query_impala_basic(q, config=config, request_pool=request_pool, mem_limit=mem_limit, floatify=floatify)

    if os.getenv("TURN_PICKLE_ON") == "TRUE" and isinstance(queryobj, QueryObject) and has_picked_data == False:
        if not os.path.exists('pickled_data'):
            os.makedirs('pickled_data')
        pickle_dump(df, open(f"pickled_data/{queryobj.name}.sav", "wb"))
    
    if time_query:
        end_time = time.time()
        time_taken = end_time - start_time
        if time_taken > 60:
            time_taken = round(time_taken/60, 1)
            units = "minutes"
        else:
            time_taken = round(time_taken, 0)
            units = "seconds"
            
        print(f"query took {time_taken} {units}")
        
    return df

# https://stackoverflow.com/questions/31071952/generate-sql-statements-from-a-pandas-dataframe
def sql_from_df(df, name, include_index=False):
    if include_index:
        sql_text = pd.io.sql.get_schema(df.reset_index(), name)   
    else:
        sql_text = pd.io.sql.get_schema(df, name)  

    # fix sql formatting for impala
    sql_text = sql_text\
                .replace('"', '`')\
                .replace('TEXT', 'STRING')\
                .replace('%', 'pct')

    # remove first two instances of `
    sql_text = sql_text.replace('`','',2)
    return sql_text

def send_to_impala(df, name, include_index=False, config=ImpalaConfigFromEnv):
    dfi = df.rename(columns = {"Unnamed: 0": "Unnamed_0"}).copy()
    
    if include_index:
        dfi = dfi.reset_index()

    try:
        if dfi.select_dtypes('datetime').shape[1] > 0:
            print('dropping timestamp columns due to impala compatibility issues')
            dfi = dfi.drop(dfi.select_dtypes('datetime').columns, axis=1)
    except:
        pass
    
    exists = query_impala_basic(f'show tables in analytics like "{name.split(".")[-1]}"').shape[0]
    if exists == 1:
        print(f'{name.split(".")[-1]} already exists in analytics, inserting into it')
    elif exists == 0:
        query_impala_basic(sql_from_df(dfi, name))
        print(f'created {name}')
    else: 
        print(f'{name} seems to exist multiple times. This is likely to throw an error')

    base_sql_text = 'INSERT INTO '+name+' ('+ str(', '.join(dfi.columns)).replace('%', 'pct') + ') VALUES '

    # quotation/apostrophie fix
    apostrophie = 'THISISAPLACEHOLDERFORANAPOSTROPHIEUSEDBYSFULLERCONNECTIONS'
    quotationmark = 'THISISAPLACEHOLDERFORAQUOTATIONMARKUSEDBYSFULLERCONNECTIONS'
    dfi = dfi.applymap(lambda x: x if not isinstance(x, str) else x.replace("'", apostrophie).replace('"', quotationmark))
    
    sql_text = base_sql_text
    counter = 0
    for index, row in dfi.iterrows():  
        # pandas outputs with ' as default so first switch this to " which impala prefers
        # then swap back the placeholder values to their original values, properly escaped for the sql string as it'll be interpreted literally
        row_text = str(tuple(row.values)).replace("'", '"').replace(apostrophie, "\\'").replace(quotationmark, '\\"')
        sql_text = sql_text + row_text + ','   
        counter += 1 
        if counter == 999:
                query_impala_basic(sql_text[0:len(sql_text)-1])
                sql_text = base_sql_text
                counter = 0

    query_impala_basic(sql_text[0:len(sql_text)-1])

    print(f"exported to {name}")

def send_to_s3(df, name, bucket=os.getenv("S3_DEFAULT"), config=S3ConfigFromEnv, force_ints=False, append=False):
    s3 = S3Connect(config, bucket=bucket)
    if append:
        filename = s3.s3_append(df, name, force_ints=force_ints)
    else:
        filename = s3.s3_create(df, name, force_ints=force_ints)
    
    print(f"exported to {filename}")

def read_from_s3(name, header, bucket=os.getenv("S3_DEFAULT"), config=S3ConfigFromEnv):
    s3 = S3Connect(config, bucket=bucket)
    df = s3.s3_read(name, header)
    return df

class DontPickle(Exception):
    pass

class InvalidQueryType(Exception):
    pass
