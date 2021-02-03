from .connection import ImpalaConnect, S3Connect, ImpalaConfigFromEnv, S3ConfigFromEnv
from pickle import dump as pickle_dump, load as pickle_load
import pandas as pd
import os

def query_impala(queryobj, config=ImpalaConfigFromEnv):
    try:
        if os.getenv("SFULLER_LOCAL_MACHINE") == "TRUE":
            df = pickle_load(open(f"pickled_data/{queryobj.name}.sav", "rb"))
        else:
            raise DontPickle 
    except:
        con = ImpalaConnect(query=queryobj.query, config=config)
        df = (ImpalaConnect.get_impala_df(con))
        try:
            df = df.reset_index(drop=True)

            for col in df.select_dtypes(include='object').columns:
                try:
                    df[col] = df[col].astype('float')
                except:
                    pass
            
            if os.getenv("SFULLER_LOCAL_MACHINE") == "TRUE":
                if not os.path.exists('pickled_data'):
                    os.makedirs('pickled_data')
                pickle_dump(df, open(f"pickled_data/{queryobj.name}.sav", "wb"))

        except AttributeError:
            df = None
    return df

def query_impala_basic(query, config=ImpalaConfigFromEnv):
    con = ImpalaConnect(query=query, config=config)
    df = (ImpalaConnect.get_impala_df(con))
    try:
        df = df.reset_index(drop=True)
    except AttributeError:
        df = None
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

    try:
        if dfi.select_dtypes('datetime').shape[1] > 0:
            print('dropping timestamp columns due to impala compatibility issues')
            dfi = dfi.drop(dfi.select_dtypes('datetime').columns, axis=1)
    except:
        pass
    
    query_impala_basic(sql_from_df(dfi, name))
    print(f'created {name}')

    base_sql_text = 'INSERT INTO '+name+' ('+ str(', '.join(dfi.columns)).replace('%', 'pct') + ') VALUES '

    sql_text = base_sql_text
    counter = 0
    for index, row in dfi.iterrows():       
        sql_text = sql_text + str(tuple(row.values)) + ','   
        counter += 1 
        if counter == 999:
                query_impala_basic(sql_text[0:len(sql_text)-1])
                sql_text = base_sql_text
                counter = 0

    sql_text = sql_text
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