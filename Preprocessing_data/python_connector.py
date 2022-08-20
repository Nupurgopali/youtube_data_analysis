from logging import getLogger
from snowflake.ingest import SimpleIngestManager
from snowflake.ingest import StagedFile
from snowflake.ingest.utils.uris import DEFAULT_SCHEME
from datetime import timedelta
from requests import HTTPError
from cryptography.hazmat.primitives import serialization
from cryptography.hazmat.primitives.serialization import load_pem_private_key
from cryptography.hazmat.backends import default_backend
from cryptography.hazmat.primitives.serialization import Encoding
from cryptography.hazmat.primitives.serialization import PrivateFormat
from cryptography.hazmat.primitives.serialization import NoEncryption
import time
import datetime
import os
import logging
import sys
import pandas as pd
import snowflake
from snowflake.connector.pandas_tools import pd_writer,write_pandas
from itertools import count
from decouple import config
import snowflake.connector as snow
from snowflake.connector.pandas_tools import write_pandas

"""
Helps in converting pandas datatype into snowflake datatypes
"""
def get_table_metadata(df):
     def map_dtypes(x):
         if (x == 'object') or (x=='category') or (x=='bool'):
             return 'NVARCHAR'
         elif 'date' in x:
             return 'DATETIME'
         elif 'int'  in x:
             return 'NUMERIC'
         elif 'float' in x:
             return 'FLOAT'
         else:
             logging.info(f"cannot parse pandas dtype for type {x}")
     sf_dtypes = [map_dtypes(str(s)) for s in df.dtypes]
     table_metadata = ", ". join([" ".join([y.upper(), x]) for x, y in zip(sf_dtypes, list(df.columns))])
     return table_metadata
 
"""
Send dataframe to snowflake data warhouse.
Accepts two operations:
1. Create_Replace: If the table already exists,it will replace it else will create a new table.
2. Insert: It will append new rows to the existing table.
""" 
def df_to_snowflake_table(table_name, operation, df, conn): 
     if operation=='create_replace':
         df.columns = [c.upper() for c in df.columns]
         table_metadata = get_table_metadata(df)
         conn.cursor().execute(f"CREATE OR REPLACE TABLE {table_name} ({table_metadata})")
         write_pandas(conn, df, table_name.upper())
         
     elif operation=='insert':
         table_rows = str(list(df.itertuples(index=False, name=None))).replace('[','').replace(']','')
         conn.cursor().execute(f"INSERT INTO {table_name} VALUES {table_rows}") 
 
"""
Send dataframe to snowflake data warhouse.
Key Pair Authentication & Key Pair Rotation is used for account authentication.Once connection is established with snowflake connector
the dataframes are send to CONTINENT_WISE_YOUTUBE_DATA database.
"""
def sending_data_to_snowflake():

 youtube_dir = os.path.join(os.path.dirname(os.path.abspath(__file__)), os.pardir)
 logging.basicConfig( filename=os.path.join(os.path.relpath(youtube_dir),'Preprocessing_data\\app.log'),filemode='w',level=logging.INFO,format="%(asctime)s %(message)s")
 
 logging.info('Reading the csv file')
 NA_Data=pd.read_csv(os.path.join(os.path.relpath(youtube_dir),'Data\Continent\Clean_NA_data.csv'),low_memory=False,encoding='gbk')
 AS_Data=pd.read_csv(os.path.join(os.path.relpath(youtube_dir),'Data\Continent\Clean_AS_data.csv'),low_memory=False,encoding='gbk')
 EU_Data=pd.read_csv(os.path.join(os.path.relpath(youtube_dir),'Data\Continent\Clean_EU_data.csv'),low_memory=False,encoding='gbk')
 Fact_Table=pd.read_csv(os.path.join(os.path.relpath(youtube_dir),'Data\Continent\\fact_table.csv'),low_memory=False)
 tables=[NA_Data,AS_Data,EU_Data,Fact_Table]
 tables_name=['NA_Dim_Table','AS_Dim_Table','EU_Dim_Table','Fact_Table']

 # Importing the required packages for all your data framing needs.
 logging.info('Starting Key Pair Authentication & Key Pair Rotation')
 
 with open(os.path.join(os.path.relpath(youtube_dir),'Preprocessing_data\\rsa_key.p8'), "rb") as key:
     p_key= serialization.load_pem_private_key(
         key.read(),
         password=config('KEY_PASSPHRASE',cast=str).encode(),
         backend=default_backend()
     )
 
 pkb = p_key.private_bytes(
     encoding=serialization.Encoding.DER,
     format=serialization.PrivateFormat.PKCS8,
     encryption_algorithm=serialization.NoEncryption())
 logging.info('connecting to cursor')
 
 ctx = snowflake.connector.connect(
     user=config('USER',cast=str),
     account=config('ACCOUNT',cast=str),
     private_key=pkb,
     warehouse='COMPUTE_WH',
     database='CONTINENT_WISE_YOUTUBE_DATA',
     schema='CONTINENT_DATA'
     )
 cs = ctx.cursor()
 logging.info('Connected to the snowflake')
 logging.info('sending data to snowflake')

 
 for _,df,title in zip(count(),tables,tables_name):
     logging.info(f"Sending {title} to snowflake")
     df_to_snowflake_table(title, 'create_replace',df,ctx)
     logging.info(f'{title} successfully sent!')
 
 
 
 
 
 
 