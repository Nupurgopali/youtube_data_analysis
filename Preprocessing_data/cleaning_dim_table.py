import pandas as pd
import os
import sys
import re
from itertools import count
import logging

"""
It will get rid of duplicate values in the table
"""
def handle_null_and_dup_values(df):
     df.drop_duplicates(subset='video_id',inplace=True)
     df=df.reset_index(drop=True)
     return df

"""
Remove all the non-alphanumeric characters in the string.
""" 
def clean_text(df):
     df.replace(to_replace=[r"\\t|\\n|\\r", "\t|\n|\r",r' +',r'\.+'], value=["",""," ","."], regex=True, inplace=True)
     return df

"""
Perform basic cleaning functions such as removing null,duplicate values, cleaning the string ,etc.
"""
def clean_data():
 youtube_dir = os.path.join(os.path.dirname(os.path.abspath(__file__)), os.pardir)
 logging.basicConfig( filename=os.path.join(os.path.relpath(youtube_dir),'Preprocessing_data\\app.log'),filemode='w',level=logging.INFO,format="%(asctime)s %(message)s")
 
 NA_Data=pd.read_csv(os.path.join(os.path.relpath(youtube_dir),'Data\Continent\\NA_data.csv'),low_memory=False,encoding='gbk')
 AS_Data=pd.read_csv(os.path.join(os.path.relpath(youtube_dir),'Data\Continent\\AS_data.csv'),low_memory=False,encoding='gbk')
 EU_Data=pd.read_csv(os.path.join(os.path.relpath(youtube_dir),'Data\Continent\\EU_data.csv'),low_memory=False,encoding='gbk')
 
 logging.info('Starting to clean the continent files')
 
 continent=[NA_Data,EU_Data,AS_Data]
 
 csv_title=['NA_Data','EU_Data','AS_Data']
 for _,df,title in zip(count(),continent,csv_title):
     logging.info(f'Calling handle_null and clean text func for {title}')
     df=handle_null_and_dup_values(df)
     df=clean_text(df)
     csv_name='Clean_{name}.csv'.format(name=title)
     logging.info(f'Removing {csv_name} file that already exsists.')
     if os.path.exists(os.path.join(os.path.relpath(youtube_dir),'Data\Continent\\',csv_name)):
         os.remove(os.path.join(os.path.relpath(youtube_dir),'Data\Continent\\',csv_name))
     logging.info(f'saving the {csv_name} file')
     df.to_csv(os.path.join(os.path.relpath(youtube_dir),'Data\Continent\\',csv_name),encoding='gbk',index=False,mode='a')
 
 