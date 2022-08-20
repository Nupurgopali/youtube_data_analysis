import os
import pandas as pd
import sys
import random
import logging
from itertools import count

"""
Create surrogate key that acts as primary key for the table and as a foreign key while performing joining operations.
"""
def generate_surrogate_key(df,lower_limit,upper_limit):
     df['video_key']=random.sample(range(lower_limit,upper_limit), df.shape[0])
     return df

"""
Calculates percentage of video interaction, which provides an overview about the most interacted videos in 
each region using views,dislikes and likes rates.
"""
def video_interaction_percentage(dest_df,source_df,col_name):
     logging.info(f'Starting processing for col {col_name}')
     dest_df[col_name]=round((source_df['likes']+source_df['dislikes']+source_df['comment_count'])/source_df['views']*100,2)
     dest_df[col_name].fillna(0,inplace=True)
     logging.info(f'Finished processing for col {col_name}')

"""
Create a fact table.
"""
def creating_Fact_table():
 youtube_dir = os.path.join(os.path.dirname(os.path.abspath(__file__)), os.pardir)
 logging.basicConfig( filename=os.path.join(os.path.relpath(youtube_dir),'Preprocessing_data\\app.log'),
 filemode='w',level=logging.INFO,format="%(asctime)s %(message)s")
 
 NA_Data=pd.read_csv(os.path.join(os.path.relpath(youtube_dir),'Data\Continent\Clean_NA_data.csv'),low_memory=False,encoding='gbk')
 AS_Data=pd.read_csv(os.path.join(os.path.relpath(youtube_dir),'Data\Continent\Clean_AS_data.csv'),low_memory=False,encoding='gbk')
 EU_Data=pd.read_csv(os.path.join(os.path.relpath(youtube_dir),'Data\Continent\Clean_EU_data.csv'),low_memory=False,encoding='gbk')
 continent=[NA_Data,EU_Data,AS_Data]
 
 
 logging.info('Calling generate_surrogate_key func')
 NA_Data=generate_surrogate_key(NA_Data,100000,1000000)
 EU_Data=generate_surrogate_key(EU_Data,1000000,10000000)
 AS_Data=generate_surrogate_key(AS_Data,10000000,100000000)
 logging.info('Finished executing generate_surrogate_key func')
 
 
 logging.info('Checking if fact_table exists and, if exists then removing it.')
 if os.path.exists(os.path.join(os.path.join(os.path.relpath(youtube_dir),'Data\Continent\fact_table.csv'))):
     os.remove(os.path.join(os.path.join(os.path.relpath(youtube_dir),'Data\Continent\fact_table.csv')))
 
 logging.info('Creating fact_table')
 fact_table=pd.DataFrame()
     
 video_interaction_percentage(fact_table,EU_Data, 'eu_video_interaction_rate')
 video_interaction_percentage(fact_table,NA_Data, 'na_video_interaction_rate')
 video_interaction_percentage(fact_table,AS_Data,'as_video_interaction_rate')
 fact_table['NA_key']=NA_Data['video_key']
 fact_table['AS_key']=AS_Data['video_key']
 fact_table['EU_key']=EU_Data['video_key']
 
 logging.info('Saving fact table')
 fact_table.to_csv(os.path.join(os.path.join(os.path.relpath(youtube_dir),'Data\Continent\\fact_table.csv')),index=False)
 logging.info('Fact table saved')
 
 logging.info('Updating and saving continents csv')
 csv_title=['NA_Data','EU_Data','AS_Data']
 for _,df,title in zip(count(),continent,csv_title):
     csv_name='Clean_{name}.csv'.format(name=title)
     os.remove(os.path.join(os.path.relpath(youtube_dir),'Data\Continent\\',csv_name))
     df.to_csv(os.path.join(os.path.relpath(youtube_dir),'Data\Continent\\',csv_name),encoding='gbk',index=False)

