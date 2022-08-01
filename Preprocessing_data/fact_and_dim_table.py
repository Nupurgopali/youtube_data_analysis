import os
import pandas as pd
import sys
import random
import logging
from itertools import count

#os.chdir('..')
def creating_Fact_table():
 youtube_dir = os.path.join(os.path.dirname(os.path.abspath(__file__)), os.pardir)
 logging.basicConfig( filename=os.path.join(os.path.relpath(youtube_dir),'Preprocessing_data\\app.log'),
 filemode='w',level=logging.INFO,format="%(asctime)s %(message)s")
 
 NA_Data=pd.read_csv(os.path.join(os.path.relpath(youtube_dir),'Data\Continent\Clean_NA_data.csv'),low_memory=False,encoding='gbk')
 AS_Data=pd.read_csv(os.path.join(os.path.relpath(youtube_dir),'Data\Continent\Clean_AS_data.csv'),low_memory=False,encoding='gbk')
 EU_Data=pd.read_csv(os.path.join(os.path.relpath(youtube_dir),'Data\Continent\Clean_EU_data.csv'),low_memory=False,encoding='gbk')
 #print(NA_Data.shape,' ',AS_Data.shape,' ',EU_Data.shape)
 continent=[NA_Data,EU_Data,AS_Data]
 
 def generate_surrogate_key(df,lower_limit,upper_limit):
     df['video_key']=random.sample(range(lower_limit,upper_limit), df.shape[0])
     return df
 
 logging.info('Calling generate_surrogate_key func')
 NA_Data=generate_surrogate_key(NA_Data,100000,1000000)
 EU_Data=generate_surrogate_key(EU_Data,1000000,10000000)
 AS_Data=generate_surrogate_key(AS_Data,10000000,100000000)
 logging.info('Finished executing generate_surrogate_key func')
 
 #print(NA_Data['video_key'].duplicated().any())
 #print(EU_Data['video_key'].duplicated().any())
 #print(AS_Data['video_key'].duplicated().any())
 #print(set(NA_Data['video_key']).intersection(set(AS_Data['video_key'])).intersection(set(EU_Data['video_key'])))
 
 logging.info('Checking if fact_table exists and, if exists then removing it.')
 if os.path.exists(os.path.join(os.path.join(os.path.relpath(youtube_dir),'Data\Continent\fact_table.csv'))):
     os.remove(os.path.join(os.path.join(os.path.relpath(youtube_dir),'Data\Continent\fact_table.csv')))
 
 logging.info('Creating fact_table')
 fact_table=pd.DataFrame()
 
 def video_interaction_percentage(dest_df,source_df,col_name):
     if dest_df.shape[0]>0:
         dest_df[col_name]=[0]*dest_df.shape[0]
     else:
         dest_df[col_name]=[0]*source_df.shape[0]
     logging.info(f'Starting processing for col {col_name}')
     for i in range(source_df.shape[0]):
         dest_df[col_name][i]='{:.2f}'.format(((source_df['likes'][i]+source_df['dislikes'][i]+source_df['comment_count'][i])/source_df['views'][i])*100)
         #print(i,' ','{:.2f}'.format(((source_df['likes'][i]+source_df['dislikes'][i]+source_df['comment_count'][i])/source_df['views'][i])*100))
     logging.info(f'Finished processing for col {col_name}')
 
     
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
 for i,df,title in zip(count(),continent,csv_title):
     csv_name='Clean_{name}.csv'.format(name=title)
     os.remove(os.path.join(os.path.relpath(youtube_dir),'Data\Continent\\',csv_name))
     #df.drop(['views','likes','dislikes'],inplace=True,axis=1)
     df.to_csv(os.path.join(os.path.relpath(youtube_dir),'Data\Continent\\',csv_name),encoding='gbk',index=False)
 