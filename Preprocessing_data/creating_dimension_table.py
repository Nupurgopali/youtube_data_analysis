import pandas as pd 
import os
import sys
from os import listdir
import glob
import logging
from itertools import count

"""
3 dimension table: 'NA_data','EU_data','AS_data' are created
"""
def create_dim_table():
 youtube_dir = os.path.join(os.path.dirname(os.path.abspath(__file__)), os.pardir)
 #print('Dir',os.path.join(os.path.relpath(youtube_dir),'Preprocessing_data\\app.log'))
 logging.basicConfig( filename=os.path.join(os.path.relpath(youtube_dir),'Preprocessing_data\\app.log'),filemode='w',level=logging.INFO,format="%(asctime)s %(message)s")
 
 data_path=os.path.join(os.path.relpath(youtube_dir),'Data')
 
 continent=[NA_Data,EU_Data,AS_Data]
 csv_title=['NA_data','EU_data','AS_data']
 north_america=['US','CA','MX']
 europe=['GB','DE','FR','RU']
 asia=['KR','IN','JP']
 
 logging.info('Dividing the dataset into 3 regions Asia,Europe and North America')
 NA_Data,EU_Data,AS_Data=pd.DataFrame(),pd.DataFrame(),pd.DataFrame()
 
 for filename in glob.glob(data_path+'/*.csv'):
     
     temp_df=pd.read_csv(filename,low_memory=False,encoding='latin')
 
     if any(val in filename for val in north_america):
         logging.info(f'Adding {filename} to NA region')
         
         NA_Data=pd.concat([NA_Data,temp_df],axis=0)
     elif any(val in filename for val in europe):
         #print(filename)
         logging.info(f'Adding {filename} to EU region')
         EU_Data=pd.concat([EU_Data,temp_df],axis=0)
     else:
         logging.info(f'Adding {filename} to AS region')
         AS_Data=pd.concat([AS_Data,temp_df],axis=0)
 
 for i,df,title in zip(count(),continent,csv_title):
     
     logging.info(f'Removing {csv_name} file that already exsists.')
     csv_name='{name}.csv'.format(name=title)
     if os.path.exists(os.path.join(os.path.relpath(youtube_dir),'Data\Continent\\',csv_name)):
         os.remove(os.path.join(os.path.relpath(youtube_dir),'Data\Continent\\',csv_name))
     logging.info(f'saving the {title} file')
     df.to_csv(os.path.join(os.path.relpath(youtube_dir),'Data\Continent\\',csv_name),index=False,mode='a')

 
 
 