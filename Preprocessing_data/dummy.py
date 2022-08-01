import os
import pandas as pd
import sys
import random
import logging
from itertools import count


path = os.getcwd()
print("Current Directory:", path)
  
# parent directory
parent = os.path.join(os.getcwd(), os.pardir)
  
#prints parent directory
print("\nParent Directory:", os.path.relpath(parent))
df=pd.read_csv(os.path.join(os.path.relpath(parent),'Data\Continent\Clean_AS_Data.csv'),encoding='gbk')
print(df.head)

# import chardet
# with open(os.path.join(os.path.relpath(parent),'Data\MXvideos.csv'), 'rb') as rawdata:
#     result = chardet.detect(rawdata.read(100000))
# print(result)
