from prefect import task, Flow
from Preprocessing_data.creating_dimension_table import create_dim_table
from Preprocessing_data.cleaning_dim_table import clean_data
from Preprocessing_data.fact_and_dim_table import creating_Fact_table
from Preprocessing_data.python_connector import sending_data_to_snowflake
from datetime import timedelta, datetime
from prefect.schedules.clocks import RRuleClock
import pendulum
from dateutil.rrule import rrule, DAILY
from prefect.schedules import Schedule
import datetime
from prefect.schedules import IntervalSchedule


start_date = pendulum.now().add(days=1)
r_rule = rrule(freq=DAILY, count=7)

schedule = IntervalSchedule(interval=timedelta(hours=24))

@task
def create_Data():
    create_dim_table()
    return
@task 
def clean_Data():
    clean_data()
    return

@task
def create_ft():
    creating_Fact_table()
    return
@task
def send_data_sf():
    sending_data_to_snowflake()
    return

with Flow('Youtube_ETL',schedule) as flow:
    task1=create_Data()
    task2=clean_Data(upstream_tasks=[task1])
    task3=create_ft(upstream_tasks=[task2])
    task4=send_data_sf(upstream_tasks=[task3])
    
flow.register(project_name='youtube_analysis')
#flow.run()
