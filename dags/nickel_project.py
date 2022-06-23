from datetime import timedelta, datetime
import boto3
import os
from io import StringIO
import feedparser
import pandas as pd
from google.cloud import bigquery

# The DAG object; we'll need this to instantiate a DAG
from airflow import DAG
from airflow.decorators import task

# from airflow.utils.dates import days_ago
from airflow.operators.empty import EmptyOperator
from airflow.operators.bash import BashOperator
from airflow.models import Variable
from airflow.operators.email_operator import EmailOperator
from datetime import datetime

## Weather
from meteostat import Point, Daily

## NIKL prices
from fastquant import get_pse_data

## import spacy for transformation
import spacy
import ast

from yaml import load



#####################
# INITIAL VARIABLES #
#####################

# Our bucket
BUCKET_NAME = "nickel-data"
# Folder name
MY_FOLDER_PREFIX = "bonojoves-DEC1-final"
# start date and time - start at 2020
start = datetime(2022, 1, 1)
end = datetime.now()
# Create Point for Cagayan De Oro - Surigao weather station not returning anything
cdo = Point(8.4542, 124.6319)
# file path
DATA_PATH = "/opt/airflow/data/"


#############
# FUNCTIONS #
#############

def upload_formatted_rss_feed(feed, feed_name):
    feed = feedparser.parse(feed)
    df = pd.DataFrame(feed['entries'])
    time_now = datetime.now().strftime('%Y-%m-%d_%I-%M-%S')
    filename = feed_name + '_' + time_now + '.csv'
    df.to_csv(f"{DATA_PATH}{filename}", index=False)

def upload_string_to_gcs(csv_body, uploaded_filename):
    gcs_resource = boto3.resource(
        "s3",
        region_name="auto",
        endpoint_url="https://storage.googleapis.com",
        aws_access_key_id=Variable.get("SERVICE_ACCESS_KEY"),
        aws_secret_access_key=Variable.get("SERVICE_SECRET"),
    )
    gcs_resource.Object(BUCKET_NAME, MY_FOLDER_PREFIX + "/" + uploaded_filename).put(Body=csv_body.getvalue())

def download_weather_data():
    # Get daily data from 2020
    data = Daily(cdo, start, end)
    data = data.fetch()
    # Save the data to .csv
    filename = f"weather-data-{end.strftime('%m-%d-%Y')}.csv"
    data.to_csv(f"{DATA_PATH}/{filename}")
    # upload
    # upload_string_to_gcs(csv_body='weather_data.csv', uploaded_filename=filename)

def download_stock_data(ticker='NIKL'):
    # get data
    data = get_pse_data(ticker, start.strftime('%Y-%m-%d'), end.strftime('%Y-%m-%d'))
    # save data to csv
    filename = f"stock-data-{end.strftime('%m-%d-%Y')}.csv"
    data.to_csv(f"{DATA_PATH}/{filename}")
    # upload
    # upload_string_to_gcs(csv_body='stock_data.csv', uploaded_filename=filename)

#################
# EXTRACT TASKS #
#################

# RSS for business mirror
@task(task_id="business_mirror")
def business_mirror_feed(ds=None, **kwargs):
    upload_formatted_rss_feed("https://businessmirror.com.ph/feed/", "business_mirror")
    return True

# RSS for business mirror
@task(task_id="business_world")
def business_world_feed(ds=None, **kwargs):
    upload_formatted_rss_feed("https://www.bworldonline.com/feed/", "business_world")
    return True

@task(task_id="weather")
def weather_data_meteostat(ds=None, **kwargs):
    download_weather_data()
    return True

@task(task_id="stock_prices")
def stock_prices(ds=None, **kwargs):
    download_stock_data()
    return True


###################
# TRANSFORM TASKS #
###################

@task(task_id="transform")
def data_transform():
    # change file path to proper
    ticker_df = pd.read_csv(f"{DATA_PATH}/stock-data-{end.strftime('%m-%d-%Y')}.csv")
    weather_df = pd.read_csv(f"{DATA_PATH}/weather-data-{end.strftime('%m-%d-%Y')}.csv")
    # combine the 2 dfs
    combined = weather_df.join(ticker_df)
    # columns to drop
    cols_to_drop = ['prcp', 'snow', 'wpgt', 'tsun', 'dt']
    combined.drop(cols_to_drop, axis=1, inplace=True)
    # set_index to date
    combined.set_index('time', inplace=True)
    # replace NaNs with median
    for col in list(combined.columns):
        value = combined[col].median()
        combined[col].fillna(value, inplace=True)
    # save
    filename = f"combined-data-{end.strftime('%m-%d-%Y')}.csv"
    combined.to_csv(f"{DATA_PATH}/{filename}")

##############
# LOAD TASKS #
##############

@task(task_id="load_data")
def load_data(ds=None, **kwargs):
    files = os.listdir(DATA_PATH)
    for file in files:
        outfile = f"{DATA_PATH}{file}"
        if not outfile.endswith('.csv'):
            continue
        
        try:
            df = pd.read_csv(outfile)
            csv_buffer = StringIO()
            df.to_csv(csv_buffer)
            upload_string_to_gcs(csv_body=csv_buffer, uploaded_filename=file)
        except: pass



##############
# DAG PROPER #
##############

with DAG(
    'nickel-scraper-v1',
    # These args will get passed on to each operator
    # You can override them on a per-task basis during operator initialization
    default_args={
        # 'depends_on_past': False,
        # 'email': ['sj_coolness@yahoo.com'],
        # 'email_on_failure': False,
        # 'email_on_retry': False,
        # 'retries': 1,
        # 'retry_delay': timedelta(minutes=5),
        # 'queue': 'bash_queue',
        # 'pool': 'backfill',
        # 'priority_weight': 10,
        # 'end_date': datetime(2016, 1, 1),
        # 'wait_for_downstream': False,
        # 'sla': timedelta(hours=2),
        # 'execution_timeout': timedelta(seconds=300),
        # 'on_failure_callback': some_function,
        # 'on_success_callback': some_other_function,
        # 'on_retry_callback': another_function,
        # 'sla_miss_callback': yet_another_function,
        # 'trigger_rule': 'all_success'
    },
    description='Nickel data scraping',
    schedule_interval=timedelta(days=1),
    start_date=datetime(2022, 6, 15),
    catchup=False,
    tags=['scrapers'],
) as dag:

    tdag_start = BashOperator(
        task_id="dag_start_msg",
        bash_command = "echo Starting",
        dag=dag
    )
    
    tdag_end = BashOperator(
        task_id="dag_end_msg",
        bash_command = "echo End",
        dag=dag
    )


################
# ETL PIPELINE #
################

tdag_start >> [business_world_feed(), business_mirror_feed(), weather_data_meteostat(), stock_prices()] >> data_transform() >> load_data() >> tdag_end
