
from airflow import DAG
from airflow.operators.python import PythonOperator
from datetime import datetime
import requests
import gzip
import pandas as pd
import sqlalchemy

def download_pageviews(ds, **kwargs):
    url = "https://dumps.wikimedia.org/other/pageviews/2024/10/pageviews-20241001-230000.gz"
    response = requests.get(url)
    with open('/tmp/pageviews.gz', 'wb') as f:
        f.write(response.content)

def extract_and_filter():
    with gzip.open('/tmp/pageviews.gz', 'rt') as f:
        lines = f.readlines()
    data = [line.split() for line in lines]
    df = pd.DataFrame(data, columns=["project", "page_name", "views", "bytes"])

    # Filter for specific companies
    companies = ["Amazon", "Apple_Inc.", "Meta_Platforms", "Google", "Microsoft"]
    filtered_df = df[df['page_name'].isin(companies)]

    # Connect to the database
    engine = sqlalchemy.create_engine('postgresql://username:password@localhost/db_name')
    filtered_df.to_sql('pageviews', engine, if_exists='replace', index=False)

def analyze_pageviews():
    engine = sqlalchemy.create_engine('postgresql://username:password@localhost/db_name')
    result = pd.read_sql_query('SELECT page_name, MAX(views) AS max_views FROM pageviews GROUP BY page_name ORDER BY max_views DESC LIMIT 1;', engine)
    print(result)

# Defining the DAG
dag = DAG('wikipedia_pageviews_dag', start_date=datetime(2024, 10, 10), schedule_interval='@hourly')

# Defining the tasks
download_task = PythonOperator(task_id='download_data', python_callable=download_pageviews, dag=dag)
extract_task = PythonOperator(task_id='extract_filter_data', python_callable=extract_and_filter, dag=dag)
analyze_task = PythonOperator(task_id='analyze_data', python_callable=analyze_pageviews, dag=dag)

# Setting up the task dependencies
download_task >> extract_task >> analyze_task

"""## This is an SQL Code to Analyze Highest page views by company."""

SELECT company_name, MAX(views) AS highest_pageviews_by_company
FROM pageviews
GROUP BY company_name
ORDER BY highest_pageviews_by_company DESC
LIMIT 5;
