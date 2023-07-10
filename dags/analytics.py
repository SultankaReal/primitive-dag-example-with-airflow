import logging as _log

from airflow import DAG
from airflow.utils.dates import days_ago

from airflow.providers.postgres.hooks.postgres import PostgresHook
from airflow.hooks.base import BaseHook

from airflow.operators.python import PythonOperator
from airflow.providers.postgres.operators.postgres import PostgresOperator

import requests
import json

from typing import Optional
from typing import List
from pydantic import BaseModel, ValidationError

PG_CONN_ID = 'analytics'
PG_TABLE = 'cats_new'

URL_RESPONSE_CAT_FACTS = requests.get('https://catfact.ninja/fact')


def conn_cat_facts():
     try:
         URL_RESPONSE_CAT_FACTS.raise_for_status()
     except Exception as ex:
         print(ex)

     _log.info('Connecting to cat_facts api')


def get_cat_facts():
    dict_of_values_cat_facts = json.dumps(URL_RESPONSE_CAT_FACTS.json())

    class CatFacts(BaseModel):
        fact: str
        length: int

    try:
        catfacts = CatFacts.parse_raw(dict_of_values_cat_facts)
    except ValidationError as e:
        print("Exception",e.json())
    else:
        cat_facts = catfacts.fact
        cat_facts_length = catfacts.length
    return cat_facts, cat_facts_length

def put_to_psql(**context):
    population_string = """ CREATE TABLE IF NOT EXISTS {0} (CatFact TEXT, Length INT, CurrentTime timestamp);
                            INSERT INTO {0}
                            (CatFact, Length, CurrentTime)
                            VALUES ($uniq_tAg${1}$uniq_tAg$,$uniq_tAg${2}$uniq_tAg$, current_timestamp);
                        """ \
                        .format(PG_TABLE, get_cat_facts()[0], get_cat_facts()[1]) 
    return population_string


def select_from_table(**context):
    select_string = """ SELECT * FROM cats_new; """
    return select_string

args = {
    'owner': 'airflow',
    'catchup': 'False',
}

with DAG(
    dag_id='analytics',
    default_args=args,
    schedule_interval='*/20 * * * *',
    start_date=days_ago(2),
    max_active_runs=2,
    tags=['4otus', 'API'],
    catchup=False,
) as dag:
    task_connect_cat_facts = PythonOperator(
        task_id='connect_to_api_cat_facts',
        python_callable=conn_cat_facts,
        dag=dag
    )
    task_get_cat_facts = PythonOperator(
        task_id='get_cat_facts',
        python_callable=get_cat_facts,
        #provide_context=True,
        dag=dag
    )
    task_populate = PostgresOperator(
        task_id="put_to_psql_cat_facts",
        postgres_conn_id=PG_CONN_ID,
        sql=put_to_psql(),
        #provide_context=True,
        dag=dag
    )
    task_select_from_table = PostgresOperator(
        task_id="select_from_cat_facts",
        postgres_conn_id=PG_CONN_ID,
        sql=select_from_table(),
        #provide_context=True,
        dag=dag
    )

    task_connect_cat_facts >> task_get_cat_facts >> task_populate >> task_select_from_table
