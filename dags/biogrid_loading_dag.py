import logging
import pendulum
import pandas as pd
import requests
from bs4 import BeautifulSoup
from airflow import DAG
from airflow.operators.python import PythonOperator, BranchPythonOperator
from airflow.operators.empty import EmptyOperator
from airflow.providers.postgres.hooks.postgres import PostgresHook
from airflow.models import (
    Param,
    Variable,
)
from airflow.providers.amazon.aws.hooks.s3 import S3Hook

def get_latest_version():
    logging.info('Retrieving the latest BioGRID version...')
    biogrid_base_url = Variable.get('biogrid_base_url')
    response = requests.get(biogrid_base_url)

    if response.status_code != 200:
        logging.error(f"Failed to retrieve BioGRID versions. Status code: {response.status_code}")
        raise Exception("Failed to retrieve BioGRID versions.")

    logging.info('Successfully retrieved the BioGRID versions page.')
    soup = BeautifulSoup(response.content, 'html.parser')
    table = soup.find('table', class_='table table-sm table-striped')

    if table:
        folder_icons = table.find_all('i', class_='fa fa-lg fa-folder lightBlueIcon')

        if folder_icons:
            version_numbers = []
            for icon in folder_icons:
                latest_version = icon.find_next('a').text.strip()
                version_number = latest_version.split('BIOGRID-')[-1]
                version_numbers.append(version_number)

            latest_version = max(version_numbers, key=lambda v: [int(x) for x in v.split('.')])
            return latest_version
        else:
            raise Exception("No folder icons found.")
    else:
        raise Exception("Table not found.")


def _check_version_existence(**kwargs):
    biogrid_version = kwargs['ti'].xcom_pull(task_ids='get_latest_version')

    postgres_hook = PostgresHook(postgres_conn_id='postgres_local')
    connection = postgres_hook.get_conn()
    with connection.cursor() as cursor:
        cursor.execute('SELECT DISTINCT(version) FROM biogrid_data;')
        loaded_versions = [version[0] for version in cursor.fetchall()]

        if biogrid_version not in loaded_versions:
            return 'load_latest_data'
        else:
            return 'finish'


def load_biogrid(version):
    logging.info('Loading biogrid file...')
    biogrid_url = Variable.get('biogrid_url')
    response = requests.get(
        biogrid_url.format(version=version),
        params={'downloadformat': 'zip'}
    )

    if response.status_code == 200:
        s3_hook = S3Hook(aws_conn_id='aws_conn_id')
        s3_bucket = Variable.get('s3_bucket_name')
        s3_key = f'biogrid/biogrid_{version}.zip'

        s3_hook.load_bytes(response.content, key=s3_key, bucket_name=s3_bucket, replace=True)
        logging.info(f'Biogrid file has been uploaded to S3 at s3://{s3_bucket}/{s3_key}')
    else:
        logging.error("The specified version is not found")
        raise Exception("Failed to download the specified version.")


def ingest_data(version):
    s3_hook = S3Hook(aws_conn_id='aws_conn_id')
    s3_bucket = Variable.get('s3_bucket_name')
    s3_key = f'biogrid/biogrid_{version}.zip'
    s3_url = s3_hook.get_uri(key=s3_key, bucket_name=s3_bucket)

    df = pd.read_csv(s3_url, delimiter='\t', compression='zip', nrows=100)

    df = df.rename(
        lambda column_name: column_name.lower().replace(' ', '_').replace('#', '_').strip('_'),
        axis='columns'
    )

    df = df[[
        'biogrid_interaction_id',
        'biogrid_id_interactor_a',
        'biogrid_id_interactor_b',
    ]]

    df['version'] = version

    logging.info('Biogrid file has been transformed')
    logging.info('Starting ingestion into database...')

    postgres_hook = PostgresHook(postgres_conn_id='postgres_local')
    engine = postgres_hook.get_sqlalchemy_engine()
    df.to_sql('biogrid_data', engine, if_exists='append')
    logging.info('Data successfully ingested')


with DAG(
        dag_id='biogrid_loading_dag',
        start_date=pendulum.today(),
        schedule_interval=None,
        tags=['lesson1', 'biogrid'],
        description='A DAG to load biogrid from website into Postgres database',
        catchup=False
) as dag:
    start_op = EmptyOperator(task_id='start')

    get_latest_version_op = PythonOperator(
        task_id='get_latest_version',
        python_callable=get_latest_version,
    )

    check_version_existence_op = BranchPythonOperator(
        task_id='check_version_existence',
        python_callable=_check_version_existence,
        provide_context=True,
    )

    load_data_op = PythonOperator(
        task_id='load_latest_data',
        python_callable=lambda **kwargs: load_biogrid(kwargs['ti'].xcom_pull(task_ids='get_latest_version')),
        provide_context=True,
    )

    ingest_data_op = PythonOperator(
        task_id='ingest_data',
        python_callable=lambda **kwargs: ingest_data(kwargs['ti'].xcom_pull(task_ids='get_latest_version')),
        provide_context=True,
    )

    finish_op = EmptyOperator(
        task_id='finish',
    )
    start_op >> get_latest_version_op >> check_version_existence_op
    check_version_existence_op >> load_data_op >> ingest_data_op >> finish_op
    check_version_existence_op >> finish_op
