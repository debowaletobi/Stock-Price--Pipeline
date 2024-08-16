from airflow.decorators import dag, task
from airflow.operators.python import PythonOperator
from airflow.providers.docker.operators.docker import DockerOperator
from astro import sql as aql
from astro.files import File
from astro.sql.table import Table, Metadata
from airflow.sensors.base import PokeReturnValue
from airflow.hooks.base import BaseHook
from datetime import datetime
from include.stock_markets.tasks import _get_stock_prices, _store_prices, _get_formatted_csv, resolve_file_path ,bucket_name
import requests
import logging


SYMBOL = 'AAPL'
@dag(
    start_date=datetime(2024,8,7),
    schedule='@daily',
    catchup=False,
    tags=['stock_market']
)
def stock_market():

    @task.sensor(poke_interval=10, timeout=10, mode='poke')
    def is_api_available()-> PokeReturnValue:
        api = BaseHook.get_connection('stock_api')
        url = f"{api.host}"
        try:
            response = requests.get(url, headers=api.extra_dejson['headers'])
            response.raise_for_status()  # This will raise an HTTPError for bad responses
            json_response = response.json()
            condition = json_response.get('chart', {}).get('error') is None
            return PokeReturnValue(is_done=condition, xcom_value=url)
        except requests.exceptions.RequestException as e:
            logging.error(f"Request failed: {e}")
            return PokeReturnValue(is_done=False,xcom_value='boy')
        except ValueError as e:
            logging.error(f"Invalid JSON response: {e}")
            return PokeReturnValue(is_done=False, xcom_value='boy')
    

    get_stock_prices = PythonOperator(
        task_id='get_stock_prices',
        python_callable=_get_stock_prices,
        op_kwargs={'url':'{{task_instance.xcom_pull(task_ids="is_api_available", key="return_value")}}', 'symbol':SYMBOL},
        #setting trigger_rule to all_done just for testing purspose 
        trigger_rule='all_done'
    )

    store_prices = PythonOperator(
        task_id='store_prices',
        python_callable=_store_prices,
        op_kwargs={'stock':'{{ task_instance.xcom_pull(task_ids="get_stock_prices", key="return_value") }}'}
    )

    format_prices = DockerOperator(
        task_id='format_prices',
        image='airflow/stock-app',
        container_name='format_prices',
        api_version='auto',
        auto_remove=True,
        docker_url='tcp://docker-proxy:2375',
        network_mode='container:spark-master',
        tty=True,
        xcom_all=False,
        mount_tmp_dir=False,
        environment= {
            'SPARK_APPLICATION_ARGS': '{{ task_instance.xcom_pull(task_ids="store_prices", key="return_value") }}'
        }
    )

    get_formatted_csv = PythonOperator(
        task_id='get_formatted_csv',
        python_callable=_get_formatted_csv,
        op_kwargs={'path':'{{ task_instance.xcom_pull(task_ids="store_prices", key="return_value") }}'}
    )

    resolve_file_path_task = PythonOperator(
        task_id='resolve_file_path',
        python_callable=resolve_file_path,
        provide_context=True
    )

    load_to_dw = aql.load_file(
        task_id='load_to_dw',
        input_file=File(path='{{ task_instance.xcom_pull(task_ids="resolve_file_path") }}',
                       conn_id='minio'),
        output_table=Table(
            name='stock_market',
            conn_id='postgres',
            metadata=Metadata(schema='public')
        )
    )


    is_api_available() >> get_stock_prices >> store_prices >> format_prices >> get_formatted_csv >> resolve_file_path_task >> load_to_dw

stock_market()