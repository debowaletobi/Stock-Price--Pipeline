from airflow.hooks.base import BaseHook
from airflow.exceptions import AirflowNotFoundException
import requests
import logging
import json
from io import BytesIO
from minio import Minio


bucket_name='stock-market'

def _get_stock_prices(url, symbol):
    url = url.split('AAPL?')[0]
    url = f"{url}{symbol}?metrics=high?&interval=1d&range=365d"
    print(f"This is the new url: {url}")
    api = BaseHook.get_connection('stock_api')
    try:
        response = requests.get(url, headers=api.extra_dejson['headers'])
        response.raise_for_status()  
        data = response.json()  # Parse the JSON response
        result = data['chart']['result'][0]  
        print(f'data-{type(data)}', data)    
        print(f'result-{type(result)}', result)
        return json.dumps(result)
    except requests.exceptions.RequestException as e:
        return f"Request failed: {e}"
    except ValueError as e:
        return f"Error parsing JSON: {e}"
    except Exception as e:
        return f"Error occurred: {e}"
    
def get_minio_client(client_name='minio'):
    minio=BaseHook.get_connection(client_name).extra_dejson
    client = Minio(
        endpoint=minio['endpoint_url'].split('//')[1],
        access_key=minio['aws_access_key_id'],
        secret_key=minio['aws_secret_access_key'],
        secure=False)
    return client

def _store_prices(stock):
    print(type(stock))
    print(stock)
    if isinstance(stock, str):
        #stock = fix_single_quotes(stock)
        try:
            stock = json.loads(stock)
        except json.JSONDecodeError as e:
            print(f"JSON decode error: {e}")
            raise

    client = get_minio_client()
    bucket_name = 'stock-market'
    if not client.bucket_exists(bucket_name):
        client.make_bucket(bucket_name)
    symbol = stock['meta']['symbol']
    data = json.dumps(stock, ensure_ascii=True).encode('utf8')
    objw = client.put_object(
        bucket_name=bucket_name,
        object_name=f'{symbol}/prices.json',
        data=BytesIO(data),
        length=len(data)
    )
    return f'{objw.bucket_name}/{symbol}'


def _get_formatted_csv(path,bucket_name=bucket_name):
    client = get_minio_client()
    prefix_name = f"{path.split('/')[1]}/formatted_prices"
    objects = client.list_objects(bucket_name, prefix=prefix_name,recursive=True)
    for obj in objects:
        if obj.object_name.endswith('.csv'):
            return obj.object_name
    raise AirflowNotFoundException('The csv file does not exist')


def resolve_file_path(**kwargs):
    task_instance = kwargs['task_instance']
    formatted_csv_path = task_instance.xcom_pull(task_ids='get_formatted_csv')
    
    # Generate a pre-signed URL
    client = get_minio_client()
    bucket_name = 'stock-market'
    try:
        presigned_url = client.presigned_get_object(bucket_name, formatted_csv_path)
        return presigned_url
    except Exception as e:
        logging.error(f"Failed to generate pre-signed URL: {e}")
        raise