import uuid
from datetime import datetime, timedelta
from airflow import DAG
from airflow.operators.python import PythonOperator

default_args = {
    'owner': 'OsamaMustafa32',
    'start_date': datetime(2026, 1, 1, 10, 0),
    'retries': 2,
    'retry_delay': timedelta(minutes=5),
    'tags': ['portfolio', 'kafka', 'streaming']
}

def get_data():
    import requests

    res = requests.get('https://randomuser.me/api/')
    res = res.json()
    res = res['results'][0]

    return res

def format_data(res):
    data = {}
    location = res['location']
    data['id'] = str(uuid.uuid4())
    data['first_name'] = res['name']['first']
    data['last_name'] = res['name']['last']
    data['gender'] = res['gender']
    data['address'] = f"{str(location['street']['number'])} {location['street']['name']}, {location['city']}, {location['state']}, {location['country']}"
    data['postcode'] = location['postcode']
    data['email'] = res['email']
    data['username'] = res['login']['username']
    data['dob'] = res['dob']['date']
    data['registered_date'] = res['registered']['date']
    data['phone'] = res['phone']
    data['picture'] = res['picture']['medium']
    
    return data


def stream_data():
    import json
    import time
    import logging
    from kafka import KafkaProducer

    producer = KafkaProducer(
        bootstrap_servers=['broker:29092'],
        max_block_ms=5000
        )
    current_time = time.time()

    while True:
        if time.time() > current_time + 300:
            break
        try:
            res = get_data()
            res = format_data(res)
            producer.send('users_data', json.dumps(res).encode('utf-8'))
            print("Sent data to Kafka")
        except Exception as e:
            logging.error(f"Error sending data to Kafka: {e}")
            continue


with DAG('kafka_stream', 
        default_args=default_args, 
        schedule_interval='@daily', 
        catchup=False) as dag:

    streaming_task = PythonOperator(
        task_id='stream_data_from_api',
        python_callable=stream_data
    )