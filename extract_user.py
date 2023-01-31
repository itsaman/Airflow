import json
from airflow import DAG
from datetime import datetime
from airflow.providers.postgres.operators.postgres import PostgresOperator
from airflow.providers.http.sensors.http import HttpSensor
from airflow.providers.http.operators.http import SimpleHttpOperator
from airflow.operators.python import PythonOperator
from airflow.providers.postgres.hooks.postgres import PostgresHook
from pandas import json_normalize


def _process_user(ti): #here ti is task instance
    user = ti.xcom_pull(task_ids = "extract_data")
    user = user['results'][0]
    processed_user = json_normalize({
        'first_name' : user['name']['first'],
        'last_name' : user['name']['last'],
        'country' : user['location']['country'],
        'username' : user['login']['username'],
        'email' : user['email']
        })

    processed_user.to_csv('processed_user.csv', index=None, header= False)


def _store_user():
    hook = PostgresHook(postgres_conn_id = 'postgres')
    hook.copy_expert(
        sql = "COPY users from stdin WITH DELIMITER AS ','", filename= 'processed_user.csv'
    )


with DAG('user_processing', start_date = datetime(2023,1,27), schedule_interval = '@daily', catchup = False) as dag:
    
    create_table = PostgresOperator(
        task_id = 'create_table', #to provide unique name, the task id must be unique across all tasks in the same DAG
        postgres_conn_id = 'postgres', #get connection 
        sql = ''' 
                CREATE TABLE IF NOT EXISTS users(
                first_name TEXT NOT NULL,
                last_name TEXT NOT NULL,
                country TEXT NOT NULL,
                username TEXT NOT NULL,
                email TEXT NOT NULL 
                );     
            '''
        )
    
    #to check is the URL is active or not 
    
    is_api_available = HttpSensor(
        task_id = 'is_api_available',
        http_conn_id= 'user_api',
        endpoint = 'api/'  #(?)
    )

    #now to extract data from URL 

    extract_data = SimpleHttpOperator(
        task_id = 'extract_data',
        http_conn_id= 'user_api',
        endpoint= 'api/',
        method= 'GET',
        response_filter = lambda response: json.loads(response.text),
        log_response= True
    )

    #to execute python functions 
    process_user = PythonOperator(
        task_id = 'process_user',
        python_callable= _process_user
    )

    #to store user data into postgres table 

    store_user = PythonOperator(
        task_id = 'store_user',
        python_callable= _store_user
    )

    #Creating dependencies

    create_table >> is_api_available >> extract_data >> process_user >> store_user
