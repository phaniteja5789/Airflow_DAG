from airflow.models import DAG
from airflow.providers.sqlite.operators.sqlite import SqliteOperator
from airflow.providers.http.sensors.http import HttpSensor
from airflow.providers.http.operators.http import SimpleHttpOperator
from airflow.operators.python import PythonOperator
from airflow.operators.bash_operator import BashOperator
from datetime import datetime,timedelta
import json
import pandas as pd


def process_user_details(**kwargs):
    user_details=kwargs['ti'].xcom_pull(task_ids=['extract_details'])
    if len(user_details)==0:
        print("user details is empty")
    else:
        user=user_details[0]['results'][0]
        user_details_csv=pd.json_normalize(user)
        user_dict={}
        user_dict['firstname']=user_details_csv['name.first']
        user_dict['lastname']=user_details_csv['name.last']
        user_dict['country']=user_details_csv['location.country']
        user_dict['username']=user_details_csv['login.username']
        user_dict['password']=user_details_csv['login.password']
        user_dict['email']=user_details_csv['email']
        user_df=pd.DataFrame(user_dict)
        user_df.to_csv('./user_details.csv',index=None,header=False)


default_args={
    'start_date':datetime(2021,10,17)
}

dag_object=DAG(
    'storing_user_to_table',
    schedule_interval='@yearly',
    default_args=default_args,
    catchup=False
)

creating_table=SqliteOperator(
    task_id='table_creation',
    sqlite_conn_id='db_sqlite',
    sql='''create table Customers(
        firstname TEXT NOT NULL,
        lastname TEXT NOT NULL,
        country TEXT NOT NULL,
        username TEXT NOT NULL,
        password TEXT NOT NULL,
        email TEXT NOT NULL PRIMARY KEY

    );
    ''',
    dag=dag_object
)

checking_url=HttpSensor(
    task_id='url_check',
    http_conn_id='user_api_check',
    endpoint='api/',
    dag=dag_object
)

extracting_details=SimpleHttpOperator(
    task_id='extract_details',
    http_conn_id='user_api_check',
    endpoint='api/',
    method='GET',
    response_filter=lambda response:json.loads(response.text),
    log_response=True,
    dag=dag_object
)

processing_user=PythonOperator(
    task_id='user_process_details',
    python_callable=process_user_details,
    provide_context=True,
    dag=dag_object
)


storing_user_details=BashOperator(
    task_id='user_storage',
    dag=dag_object,
    bash_command=
        'echo -e ".separator ","\n.import /home/airflow/user_details.csv Customers" | sqlite3 /home/airflow/airflow/airflow.db'
)

creating_table >> checking_url >> extracting_details >> processing_user>>storing_user_details