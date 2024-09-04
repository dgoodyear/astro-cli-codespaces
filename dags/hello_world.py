from airflow import DAG
from datetime import datetime
from airflow.operators.python import PythonOperator
from airflow.operators.bash import BashOperator
from airflow.providers.common.sql.operators.sql import SQLExecuteQueryOperator
from airflow.providers.postgres.hooks.postgres import PostgresHook
from airflow.models import Variable
from airflow.providers.common.sql.sensors.sql import SqlSensor 

#import PostgresHook

def print_message():
    print("Hello, Airflow!")

def print_date(data_interval_start=None, data_interval_end=None):
    print(f"Current date is: {datetime.now().date()}")
    print(f"DAG start date is: {data_interval_start} - {data_interval_end}")

def get_db_connection(ti=None):
    schema = Variable.get("schema") # Created via Airflow UI Admin->Variables
    pg_hook = PostgresHook(postgres_conn_id="my_postgress_db")
    results = pg_hook.get_first("SELECT conn_id FROM {schema}.connection;")
    first_rec = results[0]
    print(first_rec)
    print(f"Results: {results}") 
    ti.xcom_push(key='first_rec_id', value=first_rec)

def print_connection_id(ti=None):
    connect_id = ti.xcom_pull(key='first_rec_id', task_ids='task_print_connection')
    print(connect_id)

 


with DAG (
    "hello_world", # Required
    start_date=datetime(2024,1,1), # Required

    #schedule="0/5 * * * *", # Used Cron Notation
    #schedule=timedelta(hours=1), 
    #schedule="@continuous", 
    #schedule="@hourly", # Presets but get interpreted as cron 0 * * * * 
    schedule="@daily",

    catchup=False # Required - Should it run the Dag for the intervals that were missed
) as dag:
    
    wait_for_connection = SqlSensor(
        task_id = "wait_for_connection",
        conn_id = "my_postgress_db",
        sql = "Select count(1) from connection where conn_id = 'my_postgress_db';",
        poke_interval=60,
        timeout=3600
    
    )


    task_hello = PythonOperator (
        task_id="task_hello",  # Required in all operators
        python_callable=print_message
    )

    task_date = PythonOperator (
        task_id="task_date",  # Required in all operators
        python_callable=print_date
    )

    task_dir = BashOperator (
        task_id="task_dir", 
        bash_command="pwd"
    )

    task_sql = SQLExecuteQueryOperator (
        task_id="task_sql",
        conn_id="my_postgress_db",
        sql="SELECT conn_id FROM connection;"        
    )

    task_sql_hook = PythonOperator (
        task_id="task_sql_hook",  # Required in all operators
        python_callable=get_db_connection
    )


    task_print_connection = PythonOperator (
        task_id="task_print_connection",  # Required in all operators
        python_callable=get_db_connection
    )




    # dependencies
    wait_for_connection >> task_hello >> task_date >> task_dir 
    task_dir >> task_sql >> task_sql_hook >> task_print_connection