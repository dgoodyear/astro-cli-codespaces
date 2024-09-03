from airflow import DAG
from datetime import datetime
from airflow.operators.python import PythonOperator
from airflow.operators.bash import BashOperator


def print_message():
    print("Hello, Airflow!")

def print_dir():
    return "pwd"

def print_date(data_interval_start=None, data_interval_end=None):
    print(f"Current date is: {datetime.now().date()}")
    print(f"DAG start date is: {data_interval_start} - {data_interval_end}")

with DAG (
    "hello_world", # Required
    start_date=datetime(2024,1,1), # Required

    #schedule="0/5 * * * *", # Used Cron Notation
    #schedule=timedelta(hours=1), 
    #schedule="@continuous", 
    #schedule="@hourly", # Presets but get interpreted as cron 0 * * * * 
    
    catchup=False # Required - Should it run the Dag for the intervals that were missed
) as dag:
    
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
        #bash_command=print_dir
    )

   # dependencies
task_hello >> task_date >> task_dir