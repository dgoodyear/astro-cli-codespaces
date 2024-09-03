from airflow import DAG
from datetime import datetime
from airflow.operators.python import PythonOperator
from airflow.operators.bash import BashOperator


def print_message():
    print("Hello, Airflow!")

def print_dir():
    return "pwd"

def print_date():
    print(f"Current date is: {datetime.now().date()}")

with DAG (
    "hello_world", # Required
    start_date=datetime(2024,1,1), # Required
    catchup=False # Required
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