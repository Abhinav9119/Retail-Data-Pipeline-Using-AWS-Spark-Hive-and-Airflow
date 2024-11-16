from airflow import DAG
from airflow.operators.python_operator import PythonOperator
from datetime import datetime, timedelta
import subprocess

# Define the Python function that will run your main.py script
def run_main_script():
    # Adjust the path to where your main.py is located
    subprocess.run(["python", "/path/to/your/main.py"])

# Default arguments for the DAG
default_args = {
    'owner': 'airflow',
    'depends_on_past': False,
    'start_date': datetime(2024, 11, 16, 10, 0, 0),  # Start date is 10 AM today
    'retries': 1,
    'retry_delay': timedelta(minutes=5),
}

# Define the DAG
dag = DAG(
    'run_main_script_dag',
    default_args=default_args,
    description='A simple DAG to run main.py every day at 10 AM',
    schedule_interval='0 10 * * *',  # Runs at 10 AM every day
    catchup=False,  # Don't run past instances
)

# Define the task that runs the main.py script
run_script_task = PythonOperator(
    task_id='run_main_script',
    python_callable=run_main_script,
    dag=dag,
)

# Set the task dependencies (if you have multiple tasks)
run_script_task
