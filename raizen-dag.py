from airflow import DAG
from datetime import datetime, timedelta
from airflow.operators.python import PythonOperator, BaschPythonOperator
from airflow.operators.bash import BaschOperator
import pandas as pd

one_day_ago = datetime.combine(datetime.today() - timedelta(1),
                                      datetime.min.time())


default_args = {
        'owner': 'airflow',
        'depends_on_past': False,
        'start_date': one_day_ago,
        'email': ['clarissasouza950@gmail.com'],
        'email_on_failure': False,
        'email_on_retry': False,
        'retries': 1,
        'retry_delay': timedelta(minutes=1)
      }


dag = DAG('desafio-raizen', default_args=default_args)
t1 = BashOperator( 
      task_id='desafio-raizen', 
      bash_command='desafio_raizen.py', 
      dag=dag
      )
  
t1