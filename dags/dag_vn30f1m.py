from datetime import timedelta, datetime
from airflow import DAG
from src.etl_vn30f1m import etl
from airflow.operators.dummy_operator import DummyOperator
from airflow.operators.python_operator import PythonOperator

# initializing the default arguments
default_args = {
		'owner': 'tuannm101',
		'start_date': datetime(2024, 5, 2),
		'retries': 0
}

# Instantiate a DAG object
vn30f1m_dag = DAG('vn30f1m',
		default_args=default_args,
		description='vn301fm',
		schedule_interval=timedelta(minutes=1), 
		catchup=False,
		tags=['vn301fm']
)
# Creating start task
start_task = DummyOperator(task_id='start_task', dag=vn30f1m_dag)

# Creating etl task
etl_vn30f1m_task = PythonOperator(task_id='etl_vn30f1m_task', python_callable=etl, dag=vn30f1m_dag)

# Creating end task
end_task = DummyOperator(task_id='end_task', dag=vn30f1m_dag)

# Set the order of execution of tasks. 
start_task >> etl_vn30f1m_task >> end_task


