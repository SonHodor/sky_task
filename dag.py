from datetime import datetime, timedelta
from airflow import DAG
from airflow.operators.bash_operator import BashOperator
from airflow.operators.python_operator import PythonOperator

default_args = {
    'owner': 'some_user',
    'depends_on_past': False,
    'start_date': datetime(2023, 5, 2),
    'retries': 1,
    'retry_delay': timedelta(minutes=5),
}

dag = DAG(
    'increment_dag', 
    default_args=default_args, 
    schedule_interval=timedelta(days=1))

def run_pyspark_job(**kwargs):
    from main import main
    main(kwargs['PG_WAREHOUSE_HOST'], kwargs['PG_WAREHOUSE_PORT'], kwargs['PG_WAREHOUSE_DBNAME'],
         kwargs['PG_WAREHOUSE_USER'], kwargs['PG_WAREHOUSE_PASSWORD'], kwargs['TABLE_STREAM_MODULE_LESSON'],
         kwargs['TABLE_STREAM_MODULE'], kwargs['TABLE_COURSE'], kwargs['TABLE_STREAM'])


task = PythonOperator(
    task_id='increment',
    python_callable=run_pyspark_job,
    op_kwargs={
        'PG_WAREHOUSE_HOST': "host",
        'PG_WAREHOUSE_PORT': 5432,
        'PG_WAREHOUSE_DBNAME': "db_name",
        'PG_WAREHOUSE_USER': "user_name",
        'PG_WAREHOUSE_PASSWORD': "password",
        'TABLE_STREAM_MODULE_LESSON': "stream_module_lesson",
        'TABLE_STREAM_MODULE': "stream_module",
        'TABLE_COURSE': "course",
        'TABLE_STREAM': "stream"
    },
    dag=dag
)

task
