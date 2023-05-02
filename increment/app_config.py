from datetime import datetime, timedelta
from airflow import DAG
from airflow.operators.bash_operator import BashOperator
from airflow.contrib.operators.spark_submit_operator import SparkSubmitOperator


default_args = {
    'owner': 'some_user',
    'start_date': datetime(2023, 5, 2),
    'retries': 1,
    'retry_delay': timedelta(minutes=5)
}

dag = DAG(
    'increment_dag',
    default_args=default_args,
    schedule_interval=timedelta(days=7),
    catchup=False
)

# Define the parameters for the PySpark job
PG_WAREHOUSE_HOST = "some_host"
PG_WAREHOUSE_PORT = "some_port"
PG_WAREHOUSE_DBNAME = "some_dbname"
PG_WAREHOUSE_USER = "some_user"
PG_WAREHOUSE_PASSWORD = "some_password"

TABLE_STREAM_MODULE_LESSON = "stream_module_lesson_table"
TABLE_STREAM_MODULE = "stream_module_table"
TABLE_COURSE = "course_table"
TABLE_STREAM = "stream_table"

# Define the PySpark job command
spark_submit_command = (
    "spark-submit --master yarn" +
    "--deploy-mode client " +
    "--executor-memory 4g " +
    "--num-executors 4 " +
    "main.py " + 
    "--pg_warehouse_host " + PG_WAREHOUSE_HOST + 
    " --pg_warehouse_port " + PG_WAREHOUSE_PORT + 
    " --pg_warehouse_dbname " + PG_WAREHOUSE_DBNAME + 
    " --pg_warehouse_user " + PG_WAREHOUSE_USER + 
    " --pg_warehouse_password " + PG_WAREHOUSE_PASSWORD + 
    " --table_stream_module_lesson " + TABLE_STREAM_MODULE_LESSON + 
    " --table_stream_module " + TABLE_STREAM_MODULE + 
    " --table_course " + TABLE_COURSE + 
    " --table_stream " + TABLE_STREAM)

# Submit the PySpark job
increment_job = SparkSubmitOperator(
    task_id='increment_job',
    application='main.py',
    name='increment_job',
    conn_id='spark_pg',
    verbose=False,
    application_args=[spark_submit_command],
    dag=dag
)

increment_job
