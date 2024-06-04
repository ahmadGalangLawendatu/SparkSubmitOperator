import airflow
from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.providers.apache.spark.operators.spark_submit import SparkSubmitOperator
from datetime import datetime

dag = DAG(
    dag_id="spark_airflow",
    default_args={
        "owner": "galang lawendatu",
        "start_date": airflow.utils.dates.days_ago(1)
    },
    schedule_interval="0 1 * * *"
)

def start_job():
    print("Jobs Started")

def end_job():
    print("Jobs Completed")

start = PythonOperator(
    task_id='start',
    python_callable=start_job,
    dag=dag
)

helloworld = SparkSubmitOperator(
    task_id='helloworld',
    conn_id="spark-conn",
    application="jobs/python/helloword.py",
    dag=dag
)

wordcount = SparkSubmitOperator(
    task_id='wordcount',
    conn_id="spark-conn",
    application="jobs/python/wordcount.py",
    dag=dag
)

wordcountbook = SparkSubmitOperator(
    task_id='wordcountbook',
    conn_id="spark-conn",
    application="jobs/python/wordcountbook.py",
    dag=dag
)

end = PythonOperator(
    task_id='end',
    python_callable=end_job,
    dag=dag
)

start >> [helloworld, wordcount, wordcountbook] >> end
