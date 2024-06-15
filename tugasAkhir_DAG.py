from airflow import DAG
from airflow.providers.apache.spark.operators.spark_submit import SparkSubmitOperator
from airflow.utils.dates import days_ago

default_args = {
    'owner': 'airflow',
    'depends_on_past': False,
    'start_date': days_ago(1),
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 1,
}

dag = DAG(
    'pyspark_transformations_dag',
    default_args=default_args,
    description='A DAG to run multiple PySpark transformation scripts',
    schedule_interval='@daily'
)

jdbc_driver_path = '/home/hansel/postgres/postgresql-42.7.3.jar'

transform_customer = SparkSubmitOperator(
    task_id='transform_customer',
    application='/home/hansel/airflow/dags/olap_customer.py',
    conn_id='spark_default',
    jars=jdbc_driver_path,
    dag=dag,
)

transform_product = SparkSubmitOperator(
    task_id='transformation_product',
    application='/home/hansel/airflow/dags/olap_product.py',
    conn_id='spark_default',
    jars=jdbc_driver_path,
    dag=dag
)

transform_order= SparkSubmitOperator(
    task_id='transform_order',
    application='/home/hansel/airflow/dags/olap_order.py',
    conn_id='spark_default',
    jars=jdbc_driver_path,
    dag=dag
)

transform_customer >> transform_product >> transform_order