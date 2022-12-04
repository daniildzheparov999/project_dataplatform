from airflow import DAG
from datetime import datetime 
from airflow.operators.postgres_operator import PostgresOperator
from airflow.operators.python_operator import PythonOperator 
import clickhouse_connect

def clch_insert(table_name, df):
          client = clickhouse_connect.get_client(host='localhost', username='admin1', password='admin1')
          

with DAG(
          dag_id='etl_dag_test',
          start_date=datetime(2022, 11, 27),
          schedule_interval='@once',
          catchup=False
) as dag:

          create_table_department = PostgresOperator(
                    task_id = 'create_table_department',
                    postgres_conn_id = 'postgres_db',
                    sql = 'sql/create_tbl_dep.sql'
          )

          insert_department = PostgresOperator(
                    task_id = 'insert_department',
                    postgres_conn_id = 'postgres_db',
                    sql = 'sql/insert_dep.sql'
          )

          create_table_department >> insert_department