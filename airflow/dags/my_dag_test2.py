from airflow import DAG
from datetime import datetime 
from airflow.operators.python_operator import PythonOperator 
import clickhouse_connect
import sqlalchemy
import pandas as pd

def extract_data(table_name):
          conn_string = 'postgresql://admin:admin@host.docker.internal:5435/platform_db'
          engine = sqlalchemy.create_engine(conn_string) 
          con = engine.connect()
          sql = f'SELECT * FROM public."{table_name}"'
          df = pd.read_sql(sql, con=con)
          df_data = df.values.tolist()
          return df_data    

def load_dep_data(table_name, ti):
          data = ti.xcom_pull(task_ids='extract_department')
          client = clickhouse_connect.get_client(host='host.docker.internal', username='default', password='default')
          client.command(f'TRUNCATE {table_name}')
          client.insert(table_name, data) 
          return ('Successfully Added!')

with DAG(
          dag_id='etl_dag_test1',
          start_date=datetime(2022, 11, 27),
          schedule_interval='@once',
          catchup=False
) as dag:

          extract_department = PythonOperator(
                    task_id = 'extract_department',
                    python_callable=extract_data,
                    op_kwargs={'table_name':'EmployeeApp_departments'} 

          )

          load_department = PythonOperator(
                   task_id = 'load_department',
                   python_callable=load_dep_data,
                   op_kwargs={'table_name':'clch_db.departments'} 
          )

          extract_department >> load_department