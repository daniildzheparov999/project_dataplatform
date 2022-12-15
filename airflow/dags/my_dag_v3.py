from airflow import DAG
from datetime import datetime 
from airflow.operators.python_operator import PythonOperator 
from airflow.operators.empty import EmptyOperator


def extract_data(table_name):
          import sqlalchemy
          import pandas as pd
          conn_string = 'postgresql://admin:admin@host.docker.internal:5435/platform_db'
          engine = sqlalchemy.create_engine(conn_string) 
          con = engine.connect()
          sql = f'SELECT * FROM public."{table_name}"'
          df = pd.read_sql(sql, con=con)
          df_data = df.values.tolist()
          return df_data    

def load_dep_data(table_name):
          import clickhouse_connect
          data = extract_data('EmployeeApp_departments')
          client = clickhouse_connect.get_client(host='host.docker.internal', username='default')
          client.command(f'TRUNCATE {table_name}')
          client.insert(table_name, data) 
          return ('Successfully Added!')

def load_emp_data(table_name):
          import clickhouse_connect
          data = extract_data('EmployeeApp_employees')
          client = clickhouse_connect.get_client(host='host.docker.internal', username='default')
          client.command(f'TRUNCATE {table_name}')
          client.insert(table_name, data) 
          return ('Successfully Added!')

def load_wkl_data(table_name):
          import clickhouse_connect
          data = extract_data('EmployeeApp_worklogs')
          client = clickhouse_connect.get_client(host='host.docker.internal', username='default')
          client.command(f'TRUNCATE {table_name}')
          client.insert(table_name, data) 
          return ('Successfully Added!')


with DAG(
          dag_id='etl_dag_v3',
          start_date=datetime(2022, 11, 27),
          schedule_interval='@once',
          catchup=False
) as dag:

          load_department = PythonOperator(
                   task_id = 'load_department',
                   python_callable=load_dep_data,
                   op_kwargs={'table_name':'clch_db.departments'} 
          )


          load_employee = PythonOperator(
                   task_id = 'load_employee',
                   python_callable=load_emp_data,
                   op_kwargs={'table_name':'clch_db.employees'} 
          )


          load_worklog = PythonOperator(
                   task_id = 'load_worklog',
                   python_callable=load_wkl_data,
                   op_kwargs={'table_name':'clch_db.worklogs'} 
          )

          sync1 = EmptyOperator(
                    task_id = 'sync_start'
          )

          sync2 = EmptyOperator(
                    task_id='sync_end'
          )


          sync1 >> load_department >> sync2
          sync1 >> load_employee >> sync2
          sync1 >> load_worklog >> sync2