from airflow import DAG
from datetime import datetime 
from airflow.operators.postgres_operator import PostgresOperator

with DAG(
          dag_id='etl_dag',
          start_date=datetime(2022, 11, 26),
          schedule_interval='@once',
          catchup=False
) as dag:

          extract_deparptment_data = PostgresOperator(
                    task_id = 'extract_department_data',
                    postgres_conn_id = 'postgres_db',
                    sql = 'sql/extract_dep.sql'
          )

          extract_employee_data = PostgresOperator(
                    task_id = 'extract_employee_data',
                    postgres_conn_id = 'postgres_db',
                    sql = 'sql/extract_emp.sql'
          )

          extract_worklog_data = PostgresOperator(
                    task_id = 'extract_worklog_data',
                    postgres_conn_id = 'postgres_db',
                    sql = 'sql/extract_wkl.sql'
          )