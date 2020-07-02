from datetime import timedelta
from airflow import DAG
from airflow.operators.python_operator import PythonOperator
from airflow.operators.postgres_operator import PostgresOperator
from airflow.utils.dates import days_ago
import sys
import os
#adding to path directory above
PACKAGE_PARENT = '..'
SCRIPT_DIR = os.path.dirname(os.path.realpath(os.path.join(os.getcwd(), os.path.expanduser(__file__))))
sys.path.append(os.path.normpath(os.path.join(SCRIPT_DIR, PACKAGE_PARENT)))

from python_callables.import_files_into_postgres import main, dim_table, run_sql


args = {
    'owner': 'airflow',
    'start_date': days_ago(0)}


dag = DAG(
    dag_id='taxi_dag',
    default_args=args,
    schedule_interval='0 0 * * *',
    dagrun_timeout=timedelta(minutes=60),
    template_searchpath='/usr/local/airflow/sql',
    tags=['example']
)

importing_files_from_S3= PythonOperator(dag=dag,
                            task_id='importing_files_from_S3',
                            provide_context=False,
                            python_callable=main)

importing_zones_from_S3=PythonOperator(dag=dag,
                            task_id='importing_zones_from_S3',
                            provide_context=False,
                            python_callable=dim_table)

# popular_destinations=PythonOperator(dag=dag,
#                             task_id='popular_destinations',
#                             provide_context=False,
#                             python_callable=run_sql,
#                             op_kwargs={'file_path': './sql/DML/popular_destinations_monthly.sql'})

postgres_conn_id='postgres_airflow'

create_stg_taxis_data= PostgresOperator(
    dag=dag,
    postgres_conn_id=postgres_conn_id,
    task_id='create_stg_taxis_data',
    sql='DDL/create_stg_taxis_data.sql'
    )

staging_taxis_data= PostgresOperator(
    dag=dag,
    postgres_conn_id=postgres_conn_id,
    task_id='staging_taxis_data',
    sql='DML/staging_taxis_data.sql'
    )

create_table_popular_destination_zones= PostgresOperator(
    dag=dag,
    postgres_conn_id=postgres_conn_id,
    task_id='create_table_popular_destination_zones',
    sql='DDL/create_table_popular_destination_zones.sql'
    )

popular_destinations_zones_passengers= PostgresOperator(
    dag=dag,
    postgres_conn_id=postgres_conn_id,
    task_id='popular_destinations_zones_passengers',
    sql='DML/popular_destinations_zones_passengers.sql'
    )

create_table_popular_destination_borough_rides= PostgresOperator(
    dag=dag,
    postgres_conn_id=postgres_conn_id,
    task_id='create_table_popular_destination_borough_rides',
    sql='DDL/create_table_popular_destination_borough_rides.sql'
    )

popular_destinations_borough_rides= PostgresOperator(
    dag=dag,
    postgres_conn_id=postgres_conn_id,
    task_id='popular_destinations_borough_rides',
    sql='DML/popular_destinations_borough_rides.sql'
    )

history_table_borough_rides= PostgresOperator(
    dag=dag,
    postgres_conn_id=postgres_conn_id,
    task_id='history_table_borough_rides',
    sql='DML/history_table_borough_rides.sql'
    )

history_table_zones_passengers= PostgresOperator(
    dag=dag,
    postgres_conn_id=postgres_conn_id,
    task_id='history_table_zones_passengers',
    sql='DML/history_table_zones_passengers.sql'
    )

importing_zones_from_S3 >> importing_files_from_S3 >> create_stg_taxis_data >> staging_taxis_data

staging_taxis_data >> create_table_popular_destination_zones >> popular_destinations_zones_passengers
staging_taxis_data >> create_table_popular_destination_borough_rides >> popular_destinations_borough_rides

popular_destinations_zones_passengers >> history_table_zones_passengers
popular_destinations_borough_rides >> history_table_borough_rides

if __name__ == "__main__":
    dag.cli()