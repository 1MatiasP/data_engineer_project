import pandas as pd
from airflow import DAG
from airflow.operators.python_operator import PythonOperator
from datetime import datetime, timedelta

# Importar las funciones del script modificado
from Etl_Crypto import get_crypto_data, create_redshift_connection, create_crypto_table, insert_data_into_redshift

# Definir los argumentos predeterminados del DAG
default_args = {
    'owner': 'matiaspereyra',
    'depends_on_past': False,
    'start_date': datetime(2024, 5, 19),
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 3,
    'retry_delay': timedelta(minutes=5),
}

# Definir el DAG
dag = DAG('etl_crypto', default_args=default_args, schedule_interval='@daily')

# Tareas
get_crypto_data_task = PythonOperator(
    task_id='get_crypto_data',
    python_callable=get_crypto_data,
    dag=dag,
    do_xcom_push=False  # Evitar la serialización del DataFrame como XCom
)


create_redshift_connection_task = PythonOperator(
    task_id='create_redshift_connection',
    python_callable=create_redshift_connection,
    dag=dag,
    do_xcom_push=False  # Evitar la serialización del objeto de conexión como XCom
)

create_crypto_table_task = PythonOperator(
    task_id='create_crypto_table',
    python_callable=create_crypto_table,
    dag=dag,
)

insert_data_into_redshift_task = PythonOperator(
    task_id='insert_data_into_redshift',
    python_callable=insert_data_into_redshift,
    op_kwargs={'conn': create_redshift_connection(), 'df': get_crypto_data()},
    dag=dag,
)



get_crypto_data_task >> create_redshift_connection_task >> create_crypto_table_task >> insert_data_into_redshift_task


