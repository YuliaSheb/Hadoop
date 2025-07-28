import subprocess
from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.providers.postgres.hooks.postgres import PostgresHook
import pandas as pd
import os
from datetime import datetime, timedelta
import logging
from airflow.operators.email import EmailOperator


log = logging.getLogger(__name__)


POSTGRES_CONN_ID = 'wave20_postgres_conn'
GREENPLUM_CONN_ID = 'wave20_greenplum_conn'
CSV_PATH = '/opt/airflow/dags/output/{table_name}_data.csv'


def send_notification(subject, message, recipient_list):
    return EmailOperator(
        task_id='send_email',
        to=recipient_list,
        subject=subject,
        html_content=message,
    )

def send_notification_on_success(context):
    subject = f"Задача {context['task_instance'].task_id} завершена успешно"
    message = f"Задача {context['task_instance'].task_id} была выполнена успешно в {context['execution_date']}"
    send_notification(subject, message, ['***']).execute(context=context)

def send_notification_on_failure(context):
    subject = f"Задача {context['task_instance'].task_id} завершена с ошибкой"
    message = f"Задача {context['task_instance'].task_id} завершена с ошибкой в {context['execution_date']}\n\nОшибка:\n{context['exception']}"
    send_notification(subject, message, ['***']).execute(context=context)


def check_connection():
    try:
        pg_hook = PostgresHook(postgres_conn_id=POSTGRES_CONN_ID)
        gp_hook = PostgresHook(postgres_conn_id=GREENPLUM_CONN_ID)

        pg_hook.get_first("SELECT 1")  
        gp_hook.get_first("SELECT 1")  

        log.info("Подключения успешны")
    except Exception as e:
        log.error(f"Ошибка подключения: {e}")
        raise


def table_data(schema_src, table_src, schema_tgt, table_tgt):
    log.info(f"Обработка таблицы: {schema_src}.{table_src} -> {schema_tgt}.{table_tgt}")


def clear_gp_table(schema_tgt, table_tgt, delta_value):
    if delta_value == 0: 
        gp_hook = PostgresHook(postgres_conn_id=GREENPLUM_CONN_ID)
        gp_hook.run(f"TRUNCATE TABLE {schema_tgt}.{table_tgt};")


def extract_and_load(schema_src, table_src, schema_tgt, table_tgt, delta_col, delta_value, delta_size):
        pg_hook = PostgresHook(postgres_conn_id=POSTGRES_CONN_ID)
        gp_hook = PostgresHook(postgres_conn_id=GREENPLUM_CONN_ID)
        pg_conn = pg_hook.get_conn()

        while True:
                extract_query = f"""
                SELECT * FROM {schema_src}.{table_src}
                WHERE ({delta_col}>{delta_value}) AND ({delta_col} <= {delta_value} + {delta_size})
                ORDER BY {delta_col};
                """

                df = pd.read_sql(extract_query, pg_conn)
                if df.empty:
                        break

                table_csv_path = CSV_PATH.format(table_name=table_tgt)
                df.to_csv(table_csv_path, index=False)

                cmd = f"""psql -h '****' -U '****' -d '****' -c \"\copy {schema_tgt}.{table_tgt} 
                        FROM '{table_csv_path}' DELIMITER ',' CSV HEADER;\""""
                subprocess.run(cmd, shell=True, check=True)

                max_delta_query = f"SELECT MAX({delta_col}) FROM {schema_tgt}.{table_tgt};"
                max_delta_value = gp_hook.get_first(max_delta_query)[0]

                if max_delta_value:
                        update_query = f"""
                        UPDATE shebeko_tech.etl_pg_gp_tech
                        SET delta_clmn_value = {max_delta_value}
                        WHERE schema_name = '{schema_src}' AND table_name = '{table_src}' AND schema_name_target = '{schema_tgt}' AND table_name_target = '{table_tgt}';
                        """

                        pg_hook.run(update_query)
                        delta_value = max_delta_value

                os.remove(table_csv_path)

default_args = {
        'owner': 'airflow',
        'start_date': datetime(2025, 3, 20),
        'retries': 3,
        'retry_delay': timedelta(minutes=3),
        'email_on_failure': True,  
        'email_on_retry': False,   
        'email_on_success': True,  
        'email': ['*****'],  
}

dag = DAG('process_etl_data',
        schedule=None,
        start_date=datetime(2025, 3, 21, 0),
        catchup=False
)

check = PythonOperator(
        task_id='check_connection',
        python_callable=check_connection,
        on_failure_callback=send_notification_on_failure,
        on_success_callback=send_notification_on_success,
        dag=dag
)

data = PythonOperator(
        task_id='table_data',
        python_callable=table_data,
        op_kwargs={'schema_src': '{{dag_run.conf["schema_name"] }}', 'table_src': '{{ dag_run.conf["table_name"]}}','schema_tgt': '{{dag_run.conf["schema_name_target"] }}', 'table_tgt': '{{ dag_run.conf["table_name_target"]}}'},
        on_failure_callback=send_notification_on_failure,
        on_success_callback=send_notification_on_success,
        dag=dag
)

clear_gp = PythonOperator(
        task_id='clear_gp_table',
        python_callable=clear_gp_table,
        op_kwargs={'delta_value': '{{dag_run.conf["delta_clmn_value"] }}', 'schema_tgt': '{{dag_run.conf["schema_name_target"] }}', 'table_tgt': '{{ dag_run.conf["table_name_target"]}}'},
        on_failure_callback=send_notification_on_failure,
        on_success_callback=send_notification_on_success,
        dag=dag
)

extract_load = PythonOperator(
        task_id='extract_and_load',
        python_callable=extract_and_load,
        on_failure_callback=send_notification_on_failure,
        on_success_callback=send_notification_on_success,
        dag=dag,
        op_kwargs={
                'schema_src': '{{dag_run.conf["schema_name"]}}',
                'table_src': '{{dag_run.conf["table_name"]}}',
                'schema_tgt': '{{dag_run.conf["schema_name_target"]}}',
                'table_tgt': '{{dag_run.conf["table_name_target"]}}',
                'delta_col': '{{dag_run.conf["delta_clmn"]}}',
                'delta_value': '{{dag_run.conf["delta_clmn_value"]}}',
                'delta_size': '{{dag_run.conf["delta_size"]}}'
        }
)

check >> data >> clear_gp >> extract_load