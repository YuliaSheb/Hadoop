import os
import pandas as pd
from airflow import DAG
from airflow.operators.python import PythonOperator
from datetime import datetime, timedelta
from airflow.providers.postgres.hooks.postgres import PostgresHook
from airflow.operators.trigger_dagrun import TriggerDagRunOperator

default_args = {
        'owner': 'airflow',
        'start_date': datetime(2025, 3, 20),
        'retries': 3,
        'retry_delay': timedelta(minutes=3)
}

dag = DAG('extract_tech',
        schedule="@daily",
        start_date=datetime(2025, 3, 21, 0),
        catchup=False
)

POSTGRES_CONN_ID = 'wave20_postgres_conn'
CSV_PATH = '/opt/airflow/dags/output/etl_pg_gp_tech.csv'


def export_to_csv():
        pg_hook = PostgresHook(postgres_conn_id=POSTGRES_CONN_ID)
        conn = pg_hook.get_conn()
        sql_query = "SELECT * FROM shebeko_tech.etl_pg_gp_tech WHERE is_active = TRUE;"

        df = pd.read_sql(sql_query, conn)

        os.makedirs(os.path.dirname(CSV_PATH), exist_ok=True)
        df.to_csv(CSV_PATH, index=False)

        return CSV_PATH

def trigger_dag(**kwargs):
        context = kwargs
        df = pd.read_csv(CSV_PATH)

        for _, row in df.iterrows():
                conf = row.to_dict()

                trigger = TriggerDagRunOperator(
                        task_id=f"trigger_load_{row['schema_name']}_{row['table_name']}",
                        trigger_dag_id="process_etl_data",
                        conf=conf,
                        dag=context['dag'],
                )
                trigger.execute(context)

export_task = PythonOperator(
        task_id='extract_and_save_csv',
        python_callable=export_to_csv,
        dag=dag
)

trigger_task = PythonOperator(
        task_id="trigger_load_data_to_gp",
        python_callable=trigger_dag,    
        dag=dag
)

export_task >> trigger_task