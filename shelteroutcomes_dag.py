
import os
import sys
import json
from airflow import DAG
from datetime import datetime, timedelta
from airflow.operators.bash import BashOperator
from airflow.operators.python_operator import PythonOperator


# code_path = "/root/demo/lab03/etl_scripts"
# sys.path.insert(0, code_path)

from ExtractTransformLoad_Steps.TransformData import transform_data
from ExtractTransformLoad_Steps.ExtractAPItoGCP import main
from ExtractTransformLoad_Steps.LoadData import load_data_to_postgres

# AIRFLOW_HOME = os.environ.get('AIRFLOW_HOME', '/opt/airflow')
# CREDS_TARGET_DIR = AIRFLOW_HOME + '/warm-physics-405522-a07e9b7bfc0d.json'

# with open(CREDS_TARGET_DIR, 'r') as f:
#     credentials_content = f.read()


default_args = {
    "owner": santosh.adabala",
    "depends_on_past": False,
    "start_date": datetime(2023, 11, 1),
    "retries": 1,
    "retry_delay": timedelta(seconds=5)
}


with DAG(
    dag_id="outcomes_dag",
    default_args=default_args,
    schedule_interval='@daily',
    catchup=False,
) as dag:
        start = BashOperator(task_id = "START",
                             bash_command = "echo start")

        # copy_creds = BashOperator(task_id = "COPY_CREDS", bash_command = "echo start")

        extract_data_from_api_to_gcp =  PythonOperator(task_id = "EXTRACT_DATA_FROM_API_TO_GCP",
                                                  python_callable = main,)

        transform_data_from_gcp_step = PythonOperator(task_id="TRANSFORM_DATA_FROM_GCP",
                                              python_callable=transform_data,)

        load_dim_animals_tab = PythonOperator(task_id="LOAD_DIM_ANIMALS",
                                            python_callable=load_data_to_postgres,
                                             op_kwargs={"file_name": 'dim_animal.csv', "table_name": 'animaldimension'},)

        load_dim_outcome_types_tab = PythonOperator(task_id="LOAD_DIM_OUTCOME_TYPES",
                                              python_callable=load_data_to_postgres,
                                              op_kwargs={"file_name": 'dim_outcome_types.csv', "table_name": 'outcomedimension'},)

        load_dim_dates_tab = PythonOperator(task_id="LOAD_DIM_DATES",
                                             python_callable=load_data_to_postgres,
                                              op_kwargs={"file_name": 'dim_dates.csv', "table_name": 'datedimension'},)

        load_fct_outcomes_tab = PythonOperator(task_id="LOAD_FCT_OUTCOMES",
                                              python_callable=load_data_to_postgres,
                                              op_kwargs={"file_name": 'fct_outcomes.csv', "table_name": 'outcomesfact'},)

        end = BashOperator(task_id = "END", bash_command = "echo end")

        #  start >> extract_data_from_api_to_gcp >> transform_data_step >> 
        # [load_dim_animals, load_dim_outcome_types, load_dim_dates, load_fct_outcomes] >> end
        start >> extract_data_from_api_to_gcp >> transform_data_from_gcp_step >> [load_dim_animals_tab, load_dim_outcome_types_tab, load_dim_dates_tab] >> load_fct_outcomes_tab >> end