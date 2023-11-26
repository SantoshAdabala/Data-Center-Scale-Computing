
import os
import sys
import json
from airflow import DAG
from datetime import datetime, timedelta
from airflow.operators.bash import BashOperator
from airflow.operators.python_operator import PythonOperator




from ExtractTransformLoad_Steps.TransformData import transform_data
from ExtractTransformLoad_Steps.ExtractAPItoGCP import main
from ExtractTransformLoad_Steps.LoadData import load_data_to_postgres



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

        extract_api_to_gcp =  PythonOperator(task_id = "EXTRACT_DATA_FROM_API_TO_GCP",
                                                  python_callable = main,)

        transform_gcp_step = PythonOperator(task_id="TRANSFORM_DATA_FROM_GCP",
                                              python_callable=transform_data,)

        load_dim_animals = PythonOperator(task_id="LOAD_DIM_ANIMALS",
                                            python_callable=load_data_to_postgres,
                                             op_kwargs={"file_name": 'dim_animal.csv', "table_name": 'animaldimension'},)

        load_dim_outcome = PythonOperator(task_id="LOAD_DIM_OUTCOME_TYPES",
                                              python_callable=load_data_to_postgres,
                                              op_kwargs={"file_name": 'dim_outcome_types.csv', "table_name": 'outcomedimension'},)

        load_dim_dates = PythonOperator(task_id="LOAD_DIM_DATES",
                                             python_callable=load_data_to_postgres,
                                              op_kwargs={"file_name": 'dim_dates.csv', "table_name": 'datedimension'},)

        load_fct_outcomes = PythonOperator(task_id="LOAD_FCT_OUTCOMES",
                                              python_callable=load_data_to_postgres,
                                              op_kwargs={"file_name": 'fct_outcomes.csv', "table_name": 'outcomesfact'},)

        end = BashOperator(task_id = "END", bash_command = "echo end")

        start >> extract_api_to_gcp >> transform_gcp_step >> [load_dim_animals, load_dim_outcome, load_dim_dates] >> load_fct_outcomes >> end