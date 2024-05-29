from datetime import timedelta,datetime
from pathlib import Path
import json
import requests
import psycopg2
from modulos.modulo1 import * 
#from dotenv import load_dotenv
from airflow import DAG
from airflow.operators.python import PythonOperator
import pandas as pd
import os

# Cargar variables de entorno desde el archivo .env
load_dotenv()
dag_path = os.getcwd()

# argumentos por defecto para el DAG
default_args = {
    'owner': 'Pirela, Moises',
    'start_date': datetime(2024,5,15),
    'email': ['m.pirela@gmail.com'],
    'email_on_retry': False,
    'email_on_failure': True,
    'retries':5,
    'retry_delay': timedelta(minutes=5),
}

premier_dag = DAG(
    dag_id='Futbol_Games_ETL',
    default_args=default_args,
    description='Agregar datos de partidos de las diferentes ligas de fútbol diariamente',
    schedule_interval="@daily",
    catchup=False,
    doc_md="""ETL para la base de datos de partidos de las ligas de fútbol mundial"""
)

# Tasks
#1. Data Extraction
task_1 = PythonOperator(
    task_id='extraer_data',
    python_callable=extract_data,
    op_args=["{{ ds }} {{ execution_date.hour }}"],
    dag=premier_dag,
    provide_context=True,
)

#2. Data Transformation
task_2 = PythonOperator(
    task_id='transformar_data',
    python_callable=transform_data,
    op_args=["{{ ds }} {{ execution_date.hour }}"],
    dag=premier_dag,
    provide_context=True,
)

# 3. Data Loading 
task_3 = PythonOperator(
    task_id='load_data',
    python_callable=loading_data,
    op_args=["{{ ds }} {{ execution_date.hour }}"],
    dag=premier_dag,
    provide_context=True,
)

# 4. Send Email Notification
task_4 = PythonOperator(
    task_id='enviar_email_notificacion',
    python_callable=check_draw_games,
    op_args=["{{ ds }} {{ execution_date.hour }}"],
    dag=premier_dag,
    provide_context=True,
)

task_1 >> task_2 >> task_3 >> task_4