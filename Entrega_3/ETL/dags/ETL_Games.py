from datetime import timedelta,datetime
from pathlib import Path
import json
import requests
import psycopg2
from modulos.modulo1 import * 
from airflow import DAG
from sqlalchemy import create_engine
# Operadores
from airflow.operators.python_operator import PythonOperator
#from airflow.utils.dates import days_ago
import pandas as pd
import os

# Conexion a Redshift 
conn = redshift_conn()
# Cargar variables de entorno desde el archivo .env
load_dotenv()
dag_path = os.getcwd()

# argumentos por defecto para el DAG
default_args = {
    'owner': 'Pirela, Moises',
    'start_date': datetime(2024,5,15),
    'retries':5,
    'retry_delay': timedelta(minutes=5)
}

premier_dag = DAG(
    dag_id='Premier_ETL',
    default_args=default_args,
    description='Agregar datos de partidos de la liga inglesa diariamente',
    schedule_interval="@daily",
    catchup=False,
    doc_md="""ETL para la base de datos de partidos de la liga inglesa"""
)

# Tasks
# 1. Conexión a Redshift
task_1 = PythonOperator(
    task_id='conexión a Redshift',
    python_callable=redshift_conn,
    op_args=["{{ ds }} {{ execution_date.hour }}"],
    dag=premier_dag,
)