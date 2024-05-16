#!/bin/bash

# Inicializar la base de datos de Airflow
airflow db init

# Crear un usuario administrador si no existe
airflow users create \
    --username admin \
    --password admin \
    --firstname Moises \
    --lastname Pirela \
    --role Admin \
    --email m.pirela@gmail.com

# Iniciar el scheduler y el webserver de Airflow
airflow scheduler &
airflow webserver
