# # Utiliza la imagen de python
# FROM python:3.9.5-slim-buster

# # Correr airflow  
# RUN pip install apache-airflow
FROM apache/airflow:2.5.1

# Establece el directorio de trabajo en el contenedor
WORKDIR /opt/airflow

# Copia los archivos de requisitos si los tienes
COPY requirements.txt .

# Instala las dependencias necesarias incluyendo python-dotenv
RUN pip install --no-cache-dir -r requirements.txt

# Copia los DAGs al directorio de Airflow
COPY dags /opt/airflow/dags

# Copia los plugins si tienes
COPY plugins /opt/airflow/plugins

# Copia el archivo .env al directorio de trabajo
COPY .env /opt/airflow/

# Copia los scripts de entrada, configuraciones o archivos necesarios
COPY entrypoint.sh /entrypoint.sh

# Cambia los permisos del script de entrada con usuario root
USER root
RUN mkdir -p /opt/airflow/raw_data && chmod -R 777 /opt/airflow/raw_data
RUN mkdir -p /opt/airflow/processed_data && chmod -R 777 /opt/airflow/processed_data
RUN chmod +x /entrypoint.sh
USER airflow

# Configura el punto de entrada
ENTRYPOINT ["/entrypoint.sh"]

# Exponer los puertos necesarios
EXPOSE 8080 8793

# Inicia el scheduler y el webserver
CMD ["webserver"]
