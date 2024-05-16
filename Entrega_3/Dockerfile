# Utiliza la imagen base oficial de Airflow
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

# Copia los scripts de entrada, configuraciones o archivos necesarios
COPY entrypoint.sh /entrypoint.sh

# Cambia los permisos del script de entrada con usuario root
USER root
RUN chmod +x /entrypoint.sh
USER airflow

# Configura el punto de entrada
ENTRYPOINT ["/entrypoint.sh"]

# Exponer los puertos necesarios
EXPOSE 8080 8793

# Inicia el scheduler y el webserver
CMD ["webserver"]


