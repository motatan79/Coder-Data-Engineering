FROM python:3.9.5-slim-buster

# Crear el usuario airflow con UID y GID
RUN groupadd -g 50000 airflow && \
    useradd -u 50000 -g airflow -m -s /bin/bash airflow

# Instalar Apache Airflow
RUN pip install apache-airflow

# Establecer el directorio de trabajo en el contenedor
WORKDIR /opt/airflow

# Copiar los archivos de requisitos
COPY requirements.txt .

# Instalar las dependencias necesarias incluyendo python-dotenv
RUN pip install --no-cache-dir -r requirements.txt

# Copiar los DAGs al directorio de Airflow
COPY dags /opt/airflow/dags

# Copiar los plugins
COPY plugins /opt/airflow/plugins

# Copiar el archivo .env al directorio de trabajo
COPY .env /opt/airflow/

# Copiar el archivo entrypoint.sh
COPY entrypoint.sh /entrypoint.sh

# Cambiar los permisos del script de entrada con usuario root
USER root
RUN chmod +x /entrypoint.sh
USER airflow

# Configurar el punto de entrada
ENTRYPOINT ["/entrypoint.sh"]

# Exponer los puertos necesarios
EXPOSE 8080 8793

# Iniciar el scheduler y el webserver
CMD ["webserver"]


