# Entrega Final. Carga de datos de API en Amazon Redshift usando Apache Airflow.
Comision: 60340

Creador: Pirela, Moises.
## Nombre del Proyecto

## Versión
1.0

# DESCRIPCION DEL PROYECTO
El proyecto consiste en obtener datos de la API gratuita  https://www.football-data.org/ de fútbol y cargar los datos en la base de datos de Amazon Redshift, usando un DAG con Airflow. Adicionalmente enviar un correo cuando existan juegos empatados.

# Para ejecutar el proyecto seguir los siguientes pasos:

1) Extraer el contenido desde el siguiente repositorio en git con el siguiente comando
git clone https://github.com/motatan79/Coder-Data-Engineering/tree/proyecto_final/Entrega_3

2) Situado en la carpeta "Entrega_3":
 - Reconstruir la imagen de docker con el siguiente comando:
  docker build -t my_airflow_image .
 - Ejecutar el contenedor con el siguiente comando:
  docker run -p 8080:8080 -d my_airflow_image

3) Ir a la web de Airflow e ingresar con las credenciales en entrypoint.sh. 

Los diferentes pasos seguidos para la ejecución del proyecto son los siguientes: 

En DAG en Airflow se ejecutarán en orden las cuatro tareas siguientes:
    - Task 1: Obtener datos de la API. 
        Desde la url 'https://api.football-data.org/v4/matches?date={date_to}', donde el date_to es la fecha de ejecución del DAG menos 1 día. Generar el archivo JSON a partir de los datos obtenidos de la API. 
    - Task 2: Transformación de datos. 
        Transformar el archivo JSON a un DataFrame, generando entradas relacionadas a los partidos extrayendo del archivo JSON, los siguientes campos: 
    - country.
    - competition. 
    - season_start.
    - season_end.
    - match_day.
    - home_team_id.
    - home_team.
    - away_team_id.
    - away_team.
    - home_goal.
    - away_goal.
    - winner.
    - status.             
    Adición de columna temporal con la fecha en la cual será realizada la carga a la base de datos. 
        Generar .csv a partir del DataFrame.
    - Task 3: Carga de datos en la base de datos.
        Cargar el archivo .csv a la base de datos, usando la configuración de Redshift.
    - Task 4: Enviar correo cuando existan partidos empatados.
        Enviar un correo usando SendGrid con el total de partidos empatados y el día en el cual se produce la acción.
