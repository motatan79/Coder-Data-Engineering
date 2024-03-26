# Entrega # 1. Obteniendo datos de API.
Comision: 60340

Creador: Pirela, Moises.
## Nombre del Proyecto

## Versión
1.0

# DESCRIPCION DEL PROYECTO
El proyecto consiste en obtener datos de la API gratuita  https://www.football-data.org/ de fútbol.

## Extraer el contenido desde el siguiente repositorio en git con el siguiente comando
git clone https://github.com/motatan79/Coder-Data-Engineering.git

## Para ingresar al proyecto se debe crear un ambiente virtual en la terminal
- python -m venv .venv (Windows)
- python3 -m venv .venv (Linux o Mac)

## Posteriormente activar el entorno virtual
- .\.venv\Scripts\activate (Windows Powershell)
- source .venv/bin/activate (Linux o Mac)

## Instalar pandas, requests, psycopg2 y datetime en el entorno virtual
pip install pandas
pip install requests
pip install psycopg2
pip install datetime

Para ejecutar el proyecto se debe ejecutar el archivo 'main.py'. 

Los diferentes pasos seguidos para la ejecución del proyecto son los siguientes: 

1) Llamar a la API y obtención de los datos 
Usando la url 'https://api.football-data.org/v4/competitions/PL/matches?dateFrom=2022-01-01&dateTo=2025-01-01' y el token de acceso (API_KEY) proporcionado por los creadores de la API, se obtienen los datos de los partidos de la liga.

2) Generación de un archivo JSON con los datos de la API. 

3) Generación de instancias relacionadas a los juegos en la Premier League, extrayendo del archivo JSON, los siguientes campos: 
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

4) Generación de DataFrame a partir del archivo JSON con las instancias.

5) Adición de columna temporal con la fecha en la cual será realizada la carga a la base de datos. 

6) Creación de la conexión a la base de datos en Redshift.

7) En caso de que no exista, creación de la tabla "games" y de existir eliminación de los datos presentes en ella.

8) Por último, carga de los datos a la tabla "games" de la base de datos.




