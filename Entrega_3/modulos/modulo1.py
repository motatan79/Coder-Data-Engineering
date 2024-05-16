import requests
import json
import psycopg2
from psycopg2.extras import execute_values
import os
from dotenv import load_dotenv
from datetime import timedelta,datetime
import datetime as dt
import pandas as pd

# Cargar variables de entorno desde el archivo .env
load_dotenv()

def extract_data(exec_date:str) -> None:
    '''Obtención de datos de la API de football-data.org'''
    try: 
        date_to = datetime.strptime(exec_date, '%Y-%m-%d')
        url = f'https://api.football-data.org/v4/competitions/PL/matches'
        headers = { 'X-Auth-Token':  os.getenv('API_KEY')}
    except Exception as e:
        print('Error al obtener la URL:', e)
    response = requests.get(url, headers=headers)
    try:
        if response.status_code == 200:
            print('Conexion exitosa a API de football-data.org')
            try:
                data = response.json()
                with open(dag_path+'/raw_data/'+"data_"+str(date_to.year)+'-'+str(date_to.month)+'-'+str(date_to.day)+ ".json", "w") as f:
                    json.dump(data, f, indent=4, sort_keys=True)
            except json.JSONDecodeError as e:
                print(f'JSONDecodeError: {e}')
    except Exception as e:
        print(f'Request failed with status code: {response.status_code}')

def transform_data(exec_date): 
    '''Generación de DataFrame con datos transformados'''      
    print(f"Transformando la data para la fecha: {exec_date}") 
    date_to = datetime.strptime(exec_date, '%Y-%m-%d %H')
    with open(dag_path+'/raw_data/'+"data_"+str(date_to.year)+'-'+str(date_to.month)+'-'+str(date_to.day)+".json", "r") as f:
        matches=json.load(f)
    games = []
    for i in range(len(matches['matches'])): 
        if matches['matches'][i]['utcDate'][:10] == '2024-05-15' and matches['matches'][i]['status'] == 'FINISHED':  
            country = matches['matches'][i]['area']['name']
            season_start = matches['matches'][i]['season']['startDate']
            season_end = matches['matches'][i]['season']['endDate']
            home_team_id = matches['matches'][i]['homeTeam']['id']
            home_team = matches['matches'][i]['homeTeam']['name']
            away_team_id = matches['matches'][i]['awayTeam']['id']
            away_team = matches['matches'][i]['awayTeam']['name']
            competition = matches['matches'][i]['competition']['name']
            match_day = matches['matches'][i]['utcDate']
            away_goal = matches['matches'][i]['score']['fullTime']['away']
            home_goal = matches['matches'][i]['score']['fullTime']['home']
            winner = matches['matches'][i]['score']['winner']
            status = matches['matches'][i]['status']    
            games.append({'country': country, 'season_start':season_start, 'season_end':season_end ,
                          'home_team_id':home_team_id, 'home_team':home_team, 'away_team_id':away_team_id,
                          'away_team':away_team, 'competition':competition, 'match_day':match_day, 
                          'away_goal':away_goal, 'home_goal':home_goal, 'winner':winner, 'status':status})
    if len(games) == 0:
        return f'No hay partidos en este día: {date_to}'
    else: 
        df = pd.DataFrame(games)
        df['fecha_ingesta'] = datetime.strptime(exec_date, '%Y-%m-%d')
        print(df)  
        df.to_csv(dag_path+'/processed_data/'+"data_"+str(date_to.year)+'-'+str(date_to.month)+'-'+str(date_to.day)+".csv", index=False, mode='w')



# Generación de conexión a RedShift
def redshift_conn() -> None:
    try:
        conn = psycopg2.connect(
        host=os.getenv('DB_HOST'),
        database=os.getenv('DB_NAME'),
        user=os.getenv('DB_USER'),
        password=os.getenv('DB_PASSWORD'), 
        port=os.getenv('DB_PORT') 
        )
        print("Conectado a Redshift con éxito!")
    except Exception as e:
        print("No es posible conectar a Redshift")
        print(e)
    return conn

# Creación de tabla en Redshift
def crear_tabla_redshift(conn):
    cursor = conn.cursor()
    try:
        cursor.execute("""
            CREATE TABLE IF NOT EXISTS games (
				country varchar(50)
                ,competition varchar(50)
                ,season_start date
                ,season_end date
                ,match_day timestamp
                ,home_team_id int
                ,home_team varchar(50)
                ,away_team_id int
                ,away_team varchar(50)
                ,home_goal int
                ,away_goal int
                ,winner varchar(50)
                ,status varchar(50)
                ,fecha_ingesta timestamp default getdate()
                ,primary key(match_day, home_team_id, away_team_id, fecha_ingesta)
            );
        """)
        conn.commit()
        print("Tabla creada con éxito en Redshift!")
    except Exception as e:
        print("No es posible crear la tabla en Redshift")
        print(e)
        
# Inserción de datos en Redshift

def loading_data(conn):
    date_to = dt.date.today()
    try:
        registros = pd.read_csv(dag_path+'/processed_data/'+"data_"+str(date_to.year)+'-'+str(date_to.month)+'-'+str(date_to.day)+".csv")
        print(registros)
        try:
            with conn.cursor() as cur:
                execute_values(cur, 'INSERT INTO games VALUES %s', registros.values)
                conn.commit()
        except Exception as e:
            print("No es posible insertar datos en Redshift")
            print(e)
        finally:
            cur.close()
            conn.close()
    except FileNotFoundError:
        print(f'No hay registros para este día: {date_to}')


def insertar_datos_redshift(conn, df):
    try:
        with conn.cursor() as cur:
            execute_values(cur, 'INSERT INTO games VALUES %s', df.values)
            conn.commit()
    except Exception as e:
        print("No es posible insertar datos en Redshift")
        print(e)
    finally:
        cur.close()
        conn.close()


