import requests
import json
import psycopg2
from psycopg2.extras import execute_values
import os
from datetime import timedelta,datetime
import datetime as dt
import pandas as pd
from dotenv import load_dotenv


dag_path = os.getcwd()

# Cargar variables de entorno desde el archivo .env
load_dotenv()

def extract_data(exec_date) -> None:
    '''Obtención de datos de la API de football-data.org'''
    print(f"Adquiriendo data para la fecha: {exec_date[:10]}")
    print(f'este es el path: {dag_path}')
    try:
        date_to = '2024-05-19'  #exec_date[:10]
        # date_to = exec_date[:10]
        url = f'https://api.football-data.org/v4/matches'
        headers = { 'X-Auth-Token': os.getenv('API_KEY')}
        response = requests.get(url, headers=headers)
        if response.status_code == 200:
            print('Conexion exitosa a API de football-data.org')
            try:
                data = response.json()
                with open(dag_path+'/raw_data/'+"data_"+(date_to[:4])+'-'+(date_to[5:7])+'-'+(date_to[8:])+ ".json", "w") as f:
                    json.dump(data, f, indent=4, sort_keys=True)
            except json.JSONDecodeError as e:
                print(f'JSONDecodeError: {e}')
    except Exception as e:
        print(f'Request failed: {e}')

def transform_data(exec_date): 
    '''Generación de DataFrame con datos transformados'''      
    print(f"Transformando la data para la fecha: {exec_date}") 
    date_to = '2024-05-19'  #exec_date[:10]
    # date_to = exec_date[:10]
    with open(dag_path+'/raw_data/'+"data_"+(date_to[:4])+'-'+(date_to[5:7])+'-'+(date_to[8:])+ ".json", "r") as f:
        matches=json.load(f)
    games = []
    for i in range(len(matches['matches'])): 
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
        games.append({'country': country, 'competition':competition, 'season_start':season_start, 'season_end':season_end ,
                      'match_day':match_day, 'home_team_id':home_team_id, 'home_team':home_team, 'away_team_id':away_team_id,
                        'away_team':away_team, 'away_goal':away_goal, 'home_goal':home_goal, 'winner':winner, 'status':status})
    if len(games) == 0:
            return f'No hay partidos en este día: {date_to}'
    else: 
        df = pd.DataFrame(games)
        df['fecha_ingesta'] = date_to
        print(df)  
        df.to_csv(dag_path+'/processed_data/'+"data_"+(date_to[:4])+'-'+(date_to[5:7])+'-'+(date_to[8:])+".csv", index=False, mode='w')
    
# Generación de conexión a RedShift
# def loading_data(exec_date):
#     print(f"Insertando datos para la fecha: {exec_date}") 
#     date_to = '2024-05-15'  #exec_date[:10]
    
#     try:
#         url="data-engineer-cluster.cyhh5bfevlmn.us-east-1.redshift.amazonaws"
#         with open(dag_path+'/keys/'+"db.txt",'r') as f:
#             data_base= f.read()
#         with open(dag_path+'/keys/'+"user.txt",'r') as f:
#             user= f.read()
#         with open(dag_path+'/keys/'+"pwd.txt",'r') as f:
#             pwd= f.read()
#         # Conexión a Redshift       
#         conn = psycopg2.connect(
#         host=url,  #os.getenv('DB_HOST'),
#         database=data_base,  #os.getenv('DB_NAME'),
#         user=user,  #os.getenv('DB_USER'),
#         password=pwd,  #os.getenv('DB_PASSWORD'), 
#         port='5439'  #os.getenv('DB_PORT') 
#         )
#         print("Conectado a Redshift con éxito!")
#     except Exception as e:
#         print("No es posible conectar a Redshift")
#         print(e)
#     try:
#         registros = pd.read_csv(dag_path+'/processed_data/'+"data_"+(date_to[:4])+'-'+(date_to[5:7])+'-'+(date_to[8:])+".csv")
#         print(registros)
#         try:
#             with conn.cursor() as cur:
#                 execute_values(cur, 'INSERT INTO games VALUES %s', registros.values)
#                 conn.commit()
#         except Exception as e:
#             print("No es posible insertar datos en Redshift")
#             print(e)
#         finally:
#             cur.close()
#             conn.close()
#     except FileNotFoundError:
#         print(f'No hay registros para este día: {date_to}')
        
        
def loading_data(exec_date):
    print(f"Insertando datos para la fecha: {exec_date}") 
    date_to = '2024-05-19'  # exec_date[:10]
    try:
    #     url = "data-engineer-cluster.cyhh5bfevlmn.us-east-1.redshift.amazonaws.com"
    #     with open(dag_path+'/keys/'+"db.txt", 'r') as f:
    #         data_base = f.read().strip()
    #     with open(dag_path+'/keys/'+"user.txt", 'r') as f:
    #         user = f.read().strip()
    #     with open(dag_path+'/keys/'+"pwd.txt", 'r') as f:
    #         pwd = f.read().strip()
        # Conexión a Redshift       
        conn = psycopg2.connect(
            host= os.getenv('DB_HOST'),  #url,
            database= os.getenv('DB_NAME'),  #data_base,  
            user= os.getenv('DB_USER'), #user,  
            password= os.getenv('DB_PASSWORD'), #pwd,   
            port= os.getenv('DB_PORT') #'5439'   
        )
        print("Conectado a Redshift con éxito!")
    except Exception as e:
        print("No es posible conectar a Redshift")
        print(e)
        return

    try:
        registros = pd.read_csv(dag_path + '/processed_data/' + "data_" + (date_to[:4]) + '-' + (date_to[5:7]) + '-' + (date_to[8:]) + ".csv")
        print(registros.dtypes)
        try:
            with conn.cursor() as cur:
                execute_values(cur, 'INSERT INTO games VALUES %s', registros.values)
                conn.commit()
        except Exception as e:
            print("No es posible insertar datos en Redshift")
            print(e)
    except FileNotFoundError:
        print(f'No hay registros para este día: {date_to}')
    finally:
        if 'cur' in locals():
            cur.close()
        if 'conn' in locals():
            conn.close()

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

# def loading_data(conn, exec_date):
#     print(f"Transformando la data para la fecha: {exec_date}") 
#     date_to = '2024-05-15'  #exec_date[:10]
#     # date_to = exec_date[:10]
#     try:
#         registros = pd.read_csv(dag_path+'/processed_data/'+"data_"+(date_to[:4])+'-'+(date_to[5:7])+'-'+(date_to[8:])+".csv")
#         print(registros)
#         try:
#             with conn.cursor() as cur:
#                 execute_values(cur, 'INSERT INTO games VALUES %s', registros.values)
#                 conn.commit()
#         except Exception as e:
#             print("No es posible insertar datos en Redshift")
#             print(e)
#         finally:
#             cur.close()
#             conn.close()
#     except FileNotFoundError:
#         print(f'No hay registros para este día: {date_to}')


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


