import requests
import json
import psycopg2
from psycopg2.extras import execute_values
import os
from datetime import timedelta,datetime
import pandas as pd
from dotenv import load_dotenv
from sendgrid import SendGridAPIClient
from sendgrid.helpers.mail import Mail


dag_path = os.getcwd()

# Cargar variables de entorno desde el archivo .env
load_dotenv()

def extract_data(exec_date, **context) -> None:
    '''Obtención de datos de la API de football-data.org'''
    print(f"Adquiriendo data para la fecha: {exec_date}")
    execution_date = datetime.strptime(exec_date, '%Y-%m-%d %H')
    execution_date_previous = execution_date - timedelta(days=1)
    date_to = execution_date_previous.strftime('%Y-%m-%d')
    print(date_to)
    try:
        url = f'https://api.football-data.org/v4/matches?date={date_to}'
        headers = { 'X-Auth-Token': os.getenv('API_KEY')}
        response = requests.get(url, headers=headers)
        if response.status_code == 200:
            print('Conexion exitosa a API de football-data.org')
            try:
                data = response.json()
                # También puede ser de esta manera
                jsonfilename = f"{context['ds']}.json"
                print(jsonfilename)
                print(dag_path + '/raw_data/'+ "data_"+ jsonfilename)
                #with open(dag_path+'/raw_data/'+"data_"+(date_to[:4])+'-'+(date_to[5:7])+'-'+(date_to[8:])+ ".json", "w") as f:
                with open(dag_path + '/raw_data/'+ "data_"+ jsonfilename, "w") as f:
                    json.dump(data, f, indent=4, sort_keys=True)
            except json.JSONDecodeError as e:
                print(f'JSONDecodeError: {e}')
    except Exception as e:
        print(f'Request failed: {e}')

def transform_data(exec_date, **context) -> None: 
    '''Generación de DataFrame con datos transformados'''      
    print(f"Transformando datos para la fecha: {exec_date}")
    execution_date = datetime.strptime(exec_date, '%Y-%m-%d %H')
    execution_date_previous = execution_date - timedelta(days=1)
    date_to = execution_date_previous.strftime('%Y-%m-%d')
    print(date_to)
    jsonfilename = f"{context['ds']}.json"
    #with open(dag_path+'/raw_data/'+"data_"+(date_to[:4])+'-'+(date_to[5:7])+'-'+(date_to[8:])+ ".json", "r") as f:
    with open(dag_path+'/raw_data/'+ 'data_' + jsonfilename, "r") as f:
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
        #df.to_csv(dag_path+'/processed_data/'+"data_"+(date_to[:4])+'-'+(date_to[5:7])+'-'+(date_to[8:])+".csv", index=False, mode='w')
        df.to_csv(dag_path+'/processed_data/'+"data_"+ f"{context['ds']}.csv", index=False, mode='w')   
     
        
def loading_data(exec_date, **context):
    '''Conexión a Redshift y Carga de datos en Redshift'''
    
    print(f"Insertando datos para la fecha: {exec_date}")
    execution_date = datetime.strptime(exec_date, '%Y-%m-%d %H')
    execution_date_previous = execution_date - timedelta(days=1)
    date_to = execution_date_previous.strftime('%Y-%m-%d')
    print(date_to)
    
    try:
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
        registros = pd.read_csv(dag_path + '/processed_data/' + "data_" + f"{context['ds']}.csv")
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

def check_draw_games(exec_date, **context):
    '''Contar cuantos partidos han terminado en empate'''
    execution_date = datetime.strptime(exec_date, '%Y-%m-%d %H')
    execution_date_previous = execution_date - timedelta(days=1)
    date_to = execution_date_previous.strftime('%Y-%m-%d')
    
    try:
        df = pd.read_csv(dag_path + '/processed_data/' + "data_" + f"{context['ds']}.csv")
        if len(df[df['winner'] == 'DRAW']) > 0:
            subject = f'Hay partidos en empate para el {date_to}'
        else:
            subject =  f'No hay partidos con empate {date_to}'
            
            body = f"""
            Hola Moises,
            
            Para el día de hoy {date_to}, hay un total de {len(df[df['winner'] == 'DRAW'])} partidos
            que terminaron con empate."""
            
            message = Mail(
                from_email=os.getenv('EMAIL_FROM'),
                to_emails=os.getenv('EMAIL_TO'),
                subject=subject,
                html_content=body)
        try:
            sg = SendGridAPIClient(os.getenv('SENDGRID_API_KEY'))
            response = sg.send(message)
            print(response.status_code)
        except Exception as e:
            print(e.message)
    except FileNotFoundError:
        print(f'No hay registros para este día: {date_to}')  
        subject =  f'No hay partidos para este día {date_to}'
            
        body = f"""
        Hola Moises,
        
        Para el día de hoy {date_to}, NO HAY PARTIDOS REGISTRADOS."""
            
        message = Mail(
                from_email=os.getenv('EMAIL_FROM'),
                to_emails=os.getenv('EMAIL_TO'),
                subject=subject,
                html_content=body)
        try:
            sg = SendGridAPIClient(os.getenv('SENDGRID_API_KEY'))
            response = sg.send(message)
            print(response.status_code)
        except Exception as e:
            print(e.message)

# # Creación de tabla en Redshift
# def crear_tabla_redshift(conn):
#     cursor = conn.cursor()
#     try:
#         cursor.execute("""
#             CREATE TABLE IF NOT EXISTS games (
# 				country varchar(50)
#                 ,competition varchar(50)
#                 ,season_start date
#                 ,season_end date
#                 ,match_day timestamp
#                 ,home_team_id int
#                 ,home_team varchar(50)
#                 ,away_team_id int
#                 ,away_team varchar(50)
#                 ,home_goal int
#                 ,away_goal int
#                 ,winner varchar(50)
#                 ,status varchar(50)
#                 ,fecha_ingesta timestamp default getdate()
#                 ,primary key(match_day, home_team_id, away_team_id, fecha_ingesta)
#             );
#         """)
#         conn.commit()
#         print("Tabla creada con éxito en Redshift!")
#     except Exception as e:
#         print("No es posible crear la tabla en Redshift")
#         print(e)
        
# Inserción de datos en Redshift

# def insertar_datos_redshift(conn, df):
#     try:
#         with conn.cursor() as cur:
#             execute_values(cur, 'INSERT INTO games VALUES %s', df.values)
#             conn.commit()
#     except Exception as e:
#         print("No es posible insertar datos en Redshift")
#         print(e)
#     finally:
#         cur.close()
#         conn.close()


