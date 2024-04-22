import requests
import json
import psycopg2
from psycopg2.extras import execute_values
import os
from dotenv import load_dotenv

# Cargar variables de entorno desde el archivo .env
load_dotenv()

def api_key() -> str:
    '''Obtención de apikey para la API de football-data.org'''
    return os.getenv('API_KEY')

def get_data(url:str, headers:dict) -> dict:
    '''Obtención de datos de la API de football-data.org'''
    response = requests.get(url, headers=headers)
    if response.status_code == 200:
        try:
            data = response.json()
            print('Conexion exitosa a API de football-data.org')
            return data
        except json.JSONDecodeError as e:
            print(f'JSONDecodeError: {e}')
    else:
        print(f'Request failed with status code: {response.status_code}')
    return None  


def eliminar_registros_json(file_path):
    '''Eliminación de registros del archivo games.json'''
    try:
        with open(file_path, 'r') as file:
            data = json.load(file)
            
            # Eliminar todos los registros de la estructura de datos cargada
            data.clear()
            
        # Guardar la estructura de datos actualizada en el mismo archivo JSON
        with open(file_path, 'w') as file:
            json.dump(data, file, indent=4)  
                        
        print("Registros eliminados con éxito.")
        
    except FileNotFoundError:
        print(f"No se pudo encontrar el archivo '{file_path}'.")
    except json.JSONDecodeError:
        print(f"Error al decodificar el archivo JSON '{file_path}'.")

class CreateRegister:
    """ 
    Generación de base de registros (lista de diccionarios) en un archivo json, con datos de juegos de la Liga Inglesa Temporada
    2022-2023 y 2023-2024
    """
    
    register = []
    
    def __init__(self, country: str, competition:str, season_start: str, season_end: str,  match_day:str, home_team_id: int, home_team: str, 
                 away_team_id: int, away_team: str, home_goal:int, away_goal:int, winner:str, status: str) -> None:
        self.__country = country 
        self.__competition = competition
        self.__season_start = season_start
        self.__season_end = season_end
        self.__match_day = match_day
        self.__home_team_id = home_team_id
        self.__home_team = home_team
        self.__away_team_id = away_team_id
        self.__away_team = away_team
        self.__home_goal = home_goal
        self.__away_goal = away_goal
        self.__winner = winner
        self.__status = status
        
    def add_contacto(self):
        self.register.append({'country':self.__country.capitalize(), 'competition': self.__competition.capitalize(), 
                              'season_start': self.__season_start, 'season_end': self.__season_end,                               
                              'match_day': self.__match_day, 'home_team_id': self.__home_team_id, 'home_team': self.__home_team,
                             'away_team_id': self.__away_team_id, 'away_team': self.__away_team,
                             'home_goal': self.__home_goal, 'away_goal': self.__away_goal, 'winner': self.__winner, 'status': self.__status})
    
    @classmethod
    def guardar_json(cls):
        with open(r'games.json', 'w') as f:
            json.dump(cls.register, f, indent=4, ensure_ascii=False)
            
            
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
                ,primary key(match_day, home_team_id, away_team_id)
            );
        """)
        conn.commit()
        print("Tabla creada con éxito en Redshift!")
    except Exception as e:
        print("No es posible crear la tabla en Redshift")
        print(e)
        
def delete_register(conn):
    cursor = conn.cursor()
    try:
        cursor.execute("""
            DELETE FROM games WHERE fecha_ingesta::date = match_day::date;
        """)
        conn.commit()
        print("Registros eliminados con éxito en la tabla!")
    except Exception as e:
        print("No es posible eliminar los registros en la tabla")
        print(e)
        
# Inserción de datos en Redshift
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


def chequear_datos_redshift(conn):
    try:
        with conn.cursor() as cur:
            cur.execute('SELECT max(fecha_ingesta) FROM games') 
            max = cur.fetchall()[0]
    except Exception as e:
        print("No es posible chequear datos en Redshift")
        print(e)
    finally:
        cur.close()
        conn.close()
    return  max