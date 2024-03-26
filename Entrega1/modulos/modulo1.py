import requests
import json

def api_key() -> str:
    '''Obtención de apikey para la API de football-data.org'''
    f = open("apikey.txt", "r")
    api_key = f.read()
    f.close()
    return api_key

def get_data(url:str, headers:dict) -> dict:
    response = requests.get(url, headers=headers)
    if response.status_code == 200:
        try:
            data = response.json()
            return data
        except json.JSONDecodeError as e:
            print(f'JSONDecodeError: {e}')
    else:
        print(f'Request failed with status code: {response.status_code}')
    return None  

def password():
    '''Obtención de password para la conexíon a la base de datos en Redshift con la API de football-data.org'''
    f = open("pwd_redshift.txt", "r")
    password = f.read()
    f.close()
    return password


class CreateRegister:
    """ 
    Generación de base de registros (lista de diccionarios) en un archivo json, con datos de juegos de la Liga Inglesa Temporada
    2022-2023 y 2023-2024
    """
    
    register = []
    
    def __init__(self, country: str, season_start: str, season_end: str, competition:str, match_day:str, home_team_id: int, home_team: str, 
                 away_team_id: int, away_team: str, home_goal:int, away_goal:int, winner:str, status: str) -> None:
        self.__country = country 
        self.__season_start = season_start
        self.__season_end = season_end
        self.__competition = competition
        self.__match_day = match_day
        self.__home_team_id = home_team_id
        self.__home_team = home_team
        self.__away_team_id = away_team_id
        self.__away_team = away_team
        self.__home_goal = home_goal
        self.__away_goal = away_goal
        self.__winner = winner
        self.__status = status
        
    #def __str__(self) -> str:
    #    return f'El cliente {self.__apellido}, {self.__nombre} de {self.__edad} años, se registró con el teléfono {self.__telefono} y correo {self.__correo}'
    
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