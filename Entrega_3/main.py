from modulos.modulo1 import * 
import pandas as pd
import datetime as dt
import os

# Conexion a Redshift para chequear última fecha de ingesta
conn = redshift_conn()

# Obtener última fecha de ingesta
max_date = chequear_datos_redshift(conn)

# Obtención de datos de la API de football-data.org para partidos de la Liga Inglesa desde 2022-01-01 hasta fecha actual

headers = { 'X-Auth-Token':  api_key() }
matches = get_data(get_url(max_date), headers)

# Eliminar todos los registros del json para que no se repitan los partidos
eliminar_registros_json(r'games.json')

# Generación de archivo json
with open(r'matches.json', 'w') as f:
    json.dump(matches, f, indent=4, sort_keys=True)
    
# Generación de Instancia relacionadas a juegos en la Liga Inglesa 
with open(r'matches.json', 'r') as f:
    matches = json.load(f)
  
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
    r = CreateRegister(country, competition, season_start, season_end, 
                       match_day, home_team_id, home_team, away_team_id, 
                       away_team, home_goal, away_goal, winner, status)
    r.add_contacto()
    r.guardar_json()
 
print('Instancias de clase CreateRegister creada con éxito')    
    
# Generación de DataFrame    
if os.stat('games.json').st_size != 2:
    df = pd.read_json('games.json')
    print(f'El Dataframe generado tiene un total de {df.shape[0]} filas y {df.shape[1]} columnas')
    print('----------------------------------------')
    print('Registros aleatorios tomados de DataFrame generado')
    if df.shape[0] < 3:
        print(df)
    else:
        print(df.sample(3))
    # Adición de columna temporal
    df['fecha_ingesta'] = dt.datetime.now().date()
    
    
    # Conexion a Redshift
    conn = redshift_conn()

    # Creación de tabla
    print('Creando tabla en Redshift')
    crear_tabla_redshift(conn)

    # Limpieza de tabla en Redshift
    print('Eliminar registros de tabla')
    delete_register(conn)

    # Inserción de datos en Redshift
    print('Insertando datos en Redshift')
    insertar_datos_redshift(conn, df)
    print("Última actualización de datos: ", dt.datetime.now().date())
else:
    print("Última actualización de datos: ", max[0])
    print('No hay registros que insertar para el día de hoy')
    



