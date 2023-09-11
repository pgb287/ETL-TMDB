import requests
import json
import pandas as pd
from pprint import pprint
import sqlalchemy as sa
from configparser import ConfigParser
from datetime import datetime
from airflow import DAG
from airflow.operators.python_operator import PythonOperator


def extract_data():    
    
    # Accedo al archivo que contiene el token de acceso a la API    
    
    with open("config_tmdb/access_token.json") as f:
        token_file = json.load(f)        

    token = token_file['access_token']
      
    # Armo la URL
    # URL base:
    url_base = "https://api.themoviedb.org/3/"
    # Endpoint (películas más vistas en TMDB):
    endpoint = "trending/movie/day"
    # URL endpoint final:
    url = f"{url_base}{endpoint}"

    # Parametros que necesita la API
    headers = {
        "accept": "application/json",
        "Authorization": f"Bearer {token}"
    }

    # Hago la peticion GET a la API
    resp = requests.get(url, headers=headers)

    # Controlo el estado de la peticion, si es correcto almaceno la data en una variable (diccionario), caso contrario muestro el codigo de error y descripcion del mismo
    if resp.status_code == 200:
        resp_json = resp.json()
        print("Conexión exitosa")
        return resp_json
    else:
        print(resp.status_code, resp.content)

def transform_data(data):
    
    # Paso la data contenida en 'results' a un dataframe    
    df_previo = pd.DataFrame(data["results"])  

    # Creo un nuevo DF con las columnas necesarias
    df_movies = df_previo[["id","title","release_date","popularity","vote_average","vote_count"]]    
    
    return df_movies
    

def connect_to_redshift(config_file_path):
    
    config = ConfigParser()
    config.read(config_file_path)
    conn_data = config["Redshift"]

    host = conn_data["host"]
    port = conn_data["port"]
    db = conn_data["db"]
    user = conn_data["user"]
    pwd = conn_data["pwd"]   

    dbschema = f'{user}'

    conn = sa.create_engine(
        f"postgresql://{user}:{pwd}@{host}:{port}/{db}",
        connect_args={'options': f'-csearch_path={dbschema}'}
    )
    return conn

def load_to_redshift(df, tbl_name, conn):    

    try:
        df.to_sql(tbl_name, conn,
                index=False,
                method='multi',
                if_exists='append')
        print(f"Los datos se han cargado correctamente en la tabla '{tbl_name}'.")
    except Exception as e:
        print(f"Error al cargar los datos en la tabla '{tbl_name}': {e}")

def load_data(df_final):
    # Llamo la conexion
    conn = connect_to_redshift("config_tmdb/config.ini")

    # Creo la tabla
    with conn.connect() as cur:
        cur.execute(f'''
            begin transaction;
            drop table if exists movies_trending;
            CREATE TABLE if not exists movies_trending (
            id INTEGER PRIMARY KEY,
            title TEXT,
            release_date DATE,
            popularity INTEGER,
            vote_average INTEGER,
            vote_count INTEGER
            )SORTKEY(release_date);
            end transaction
        ''')

    # Cargo el DF en redshift
    table_name = "movies_trending"
    load_to_redshift(df_final, table_name, conn)


with DAG(
    dag_id="tmdb_etl",    
    description="Realiza la extraccion, transformacion y carga de datos desde la API de TMDB hasta Redshift",
    start_date=datetime(2023,9,10),    
    schedule_interval='@daily') as dag: 

    extract_data_task = PythonOperator(
        task_id="extraer_datos",
        python_callable=extract_data               
    )

    transform_data_task = PythonOperator(
        task_id="transformar_datos",
        python_callable=transform_data,
        op_args=[extract_data_task.output]             
    )

    load_data_task = PythonOperator(
        task_id="cargar_datos",
        python_callable=load_data,
        op_args=[transform_data_task.output]      
    )

    extract_data_task >> transform_data_task >> load_data_task
   