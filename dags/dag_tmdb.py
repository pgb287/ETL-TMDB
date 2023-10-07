import requests
import json
import pandas as pd
from pprint import pprint
import sqlalchemy as sa
from configparser import ConfigParser
from datetime import datetime
from airflow import DAG
from airflow.operators.python_operator import PythonOperator
import smtplib
from email.mime.text import MIMEText
from email.header import Header

def extract_data():    
    
    # Accedo al archivo que contiene el token de acceso a la API        
    with open("config_tmdb/access_token.json") as f:
        token_file = json.load(f)       

    token = token_file['access_token']
      
    # Armo la URL
    # URL base:
    url_base = "https://api.themoviedb.org/3/"
    # Endpoint (películas más populares actualmente en TMDB):
    endpoint = "movie/popular"
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

    # Creo un nuevo DF con las primera fila (es decir la pelicula mas popular) y las columnas mas importantes
    df_movies = df_previo[["id","title","release_date","popularity","vote_average","vote_count"]].head(1) 

    # Capturo la fecha actual 
    fecha_actual = datetime.now().date()
    df_movies['capture_date'] = fecha_actual   
    
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
                if_exists='append',
                method='multi')
        print(f"Los datos se han cargado correctamente en la tabla '{tbl_name}'.")
    except Exception as e:
        print(f"Error al cargar los datos en la tabla '{tbl_name}': {e}")

def load_data(df_final):
    # Llamo la conexion
    conn = connect_to_redshift("config_tmdb/config.ini")

    # Creo la tabla
    with conn.connect() as cur:
        cur.execute(f'''            
            CREATE TABLE if not exists movies_trending (
            id INTEGER PRIMARY KEY,
            title TEXT,
            release_date DATE,
            popularity INTEGER,
            vote_average INTEGER,
            vote_count INTEGER,
            capture_date DATE
            )SORTKEY(capture_date);            
        ''')

    # Cargo el DF en redshift
    table_name = "movies_trending"
    load_to_redshift(df_final, table_name, conn)


def check_data():  

    date = datetime.now().date()    
    query = f"SELECT title,popularity FROM movies_trending WHERE capture_date = '{date}'"    
    conn = connect_to_redshift("config_tmdb/config.ini")        
    df = pd.read_sql_query(query, conn)
        
    if df['popularity'].values[0] > 4000:       
    
        message = MIMEText(f"La pelicula {df['title'].values[0]} superó las expectativas", 'plain', 'utf-8')        
        message["Subject"] = Header("Pelicula muy popular!", 'utf-8')    
            
        smtp = smtplib.SMTP('smtp.gmail.com',587)
        smtp.starttls()
        smtp.login('pgb287@gmail.com','Aqui_estaba_el_password')
        smtp.sendmail('pgb287@gmail.com','pgb287@gmail.com',message.as_string())
        smtp.quit()
        print("Email Enviado!")
    

default_args = {
    'retries':3,
    'start_date':datetime(2023,10,7),
    'catchup':False
}

with DAG(
    dag_id="tmdb_etl",    
    description="Realiza la extraccion, transformacion y carga de datos desde la API de TMDB hasta Redshift",      
    schedule_interval='@daily',
    default_args=default_args
    ) as dag: 

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

    check_data_task = PythonOperator(
        task_id="verficar_datos",
        python_callable=check_data,        
    )


    extract_data_task >> transform_data_task >> load_data_task >> check_data_task
   
