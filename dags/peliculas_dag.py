from airflow import DAG
from airflow.operators.python import PythonOperator
from datetime import datetime, timedelta
import os
from dotenv import load_dotenv, find_dotenv
import requests
from sqlalchemy import create_engine
import psycopg2
import pandas as pd
import numpy as np

# Define los parámetros predeterminados del DAG
default_args = {
    'owner': 'cristian',
    'start_date': datetime(2023, 11, 1),
    #'schedule_interval': '@daily',
    'retry_delay': timedelta(minutes=5),  # Tiempo de espera entre reintentos
    'retries': 5,  # Número máximo de reintentos en caso de fallo
}

# Define las credenciales de la base de datos
dotenv_path = ".env"
env = load_dotenv(find_dotenv())

host = os.getenv('REDSFHIT_HOST')
port = os.getenv('REDSHIFT_PORT')
database = os.getenv('REDSHIFT_DATABASE')
user = os.getenv('REDSHIFT_USER')
password = os.getenv('REDSHIFT_PASSWORD')
schema = os.getenv('REDSHIFT_SCHEMA')

conn_string = f"postgresql+psycopg2://{user}:{password}@{host}:{port}/{database}"

# Task 1: Creación de la Base de Datos (si no existe)
def task1():
    try:
        engine = create_engine(conn_string)
        
        with engine.connect() as connection:
            print("Conexión exitosa a la base de datos!")
            connection.execute("CREATE DATABASE IF NOT EXISTS peliculas;")
    except Exception as e:
        print("Error al conectar a la base de datos:", e)

        

def task2():
    try:
        # Realiza la solicitud a la API y obtiene los datos en formato JSON
        base_url = "https://api.themoviedb.org/3/movie/top_rated?api_key=59a68b6170002814047c923975011cff"
        api_url = f"{base_url}"
        response = requests.get(api_url)

        if response.status_code == 200:
            data = response.json()
            results = data.get('results', [])

            # Establece la conexión a la base de datos
            conn = psycopg2.connect(host=host, database=database, user=user, password=password, port=port,
                                    options="-c client_encoding=UTF8")
            cur = conn.cursor()

            # Obtiene los IDs existentes en la base de datos
            cur.execute("SELECT id FROM peliculas;")
            existing_ids = set([row[0] for row in cur.fetchall()])

            # Itera a través de los datos de la API y carga en la base de datos
            for result in results:
                pelicula_id = result.get('id') or 'Info. No disponible'

                # En caso de no existir el ID, carga los demás datos
                if pelicula_id not in existing_ids:
                    properties = result.get('properties', {})
                    title = properties.get('title') or 'Info. No disponible'
                    popularity = properties.get('popularity') or 0
                    release_date = properties.get('release_date') or 'Info. No disponible'
                    vote_average = properties.get('vote_average') or 0
                    vote_count = properties.get('vote_count') or 0
                    

                    # Inserta el registro en la base de datos si no existe
                    cur.execute("""
                        INSERT INTO peliculas (id, title, popularity, release_date, vote_average, vote_count)
                        VALUES (%s, %s, %s, %s, %s, %s);
                    """, (pelicula_id, title, popularity, release_date, vote_average, vote_count))

                    # Agrega el ID al conjunto de IDs insertados
                    existing_ids.add(pelicula_id)
            conn.commit()
            print("Se extrajo y almacenó todo correctamente en la base de datos.")

            # Cierra la conexión a la base de datos
            cur.close()
            conn.close()

        else:
            print("La solicitud a la API falló.")

    except Exception as e:
        print("Error al obtener datos de la API o cargarlos en la base de datos:", str(e))

# Crea el DAG
dag = DAG('peliculas_dag', default_args=default_args, description='DAG para cargar datos de peliculas')

#Define las tareas utilizando PythonOperator
task1 = PythonOperator(
    task_id='task1',
    python_callable=task1,
    dag=dag,
)

task2 = PythonOperator(
    task_id='task2',
    python_callable=task2,
    dag=dag,
)

#Define la secuencia de tareas: task1 -> task2
task1 >> task2