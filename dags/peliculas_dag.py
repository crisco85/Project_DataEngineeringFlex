from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.decorators import task
from airflow.providers.postgres.operators.postgres import PostgresOperator
from datetime import datetime, timedelta
import os
from dotenv import load_dotenv, find_dotenv
import requests
from sqlalchemy import create_engine
import psycopg2
import pandas as pd
import numpy as np
import smtplib
from email.mime.text import MIMEText
from email.mime.multipart import MIMEMultipart

# Define los parámetros predeterminados del DAG
default_args = {
    'owner': 'cristian',
    'start_date': datetime(2023, 11, 15),
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

# Configura la conexión SMTP
smtp_server = os.getenv('SMTP_SERVER')
smtp_port = os.getenv('SMTP_PORT')
smtp_username = os.getenv('SMTP_USERNAME')
smtp_password = os.getenv('SMTP_PASSWORD')

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


def send_email_alert(subject, body, recipients, movie_data):
    try:
        server = smtplib.SMTP(smtp_server, smtp_port)
        server.starttls()
        server.login(smtp_username, smtp_password)

        # Crea el mensaje de correo electrónico
        msg = MIMEMultipart()
        msg['From'] = smtp_username
        msg['To'] = ', '.join(recipients)
        msg['Subject'] = subject  # Asunto personalizado por nosotros

        # Cuerpo del correo personalizado con detalles de peliculas
        msg.attach(MIMEText(body, 'plain'))

        # Adjunta detalles de peliculas al cuerpo del correo
        for movie in movie_data:
            movie_info = f"ID: {movie['id']}\nTitulo: {movie['title']}\nPopularidad: {movie['popularity']}\nFecha de publicación: {movie['release_date']}\nVoto promedio: {movie['vote_average']}\n\n"
            msg.attach(MIMEText(movie_info, 'plain'))

        # Envía el correo electrónico
        server.sendmail(smtp_username, recipients, msg.as_string())
        server.quit()
        print('Correo electrónico de alerta enviado con éxito.')
    except Exception as e:
        print(f'Error al enviar el correo electrónico: {str(e)}')


@task
def export_data_to_xcom(conn_id, sql_query):
    export_task = PostgresOperator(
        task_id='export_data_task',
        postgres_conn_id=conn_id,
        sql=sql_query,
    )
    return export_task


# Crea el DAG
dag = DAG('peliculas_dag', default_args=default_args, description='DAG para cargar datos de peliculas')

# Función que decide si enviar un mail
def decide_email_or_export(**kwargs):
    ti = kwargs['ti']
    exported_data = ti.xcom_pull(task_ids='export_data_task')

    # Verifica si la variable 'popularity' en los datos exportados es mayor que 100.000
    popularity_value = exported_data[0]['popularity'] if exported_data else None

    if popularity_value is not None and popularity_value > 100000:
        return 'send_email_alert'  # Si popularity > 100000, enviar correo
    return 'export_data_to_xcom'  # En cualquier otro caso, exporta la tabla

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

task3 = PythonOperator(
    task_id='send_email_alert',
    python_callable=send_email_alert,
    op_args=['Alerta de peliculas Recientes', '', ['cristiancorrea85@gmail.com'], "{{ ti.xcom_pull(task_ids='task2') }}"],  # Se pasa la información de peliculas desde task2
    provide_context=True,
    dag=dag,
)

#Task4: Exportar datos a xcom
task4 = PythonOperator(
    task_id='export_data_to_xcom',
    python_callable=export_data_to_xcom,
    op_args=[conn_string, 'SELECT * FROM peliculas LIMIT 10'],
    dag=dag,
)

# task5: Decidir si enviar un correo de alerta o exportar datos a xcom
decide_email_or_export_task = PythonOperator(
    task_id='decide_email_or_export',
    python_callable=decide_email_or_export,
    provide_context=True,
    dag=dag,
)


#Define la secuencia de tareas: task1 -> task2
task1 >> task2 >> decide_email_or_export_task
decide_email_or_export_task >> task3
decide_email_or_export_task >> task4