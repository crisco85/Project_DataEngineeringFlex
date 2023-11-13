import requests
import psycopg2

# Realiza la solicitud a la API y obtiene los datos en formato JSON

def get_movie_data(conn_string):
    try:
        base_url = "https://api.themoviedb.org/3/movie/top_rated?api_key=59a68b6170002814047c923975011cff"
        api_url = f"{base_url}"
        response = requests.get(api_url)

        if response.status_code == 200:
            movie_data = response.json()
            results = movie_data.get('results', [])

            # Establece la conexión a la base de datos
            conn = psycopg2.connect(conn_string)
            cur = conn.cursor()

            # Obtiene los IDs existentes en la base de datos
            cur.execute("SELECT id FROM peliculas;")
            existing_ids = set([row[0] for row in cur.fetchall()])

            # Itera a través de los datos de la API y carga en la base de datos
            for result in results:
                movie_id = result.get('id') or 'Info. No disponible'

                # En caso de no existir el ID, carga los demás datos
                if movie_id not in existing_ids:
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
                    """, (movie_id, title, popularity, release_date, vote_average, vote_count))

                    # Agrega el ID al conjunto de IDs insertados
                    existing_ids.add(movie_id)
            conn.commit()
            print("Se extrajo y almacenó todo correctamente en la base de datos.")

            # Cierra la conexión a la base de datos
            cur.close()
            conn.close()

        else:
            print("La solicitud a la API falló.")

    except Exception as e:
        print("Error al obtener datos de la API o cargarlos en la base de datos:", str(e))

