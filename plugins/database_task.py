from sqlalchemy import create_engine


# create_databse: Creación de la Base de Datos (si no existe)
def create_databse(conn_string):
    try:
        engine = create_engine(conn_string)
        
        with engine.connect() as connection:
            print("Conexión exitosa a la base de datos!")
            connection.execute("CREATE DATABASE IF NOT EXISTS peliculas;")
    except Exception as e:
        print("Error al conectar a la base de datos:", e)