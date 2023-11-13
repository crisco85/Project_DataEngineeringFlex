import smtplib
from email.mime.text import MIMEText
from email.mime.multipart import MIMEMultipart
from dotenv import load_dotenv, find_dotenv
import os

dotenv_path = ".env"
env = load_dotenv(find_dotenv())

# Configura la conexión SMTP
smtp_server = os.getenv('SMTP_SERVER')
smtp_port = os.getenv('SMTP_PORT')
smtp_username = os.getenv('SMTP_USERNAME')
smtp_password = os.getenv('SMTP_PASSWORD')

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

        # Adjunta detalles de terremotos al cuerpo del correo
        for movie in movie_data:
            movie_info = f"ID: {movie['id']}\nTitulo: {movie['title']}\nPopularidad: {movie['popularity']}\nFecha de publicación: {movie['release_date']}\nVoto promedio: {movie['vote_average']}\n\n"
            msg.attach(MIMEText(movie_info, 'plain'))

        # Envía el correo electrónico
        server.sendmail(smtp_username, recipients, msg.as_string())
        server.quit()
        print('Correo electrónico de alerta enviado con éxito.')
    except Exception as e:
        print(f'Error al enviar el correo electrónico: {str(e)}')