# Usar la imagen oficial de Python con una versión específica como base
FROM python:3.8-slim

# Establecer el directorio de trabajo en el contenedor
WORKDIR /app

# Instalar Apache Airflow usando constraints para evitar conflictos de dependencias
# Usamos la versión 2.1.0 de Airflow como ejemplo, asegúrate de ajustar esto según tus necesidades
RUN pip install "apache-airflow==2.1.0" --constraint "https://raw.githubusercontent.com/apache/airflow/constraints-2.1.0/constraints-3.8.txt"

# Agregar argumentos para la instalación de psycopg2, librería necesaria para la conexión con PostgreSQL/Redshift
RUN apt-get update && apt-get install -y gcc libpq-dev && \
    pip install psycopg2

# Instalar otras dependencias de Python si es necesario
RUN pip install pandas requests tqdm numpy

# Copiar el script de Airflow y el script de Python al directorio de trabajo en el contenedor
COPY jeshua_dag.py /app/dags/
COPY jeshua_script.py /app/

# Configurar variables de entorno para Airflow
ENV AIRFLOW_HOME=/app/airflow

# Inicializar la base de datos de Airflow para preparar el entorno
RUN airflow db init

# Crear un usuario por defecto (opcional)
RUN airflow users create --username admin --password admin --firstname Admin --lastname User --role Admin --email admin@example.com

# Exponer el puerto 8080 para acceder a la interfaz web de Airflow
EXPOSE 8080

# Comando para iniciar el servidor web y el scheduler de Airflow
# -D indica que se ejecutarán como demonios
CMD airflow webserver -p 8080 -D && airflow scheduler -D && tail -f /dev/null
