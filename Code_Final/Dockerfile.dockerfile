# Usar la imagen oficial de Python con una versión específica como base
FROM python:3.12-slim

# Establecer el directorio de trabajo en el contenedor
WORKDIR /app

# Instalar Apache Airflow usando constraints para evitar conflictos de dependencias
# Versión 3.3.2 de Airflow (sin vulnerabilidades conocidas) con las constraints oficiales para Python 3.12
RUN pip install "apache-airflow==3.3.2" --constraint "https://raw.githubusercontent.com/apache/airflow/constraints-3.3.2/constraints-3.12.txt"

# Agregar argumentos para la instalación de psycopg2, librería necesaria para la conexión con PostgreSQL/Redshift
RUN apt-get update && apt-get install -y gcc libpq-dev && \
    pip install psycopg2-binary

# Instalar otras dependencias de Python si es necesario
RUN pip install pandas requests tqdm numpy

# Copiar el script de Airflow y el script de Python al directorio de trabajo en el contenedor
COPY dags/jeshua_dag.py /app/dags/
COPY scripts/jeshua_script.py /app/

# Configurar variables de entorno para Airflow
ENV AIRFLOW_HOME=/app/airflow

# Inicializar la base de datos de Airflow para preparar el entorno
RUN airflow db migrate

# Airflow 3 usa SimpleAuthManager: el usuario admin y su contraseña se generan al arrancar

# Exponer el puerto 8080 para acceder a la interfaz web de Airflow
EXPOSE 8080

# Comando para iniciar el servidor web y el scheduler de Airflow
CMD airflow scheduler & airflow dag-processor & airflow api-server --port 8080
