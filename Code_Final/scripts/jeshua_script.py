import requests
import pandas as pd
from tqdm import tqdm
import psycopg2
from psycopg2.extras import execute_values

def obtener_datos_desde_api(codigo_pais, codigo_indicador):
    """
    Realiza una petición a la API del Banco Mundial para obtener datos de un indicador específico de un país.
    """
    url = f"https://api.worldbank.org/v2/country/{codigo_pais}/indicator/{codigo_indicador}?format=json&date=2000:2021"
    headers = {'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/58.0.3029.110 Safari/537.3'}
    response = requests.get(url, headers=headers)
    if response.status_code == 200:
        data = response.json()
        if len(data) == 2 and 'page' in data[0]:
            return data[1]
    return []

def transformar_datos(datos_crudos):
    """
    Transforma los datos crudos obtenidos de la API en una lista de diccionarios adecuada para DataFrame.
    """
    return [
        {'Country Code': dato.get('country', {}).get('id', 'N/A'),
         'Country Name': dato.get('country', {}).get('value', 'N/A'),
         'Indicator Code': dato.get('indicator', {}).get('id', 'N/A'),
         'Year': int(dato.get('date', '1900')),
         'Value': dato.get('value', None)}
        for dato in datos_crudos if dato.get('value') is not None
    ]

def main():
    indicadores = ['FR.INR.RINR', 'PA.NUS.FCRF', 'NY.GDP.MKTP.CD', 'NY.GDP.PCAP.CD', 'SP.POP.TOTL', 'FP.CPI.TOTL']
    paises = ['MEX']
    resultados = []

    for pais in tqdm(paises, desc="Procesando países"):
        for indicador in tqdm(indicadores, desc=f"Indicadores para {pais}"):
            datos_crudos = obtener_datos_desde_api(pais, indicador)
            resultados.extend(transformar_datos(datos_crudos))

    df = pd.DataFrame(resultados)
    df['Indicator Code'] = df['Indicator Code'].str.replace('.', '_')
    df['Value'] = df['Value'].round(2)
    cargar_datos_a_redshift(df)

def cargar_datos_a_redshift(df):
    """
    Carga datos en Amazon Redshift.
    """
    conn_info = {
        "host": 'data-engineer-cluster.cyhh5bfevlmn.us-east-1.redshift.amazonaws.com',
        "dbname": 'data-engineer-database',
        "user": 'your_user',
        "password": 'your_password',
        "port": 5439
    }
    with psycopg2.connect(**conn_info) as conn:
        with conn.cursor() as cursor:
            cursor.execute("DROP TABLE IF EXISTS economic_data;")
            cursor.execute("""
            CREATE TABLE economic_data (
                "Country Code" VARCHAR NOT NULL,
                "Country Name" VARCHAR NOT NULL,
                "Indicator Code" VARCHAR NOT NULL,
                "Year" INTEGER NOT NULL,
                "Value" FLOAT,
                PRIMARY KEY ("Country Code", "Indicator Code", "Year")
            );
            """)
            execute_values(cursor,
                           "INSERT INTO economic_data (Country Code, Country Name, Indicator Code, Year, Value) VALUES %s",
                           [tuple(x) for x in df.to_records(index=False)])

if __name__ == "__main__":
    main()
