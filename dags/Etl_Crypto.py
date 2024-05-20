import requests
import pandas as pd
import psycopg2
from datetime import datetime

def get_crypto_data():
    # URL base de la API de CoinCap
    base_url = "https://api.coincap.io/v2"
    # Endpoint para obtener datos de activos
    endpoint = "/assets"
    # Encabezados de la solicitud con la clave API
    headers = {"Accept-Encoding": "gzip, deflate"}
    # URL completa para la solicitud
    url = base_url + endpoint
    # Realizar la solicitud GET a la API
    response = requests.get(url, headers=headers)
    # Verificar el código de estado de la respuesta
    if response.status_code == 200:
        # La solicitud fue exitosa
        data = response.json()
        # Convertir el JSON a un DataFrame de pandas
        df = pd.DataFrame(data['data'])
        # Agregamos columna fecha de consulta
        fecha_consulta = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
        df['fecha_consulta'] = fecha_consulta
        # Eliminar columnas innecesarias
        df.drop(['id'], axis=1, inplace=True)
        # Elegimos la columna symbol como ID. Reordenamos y renombramos
        symbol_column = df.pop('symbol')
        df.insert(0, 'id', symbol_column)
        return df  # Devolver DataFrame directamente
    else:
        # La solicitud falló
        print("Error al realizar la solicitud:", response.status_code)
        return None


def create_redshift_connection():
    url = "data-engineer-cluster.cyhh5bfevlmn.us-east-1.redshift.amazonaws.com"
    data_base = "data-engineer-database"
    user = "matiaspereyra_coderhouse"
    pwd = "oDSU1mg8L8"
    try:
        conn = psycopg2.connect(
            host=url,
            dbname=data_base,
            user=user,
            password=pwd,
            port='5439'
        )
        print("Conectado a Redshift con éxito!")
        return conn  # Devolver el objeto de conexión
    except Exception as e:
        print("No es posible conectar a Redshift")
        print(e)
        return None  # Devolver None en caso de error

def create_crypto_table(conn):
    try:
        # Crear cursor
        cursor = conn.cursor()

        # Crear la tabla
        cursor.execute("""
            CREATE TABLE IF NOT EXISTS matiaspereyra_coderhouse.crypto
            (
            id VARCHAR(50) primary key  
            ,rank INTEGER
            ,name VARCHAR(255)  
            ,supply DECIMAL
            ,maxSupply DECIMAL
            ,marketCapUsd DECIMAL
            ,volumeUsd24Hr DECIMAL  
            ,priceUsd DECIMAL 
            ,changePercent24Hr DECIMAL  
            ,vwap24Hr DECIMAL
            ,explorer VARCHAR(255)
            ,fecha_consulta TIMESTAMP
            ,fecha_insercion TIMESTAMP
            )
        """)

        conn.commit()
        print("Tabla de criptomonedas creada exitosamente en Redshift.")
    except Exception as e:
        print("Error al crear la tabla de criptomonedas en Redshift:", e)

def insert_data_into_redshift(conn, df):
    # Crear cursor
    cursor = conn.cursor()
    # Eliminar los registros existentes
    cursor.execute("DELETE FROM crypto")
    conn.commit()
    # Insertar registros
    for index, row in df.iterrows():
        cursor.execute("INSERT INTO crypto (id, rank, name, supply, maxSupply, marketCapUsd, volumeUsd24Hr, priceUsd, changePercent24Hr, vwap24Hr, explorer, fecha_consulta, fecha_insercion) VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)",
                       (row['id'], row['rank'], row['name'], row['supply'], row['maxSupply'], row['marketCapUsd'], row['volumeUsd24Hr'], row['priceUsd'], row['changePercent24Hr'], row['vwap24Hr'], row['explorer'], row['fecha_consulta'], datetime.now()))
    # Confirmar la inserción de los registros
    conn.commit()



def main():
    # Obtener datos de criptomonedas
    df = get_crypto_data()
    if df is not None:
        # Crear conexión a Redshift
        conn = create_redshift_connection()
        if conn is not None:
            # Crear tabla en Redshift
            create_crypto_table(conn)
            # Insertar datos en Redshift
            insert_data_into_redshift(conn, df)
            # Cerrar la conexión a Redshift
            conn.close()
        else:
            print("No se pudo establecer conexión a Redshift.")
    else:
        print("No se pudieron obtener datos de criptomonedas.")

if __name__ == "__main__":
    main()
