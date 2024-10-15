import pandas as pd
import mysql.connector
from datetime import datetime
import os

# RUTA A ARCHIVOS EXCEL
ruta_base = "C:/Users/Felipe/Desktop/ExcelListo/excelDate/"

# Conexión a la base de datos MySQL
def connect_to_db():
    try:
        connection = mysql.connector.connect(
            host="localhost",
            user="root",
            password="admin",
            database="autos_bd"
        )
        return connection
    except mysql.connector.Error as err:
        print(f"Error al conectar a la base de datos: {err}")
        return None

# Función para procesar el DataFrame y obtener los componentes de la fecha
def process_date_dataframe(df, date_column):
    try:
        # Verificar si la columna existe en el DataFrame
        if date_column not in df.columns:
            raise ValueError(f"La columna '{date_column}' no existe en el archivo Excel.")
        
        # Convertir la columna de fechas a datetime
        df['date'] = pd.to_datetime(df[date_column], errors='coerce')
        
        # Extraer día, mes y año
        df['day'] = df['date'].dt.day
        df['month'] = df['date'].dt.month
        df['year'] = df['date'].dt.year
        
        # Filtrar filas con fechas válidas
        df = df.dropna(subset=['date'])
        
        # Extraer el ID (se asume que está en la primera columna)
        df['id'] = df.iloc[:, 0]  # Suponiendo que el ID está en la primera columna
        
        return df
    except Exception as e:
        print(f"Error al procesar las fechas del archivo: {e}")
        return None

# Función para insertar datos en la tabla correspondiente de MySQL
def insert_data_into_db(cursor, table_name, data):
    try:
        # Mapear los nombres de las columnas ID a sus correspondientes nombres reales
        id_column = {
            'dim_datecrawled': 'dateCrawled_id',
            'dim_datecreated': 'dateCreated_id',
            'dim_lastseen': 'lastSeen_id'
        }.get(table_name, f"{table_name[:-3]}_id")  # Valor por defecto en caso de no coincidir

        insert_query = f"""
        INSERT INTO {table_name} ({id_column}, date, day, month, year)
        VALUES (%s, %s, %s, %s, %s)
        """
        cursor.executemany(insert_query, data)
    except mysql.connector.Error as err:
        print(f"Error al insertar datos en la tabla {table_name}: {err}")

# Función principal que procesa e inserta los datos
def process_and_insert(file_name, table_name, date_column):
    try:
        # Verificar si el archivo existe
        if not os.path.exists(file_name):
            print(f"El archivo {file_name} no existe.")
            return

        # Leer el archivo Excel
        df = pd.read_excel(file_name)
        print(f"Archivo {file_name} leído correctamente.")

        # Procesar el DataFrame para extraer el día, mes, año y ID
        df = process_date_dataframe(df, date_column)
        
        if df is None:
            print(f"Error en el procesamiento del archivo {file_name}.")
            return
        
        # Preparar los datos para inserción en la base de datos
        data_to_insert = df[['id', 'date', 'day', 'month', 'year']].values.tolist()

        # Conectar a la base de datos
        connection = connect_to_db()
        if connection is not None:
            cursor = connection.cursor()

            # Insertar los datos en la tabla correspondiente
            insert_data_into_db(cursor, table_name, data_to_insert)
            
            # Confirmar la transacción
            connection.commit()
            print(f"Datos insertados exitosamente en {table_name}")
        
        else:
            print("No se pudo conectar a la base de datos")
    except Exception as e:
        print(f"Error durante el proceso de inserción para el archivo {file_name}: {e}")
    finally:
        # Cerrar conexión y cursor
        if 'cursor' in locals() and cursor:
            cursor.close()
        if 'connection' in locals() and connection:
            connection.close()

# Función que automatiza la inserción para las 3 tablas
def automate_insertion():
    try:
        # Procesar el archivo 'dateCrawledTable.xlsx' e insertar en 'dim_datecrawled'
        process_and_insert(os.path.join(ruta_base, 'dateCrawledTable.xlsx'), 'dim_datecrawled', 'dateCrawled')
        
        # Procesar el archivo 'dateCreatedTable.xlsx' e insertar en 'dim_datecreated'
        process_and_insert(os.path.join(ruta_base, 'dateCreatedTable.xlsx'), 'dim_datecreated', 'dateCreated')
        
        # Procesar el archivo 'lastSeenTable.xlsx' e insertar en 'dim_lastseen'
        process_and_insert(os.path.join(ruta_base, 'lastSeenTable.xlsx'), 'dim_lastseen', 'lastSeen')

    except Exception as e:
        print(f"Error en la automatización del proceso de inserción: {e}")

# Ejecutar el proceso de inserción automática
automate_insertion()
