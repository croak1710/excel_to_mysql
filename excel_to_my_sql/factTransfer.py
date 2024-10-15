import pandas as pd
import mysql.connector
import os

# RUTA A ARCHIVOS EXCEL
ruta_base = "C:/Users/Felipe/Desktop/ExcelListo/"

# Conexión a la base de datos MySQL
def connect_to_db():
    """Establece una conexión a la base de datos MySQL."""
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

# El orden de las columnas en el archivo Excel no es relevante ya que todos los campos
# contienen valores secuenciales (IDs del 1 al 371527). La estructura de la tabla en la
# base de datos es lo que se utiliza para la inserción.
# Función para insertar datos en la tabla fact_transactions
def insert_transactions_into_db(cursor, data):
    """Inserta los datos de las transacciones en la tabla fact_transactions."""
    try:
        insert_query = """
        INSERT INTO fact_transactions (transaction_id, dateCrawled, name, seller, offerType, price,
                                        abtest, vehicleType, yearOfRegistration, gearbox,
                                        powerPS, model, kilometer, monthOfRegistration,
                                        fuelType, brand, notRepairedDamage, dateCreated,
                                        nrOfPictures, postalCode, lastSeen)
        VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
        """
        cursor.executemany(insert_query, data)
    except mysql.connector.Error as err:
        print(f"Error al insertar datos en la tabla fact_transactions: {err}")

# Función principal que procesa e inserta los datos
def process_and_insert_transactions(file_name):
    """Lee el archivo Excel y procesa e inserta los datos en la base de datos."""
    try:
        # Verificar si el archivo existe
        if not os.path.exists(file_name):
            print(f"El archivo {file_name} no existe.")
            return

        # Leer el archivo Excel
        df = pd.read_excel(file_name)
        print(f"Archivo {file_name} leído correctamente.")

        # Preparar los datos para inserción en la base de datos
        data_to_insert = df.values.tolist()

        # Conectar a la base de datos
        connection = connect_to_db()
        if connection is not None:
            with connection.cursor() as cursor:
                # Insertar los datos en la tabla fact_transactions
                insert_transactions_into_db(cursor, data_to_insert)
                
                # Confirmar la transacción
                connection.commit()
                print("Datos insertados exitosamente en fact_transactions")
        
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

def main():
    process_and_insert_transactions(os.path.join(ruta_base, 'Fact_Transactions.xlsx'))

if __name__ == "__main__":
    main()
