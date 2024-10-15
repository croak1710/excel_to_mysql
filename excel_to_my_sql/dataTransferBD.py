import mysql.connector
import pandas as pd
import os

try:
    conn = mysql.connector.connect(
        host='localhost',
        user='root',
        password='admin',
        database='autos_bd'
    )
    cursor = conn.cursor()

    def insert_data_from_excel(file_path, table_name, column_mapping):
        df = pd.read_excel(file_path)

        # Reemplazar valores NaN con None (que se convierte en NULL en MySQL)
        df = df.where(pd.notnull(df), None)

        # Convertir columnas numéricas a int solo si son completamente numéricas
        numeric_columns = df.select_dtypes(include=['number']).columns

        # Convertir columnas numpy.int64 a int (compatible con MySQL)
        for column in numeric_columns:
            if df[column].notnull().all():  # Solo convertir si no hay NaN
                df[column] = df[column].astype('Int64')  # Utilizar 'Int64' para permitir NA

        for column_name in column_mapping.keys():
            if column_name not in df.columns:
                raise ValueError(f"La columna '{column_name}' no se encuentra en el archivo Excel '{file_path}'.")

        values_list = []
        for index, row in df.iterrows():
            values = tuple(row[column_name] for column_name in column_mapping.keys())
            values_list.append(values)

        columns = ', '.join(column_mapping.values())
        placeholders = ', '.join(['%s'] * len(column_mapping))
        insert_query = f"INSERT INTO {table_name} ({columns}) VALUES ({placeholders})"
        
        cursor.executemany(insert_query, values_list)
        conn.commit()

    # Definir mapeos de columnas para cada tabla
    column_mappings = {
        'dim_abtest': {'index': 'abtest_id', 'abtest': 'abtest'},
        'dim_brand': {'index': 'brand_id', 'brand': 'brand'},
        'dim_fueltype': {'index': 'fuelType_id', 'fuelType': 'fuelType'},
        'dim_gearbox': {'index': 'gearbox_id', 'gearbox': 'gearbox'},
        'dim_kilometer': {'index': 'kilometer_id', 'kilometer': 'kilometer'},
        'dim_model': {'index': 'model_id', 'model': 'model'},
        'dim_monthofregistration': {'index': 'monthOfRegistration_id', 'monthOfRegistration': 'monthOfRegistration'},
        'dim_name': {'index': 'name_id', 'name': 'name'},
        'dim_notrepaireddamage': {'index': 'notRepairedDamage_id', 'notRepairedDamage': 'notRepairedDamage'},
        'dim_nrofpictures': {'index': 'nrOfPictures_id', 'nrOfPictures': 'nrOfPictures'},
        'dim_offertype': {'index': 'offerType_id', 'offerType': 'offerType'},
        'dim_postalcode': {'index': 'postalCode_id', 'postalCode': 'postalCode'},
        'dim_powerps': {'index': 'powerPS_id', 'powerPS': 'powerPS'},
        'dim_price': {'index': 'price_id', 'price': 'price'},
        'dim_seller': {'index': 'seller_id', 'seller': 'seller'},
        'dim_vehicletype': {'index': 'vehicleType_id', 'vehicleType': 'vehicleType'},
        'dim_yearofregistration': {'index': 'yearOfRegistration_id', 'yearOfRegistration': 'yearOfRegistration'},
    }

    # Mapeo de nombres de archivos a nombres de tablas
    file_table_mapping = {
        'abtestTable.xlsx': 'dim_abtest',
        'brandTable.xlsx': 'dim_brand',
        'fuelTypeTable.xlsx': 'dim_fueltype',
        'gearboxTable.xlsx': 'dim_gearbox',
        'kilometerTable.xlsx': 'dim_kilometer',
        'modelTable.xlsx': 'dim_model',
        'monthOfRegistrationTable.xlsx': 'dim_monthofregistration',
        'nameTable.xlsx': 'dim_name',
        'notRepairedDamageTable.xlsx': 'dim_notrepaireddamage',
        'nrOfPicturesTable.xlsx': 'dim_nrofpictures',
        'offerTypeTable.xlsx': 'dim_offertype',
        'postalcodeTable.xlsx': 'dim_postalcode',
        'powerPSTable.xlsx': 'dim_powerps',
        'priceTable.xlsx': 'dim_price',
        'sellerTable.xlsx': 'dim_seller',
        'vehicleTypeTable.xlsx': 'dim_vehicletype',
        'yearOfRegistrationTable.xlsx': 'dim_yearofregistration',
    }

    for file_name, table_name in file_table_mapping.items():
        file_path = f'C:/Users/Felipe/Desktop/ExcelListo/{file_name}'
        
        if os.path.exists(file_path):
            try:
                insert_data_from_excel(file_path, table_name, column_mappings[table_name])
            except Exception as e:
                print(f"Error al insertar datos de {file_name} en {table_name}: {e}")
        else:
            print(f"El archivo {file_path} no existe.")

except mysql.connector.Error as err:
    print(f"Error de MySQL: {err}")
except ValueError as ve:
    print(ve)
finally:
    if cursor:
        cursor.close()
    if conn:
        conn.close()