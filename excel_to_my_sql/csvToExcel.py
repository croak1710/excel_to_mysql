import pandas as pd

# Reemplaza 'ruta/a/tu/archivo.csv' con la ruta de tu archivo CSV
csv_file_path = 'C:/Users/Felipe/Desktop/ExcelAutos/autos.csv'  
excel_file_path = 'C:/Users/Felipe/Desktop/ExcelAutos/autosExcel.xlsx'  # Nombre del archivo de salida

# Leer el archivo CSV
try:
    data = pd.read_csv(csv_file_path)
    print("CSV cargado correctamente.")
    
    # Convertir a Excel
    data.to_excel(excel_file_path, index=False, engine='openpyxl')
    print(f"Archivo Excel guardado como {excel_file_path}")

except Exception as e:
    print(f"Ocurrió un error: {e}")
