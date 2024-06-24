import csv
import json
import openpyxl
from datetime import datetime
import time
import sys,os
from dotenv import load_dotenv
load_dotenv()
sys.path.append(os.getenv('RUTA_PROYECTO'))
import helpers.cambioRuta as cambio_r
import helpers.error as er
import helpers.rabbitMQ as rabbit


def csv_json(csvFilePath, jsonFilePath):
    workbook = openpyxl.load_workbook(csvFilePath)
    sheet = workbook.active

    # Leer los encabezados de la primera fila
    headers = [cell.value for cell in sheet[1]]

    # Leer el resto de las filas
    data = []
    for row in sheet.iter_rows(min_row=2, values_only=True):
        record = {headers[i]: row[i] for i in range(len(headers))}
        data.append(record)

    # Función para manejar la serialización de datetime
    def datetime_converter(o):
        if isinstance(o, datetime):
            return o.isoformat()
        raise TypeError(f'Object of type {o.__class__.__name__} is not JSON serializable')

    # Convertir la lista de registros a JSON
    json_data = json.dumps(data, indent=4, default=datetime_converter)
        # Escribir el JSON en un archivo

    return json_data

def vscan_folders():
    try:
        ruta_entrada = os.getenv('RUTA_EXCEL')
        ruta_entrada_excel = ruta_entrada + "/Entrada"
        jsonFilePath = r'names.json'
        ruta_entrada_excel = cambio_r.cambio_ruta_windows(ruta_entrada_excel)
        folders = listar_archivos(ruta_entrada_excel)   
        for ruta_interna in folders:  
            fe_inicio_tar = datetime.now() 
            fecha_modificacion = datetime.fromtimestamp(os.path.getmtime(ruta_interna))
            fecha_creacion = datetime.fromtimestamp(os.path.getctime(ruta_interna))
            tiempo_modificacion = (fe_inicio_tar - fecha_modificacion).total_seconds()
            tiempo_creacion = (fe_inicio_tar - fecha_creacion).total_seconds()
            if tiempo_modificacion < int(60) or tiempo_creacion < int(60):
                continue
            incapacidades_json = csv_json(ruta_interna, jsonFilePath)
            with open(jsonFilePath, 'w') as json_file:
                json_file.write(incapacidades_json)
            rabbit.publicar_mensaje_rabbit(os.getenv('URL_RABBIT'), os.getenv('COLA_RABBIT'), os.getenv('EXCHANGE'), os.getenv('COLA_RABBIT'),str(incapacidades_json))  
    except Exception as ex:
        json_error = er.datos_error(sys,ex)
        print(json_error)
            
def listar_archivos(folder):
    """ Lista los archivos de un folder principal. """
    try:
        with os.scandir(folder) as ficheros:
            archivos = [fichero.path for fichero in ficheros if fichero.is_file()]
        return archivos
    except Exception as msg_error:  
        json_error = er.datos_error(sys,msg_error)
        raise Exception(json_error)      

while True:
    vscan_folders()   
    time.sleep(60)


    
