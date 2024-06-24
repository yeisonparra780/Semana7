
import sys,os
import json
from dotenv import load_dotenv
load_dotenv()
sys.path.append(os.getenv('RUTA_PROYECTO'))
import helpers.rabbitMQ as rabbit
import flujo_directo_nemesis as fdn
from helpers.agruparNit import agrupar_nit
import controladores.transacciones_mongo as tm
estado_orden = 4

def export_datos_rabbit():
    try:
        rabbit.leer_cola_rabbit(os.getenv('URL_RABBIT'),os.getenv('COLA_RABBIT'),export_datos)
    except Exception as msg_error:             
        print(msg_error)
        
def export_datos(ch, method, properties, body):
    try:
        mensaje_dic = body
        json_mensaje_dic = mensaje_dic.decode("utf-8")
        incapacidades_json = agrupar_nit(json_mensaje_dic)
        incapacidades_json = tm.registrar_incapacidades(incapacidades_json,estado_orden)
        fdn.flujo_directo_nemesis(incapacidades_json)
        print(mensaje_dic)
                
    except Exception as msg_error:
        print(msg_error)
    
export_datos_rabbit()