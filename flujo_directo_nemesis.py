# coding=utf-8
from dotenv import load_dotenv
from datetime import datetime
import time
import sys
import os
import json
import logging
import requests
from dotenv import load_dotenv
load_dotenv()
sys.path.append(os.getenv('RUTA_PROYECTO'))
import controladores.transacciones_mongo as tm
from urllib.parse import urlparse, unquote

# Configura el nivel de registro
logging.basicConfig(filename='flujo_directo_nemesis.log', level=logging.DEBUG)

load_dotenv()
sys.path.append(os.getenv('RUTA_PROYECTO'))

from controladores.conexiones_oracle import execute
from controladores.transacciones_oracle import *
from helpers.cargar_sftp_nemesis import cargar_a_nemesis
import controladores.transacciones_mongo as tm


def flujo_directo_nemesis(incapacidades_json): 
    estado_nemesis = 1
    for lote in incapacidades_json:  
        procesar_nemesis_flujo_directo(lote,estado_nemesis)

def procesar_nemesis_flujo_directo(lote,estado_nemesis):
    # existe_nemesis = validar_formulario_nemesis(str(consecutivo),id_formulario)

    # if existe_nemesis  == False:
    #     print("El lote ya existe en nemesis")
    #     logging.info("El lote ya existe en nemesis")
    #     return False          

    # print(f"id_formularioBD - {id_formulario}")


    sequencia_lote = generar_secuencia_lote(lote[0]["lot_id"])   
    print('secuencia lote...',sequencia_lote)
    
    if sequencia_lote:
        for incapacidad in lote:
            documentos_ok = cargar_imagenes_nemesis(incapacidad,str(sequencia_lote))

        #si todo salió bien, insertar nemesis  
        if documentos_ok:  
            # documentos = sql.select_sql_json(config('select_consultar_documentos'), (id_formulario))  
            print('creando en nemesis...')  
            execute("SET TRANSACTION READ WRITE")
            # radicacion_lote = crear_lote_nemesis(id_formulario,cons_formulario,numero_documento,len(documentos),celular,fecha_solicitud,cant_ben,detalle[0],str(sequencia_lote),estado_nemesis)
            radicacion_lote = crear_lote_nemesis(lote,str(sequencia_lote),estado_nemesis)
        #     print("radicacion_lote", radicacion_lote)
        #     if radicacion_lote == False:
        #         print("ROLLBACK")  
        #         execute("ROLLBACK") 
        #     else:
        #         insert_nemesis = insertar_nemesis_flujo_directo(id_formulario,documentos,cons_formulario,numero_documento,detalle,radicacion_lote['radicacion'],radicacion_lote['lote'],cant_ben, tipo_json)
        #         if insert_nemesis: 
        #             print("COMMIT") 
        #             #execute("ROLLBACK")
        #             execute("COMMIT")
        #             #guardar secuencia utilizada
        #             sql.insert_sql(config('update_lote'), [id_formulario,sequencia_lote])

        #         else: 
        #             print("ROLLBACK") 
        #             execute("ROLLBACK")

def generar_secuencia_lote(lot_id):
    try:
        print('generando secuencia lote...')

        sequencia_lote=None
        #leer si existe una secuencia de lote


        sql_sq_lote = 'SELECT SEQ_LOTE_01.NEXTVAL FROM DUAL'
        res = select(sql_sq_lote,[])

        if res != False:
            sequencia_lote = res[0][0]
            update_data = {
                "lot_lote_nemesis": str(sequencia_lote),
                "lot_updated": datetime.now(),
            }
            tm.update_campos(lot_id,update_data,"lote")

        return sequencia_lote

        
    except Exception as e:
        print(e)
        return False

def crear_lote_nemesis(lote,sequencia_lote,estado_nemesis):

    fecha_creacion = lote[0]['FECHA INI INPC']

    # #1 insertar radicacion previa

    sequencia_lote = insertar_lote_flujo_directo(lote,fecha_creacion,sequencia_lote,estado_nemesis)
    print(f"sequencia_lote - {sequencia_lote}")

    if sequencia_lote:
        return {"lote": sequencia_lote}
    return False

def insertar_nemesis_flujo_directo(id_formulario,documentos,cons_formulario,numero_documento,detalle,sequencia_radicacion_previa,sequencia_lote,cant_ben,tipo_json):

    
    #cant_ben=len(detalle)
    obs_analitica='null'

    celular = detalle[0]['celular_cotizante'] or 'null'

    # objcreacion  = datetime.strptime(datetime.strftime(sql.select_sql(config('select_fecha_creacion'), (id_formulario))[0][0], "%d/%m/%Y %I:%M:%S"), "%d/%m/%Y %I:%M:%S")
    objcreacion  = ''
    #fecha de radicacion estaba quedando errada en algunos lotes ya que se le quitaba 5 horas a la fecha y si se creaba antes de las 5 de la mañana quedaba con la fecha anterior
    fecha_creacion = datetime.strftime(objcreacion, "%d/%m/%Y")
    #fecha_creacion = datetime.strftime(objcreacion - timedelta(hours=5), "%d/%m/%Y %I:%M:%S")

    fecha_solicitud = datetime.strftime(detalle[0]['fecha_solicitud'], "%d/%m/%Y %H:%M:%S")

    #consultar obs analitica
    obs_analitica = sql.select_sql_json(config('var_trazabilidad_form'), (id_formulario))
    obs_analitica = str(obs_analitica[0]['observaciones'])

    sequencia_radicacion = ''
    sequencia_traz = ''
    sequencia_bitacora = ''
    sequencia_encabezado = ''
    
    # #3 insertar radicacion    
    if sequencia_lote:                 
        sequencia_radicacion = insertar_radicacion(id_formulario,sequencia_lote,cons_formulario,numero_documento,obs_analitica)   
        print(f"sequencia_radicacion - {sequencia_radicacion}")

    # #4 insertar trazabilidad lote
    if sequencia_radicacion:  
        sequencia_traz = insertar_trazabilidad_lote(id_formulario,sequencia_lote,True)
        print(f"sequencia_traz - {sequencia_traz}")

    # #5 insertar bitacora
    if sequencia_traz:  
        sequencia_bitacora = insertar_bitacora(id_formulario,sequencia_radicacion)
        print(f"sequencia_bitacora - {sequencia_bitacora}")

    #6 insertar encabezado_unc y detalle_unc
    if sequencia_bitacora:  
        sequencia_encabezado = insertar_encabezado_flujo_directo(id_formulario,sequencia_lote,detalle,documentos,cons_formulario,fecha_creacion,celular,cant_ben,numero_documento,obs_analitica,tipo_json)
        print(f"sequencia_encabezado - {sequencia_encabezado}")

    print('sequencia_lote,sequencia_radicacion,sequencia_radicacion_previa,sequencia_traz,sequencia_bitacora,sequencia_encabezado')
    print(sequencia_lote,sequencia_radicacion,sequencia_radicacion_previa,sequencia_traz,sequencia_bitacora,sequencia_encabezado)

    # if sequencia_encabezado:
    #     sql.insert_sql(config('update_trazabilidad_lote'), (23,sequencia_lote,id_formulario))#Radicado Nemesis
    # else:
    #     sql.insert_sql(config('update_trazabilidad_ok'), (24,id_formulario))#Error Nemesis
    return False
    print(f"radicado en nemesis")
    
    
    return True

def cargar_imagenes_nemesis(incapacidad,sequencia_lote):
    try: 
        nom_documento=0
        adjuntos = ["ADJUNTO1","ADJUNTO2","ADJUNTO3","ADJUNTO4"]
        content_adjuntos = []
        for adjunto in adjuntos:
            url_documento = incapacidad[adjunto]
            if url_documento != None and url_documento != '':
                response = requests.get(url_documento)
                parsed_url = urlparse(url_documento)
                path = parsed_url.path
                filename = unquote(path.split('/')[-1])
                if response.status_code == 200:
                    # with open("Incapacidad.tif", "wb") as file:
                    #     file.write(response.content)
                    content_adjuntos.append([response.content,filename])

        for i,doc in enumerate(content_adjuntos):
            nom_documento+=1
            documentos_ok = True

            resp = cargar_a_nemesis(doc,str(sequencia_lote))
            if resp["status"] == False:
                tm.update_campos(incapacidad["ncp_id"],{"ncp_observaciones":f"No se cargó documento a Nemesis -> {resp['error']}"},"incapacidad")
                documentos_ok = False
            
            #todo ok
            campo_adjunto = f"ncp_adjunto{nom_documento}"
            tm.update_campos(incapacidad["ncp_id"],{campo_adjunto:resp['path_nemesis']},"incapacidad")
                
        # if documentos_ok:
        #     sql.insert_sql(config('update_trazabilidad_ok'), (8,id_formularioBD))#Imagenes enviadas nemesis
            
        # else:
        #     sql.insert_sql(config('update_trazabilidad'), (9,resp['error'],id_formularioBD))#Error cargando imagenes en nemesis
        
        print("Documentos ok en nemesis")
        return True
    except Exception as e:
        print(e)
        return False

def validar_formulario_nemesis(consecutivo, id_formulario):
    print("validando formulario....")
    sql_validarform=f"SELECT lote FROM ENCABEZADO_UNC WHERE numformulario= :consecutivo"
    res = select(sql_validarform,[consecutivo])
    print(res)
    if res == False:
        return True
    else:
        # sql.insert_sql(config('update_trazabilidad'), (17,'Lote Existe en nemesis',id_formulario))#Error Nemesis
        return False

def existe_docformulario(documentos):
    for doc in documentos:
        if 'P' in doc['tipo_documental']:
            return True
    print('No existe documento Formulario')
    return False

def lambda_handler():

    while True:
        time.sleep(int(os.getenv('intervalo_flujo_directo')))
        print("Fecha Inicio: ", datetime.now())
        flujo_directo_nemesis() 
        print("Fecha Fin: ", datetime.now())
        
# lambda_handler()