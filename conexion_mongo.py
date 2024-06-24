def crear_lote_nemesis(id_formularioBD,cons_formulario,numero_documento,cant_docs,celular,fecha_solicitud,cant_ben,detalle,sequencia_lote,estado_nemesis):

    fecha_creacion = datetime.strftime(sql.select_sql(config('select_fecha_creacion'), (id_formularioBD))[0][0], "%d/%m/%Y %I:%M:%S")

    # #1 insertar radicacion previa
    sequencia_radicacion_previa = insertar_radicacion_previa(id_formularioBD,cons_formulario,numero_documento,cant_docs,celular,fecha_solicitud,cant_ben,detalle)
    print(f"sequencia_radicacion_previa - {sequencia_radicacion_previa}")
    
    # #2 insertar lote
    if sequencia_radicacion_previa:  
        sequencia_lote = insertar_lote_flujo_directo(id_formularioBD,fecha_creacion,sequencia_lote,estado_nemesis)
        print(f"sequencia_lote - {sequencia_lote}")

    if sequencia_radicacion_previa and sequencia_lote:
        return {"radicacion": sequencia_radicacion_previa, "lote": sequencia_lote}
    return False

def insertar_radicacion_previa(id_formularioBD,cons_formulario,numero_documento,cant_docs,celular,fecha_creacion,cant_ben,detalle):
    
    try:
        usuario_nemesis = config('usuario_nemesis')
        tipologia = config('tipologia_radicacionprevia') ##formulario_unico

        #Si es traslado
        tramite = consultar_tipotrabajador(detalle['codigo_tipo_trabajador_cotizante'])


        fecha_actual = (datetime.now() - timedelta(hours=5)).strftime("%d/%m/%Y %H:%M:%S")
        print('fecha_actual')
        print(fecha_actual)


        sql_sq_rad_p = 'SELECT SEQ_RADICACION_PREVIA.NEXTVAL from DUAL'
        res = select(sql_sq_rad_p,[])

        if res != False:
            sequencia_rad_p = res[0][0]
            sentencia_sql_p  = f"INSERT INTO RADICACION_PREVIA (ID,FECREA,USUCRE,CODCRE,TIPO,TRAMITE,FORMULARIO,CANBEN,OBS,ID_CLIENTE,ESTADO,IDE,DECSAL,TIPVEN,FECRAD,FECMOD,CERMOV,ANEXOS,TELEFONO,CELULAR,TIPEMP,CANVEN) VALUES ({sequencia_rad_p},to_date('{fecha_actual}','DD/MM/YYYY HH24:MI:SS'),'{usuario_nemesis}','{usuario_nemesis}','{tipologia}','{tramite}','{cons_formulario}','{cant_ben}',null,'1','201','{numero_documento}','N','{cant_ben}',to_date('{fecha_actual}','DD/MM/YYYY HH24:MI:SS'),to_date('{fecha_actual}','DD/MM/YYYY HH24:MI:SS'),'N','{cant_docs}',null,'{celular}',null,'{cant_ben}')"
        
        res = insert(sentencia_sql_p,[]) 
        if res != False:
            return sequencia_rad_p
    except Exception as e:
        print(e)
        sql.insert_sql(config('update_trazabilidad'), (14,'No se insertó la radicacion previa',id_formularioBD))
        return False
    
def insertar_lote_flujo_directo(id_formularioBD,fecha_creacion,sequencia_lote,estado_nemesis):
    try:
        usuario_nemesis = config('usuario_nemesis')
        tipo_tramite='1' 
        fecha_actual = (datetime.now() - timedelta(hours=5)).strftime("%d/%m/%Y %H:%M:%S")

        sentencia_sql = f"INSERT INTO LOTE (LOTE,GABINETE,ESTADO,FECREA,USUCRE,FECSEL,USUSEL,FECIND,USUIND,FECGUA,USUGUA,NUMIMA,OBS,PATH,FECCON,NUMFOR,FECARGINI,FECARGFIN,USUCAR,FECRAD,USURAD,ID_TRAMITE,FECCALINI,FECCALFIN,USUCAL,ID_CLIENTE,TRANS,CAR_PRO,FECCAR_PRO,USUREC,FECREC,CAJA,POS_ALM,ENVIADA,CIUDAD,AUDITORIA,CALIDADOK,GUIA,EMPRESA,TIPOLOTE,GRUPO_CAJA,LOTEFMS1,FECHALOTE,INDICE1,INDICE2,LOTEFMS,USUGES,FECGES,TIPOPROCESAMIENTO,USUQA,FEQA,PRODUCTO,FEFH,FEMC,USUFH,USUMC,FECPREP,USUPREP,USUCOLA,FECCOLA,NMNUMERO_IMAGENES,NMNUMERO_REGISTROS,USUPOSTMC,FECPOSTMC,NMCANTIDAD_INTENTOS,NMPRIORIZAR_LOTE,SERIE,TRANSMITIDO_SCAV,LOTELE,DADO_BAJA,NMCONTOPDF,FECDIG,USUDIG,USUDET,USUAUD,FECAUD,FECDET,NMEST_FIN_CONV,POS_ALM_CARPETA,CARPETA_CERRADA,USU_TRANS,AREA,HORA_RECEPCION,MEDIO_RECEPCION,NRO_PAGINAS,ESTRUCTURADO) VALUES ({sequencia_lote},'1',{estado_nemesis},to_date('{fecha_actual}','DD/MM/YYYY HH24:MI:SS'),'{usuario_nemesis}',null,null,null,null,null,null,null,null,null,null,null,null,null,null,to_date('{fecha_creacion}','DD/MM/YYYY HH24:MI:SS'),'{usuario_nemesis}',{tipo_tramite},null,null,null,'1','1',null,null,null,null,null,null,null,null,null,null,null,null,'Dia a Dia',null,null,null,null,null,null,null,null,'DIGITAL',null,null,'PRODUCCION',null,null,null,null,null,null,null,null,null,null,null,null,null,'3',null,null,null,null,null,null,null,null,null,null,null,null,null,null,null,null,null,null,null,1)"
        
        res = insert(sentencia_sql,[]) 
        if res != False:
            return sequencia_lote
    except Exception as e:
        print(e)
        sql.insert_sql(config('update_trazabilidad'), (24,'No se insertó lote',id_formularioBD))
        return False
    
