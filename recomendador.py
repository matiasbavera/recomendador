# -*- coding: utf-8 -*-
from flask import Flask, request, make_response
import pprint
import requests
from datetime import *
import json
from pymongo import MongoClient
from bson.json_util import dumps
import logging

from evaluacion.evaluation import evaluacion_total_recomendaciones
from filtro_colaborativo import calculate_similar_items, \
    calculate_similar_items_for_users, calculate_collaborative_filtering, \
    calculate_users_users

import six
from multiobjective import obtener_recomendacion_pareto_efficient_v2
from preprocesing.historial import obtener_historial_usuarios
from recomendadores.evento import obtener_temperatura, obtener_estacion, \
    obtener_recomendaciones
from reporte.reporte_datos import reporte_preprocesamiento_cliente
from variables_sistema import sysvar
from inicial import df, info_dataset, distance, userlist, \
    gral_eval_val, gral_eval, rating_raw, lista_usuarios_all, datos_archivo_raw
import gridfs
import requests

if (sysvar['estado_proyecto'] == 'tesis'):
    from inicial import trainset_folder
else:
    from inicial import trainset

app = Flask(__name__)

# Configuracion de sysvar
base_datos = sysvar['bd']
proyecto = sysvar['proyecto']
configuracion = sysvar['configuracion']
modelo = sysvar['modelo']
archivo = sysvar['archivo']

# Conexion a mongo
client = MongoClient(sysvar['bd_address'], 27017)
db = client[base_datos]
fs = gridfs.GridFS(db)

def reportes():
    reporte_preprocesamiento_cliente(lista_usuarios_all, userlist)
    pass


def obtener_filtro_colaborativo_usuario(folder=0):

    if (sysvar['estado_proyecto'] == 'tesis'):
        datos_a_entrenar = trainset_folder[folder]
    else:
        datos_a_entrenar = trainset

    return calculate_collaborative_filtering(
        recommender='user',
        rating=datos_a_entrenar,
    )


def obtener_filtro_colaborativo_item(model_name="als", folder=0):
    if (sysvar['estado_proyecto'] == 'tesis'):
        datos_a_entrenar = trainset_folder[folder]
    else:
        datos_a_entrenar = trainset
    proyecto = sysvar['proyecto']
    datos = sysvar['datos']
    datos_proyecto = sysvar['var_proyecto'][proyecto][datos]

    return calculate_collaborative_filtering(
        recommender='item',
        model_name=model_name,
        rating=datos_a_entrenar.T.tocoo(),
        KN=datos_proyecto['cosine'].get('k', 15),
        KNbm25=datos_proyecto['bm25'].get('k', 30),
        factors=datos_proyecto['als'].get('factors', 50),
        iterations=datos_proyecto['als'].get('iterations', 15),
    )


def obtener_modelo_item():
    return calculate_collaborative_filtering(
        recommender='distancia_item',
        rating=rating_raw,
    )


def obtener_recomendacion_multiobjetivo(recomendadores,
                                        model_name="opcion1",
                                        folder=0):
    # desarmar listas y combinarlas en una

    return obtener_recomendacion_pareto_efficient_v2(recomendadores,
                                                     distance,
                                                     info_dataset,
                                                     model_name,
                                                     folder=folder)


'''
:tipo_recomendador tiene un string con el nombre del recomendador
'''


def obtener_recomendacion_no_tradicional(tipo_recomendador):
    # conjunto_recomendacion = {}
    obtener_recomendacion = \
        json.loads(dumps(db.recomendadores.find().sort("fecha", -1).limit(1)))[
            0][tipo_recomendador]
    recomendacion = obtener_recomendacion['recomendacion']
    return recomendacion


def optimizacion_multiobjetivo():
    return "hola"


@app.route('/' + proyecto + '/actualizar/empresa', methods=['POST'])
def actualizar_empresa():
    content = request.get_json()
    datos_recomendador = content.get('empresa', {})
    datos = {
        "recomendacion": datos_recomendador,
        "fecha": datetime.now()
    }
    if bool(datos_recomendador):
        recomendador = {}
        recomendador['empresa'] = datos
        result = db.recomendadores.insert_one(
            recomendador
        )
        return str(recomendador)
    return str('No es el formato correcto')


@app.route('/' + proyecto + '/actualizar/experto', methods=['POST'])
def actualizar_experto():
    content = request.get_json()
    datos_recomendador = content.get('experto', {})
    datos = {
        "recomendacion": datos_recomendador,
        "fecha": datetime.now()
    }

    if bool(datos_recomendador):
        recomendador = {}
        recomendador['experto'] = datos
        result = db.recomendadores.insert_one(
            recomendador
        )
        return str(recomendador)
    # print(result.inserted_id)
    return str('No es el formato correcto')


@app.route('/' + proyecto + '/evento')
def recomendacion_por_evento():
    clima = obtener_temperatura()
    estacion = obtener_estacion()
    # fecha_relevante = obtener_fecha_relevante()
    # evento_deportivo = obtener_evento()

    """
    Se obtienen las recomendaciones de acuerdo al clima y fechas especiales
    """
    recomendaciones = obtener_recomendaciones(clima, estacion)
    print(recomendaciones)
    print(clima)
    print(estacion)
    return json.dumps({"clima": recomendaciones[0]})


@app.route('/' + proyecto + '/experto')
def recomendacion_experto():
    return json.dumps({"experto": obtener_recomendacion_no_tradicional(
        'experto')})


@app.route('/' + proyecto + '/empresa')
def recomendacion_empresa():
    return json.dumps({"empresa": obtener_recomendacion_no_tradicional(
        'empresa')})


@app.route('/' + proyecto + '/proveedor')
def recomendacion_proveedor():
    # TODO crear recomendacion por proveedor
    return json.dumps({"proveedor": obtener_recomendacion_no_tradicional(
        'proveedor')})


@app.route('/' + proyecto + '/frecuencia')
def recomendacion_frecuencia():
    # TODO crear recomendacion por frecuencia de compra
    return json.dumps({"frecuencia": obtener_recomendacion_no_tradicional(
        'frecuencia')})


@app.route('/' + proyecto + '/colaborativo/usuario')
def recomendacion_filtro_colaborativo_usuario():
    # TODO crear recomendacion por frecuencia de compra
    return json.dumps(
        {"colaborativo_usuario":
             obtener_filtro_colaborativo_usuario()
         }
    )


@app.route('/' + proyecto + '/colaborativo/item')
def recomendacion_filtro_colaborativo_item():
    # TODO crear recomendacion por frecuencia de compra
    return json.dumps(
        {"colaborativo_item":
             obtener_filtro_colaborativo_item()
         }
    )


@app.route('/' + proyecto + '/historial/guardar', methods=['GET'])
def historial_guardar():
    datos = {
        "historial": obtener_historial_usuarios(datos_archivo_raw),
        "fecha": datetime.now()
    }
    if (sysvar['datos'] == 'articulo'):
        result = db.historial.insert_one(datos)

    elif (sysvar['datos'] == 'clasificacion'):
        result = db.historial_clasificacion.insert_one(datos)

    return str(datos)


@app.route('/' + proyecto + '/distancia/guardar', methods=['GET'])
def distancia_guardar():
    datos = {
        "distancia": {}
    }
    # "historial": obtener_historial_usuarios(datos_archivo_raw),
    distancias = sysvar['distancia']
    for kdiv, valdiv in sorted(distancias.items()):
        if valdiv:
            datos["distancia"][kdiv] = distance[kdiv]

    if (sysvar['datos'] == 'articulo'):
        # result = db.distancia.insert_one(datos)
        filename = 'distancia' + '_' + 'articulo'
        fs.put(json.dumps(datos), filename=filename, encoding='utf8')

    elif (sysvar['datos'] == 'clasificacion'):
        # result = db.distancia_clasificacion.insert_one(datos)
        filename = 'distancia' + '_' + 'clasificacion'
        fs.put(json.dumps(datos), filename=filename, encoding='utf8')

    return str(datos)


###############################################################################
########################## #ENTRENAMIENTO #####################################
###############################################################################

def limpiar_recomendaciones(recomendaciones):
    lista_nueva = {}

    for recomendador in recomendaciones:
        lista_nueva[recomendador] = {}

        for usuario in recomendaciones[recomendador]:
            if usuario in sysvar['eval']['usuario_prueba']:
                lista_nueva[recomendador][usuario] = recomendaciones[
                    recomendador][usuario]
    return lista_nueva


def obtener_lista_recomendadores(folder=0):
    '''
    Esta funcion obtiene la lista recomendaciones brindada por cada
    recomendador en particular. En caso de que el recomendador esté
    habilitado para realizar una recomendación.
    Retorna un diccionario con los recomendadores y sus recomendaciones
    '''
    recomendaciones = {}
    if configuracion['evento']:
        clima = obtener_temperatura()
        estacion = obtener_estacion()
        recomendaciones['evento'] = obtener_recomendaciones(clima, estacion)
    if configuracion['empresa']:
        recomendaciones['empresa'] = obtener_recomendacion_no_tradicional(
            'empresa')
    if configuracion['experto']:
        recomendaciones['experto'] = obtener_recomendacion_no_tradicional(
            'experto')
    if configuracion['proveedor']:
        recomendaciones['proveedor'] = obtener_recomendacion_no_tradicional(
            'proveedor')
    if configuracion['frecuencia']:
        recomendaciones['frecuencia'] = obtener_recomendacion_no_tradicional(
            'frecuencia')
    if configuracion['tradicional']:
        recomendaciones['tradicional'] = \
            obtener_recomendacion_no_tradicional('experto')

    if configuracion['colaborativo_usuario']:
        recomendaciones["colaborativo_usuario"] = \
            obtener_filtro_colaborativo_usuario(folder=folder)

    if configuracion['colaborativo_item_als']:
        recomendaciones["colaborativo_item_als"] = \
            obtener_filtro_colaborativo_item(folder=folder)

    if configuracion['colaborativo_item_cosine']:
        recomendaciones["colaborativo_item_cosine"] = \
            obtener_filtro_colaborativo_item(model_name="cosine", folder=folder)

    if configuracion['colaborativo_item_bm25']:
        recomendaciones["colaborativo_item_bm25"] = \
            obtener_filtro_colaborativo_item(model_name="bm25", folder=folder)

    if configuracion['multiobjetivo_opcion1']:
        recomendaciones["multiobjetivo_opcion1"] = \
            obtener_recomendacion_multiobjetivo(recomendaciones,
                                                'opcion1',
                                                folder=folder)

    if configuracion['multiobjetivo_opcion2']:
        recomendaciones["multiobjetivo_opcion2"] = \
            obtener_recomendacion_multiobjetivo(recomendaciones,
                                                'opcion2',
                                                folder=folder)

    if configuracion['multiobjetivo_opcion3']:
        recomendaciones["multiobjetivo_opcion3"] = \
            obtener_recomendacion_multiobjetivo(recomendaciones,
                                                'opcion3',
                                                folder=folder)

    if configuracion['multiobjetivo_opcion4']:
        recomendaciones["multiobjetivo_opcion4"] = \
            obtener_recomendacion_multiobjetivo(recomendaciones,
                                                'opcion4',
                                                folder=folder)

    return recomendaciones


def obtener_recomendadores_tradicionales(folder=''):
    recomendaciones = {}

    if folder != '':
        fold = '_' + folder
    if (sysvar['datos'] == 'articulo'):
        if configuracion['colaborativo_item_als']:

            nombre_recomendador = 'colaborativo_item_als_articulo' + fold
            recomendaciones["colaborativo_item_als"] = json.loads(
                str(fs.get_last_version(
                    filename=nombre_recomendador).read(), 'utf8'))

        if configuracion['colaborativo_item_cosine']:

            nombre_recomendador = 'colaborativo_item_cosine_articulo' + fold
            recomendaciones["colaborativo_item_cosine"] = json.loads(
                str(fs.get_last_version(
                    filename=nombre_recomendador).read(), 'utf8'))

        if configuracion['colaborativo_item_bm25']:

            nombre_recomendador = 'colaborativo_item_bm25_articulo' + fold
            recomendaciones["colaborativo_item_bm25"] = json.loads(
                str(fs.get_last_version(
                    filename=nombre_recomendador).read(), 'utf8'))
    else:

        if configuracion['colaborativo_item_als']:

            nombre_recomendador = 'colaborativo_item_als_clasificacion' + fold
            recomendaciones["colaborativo_item_als"] = json.loads(
                str(fs.get_last_version(
                    filename=nombre_recomendador).read(), 'utf8'))

        if configuracion['colaborativo_item_cosine']:

            nombre_recomendador = 'colaborativo_item_cosine_clasificacion' + fold
            recomendaciones["colaborativo_item_cosine"] = json.loads(
                str(fs.get_last_version(
                    filename=nombre_recomendador).read(), 'utf8'))

        if configuracion['colaborativo_item_bm25']:

            nombre_recomendador = 'colaborativo_item_bm25_clasificacion' + fold
            recomendaciones["colaborativo_item_bm25"] = json.loads(
                str(fs.get_last_version(
                    filename=nombre_recomendador).read(), 'utf8'))

    return recomendaciones


def guardar_tradicionales_en_gridfs():
    recomendaciones = obtener_recomendadores_tradicionales()

    for recomedador in recomendaciones:
        if 'fecha' in recomendaciones[recomedador]:
            del recomendaciones[recomedador]['fecha']
        if '_id' in recomendaciones[recomedador]:
            del recomendaciones[recomedador]['_id']
        if 'estado' in recomendaciones[recomedador]:
            del recomendaciones[recomedador]['estado']

    if (sysvar['datos'] == 'articulo'):
        # Esto se agregó solamente para el caso que mongo no pueda guardar los
        #  16 mb
        if sysvar['tesis']:
            if 'usuario_prueba' in sysvar['eval']:
                if bool(sysvar['eval']['usuario_prueba']):
                    rec = limpiar_recomendaciones(recomendaciones)
                    guardar_recomendaciones_grid('articulo', rec)
                else:
                    print('La lista está vacia')
            else:
                guardar_recomendaciones_grid('articulo', recomendaciones)

    elif (sysvar['datos'] == 'clasificacion'):
        guardar_recomendaciones_grid('clasificacion', recomendaciones)


def entrenar_por_recomendador(opcion, folder=''):
    recomendaciones_mult = {}

    recomendaciones = obtener_recomendadores_tradicionales(folder=folder)

    for recomedador in recomendaciones:
        if 'fecha' in recomendaciones[recomedador]:
            del recomendaciones[recomedador]['fecha']
        if '_id' in recomendaciones[recomedador]:
            del recomendaciones[recomedador]['_id']
        if 'estado' in recomendaciones[recomedador]:
            del recomendaciones[recomedador]['estado']

    nombre_opcion = 'multiobjetivo_' + opcion
    recomendaciones_mult[nombre_opcion] = obtener_recomendacion_multiobjetivo(
        recomendaciones,
        opcion,
        folder=folder
    )

    if (sysvar['datos'] == 'articulo'):
        # Esto se agregó solamente para el caso que mongo no pueda guardar los
        #  16 mb
        if sysvar['tesis']:
            if 'usuario_prueba' in sysvar['eval']:
                if bool(sysvar['eval']['usuario_prueba']):
                    rec = limpiar_recomendaciones(recomendaciones_mult)
                    guardar_recomendaciones_grid('articulo', rec)
                else:
                    print('La lista está vacia')
            else:
                guardar_recomendaciones_grid('articulo',
                                             recomendaciones_mult,
                                             folder=folder
                                             )

    elif (sysvar['datos'] == 'clasificacion'):
        guardar_recomendaciones_grid('clasificacion',
                                     recomendaciones_mult,
                                     folder=folder
                                     )


@app.route('/' + proyecto + '/entrenar')
def recomendacion_general(folder=0):
    """
    Se ejecutan todos los recomendadores configurados
    Se les da formato y se guarda en una base de datos
    """
    # obtiene las recomendaciones habilitadas
    recomendaciones = obtener_lista_recomendadores(folder=folder)

    # Se guardan las recomendaciones para el usuario en la base de datos
    if (sysvar['datos'] == 'articulo'):
        # Esto se agregó solamente para el caso que mongo no pueda guardar los
        #  16 mb
        if sysvar['tesis']:
            if 'usuario_prueba' in sysvar['eval']:
                if bool(sysvar['eval']['usuario_prueba']):
                    rec = limpiar_recomendaciones(recomendaciones)
                    guardar_recomendaciones_grid('articulo', rec, folder=folder)
                else:
                    print('La lista está vacia')
            else:
                guardar_recomendaciones_grid('articulo',
                                             recomendaciones,
                                             folder=folder
                                             )

    elif (sysvar['datos'] == 'clasificacion'):
        guardar_recomendaciones_grid(
            'clasificacion',
            recomendaciones,
            folder=folder
        )

    return str(recomendaciones)


def guardar_recomendaciones(coleccion, recomendaciones):
    for recomendador in recomendaciones:
        nombre_coleccion = recomendador + '_' + coleccion
        datos = recomendaciones[recomendador]
        ########### Guardan resultados a la bd ##########################
        # Se agrega la fecha a la recomendaciones encontradas
        # datos['fecha'] = datetime.now()
        datos['estado'] = sysvar['estado_proyecto']
        filename = nombre_coleccion
        db[nombre_coleccion].insert_one(datos)


def guardar_recomendaciones_grid(coleccion, recomendaciones, folder=0):
    """

    :param coleccion: Tipo de colección. articulo o clasificacion
    :param recomendaciones: diccionario con los recomendadores y sus contenidos
    :param folder: str con el nombre del folder correspondiente folder_X
    :return:
    """
    for recomendador in recomendaciones:
        nombre_coleccion = recomendador + '_' + coleccion
        datos = recomendaciones[recomendador]
        ########### Guardan resultados a la bd ##########################
        # Se agrega la fecha a la recomendaciones encontradas
        datos['estado'] = sysvar['estado_proyecto']
        filename = nombre_coleccion

        if (sysvar['estado_proyecto'] == 'tesis'):
            filename = nombre_coleccion + '_' + folder
        fs.put(json.dumps(datos), filename=filename, encoding='utf8')


@app.route('/' + proyecto + '/entrenar/modelo')
def entrenar():
    if modelo['item_item']:
        recomendacion = obtener_modelo_item()

    datos_a_guardar = {"item_item": recomendacion,
                       "fecha": datetime.now()}

    if (sysvar['datos'] == 'articulo'):
        result = db.modelo.insert_one(datos_a_guardar)

    elif (sysvar['datos'] == 'clasificacion'):
        result = db.modelo_clasificacion.insert_one(datos_a_guardar)

    result = db.modelo.insert_one(
        datos_a_guardar
    )
    return str(datos_a_guardar)


###############################################################################
############################### #BUSCAR #####################################
###############################################################################

## CONSULTAS ##
@app.route('/' + proyecto + '/recomendar/usuario/<int:user_id>/',
           methods=["GET"])
def movie_ratings(user_id):
    logging.debug("User %s rating requested for movie %s", user_id)
    # recomendacion_usuario = db.recomendaciones.find({}).sort({ "fecha" : -1 })
    # .limit(1)['normal']['colaborativo'][str(user_id)]
    obtener_recomendacion = \
        json.loads(dumps(db.recomendaciones.find().sort("fecha", -1).limit(1)))[
            0]
    recomendadores_generales = {}

    def obtener_recomendaciones(recomendador):
        try:
            if obtener_recomendacion[recomendador]:
                recomendadores_generales[recomendador] = obtener_recomendacion[
                    recomendador]
        except:
            pass

    obtener_recomendaciones('evento')
    obtener_recomendaciones('empresa')
    obtener_recomendaciones('experto')
    obtener_recomendaciones('proveedor')
    obtener_recomendaciones('frecuencia')
    obtener_recomendaciones('tradicional')

    item1 = obtener_recomendacion.get('colaborativo_item_als', False)
    item2 = obtener_recomendacion.get('colaborativo_item_cosine', False)
    item3 = obtener_recomendacion.get('colaborativo_item_bm25', False)
    usuario = obtener_recomendacion.get('colaborativo_usuario', False)

    if (item1 or item2 or item3 or usuario):

        def cargar_recomendaciones(recomendadores):
            for recomendador in recomendadores:
                try:
                    recomendadores_generales[recomendador] = \
                        obtener_recomendacion[recomendador].get(
                            str(user_id), {})
                except:
                    pass

        cargar_recomendaciones(['colaborativo_usuario',
                                'colaborativo_item_als',
                                'colaborativo_item_cosine',
                                'colaborativo_item_bm25'])

        item1r = recomendadores_generales.get('colaborativo_item_als', False)
        item2r = recomendadores_generales.get('colaborativo_item_cosine', False)
        item3r = recomendadores_generales.get('colaborativo_item_bm25', False)

        usuariosad = recomendadores_generales.get('colaborativo_usuario', False)

        if not bool(item1r) and not bool(item2r) and not bool(item3r):
            recomendadores_generales['random'] = {}
            # obtener una recomendación según historial
            # obtener historial
            historial_usuario = historial_usuarios(user_id, formato=False)
            if bool(historial_usuario):
                # obtener el primer elemento del historial
                item_historial = historial_usuario[0]['item']
                recomendacion_item_raw = recommend_item(item_historial,
                                                        formato=False)
                rec_item = recomendacion_item_raw.get('distancia_item', {})
                if bool(rec_item):
                    recomendadores_generales['random'] = rec_item
            else:

                recomendadores_generales['random'] = six.next(
                    six.itervalues(
                        obtener_recomendacion['colaborativo_item_als']
                    )
                )
    return json.dumps(recomendadores_generales)


@app.route('/' + proyecto + '/recomendar/item/<int:item_id>/', methods=["GET"])
def recommend_item(item_id, formato=True):
    logging.debug("User %s rating requested for movie %s", item_id)

    recomendadores_tradicionales = \
        json.loads(dumps(db.recomendaciones.find().sort("fecha", -1).limit(1)))[
            0]

    obtener_recomendacion = json.loads(dumps(db.modelo.find().sort("fecha",
                                                                   -1).limit(
        1)))[0]['item_item']

    recomendadores_generales = {}

    def obtener_recomendaciones(recomendador):
        try:
            if recomendadores_tradicionales[recomendador]:
                recomendadores_generales[recomendador] = \
                    recomendadores_tradicionales[
                        recomendador]
        except:
            pass

    obtener_recomendaciones('evento')
    obtener_recomendaciones('empresa')
    obtener_recomendaciones('experto')
    obtener_recomendaciones('proveedor')
    obtener_recomendaciones('frecuencia')
    obtener_recomendaciones('tradicional')

    # Filtro colaborativo distancia item

    recomendadores_generales['distancia_item'] = obtener_recomendacion.get(
        str(item_id), {})

    if not (bool(recomendadores_generales['distancia_item'])):
        recomendadores_generales['random'] = {}
        recomendadores_generales['random'] = six.next(
            six.itervalues(obtener_recomendacion))

    if formato:
        recomendadores_generales = json.dumps(recomendadores_generales)
    return recomendadores_generales


@app.route('/' + proyecto + '/historial/usuario/<int:item_id>/',
           methods=['GET'])
def historial_usuarios(item_id, formato=True):
    historial = \
        json.loads(dumps(db.historial.find().sort("fecha", -1).limit(1)))[0]

    historialusuario = historial['historial'].get(str(item_id), {})
    if formato:
        historialusuario = json.dumps(historialusuario)
    return historialusuario


###############################################################################
############################### #EVALUAR #####################################
###############################################################################

@app.route('/' + proyecto + '/evaluar')
def evaluar_proyecto():

    return evaluacion_total_recomendaciones()


if __name__ == '__main__':
    app.run()
