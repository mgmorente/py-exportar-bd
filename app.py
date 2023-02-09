from distutils import config
import json
import os
import psycopg2
from config import config

def exportar_tabla(entidad):
    
    tabla = entidad[0]
    opcion = entidad[1]

    print(f'>> Exportando',tabla)

    # por sucursal
    if opcion == 0:
        sql = f"""SELECT row_to_json({tabla})
            FROM {tabla}
            WHERE sucursal = '{sucursal}' """
    # por usuario
    elif opcion == 1:
       sql = f"""SELECT row_to_json(row) FROM (
                SELECT t.*
                FROM {tabla} t LEFT JOIN usuarios u ON t.usuario = u.usuario
                WHERE u.sucursal = '{sucursal}'
            ) row """ 
    # por poliza
    elif opcion == 2:
       sql = f"""SELECT row_to_json(row) FROM (
                SELECT t.*
                FROM {tabla} t LEFT JOIN polizas p ON p.poliza = t.poliza
                WHERE p.sucursal = '{sucursal}'
            ) row """ 
    # documentos
    elif opcion == 3:
       sql = f"""SELECT row_to_json(row) FROM (
                SELECT t.*
                FROM documentos t 
                    LEFT JOIN clientes c ON c.nif = t.nif 
                    LEFT JOIN polizas p ON p.poliza = t.poliza
                    LEFT JOIN recibos r ON r.recibo = t.recibo
                    LEFT JOIN siniestros s ON s.siniestro = t.siniestro
                WHERE (
                        (t.nif != '' AND c.sucursal = '{sucursal}') 
                        OR (t.poliza != ''  AND p.sucursal = '{sucursal}')
                        OR (t.recibo != ''  AND r.sucursal = '{sucursal}')
                        OR (t.siniestro != '' AND s.sucursal = '{sucursal}') 
                    )
            ) row """ 
    
    cur = conn.cursor()
    cur.execute(f"""COPY ({sql}) to '{absolute_path}/ficheros/{tabla}.json'""")
    conn.commit()
    cur.close()

    return

def copiar_doc(doc):
    print('copiar...',doc["ruta"])

    # fecha = valida_fecha(r["fecha"])
    # grupo = get_carpeta_grupo(r["grupo"])
    # fichero = r["fichero"]

    # # [ Copiar fichero ]
    # path_origen = f'E:/EBROKER-GUAITA-LAST/Documentos/doc/{grupo}/{("/").join(str(r["codigo"]))}/{fichero}'
    # path_destino = f'E:/EBROKER-GUAITA-LAST/Documentos/fix/{fecha.replace("-","/")[:8]}'

    # # Verificar existencia fichero
    # if not os.path.exists(path_origen):
    #     prRed(f'El fichero origen {path_origen} no existe')
    #     return None

    # # Crear carpetas
    # if not os.path.exists(path_destino):
    #     os.makedirs(path_destino, exist_ok=True)

    # # Copiar fichero
    # shutil.copyfile(path_origen, path_destino + "/" + fichero)

    # return

def importFile(name):
    list = []

    with open(f'{absolute_path}/ficheros/docs.json', encoding="utf8") as f:
        for jsonObj in f:
            resourceDict = json.loads(jsonObj)
            list.append(resourceDict)

    return list


# # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # #
# INIT

# read database configuration
params = config()
# connect to the PostgreSQL database
conn = psycopg2.connect(**params)

setting = config(section='setting')
sucursal = setting["sucursal"]

absolute_path = os.path.dirname(__file__)

entidades = [
    ("agenda",1),
    ("clientes",0),
    ("polizas",0),
    ("polizas_autos",2),
    ("polizas_hogar",2),
    ("polizas_salud",2),
    ("polizas_comercio",2),
    ("polizas_pyme",2),
    ("suplementos",2),
    ("recibos",0),
    ("siniestros",0),
    ("docs",3),
]

# exportar tablas a json
for entidad in entidades: 
    exportar_tabla(entidad)

# copiar documentos
print('>> Copiar Docs')
docusList = importFile('docu')

for doc in docusList: 
    copiar_doc(doc)

print(f'>> Fin ...')

