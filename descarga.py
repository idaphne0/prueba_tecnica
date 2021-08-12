import wget
import zipfile
from datetime import date
from dateutil.relativedelta import relativedelta
import os
from io import StringIO
import pandas as pd
import shutil 
from src import (
  #Database,
  connect_to_db,
  wait_select,
  load_yml_env
)

load_yml_env('local/.env.yml')

shutil.rmtree('descarga')
shutil.rmtree('csv')

os.mkdir('descarga/')
os.mkdir('csv/')

numero_meses = -1
url_base = 'https://transparenciachc.blob.core.windows.net/lic-da/'
fecha_fin = date.today()

if date.today().strftime('%d') == 1:
    fecha_fin = fecha_fin + relativedelta(months=-1)

fecha_inicio = fecha_fin + relativedelta(months=numero_meses)

con = connect_to_db()
wait_select(con)

while fecha_inicio <= fecha_fin:
    nombre_archivo = fecha_inicio.strftime('%Y') + '-' + str(int(fecha_inicio.strftime('%m'))) + '.zip'
    url_final = url_base + nombre_archivo
    wget.download(url_final, 'descarga/' + nombre_archivo)

    ruta_zip = 'descarga/' + nombre_archivo
    ruta_extraccion = 'csv/' + nombre_archivo.replace('.zip', '')
    password = None
    archivo_zip = zipfile.ZipFile(ruta_zip, "r")
    try:
        print(archivo_zip.namelist())
        archivo_zip.extractall(pwd=password, path=ruta_extraccion)
    except:
        pass
    archivo_zip.close()

    ruta_csv = 'csv/' + nombre_archivo.replace('.zip', '') + '/lic_' + nombre_archivo.replace('.zip', '.csv')
    data = pd.read_csv(ruta_csv, sep=';', encoding='latin-1', dtype=str)
    df = pd.DataFrame(data)
    df['nombre_archivo'] = 'lic_' + nombre_archivo.replace('.zip', '.csv')

    binary_file = StringIO()
    df.to_csv(binary_file, index=False)
    binary_file.seek(0)

    cur = con.cursor()

    sql = f'''
    COPY public.licitacion_tmp
    FROM stdin
    WITH (
        FORMAT 'csv',
        FREEZE 'false',
        DELIMITER ',',
        NULL '',
        HEADER 'true',
        QUOTE '"',
        ESCAPE '\\',
        ENCODING 'utf-8'
    )
    '''
    cur.copy_expert(sql = sql, file = binary_file, size = 8192)
    con.commit()
    print('Carga completa del archivo: ---->' + 'lic_' + nombre_archivo.replace('.zip', '.csv'))
    fecha_inicio += relativedelta(months=1)


sql_sp = f'''
CALL public.carga_licitacion();
'''

with con.cursor() as cursor:
    cursor.execute(sql_sp)

con.commit()
cur.close()
con.close()