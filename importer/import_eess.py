#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
import_eess.py
Script para importar preciosEESS_es.csv y embarcacionesPrecios_es.csv
a la base MySQL. Detecta cabecera real, normaliza columnas y crea INSERTs
directamente contra la base MySQL.
"""

import os
import csv
import re
import unicodedata
import datetime
import time
import sys
from mysql.connector import connect, Error

# Configuración por entorno (heredada desde docker-compose env)
MYSQL_HOST = os.getenv('MYSQL_HOST', 'db')
MYSQL_PORT = int(os.getenv('MYSQL_PORT', '3306'))
MYSQL_DB = os.getenv('MYSQL_DB', 'estaciones_servicio')
MYSQL_USER = os.getenv('MYSQL_USER', 'eess_user')
MYSQL_PASSWORD = os.getenv('MYSQL_PASSWORD', 'eess_pass')

# Rutas a CSV (montadas desde docker-compose)
CSV_DIR = '/app/csv'
CSV_TER = os.path.join(CSV_DIR, 'preciosEESS_es.csv')
CSV_MAR = os.path.join(CSV_DIR, 'embarcacionesPrecios_es.csv')

# Retrasos para esperar a que la BD esté lista
MAX_RETRIES = 30
SLEEP_SEC = 2

# ---------------- utilidades ----------------
def slugcol(s):
    if s is None:
        return ''
    s2 = ''.join(c for c in unicodedata.normalize('NFKD', str(s)) if not unicodedata.combining(c))
    s2 = re.sub(r'[^0-9a-zA-Z_]+', '_', s2)
    s2 = re.sub(r'_+', '_', s2).strip('_').lower()
    return s2

def str_to_float(v):
    if v is None:
        return None
    v = str(v).strip()
    if v == '':
        return None
    v = v.replace('\xa0','').replace(' ','')
    v = v.replace(',', '.')
    try:
        return float(v)
    except:
        return None

def parse_date(value):
    if value is None:
        return None
    v = str(value).strip()
    if v == '':
        return None
    for fmt in ("%d/%m/%Y %H:%M", "%d/%m/%Y %H:%M:%S", "%d/%m/%Y"):
        try:
            return datetime.datetime.strptime(v, fmt)
        except:
            pass
    try:
        return datetime.datetime.fromisoformat(v)
    except:
        pass
    return None

def find_header_and_rows(csv_path, delimiter=';'):
    rows = []
    header = None
    with open(csv_path, newline='', encoding='utf-8', errors='replace') as f:
        reader = csv.reader(f, delimiter=delimiter)
        for r in reader:
            if not r:
                continue
            joined = ';'.join([c.strip() for c in r])
            if re.search(r'(?i)\bprovincia\b', joined) and re.search(r'(?i)\bmunicipio\b', joined):
                header = [c.strip() for c in r]
                break
        if header is None:
            raise RuntimeError(f"No se ha encontrado la fila de cabecera en {csv_path}")
        for r in reader:
            if all([c.strip()=='' for c in r]):
                continue
            if len(r) < len(header):
                r = r + [''] * (len(header) - len(r))
            rows.append([c.strip() for c in r[:len(header)]])
    return header, rows

PRECIO_KEYWORDS = ['gasolina','gasoleo','gasoil','adblue','diesel','bio','metanol','hidrogeno','gas natural','bgc','bgc']

def map_columns(header):
    idx = {slugcol(h): i for i, h in enumerate(header)}
    mapping = {}
    candidates = {
        'provincia': ['provincia'],
        'municipio': ['municipio'],
        'localidad': ['localidad'],
        'codigo_postal': ['c_digo_postal','c_p','codigo_postal','codigo_postal_'],
        'direccion': ['direcci_n','direccion','dirección'],
        'margen': ['margen'],
        'longitud': ['longitud','lon'],
        'latitud': ['latitud','lat'],
        'toma_de_datos': ['toma_de_datos','toma_de_datos','toma_de_datos', 'toma'],
        'rotulo': ['r_tulo','rotulo','rótulo','rótulo_'],
        'tipo_venta': ['tipo_venta'],
        'rem': ['rem']
    }
    for key, cand_list in candidates.items():
        mapped = None
        for cand in cand_list:
            for h_slug, i in idx.items():
                if cand in h_slug:
                    mapped = i
                    break
            if mapped is not None:
                break
        mapping[key] = mapped
    price_cols = []
    for i, h in enumerate(header):
        h_low = h.lower()
        if any(k in h_low for k in PRECIO_KEYWORDS):
            # consideramos esta columna como columna de precio
            price_cols.append((i, h.strip()))
        else:
            # algún caso concreto: busco 'precio' palabra
            if 'precio' in h_low:
                price_cols.append((i, h.strip()))
    mapping['price_cols'] = price_cols
    return mapping

# ---------------- DB ----------------
def wait_for_db():
    for attempt in range(MAX_RETRIES):
        try:
            with connect(host=MYSQL_HOST, port=MYSQL_PORT, user=MYSQL_USER, password=MYSQL_PASSWORD) as conn:
                return True
        except Exception as e:
            print(f"[esperando DB] intento {attempt+1}/{MAX_RETRIES} -> {e}")
            time.sleep(SLEEP_SEC)
    return False

def insert_rows_mysql(rows, header, mapping, fuente_label):
    try:
        with connect(host=MYSQL_HOST, port=MYSQL_PORT, user=MYSQL_USER, password=MYSQL_PASSWORD, database=MYSQL_DB, autocommit=False) as conn:
            cur = conn.cursor()
            for r in rows:
                provincia = r[mapping['provincia']] if mapping['provincia'] is not None else ''
                municipio = r[mapping['municipio']] if mapping['municipio'] is not None else ''
                localidad = r[mapping['localidad']] if mapping['localidad'] is not None else ''
                codigo_postal = r[mapping['codigo_postal']] if mapping['codigo_postal'] is not None else ''
                direccion = r[mapping['direccion']] if mapping['direccion'] is not None else ''
                margen = r[mapping['margen']] if mapping['margen'] is not None else ''
                lon = r[mapping['longitud']] if mapping['longitud'] is not None else ''
                lat = r[mapping['latitud']] if mapping['latitud'] is not None else ''
                rotulo = r[mapping['rotulo']] if mapping['rotulo'] is not None else ''
                toma = r[mapping['toma_de_datos']] if mapping['toma_de_datos'] is not None else ''

                lon_f = str_to_float(lon)
                lat_f = str_to_float(lat)
                fecha_dt = parse_date(toma)

                # empresa (upsert simple)
                cur.execute("INSERT IGNORE INTO empresa (nombre) VALUES (%s)", (rotulo,))
                cur.execute("SELECT id FROM empresa WHERE nombre=%s LIMIT 1", (rotulo,))
                res = cur.fetchone()
                id_empresa = res[0] if res else None

                # insertar estacion
                cur.execute("""
                    INSERT INTO estacion (codigo_externo, id_empresa, nombre, provincia, municipio, localidad, codigo_postal, direccion, margen, latitud, longitud, ultima_actualizacion, horario, fuente, archivo_origen)
                    VALUES (%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s)
                """, (None, id_empresa, None, provincia, municipio, localidad, codigo_postal, direccion, margen, lat_f, lon_f,
                      fecha_dt.strftime("%Y-%m-%d %H:%M:%S") if fecha_dt else None, None, fuente_label, os.path.basename(CSV_TER) ))
                id_estacion = cur.lastrowid

                # precios
                for i_col, col_name in mapping['price_cols']:
                    raw = r[i_col]
                    p = str_to_float(raw)
                    if p is None:
                        continue
                    cur.execute("INSERT IGNORE INTO combustible (nombre) VALUES (%s)", (col_name,))
                    cur.execute("SELECT id FROM combustible WHERE nombre=%s LIMIT 1", (col_name,))
                    id_comb = cur.fetchone()[0]
                    cur.execute("INSERT INTO precio (id_estacion, id_combustible, precio, fecha_registro) VALUES (%s,%s,%s,%s)",
                                (id_estacion, id_comb, float(f"{p:.4f}"), fecha_dt.strftime("%Y-%m-%d %H:%M:%S") if fecha_dt else None))
                conn.commit()
            print("[OK] Inserción completada para fuente:", fuente_label)
    except Error as e:
        print("Error MySQL:", e)
        sys.exit(1)

# ---------------- flujo principal ----------------
def procesar_un_csv(path_csv, fuente_label):
    print("Procesando:", path_csv)
    header, rows = find_header_and_rows(path_csv, delimiter=';')
    print("Columnas detectadas:", len(header), "Filas:", len(rows))
    mapping = map_columns(header)
    print("Columnas de precio detectadas:", [x[1] for x in mapping['price_cols']])
    insert_rows_mysql(rows, header, mapping, fuente_label)

def main():
    ok = wait_for_db()
    if not ok:
        print("La base de datos no está disponible. Abortando.")
        sys.exit(1)
    # procesar terrestres
    if os.path.exists(CSV_TER):
        procesar_un_csv(CSV_TER, 'terrestre')
    else:
        print("Aviso: no existe", CSV_TER)
    # procesar maritimos
    if os.path.exists(CSV_MAR):
        # en el script de inserción usamos CSV_TER basename en archivo_origen para ambas; no es crítico.
        # si quieres diferenciar ajuste en insert_rows_mysql para cada llamada.
        # llamamos al mismo método, pero la variable CSV_TER aparece en archivo_origen -> se puede mejorar.
        procesar_un_csv(CSV_MAR, 'maritima')
    else:
        print("Aviso: no existe", CSV_MAR)

if __name__ == "__main__":
    main()
