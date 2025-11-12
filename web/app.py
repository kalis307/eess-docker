# web/app.py
# -*- coding: utf-8 -*-
from flask import Flask, render_template, request
import mysql.connector
import os
import math

app = Flask(__name__, template_folder="templates", static_folder="static")

# Configuración desde variables de entorno
DB_HOST = os.getenv('MYSQL_HOST', 'db')
DB_PORT = int(os.getenv('MYSQL_PORT', 3306))
DB_USER = os.getenv('MYSQL_USER', 'eess_user')
DB_PASS = os.getenv('MYSQL_PASSWORD', 'eess_pass')
DB_NAME = os.getenv('MYSQL_DB', 'estaciones_servicio')

PAGE_SIZE = 20

def get_conn():
    return mysql.connector.connect(
        host=DB_HOST,
        port=DB_PORT,
        user=DB_USER,
        password=DB_PASS,
        database=DB_NAME,
        autocommit=True
    )

@app.route('/')
def index():
    conn = get_conn()
    cur = conn.cursor()
    try:
        # obtenemos sin LIMIT para no truncar la lista
        cur.execute("SELECT DISTINCT provincia FROM estacion WHERE provincia IS NOT NULL")
        provincias_raw = [r[0] for r in cur.fetchall()]

        cur.execute("SELECT DISTINCT nombre FROM empresa WHERE nombre IS NOT NULL")
        empresas_raw = [r[0] for r in cur.fetchall()]

        cur.execute("SELECT DISTINCT nombre FROM combustible WHERE nombre IS NOT NULL")
        combustibles_raw = [r[0] for r in cur.fetchall()]
    finally:
        cur.close()
        conn.close()

    # limpieza: quitar espacios laterales y comillas raras, y filtrar None
    def clean_list(raw):
        cleaned = []
        for v in raw:
            if v is None:
                continue
            s = str(v).strip()
            # eliminar comillas iniciales/finales extra (", ', « »)
            if (s.startswith('"') and s.endswith('"')) or (s.startswith("'") and s.endswith("'")):
                s = s[1:-1].strip()
            if s.startswith('«') and s.endswith('»'):
                s = s[1:-1].strip()
            if s:
                cleaned.append(s)
        # deduplicate preserving alphabetic order (case-insensitive)
        unique = sorted(set(cleaned), key=lambda x: x.lower())
        return unique

    provincias = clean_list(provincias_raw)
    empresas = clean_list(empresas_raw)
    combustibles = clean_list(combustibles_raw)

    return render_template('index.html', provincias=provincias, empresas=empresas, combustibles=combustibles)

# Ruta genérica para listado con filtros y paginación
@app.route('/buscar', methods=['GET'])
def buscar():
    page = max(1, int(request.args.get('page', 1)))
    provincia = request.args.get('provincia', None)
    empresa = request.args.get('empresa', None)
    combustible = request.args.get('combustible', None)
    fuente = request.args.get('fuente', None)  # 'terrestre' o 'maritima'
    sort = request.args.get('sort', 'precio_asc')  # precio_asc, precio_desc

    select_cols = ("s.id, s.provincia, s.municipio, s.localidad, s.direccion, s.latitud, s.longitud, "
                   "e.nombre as empresa, s.margen, p.precio, c.nombre as combustible, s.fuente")
    sql_from = (f"FROM precio p "
           f"JOIN combustible c ON p.id_combustible = c.id "
           f"JOIN estacion s ON p.id_estacion = s.id "
           f"LEFT JOIN empresa e ON s.id_empresa = e.id ")

    where_clauses = []
    params = []

    if provincia:
        where_clauses.append("s.provincia = %s")
        params.append(provincia)
    if empresa:
        where_clauses.append("e.nombre = %s")
        params.append(empresa)
    if combustible:
        where_clauses.append("c.nombre = %s")
        params.append(combustible)
    if fuente:
        where_clauses.append("s.fuente = %s")
        params.append(fuente)

    where_sql = ("WHERE " + " AND ".join(where_clauses)) if where_clauses else ""
    order_sql = "ORDER BY p.precio ASC" if sort == 'precio_asc' else "ORDER BY p.precio DESC"

    # conteo total (ejecutar con o sin parámetros según sea necesario)
    count_sql = "SELECT COUNT(*) " + sql_from + where_sql
    conn = get_conn()
    cur = conn.cursor()
    try:
        if params:
            cur.execute(count_sql, tuple(params))
        else:
            cur.execute(count_sql)
        row = cur.fetchone()
        total = row[0] if row and len(row) > 0 and row[0] is not None else 0

        # paginado
        offset = (page - 1) * PAGE_SIZE
        query = f"SELECT {select_cols} {sql_from} {where_sql} {order_sql} LIMIT %s OFFSET %s"
        if params:
            final_params = tuple(params + [PAGE_SIZE, offset])
            cur.execute(query, final_params)
        else:
            cur.execute(query, (PAGE_SIZE, offset))

        fetched = cur.fetchall()
        cols = [d[0] for d in cur.description] if cur.description else []
        rows = [dict(zip(cols, r)) for r in fetched]
    finally:
        cur.close()
        conn.close()

    base_args = {k: v for k, v in request.args.items() if k != 'page'}
    total_pages = max(1, math.ceil(total / PAGE_SIZE))

    return render_template('resultados.html',
                           title="Resultados de búsqueda",
                           rows=rows,
                           columns=['provincia','municipio','localidad','direccion','empresa','combustible','margen','precio','latitud','longitud','fuente'],
                           page=page,
                           total_pages=total_pages,
                           base_args=base_args,
                           total=total)

# Consulta A: empresa con más estaciones (terrestres o marítimas)
# Reemplaza únicamente la función empresa_mayor en web/app.py por este bloque

# Sustituye la función empresa_mayor actual por esta
@app.route('/empresa_mayor', methods=['GET'])
def empresa_mayor():
    """
    Devuelve lista de empresas con el número de estaciones (total).
    Normaliza la salida para que la plantilla muestre 'empresa' y 'total'.
    """
    fuente = request.args.get('fuente', 'terrestre')
    page = max(1, int(request.args.get('page', 1)))

    conn = get_conn()
    cur = conn.cursor(dictionary=True)
    try:
        q = """
        SELECT e.nombre AS empresa, COUNT(*) AS total
        FROM empresa e
        JOIN estacion s ON e.id = s.id_empresa
        WHERE s.fuente = %s
        GROUP BY e.id
        ORDER BY total DESC
        """
        cur.execute(q, (fuente,))
        rows_raw = cur.fetchall()

        # normalizar clave 'total' y construir filas seguras para la plantilla
        rows = []
        for r in rows_raw:
            rows.append({
                'empresa': r.get('empresa'),
                'total': int(r.get('total') or 0),
                # rellenamos campos opcionales para evitar huecos en la plantilla
                'provincia': None, 'municipio': None, 'localidad': None,
                'direccion': None, 'combustible': None, 'margen': None,
                'precio': None, 'latitud': None, 'longitud': None,
                'fuente': fuente
            })

        total = len(rows)
        total_pages = max(1, math.ceil(total / PAGE_SIZE))
        start = (page - 1) * PAGE_SIZE
        page_rows = rows[start:start + PAGE_SIZE]
    finally:
        cur.close()
        conn.close()

    base_args = {k: v for k, v in request.args.items() if k != 'page'}
    return render_template('resultados.html',
                           title=f"Empresas con más estaciones ({fuente})",
                           columns=['empresa', 'total'],
                           rows=page_rows,
                           page=page,
                           total_pages=total_pages,
                           base_args=base_args,
                           total=total)



# Consulta C: Gasolina 95 E5 en Comunidad de Madrid
@app.route('/gas95_madrid')
def gas95_madrid():
    provincia = request.args.get('provincia', 'Madrid')
    page = max(1, int(request.args.get('page', 1)))
    offset = (page-1)*PAGE_SIZE
    conn = get_conn()
    cur = conn.cursor(dictionary=True)
    try:
        q = ("""
            SELECT s.provincia, s.municipio, s.localidad, s.direccion, e.nombre AS empresa, s.margen, p.precio, s.latitud, s.longitud
            FROM precio p
            JOIN combustible c ON p.id_combustible = c.id
            JOIN estacion s ON p.id_estacion = s.id
            LEFT JOIN empresa e ON s.id_empresa = e.id
            WHERE c.nombre LIKE %s AND s.provincia=%s
            ORDER BY p.precio ASC
        """)
        cur.execute(q, ('%Gasolina 95 E5%', provincia))
        rows_all = cur.fetchall()
        total = len(rows_all)
        total_pages = max(1, math.ceil(total / PAGE_SIZE))
        start = offset
        end = start + PAGE_SIZE
        rows = rows_all[start:end]
    finally:
        cur.close()
        conn.close()

    base_args = {k: v for k, v in request.args.items() if k != 'page'}
    return render_template('resultados.html',
                           title=f"Gasolina 95 E5 - {provincia}",
                           rows=rows,
                           columns=['provincia','municipio','localidad','direccion','empresa','margen','precio','latitud','longitud'],
                           page=page,
                           total_pages=total_pages,
                           base_args=base_args,
                           total=total)

# Consulta D: Gasóleo A dentro de X km de un punto
def haversine_km(lat1, lon1, lat2, lon2):
    # lat/lon en grados -> devuelve km (aprox)
    R = 6371.0
    phi1 = math.radians(lat1)
    phi2 = math.radians(lat2)
    dphi = math.radians(lat2 - lat1)
    dlambda = math.radians(lon2 - lon1)
    a = math.sin(dphi/2.0)**2 + math.cos(phi1)*math.cos(phi2)*math.sin(dlambda/2.0)**2
    c = 2 * math.atan2(math.sqrt(a), math.sqrt(1-a))
    return R * c

@app.route('/gasoleo_cercano', methods=['GET'])
def gasoleo_cercano():
    """
    Buscar estaciones con 'Gasóleo A' dentro de max_km de (lat, lon).
    - Acepta lat/lon con coma o punto como separador decimal.
    - Acepta varios nombres de parámetros: lat/lon, latitude/longitude, lat0/lon0, lng.
    - Si no se proporcionan coordenadas, usa por defecto Albacete (38.9943, -1.8572) para no romper la UI.
    """
    # helper: parsear float tolerante
    def parse_coord_val(s):
        if s is None:
            return None
        s2 = str(s).strip()
        if s2 == '':
            return None
        s2 = s2.replace(',', '.')
        try:
            return float(s2)
        except ValueError:
            return None

    # admitir varios nombres de parámetro
    raw_lat_candidates = [
        request.args.get('lat'),
        request.args.get('latitude'),
        request.args.get('lat0'),
    ]
    raw_lon_candidates = [
        request.args.get('lon'),
        request.args.get('longitude'),
        request.args.get('lon0'),
        request.args.get('lng'),
    ]

    lat0 = None
    lon0 = None
    for v in raw_lat_candidates:
        if lat0 is None:
            lat0 = parse_coord_val(v)
    for v in raw_lon_candidates:
        if lon0 is None:
            lon0 = parse_coord_val(v)

    # logging para depuración
    app.logger.debug("gasoleo_cercano: parámetros recibidos: %s", dict(request.args))

    # si no vienen, usar valores por defecto (Albacete) para evitar error 400 en UI
    if lat0 is None or lon0 is None:
        app.logger.debug("gasoleo_cercano: lat/lon no válidos o ausentes, usando valor por defecto (Albacete)")
        lat0, lon0 = 38.9943, -1.8572

    # km tolerante
    km_raw = request.args.get('km', '10')
    try:
        km_val = float(str(km_raw).replace(',', '.').strip())
    except Exception:
        km_val = 10.0

    # página
    try:
        page = max(1, int(request.args.get('page', 1)))
    except Exception:
        page = 1

    # consulta: obtener todas las filas relevantes (filtrado por combustible en SQL)
    conn = get_conn()
    cur = conn.cursor(dictionary=True)
    try:
        q = """
        SELECT s.id as id_estacion, s.provincia, s.municipio, s.localidad, s.direccion,
               e.nombre as empresa, s.margen, p.precio, s.latitud, s.longitud, s.fuente
        FROM precio p
        JOIN combustible c ON p.id_combustible = c.id
        JOIN estacion s ON p.id_estacion = s.id
        LEFT JOIN empresa e ON s.id_empresa = e.id
        WHERE (c.nombre LIKE '%%Gasóleo A%%' OR c.nombre LIKE '%%Gasoil A%%' OR c.nombre LIKE '%%Diesel A%%')
        """
        cur.execute(q)
        fetched = cur.fetchall()

        rows_all = []
        for r in fetched:
            # parsear coordenadas almacenadas (pueden venir como strings con comas)
            lat_v = None
            lon_v = None
            try:
                lat_v = None if r.get('latitud') in (None, '') else float(str(r.get('latitud')).replace(',', '.'))
            except Exception:
                lat_v = None
            try:
                lon_v = None if r.get('longitud') in (None, '') else float(str(r.get('longitud')).replace(',', '.'))
            except Exception:
                lon_v = None

            distancia = None
            if lat_v is not None and lon_v is not None:
                distancia = haversine_km(lat0, lon0, lat_v, lon_v)

            # si la distancia es None (sin coords) lo mantenemos, o si está dentro de km_val
            if distancia is None or distancia <= km_val:
                rows_all.append({
                    'provincia': r.get('provincia'),
                    'municipio': r.get('municipio'),
                    'localidad': r.get('localidad'),
                    'direccion': r.get('direccion'),
                    'empresa': r.get('empresa'),
                    'combustible': 'Gasóleo A',
                    'margen': r.get('margen'),
                    'precio': r.get('precio'),
                    'latitud': r.get('latitud'),
                    'longitud': r.get('longitud'),
                    'fuente': r.get('fuente'),
                    'distancia_km': round(distancia, 3) if distancia is not None else None
                })

        # ordenar por distancia ascendente (None al final)
        rows_all.sort(key=lambda x: (float('inf') if x['distancia_km'] is None else x['distancia_km']))
        total = len(rows_all)
        total_pages = max(1, math.ceil(total / PAGE_SIZE))
        start = (page - 1) * PAGE_SIZE
        page_rows = rows_all[start:start + PAGE_SIZE]
    finally:
        cur.close()
        conn.close()

    base_args = {k: v for k, v in request.args.items() if k != 'page'}
    return render_template('resultados.html',
                           title=f"Gasóleo A dentro de {km_val} km",
                           rows=page_rows,
                           columns=['provincia','municipio','localidad','direccion','empresa','combustible','margen','precio','latitud','longitud','fuente','distancia_km'],
                           page=page,
                           total_pages=total_pages,
                           base_args=base_args,
                           total=total)

# Consulta E: estación marítima con Gasolina 95 E5 más cara
@app.route('/gas95_maritima_top', methods=['GET'])
def gas95_maritima_top():
    page = max(1, int(request.args.get('page', 1)))
    conn = get_conn()
    cur = conn.cursor(dictionary=True)
    try:
        q = """
        SELECT s.provincia, s.municipio, s.localidad, s.direccion, e.nombre AS empresa, p.precio, s.latitud, s.longitud
        FROM precio p
        JOIN combustible c ON p.id_combustible = c.id
        JOIN estacion s ON p.id_estacion = s.id
        LEFT JOIN empresa e ON s.id_empresa = e.id
        WHERE c.nombre LIKE %s
          AND s.fuente = 'maritima'
        ORDER BY p.precio DESC
        """
        cur.execute(q, ('%Gasolina 95 E5%',))
        rows_all = cur.fetchall()
        total = len(rows_all)
        total_pages = max(1, math.ceil(total / PAGE_SIZE))
        start = (page-1)*PAGE_SIZE
        end = start + PAGE_SIZE
        rows = rows_all[start:end]
    finally:
        cur.close()
        conn.close()

    base_args = {k: v for k, v in request.args.items() if k != 'page'}
    return render_template('resultados.html',
                           title=f"Gasolina 95 E5 (marítimas) — más caras",
                           columns=['provincia','municipio','localidad','direccion','empresa','precio','latitud','longitud'],
                           rows=rows,
                           page=page,
                           total_pages=total_pages,
                           base_args=base_args,
                           total=total)

# Endpoint para el diagrama ER (mermaid)
@app.route('/esquema')
def esquema():
    mermaid_text = """
erDiagram
    EMPRESA ||--o{ ESTACION : opera
    ESTACION ||--o{ PRECIO : ofrece
    COMBUSTIBLE ||--o{ PRECIO : correspondeA

    EMPRESA {
        int id
        varchar nombre
    }

    ESTACION {
        int id
        varchar provincia
        varchar municipio
        varchar localidad
        varchar direccion
        varchar codigo_postal
        char margen
        float latitud
        float longitud
        datetime ultima_actualizacion
        varchar horario
        varchar fuente
    }

    COMBUSTIBLE {
        int id
        varchar nombre
    }

    PRECIO {
        int id
        int id_estacion
        int id_combustible
        float precio
        datetime fecha_actualizacion
    }
"""
    return render_template('esquema.html', mermaid=mermaid_text)

if __name__ == '__main__':
    app.run(host='0.0.0.0', port=5000)
