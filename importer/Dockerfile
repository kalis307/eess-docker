FROM python:3.11-slim

# instalar dependencias del sistema necesarias
RUN apt-get update && apt-get install -y --no-install-recommends \
    gcc \
    default-libmysqlclient-dev \
    build-essential \
    && rm -rf /var/lib/apt/lists/*

WORKDIR /app
COPY requirements.txt /app/requirements.txt
RUN pip install --no-cache-dir -r requirements.txt

COPY import_eess.py /app/import_eess.py

# punto de entrada: ejecuta el script de importación
CMD ["python", "import_eess.py"]
