CREATE DATABASE IF NOT EXISTS estaciones_servicio CHARACTER SET utf8mb4 COLLATE utf8mb4_unicode_ci;
USE estaciones_servicio;

CREATE TABLE IF NOT EXISTS empresa (
  id INT AUTO_INCREMENT PRIMARY KEY,
  nombre VARCHAR(200) NOT NULL,
  nif VARCHAR(50),
  contacto VARCHAR(255),
  created_at DATETIME DEFAULT CURRENT_TIMESTAMP,
  UNIQUE KEY uq_empresa_nombre (nombre)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4;

CREATE TABLE IF NOT EXISTS estacion (
  id INT AUTO_INCREMENT PRIMARY KEY,
  codigo_externo VARCHAR(100),
  id_empresa INT,
  nombre VARCHAR(255),
  provincia VARCHAR(100),
  municipio VARCHAR(100),
  localidad VARCHAR(100),
  codigo_postal VARCHAR(20),
  direccion VARCHAR(255),
  margen VARCHAR(20),
  latitud DECIMAL(10,7),
  longitud DECIMAL(10,7),
  ultima_actualizacion DATETIME,
  horario VARCHAR(255),
  fuente ENUM('terrestre','maritima') NOT NULL,
  archivo_origen VARCHAR(255),
  created_at DATETIME DEFAULT CURRENT_TIMESTAMP,
  FOREIGN KEY (id_empresa) REFERENCES empresa(id) ON DELETE SET NULL
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4;

CREATE TABLE IF NOT EXISTS combustible (
  id INT AUTO_INCREMENT PRIMARY KEY,
  codigo VARCHAR(80),
  nombre VARCHAR(200) NOT NULL,
  created_at DATETIME DEFAULT CURRENT_TIMESTAMP,
  UNIQUE KEY uq_combustible_nombre (nombre)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4;

CREATE TABLE IF NOT EXISTS precio (
  id INT AUTO_INCREMENT PRIMARY KEY,
  id_estacion INT NOT NULL,
  id_combustible INT NOT NULL,
  precio DECIMAL(10,4) NOT NULL,
  fecha_registro DATETIME,
  created_at DATETIME DEFAULT CURRENT_TIMESTAMP,
  FOREIGN KEY (id_estacion) REFERENCES estacion(id) ON DELETE CASCADE,
  FOREIGN KEY (id_combustible) REFERENCES combustible(id) ON DELETE CASCADE
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4;

CREATE INDEX idx_estacion_provincia ON estacion(provincia);
CREATE INDEX idx_precio_combustible ON precio(id_combustible);
CREATE INDEX idx_precio_estacion ON precio(id_estacion);
