
-- Este sript agrega índices que permiten realizar consultas de manera mas óptima, pueden ser agregados después de iniciar y ejecutar la base de datos.

USE estaciones_servicio;
-- 1) Índice compuesto para búsquedas por provincia + municipio (+ localidad)
CREATE INDEX idx_estacion_prov_mun
ON estacion (provincia, municipio, localidad);

-- 2) Índice para ranking de empresas por tipo de estación (terrestre/maritima)
CREATE INDEX idx_estacion_fuente_empresa
ON estacion (fuente, id_empresa);

-- 3) Índice para precios de un combustible ordenados por precio
CREATE INDEX idx_precio_comb_precio
ON precio (id_combustible, precio);
