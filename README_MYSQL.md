# Cargar el modelo dimensional a MySQL

Este flujo crea una base de datos MySQL y carga todas las tablas `dim_` y `fact_` desde `data/processed/dimensional`.

## Requisitos
- MySQL 5.7+ (ideal 8.0+)
- Python 3.9+
- Paquetes:
  - mysql-connector-python
  - python-dotenv
  - pandas

## Configuración
1) Copia `.env.example` a `.env` y ajusta las credenciales:

```
MYSQL_HOST=localhost
MYSQL_PORT=3306
MYSQL_USER=root
MYSQL_PASSWORD=changeme
MYSQL_DB=enacom_obs
```

2) Opcional: crea la base y el usuario en MySQL si no existen.

## Ejecución
Ejecuta el cargador:

```
python pipelines/load_to_mysql.py
```

Esto:
- Creará la base `enacom_obs` (o la que definas)
- Creará tablas por cada CSV
- Insertará los datos en lotes

## Esquema y convenciones
- Tablas de hechos: `fact_*`
- Dimensiones: `dim_*`
- Claves: `*_id`
- Tiempo: columnas normalizadas `anio`, `trimestre`, `mes` (sin `tiempo_id`).

## Notas
- Los tipos MySQL se infieren automáticamente de los datos.
- Se crean índices en FKs (`*_id`) y en columnas de tiempo cuando existan.
- Si cambian columnas de los CSV, vuelve a ejecutar el script. Puedes `DROP TABLE` si requieres recreación limpia.
