# AR-Telecom-2025-Observatorio-ENACOM

Observatorio 2025 de telecomunicaciones en Argentina con datos ENACOM: pipeline ETL optimizado que genera directamente un modelo BI curado para Tableau con relaciones claras.

## Objetivo

Generar un modelo de datos BI listo para Tableau con dimensiones y hechos relacionados por IDs consistentes, eliminando la complejidad de archivos intermedios.

## Estructura del Proyecto

```
AR-Telecom-2025-Observatorio-ENACOM/
├── data/
│   ├── raw/enacom/          # Datos originales CSV (33 archivos)
│   └── processed/bi/        # Modelo BI curado (8 archivos)
├── pipelines/
│   └── etl_bi_curado.py     # ETL principal que genera modelo BI
├── tests/
│   └── test_bi_curado.py    # Validacion del modelo BI
├── requirements.txt
└── README.md
```

## Inicio Rapido

### Prerequisitos
- Python 3.8+
- pip

### Instalacion
```bash
git clone https://github.com/jorgesislema/AR-Telecom-2025-Observatorio-ENACOM.git
cd AR-Telecom-2025-Observatorio-ENACOM
pip install -r requirements.txt
```

### Generar Modelo BI
```bash
python pipelines/etl_bi_curado.py
```

### Validar Modelo
```bash
python tests/test_bi_curado.py
```

## Modelo BI para Tableau

El ETL genera 8 archivos en `data/processed/bi/`:

### Dimensiones (4 archivos)

**dim_provincias.csv** (24 filas)
- provincia_id (1-24)
- provincia (BUENOS AIRES, CABA, etc.)
- region (PAMPEANA, NOA, NEA, CUYO, PATAGONIA)
- poblacion_2023, superficie_km2

**dim_tiempo.csv** (48 filas)  
- tiempo_id (1-48)
- anio (2014-2025)
- trimestre (1-4)
- periodo (2014-T1, 2014-T2, etc.)

**dim_velocidades.csv** (7 filas)
- velocidad_id (1-7)
- rango_velocidad (256 kbps, 512 kbps - 1 Mbps, etc.)
- velocidad_min_kbps, velocidad_max_kbps

**dim_tecnologias.csv** (11 filas)
- tecnologia_id (1-11)  
- tecnologia (ADSL, CABLE_MODEM, FIBRA_OPTICA, etc.)
- categoria (INTERNET_FIJO, MOVIL, TELEFONIA_FIJA, TV_PAGA)

### Hechos (4 archivos)

**fact_internet_accesos.csv**
- tiempo_id, provincia_id
- accesos_cada_100_hogares, accesos_cada_100_habitantes

**fact_internet_velocidad.csv**
- tiempo_id, provincia_id, velocidad_id
- mbps

**fact_telefonia_accesos.csv**
- tiempo_id, provincia_id
- hogares, comercial, gobierno, total

**fact_movil_accesos.csv**  
- tiempo_id
- pospago, prepago, operativos

## Relaciones en Tableau

### Joins Principales
- fact_internet_velocidad ↔ dim_provincias: provincia_id
- fact_internet_velocidad ↔ dim_tiempo: tiempo_id  
- fact_internet_velocidad ↔ dim_velocidades: velocidad_id

### Joins Secundarios
- fact_internet_accesos ↔ dim_provincias: provincia_id
- fact_internet_accesos ↔ dim_tiempo: tiempo_id
- fact_telefonia_accesos ↔ dim_provincias: provincia_id
- fact_telefonia_accesos ↔ dim_tiempo: tiempo_id
- fact_movil_accesos ↔ dim_tiempo: tiempo_id

## Ventajas del Modelo

- **IDs Consistentes**: Todas las relaciones usan claves numericas secuenciales
- **Nombres Claros**: Sin caracteres especiales ni espacios
- **Estructura Normalizada**: Dimensiones separadas de hechos
- **Facil Relacionar**: Un solo archivo por concepto
- **Optimizado Tableau**: Diseñado especificamente para herramientas BI

## Testing

9 tests automatizados validan:
- Existencia de archivos
- Estructura de dimensiones
- Integridad referencial
- Tipos de datos
- Rangos de valores

```bash
python tests/test_bi_curado.py
# 9 passed in 0.39s
```

## Datos Procesados

- **4 dimensiones**: 90 registros totales
- **4 hechos**: Miles de registros de metricas
- **Cobertura temporal**: 2014-2025
- **Cobertura geografica**: 24 provincias argentinas

## 📊 Dominios de Datos

Los datos incluyen los siguientes dominios de telecomunicaciones argentinas:

### 🌐 Internet (9 archivos)
- **Accesos BAF**: Banda ancha fija por período y provincia
- **Penetración**: Accesos por cada 100 hogares/habitantes
- **Tecnologías**: ADSL, Cable Modem, Fibra Óptica, Wireless, Otros
- **Velocidades**: Rangos de velocidad por localidad y provincia
- **Ingresos**: Facturación del sector internet

### 📱 Comunicaciones Móviles (6 archivos)
- **Accesos**: Pospago, prepago y total de líneas
- **Ingresos**: Facturación del sector móvil
- **Llamadas y Minutos**: Volumen de llamadas por tipo
- **SMS**: Cantidad de mensajes de texto
- **Penetración**: Accesos cada 100 habitantes
- **Portabilidad**: Cambios de operador

### 📞 Telefonía Fija (5 archivos)
- **Accesos**: Hogares, comercial, gobierno y total
- **Penetración**: Por cada 100 habitantes/hogares
- **Ingresos**: Facturación del sector

### 📺 TV Paga (5 archivos)
- **Accesos**: TV suscripción y satelital por provincia
- **Penetración**: Por cada 100 habitantes/hogares
- **Ingresos**: Facturación del sector TV

### 📮 Mercado Postal (4 archivos)
- **Facturación**: Ingresos por servicios postales
- **Producción**: Volumen de envíos por provincia
- **Personal**: Empleados del sector postal

### 📊 Otros (4 archivos)
- **Resumen General**: Métricas consolidadas del pipeline

## 🏗️ Modelo Dimensional

El proyecto incluye 5 tablas dimensionales para análisis avanzado:

### 📍 dim_provincias.csv (24 registros)
Información geográfica y demográfica de todas las provincias argentinas:
- `provincia_id`, `provincia`, `region`
- `poblacion_2023`, `superficie_km2`, `densidad_poblacional`
- `capital`

### 📅 dim_tiempo.csv (44 registros)
Dimensión temporal para series de tiempo:
- `anio`, `trimestre`, `mes_inicio`, `mes_fin`
- `periodo_completo`, `dias_trimestre`

### 🔧 dim_tecnologias.csv (16 registros)
Catálogo completo de tecnologías de telecomunicaciones:
- `tecnologia_id`, `tecnologia`, `categoria`
- `descripcion`, `velocidad_maxima_mbps`, `tipo_conexion`

### ⚡ dim_velocidades.csv (8 registros)
Rangos y clasificaciones de velocidades de internet:
- `velocidad_id`, `rango_velocidad`
- `velocidad_min_kbps`, `velocidad_max_kbps`
- `categoria`, `uso_recomendado`

### 🏘️ dim_localidades.csv (50 registros)
Muestra representativa de localidades argentinas:
- `localidad_id`, `provincia`, `partido`, `localidad`
- `link_indec`, `tipo_localidad`, `poblacion_estimada`

## 📦 Modelo para Tableau / BI

Tras ejecutar el pipeline principal puedes generar un conjunto optimizado para herramientas de BI (Tableau, Power BI) ejecutando:

```bash
python pipelines/prepare_enacom.py
```

Esto crea la carpeta `data/processed/out/` con:

Dimensiones normalizadas:
- `dim_provincias_norm.csv` (agrega ProvinciaNorm en mayúsculas sin tildes)
- `dim_tiempo_norm.csv` (anio y trimestre tipados como enteros)
- `dim_velocidades_ready.csv` (con columnas: vel_min_kbps, vel_max_kbps, orden, rango_key)
- `dim_tecnologias_ready.csv` (agrega tec_key para joins rápidos)

Tablas de hechos largas (ideal para vistas dinámicas):
- `fact_penetracion_provincias.csv`
- `fact_velocidad_media_provincias.csv`
- `fact_velocidad_numerica_provincias.csv` (incluye Velocidad_kbps)
- `fact_velocidad_rangos_long.csv` (wide -> long de rangos de velocidad)
- `fact_tecnologias_long.csv` (wide -> long de tecnologías)
 
Notas de enriquecimiento reciente:
- `fact_velocidad_media_provincias.csv` ahora incluye `velocidad_id` (join con `dim_velocidades_ready.csv`).
- `fact_velocidad_numerica_provincias.csv` ahora incluye `velocidad_id` (derivado de `Velocidad_kbps`).
- `fact_tecnologias_long.csv` incluye `tecnologia_id` (join con `dim_tecnologias_ready.csv`).

### 🔗 Sugerencias de Joins en Tableau
- Hechos ↔ `dim_provincias_norm.csv`: `ProvinciaNorm` ↔ `ProvinciaNorm`
- Hechos ↔ `dim_tiempo_norm.csv`: (`anio`, `trimestre`)
- `fact_velocidad_rangos_long.csv` ↔ `dim_velocidades_ready.csv`: `rango_velocidad` ↔ `rango_velocidad` o usar `rango_key`
- `fact_tecnologias_long.csv` ↔ `dim_tecnologias_ready.csv`: `tec_key`

### 🧪 Validación
Los tests en `tests/test_etl.py` incluyen verificación de presencia y estructura de estos archivos. Ejecuta:
```bash
python tests/test_etl.py -k Tableau -v
```
para enfocarte en la suite relacionada si lo adaptas.

## 🔄 Proceso ETL

### Transformaciones Aplicadas
1. **Normalización**: Columnas a snake_case
2. **Limpieza**: Datos numéricos y texto estandarizado
3. **Validación**: Provincias normalizadas y estructura verificada
4. **Enriquecimiento**: Tablas dimensionales con metadatos

### Pipeline Automatizado
```bash
# El pipeline procesa automáticamente:
# ✅ 33 archivos CSV originales
# ✅ Genera 33 archivos _clean.csv
# ✅ Valida 5 tablas dimensionales
# ✅ Crea resumen consolidado
# 🎯 Total: 10.67 MB de datos procesados
```

## 🧪 Testing y Calidad

El proyecto incluye 12 tests automatizados:
- **Tests ETL básicos**: Estructura de directorios, archivos procesados
- **Tests de calidad**: Formato de columnas, normalización de provincias
- **Tests dimensionales**: Validación de todas las tablas dim_*
- **Tests de integridad**: IDs únicos, categorías esperadas

```bash
# Ejecutar todos los tests
python tests/test_etl.py
# Resultado esperado: 12 passed in 0.14s
```

## 📈 Casos de Uso

### Para Analistas de Datos
- Análisis de penetración de servicios por provincia
- Evolución temporal de tecnologías (2014-2024)
- Comparativas regionales de velocidades de internet

### Para Business Intelligence
- Dashboards con modelo dimensional completo
- KPIs por región, tecnología y período
- Análisis de mercado y competencia

### Para Data Science
- Predicción de adopción tecnológica
- Clustering de provincias por perfiles de conectividad
- Análisis de correlaciones entre variables

## 💾 Datos Procesados

El directorio `data/processed/` contiene:
- **33 archivos** de datos transaccionales (*_clean.csv)
- **5 tablas** dimensionales (dim_*.csv)
- **1 archivo** de resumen (resumen_datos.csv)
- **Total**: 41 archivos, 10.67 MB

## Fact Table Unificada

Puedes generar una tabla de hechos única consolidando todas las métricas transversales (accesos, ingresos, penetración, velocidad, sms, minutos, etc.) en un formato largo estándar para exploración rápida, modelado o carga en un solo datasource BI.

```bash
python pipelines/build_fact_unificado.py
```
Genera:
- `data/processed/out/fact_unificado_long.csv`
- `data/processed/out/fact_unificado_long.parquet` (requiere pyarrow)

### Esquema de columnas
| Columna | Descripción |
|---------|-------------|
| anio | Año (Int64) |
| trimestre | Trimestre (1-4, nullable si no aplica) |
| mes | Mes (nullable, usado en series mensuales) |
| provincia | Provincia original (nullable) |
| ProvinciaNorm | Provincia normalizada (upper, sin tildes) |
| dominio | Dominio macro (Internet, Movil, TelefoniaFija, TV, Postal, Portabilidad, Otros) |
| subcategoria | Subclasificación derivada del archivo (ej: penetracion, ingresos, velocidad_rangos) |
| variable | Nombre original de la métrica pivotada |
| valor | Valor numérico (float) |
| unidad | Inferida (mbps, accesos, pesos, sms, minutos, ratio, unidades) |
| fuente_archivo | Archivo de origen *_clean.csv |
| velocidad_id | (Solo métricas de velocidad) FK a dim_velocidades_ready |
| tecnologia_id | (Solo métricas de tecnologías) FK a dim_tecnologias_ready |

### Ejemplos de uso (SQL sobre DuckDB / Parquet)
```sql
-- Top 5 tecnologías / métricas por crecimiento promedio anual
SELECT dominio, subcategoria, variable,
       AVG(valor) AS valor_medio
FROM read_parquet('data/processed/out/fact_unificado_long.parquet')
WHERE anio BETWEEN 2019 AND 2024
GROUP BY dominio, subcategoria, variable
ORDER BY valor_medio DESC
LIMIT 5;
```
### Ventajas
- Un solo datasource para dashboards ad-hoc
- Facilita análisis exploratorio rápido (profiling / correlaciones)
- Permite feature store básica para modelos ML futuros

### Consideraciones
- Evita sumar indiscriminadamente variables de diferente naturaleza (ej: accesos vs ingresos)
- Filtra por `dominio` + `subcategoria` antes de agregaciones cruzadas
- `total` no se incluye para evitar doble conteo (puedes modificar script si lo necesitas)

---

## Diccionario de Métricas

Para facilitar la documentación y exploración semántica se genera un diccionario consolidado de todas las métricas disponibles en la tabla de hechos unificada.

Generar:
```
python pipelines/build_diccionario_metricas.py
```
Salida: `data/processed/out/diccionario_metricas.csv`

Columnas principales:
- dominio, subcategoria, variable
- unidad_inferida (heurística basada en nombre / contenido)
- archivos_fuente (lista de archivos *_clean.csv originales)
- anios_min_max (rango temporal detectado)
- observaciones (conteo de filas en fact_unificado)
- cobertura_provincias (número de provincias distintas si aplica)
- nota_heuristica (comentarios automáticos sobre tipo de indicador)

Usos recomendados:
- Curaduría y eliminación de métricas redundantes
- Construcción de catálogos de negocio en BI
- Priorización de indicadores para dashboards

Ejemplo de filtro rápido (DuckDB):
```sql
SELECT dominio, subcategoria, COUNT(*) AS cant_metricas
FROM read_csv('data/processed/out/diccionario_metricas.csv')
GROUP BY 1,2
ORDER BY 1,2;
```

## Exportación a Tableau Hyper (Opcional)

Puedes exportar datasets clave a formato `.hyper` para acelerar la carga en Tableau Desktop/Server.

Script:
```
python pipelines/export_hyper.py
```
Requiere paquete opcional:
```
pip install tableauhyperapi
```
Genera (si existen los CSV):
- `data/processed/out/hyper/fact_unificado.hyper`
- `data/processed/out/hyper/diccionario_metricas.hyper`

Ventajas:
- Ingesta más rápida que CSV en grandes volúmenes
- Tipado explícito de columnas
- Evita pasos de unión manual repetitivos

Si la librería no está instalada, el script mostrará un mensaje instructivo y saldrá sin error.

---

## 🛠️ Tecnologías

- **Python 3.8+**: Lenguaje principal
- **pandas**: Manipulación de datos
- **numpy**: Cálculos numéricos  
- **matplotlib/seaborn**: Visualizaciones
- **pytest**: Testing automatizado
- **jupyter**: Notebooks interactivos

## 📝 Licencia

Este proyecto utiliza datos públicos de ENACOM y está disponible bajo licencia MIT.

## 👥 Contribuciones

Las contribuciones son bienvenidas. Por favor:
1. Fork el repositorio
2. Crea una rama para tu feature
3. Ejecuta los tests
4. Envía un pull request

---

**Dataset actualizado**: 16 de septiembre de 2025  
**Fuente**: ENACOM (Ente Nacional de Comunicaciones)  
**Cobertura temporal**: 2014-2024  
**Granularidad**: Trimestral/Provincial/Local según dataset