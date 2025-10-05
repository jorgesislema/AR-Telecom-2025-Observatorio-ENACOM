# Observatorio AR-Telecom-2025 ENACOM# AR-Telecom-2025-Observatorio-ENACOM

Observatorio 2025 de telecom en Argentina con datos ENACOM: pipeline y dashboard para medir penetración, velocidades y brechas por provincia (T1/2025)

Proyecto de análisis de datos de telecomunicaciones en Argentina basado en datos públicos de ENACOM.

# AR-Telecom-2025 Observatorio ENACOM

## 🎯 Objetivo

Proyecto de ciencia de datos para análisis de telecomunicaciones en Argentina. Incluye ingestión, transformación, validación, visualización y machine learning.

Realizar análisis exploratorio y ETL (Extract, Transform, Load) de los datos de telecomunicaciones de Argentina para generar insights sobre el sector.

## Conversión a CSV

## 📁 Estructura del ProyectoConvertimos todos los archivos `.xlsx` ubicados en `data/raw/` a formato `.csv` en `data/raw/enacom/` para estandarizar el pipeline.



```Ejecutamos:

AR-Telecom-2025-Observatorio-ENACOM/```

├── data/python pipelines/exportamos_csv.py

│   ├── raw/enacom/          # Datos originales en formato CSV```

│   └── processed/           # Datos limpios y transformados

├── notebooks/## Dataset procesado y diccionario

│   └── eda_etl_completo.ipynb    # Análisis exploratorio y ETLGeneramos dataset unificado y artefactos de documentación:

├── pipelines/```

│   └── etl_principal.py     # Script principal de transformaciónpython pipelines/generamos_diccionario.py

├── tests/```

│   └── test_etl.py         # Tests básicos de validaciónEsto produce:

├── requirements.txt        # Dependencias del proyecto- `data/processed/datos_unificados.parquet`

└── README.md              # Este archivo- `docs/diccionario_datos.csv`

```- `docs/catalogo_valores.csv`



## 🚀 Inicio RápidoDesde el notebook `notebooks/etl.ipynb` también exportamos un diccionario básico (`docs/diccionario_datos_basico.csv`).



### 1. Instalación de dependencias## Dimensiones y hechos iniciales

Creamos dimensiones base y un fact de accesos de internet:

```bash```

pip install -r requirements.txtpython pipelines/generamos_dimensiones.py

``````

Esto produce:

### 2. Ejecutar pipeline ETL- `data/processed/dim_tiempo.parquet`

- `data/processed/dim_provincia.parquet`

```bash- `data/processed/fact_internet_accesos.parquet`

python pipelines/etl_principal.py

```## Particionado por tecnología (Wide -> Long)

Normalizamos las tecnologías de acceso (adsl, cablemodem, fibra_optica, wireless, otros) a formato largo y generamos:

### 3. Explorar datos```

python pipelines/generamos_particionado.py

Abrir y ejecutar el notebook:```

```bashSalidas:

jupyter notebook notebooks/eda_etl_completo.ipynb- `data/processed/dim_tecnologia.parquet`

```- `data/processed/fact_internet_accesos_tecnologia.parquet`

- Directorio particionado `data/processed/particionado_internet_accesos_tecnologias/` con subcarpetas `anio=YYYY/trimestre=T/`

## 📊 Dominios de Datos

Este particionado permite queries selectivas por año y trimestre sin leer todo el dataset completo, optimizando futuros análisis y cargas a motores tipo DuckDB / Spark.

Los datos incluyen los siguientes dominios:

## Pipelines principales

- **Internet**: Accesos, penetración, tecnologías, velocidades e ingresos- `pipelines/cargamos_enacom.py`

- **Telefonía Fija**: Accesos, penetración e ingresos  - `pipelines/limpiamos_datos.py`

- **Televisión**: Accesos, penetración e ingresos- `pipelines/elaboramos_curacion.py`

- **Comunicaciones Móviles**: Accesos, llamadas, minutos, SMS, penetración- `pipelines/preparamos_ml.py`

- **Mercado Postal**: Facturación, producción, personal ocupado- `pipelines/exportamos_csv.py`

- **Portabilidad**: Datos de portabilidad móvil- `pipelines/generamos_diccionario.py`

- `pipelines/generamos_dimensiones.py`

## 🔄 Proceso ETL

## Reproducibilidad

### Extracción```

- Lectura de archivos CSV desde `data/raw/enacom/`python -m venv venv

- Manejo de errores de codificación y formato./venv/Scripts/Activate.ps1

pip install -r requirements.txt

### Transformaciónpython pipelines/exportamos_csv.py

- Normalización de nombres de columnas a snake_casepython pipelines/generamos_diccionario.py

- Limpieza de datos básica (tipos, valores nulos)python pipelines/generamos_dimensiones.py

- Normalización de nombres de provinciaspython pipelines/generamos_particionado.py

- Conversión de tipos de datos apropiadospython pipelines/exportamos_unificado_csv.py

```

### Carga

- Guardado de datos limpios en `data/processed/`## Exportar dataset unificado a CSV (comprimido)

- Generación de archivo de resumen con metadatosPara compartir el dataset unificado en formato tabular ligero:

```

## 📈 Análisis Disponiblespython pipelines/exportamos_unificado_csv.py

```

El notebook `eda_etl_completo.ipynb` incluye:Genera:

- `data/processed/datos_unificados.csv.gz` (mismas columnas que el parquet)

1. **Configuración del entorno**

2. **Exploración del directorio raw**Ventajas: interoperabilidad (BI externo, hojas de cálculo). Para análisis interno preferir parquet.

3. **Carga e inspección inicial**

4. **Proceso ETL completo**## Dataset Light (últimos 4 trimestres)

5. **Análisis exploratorio de datos (EDA)**Para acelerar el dashboard se genera una versión reducida:

6. **Visualizaciones**```

7. **Análisis estadístico descriptivo**python pipelines/exportamos_unificado_light.py

8. **Detección de valores atípicos**```

9. **Análisis de correlaciones**Produce:

10. **Guardado de datos procesados**- `data/processed/datos_unificados_light.parquet`

- `data/processed/datos_unificados_light.csv.gz`

## 🧪 Tests

Uso en Streamlit: la página `Home.py` detecta y muestra KPIs si existe el parquet light. Si no, mostrará una advertencia con el comando necesario.

Ejecutar tests de validación:

```bash
python -m pytest tests/
```

## 📋 Dependencias Principales

- `pandas`: Manipulación de datos
- `numpy`: Cálculos numéricos
- `matplotlib`: Visualizaciones básicas
- `seaborn`: Visualizaciones estadísticas
- `jupyter`: Entorno de notebooks

## 🤝 Contribuir

1. Fork del repositorio
2. Crear rama para nueva funcionalidad
3. Realizar cambios y tests
4. Crear pull request

## 📄 Licencia

Este proyecto está bajo licencia MIT. Ver archivo LICENSE para detalles.

## 🔗 Fuente de Datos

Los datos provienen de ENACOM (Ente Nacional de Comunicaciones) de Argentina:
- Sitio web: https://www.enacom.gob.ar/
- Datos abiertos y estadísticas oficiales del sector telecomunicaciones

## 📞 Contacto

Para consultas sobre el proyecto, abrir un issue en GitHub.