# Guía de Relaciones del Modelo Dimensional para Dashboards

## 📊 Resumen del Modelo

El modelo dimensional cuenta con **5 dimensiones** y **26 tablas de hechos** con IDs alfanuméricos de 4 dígitos para facilitar las relaciones en herramientas como Tableau, Power BI y similares.

**Ubicación:** `data/processed/dimensional/`  
**Total de archivos:** 31 CSV  
**Tamaño:** 0.78 MB

---

## 🔑 Dimensiones y sus IDs

### 1. **dim_provincias.csv** (24 registros)
- **ID:** `provincia_id` → **PR01, PR02, PR03...**
- **Columnas:** provincia_id, provincia, region, poblacion_2023, superficie_km2, capital, densidad_poblacional
- **Uso:** Filtros geográficos, mapas, análisis regional

### 2. **dim_tiempo.csv** (48 registros)
- **ID:** `tiempo_id` → **TM01, TM02, TM03...**
- **Columnas:** tiempo_id, anio, trimestre, periodo, mes_inicio, mes_fin, dias_trimestre
- **Uso:** Series temporales, filtros por año/trimestre, tendencias

### 3. **dim_tecnologias.csv** (13 registros)  
- **ID:** `tecnologia_id` → **TEC1, TEC2, TEC3...**
- **Columnas:** tecnologia_id, tecnologia, categoria, descripcion
- **Uso:** Análisis por tipo de tecnología (ADSL, Fibra, 4G, etc.)

### 4. **dim_velocidades.csv** (7 registros)
- **ID:** `velocidad_id` → **VEL1, VEL2, VEL3...**
- **Columnas:** velocidad_id, rango_velocidad, velocidad_min_kbps, velocidad_max_kbps
- **Uso:** Análisis de velocidades de internet, segmentación por rangos

### 5. **dim_servicios.csv** (6 registros)
- **ID:** `servicio_id` → **SRV1, SRV2, SRV3...**
- **Columnas:** servicio_id, servicio, categoria, descripcion
- **Uso:** Segmentación por tipo de servicio (Internet, Telefonía, TV, etc.)

---

## 🔗 Cómo Establecer Relaciones en Tableau

### Relaciones Principales
```
dim_provincias (provincia_id) ← → fact_*_provincias (provincia_id)
dim_tiempo (tiempo_id) ← → fact_* (tiempo_id)
dim_tecnologias (tecnologia_id) ← → fact_*_tecnologias (tecnologia_id)
dim_velocidades (velocidad_id) ← → fact_*_velocidad (velocidad_id)
dim_servicios (servicio_id) ← → fact_*_ingresos (servicio_id)
```

### Tablas de Hechos por Categoría

#### **Internet (11 tablas)**
- `fact_internet_accesos_baf.csv` → **[tiempo_id]**
- `fact_internet_accesos_baf_provincias.csv` → **[tiempo_id, provincia_id]**
- `fact_internet_accesos_penetracion.csv` → **[tiempo_id]**
- `fact_internet_accesos_penetracion_provincias.csv` → **[tiempo_id, provincia_id]**
- `fact_internet_accesos_tecnologias.csv` → **[tiempo_id]**
- `fact_internet_accesos_tecnologias_provincias.csv` → **[tiempo_id, provincia_id]**
- `fact_internet_accesos_velocidad_provincias.csv` → **[tiempo_id, provincia_id]**
- `fact_internet_accesos_velocidad_rangos.csv` → **[tiempo_id]**
- `fact_internet_accesos_velocidad_rangos_provincias.csv` → **[tiempo_id, provincia_id]**
- `fact_internet_ingresos.csv` → **[tiempo_id]**

#### **Móvil (6 tablas)**
- `fact_comunicaciones_moviles_accesos.csv` → **[tiempo_id]**
- `fact_comunicaciones_moviles_ingresos.csv` → **[tiempo_id]**
- `fact_comunicaciones_moviles_llamadas.csv` → **[tiempo_id]**
- `fact_comunicaciones_moviles_minutos.csv` → **[tiempo_id]**
- `fact_comunicaciones_moviles_penetracion.csv` → **[tiempo_id]**
- `fact_comunicaciones_moviles_sms.csv` → **[tiempo_id]**

#### **Telefonía Fija (5 tablas)**
- `fact_telefonia_fija_accesos.csv` → **[tiempo_id]**
- `fact_telefonia_fija_accesos_provincias.csv` → **[tiempo_id, provincia_id]**
- `fact_telefonia_fija_ingresos.csv` → **[tiempo_id]**
- `fact_telefonia_fija_penetracion.csv` → **[tiempo_id]**
- `fact_telefonia_fija_penetracion_provincias.csv` → **[tiempo_id, provincia_id]**

#### **TV (4 tablas)**
- `fact_tv_accesos.csv` → **[tiempo_id]**
- `fact_tv_accesos_provincias.csv` → **[tiempo_id, provincia_id]**
- `fact_tv_ingresos.csv` → **[tiempo_id]**
- `fact_tv_penetracion.csv` → **[tiempo_id]**
- `fact_tv_penetracion_provincias.csv` → **[tiempo_id, provincia_id]**

---

## 📈 Ejemplos de Dashboards

### 1. **Dashboard Regional**
```sql
-- Conectar: fact_internet_accesos_baf_provincias + dim_provincias + dim_tiempo
SELECT 
    dp.provincia,
    dp.region,
    dt.anio,
    dt.trimestre,
    fi.banda_ancha_fija,
    fi.total
FROM fact_internet_accesos_baf_provincias fi
JOIN dim_provincias dp ON fi.provincia_id = dp.provincia_id
JOIN dim_tiempo dt ON fi.tiempo_id = dt.tiempo_id
```

### 2. **Dashboard Temporal**
```sql
-- Evolución de servicios móviles
SELECT 
    dt.anio,
    dt.trimestre,
    fm.pospago,
    fm.prepago,
    fm.operativos
FROM fact_comunicaciones_moviles_accesos fm
JOIN dim_tiempo dt ON fm.tiempo_id = dt.tiempo_id
ORDER BY dt.anio, dt.trimestre
```

### 3. **Dashboard Tecnológico**
```sql
-- Adopción de tecnologías por provincia
SELECT 
    dp.provincia,
    dt.anio,
    ft.adsl,
    ft.cablemodem,
    ft.fibraOptica,
    ft.wireless
FROM fact_internet_accesos_tecnologias_provincias ft
JOIN dim_provincias dp ON ft.provincia_id = dp.provincia_id
JOIN dim_tiempo dt ON ft.tiempo_id = dt.tiempo_id
```

---

## 🎯 Ventajas para BI

### ✅ **IDs Alfanuméricos Únicos**
- **PR01-PR24**: 24 provincias argentinas
- **TM01-TM48**: 48 períodos trimestrales (2013-2024)
- **TEC1-TEC13**: 13 tecnologías de telecomunicaciones
- **VEL1-VEL7**: 7 rangos de velocidad
- **SRV1-SRV6**: 6 tipos de servicios

### ✅ **Estructura Star Schema**
- Dimensiones normalizadas
- Hechos con métricas específicas
- Relaciones 1:N claras

### ✅ **Fácil Relacionar**
- Un archivo por concepto
- Sin caracteres especiales
- Nombres descriptivos

### ✅ **Validado**
- 15 tests de integridad
- Integridad referencial verificada
- Calidad de datos asegurada

---

## 🚀 Cómo Usar

### 1. **Cargar en Tableau**
```bash
# Conectar a carpeta
File → Open → data/processed/dimensional/

# Relacionar tablas
Data → Relationships → Drag provincia_id, tiempo_id, etc.
```

### 2. **Configurar Relaciones**
```
dim_provincias (provincia_id) = fact_*_provincias (provincia_id)
dim_tiempo (tiempo_id) = fact_* (tiempo_id)
```

### 3. **Crear Visualizaciones**
- **Mapas**: Usar dim_provincias + fact_*_provincias
- **Series temporales**: Usar dim_tiempo + fact_*
- **Comparativas**: Usar multiple facts con mismas dimensiones

---

## 📝 Notas Técnicas

- **Encoding:** UTF-8
- **Separador:** Coma (,)
- **Integridad:** 100% validada
- **Performance:** Optimizado para joins
- **Compatibilidad:** Excel, Tableau, Power BI, Python, R

---

**🎉 ¡El modelo dimensional está listo para crear dashboards profesionales con relaciones claras y consistentes!**