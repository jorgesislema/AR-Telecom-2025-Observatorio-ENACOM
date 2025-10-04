#!/usr/bin/env python3
"""
ETL Dimensional Completo - Observatorio ENACOM 2025
Procesa todos los archivos raw XLSX y genera modelo dimensional con IDs alfanuméricos de 4 dígitos
"""
import pandas as pd
import numpy as np
from pathlib import Path
import unicodedata
import glob
import os
from typing import Dict, List, Tuple, Optional
import shutil

# Configuración
RAW_DATA_PATH = Path("data/raw")
OUTPUT_PATH = Path("data/processed/dimensional")
LOG_PATH = Path("logs")

def normalizar_texto(texto: str) -> str:
    """Normaliza texto eliminando tildes y caracteres especiales"""
    if pd.isna(texto):
        return texto
    texto = str(texto)
    texto = unicodedata.normalize('NFD', texto)
    texto = ''.join(c for c in texto if unicodedata.category(c) != 'Mn')
    return texto.upper().strip()

def generar_id_alfanumerico(prefijo: str, numero: int) -> str:
    """Genera ID alfanumérico de 4 dígitos con prefijo"""
    if len(prefijo) == 2:
        return f"{prefijo}{numero:02d}"  # PR01, TM01, etc.
    elif len(prefijo) == 3:
        return f"{prefijo}{numero:01d}"  # TEC1, VEL1, etc.
    else:
        return f"{prefijo[:3]}{numero:01d}"

def crear_directorio_salida():
    """Crea directorios de salida necesarios"""
    OUTPUT_PATH.mkdir(parents=True, exist_ok=True)
    LOG_PATH.mkdir(parents=True, exist_ok=True)
    
    # Limpiar carpeta anterior si existe
    if OUTPUT_PATH.exists():
        shutil.rmtree(OUTPUT_PATH)
    OUTPUT_PATH.mkdir(parents=True, exist_ok=True)

def crear_dim_provincias() -> pd.DataFrame:
    """Crea dimensión de provincias con IDs alfanuméricos"""
    print("Creando dim_provincias...")
    
    # Datos de provincias argentinas con información completa
    provincias_info = {
        'BUENOS AIRES': {'region': 'PAMPEANA', 'poblacion': 17569053, 'superficie': 307571, 'capital': 'LA PLATA'},
        'CABA': {'region': 'PAMPEANA', 'poblacion': 3075646, 'superficie': 200, 'capital': 'CABA'},
        'CATAMARCA': {'region': 'NOA', 'poblacion': 429556, 'superficie': 102602, 'capital': 'SAN FERNANDO DEL VALLE DE CATAMARCA'},
        'CHACO': {'region': 'NEA', 'poblacion': 1204541, 'superficie': 99633, 'capital': 'RESISTENCIA'},
        'CHUBUT': {'region': 'PATAGONIA', 'poblacion': 618994, 'superficie': 224686, 'capital': 'RAWSON'},
        'CORDOBA': {'region': 'PAMPEANA', 'poblacion': 3978984, 'superficie': 165321, 'capital': 'CORDOBA'},
        'CORRIENTES': {'region': 'NEA', 'poblacion': 1120801, 'superficie': 88199, 'capital': 'CORRIENTES'},
        'ENTRE RIOS': {'region': 'PAMPEANA', 'poblacion': 1426426, 'superficie': 78781, 'capital': 'PARANA'},
        'FORMOSA': {'region': 'NEA', 'poblacion': 606041, 'superficie': 72066, 'capital': 'FORMOSA'},
        'JUJUY': {'region': 'NOA', 'poblacion': 770881, 'superficie': 53219, 'capital': 'SAN SALVADOR DE JUJUY'},
        'LA PAMPA': {'region': 'PAMPEANA', 'poblacion': 364488, 'superficie': 143440, 'capital': 'SANTA ROSA'},
        'LA RIOJA': {'region': 'NOA', 'poblacion': 393531, 'superficie': 89680, 'capital': 'LA RIOJA'},
        'MENDOZA': {'region': 'CUYO', 'poblacion': 2014533, 'superficie': 148827, 'capital': 'MENDOZA'},
        'MISIONES': {'region': 'NEA', 'poblacion': 1261294, 'superficie': 29801, 'capital': 'POSADAS'},
        'NEUQUEN': {'region': 'PATAGONIA', 'poblacion': 726590, 'superficie': 94078, 'capital': 'NEUQUEN'},
        'RIO NEGRO': {'region': 'PATAGONIA', 'poblacion': 747610, 'superficie': 203013, 'capital': 'VIEDMA'},
        'SALTA': {'region': 'NOA', 'poblacion': 1424397, 'superficie': 155488, 'capital': 'SALTA'},
        'SAN JUAN': {'region': 'CUYO', 'poblacion': 789489, 'superficie': 89651, 'capital': 'SAN JUAN'},
        'SAN LUIS': {'region': 'CUYO', 'poblacion': 508328, 'superficie': 76748, 'capital': 'SAN LUIS'},
        'SANTA CRUZ': {'region': 'PATAGONIA', 'poblacion': 374756, 'superficie': 243943, 'capital': 'RIO GALLEGOS'},
        'SANTA FE': {'region': 'PAMPEANA', 'poblacion': 3563390, 'superficie': 133007, 'capital': 'SANTA FE'},
        'SANTIAGO DEL ESTERO': {'region': 'NOA', 'poblacion': 978313, 'superficie': 136351, 'capital': 'SANTIAGO DEL ESTERO'},
        'TIERRA DEL FUEGO': {'region': 'PATAGONIA', 'poblacion': 190641, 'superficie': 21263, 'capital': 'USHUAIA'},
        'TUCUMAN': {'region': 'NOA', 'poblacion': 1703186, 'superficie': 22524, 'capital': 'SAN MIGUEL DE TUCUMAN'}
    }
    
    data = []
    for i, (provincia, info) in enumerate(provincias_info.items(), 1):
        data.append({
            'provincia_id': generar_id_alfanumerico('PR', i),
            'provincia': provincia,
            'region': info['region'],
            'poblacion_2023': info['poblacion'],
            'superficie_km2': info['superficie'],
            'capital': info['capital'],
            'densidad_poblacional': round(info['poblacion'] / info['superficie'], 2)
        })
    
    df = pd.DataFrame(data)
    return df



def crear_dim_tecnologias() -> pd.DataFrame:
    """Crea dimensión de tecnologías con IDs alfanuméricos"""
    print("Creando dim_tecnologias...")
    
    tecnologias_info = [
        {'tecnologia': 'ADSL', 'categoria': 'INTERNET_FIJO', 'descripcion': 'Asymmetric Digital Subscriber Line'},
        {'tecnologia': 'CABLE_MODEM', 'categoria': 'INTERNET_FIJO', 'descripcion': 'Internet por cable coaxial'},
        {'tecnologia': 'FIBRA_OPTICA', 'categoria': 'INTERNET_FIJO', 'descripcion': 'Fiber To The Home/Building'},
        {'tecnologia': 'WIRELESS', 'categoria': 'INTERNET_FIJO', 'descripcion': 'Internet inalámbrico fijo'},
        {'tecnologia': 'OTROS', 'categoria': 'INTERNET_FIJO', 'descripcion': 'Otras tecnologías de internet fijo'},
        {'tecnologia': 'SATELITAL', 'categoria': 'INTERNET_FIJO', 'descripcion': 'Internet satelital'},
        {'tecnologia': 'DIAL_UP', 'categoria': 'INTERNET_FIJO', 'descripcion': 'Conexión telefónica'},
        {'tecnologia': 'LTE', 'categoria': 'INTERNET_MOVIL', 'descripcion': 'Long Term Evolution 4G'},
        {'tecnologia': '3G', 'categoria': 'INTERNET_MOVIL', 'descripcion': 'Tercera generación móvil'},
        {'tecnologia': '5G', 'categoria': 'INTERNET_MOVIL', 'descripcion': 'Quinta generación móvil'},
        {'tecnologia': 'TELEFONIA_FIJA', 'categoria': 'TELEFONIA', 'descripcion': 'Líneas de telefonía fija'},
        {'tecnologia': 'TV_CABLE', 'categoria': 'TV_PAGA', 'descripcion': 'Televisión por cable'},
        {'tecnologia': 'TV_SATELITAL', 'categoria': 'TV_PAGA', 'descripcion': 'Televisión satelital'},
        {'tecnologia': 'IPTV', 'categoria': 'TV_PAGA', 'descripcion': 'Internet Protocol Television'}
    ]
    
    data = []
    for i, tech in enumerate(tecnologias_info, 1):
        data.append({
            'tecnologia_id': generar_id_alfanumerico('TEC', i),
            'tecnologia': tech['tecnologia'],
            'categoria': tech['categoria'],
            'descripcion': tech['descripcion']
        })
    
    df = pd.DataFrame(data)
    return df

def crear_dim_velocidades() -> pd.DataFrame:
    """Crea dimensión de velocidades con IDs alfanuméricos"""
    print("Creando dim_velocidades...")
    
    velocidades_info = [
        {'rango': 'HASTA_512_KBPS', 'min_kbps': 0, 'max_kbps': 512},
        {'rango': '512_KBPS_A_1_MBPS', 'min_kbps': 513, 'max_kbps': 1024},
        {'rango': '1_A_6_MBPS', 'min_kbps': 1025, 'max_kbps': 6144},
        {'rango': '6_A_10_MBPS', 'min_kbps': 6145, 'max_kbps': 10240},
        {'rango': '10_A_20_MBPS', 'min_kbps': 10241, 'max_kbps': 20480},
        {'rango': '20_A_30_MBPS', 'min_kbps': 20481, 'max_kbps': 30720},
        {'rango': 'MAS_30_MBPS', 'min_kbps': 30721, 'max_kbps': 999999}
    ]
    
    data = []
    for i, vel in enumerate(velocidades_info, 1):
        data.append({
            'velocidad_id': generar_id_alfanumerico('VEL', i),
            'rango_velocidad': vel['rango'],
            'velocidad_min_kbps': vel['min_kbps'],
            'velocidad_max_kbps': vel['max_kbps']
        })
    
    df = pd.DataFrame(data)
    return df

def crear_dim_servicios() -> pd.DataFrame:
    """Crea dimensión de servicios con IDs alfanuméricos"""
    print("Creando dim_servicios...")
    
    servicios_info = [
        {'servicio': 'INTERNET_FIJO', 'categoria': 'CONECTIVIDAD', 'descripcion': 'Servicios de internet fijo'},
        {'servicio': 'INTERNET_MOVIL', 'categoria': 'CONECTIVIDAD', 'descripcion': 'Servicios de internet móvil'},
        {'servicio': 'TELEFONIA_FIJA', 'categoria': 'TELEFONIA', 'descripcion': 'Servicios de telefonía fija'},
        {'servicio': 'TELEFONIA_MOVIL', 'categoria': 'TELEFONIA', 'descripcion': 'Servicios de telefonía móvil'},
        {'servicio': 'TV_PAGA', 'categoria': 'ENTRETENIMIENTO', 'descripcion': 'Servicios de TV por suscripción'},
        {'servicio': 'MERCADO_POSTAL', 'categoria': 'POSTAL', 'descripcion': 'Servicios postales y envíos'}
    ]
    
    data = []
    for i, srv in enumerate(servicios_info, 1):
        data.append({
            'servicio_id': generar_id_alfanumerico('SRV', i),
            'servicio': srv['servicio'],
            'categoria': srv['categoria'],
            'descripcion': srv['descripcion']
        })
    
    df = pd.DataFrame(data)
    return df

def obtener_provincia_id(provincia: str, dim_provincias: pd.DataFrame) -> Optional[str]:
    """Obtiene ID de provincia normalizada"""
    provincia_norm = normalizar_texto(provincia)
    match = dim_provincias[dim_provincias['provincia'] == provincia_norm]
    return match.iloc[0]['provincia_id'] if not match.empty else None

def obtener_tiempo_id(anio: int, trimestre: int, dim_tiempo: pd.DataFrame) -> Optional[str]:
    """Obtiene ID de tiempo para año y trimestre"""
    match = dim_tiempo[(dim_tiempo['anio'] == anio) & (dim_tiempo['trimestre'] == trimestre)]
    return match.iloc[0]['tiempo_id'] if not match.empty else None

def obtener_velocidad_id(velocidad_mbps: float, dim_velocidades: pd.DataFrame) -> Optional[str]:
    """Obtiene ID de velocidad basado en el valor en Mbps"""
    if pd.isna(velocidad_mbps):
        return None
    
    # Convertir Mbps a kbps
    velocidad_kbps = velocidad_mbps * 1024
    
    for _, row in dim_velocidades.iterrows():
        if row['velocidad_min_kbps'] <= velocidad_kbps <= row['velocidad_max_kbps']:
            return row['velocidad_id']
    
    # Si no encuentra, asignar al rango más alto
    return dim_velocidades.iloc[-1]['velocidad_id']

def obtener_tecnologia_id(tecnologia_nombre: str, dim_tecnologias: pd.DataFrame) -> Optional[str]:
    """Obtiene ID de tecnología basado en el nombre"""
    if pd.isna(tecnologia_nombre):
        return None
    
    # Mapeo de nombres de columnas a tecnologías
    mapeo_tecnologias = {
        'adsl': 'ADSL',
        'cablemodem': 'CABLE_MODEM', 
        'fibraoptica': 'FIBRA_OPTICA',
        'wireless': 'WIRELESS',
        'otros': 'OTROS',
        'satelital': 'SATELITAL',
        'dial_up': 'DIAL_UP'
    }
    
    tech_normalizada = tecnologia_nombre.lower().replace('_', '').replace(' ', '')
    tecnologia_buscar = mapeo_tecnologias.get(tech_normalizada, tecnologia_nombre.upper())
    
    match = dim_tecnologias[dim_tecnologias['tecnologia'] == tecnologia_buscar]
    return match.iloc[0]['tecnologia_id'] if not match.empty else None

def procesar_archivos_raw():
    """Procesa todos los archivos raw XLSX y genera tablas de hechos"""
    print("Procesando archivos raw...")
    
    # Cargar dimensiones previamente creadas
    dim_provincias = pd.read_csv(OUTPUT_PATH / "dim_provincias.csv")
    dim_tecnologias = pd.read_csv(OUTPUT_PATH / "dim_tecnologias.csv")
    dim_velocidades = pd.read_csv(OUTPUT_PATH / "dim_velocidades.csv")
    dim_servicios = pd.read_csv(OUTPUT_PATH / "dim_servicios.csv")
    
    # Buscar todos los archivos XLSX
    archivos_xlsx = glob.glob(str(RAW_DATA_PATH / "*.xlsx"))
    print(f"Encontrados {len(archivos_xlsx)} archivos XLSX")
    
    hechos_generados = []
    
    for archivo_path in archivos_xlsx:
        nombre_archivo = Path(archivo_path).stem
        print(f"Procesando: {nombre_archivo}")
        
        try:
            df = pd.read_excel(archivo_path)
            
            # Normalizar nombres de provincias si existe la columna
            if 'provincia' in df.columns:
                df['provincia'] = df['provincia'].apply(normalizar_texto)
                df['provincia_id'] = df['provincia'].apply(
                    lambda x: obtener_provincia_id(x, dim_provincias)
                )
            
            # Procesar según el tipo de archivo
            if 'internet_accesos' in nombre_archivo:
                fact_df = procesar_internet_accesos(df, nombre_archivo)
            elif 'comunicaciones_moviles' in nombre_archivo:
                fact_df = procesar_moviles(df, nombre_archivo)
            elif 'telefonia_fija' in nombre_archivo:
                fact_df = procesar_telefonia(df, nombre_archivo)
            elif 'tv_' in nombre_archivo:
                fact_df = procesar_tv(df, nombre_archivo)
            elif 'ingresos' in nombre_archivo:
                fact_df = procesar_ingresos(df, nombre_archivo)
            else:
                print(f"  -> Tipo no reconocido, saltando...")
                continue
            
            if fact_df is not None and not fact_df.empty:
                output_file = OUTPUT_PATH / f"fact_{nombre_archivo.replace('_', '_')}.csv"
                fact_df.to_csv(output_file, index=False)
                hechos_generados.append(output_file.name)
                print(f"  -> Generado: {output_file.name} ({len(fact_df)} filas)")
            
        except Exception as e:
            print(f"  -> Error procesando {nombre_archivo}: {e}")
    
    return hechos_generados

def procesar_internet_accesos(df: pd.DataFrame, nombre_archivo: str) -> pd.DataFrame:
    """Procesa archivos de accesos de internet"""
    columnas_base = []
    
    # Agregar fechas normalizadas si existen
    if 'anio' in df.columns:
        columnas_base.append('anio')
    if 'trimestre' in df.columns:
        columnas_base.append('trimestre')
    if 'mes' in df.columns:
        columnas_base.append('mes')
    
    # Agregar provincia_id solo si existe en el dataframe    
    if 'provincia_id' in df.columns:
        columnas_base.append('provincia_id')
    
    # Identificar columnas de métricas (que no sean tiempo, provincia)
    columnas_excluir = ['anio', 'trimestre', 'mes', 'provincia', 'provincia_id']
    columnas_metricas = [col for col in df.columns if col not in columnas_excluir]
    
    fact_df = df[columnas_base + columnas_metricas].copy()
    
    # Solo agregar velocidad_id si hay columna 'velocidad'
    if 'velocidad' in df.columns:
        # Cargar dim_velocidades
        dim_velocidades = pd.read_csv(Path("data/processed/dimensional") / "dim_velocidades.csv")
        fact_df['velocidad_id'] = df['velocidad'].apply(
            lambda x: obtener_velocidad_id(x, dim_velocidades)
        )
    
    # Solo agregar tecnologia_id si es archivo de tecnologías Y tiene columnas de tecnologías
    if 'tecnologias' in nombre_archivo:
        # Cargar dim_tecnologias
        dim_tecnologias = pd.read_csv(Path("data/processed/dimensional") / "dim_tecnologias.csv")
        
        # Crear tabla long para tecnologías (una fila por tecnología)
        tech_cols = ['adsl', 'cablemodem', 'fibraOptica', 'wireless', 'otros']
        tech_cols_exist = [col for col in tech_cols if col in df.columns]
        
        if tech_cols_exist:
            # Convertir a formato long
            id_vars = columnas_base.copy()
            fact_long = pd.melt(df[id_vars + tech_cols_exist], 
                              id_vars=id_vars,
                              value_vars=tech_cols_exist,
                              var_name='tecnologia',
                              value_name='accesos')
            
            # Agregar tecnologia_id
            fact_long['tecnologia_id'] = fact_long['tecnologia'].apply(
                lambda x: obtener_tecnologia_id(x, dim_tecnologias)
            )
            
            return fact_long[id_vars + ['tecnologia_id', 'accesos']]
    
    return fact_df

def procesar_moviles(df: pd.DataFrame, nombre_archivo: str) -> pd.DataFrame:
    """Procesa archivos de comunicaciones móviles"""
    columnas_base = []
    
    # Agregar fechas normalizadas si existen
    if 'anio' in df.columns:
        columnas_base.append('anio')
    if 'trimestre' in df.columns:
        columnas_base.append('trimestre')
    if 'mes' in df.columns:
        columnas_base.append('mes')
    
    columnas_excluir = ['anio', 'trimestre', 'mes']
    columnas_metricas = [col for col in df.columns if col not in columnas_excluir]
    
    return df[columnas_base + columnas_metricas].copy()

def procesar_telefonia(df: pd.DataFrame, nombre_archivo: str) -> pd.DataFrame:
    """Procesa archivos de telefonía fija"""
    columnas_base = []
    
    # Agregar fechas normalizadas si existen
    if 'anio' in df.columns:
        columnas_base.append('anio')
    if 'trimestre' in df.columns:
        columnas_base.append('trimestre')
    if 'mes' in df.columns:
        columnas_base.append('mes')
    
    # Agregar provincia_id solo si existe en el dataframe    
    if 'provincia_id' in df.columns:
        columnas_base.append('provincia_id')
        
    columnas_excluir = ['anio', 'trimestre', 'mes', 'provincia', 'provincia_id']
    columnas_metricas = [col for col in df.columns if col not in columnas_excluir]
    
    return df[columnas_base + columnas_metricas].copy()

def procesar_tv(df: pd.DataFrame, nombre_archivo: str) -> pd.DataFrame:
    """Procesa archivos de TV"""
    columnas_base = []
    
    # Agregar fechas normalizadas si existen
    if 'anio' in df.columns:
        columnas_base.append('anio')
    if 'trimestre' in df.columns:
        columnas_base.append('trimestre')
    if 'mes' in df.columns:
        columnas_base.append('mes')
    
    # Agregar provincia_id solo si existe en el dataframe    
    if 'provincia_id' in df.columns:
        columnas_base.append('provincia_id')
        
    columnas_excluir = ['anio', 'trimestre', 'mes', 'provincia', 'provincia_id']
    columnas_metricas = [col for col in df.columns if col not in columnas_excluir]
    
    return df[columnas_base + columnas_metricas].copy()

def procesar_ingresos(df: pd.DataFrame, nombre_archivo: str) -> pd.DataFrame:
    """Procesa archivos de ingresos"""
    columnas_base = []
    
    # Agregar fechas normalizadas si existen
    if 'anio' in df.columns:
        columnas_base.append('anio')
    if 'trimestre' in df.columns:
        columnas_base.append('trimestre')
    if 'mes' in df.columns:
        columnas_base.append('mes')
    
    # Determinar servicio_id basado en el nombre del archivo
    servicio_mapping = {
        'internet': 'SRV1',
        'comunicaciones_moviles': 'SRV2', 
        'telefonia_fija': 'SRV3',
        'tv': 'SRV5'
    }
    
    servicio_id = None
    for key, value in servicio_mapping.items():
        if key in nombre_archivo:
            servicio_id = value
            break
    
    if servicio_id:
        df['servicio_id'] = servicio_id
        columnas_base.append('servicio_id')
    
    columnas_excluir = ['anio', 'trimestre', 'tiempo_id', 'servicio_id']
    columnas_metricas = [col for col in df.columns if col not in columnas_excluir]
    
    return df[columnas_base + columnas_metricas].copy()

def main():
    """Función principal del ETL"""
    print("="*60)
    print("ETL DIMENSIONAL COMPLETO - OBSERVATORIO ENACOM 2025")
    print("="*60)
    
    # Crear directorio de salida
    crear_directorio_salida()
    
    # Crear todas las dimensiones
    print("\n1. CREANDO DIMENSIONES...")
    dimensiones = {
        'dim_provincias': crear_dim_provincias(),
        'dim_tecnologias': crear_dim_tecnologias(),
        'dim_velocidades': crear_dim_velocidades(),
        'dim_servicios': crear_dim_servicios()
    }
    
    # Guardar dimensiones
    for nombre, df in dimensiones.items():
        output_file = OUTPUT_PATH / f"{nombre}.csv"
        df.to_csv(output_file, index=False)
        print(f"✓ Creada: {nombre}.csv ({len(df)} registros)")
    
    # Procesar archivos raw y crear hechos
    print("\n2. PROCESANDO ARCHIVOS RAW Y CREANDO HECHOS...")
    hechos_generados = procesar_archivos_raw()
    
    # Resumen final
    print("\n" + "="*60)
    print("RESUMEN DEL PROCESAMIENTO")
    print("="*60)
    print(f"✓ Dimensiones creadas: {len(dimensiones)}")
    print(f"✓ Tablas de hechos generadas: {len(hechos_generados)}")
    print(f"✓ Directorio de salida: {OUTPUT_PATH}")
    
    print("\nDimensiones creadas:")
    for nombre in dimensiones.keys():
        print(f"  • {nombre}.csv")
    
    if hechos_generados:
        print("\nTablas de hechos creadas:")
        for nombre in hechos_generados:
            print(f"  • {nombre}")
    
    print(f"\n🎯 Modelo dimensional listo para dashboards!")
    print(f"📁 Ubicación: {OUTPUT_PATH}")

if __name__ == "__main__":
    main()