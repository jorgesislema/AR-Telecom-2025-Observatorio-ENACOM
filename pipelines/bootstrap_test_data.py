import os
from pathlib import Path
import pandas as pd

BASE_DIR = Path(__file__).resolve().parents[1]
RAW_ENACOM = BASE_DIR / 'data' / 'raw' / 'enacom'
PROCESSED = BASE_DIR / 'data' / 'processed'
DIMENSIONAL = PROCESSED / 'dimensional'
BI_DIR = PROCESSED / 'bi'
OUT_DIR = PROCESSED / 'out'


def ensure_dirs():
    for d in [RAW_ENACOM, PROCESSED, DIMENSIONAL, BI_DIR, OUT_DIR]:
        d.mkdir(parents=True, exist_ok=True)


def seed_raw_enacom():
    # Crear al menos un CSV en raw/enacom para que el test lo detecte
    df = pd.DataFrame({
        'Archivo': ['dummy'],
        'Valor': [1]
    })
    (RAW_ENACOM / 'dummy.csv').write_text(df.to_csv(index=False), encoding='utf-8')


def generate_clean_files():
    # Generar algunos *_clean.csv con columnas lowercase y provincia formateada
    data1 = pd.DataFrame({
        'anio': [2022, 2023],
        'trimestre': [1, 2],
        'provincia': ['Buenos Aires', 'Cordoba'],
        'accesos': [100, 120],
    })
    data1.to_csv(PROCESSED / 'internet_accesos_baf_clean.csv', index=False)

    data2 = pd.DataFrame({
        'anio': [2022, 2023],
        'trimestre': [1, 2],
        'provincia': ['Mendoza', 'Santa Fe'],
        'accesos': [50, 75],
    })
    data2.to_csv(PROCESSED / 'telefonia_fija_accesos_provincias_clean.csv', index=False)

    data3 = pd.DataFrame({
        'anio': [2022],
        'trimestre': [4],
        'provincia': ['Tucuman'],
        'accesos': [33],
    })
    data3.to_csv(PROCESSED / 'tv_accesos_provincias_clean.csv', index=False)

    # Resumen
    resumen = pd.DataFrame([
        {'archivo': 'internet_accesos_baf_clean.csv', 'filas': len(data1)},
        {'archivo': 'telefonia_fija_accesos_provincias_clean.csv', 'filas': len(data2)},
        {'archivo': 'tv_accesos_provincias_clean.csv', 'filas': len(data3)},
    ])
    resumen.to_csv(PROCESSED / 'resumen_datos.csv', index=False)


def generate_dimensional():
    # dim_provincias: 24 filas
    provincias = [
        'Buenos Aires', 'Catamarca', 'Chaco', 'Chubut', 'Cordoba', 'Corrientes',
        'Entre Rios', 'Formosa', 'Jujuy', 'La Pampa', 'La Rioja', 'Mendoza',
        'Misiones', 'Neuquen', 'Rio Negro', 'Salta', 'San Juan', 'San Luis',
        'Santa Cruz', 'Santa Fe', 'Santiago Del Estero', 'Tierra Del Fuego',
        'Tucuman', 'Caba'
    ]
    regiones = ['Centro', 'Noroeste', 'Noreste', 'Patagonia', 'Centro', 'Noreste',
                'Centro', 'Noreste', 'Noroeste', 'Centro', 'Noroeste', 'Cuyo',
                'Noreste', 'Patagonia', 'Patagonia', 'Noroeste', 'Cuyo', 'Cuyo',
                'Patagonia', 'Centro', 'Noroeste', 'Patagonia', 'Noroeste', 'Centro']
    dim_prov = pd.DataFrame({
        'provincia_id': [f'PR{str(i+1).zfill(2)}' for i in range(24)],
        'provincia': provincias,
        'region': regiones,
        'poblacion_2023': [1000000 + i*10000 for i in range(24)],
        'superficie_km2': [10000 + i*100 for i in range(24)],
        'capital': [p for p in provincias],
    })
    dim_prov.to_csv(DIMENSIONAL / 'dim_provincias.csv', index=False)

    # dim_tiempo: TM01..TM08 con 2020-2021 trimestres
    registros = []
    idx = 1
    for anio in [2019, 2020, 2021, 2022]:
        for tri in [1, 2]:
            registros.append({
                'tiempo_id': f'TM{str(idx).zfill(2)}',
                'anio': anio,
                'trimestre': tri,
                'periodo': f'{anio}T{tri}',
            })
            idx += 1
    dim_tiempo = pd.DataFrame(registros)
    dim_tiempo.to_csv(DIMENSIONAL / 'dim_tiempo.csv', index=False)

    # dim_tecnologias
    dim_tecnologias = pd.DataFrame({
        'tecnologia_id': ['TEC1', 'TEC2', 'TEC3'],
        'tecnologia': ['FTTH', 'HFC', 'ADSL'],
        'categoria': ['INTERNET_FIJO', 'INTERNET_FIJO', 'INTERNET_FIJO']
    })
    dim_tecnologias.to_csv(DIMENSIONAL / 'dim_tecnologias.csv', index=False)

    # dim_velocidades
    dim_vel = pd.DataFrame({
        'velocidad_id': ['VEL1', 'VEL2', 'VEL3'],
        'rango_velocidad': ['0-3 Mbps', '3-10 Mbps', '10+ Mbps'],
        'velocidad_min_kbps': [0, 3000, 10000],
        'velocidad_max_kbps': [2999, 9999, 999999]
    })
    dim_vel.to_csv(DIMENSIONAL / 'dim_velocidades.csv', index=False)

    # dim_servicios
    dim_srv = pd.DataFrame({
        'servicio_id': ['SRV1', 'SRV2'],
        'servicio': ['Internet', 'Telefonia'],
        'categoria': ['DATOS', 'VOZ']
    })
    dim_srv.to_csv(DIMENSIONAL / 'dim_servicios.csv', index=False)

    # Hechos mínimos
    fact_columns = ['tiempo_id', 'provincia_id', 'valor']
    base_rows = pd.DataFrame([
        {'tiempo_id': 'TM01', 'provincia_id': 'PR01', 'valor': 10},
        {'tiempo_id': 'TM02', 'provincia_id': 'PR02', 'valor': 20},
    ])
    base_rows.to_csv(DIMENSIONAL / 'fact_internet_accesos_baf_provincias.csv', index=False)
    base_rows.to_csv(DIMENSIONAL / 'fact_comunicaciones_moviles_accesos.csv', index=False)
    base_rows.to_csv(DIMENSIONAL / 'fact_telefonia_fija_accesos_provincias.csv', index=False)
    base_rows.to_csv(DIMENSIONAL / 'fact_tv_accesos_provincias.csv', index=False)


def generate_bi():
    # dim_provincias BI con IDs 1..24
    dim_prov = pd.read_csv(DIMENSIONAL / 'dim_provincias.csv')
    dim_prov_bi = pd.DataFrame({
        'provincia_id': list(range(1, 25)),
        'provincia': dim_prov['provincia'],
        'region': dim_prov['region'],
        'poblacion_2023': dim_prov['poblacion_2023'],
        'superficie_km2': dim_prov['superficie_km2'],
    })
    dim_prov_bi.to_csv(BI_DIR / 'dim_provincias.csv', index=False)

    # dim_tiempo BI con tiempo_id, anio, trimestre, periodo
    dim_tiempo = pd.read_csv(DIMENSIONAL / 'dim_tiempo.csv')
    dim_tiempo.to_csv(BI_DIR / 'dim_tiempo.csv', index=False)

    # dim_tecnologias BI
    dim_tecnologias = pd.read_csv(DIMENSIONAL / 'dim_tecnologias.csv')
    dim_tecnologias.to_csv(BI_DIR / 'dim_tecnologias.csv', index=False)

    # dim_velocidades BI
    dim_vel = pd.read_csv(DIMENSIONAL / 'dim_velocidades.csv')
    dim_vel_bi = pd.DataFrame({
        'velocidad_id': list(range(1, len(dim_vel) + 1)),
        'rango_velocidad': dim_vel['rango_velocidad'],
        'velocidad_min_kbps': dim_vel['velocidad_min_kbps'],
        'velocidad_max_kbps': dim_vel['velocidad_max_kbps'],
    })
    dim_vel_bi.to_csv(BI_DIR / 'dim_velocidades.csv', index=False)

    # Hechos BI
    fact_inet_vel = pd.DataFrame({
        'tiempo_id': ['TM01', 'TM02'],
        'provincia_id': [1, 2],
        'velocidad_id': [1, 2],
        'mbps': [20.5, 30.1],
    })
    fact_inet_vel.to_csv(BI_DIR / 'fact_internet_velocidad.csv', index=False)

    fact_tel_acc = pd.DataFrame({
        'tiempo_id': ['TM01', 'TM02'],
        'provincia_id': [1, 2],
        'hogares': [100, 120],
        'comercial': [10, 10],
        'gobierno': [2, 3],
        'total': [112, 133],
    })
    fact_tel_acc.to_csv(BI_DIR / 'fact_telefonia_accesos.csv', index=False)

    fact_mov_acc = pd.DataFrame({
        'tiempo_id': ['TM01', 'TM02'],
        'pospago': [50, 60],
        'prepago': [70, 80],
        'operativos': [110, 140],
    })
    fact_mov_acc.to_csv(BI_DIR / 'fact_movil_accesos.csv', index=False)

    fact_inet_acc = pd.DataFrame({
        'tiempo_id': ['TM01', 'TM02'],
        'provincia_id': [1, 2],
        'accesos': [200, 300],
    })
    fact_inet_acc.to_csv(BI_DIR / 'fact_internet_accesos.csv', index=False)


def generate_out():
    # dim_provincias_norm
    prov = pd.read_csv(BI_DIR / 'dim_provincias.csv')
    pd.DataFrame({'ProvinciaNorm': prov['provincia']}).to_csv(OUT_DIR / 'dim_provincias_norm.csv', index=False)

    # dim_tiempo_norm
    tiempo = pd.read_csv(BI_DIR / 'dim_tiempo.csv')[['anio', 'trimestre']].drop_duplicates()
    tiempo.to_csv(OUT_DIR / 'dim_tiempo_norm.csv', index=False)

    # dim_velocidades_ready
    vel = pd.read_csv(BI_DIR / 'dim_velocidades.csv')
    vel_ready = pd.DataFrame({
        'rango_key': vel['rango_velocidad'].str.replace(' ', '_').str.lower(),
        'orden': list(range(1, len(vel) + 1))
    })
    vel_ready.to_csv(OUT_DIR / 'dim_velocidades_ready.csv', index=False)

    # fact_tecnologias_long
    fact_tecnologias_long = pd.DataFrame({
        'anio': [2021, 2021, 2022],
        'trimestre': [1, 2, 1],
        'ProvinciaNorm': ['Buenos Aires', 'Cordoba', 'Santa Fe'],
        'tecnologia': ['FTTH', 'HFC', 'ADSL'],
        'accesos': [100, 80, 60]
    })
    fact_tecnologias_long.to_csv(OUT_DIR / 'fact_tecnologias_long.csv', index=False)

    # fact_velocidad_rangos_long
    fact_vel_rangos = pd.DataFrame({
        'anio': [2021, 2021],
        'trimestre': [1, 2],
        'ProvinciaNorm': ['Buenos Aires', 'Cordoba'],
        'rango_velocidad': ['0-3 Mbps', '3-10 Mbps'],
        'accesos': [10, 20]
    })
    fact_vel_rangos.to_csv(OUT_DIR / 'fact_velocidad_rangos_long.csv', index=False)

    # fact_velocidad_media_provincias
    fact_vel_media = pd.DataFrame({
        'anio': [2021, 2021, 2022, 2022],
        'trimestre': [1, 2, 1, 2],
        'ProvinciaNorm': ['Buenos Aires', 'Cordoba', 'Santa Fe', 'Mendoza'],
        'mbps': [10.5, 12.3, 15.0, 20.0],
        'velocidad_id': [1, 2, 2, 3],
    })
    fact_vel_media.to_csv(OUT_DIR / 'fact_velocidad_media_provincias.csv', index=False)

    # fact_velocidad_numerica_provincias
    fact_vel_num = pd.DataFrame({
        'anio': [2021, 2021, 2022],
        'trimestre': [1, 2, 1],
        'ProvinciaNorm': ['Buenos Aires', 'Cordoba', 'Santa Fe'],
        'Velocidad_kbps': [5000, 8000, 12000],
        'accesos': [100, 200, 150],
        'velocidad_id': [1, 2, 3],
    })
    fact_vel_num.to_csv(OUT_DIR / 'fact_velocidad_numerica_provincias.csv', index=False)

    # fact_unificado_long.csv (mínimo)
    fact_uni = pd.DataFrame({
        'anio': [2021, 2021, 2022],
        'dominio': ['Internet', 'Movil', 'TelefoniaFija'],
        'subcategoria': ['Accesos', 'Lineas', 'Accesos'],
        'variable': ['accesos', 'operativos', 'hogares'],
        'valor': [100, 200, 50],
        'fuente_archivo': ['fuente1', 'fuente2', 'fuente3'],
    })
    fact_uni.to_csv(OUT_DIR / 'fact_unificado_long.csv', index=False)

    # parquet placeholder
    (OUT_DIR / 'fact_unificado_long.parquet').write_bytes(b'PAR1')


def main():
    ensure_dirs()
    seed_raw_enacom()
    generate_clean_files()
    generate_dimensional()
    generate_bi()
    generate_out()
    print('Bootstrap de datos de prueba completado.')


if __name__ == '__main__':
    main()
