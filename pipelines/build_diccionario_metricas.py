"""build_diccionario_metricas.py
---------------------------------
Genera un diccionario de métricas a partir de la tabla unificada
`fact_unificado_long.csv` producida por build_fact_unificado.py

Salida:
  data/processed/out/diccionario_metricas.csv

Columnas:
  dominio
  subcategoria
  variable
  unidad_inferida
  archivos_fuente   (lista de archivos donde aparece)
  anios_min_max     (rango de años)
  observaciones     (conteo de filas)
  cobertura_provincias (número de provincias distintas si aplica)
  nota_heuristica   (comentario automático)
"""
from __future__ import annotations

from pathlib import Path
import pandas as pd

BASE_DIR = Path(__file__).resolve().parent.parent
OUT_DIR = BASE_DIR / 'data' / 'processed' / 'out'


def cargar_fact() -> pd.DataFrame:
    fact_path = OUT_DIR / 'fact_unificado_long.csv'
    if not fact_path.exists():
        raise FileNotFoundError("fact_unificado_long.csv no encontrado. Ejecutar pipelines/build_fact_unificado.py")
    return pd.read_csv(fact_path, low_memory=False)


def generar_diccionario(fact: pd.DataFrame) -> pd.DataFrame:
    # Asegurar columnas mínimas
    requeridas = {'dominio','subcategoria','variable','valor','fuente_archivo'}
    faltantes = requeridas - set(fact.columns)
    if faltantes:
        raise ValueError(f"Columnas faltantes en fact_unificado: {faltantes}")

    # Convertir año si existe
    if 'anio' in fact.columns:
        fact['anio'] = pd.to_numeric(fact['anio'], errors='coerce')

    # Agrupación base
    grp = fact.groupby(['dominio','subcategoria','variable'], dropna=False)

    registros = []
    for (dom, sub, var), df in grp:
        archivos = sorted(df['fuente_archivo'].dropna().unique())
        unidad = None
        if 'unidad' in df.columns:
            uvals = [u for u in df['unidad'].dropna().unique() if str(u).strip()]
            unidad = uvals[0] if uvals else None
        anio_min = int(df['anio'].min()) if 'anio' in df.columns and df['anio'].notna().any() else None
        anio_max = int(df['anio'].max()) if 'anio' in df.columns and df['anio'].notna().any() else None
        provincias = None
        if 'ProvinciaNorm' in df.columns and df['ProvinciaNorm'].notna().any():
            provincias = int(df['ProvinciaNorm'].nunique())
        nota = heuristica_nota(var, unidad, sub, provincias)
        registros.append({
            'dominio': dom,
            'subcategoria': sub,
            'variable': var,
            'unidad_inferida': unidad,
            'archivos_fuente': ';'.join(archivos),
            'anios_min_max': f"{anio_min}-{anio_max}" if anio_min is not None else '',
            'observaciones': len(df),
            'cobertura_provincias': provincias,
            'nota_heuristica': nota,
        })
    dicc = pd.DataFrame(registros)
    # Normalizar tipo de anios_min_max a string
    if 'anios_min_max' in dicc.columns:
        dicc['anios_min_max'] = dicc['anios_min_max'].fillna('').astype(str)
    dicc.sort_values(['dominio','subcategoria','variable'], inplace=True)
    return dicc


def heuristica_nota(variable: str, unidad: str | None, sub: str, provincias: int | None) -> str:
    v = variable.lower()
    hints = []
    if unidad is None:
        if any(k in v for k in ['mbps','velocidad']):
            hints.append('Posible unidad Mbps')
        if any(k in v for k in ['accesos','prepago','pospago','hogares']):
            hints.append('Conteo de accesos/líneas')
        if any(k in v for k in ['ingresos','pesos']):
            hints.append('Monto monetario')
        if any(k in v for k in ['sms']):
            hints.append('Mensajes SMS')
        if any(k in v for k in ['minutos']):
            hints.append('Minutos de tráfico')
    if sub == 'penetracion':
        hints.append('Indicador relativo (por 100 hab/hog)')
    if provincias and provincias == 24:
        hints.append('Cobertura nacional completa')
    return ' | '.join(hints)


def main():
    print('🔍 Generando diccionario de métricas...')
    fact = cargar_fact()
    dicc = generar_diccionario(fact)
    out_path = OUT_DIR / 'diccionario_metricas.csv'
    dicc.to_csv(out_path, index=False, encoding='utf-8')
    print(f'✔ diccionario_metricas.csv ({len(dicc)} métricas)')


if __name__ == '__main__':
    main()
