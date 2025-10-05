"""export_hyper.py
Exporta algunos datasets clave a formato .hyper para Tableau si la librería
tableauhyperapi está disponible. El objetivo es acelerar la ingesta en Tableau Desktop
o Server evitando pasos manuales.

Datasets candidatos (si existen):
  - fact_unificado_long.csv
  - diccionario_metricas.csv

Uso:
  python pipelines/export_hyper.py

Si tableauhyperapi no está instalado se muestra instrucción de instalación.
"""
from __future__ import annotations

from pathlib import Path
import sys

BASE_DIR = Path(__file__).resolve().parent.parent
OUT_DIR = BASE_DIR / 'data' / 'processed' / 'out'
HYPER_DIR = OUT_DIR / 'hyper'
HYPER_DIR.mkdir(parents=True, exist_ok=True)

try:
    from tableauhyperapi import HyperProcess, Telemetry, Connection, CreateMode, TableDefinition, SqlType, NOT_NULLABLE, NULLABLE, Inserter
    HAS_HYPER = True
except ImportError:
    HAS_HYPER = False


def detectar_esquema(df):
    import pandas as pd
    mapping = {
        'int64': SqlType.big_int(),
        'float64': SqlType.double(),
        'object': SqlType.text(),
        'bool': SqlType.bool(),
    }
    cols = []
    for name, dtype in df.dtypes.items():
        stype = str(dtype)
        sqltype = mapping.get(stype, SqlType.text())
        nullable = NULLABLE
        cols.append(TableDefinition.Column(name=name, type=sqltype, nullability=nullable))
    return cols


def exportar_csv_a_hyper(csv_path: Path, hyper_path: Path, table_name: str):
    import pandas as pd
    df = pd.read_csv(csv_path, low_memory=False)
    table_def = TableDefinition(table_name=table_name, columns=detectar_esquema(df))
    with HyperProcess(telemetry=Telemetry.DO_NOT_SEND_USAGE_DATA_TO_TABLEAU) as hp:
        with Connection(endpoint=hp.endpoint, database=hyper_path, create_mode=CreateMode.CREATE_AND_REPLACE) as connection:
            connection.catalog.create_table(table_def)
            with Inserter(connection, table_def) as inserter:
                inserter.add_rows(df.itertuples(index=False, name=None))
                inserter.execute()
    print(f'  - {table_name.name} -> {hyper_path.name} ({len(df)} filas)')


def main():
    if not HAS_HYPER:
        print('tableauhyperapi no está instalado. Instalar con:')
        print('  pip install tableauhyperapi')
        sys.exit(0)

    objetivos = [
        ('fact_unificado_long.csv', 'fact_unificado', 'fact_unificado.hyper'),
        ('diccionario_metricas.csv', 'diccionario_metricas', 'diccionario_metricas.hyper'),
    ]
    print('Exportando a formato Hyper...')
    for csv_name, table_name, hyper_name in objetivos:
        csv_path = OUT_DIR / csv_name
        if not csv_path.exists():
            print(f'  - Omitido {csv_name} (no existe)')
            continue
        hyper_path = HYPER_DIR / hyper_name
        exportar_csv_a_hyper(csv_path, hyper_path, table_name)
    print('Finalizado.')


if __name__ == '__main__':
    main()
