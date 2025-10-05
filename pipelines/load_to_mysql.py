#!/usr/bin/env python3
"""
Carga los CSV del modelo dimensional a una base de datos MySQL.

Funcionalidad:
- Crea la base de datos si no existe
- Genera tablas a partir de los encabezados de los CSV (dim_*, fact_*)
- Inserta los datos en lotes

Configurar conexión en variables de entorno o .env:
- MYSQL_HOST, MYSQL_PORT, MYSQL_USER, MYSQL_PASSWORD, MYSQL_DB

Uso (PowerShell):
  # Activar entorno si aplica y luego
  python pipelines/load_to_mysql.py

Requisitos:
- mysql-connector-python
- python-dotenv
- pandas
"""
from __future__ import annotations

import os
import sys
import math
import time
import traceback
from pathlib import Path
from typing import Dict, List, Tuple

import pandas as pd
try:
    import mysql.connector  # type: ignore
except Exception as e:  # pragma: no cover
    print("Falta el paquete 'mysql-connector-python'. Instálelo con:\n  pip install mysql-connector-python")
    raise

try:
    from dotenv import load_dotenv  # type: ignore
except Exception:
    def load_dotenv(*args, **kwargs):
        return False


ROOT = Path(__file__).resolve().parents[1]
CSV_DIR = ROOT / "data" / "processed" / "dimensional"


def load_env() -> Dict[str, str]:
    # Cargar .env si existe
    env_file = ROOT / ".env"
    if env_file.exists():
        load_dotenv(env_file)

    cfg = {
        "host": os.getenv("MYSQL_HOST", "localhost"),
        "port": int(os.getenv("MYSQL_PORT", "3306")),
        "user": os.getenv("MYSQL_USER", "root"),
        "password": os.getenv("MYSQL_PASSWORD", ""),
        "database": os.getenv("MYSQL_DB", "enacom_obs"),
    }
    return cfg


def connect_mysql(host: str, port: int, user: str, password: str, database: str | None = None):
    conn = mysql.connector.connect(
        host=host,
        port=port,
        user=user,
        password=password,
        database=database,
        autocommit=False,
        charset='utf8mb4',
        collation='utf8mb4_0900_ai_ci' if _server_is8plus(host, port, user, password) else 'utf8mb4_general_ci',
    )
    return conn


def _server_is8plus(host: str, port: int, user: str, password: str) -> bool:
    try:
        tmp = mysql.connector.connect(host=host, port=port, user=user, password=password)
        cur = tmp.cursor()
        cur.execute("SELECT VERSION()")
        v = cur.fetchone()[0]
        cur.close(); tmp.close()
        major = int(str(v).split('.')[0])
        return major >= 8
    except Exception:
        return True


def ensure_database(cfg: Dict[str, str]) -> None:
    conn = connect_mysql(cfg["host"], cfg["port"], cfg["user"], cfg["password"], None)
    cur = conn.cursor()
    cur.execute(f"CREATE DATABASE IF NOT EXISTS `{cfg['database']}` CHARACTER SET utf8mb4 COLLATE utf8mb4_general_ci;")
    conn.commit()
    cur.close(); conn.close()


def infer_mysql_type(series: pd.Series, colname: str) -> str:
    name = colname.lower()
    # Campos ID y códigos
    if name.endswith('_id'):
        return 'VARCHAR(32)'
    if name.endswith('_code') or name.endswith('_bk'):
        return 'VARCHAR(64)'

    # Tiempo
    if name in ("anio", "year"):
        return 'SMALLINT'
    if name in ("trimestre", "quarter", "mes", "month"):
        return 'TINYINT'

    # Int / Float detección por dtype + contenido
    if pd.api.types.is_integer_dtype(series.dropna()):
        # Si valores son pequeños, SMALLINT/INT
        try:
            mx = int(series.dropna().astype(int).abs().max()) if not series.dropna().empty else 0
            if mx < 2**7:
                return 'TINYINT'
            if mx < 2**15:
                return 'SMALLINT'
            if mx < 2**31:
                return 'INT'
            return 'BIGINT'
        except Exception:
            return 'BIGINT'

    if pd.api.types.is_float_dtype(series.dropna()) or any(str(x).replace('.', '', 1).isdigit() for x in series.dropna().astype(str).tolist()):
        return 'DECIMAL(18,4)'

    # Texto
    # Longitudes típicas
    if any(k in name for k in ("descripcion", "description")):
        return 'TEXT'
    if any(k in name for k in ("categoria", "region", "rango", "periodo")):
        return 'VARCHAR(100)'

    return 'VARCHAR(255)'


def build_create_table_sql(table: str, df: pd.DataFrame) -> str:
    cols_sql: List[str] = []
    pkey: str | None = None

    for col in df.columns:
        col_sql_type = infer_mysql_type(df[col], col)
        not_null = ' NOT NULL' if col.endswith('_id') else ''
        cols_sql.append(f"  `{col}` {col_sql_type}{not_null}")
        if pkey is None and col.endswith('_id') and table.startswith('dim_'):
            pkey = col

    pkey_sql = f",\n  PRIMARY KEY (`{pkey}`)" if pkey else ""
    ddl = f"CREATE TABLE IF NOT EXISTS `{table}` (\n" + ",\n".join(cols_sql) + pkey_sql + "\n) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4;"
    return ddl


def build_indexes_sql(table: str, df: pd.DataFrame) -> List[str]:
    idx: List[str] = []
    # Índices por FKs
    for col in df.columns:
        if col.endswith('_id') and not (table.startswith('dim_') and df.columns[0] == col):
            idx.append(f"CREATE INDEX IF NOT EXISTS `idx_{table}_{col}` ON `{table}`(`{col}`);")
    # Índices por tiempo
    time_cols = [c for c in ("anio", "trimestre", "mes") if c in df.columns]
    if time_cols:
        idx.append(f"CREATE INDEX IF NOT EXISTS `idx_{table}_time` ON `{table}`(" + ", ".join(f"`{c}`" for c in time_cols) + ");")
    return idx


def sanitize_df(df: pd.DataFrame) -> pd.DataFrame:
    # Estandarizar tipos básicos y NaN -> None al insertar
    return df


def insert_rows(conn, table: str, df: pd.DataFrame, batch: int = 1000) -> int:
    if df.empty:
        return 0
    cur = conn.cursor()
    cols = list(df.columns)
    placeholders = ",".join(["%s"] * len(cols))
    sql = f"INSERT INTO `{table}` (" + ", ".join(f"`{c}`" for c in cols) + f") VALUES ({placeholders})"

    # Convertir DataFrame a lista de tuplas con None en lugar de NaN
    def _scalarize(x):
        if pd.isna(x):
            return None
        if isinstance(x, (pd.Timestamp, )):
            return x.to_pydatetime()
        # numpy -> python
        try:
            import numpy as np  # lazy
            if isinstance(x, (np.integer, )):
                return int(x)
            if isinstance(x, (np.floating, )):
                return float(x)
        except Exception:
            pass
        return x

    data = [tuple(_scalarize(v) for v in row) for row in df.itertuples(index=False, name=None)]

    total = 0
    for i in range(0, len(data), batch):
        chunk = data[i:i+batch]
        cur.executemany(sql, chunk)
        total += len(chunk)
    conn.commit()
    cur.close()
    return total


def main():
    print("==> Cargando CSV a MySQL")
    cfg = load_env()
    print(f"Destino: {cfg['user']}@{cfg['host']}:{cfg['port']} db={cfg['database']}")

    if not CSV_DIR.exists():
        print(f"No existe el directorio de CSV: {CSV_DIR}")
        sys.exit(1)

    ensure_database(cfg)
    conn = connect_mysql(cfg["host"], cfg["port"], cfg["user"], cfg["password"], cfg["database"]) 

    csv_files = sorted([p for p in CSV_DIR.glob("*.csv")])
    if not csv_files:
        print("No se encontraron CSV para cargar.")
        sys.exit(0)

    created_tables = 0
    inserted_rows = 0

    try:
        for csv in csv_files:
            table = csv.stem  # nombre de archivo sin .csv
            # Leer muestra para inferir correctamente
            df = pd.read_csv(csv)

            # Crear tabla si no existe
            ddl = build_create_table_sql(table, df)
            cur = conn.cursor()
            cur.execute(ddl)
            conn.commit()
            cur.close()
            created_tables += 1

            # Índices recomendados
            for idx_sql in build_indexes_sql(table, df):
                try:
                    cur = conn.cursor(); cur.execute(idx_sql); conn.commit(); cur.close()
                except Exception:
                    # Compatibilidad con versiones que no soportan IF NOT EXISTS en índices
                    pass

            # Insertar datos en lotes
            inserted = insert_rows(conn, table, df)
            inserted_rows += inserted
            print(f"  ✓ {table}: {inserted} filas")

    except Exception as e:
        print("ERROR durante la carga:", e)
        traceback.print_exc()
        try:
            conn.rollback()
        except Exception:
            pass
        sys.exit(1)
    finally:
        try:
            conn.close()
        except Exception:
            pass

    print("")
    print("Resumen:")
    print(f"  Tablas creadas/verificadas: {created_tables}")
    print(f"  Filas insertadas: {inserted_rows}")
    print("Hecho.")


if __name__ == "__main__":
    main()
