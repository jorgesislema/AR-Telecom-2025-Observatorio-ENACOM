"""
prepare_enacom.py
-----------------
Genera versiones normalizadas (para Tableau / BI) de dimensiones y tablas de hechos
basadas en los datos procesados existentes.

Salidas en data/processed/out:
- dim_provincias_norm.csv (ProvinciaNorm normalizada)
- dim_tiempo_norm.csv (anio/trimestre como enteros)
- dim_velocidades_ready.csv (velocidad estandarizada con orden y rango_key)
- dim_tecnologias_ready.csv (tec_key)
- fact_penetracion_provincias.csv
- fact_velocidad_media_provincias.csv
- fact_velocidad_numerica_provincias.csv (Velocidad_kbps)
- fact_velocidad_rangos_long.csv (rangos pivotados a largo)
- fact_tecnologias_long.csv (tecnologías pivotadas a largo)
"""
import os
import csv
import unicodedata
from pathlib import Path
import pandas as pd

BASE_DIR = Path(__file__).resolve().parent.parent
PROCESSED = BASE_DIR / "data" / "processed"
OUT = PROCESSED / "out"
OUT.mkdir(parents=True, exist_ok=True)


def normalize_text(s: str):
    if pd.isna(s):
        return s
    s2 = unicodedata.normalize("NFKD", str(s))
    s2 = "".join(ch for ch in s2 if not unicodedata.combining(ch))
    s2 = s2.replace("Ñ", "N").replace("ñ", "n")
    return s2.strip().upper()


def to_int(x):
    try:
        return int(x)
    except Exception:
        return pd.NA


def read_csv(path: Path, **kw):
    return pd.read_csv(path, encoding="utf-8", dtype=str, keep_default_na=False, na_values=["", "NA", "NaN"], **kw)


def write_csv(df: pd.DataFrame, path: Path):
    df.to_csv(path, index=False, encoding="utf-8", quoting=csv.QUOTE_MINIMAL)


# ---------- 1) DIMENSIONES ----------

def build_dim_provincias():
    p = PROCESSED / "dim_provincias.csv"
    if not p.exists():
        return
    dim = read_csv(p)
    # Asegurar nombre columna provincia
    if "provincia" not in dim.columns:
        for c in dim.columns:
            if c.lower() == "provincia":
                dim.rename(columns={c: "provincia"}, inplace=True)
                break
    if "provincia" in dim.columns:
        dim["ProvinciaNorm"] = dim["provincia"].apply(normalize_text)
    write_csv(dim, OUT / "dim_provincias_norm.csv")
    print("✔ dim_provincias_norm.csv")


def build_dim_tiempo():
    p = PROCESSED / "dim_tiempo.csv"
    if not p.exists():
        return
    dim = read_csv(p)
    for col in ("anio", "trimestre"):
        if col not in dim.columns:
            for c in dim.columns:
                if c.lower() == col:
                    dim.rename(columns={c: col}, inplace=True)
    if "anio" in dim.columns:
        dim["anio"] = dim["anio"].apply(to_int)
    if "trimestre" in dim.columns:
        dim["trimestre"] = dim["trimestre"].apply(to_int)
    write_csv(dim, OUT / "dim_tiempo_norm.csv")
    print("✔ dim_tiempo_norm.csv")


def build_dim_velocidades():
    p = PROCESSED / "dim_velocidades.csv"
    if not p.exists():
        return
    dim = read_csv(p)
    rename_map = {
        "velocidad_id": "velocidad_id",
        "rango_velocidad": "rango_velocidad",
        "velocidad_min_kbps": "vel_min_kbps",
        "velocidad_max_kbps": "vel_max_kbps",
    }
    dim.rename(columns=rename_map, inplace=True)
    for c in ("velocidad_id", "vel_min_kbps", "vel_max_kbps"):
        if c in dim.columns:
            dim[c] = pd.to_numeric(dim[c], errors="coerce").astype("Int64")
    if "orden" not in dim.columns and "velocidad_id" in dim.columns:
        dim["orden"] = dim["velocidad_id"]
    # rango_key limpio
    if "rango_velocidad" in dim.columns:
        dim["rango_key"] = (
            dim["rango_velocidad"].str.lower()
            .str.replace(" ", "", regex=False)
            .str.replace("≥", "mayor_a_", regex=False)
        )
    write_csv(dim, OUT / "dim_velocidades_ready.csv")
    print("✔ dim_velocidades_ready.csv")


def build_dim_tecnologias():
    p = PROCESSED / "dim_tecnologias.csv"
    if not p.exists():
        return
    dim = read_csv(p)
    if "tecnologia" in dim.columns:
        dim["tec_key"] = dim["tecnologia"].str.lower().str.replace(" ", "", regex=False)
    write_csv(dim, OUT / "dim_tecnologias_ready.csv")
    print("✔ dim_tecnologias_ready.csv")


# ---------- 2) HECHOS ----------

def add_common_keys(df: pd.DataFrame):
    for col in ("anio", "trimestre"):
        if col in df.columns:
            df[col] = pd.to_numeric(df[col], errors="coerce").astype("Int64")
    if "provincia" in df.columns:
        df["ProvinciaNorm"] = df["provincia"].apply(normalize_text)
    return df


def fact_penetracion_provincias():
    p = PROCESSED / "internet_accesos_penetracion_provincias_clean.csv"
    if not p.exists():
        return
    f = read_csv(p)
    f = add_common_keys(f)
    for c in ("accesos_cada_100_hogares", "accesos_cada_100_habitantes"):
        if c in f.columns:
            f[c] = pd.to_numeric(f[c], errors="coerce")
    write_csv(f, OUT / "fact_penetracion_provincias.csv")
    print("✔ fact_penetracion_provincias.csv")


def fact_velocidad_media_provincias():
    p = PROCESSED / "internet_velocidad_media_descarga_provincias_clean.csv"
    if not p.exists():
        return
    f = read_csv(p)
    f = add_common_keys(f)
    if "mbps" in f.columns:
        f["mbps"] = pd.to_numeric(f["mbps"], errors="coerce")
        # Intentar mapear a un rango velocidad_id usando dim_velocidades_ready si existe
        dim_path = OUT / "dim_velocidades_ready.csv"
        if dim_path.exists():
            dim = read_csv(dim_path)
            # Asegurar columnas clave
            if set(["velocidad_id","vel_min_kbps","vel_max_kbps"]).issubset(dim.columns):
                # Convertir mbps a kbps para comparar con min/max
                f_kbps = f["mbps"] * 1000
                dim["vel_min_kbps"] = pd.to_numeric(dim["vel_min_kbps"], errors="coerce")
                dim["vel_max_kbps"] = pd.to_numeric(dim["vel_max_kbps"], errors="coerce")
                # Construimos una función para asignar id
                import numpy as np
                def asignar_id(v_kbps):
                    if np.isnan(v_kbps):
                        return pd.NA
                    fila = dim[( (dim["vel_min_kbps"].isna()) | (v_kbps >= dim["vel_min_kbps"]) ) &
                               ( (dim["vel_max_kbps"].isna()) | (v_kbps < dim["vel_max_kbps"]) )]
                    if fila.empty:
                        return pd.NA
                    return int(fila.iloc[0]["velocidad_id"]) if not pd.isna(fila.iloc[0]["velocidad_id"]) else pd.NA
                f["velocidad_id"] = f_kbps.apply(asignar_id).astype("Int64")
    write_csv(f, OUT / "fact_velocidad_media_provincias.csv")
    print("✔ fact_velocidad_media_provincias.csv")


def fact_velocidad_numerica_provincias():
    p = PROCESSED / "internet_accesos_velocidad_provincias_clean.csv"
    if not p.exists():
        return
    f = read_csv(p)
    f = add_common_keys(f)
    if "velocidad" in f.columns:
        f["velocidad"] = pd.to_numeric(f["velocidad"], errors="coerce")
        # si es menor a 50 interpretamos Mbps y convertimos a kbps
        f["Velocidad_kbps"] = f["velocidad"].apply(lambda v: v * 1000 if pd.notna(v) and v < 50 else v)
        # Asignar velocidad_id (rango) usando dim_velocidades_ready
        dim_path = OUT / "dim_velocidades_ready.csv"
        if dim_path.exists():
            dim = read_csv(dim_path)
            if set(["velocidad_id","vel_min_kbps","vel_max_kbps"]).issubset(dim.columns):
                dim["vel_min_kbps"] = pd.to_numeric(dim["vel_min_kbps"], errors="coerce")
                dim["vel_max_kbps"] = pd.to_numeric(dim["vel_max_kbps"], errors="coerce")
                import numpy as np
                def asignar_id(v_kbps):
                    if np.isnan(v_kbps):
                        return pd.NA
                    fila = dim[( (dim["vel_min_kbps"].isna()) | (v_kbps >= dim["vel_min_kbps"]) ) &
                               ( (dim["vel_max_kbps"].isna()) | (v_kbps < dim["vel_max_kbps"]) )]
                    if fila.empty:
                        return pd.NA
                    return int(fila.iloc[0]["velocidad_id"]) if not pd.isna(fila.iloc[0]["velocidad_id"]) else pd.NA
                f["velocidad_id"] = f["Velocidad_kbps"].apply(asignar_id).astype("Int64")
    if "accesos" in f.columns:
        f["accesos"] = pd.to_numeric(f["accesos"], errors="coerce").fillna(0).astype("Int64")
    write_csv(f, OUT / "fact_velocidad_numerica_provincias.csv")
    print("✔ fact_velocidad_numerica_provincias.csv")


def fact_velocidad_rangos_long():
    p = PROCESSED / "internet_accesos_velocidad_rangos_provincias_clean.csv"
    if not p.exists():
        return
    f = read_csv(p)
    f = add_common_keys(f)
    cols_base = {"anio", "trimestre", "provincia", "ProvinciaNorm", "total"}
    rango_cols = [c for c in f.columns if c not in cols_base]
    long_df = f.melt(
        id_vars=[c for c in f.columns if c not in rango_cols],
        value_vars=rango_cols,
        var_name="rango_velocidad",
        value_name="accesos"
    )
    long_df["accesos"] = pd.to_numeric(long_df["accesos"], errors="coerce").fillna(0).astype("Int64")
    write_csv(long_df, OUT / "fact_velocidad_rangos_long.csv")
    print("✔ fact_velocidad_rangos_long.csv")


def fact_tecnologias_long():
    p = PROCESSED / "internet_accesos_tecnologias_provincias_clean.csv"
    if not p.exists():
        return
    f = read_csv(p)
    f = add_common_keys(f)
    cols_base = {"anio", "trimestre", "provincia", "ProvinciaNorm", "total"}
    tech_cols = [c for c in f.columns if c not in cols_base]
    long_df = f.melt(
        id_vars=[c for c in f.columns if c not in tech_cols],
        value_vars=tech_cols,
        var_name="tecnologia",
        value_name="accesos"
    )
    long_df["accesos"] = pd.to_numeric(long_df["accesos"], errors="coerce").fillna(0).astype("Int64")
    # Normalizamos similar al dim (el dim conserva acentos). Generamos una clave base sin espacios ni tildes para poder mapear.
    import unicodedata
    def strip_accents(s):
        if pd.isna(s):
            return s
        nk = unicodedata.normalize('NFKD', s)
        return ''.join(ch for ch in nk if not unicodedata.combining(ch))
    base_key = long_df["tecnologia"].str.lower().str.replace(" ", "", regex=False).apply(strip_accents)
    # Mapeos manuales a las claves del dim
    remap = {
        'fibraoptica': 'fibraóptica',
        'otros': 'otrosinternet',
        'telefonicabasica': 'telefoníabásica'
    }
    long_df["tec_key"] = base_key.replace(remap)
    # Enriquecer con tecnologia_id desde dim_tecnologias_ready si existe
    dim_path = OUT / "dim_tecnologias_ready.csv"
    if dim_path.exists():
        dim = read_csv(dim_path)
        if "tec_key" in dim.columns and "tecnologia_id" in dim.columns:
            dim_subset = dim[["tec_key", "tecnologia_id"]].drop_duplicates()
            long_df = long_df.merge(dim_subset, on="tec_key", how="left")
            if "tecnologia_id" in long_df.columns:
                long_df["tecnologia_id"] = pd.to_numeric(long_df["tecnologia_id"], errors="coerce").astype("Int64")
    write_csv(long_df, OUT / "fact_tecnologias_long.csv")
    print("✔ fact_tecnologias_long.csv")


def main():
    print("🚀 Generando datasets normalizados para Tableau...")
    # Dimensiones
    build_dim_provincias()
    build_dim_tiempo()
    build_dim_velocidades()
    build_dim_tecnologias()
    # Hechos
    fact_penetracion_provincias()
    fact_velocidad_media_provincias()
    fact_velocidad_numerica_provincias()
    fact_velocidad_rangos_long()
    fact_tecnologias_long()
    print("🎯 Finalizado. Archivos en:", OUT)


if __name__ == "__main__":
    main()
