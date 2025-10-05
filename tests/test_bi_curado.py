"""
test_bi_curado.py
-----------------
Tests para validar el modelo BI curado generado por etl_bi_curado.py
"""

import pytest
import pandas as pd
from pathlib import Path

BASE_DIR = Path(__file__).resolve().parent.parent
BI_DIR = BASE_DIR / 'data' / 'processed' / 'bi'


class TestModeloBICurado:
    """Tests para validar el modelo BI curado"""
    
    def test_directorio_bi_existe(self):
        """Verifica que el directorio BI existe"""
        assert BI_DIR.exists(), f"Directorio BI no encontrado: {BI_DIR}"
    
    def test_dimensiones_existen(self):
        """Verifica que todas las dimensiones existen"""
        dimensiones = [
            'dim_provincias.csv',
            'dim_tiempo.csv', 
            'dim_tecnologias.csv',
            'dim_velocidades.csv'
        ]
        
        for dim in dimensiones:
            path = BI_DIR / dim
            assert path.exists(), f"Dimension faltante: {dim}"
    
    def test_hechos_existen(self):
        """Verifica que todas las tablas de hechos existen"""
        hechos = [
            'fact_internet_accesos.csv',
            'fact_internet_velocidad.csv',
            'fact_telefonia_accesos.csv',
            'fact_movil_accesos.csv'
        ]
        
        for fact in hechos:
            path = BI_DIR / fact
            assert path.exists(), f"Tabla de hechos faltante: {fact}"
    
    def test_dim_provincias_estructura(self):
        """Valida estructura de dim_provincias"""
        df = pd.read_csv(BI_DIR / 'dim_provincias.csv')
        
        columnas_esperadas = ['provincia_id', 'provincia', 'region', 'poblacion_2023', 'superficie_km2']
        for col in columnas_esperadas:
            assert col in df.columns, f"Columna faltante en dim_provincias: {col}"
        
        assert len(df) == 24, f"Se esperaban 24 provincias, encontradas: {len(df)}"
        assert df['provincia_id'].is_unique, "IDs de provincia no son unicos"
        assert df['provincia_id'].min() == 1, "provincia_id debe empezar en 1"
        assert df['provincia_id'].max() == 24, "provincia_id debe terminar en 24"
    
    def test_dim_tiempo_estructura(self):
        """Valida estructura de dim_tiempo"""
        df = pd.read_csv(BI_DIR / 'dim_tiempo.csv')
        
        columnas_esperadas = ['tiempo_id', 'anio', 'trimestre', 'periodo']
        for col in columnas_esperadas:
            assert col in df.columns, f"Columna faltante en dim_tiempo: {col}"
        
        assert df['tiempo_id'].is_unique, "IDs de tiempo no son unicos"
        assert df['trimestre'].min() >= 1, "Trimestre minimo debe ser 1"
        assert df['trimestre'].max() <= 4, "Trimestre maximo debe ser 4"
        assert df['anio'].min() >= 2014, "Año minimo debe ser 2014"
    
    def test_dim_velocidades_estructura(self):
        """Valida estructura de dim_velocidades"""
        df = pd.read_csv(BI_DIR / 'dim_velocidades.csv')
        
        columnas_esperadas = ['velocidad_id', 'rango_velocidad', 'velocidad_min_kbps', 'velocidad_max_kbps']
        for col in columnas_esperadas:
            assert col in df.columns, f"Columna faltante en dim_velocidades: {col}"
        
        assert df['velocidad_id'].is_unique, "IDs de velocidad no son unicos"
        assert len(df) > 0, "dim_velocidades no puede estar vacia"
    
    def test_fact_internet_velocidad_relaciones(self):
        """Valida relaciones en fact_internet_velocidad"""
        fact = pd.read_csv(BI_DIR / 'fact_internet_velocidad.csv')
        dim_provincias = pd.read_csv(BI_DIR / 'dim_provincias.csv')
        dim_tiempo = pd.read_csv(BI_DIR / 'dim_tiempo.csv')
        dim_velocidades = pd.read_csv(BI_DIR / 'dim_velocidades.csv')
        
        columnas_esperadas = ['tiempo_id', 'provincia_id', 'velocidad_id', 'mbps']
        for col in columnas_esperadas:
            assert col in fact.columns, f"Columna faltante en fact_internet_velocidad: {col}"
        
        # Validar foreign keys
        provincia_ids_validos = set(dim_provincias['provincia_id'])
        fact_provincia_ids = set(fact['provincia_id'].dropna())
        assert fact_provincia_ids.issubset(provincia_ids_validos), "IDs de provincia invalidos en fact"
        
        tiempo_ids_validos = set(dim_tiempo['tiempo_id'])
        fact_tiempo_ids = set(fact['tiempo_id'].dropna())
        assert fact_tiempo_ids.issubset(tiempo_ids_validos), "IDs de tiempo invalidos en fact"
        
        velocidad_ids_validos = set(dim_velocidades['velocidad_id'])
        fact_velocidad_ids = set(fact['velocidad_id'].dropna())
        assert fact_velocidad_ids.issubset(velocidad_ids_validos), "IDs de velocidad invalidos en fact"
    
    def test_fact_telefonia_accesos_estructura(self):
        """Valida estructura de fact_telefonia_accesos"""
        fact = pd.read_csv(BI_DIR / 'fact_telefonia_accesos.csv')
        
        columnas_esperadas = ['tiempo_id', 'provincia_id', 'hogares', 'comercial', 'gobierno', 'total']
        for col in columnas_esperadas:
            assert col in fact.columns, f"Columna faltante en fact_telefonia_accesos: {col}"
        
        assert len(fact) > 0, "fact_telefonia_accesos no puede estar vacia"
        
        # Verificar que los valores numericos son positivos o cero
        for col in ['hogares', 'comercial', 'gobierno', 'total']:
            valores_numericos = pd.to_numeric(fact[col], errors='coerce')
            assert (valores_numericos >= 0).all() or valores_numericos.isna().all(), f"Valores negativos en {col}"
    
    def test_fact_movil_accesos_estructura(self):
        """Valida estructura de fact_movil_accesos"""
        fact = pd.read_csv(BI_DIR / 'fact_movil_accesos.csv')
        
        columnas_esperadas = ['tiempo_id', 'pospago', 'prepago', 'operativos']
        for col in columnas_esperadas:
            assert col in fact.columns, f"Columna faltante en fact_movil_accesos: {col}"
        
        assert len(fact) > 0, "fact_movil_accesos no puede estar vacia"


if __name__ == "__main__":
    pytest.main([__file__, "-v"])