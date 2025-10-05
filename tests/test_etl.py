"""
Tests básicos para validar el proceso ETL
"""
import pytest
import pandas as pd
from pathlib import Path
import sys

# Agregar el directorio del proyecto al path
sys.path.append(str(Path(__file__).parent.parent))

# Configuración de rutas
BASE_DIR = Path(__file__).resolve().parent.parent
RAW_DIR = BASE_DIR / 'data' / 'raw' / 'enacom'
PROCESSED_DIR = BASE_DIR / 'data' / 'processed'
OUT_DIR = PROCESSED_DIR / 'out'


class TestETLBasico:
    """Tests básicos para el proceso ETL"""
    
    def test_directorio_raw_existe(self):
        """Verifica que el directorio raw existe y contiene archivos"""
        assert RAW_DIR.exists(), f"Directorio raw no encontrado: {RAW_DIR}"
        archivos_csv = list(RAW_DIR.glob('*.csv'))
        assert len(archivos_csv) > 0, "No se encontraron archivos CSV en raw"
    
    def test_directorio_processed_creado(self):
        """Verifica que el directorio processed existe"""
        assert PROCESSED_DIR.exists(), f"Directorio processed no encontrado: {PROCESSED_DIR}"
    
    def test_archivos_procesados_existen(self):
        """Verifica que existen archivos procesados después del ETL"""
        # Ejecutar ETL si no hay archivos procesados
        archivos_clean = list(PROCESSED_DIR.glob('*_clean.csv'))
        if len(archivos_clean) == 0:
            # Importar y ejecutar ETL
            try:
                from pipelines.etl_principal import main
                main()
                archivos_clean = list(PROCESSED_DIR.glob('*_clean.csv'))
            except ImportError:
                pytest.skip("No se pudo importar el módulo ETL")
        
        assert len(archivos_clean) > 0, "No se encontraron archivos procesados"
    
    def test_resumen_datos_generado(self):
        """Verifica que se generó el archivo de resumen"""
        resumen_path = PROCESSED_DIR / 'resumen_datos.csv'
        
        # Generar resumen si no existe
        if not resumen_path.exists():
            try:
                from pipelines.etl_principal import generar_resumen_datos
                generar_resumen_datos()
            except ImportError:
                pytest.skip("No se pudo importar el módulo ETL")
        
        assert resumen_path.exists(), "Archivo de resumen no encontrado"
        
        # Verificar contenido del resumen
        resumen = pd.read_csv(resumen_path)
        assert len(resumen) > 0, "Archivo de resumen está vacío"
        assert 'archivo' in resumen.columns, "Columna 'archivo' faltante en resumen"
        assert 'filas' in resumen.columns, "Columna 'filas' faltante en resumen"
    
    def test_calidad_datos_basica(self):
        """Tests básicos de calidad de datos procesados"""
        archivos_clean = list(PROCESSED_DIR.glob('*_clean.csv'))
        
        if len(archivos_clean) == 0:
            pytest.skip("No hay archivos procesados para validar")
        
        # Probar algunos archivos
        for archivo in archivos_clean[:3]:  # Solo los primeros 3 para no sobrecargar
            df = pd.read_csv(archivo)
            
            # Verificaciones básicas
            assert len(df) > 0, f"Archivo {archivo.name} está vacío"
            assert len(df.columns) > 0, f"Archivo {archivo.name} no tiene columnas"
            
            # Verificar que las columnas están en formato snake_case
            for col in df.columns:
                assert col.islower(), f"Columna {col} en {archivo.name} no está en lowercase"
                assert ' ' not in col, f"Columna {col} en {archivo.name} contiene espacios"
    
    def test_provincias_normalizadas(self):
        """Verifica que las provincias están normalizadas en archivos que las contienen"""
        archivos_provinciales = [f for f in PROCESSED_DIR.glob('*provincias*_clean.csv')]
        
        for archivo in archivos_provinciales:
            df = pd.read_csv(archivo)
            
            if 'provincia' in df.columns:
                provincias = df['provincia'].dropna().unique()
                
                # Verificar que no hay espacios extra
                for prov in provincias:
                    if isinstance(prov, str):
                        assert prov == prov.strip(), f"Provincia con espacios: '{prov}'"
                        assert prov.istitle(), f"Provincia no capitalizada: '{prov}'"


class TestTablasDimensionales:
    """Tests específicos para validar tablas dimensionales"""
    
    def test_dim_provincias_existe(self):
        """Verifica que la tabla dim_provincias existe y tiene estructura correcta"""
        dim_provincias_path = PROCESSED_DIR / 'dim_provincias.csv'
        assert dim_provincias_path.exists(), "Tabla dim_provincias.csv no encontrada"
        
        df = pd.read_csv(dim_provincias_path)
        columnas_esperadas = ['provincia_id', 'provincia', 'region', 'poblacion_2023', 'superficie_km2']
        
        for col in columnas_esperadas:
            assert col in df.columns, f"Columna {col} faltante en dim_provincias"
        
        assert len(df) == 24, f"Se esperaban 24 provincias, se encontraron {len(df)}"
        assert df['provincia_id'].is_unique, "IDs de provincia no son únicos"
    
    def test_dim_tiempo_existe(self):
        """Verifica que la tabla dim_tiempo existe y tiene estructura correcta"""
        dim_tiempo_path = PROCESSED_DIR / 'dim_tiempo.csv'
        assert dim_tiempo_path.exists(), "Tabla dim_tiempo.csv no encontrada"
        
        df = pd.read_csv(dim_tiempo_path)
        columnas_esperadas = ['anio', 'trimestre', 'mes_inicio', 'mes_fin', 'periodo_completo']
        
        for col in columnas_esperadas:
            assert col in df.columns, f"Columna {col} faltante en dim_tiempo"
        
        # Verificar que los trimestres son válidos (1-4)
        assert df['trimestre'].min() >= 1, "Trimestre inválido encontrado"
        assert df['trimestre'].max() <= 4, "Trimestre inválido encontrado"
    
    def test_dim_tecnologias_existe(self):
        """Verifica que la tabla dim_tecnologias existe y tiene estructura correcta"""
        dim_tecnologias_path = PROCESSED_DIR / 'dim_tecnologias.csv'
        assert dim_tecnologias_path.exists(), "Tabla dim_tecnologias.csv no encontrada"
        
        df = pd.read_csv(dim_tecnologias_path)
        columnas_esperadas = ['tecnologia_id', 'tecnologia', 'categoria', 'descripcion']
        
        for col in columnas_esperadas:
            assert col in df.columns, f"Columna {col} faltante en dim_tecnologias"
        
        assert df['tecnologia_id'].is_unique, "IDs de tecnología no son únicos"
        categorias_esperadas = ['Internet Fijo', 'Móvil', 'Telefonía Fija', 'TV Paga', 'TV Abierta']
        categorias_encontradas = df['categoria'].unique()
        for cat in categorias_esperadas:
            assert cat in categorias_encontradas, f"Categoría {cat} no encontrada"
    
    def test_dim_velocidades_existe(self):
        """Verifica que la tabla dim_velocidades existe y tiene estructura correcta"""
        dim_velocidades_path = PROCESSED_DIR / 'dim_velocidades.csv'
        assert dim_velocidades_path.exists(), "Tabla dim_velocidades.csv no encontrada"
        
        df = pd.read_csv(dim_velocidades_path)
        columnas_esperadas = ['velocidad_id', 'rango_velocidad', 'velocidad_min_kbps', 'velocidad_max_kbps']
        
        for col in columnas_esperadas:
            assert col in df.columns, f"Columna {col} faltante en dim_velocidades"
        
        assert df['velocidad_id'].is_unique, "IDs de velocidad no son únicos"
    
    def test_dim_localidades_existe(self):
        """Verifica que la tabla dim_localidades existe y tiene estructura correcta"""
        dim_localidades_path = PROCESSED_DIR / 'dim_localidades.csv'
        assert dim_localidades_path.exists(), "Tabla dim_localidades.csv no encontrada"
        
        df = pd.read_csv(dim_localidades_path)
        columnas_esperadas = ['localidad_id', 'provincia', 'partido', 'localidad', 'link_indec']
        
        for col in columnas_esperadas:
            assert col in df.columns, f"Columna {col} faltante en dim_localidades"
        
        assert df['localidad_id'].is_unique, "IDs de localidad no son únicos"


class TestOutputsTableau:
    """Valida los archivos generados por prepare_enacom.py para Tableau"""
    def test_out_dir_exists(self):
        assert OUT_DIR.exists(), "Directorio out no existe. Ejecutar pipelines/prepare_enacom.py"

    def test_dim_provincias_norm(self):
        p = OUT_DIR / 'dim_provincias_norm.csv'
        assert p.exists(), 'dim_provincias_norm.csv faltante'
        df = pd.read_csv(p)
        assert 'ProvinciaNorm' in df.columns, 'Columna ProvinciaNorm faltante'
        assert df['ProvinciaNorm'].str.len().gt(0).all(), 'Valores vacíos en ProvinciaNorm'

    def test_dim_tiempo_norm(self):
        p = OUT_DIR / 'dim_tiempo_norm.csv'
        assert p.exists(), 'dim_tiempo_norm.csv faltante'
        df = pd.read_csv(p)
        for col in ['anio','trimestre']:
            assert col in df.columns, f'Columna {col} faltante'
            assert pd.to_numeric(df[col], errors='coerce').notna().all(), f'Valores no numéricos en {col}'

    def test_dim_velocidades_ready(self):
        p = OUT_DIR / 'dim_velocidades_ready.csv'
        assert p.exists(), 'dim_velocidades_ready.csv faltante'
        df = pd.read_csv(p)
        for col in ['rango_key','orden']:
            assert col in df.columns, f'Columna {col} faltante'
        assert df['orden'].is_monotonic_increasing or df['orden'].is_monotonic_decreasing, 'Orden no consistente'

    def test_fact_tecnologias_long(self):
        p = OUT_DIR / 'fact_tecnologias_long.csv'
        assert p.exists(), 'fact_tecnologias_long.csv faltante'
        df = pd.read_csv(p)
        for col in ['anio','trimestre','ProvinciaNorm','tecnologia','accesos']:
            assert col in df.columns, f'Columna {col} faltante'
        assert pd.to_numeric(df['accesos'], errors='coerce').notna().all(), 'Accesos contiene valores no numéricos'

    def test_fact_velocidad_rangos_long(self):
        p = OUT_DIR / 'fact_velocidad_rangos_long.csv'
        assert p.exists(), 'fact_velocidad_rangos_long.csv faltante'
        df = pd.read_csv(p)
        for col in ['anio','trimestre','ProvinciaNorm','rango_velocidad','accesos']:
            assert col in df.columns, f'Columna {col} faltante'
        assert pd.to_numeric(df['accesos'], errors='coerce').notna().all(), 'Accesos contiene valores no numéricos'

    def test_fact_velocidad_media_provincias(self):
        p = OUT_DIR / 'fact_velocidad_media_provincias.csv'
        assert p.exists(), 'fact_velocidad_media_provincias.csv faltante'
        df = pd.read_csv(p, nrows=200)
        for col in ['anio','trimestre','ProvinciaNorm','mbps']:
            assert col in df.columns, f'Columna {col} faltante'
        assert 'velocidad_id' in df.columns, 'Columna velocidad_id faltante'
        # Verificar que velocidad_id es consistente con mbps (no NA donde mbps tiene valor)
        subset = df[df['mbps'].notna() & (df['mbps'] != '')]
        # Permitir algunos NA si están fuera de rango, pero no más del 20%
        ratio_na = subset['velocidad_id'].isna().mean() if len(subset) else 0
        assert ratio_na <= 0.2, f'Demasiados NA en velocidad_id ({ratio_na:.2%})'

    def test_fact_velocidad_numerica_provincias(self):
        p = OUT_DIR / 'fact_velocidad_numerica_provincias.csv'
        assert p.exists(), 'fact_velocidad_numerica_provincias.csv faltante'
        df = pd.read_csv(p, nrows=300)
        for col in ['anio','trimestre','ProvinciaNorm','Velocidad_kbps','accesos']:
            assert col in df.columns, f'Columna {col} faltante'
        assert 'velocidad_id' in df.columns, 'Columna velocidad_id faltante'
        subset = df[df['Velocidad_kbps'].notna()]
        ratio_na = subset['velocidad_id'].isna().mean() if len(subset) else 0
        assert ratio_na <= 0.2, f'Demasiados NA en velocidad_id numerica ({ratio_na:.2%})'


class TestFactUnificado:
    """Validaciones de la tabla de hechos unificada"""
    def test_fact_unificado_existe(self):
        p = OUT_DIR / 'fact_unificado_long.csv'
        assert p.exists(), 'fact_unificado_long.csv no generado. Ejecutar build_fact_unificado.py'
        df = pd.read_csv(p, nrows=50)
        # Columnas esenciales
        for col in ['anio','dominio','subcategoria','variable','valor','fuente_archivo']:
            assert col in df.columns, f'Columna {col} faltante en fact_unificado_long'
    
class TestDiccionarioMetricas:
    def test_diccionario_metricas(self):
        path = BASE_DIR / 'data' / 'processed' / 'out' / 'diccionario_metricas.csv'
        if not path.exists():
            pytest.skip('diccionario_metricas.csv aún no generado')
        df = pd.read_csv(path)
        required = ['dominio','subcategoria','variable','unidad_inferida','archivos_fuente','observaciones']
        for col in required:
            assert col in df.columns
        # Cada fila debe ser única por la clave compuesta dominio-subcategoria-variable
        assert len(df[['dominio','subcategoria','variable']].drop_duplicates()) == len(df)
        # chequeo básico de rango años si existe
        if 'anios_min_max' in df.columns:
            import re
            for v in df['anios_min_max'].astype(str):
                if v in ('', 'nan'):
                    continue
                assert re.match(r'^\d{4}-\d{4}$', v), f'Formato anios_min_max invalido: {v}'

    def test_fact_unificado_parquet(self):
        p = OUT_DIR / 'fact_unificado_long.parquet'
        assert p.exists(), 'Parquet faltante (instalar pyarrow y re-ejecutar si se requiere)'
    
    def test_fact_unificado_dominios(self):
        import pandas as _pd
        p = OUT_DIR / 'fact_unificado_long.csv'
        df = _pd.read_csv(p, usecols=['dominio']).dropna()
        dominios = set(df['dominio'].unique())
        esperados = {'Internet','Movil','TelefoniaFija','TV','Postal','Portabilidad','Otros'}
        assert dominios.issubset(esperados), f'Dominios inesperados: {dominios - esperados}'


def test_estructura_proyecto():
    """Verifica que la estructura básica del proyecto existe"""
    assert (BASE_DIR / 'notebooks').exists(), "Directorio notebooks no encontrado"
    assert (BASE_DIR / 'pipelines').exists(), "Directorio pipelines no encontrado"
    assert (BASE_DIR / 'tests').exists(), "Directorio tests no encontrado"
    assert (BASE_DIR / 'requirements.txt').exists(), "Archivo requirements.txt no encontrado"
    assert (BASE_DIR / 'README.md').exists(), "Archivo README.md no encontrado"


if __name__ == "__main__":
    # Ejecutar tests directamente
    pytest.main([__file__, "-v"])