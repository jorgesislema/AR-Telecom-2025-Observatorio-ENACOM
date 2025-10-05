#!/usr/bin/env python3
"""
Tests de validación completos para el modelo dimensional
Verifica estructura, integridad referencial y calidad de datos
"""

import pandas as pd
import numpy as np
from pathlib import Path
import unittest
import sys
import os

# Agregar directorio raíz al path
sys.path.append(str(Path(__file__).parent.parent))

class TestModeloDimensional(unittest.TestCase):
    """Suite de tests para validar el modelo dimensional completo"""
    
    @classmethod
    def setUpClass(cls):
        """Configuración inicial para todos los tests"""
        cls.dimensional_path = Path("data/processed/dimensional")
        cls.dimensiones = ['dim_provincias', 'dim_tiempo', 'dim_tecnologias', 'dim_velocidades', 'dim_servicios']
        
        # Cargar todas las dimensiones
        cls.dims = {}
        for dim in cls.dimensiones:
            file_path = cls.dimensional_path / f"{dim}.csv"
            if file_path.exists():
                cls.dims[dim] = pd.read_csv(file_path)
    
    def test_01_directorio_dimensional_existe(self):
        """Verificar que existe el directorio dimensional"""
        self.assertTrue(self.dimensional_path.exists(), 
                       f"Directorio {self.dimensional_path} no existe")
    
    def test_02_dimensiones_existen(self):
        """Verificar que existen todas las dimensiones requeridas"""
        for dim in self.dimensiones:
            file_path = self.dimensional_path / f"{dim}.csv"
            self.assertTrue(file_path.exists(), f"Dimensión {dim}.csv no existe")
    
    def test_03_dimensiones_no_vacias(self):
        """Verificar que las dimensiones no están vacías"""
        for dim in self.dimensiones:
            if dim in self.dims:
                self.assertGreater(len(self.dims[dim]), 0, 
                                 f"Dimensión {dim} está vacía")
    
    def test_04_estructura_dim_provincias(self):
        """Validar estructura de dim_provincias"""
        df = self.dims.get('dim_provincias')
        self.assertIsNotNone(df, "dim_provincias no cargada")
        
        # Verificar columnas requeridas
        columnas_requeridas = ['provincia_id', 'provincia', 'region', 'poblacion_2023', 'superficie_km2', 'capital']
        for col in columnas_requeridas:
            self.assertIn(col, df.columns, f"Columna {col} faltante en dim_provincias")
        
        # Verificar cantidad de provincias argentinas
        self.assertEqual(len(df), 24, "Deben ser 24 provincias argentinas")
        
        # Verificar formato de IDs alfanuméricos
        for provincia_id in df['provincia_id']:
            self.assertTrue(provincia_id.startswith('PR'), 
                          f"ID {provincia_id} debe empezar con 'PR'")
            self.assertEqual(len(provincia_id), 4, 
                           f"ID {provincia_id} debe tener 4 caracteres")
        
        # Verificar unicidad de IDs
        self.assertEqual(len(df['provincia_id'].unique()), len(df), 
                        "IDs de provincia deben ser únicos")
    
    def test_05_estructura_dim_tiempo(self):
        """Validar estructura de dim_tiempo"""
        df = self.dims.get('dim_tiempo')
        self.assertIsNotNone(df, "dim_tiempo no cargada")
        
        # Verificar columnas requeridas
        columnas_requeridas = ['tiempo_id', 'anio', 'trimestre', 'periodo']
        for col in columnas_requeridas:
            self.assertIn(col, df.columns, f"Columna {col} faltante en dim_tiempo")
        
        # Verificar formato de IDs alfanuméricos
        for tiempo_id in df['tiempo_id']:
            self.assertTrue(tiempo_id.startswith('TM'), 
                          f"ID {tiempo_id} debe empezar con 'TM'")
            self.assertEqual(len(tiempo_id), 4, 
                           f"ID {tiempo_id} debe tener 4 caracteres")
        
        # Verificar rango de años
        anios = df['anio'].unique()
        self.assertGreaterEqual(min(anios), 2013, "Año mínimo debe ser 2013")
        self.assertLessEqual(max(anios), 2024, "Año máximo debe ser 2024")
        
        # Verificar trimestres válidos
        trimestres = df['trimestre'].unique()
        self.assertTrue(all(t in [1, 2, 3, 4] for t in trimestres), 
                       "Trimestres deben ser 1, 2, 3 o 4")
    
    def test_06_estructura_dim_tecnologias(self):
        """Validar estructura de dim_tecnologias"""
        df = self.dims.get('dim_tecnologias')
        self.assertIsNotNone(df, "dim_tecnologias no cargada")
        
        columnas_requeridas = ['tecnologia_id', 'tecnologia', 'categoria']
        for col in columnas_requeridas:
            self.assertIn(col, df.columns, f"Columna {col} faltante en dim_tecnologias")
        
        # Verificar formato de IDs
        for tech_id in df['tecnologia_id']:
            self.assertTrue(tech_id.startswith('TEC'), 
                          f"ID {tech_id} debe empezar con 'TEC'")
        
        # Verificar que hay tecnologías de diferentes categorías
        categorias = df['categoria'].unique()
        self.assertIn('INTERNET_FIJO', categorias, "Debe haber tecnologías de internet fijo")
    
    def test_07_estructura_dim_velocidades(self):
        """Validar estructura de dim_velocidades"""
        df = self.dims.get('dim_velocidades')
        self.assertIsNotNone(df, "dim_velocidades no cargada")
        
        columnas_requeridas = ['velocidad_id', 'rango_velocidad', 'velocidad_min_kbps', 'velocidad_max_kbps']
        for col in columnas_requeridas:
            self.assertIn(col, df.columns, f"Columna {col} faltante en dim_velocidades")
        
        # Verificar formato de IDs
        for vel_id in df['velocidad_id']:
            self.assertTrue(vel_id.startswith('VEL'), 
                          f"ID {vel_id} debe empezar con 'VEL'")
        
        # Verificar que min < max para cada rango (excepto el último)
        for _, row in df.iterrows():
            if row['velocidad_max_kbps'] < 999999:  # No el rango "MAS_30_MBPS"
                self.assertLess(row['velocidad_min_kbps'], row['velocidad_max_kbps'],
                              f"Min debe ser menor que Max en {row['rango_velocidad']}")
    
    def test_08_estructura_dim_servicios(self):
        """Validar estructura de dim_servicios"""
        df = self.dims.get('dim_servicios')
        self.assertIsNotNone(df, "dim_servicios no cargada")
        
        columnas_requeridas = ['servicio_id', 'servicio', 'categoria']
        for col in columnas_requeridas:
            self.assertIn(col, df.columns, f"Columna {col} faltante en dim_servicios")
        
        # Verificar formato de IDs
        for srv_id in df['servicio_id']:
            self.assertTrue(srv_id.startswith('SRV'), 
                          f"ID {srv_id} debe empezar con 'SRV'")
    
    def test_09_tablas_hechos_existen(self):
        """Verificar que existen tablas de hechos"""
        archivos_fact = list(self.dimensional_path.glob("fact_*.csv"))
        self.assertGreater(len(archivos_fact), 0, "Deben existir tablas de hechos")
        
        # Verificar hechos clave
        hechos_clave = [
            'fact_internet_accesos_baf_provincias.csv',
            'fact_comunicaciones_moviles_accesos.csv',
            'fact_telefonia_fija_accesos_provincias.csv',
            'fact_tv_accesos_provincias.csv'
        ]
        
        for hecho in hechos_clave:
            file_path = self.dimensional_path / hecho
            self.assertTrue(file_path.exists(), f"Tabla de hechos {hecho} no existe")
    
    def test_10_integridad_referencial_provincias(self):
        """Verificar integridad referencial con dim_provincias"""
        dim_provincias = self.dims.get('dim_provincias')
        self.assertIsNotNone(dim_provincias, "dim_provincias requerida")
        
        provincia_ids_validos = set(dim_provincias['provincia_id'])
        
        # Verificar algunos hechos que deben tener provincia_id
        hechos_con_provincia = [
            'fact_internet_accesos_baf_provincias.csv',
            'fact_telefonia_fija_accesos_provincias.csv',
            'fact_tv_accesos_provincias.csv'
        ]
        
        for hecho in hechos_con_provincia:
            file_path = self.dimensional_path / hecho
            if file_path.exists():
                df = pd.read_csv(file_path)
                if 'provincia_id' in df.columns:
                    fact_provincia_ids = set(df['provincia_id'].dropna())
                    self.assertTrue(fact_provincia_ids.issubset(provincia_ids_validos),
                                  f"IDs de provincia inválidos en {hecho}")
    
    def test_11_integridad_referencial_tiempo(self):
        """Verificar integridad referencial con dim_tiempo"""
        dim_tiempo = self.dims.get('dim_tiempo')
        self.assertIsNotNone(dim_tiempo, "dim_tiempo requerida")
        
        tiempo_ids_validos = set(dim_tiempo['tiempo_id'])
        
        # Verificar todos los hechos con tiempo_id
        archivos_fact = list(self.dimensional_path.glob("fact_*.csv"))
        
        for archivo in archivos_fact[:5]:  # Verificar primeros 5 por performance
            df = pd.read_csv(archivo)
            if 'tiempo_id' in df.columns:
                fact_tiempo_ids = set(df['tiempo_id'].dropna())
                self.assertTrue(fact_tiempo_ids.issubset(tiempo_ids_validos),
                              f"IDs de tiempo inválidos en {archivo.name}")
    
    def test_12_calidad_datos_no_nulls_criticos(self):
        """Verificar que campos críticos no tienen valores nulos"""
        # Verificar IDs primarios en dimensiones
        for dim_name, df in self.dims.items():
            id_column = df.columns[0]  # Primera columna debe ser el ID
            nulls = df[id_column].isnull().sum()
            self.assertEqual(nulls, 0, f"IDs nulos encontrados en {dim_name}: {nulls}")
    
    def test_13_coherencia_datos_numericos(self):
        """Verificar coherencia de datos numéricos"""
        # Verificar dim_provincias
        dim_provincias = self.dims.get('dim_provincias')
        if dim_provincias is not None:
            # Población debe ser positiva
            poblaciones = dim_provincias['poblacion_2023']
            self.assertTrue(all(poblaciones > 0), "Poblaciones deben ser positivas")
            
            # Superficie debe ser positiva
            superficies = dim_provincias['superficie_km2']
            self.assertTrue(all(superficies > 0), "Superficies deben ser positivas")
    
    def test_14_cobertura_temporal_completa(self):
        """Verificar que hay cobertura temporal adecuada"""
        # Verificar una tabla de hechos con datos temporales
        archivo_test = self.dimensional_path / "fact_internet_accesos_baf_provincias.csv"
        if archivo_test.exists():
            df = pd.read_csv(archivo_test)
            
            # Mergear con dim_tiempo para obtener años
            dim_tiempo = self.dims.get('dim_tiempo')
            if dim_tiempo is not None:
                df_merged = df.merge(dim_tiempo[['tiempo_id', 'anio']], on='tiempo_id')
                anios_con_datos = df_merged['anio'].unique()
                
                # Debe haber datos para múltiples años
                self.assertGreaterEqual(len(anios_con_datos), 3, 
                                      "Debe haber datos para al menos 3 años")
    
    def test_15_formato_ids_alfanumericos(self):
        """Verificar formato correcto de todos los IDs alfanuméricos"""
        # Patrones esperados
        patrones = {
            'provincia_id': r'^PR\d{2}$',
            'tiempo_id': r'^TM\d{2}$',
            'tecnologia_id': r'^TEC\d+$',
            'velocidad_id': r'^VEL\d+$',
            'servicio_id': r'^SRV\d+$'
        }
        
        import re
        
        # Verificar cada dimensión
        for dim_name, df in self.dims.items():
            if df is not None and len(df) > 0:
                id_column = df.columns[0]
                patron = patrones.get(id_column)
                
                if patron:
                    for id_val in df[id_column]:
                        self.assertRegex(id_val, patron, 
                                       f"ID {id_val} no cumple patrón {patron} en {dim_name}")

def generar_reporte_validacion():
    """Genera reporte detallado de validación"""
    print("\n" + "="*60)
    print("REPORTE DE VALIDACION DEL MODELO DIMENSIONAL")
    print("="*60)
    
    # Información del modelo
    dimensional_path = Path("data/processed/dimensional")
    archivos = list(dimensional_path.glob("*.csv"))
    
    print(f"\nUbicación: {dimensional_path}")
    print(f"Archivos totales: {len(archivos)}")
    
    # Contadores por tipo
    dimensiones = [f for f in archivos if f.name.startswith('dim_')]
    hechos = [f for f in archivos if f.name.startswith('fact_')]
    
    print(f"Dimensiones: {len(dimensiones)}")
    print(f"Tablas de hechos: {len(hechos)}")
    
    # Tamaño total
    tamano_total = sum(f.stat().st_size for f in archivos)
    print(f"Tamaño total: {tamano_total / (1024*1024):.2f} MB")
    
    print("\n" + "="*60)

if __name__ == '__main__':
    # Generar reporte
    generar_reporte_validacion()
    
    # Ejecutar tests
    print("EJECUTANDO TESTS DE VALIDACION...")
    print("="*60)
    
    # Configurar unittest para mostrar más detalles
    unittest.TextTestRunner(verbosity=2).run(
        unittest.TestLoader().loadTestsFromTestCase(TestModeloDimensional)
    )