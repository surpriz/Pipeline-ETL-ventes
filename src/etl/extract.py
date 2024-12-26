# src/etl/extract.py
import pandas as pd
import os
import sys

# Ajout du chemin parent pour l'importation
sys.path.append(os.path.dirname(os.path.dirname(os.path.dirname(__file__))))
from src.etl.config import RAW_DATA_DIR, EXPECTED_SCHEMAS
import logging

# Configuration du logging
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

class DataExtractor:
    """Classe responsable de l'extraction et la validation des données"""
    
    @staticmethod
    def validate_schema(df, table_name):
        """Valide que le DataFrame correspond au schéma attendu"""
        if table_name not in EXPECTED_SCHEMAS:
            logger.warning(f"Pas de schéma défini pour {table_name}")
            return True
            
        expected_columns = set(EXPECTED_SCHEMAS[table_name])
        actual_columns = set(df.columns)
        
        if not expected_columns.issubset(actual_columns):
            missing_cols = expected_columns - actual_columns
            logger.error(f"Colonnes manquantes pour {table_name}: {missing_cols}")
            return False
        return True

    @staticmethod
    def read_csv_file(file_name):
        """Lit un fichier CSV et retourne un DataFrame"""
        try:
            file_path = os.path.join(RAW_DATA_DIR, file_name)
            logger.info(f"Lecture du fichier: {file_name}")
            df = pd.read_csv(file_path)
            return df
        except Exception as e:
            logger.error(f"Erreur lors de la lecture de {file_name}: {str(e)}")
            raise

    def extract_customers(self):
        """Extrait et valide les données clients"""
        df = self.read_csv_file('olist_customers_dataset.csv')
        if self.validate_schema(df, 'customers'):
            return df
        raise ValueError("Validation du schéma clients échouée")

    def extract_products(self):
        """Extrait et valide les données produits"""
        df = self.read_csv_file('olist_products_dataset.csv')
        categories_df = self.read_csv_file('product_category_name_translation.csv')
        
        if self.validate_schema(df, 'products'):
            # Joindre avec les traductions de catégories
            df = df.merge(categories_df, on='product_category_name', how='left')
            return df
        raise ValueError("Validation du schéma produits échouée")

    def extract_orders(self):
        """Extrait et valide les données commandes"""
        df = self.read_csv_file('olist_orders_dataset.csv')
        if self.validate_schema(df, 'orders'):
            return df
        raise ValueError("Validation du schéma commandes échouée")

    def extract_geography(self):
        """Extrait et valide les données géographiques"""
        df = self.read_csv_file('olist_geolocation_dataset.csv')
        if self.validate_schema(df, 'geography'):
            return df
        raise ValueError("Validation du schéma géographie échouée")

if __name__ == "__main__":
    # Test du module
    extractor = DataExtractor()
    try:
        customers_df = extractor.extract_customers()
        products_df = extractor.extract_products()
        orders_df = extractor.extract_orders()
        geography_df = extractor.extract_geography()
        logger.info("Extraction des données réussie")
    except Exception as e:
        logger.error(f"Erreur lors de l'extraction: {str(e)}")