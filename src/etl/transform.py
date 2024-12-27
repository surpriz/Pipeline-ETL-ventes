# src/etl/transform.py
import pandas as pd
import numpy as np
from datetime import datetime
import logging
import sys
import os 
from src.etl.config import EXPECTED_SCHEMAS


# Ajout du chemin parent pour l'importation
#sys.path.append(os.path.dirname(os.path.dirname(os.path.dirname(__file__))))
#from src.etl.config import EXPECTED_SCHEMAS

# Configuration du logging
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

class DataTransformer:
    """Classe responsable de la transformation des données"""
    
    @staticmethod
    def clean_dates(df, date_columns):
        """Nettoie et standardise les colonnes de dates"""
        logger.info("Nettoyage des dates...")
        for col in date_columns:
            if col in df.columns:
                df[col] = pd.to_datetime(df[col], errors='coerce')
        return df

    @staticmethod
    def clean_numeric_values(df, numeric_columns):
        """Nettoie les valeurs numériques"""
        logger.info("Nettoyage des valeurs numériques...")
        for col in numeric_columns:
            if col in df.columns:
                df[col] = pd.to_numeric(df[col], errors='coerce')
        return df

    def transform_geography(self, df):
        """Transforme les données géographiques"""
        logger.info("Transformation des données géographiques...")
        try:
            # Suppression des doublons
            df = df.drop_duplicates(subset=['geolocation_zip_code_prefix'])
            
            # Nettoyage des coordonnées
            numeric_cols = ['geolocation_lat', 'geolocation_lng']
            df = self.clean_numeric_values(df, numeric_cols)
            
            # Standardisation des noms de villes et états
            df['geolocation_city'] = df['geolocation_city'].str.lower().str.strip()
            df['geolocation_state'] = df['geolocation_state'].str.upper().str.strip()
            
            return df
        except Exception as e:
            logger.error(f"Erreur lors de la transformation géographique: {str(e)}")
            raise

    def transform_customers(self, df):
        """Transforme les données clients"""
        logger.info("Transformation des données clients...")
        try:
            # Suppression des doublons potentiels
            df = df.drop_duplicates(subset=['customer_id'])
            
            # Standardisation des codes postaux
            df['customer_zip_code_prefix'] = df['customer_zip_code_prefix'].astype(str)
            
            return df
        except Exception as e:
            logger.error(f"Erreur lors de la transformation clients: {str(e)}")
            raise

    def transform_products(self, df):
        """Transforme les données produits"""
        logger.info("Transformation des données produits...")
        try:
            numeric_cols = ['product_weight_g', 'product_length_cm', 
                        'product_height_cm', 'product_width_cm', 
                        'product_photos_qty']
                        
            # Remplacer NaN par 0 pour les colonnes numériques
            for col in numeric_cols:
                if col in df.columns:
                    df[col] = df[col].fillna(0)
            
            # Valider et retourner
            return df
        except Exception as e:
            logger.error(f"Erreur lors de la transformation produits: {str(e)}")
            raise

    def transform_orders(self, df):
        """Transforme les données commandes"""
        logger.info("Transformation des données commandes...")
        try:
            # Nettoyage des dates
            date_columns = ['order_purchase_timestamp', 'order_approved_at',
                          'order_delivered_carrier_date', 'order_delivered_customer_date',
                          'order_estimated_delivery_date']
            df = self.clean_dates(df, date_columns)
            
            # Calcul du délai de livraison
            df['delivery_delay'] = (
                df['order_delivered_customer_date'] - 
                df['order_purchase_timestamp']
            ).dt.total_seconds() / 86400
            
            return df
        except Exception as e:
            logger.error(f"Erreur lors de la transformation commandes: {str(e)}")
            raise

if __name__ == "__main__":
    # Test du module
    from extract import DataExtractor
    
    try:
        # Extraction des données
        extractor = DataExtractor()
        transformer = DataTransformer()
        
        # Test de transformation
        logger.info("Début des transformations...")
        
        geography_df = transformer.transform_geography(extractor.extract_geography())
        logger.info("Transformation géographie terminée")
        
        customers_df = transformer.transform_customers(extractor.extract_customers())
        logger.info("Transformation clients terminée")
        
        products_df = transformer.transform_products(extractor.extract_products())
        logger.info("Transformation produits terminée")
        
        orders_df = transformer.transform_orders(extractor.extract_orders())
        logger.info("Transformation commandes terminée")
        
        logger.info("Transformation des données réussie")
        
    except Exception as e:
        logger.error(f"Erreur lors du processus de transformation: {str(e)}")
