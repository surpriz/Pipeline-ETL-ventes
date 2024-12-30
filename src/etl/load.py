# src/etl/load.py
import pandas as pd
import psycopg2
import logging
import os  # Ajout si nécessaire
import sys  # Ajout si nécessaire
from config import get_db_config





# Configuration du logging
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

class DataLoader:
    """Classe responsable du chargement des données dans PostgreSQL"""
    
    def __init__(self):
        self.db_config = get_db_config()
        self.conn = None
        self.cur = None

    def connect(self):
        """Établit la connexion à la base de données"""
        try:
            self.conn = psycopg2.connect(**self.db_config)
            self.cur = self.conn.cursor()
            logger.info("Connexion à la base de données établie")
        except Exception as e:
            logger.error(f"Erreur de connexion à la base de données: {str(e)}")
            raise

    def disconnect(self):
        """Ferme la connexion à la base de données"""
        if self.cur:
            self.cur.close()
        if self.conn:
            self.conn.close()
            logger.info("Connexion à la base de données fermée")

    def load_geography(self, df):
        """Charge les données géographiques"""
        try:
            logger.info("Chargement des données géographiques...")
            values = [
                (
                    row['geolocation_zip_code_prefix'],
                    row['geolocation_city'],
                    row['geolocation_state'],
                    row['geolocation_lat'],
                    row['geolocation_lng']
                )
                for _, row in df.iterrows()
            ]
            
            self.cur.executemany("""
                INSERT INTO dim_geography 
                (zip_code_prefix, city, state, latitude, longitude)
                VALUES (%s, %s, %s, %s, %s)
                ON CONFLICT (zip_code_prefix) DO UPDATE SET
                    city = EXCLUDED.city,
                    state = EXCLUDED.state,
                    latitude = EXCLUDED.latitude,
                    longitude = EXCLUDED.longitude
            """, values)
            
            self.conn.commit()
            logger.info(f"{len(values)} enregistrements géographiques chargés")
            
        except Exception as e:
            logger.error(f"Erreur lors du chargement géographique: {str(e)}")
            self.conn.rollback()
            raise

    def load_customers(self, df):
        """Charge les données clients"""
        try:
            logger.info("Chargement des données clients...")
            
            # Récupération des geography_id
            self.cur.execute("SELECT geography_id, zip_code_prefix FROM dim_geography")
            geo_mapping = dict(self.cur.fetchall())
            
            values = [
                (
                    row['customer_id'],
                    row['customer_unique_id'],
                    row['customer_zip_code_prefix'],
                    geo_mapping.get(str(row['customer_zip_code_prefix']))
                )
                for _, row in df.iterrows()
            ]
            
            self.cur.executemany("""
                INSERT INTO dim_customers 
                (customer_id, customer_unique_id, zip_code_prefix, geography_id)
                VALUES (%s, %s, %s, %s)
                ON CONFLICT (customer_id) DO UPDATE SET
                    customer_unique_id = EXCLUDED.customer_unique_id,
                    zip_code_prefix = EXCLUDED.zip_code_prefix,
                    geography_id = EXCLUDED.geography_id
            """, values)
            
            self.conn.commit()
            logger.info(f"{len(values)} enregistrements clients chargés")
            
        except Exception as e:
            logger.error(f"Erreur lors du chargement clients: {str(e)}")
            self.conn.rollback()
            raise

    def load_products(self, df):
        """Charge les données produits"""
        try:
            logger.info("Chargement des données produits...")
            values = []
            for _, row in df.iterrows():
                try:
                    values.append(
                        (
                            str(row['product_id']),
                            str(row['product_category_name']) if pd.notna(row['product_category_name']) else None,
                            str(row['product_category_name_english']) if pd.notna(row['product_category_name_english']) else None,
                            int(row['product_weight_g']) if pd.notna(row['product_weight_g']) else None,
                            float(row['product_length_cm']) if pd.notna(row['product_length_cm']) else None,
                            float(row['product_height_cm']) if pd.notna(row['product_height_cm']) else None,
                            float(row['product_width_cm']) if pd.notna(row['product_width_cm']) else None,
                            int(row['product_photos_qty']) if pd.notna(row['product_photos_qty']) else None
                        )
                    )
                except Exception as e:
                    logger.error(f"Erreur avec le produit {row['product_id']}: {str(e)}")
                    continue
            
            # Exécution de l'insertion en masse après la boucle
            if values:
                self.cur.executemany("""
                    INSERT INTO dim_products 
                    (product_id, category_name, category_name_english, 
                     product_weight_g, product_length_cm, product_height_cm,
                     product_width_cm, product_photos_qty)
                    VALUES (%s, %s, %s, %s, %s, %s, %s, %s)
                    ON CONFLICT (product_id) DO UPDATE SET
                        category_name = EXCLUDED.category_name,
                        category_name_english = EXCLUDED.category_name_english,
                        product_weight_g = EXCLUDED.product_weight_g,
                        product_length_cm = EXCLUDED.product_length_cm,
                        product_height_cm = EXCLUDED.product_height_cm,
                        product_width_cm = EXCLUDED.product_width_cm,
                        product_photos_qty = EXCLUDED.product_photos_qty
                """, values)
                
                self.conn.commit()
                logger.info(f"{len(values)} enregistrements produits chargés")
            else:
                logger.info("Aucun enregistrement produit à charger.")
                
        except Exception as e:
            logger.error(f"Erreur lors du chargement produits: {str(e)}")
            self.conn.rollback()
            raise

    def load_orders(self, df):
        """Charge les données commandes"""
        try:
            logger.info("Chargement des données commandes...")
            values = []
            for _, row in df.iterrows():
                try:
                    values.append((
                        row['order_id'],
                        row['customer_id'],
                        row['order_status'],
                        row['order_purchase_timestamp'] if pd.notna(row['order_purchase_timestamp']) else None,
                        row['order_approved_at'] if pd.notna(row['order_approved_at']) else None,
                        row['order_delivered_carrier_date'] if pd.notna(row['order_delivered_carrier_date']) else None,
                        row['order_delivered_customer_date'] if pd.notna(row['order_delivered_customer_date']) else None,
                        row['order_estimated_delivery_date'] if pd.notna(row['order_estimated_delivery_date']) else None,
                        float(row['delivery_delay']) if pd.notna(row['delivery_delay']) else None
                    ))
                except Exception as e:
                    logger.error(f"Erreur avec la commande {row['order_id']}: {str(e)}")
                    continue
        
            # Exécution de l'insertion en masse après la boucle
            if values:
                self.cur.executemany("""
                    INSERT INTO fact_orders 
                    (order_id, customer_id, order_status, purchase_timestamp,
                     approved_at, delivered_carrier_date, delivered_customer_date,
                     estimated_delivery_date, delivery_delay)
                    VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s)
                    ON CONFLICT (order_id) DO UPDATE SET
                        customer_id = EXCLUDED.customer_id,
                        order_status = EXCLUDED.order_status,
                        delivery_delay = EXCLUDED.delivery_delay
                """, values)
                
                self.conn.commit()
                logger.info(f"{len(values)} enregistrements commandes chargés")
            else:
                logger.info("Aucun enregistrement commande à charger.")
        
        except Exception as e:
            logger.error(f"Erreur lors du chargement commandes: {str(e)}")
            self.conn.rollback()
            raise

def main():
    from extract import DataExtractor
    from transform import DataTransformer
    
    try:
        # Initialisation
        extractor = DataExtractor()
        transformer = DataTransformer()
        loader = DataLoader()
        
        # Connexion à la base
        loader.connect()
        
        # ETL pour chaque type de données
        logger.info("Début du processus ETL...")
        
        # Géographie
        geography_df = transformer.transform_geography(extractor.extract_geography())
        loader.load_geography(geography_df)
        
        # Clients
        customers_df = transformer.transform_customers(extractor.extract_customers())
        loader.load_customers(customers_df)
        
        # Produits
        products_df = transformer.transform_products(extractor.extract_products())
        loader.load_products(products_df)
        
        # Commandes
        orders_df = transformer.transform_orders(extractor.extract_orders())
        loader.load_orders(orders_df)
        
        logger.info("Processus ETL terminé avec succès")
        
    except Exception as e:
        logger.error(f"Erreur lors du processus ETL: {str(e)}")
    finally:
        loader.disconnect()

if __name__ == "__main__":
    main()
