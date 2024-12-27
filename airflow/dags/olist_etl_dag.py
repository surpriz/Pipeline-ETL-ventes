# dags/olist_etl_dag.py
from airflow import DAG
from airflow.decorators import task
from airflow.utils.dates import days_ago
from datetime import datetime
import logging
import os
import sys
import pandas as pd
import io  # Importer StringIO

from airflow.providers.postgres.hooks.postgres import PostgresHook

# Ajouter le chemin des scripts ETL
sys.path.append(os.path.join(os.path.dirname(__file__), 'src', 'etl'))

from src.etl.extract import DataExtractor
from src.etl.transform import DataTransformer
from src.etl.load import DataLoader

# Configuration du logging
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

# Définir les arguments par défaut du DAG
default_args = {
    'owner': 'airflow',
    'depends_on_past': False,
    'email_on_failure': False,
    'email_on_retry': False,
}

# Définir le DAG
with DAG(
    dag_id='olist_etl_pipeline',
    default_args=default_args,
    description='Pipeline ETL complet pour l\'analyse des ventes d\'e-commerce',
    schedule_interval='@daily',  # Modifier selon les besoins
    start_date=days_ago(1),
    catchup=False,
    tags=['ecommerce', 'etl'],
) as dag:

    @task()
    def create_tables():
        """Crée les tables de dimensions et de faits dans PostgreSQL"""
        try:
            hook = PostgresHook(postgres_conn_id="postgres_default")
            with hook.get_conn() as conn:
                with conn.cursor() as cur:
                    # Création des tables
                    cur.execute("""
                        -- Table Géographie
                        CREATE TABLE IF NOT EXISTS dim_geography (
                            geography_id SERIAL PRIMARY KEY,
                            zip_code_prefix VARCHAR(5) UNIQUE,
                            city VARCHAR(100),
                            state VARCHAR(2),
                            latitude DECIMAL(9,6),
                            longitude DECIMAL(9,6),
                            created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
                        );
                        
                        -- Table Clients
                        CREATE TABLE IF NOT EXISTS dim_customers (
                            customer_id VARCHAR PRIMARY KEY,
                            customer_unique_id VARCHAR,
                            zip_code_prefix VARCHAR(5),
                            geography_id INTEGER REFERENCES dim_geography(geography_id),
                            created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
                        );
                        
                        -- Table Produits
                        CREATE TABLE IF NOT EXISTS dim_products (
                            product_id VARCHAR PRIMARY KEY,
                            category_name VARCHAR(100),
                            category_name_english VARCHAR(100),
                            product_weight_g INTEGER,
                            product_length_cm FLOAT,
                            product_height_cm FLOAT,
                            product_width_cm FLOAT,
                            product_photos_qty INTEGER,
                            created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
                        );
                        
                        -- Table Vendeurs
                        CREATE TABLE IF NOT EXISTS dim_sellers (
                            seller_id VARCHAR PRIMARY KEY,
                            zip_code_prefix VARCHAR(5),
                            geography_id INTEGER REFERENCES dim_geography(geography_id),
                            created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
                        );

                        -- Table Temps
                        CREATE TABLE IF NOT EXISTS dim_time (
                            time_id SERIAL PRIMARY KEY,
                            date DATE UNIQUE,
                            day INTEGER,
                            month INTEGER,
                            year INTEGER,
                            week INTEGER,
                            created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
                        );

                        -- Table Faits Commandes
                        CREATE TABLE IF NOT EXISTS fact_orders (
                            order_id VARCHAR PRIMARY KEY,
                            customer_id VARCHAR REFERENCES dim_customers(customer_id),
                            order_status VARCHAR(50),
                            purchase_timestamp TIMESTAMP,
                            approved_at TIMESTAMP,
                            delivered_carrier_date TIMESTAMP,
                            delivered_customer_date TIMESTAMP,
                            estimated_delivery_date TIMESTAMP,
                            delivery_delay FLOAT,
                            time_id INTEGER REFERENCES dim_time(time_id),
                            created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
                        );
                    """)
            logger.info("Tables créées ou existantes vérifiées avec succès.")
            return "Tables créées avec succès"
        except Exception as e:
            logger.error(f"Erreur lors de la création des tables: {str(e)}")
            raise

    @task()
    def extraction():
        """Extrait les données depuis les fichiers CSV"""
        try:
            extractor = DataExtractor()
            logger.info("Début de l'extraction des données...")
            geography_df = extractor.extract_geography()
            customers_df = extractor.extract_customers()
            products_df = extractor.extract_products()
            orders_df = extractor.extract_orders()
            logger.info("Extraction des données terminée.")
            # Retourner les DataFrames extraits sous forme de JSON avec dates en format ISO
            return {
                'geography_df': geography_df.to_json(date_format='iso', date_unit='s'),
                'customers_df': customers_df.to_json(date_format='iso', date_unit='s'),
                'products_df': products_df.to_json(date_format='iso', date_unit='s'),
                'orders_df': orders_df.to_json(date_format='iso', date_unit='s')
            }
        except Exception as e:
            logger.error(f"Erreur lors de l'extraction des données: {str(e)}")
            raise

    @task()
    def transformation(extracted_data):
        """Transforme les données extraites"""
        try:
            transformer = DataTransformer()
            logger.info("Début de la transformation des données...")
            # Convertir les JSON en DataFrames en utilisant StringIO
            geography_df = pd.read_json(io.StringIO(extracted_data['geography_df']), convert_dates=True)
            customers_df = pd.read_json(io.StringIO(extracted_data['customers_df']), convert_dates=True)
            products_df = pd.read_json(io.StringIO(extracted_data['products_df']), convert_dates=True)
            orders_df = pd.read_json(io.StringIO(extracted_data['orders_df']), convert_dates=True)

            # Transformation
            geography_df = transformer.transform_geography(geography_df)
            customers_df = transformer.transform_customers(customers_df)
            products_df = transformer.transform_products(products_df)
            orders_df = transformer.transform_orders(orders_df)
            logger.info("Transformation des données terminée.")
            # Sérialiser les DataFrames transformés en JSON avec dates au format ISO
            return {
                'geography_df': geography_df.to_json(date_format='iso', date_unit='s'),
                'customers_df': customers_df.to_json(date_format='iso', date_unit='s'),
                'products_df': products_df.to_json(date_format='iso', date_unit='s'),
                'orders_df': orders_df.to_json(date_format='iso', date_unit='s')
            }
        except Exception as e:
            logger.error(f"Erreur lors de la transformation des données: {str(e)}")
            raise

    @task()
    def chargement(transformed_data):
        """Charge les données transformées dans PostgreSQL"""
        try:
            loader = DataLoader()
            # Connexion à la base de données
            loader.connect()

            logger.info("Début du chargement des données...")
            # Convertir les JSON en DataFrame en utilisant StringIO
            geography_df = pd.read_json(io.StringIO(transformed_data['geography_df']), convert_dates=True)
            customers_df = pd.read_json(io.StringIO(transformed_data['customers_df']), convert_dates=True)
            products_df = pd.read_json(io.StringIO(transformed_data['products_df']), convert_dates=True)
            orders_df = pd.read_json(io.StringIO(transformed_data['orders_df']), convert_dates=True)
            
            # Ajout de logs pour vérifier les données avant chargement
            logger.info(f"Taille du DataFrame geography avant chargement: {len(geography_df)}")
            logger.info(f"Taille du DataFrame customers avant chargement: {len(customers_df)}")
            logger.info(f"Taille du DataFrame products avant chargement: {len(products_df)}")
            logger.info(f"Taille du DataFrame orders avant chargement: {len(orders_df)}")
            
            # Vérifier que les DataFrames ne sont pas vides
            if len(geography_df) == 0 or len(customers_df) == 0 or len(products_df) == 0 or len(orders_df) == 0:
                raise ValueError("Un ou plusieurs DataFrames sont vides avant le chargement")

            # Vérifier et convertir les colonnes de dates en datetime si nécessaire
            date_columns = ['purchase_timestamp', 'approved_at', 'delivered_carrier_date', 
                            'delivered_customer_date', 'estimated_delivery_date']
            for col in date_columns:
                if col in orders_df.columns:
                    orders_df[col] = pd.to_datetime(orders_df[col], errors='coerce')

            # Chargement avec vérification après chaque étape
            logger.info("Début chargement geography...")
            loader.load_geography(geography_df)
            logger.info("Chargement geography terminé")
            
            logger.info("Début chargement customers...")
            loader.load_customers(customers_df)
            logger.info("Chargement customers terminé")
            
            logger.info("Début chargement products...")
            loader.load_products(products_df)
            logger.info("Chargement products terminé")
            
            logger.info("Début chargement orders...")
            loader.load_orders(orders_df)
            logger.info("Chargement orders terminé")
            
            logger.info("Chargement des données terminé avec succès.")
            
        except Exception as e:
            logger.error(f"Erreur lors du chargement des données: {str(e)}")
            raise
        finally:
            logger.info("Déconnexion de la base de données")
            loader.disconnect()

    @task()
    def verification():
        """Vérifie que les données ont bien été chargées"""
        try:
            hook = PostgresHook(postgres_conn_id="postgres_default")
            with hook.get_conn() as conn:
                with conn.cursor() as cur:
                    # Vérification des comptages
                    tables = ['dim_geography', 'dim_customers', 'dim_products', 'fact_orders']
                    for table in tables:
                        cur.execute(f"SELECT COUNT(*) FROM {table}")
                        count = cur.fetchone()[0]
                        logger.info(f"Nombre d'enregistrements dans {table}: {count}")
                    
                    # Vérification des derniers enregistrements
                    for table in tables:
                        cur.execute(f"SELECT * FROM {table} LIMIT 1")
                        sample = cur.fetchone()
                        logger.info(f"Exemple d'enregistrement dans {table}: {sample}")
                        
            return "Vérification terminée"
        except Exception as e:
            logger.error(f"Erreur lors de la vérification: {str(e)}")
            raise

    @task()
    def analyze_tables():
        """Analyse les tables pour mettre à jour les statistiques"""
        try:
            hook = PostgresHook(postgres_conn_id="postgres_default")
            with hook.get_conn() as conn:
                with conn.cursor() as cur:
                    cur.execute("ANALYZE VERBOSE;")
            return "Analyse des tables terminée"
        except Exception as e:
            logger.error(f"Erreur lors de l'analyse des tables: {str(e)}")
            raise

    # Définir l'ordre des tâches
    create_tables_task = create_tables()
    extraction_task = extraction()
    transformation_task = transformation(extraction_task)
    chargement_task = chargement(transformation_task)
    analyze_task = analyze_tables()
    verification_task = verification()

    # Définir les dépendances
    #create_tables_task >> extraction_task >> transformation_task >> chargement_task
    create_tables_task >> extraction_task >> transformation_task >> chargement_task >> analyze_task >> verification_task

