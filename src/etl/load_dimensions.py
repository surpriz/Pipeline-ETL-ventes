import pandas as pd
import psycopg2
import os
from configparser import ConfigParser
import numpy as np

def get_config():
    """Lecture de la configuration de la base de données"""
    config_path = os.path.join(os.path.dirname(__file__), '..', 'config', 'database.ini')
    parser = ConfigParser()
    parser.read(config_path)
    
    db = {}
    if parser.has_section('postgresql'):
        params = parser.items('postgresql')
        for param in params:
            db[param[0]] = param[1]
    else:
        raise Exception('Section postgresql not found in database.ini')
    return db

def load_geography_dimension():
    """Charge les données dans la table dim_geography"""
    conn = None
    cur = None
    try:
        conn = psycopg2.connect(**get_config())
        cur = conn.cursor()
        
        csv_path = os.path.join(os.path.dirname(__file__), '..', '..', 'data', 'raw', 'olist_geolocation_dataset.csv')
        
        print("Lecture du fichier geolocation...")
        df = pd.read_csv(csv_path)
        
        print("Colonnes dans le DataFrame:", df.columns.tolist())
        
        print("Nettoyage des données...")
        if 'geolocation_zip_code_prefix' in df.columns:
            df = df.rename(columns={'geolocation_zip_code_prefix': 'zip_code_prefix',
                                  'geolocation_city': 'city',
                                  'geolocation_state': 'state',
                                  'geolocation_lat': 'latitude',
                                  'geolocation_lng': 'longitude'})
        
        df = df.drop_duplicates(subset=['zip_code_prefix'])
        df = df.replace({np.nan: None})
        
        values = []
        for index, row in df.iterrows():
            try:
                values.append((
                    str(row['zip_code_prefix']),
                    str(row['city']) if row['city'] is not None else None,
                    str(row['state']) if row['state'] is not None else None,
                    float(row['latitude']) if row['latitude'] is not None else None,
                    float(row['longitude']) if row['longitude'] is not None else None
                ))
            except Exception as e:
                print(f"Erreur avec la ligne {index}: {e}")
                continue
        
        print("Insertion des données...")
        cur.executemany("""
            INSERT INTO dim_geography 
            (zip_code_prefix, city, state, latitude, longitude)
            VALUES (%s, %s, %s, %s, %s)
            ON CONFLICT (zip_code_prefix) DO UPDATE SET
                city = EXCLUDED.city,
                state = EXCLUDED.state,
                latitude = EXCLUDED.latitude,
                longitude = EXCLUDED.longitude
        """, values)
        
        conn.commit()
        print(f"Chargement terminé. {len(values)} enregistrements insérés/mis à jour.")
        
    except Exception as e:
        print(f"Erreur lors du chargement : {str(e)}")
        if conn:
            conn.rollback()
    finally:
        if cur:
            cur.close()
        if conn:
            conn.close()

def load_customers_dimension():
    """Charge les données dans la table dim_customers"""
    conn = None
    cur = None
    try:
        conn = psycopg2.connect(**get_config())
        cur = conn.cursor()
        
        csv_path = os.path.join(os.path.dirname(__file__), '..', '..', 'data', 'raw', 'olist_customers_dataset.csv')
        
        print("Lecture du fichier customers...")
        df = pd.read_csv(csv_path)
        
        print("Colonnes dans le DataFrame:", df.columns.tolist())
        
        print("Nettoyage des données...")
        # Obtenir les geography_id correspondants aux zip_code_prefix
        cur.execute("SELECT geography_id, zip_code_prefix FROM dim_geography")
        geo_mapping = dict(cur.fetchall())
        
        values = []
        for index, row in df.iterrows():
            try:
                geography_id = geo_mapping.get(str(row['customer_zip_code_prefix']))
                values.append((
                    str(row['customer_id']),
                    str(row['customer_unique_id']),
                    str(row['customer_zip_code_prefix']),
                    geography_id
                ))
            except Exception as e:
                print(f"Erreur avec la ligne {index}: {e}")
                continue
        
        print("Insertion des données...")
        cur.executemany("""
            INSERT INTO dim_customers 
            (customer_id, customer_unique_id, zip_code_prefix, geography_id)
            VALUES (%s, %s, %s, %s)
            ON CONFLICT (customer_id) DO UPDATE SET
                customer_unique_id = EXCLUDED.customer_unique_id,
                zip_code_prefix = EXCLUDED.zip_code_prefix,
                geography_id = EXCLUDED.geography_id
        """, values)
        
        conn.commit()
        print(f"Chargement terminé. {len(values)} enregistrements insérés/mis à jour.")
        
    except Exception as e:
        print(f"Erreur lors du chargement : {str(e)}")
        if conn:
            conn.rollback()
    finally:
        if cur:
            cur.close()
        if conn:
            conn.close()

def load_products_dimension():
    """
    Charge les données dans la table dim_products
    """
    conn = None
    cur = None
    try:
        # Connexion à la base de données
        conn = psycopg2.connect(**get_config())
        cur = conn.cursor()
        
        # Chemins vers les fichiers CSV
        products_path = os.path.join(os.path.dirname(__file__), '..', '..', 'data', 'raw', 'olist_products_dataset.csv')
        categories_path = os.path.join(os.path.dirname(__file__), '..', '..', 'data', 'raw', 'product_category_name_translation.csv')
        
        # Lecture des fichiers CSV
        print("\n3. Chargement de la dimension produits")
        print("Lecture des fichiers products et categories...")
        products_df = pd.read_csv(products_path)
        categories_df = pd.read_csv(categories_path)
        
        print("Colonnes dans le DataFrame products:", products_df.columns.tolist())
        print("Colonnes dans le DataFrame categories:", categories_df.columns.tolist())
        
        # Jointure avec les catégories
        print("Fusion des données produits et catégories...")
        df = pd.merge(
            products_df,
            categories_df,
            how='left',
            left_on='product_category_name',
            right_on='product_category_name'
        )
        
        # Nettoyage et transformation
        print("Nettoyage des données...")
        df = df.replace({np.nan: None})
        
        # Préparation des données pour insertion
        values = []
        for index, row in df.iterrows():
            try:
                values.append((
                    str(row['product_id']),
                    str(row['product_category_name']) if row['product_category_name'] is not None else None,
                    str(row['product_category_name_english']) if row['product_category_name_english'] is not None else None,
                    float(row['product_weight_g']) if row['product_weight_g'] is not None else None,
                    float(row['product_length_cm']) if row['product_length_cm'] is not None else None,
                    float(row['product_height_cm']) if row['product_height_cm'] is not None else None,
                    float(row['product_width_cm']) if row['product_width_cm'] is not None else None,
                    int(row['product_photos_qty']) if row['product_photos_qty'] is not None else None
                ))
            except Exception as e:
                print(f"Erreur avec la ligne {index}: {e}")
                continue
        
        # Insertion des données
        print("Insertion des données...")
        cur.executemany("""
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
        
        # Validation
        conn.commit()
        print(f"Chargement terminé. {len(values)} enregistrements insérés/mis à jour.")
        
    except Exception as e:
        print(f"Erreur lors du chargement : {str(e)}")
        if conn:
            conn.rollback()
    finally:
        if cur:
            cur.close()
        if conn:
            conn.close()



def load_sellers_dimension():
    """
    Charge les données dans la table dim_sellers
    """
    try:
        # Connexion et chargement du fichier
        conn = psycopg2.connect(**get_config())
        cur = conn.cursor()
        
        sellers_path = os.path.join(os.path.dirname(__file__), '..', '..', 'data', 'raw', 'olist_sellers_dataset.csv')
        
        print("\n4. Chargement de la dimension vendeurs")
        print("Lecture du fichier sellers...")
        sellers_df = pd.read_csv(sellers_path)
        
        print("Colonnes dans le DataFrame sellers:", sellers_df.columns.tolist())
        
        # Obtenir les geography_id correspondants
        cur.execute("SELECT geography_id, zip_code_prefix FROM dim_geography")
        geo_mapping = dict(cur.fetchall())
        
        # Préparation des données
        values = []
        for index, row in sellers_df.iterrows():
            try:
                geography_id = geo_mapping.get(str(row['seller_zip_code_prefix']))
                values.append((
                    str(row['seller_id']),
                    str(row['seller_zip_code_prefix']),
                    geography_id
                ))
            except Exception as e:
                print(f"Erreur avec la ligne {index}: {e}")
                continue
        
        # Insertion
        cur.executemany("""
            INSERT INTO dim_sellers 
            (seller_id, zip_code_prefix, geography_id)
            VALUES (%s, %s, %s)
            ON CONFLICT (seller_id) DO UPDATE SET
                zip_code_prefix = EXCLUDED.zip_code_prefix,
                geography_id = EXCLUDED.geography_id
        """, values)
        
        conn.commit()
        print(f"Chargement terminé. {len(values)} enregistrements insérés/mis à jour.")
        
    except Exception as e:
        print(f"Erreur lors du chargement : {str(e)}")
        if conn:
            conn.rollback()
    finally:
        if cur:
            cur.close()
        if conn:
            conn.close()

def load_time_dimension():
    """
    Génère et charge la dimension temporelle
    """
    try:
        # Connexion
        conn = psycopg2.connect(**get_config())
        cur = conn.cursor()
        
        # Lecture des dates des commandes
        orders_path = os.path.join(os.path.dirname(__file__), '..', '..', 'data', 'raw', 'olist_orders_dataset.csv')
        
        print("\n5. Chargement de la dimension temporelle")
        print("Lecture des dates depuis orders...")
        orders_df = pd.read_csv(orders_path)
        
        # Extraction et nettoyage des dates
        all_dates = pd.concat([
            pd.to_datetime(orders_df['order_purchase_timestamp']),
            pd.to_datetime(orders_df['order_delivered_customer_date']),
            pd.to_datetime(orders_df['order_estimated_delivery_date'])
        ])
        
        # Suppression des NaT et doublons
        dates = pd.DataFrame(all_dates.dropna().unique(), columns=['date_actual'])
        dates = dates.sort_values('date_actual')
        
        print("Génération des attributs temporels...")
        values = []
        for index, row in dates.iterrows():
            date = row['date_actual']
            values.append((
                date.date(),                    # date_actual
                date.year,                      # year
                date.month,                     # month
                date.day,                       # day
                (date.month - 1) // 3 + 1,      # quarter
                date.week,                      # week
                date.weekday() >= 5             # is_weekend
            ))
        
        # Insertion
        print("Insertion des dates...")
        cur.executemany("""
            INSERT INTO dim_time 
            (date_actual, year, month, day, quarter, week, is_weekend)
            VALUES (%s, %s, %s, %s, %s, %s, %s)
            ON CONFLICT (date_actual) DO NOTHING
        """, values)
        
        conn.commit()
        print(f"Chargement terminé. {len(values)} enregistrements insérés/mis à jour.")
        
    except Exception as e:
        print(f"Erreur lors du chargement : {str(e)}")
        if conn:
            conn.rollback()
    finally:
        if cur:
            cur.close()
        if conn:
            conn.close()

def main():
    print("Début du chargement des dimensions...\n")
    
    print("1. Chargement de la dimension géographique")
    load_geography_dimension()
    
    print("\n2. Chargement de la dimension clients")
    load_customers_dimension()
    
    print("\n3. Chargement de la dimension produits")
    load_products_dimension()
    
    print("\n4. Chargement de la dimension vendeurs")
    load_sellers_dimension()
    
    print("\n5. Chargement de la dimension temporelle")
    load_time_dimension()
    
    print("\nProcessus de chargement terminé")

if __name__ == "__main__":
    main()