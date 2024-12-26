import pandas as pd
import psycopg2
import os
from configparser import ConfigParser
import numpy as np
from datetime import datetime

# Réutilisation de la fonction get_config()
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

def load_orders_fact():
    """
    Charge la table de faits orders_fact
    """
    try:
        conn = psycopg2.connect(**get_config())
        cur = conn.cursor()
        
        # Lecture des fichiers
        orders_path = os.path.join(os.path.dirname(__file__), '..', '..', 'data', 'raw', 'olist_orders_dataset.csv')
        payments_path = os.path.join(os.path.dirname(__file__), '..', '..', 'data', 'raw', 'olist_order_payments_dataset.csv')
        
        print("1. Chargement des commandes...")
        orders_df = pd.read_csv(orders_path)
        payments_df = pd.read_csv(payments_path)
        
        # Agrégation des paiements par commande
        payments_agg = payments_df.groupby('order_id').agg({
            'payment_value': 'sum',
            'payment_installments': 'max',
            'payment_type': lambda x: x.iloc[0]
        }).reset_index()
        
        # Fusion avec les paiements
        df = orders_df.merge(payments_agg, on='order_id', how='left')
        
        # Conversion des colonnes de date en datetime avec gestion des NaN
        date_columns = [
            'order_purchase_timestamp',
            'order_approved_at',
            'order_delivered_carrier_date',
            'order_delivered_customer_date',
            'order_estimated_delivery_date'
        ]
        
        for col in date_columns:
            df[col] = pd.to_datetime(df[col], errors='coerce')
        
        # Obtenir les date_id depuis dim_time
        print("Récupération des identifiants temporels...")
        cur.execute("SELECT date_id, date_actual FROM dim_time")
        date_mapping = dict(cur.fetchall())
        
        # Préparation des données
        values = []
        for _, row in df.iterrows():
            try:
                # Conversion des dates
                purchase_date = row['order_purchase_timestamp'].date() if pd.notnull(row['order_purchase_timestamp']) else None
                date_id = date_mapping.get(purchase_date) if purchase_date else None
                
                values.append((
                    row['order_id'],
                    row['customer_id'],
                    date_id,
                    row['order_status'],
                    row['order_purchase_timestamp'] if pd.notnull(row['order_purchase_timestamp']) else None,
                    row['order_approved_at'] if pd.notnull(row['order_approved_at']) else None,
                    row['order_delivered_carrier_date'] if pd.notnull(row['order_delivered_carrier_date']) else None,
                    row['order_delivered_customer_date'] if pd.notnull(row['order_delivered_customer_date']) else None,
                    row['order_estimated_delivery_date'] if pd.notnull(row['order_estimated_delivery_date']) else None,
                    float(row['payment_value']) if pd.notnull(row['payment_value']) else 0.0,
                    row['payment_type'] if pd.notnull(row['payment_type']) else None,
                    int(row['payment_installments']) if pd.notnull(row['payment_installments']) else None
                ))
            except Exception as e:
                print(f"Erreur avec la commande {row['order_id']}: {e}")
                continue
        
        print("Insertion des commandes...")
        cur.executemany("""
            INSERT INTO fact_orders (
                order_id, customer_id, date_id, order_status,
                purchase_timestamp, approved_at, delivered_carrier_date,
                delivered_customer_date, estimated_delivery_date,
                total_amount, payment_type, payment_installments
            ) VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
            ON CONFLICT (order_id) DO UPDATE SET
                customer_id = EXCLUDED.customer_id,
                date_id = EXCLUDED.date_id,
                order_status = EXCLUDED.order_status,
                total_amount = EXCLUDED.total_amount
        """, values)
        
        conn.commit()
        print(f"Chargement terminé. {len(values)} commandes insérées/mises à jour.")
        
    except Exception as e:
        print(f"Erreur lors du chargement : {str(e)}")
        if conn:
            conn.rollback()
    finally:
        if cur:
            cur.close()
        if conn:
            conn.close()

def load_order_items_fact():
    """
    Charge la table de faits order_items_fact
    """
    try:
        conn = psycopg2.connect(**get_config())
        cur = conn.cursor()
        
        items_path = os.path.join(os.path.dirname(__file__), '..', '..', 'data', 'raw', 'olist_order_items_dataset.csv')
        
        print("\n2. Chargement des items des commandes...")
        items_df = pd.read_csv(items_path)
        
        # Vérification des colonnes
        print("Colonnes dans le DataFrame:", items_df.columns.tolist())
        
        # Nettoyage des données
        items_df = items_df.replace({np.nan: None})
        
        values = []
        for _, row in items_df.iterrows():
            try:
                values.append((
                    str(row['order_id']),
                    str(row['product_id']),
                    str(row['seller_id']),
                    float(row['price']) if pd.notnull(row['price']) else 0.0,
                    float(row['freight_value']) if pd.notnull(row['freight_value']) else 0.0
                ))
            except Exception as e:
                print(f"Erreur avec l'item {row['order_id']}: {e}")
                continue
        
        print("Insertion des items...")
        cur.executemany("""
            INSERT INTO fact_order_items 
            (order_id, product_id, seller_id, price, freight_value)
            VALUES (%s, %s, %s, %s, %s)
            ON CONFLICT (order_item_id) DO UPDATE SET
                price = EXCLUDED.price,
                freight_value = EXCLUDED.freight_value
        """, values)
        
        conn.commit()
        print(f"Chargement terminé. {len(values)} items insérés/mis à jour.")
        
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
    print("Début du chargement des faits...\n")
    
    print("1. Chargement des commandes")
    load_orders_fact()
    
    print("\n2. Chargement des items")
    load_order_items_fact()
    
    print("\nProcessus de chargement terminé")

if __name__ == "__main__":
    main()