# src/etl/load_data.py

import pandas as pd
import psycopg2
from datetime import datetime
import os
from src.utils.db_connection import config

def load_geography_dimension():
    """Charge les données géographiques"""
    df = pd.read_csv('../data/raw/olist_geolocation_dataset.csv')
    # Dedupliquer et nettoyer les données
    df = df.drop_duplicates(subset=['zip_code_prefix'])
    # ... transformation des données ...
    return df

def load_customers_dimension():
    """Charge les données clients"""
    df = pd.read_csv('../data/raw/olist_customers_dataset.csv')
    # ... transformation des données ...
    return df

def load_products_dimension():
    """Charge les données produits"""
    df = pd.read_csv('../data/raw/olist_products_dataset.csv')
    categories_df = pd.read_csv('../data/raw/product_category_name_translation.csv')
    # Joindre avec les traductions
    df = df.merge(categories_df, on='product_category_name', how='left')
    return df

def load_time_dimension():
    """Génère la dimension temporelle"""
    # Extraire toutes les dates des commandes
    orders_df = pd.read_csv('../data/raw/olist_orders_dataset.csv')
    dates = pd.to_datetime(orders_df['order_purchase_timestamp']).dt.date.unique()
    # ... création des attributs temporels ...
    return dates_df

def load_orders_fact():
    """Charge la table de faits commandes"""
    df = pd.read_csv('../data/raw/olist_orders_dataset.csv')
    payments_df = pd.read_csv('../data/raw/olist_order_payments_dataset.csv')
    # Joindre avec les paiements
    df = df.merge(payments_df, on='order_id', how='left')
    return df

def main():
    """Fonction principale d'exécution"""
    try:
        # Charger les dimensions d'abord
        load_geography_dimension()
        load_customers_dimension()
        load_products_dimension()
        load_time_dimension()
        
        # Puis les faits
        load_orders_fact()
        
        print("Chargement des données terminé avec succès")
        
    except Exception as e:
        print(f"Erreur lors du chargement : {str(e)}")

if __name__ == "__main__":
    main()