# src/etl/config.py
import os
from configparser import ConfigParser

# Chemins des fichiers
BASE_DIR = os.path.dirname(os.path.dirname(os.path.dirname(__file__)))
RAW_DATA_DIR = os.path.join(BASE_DIR, 'data', 'raw')

# Structure attendue des fichiers
EXPECTED_SCHEMAS = {
    'customers': ['customer_id', 'customer_unique_id', 'customer_zip_code_prefix', 'customer_city', 'customer_state'],
    'geography': ['geolocation_zip_code_prefix', 'geolocation_lat', 'geolocation_lng', 'geolocation_city', 'geolocation_state'],
    'orders': ['order_id', 'customer_id', 'order_status', 'order_purchase_timestamp', 'order_approved_at', 
               'order_delivered_carrier_date', 'order_delivered_customer_date', 'order_estimated_delivery_date'],
    'products': ['product_id', 'product_category_name', 'product_name_lenght', 'product_description_lenght', 
                'product_photos_qty', 'product_weight_g', 'product_length_cm', 'product_height_cm', 'product_width_cm']
}

def get_db_config(filename='database.ini', section='postgresql'):
    """Récupère la configuration de la base de données"""
    config_path = os.path.join(BASE_DIR, 'src', 'config', filename)
    parser = ConfigParser()
    parser.read(config_path)
    
    db = {}
    if parser.has_section(section):
        params = parser.items(section)
        for param in params:
            db[param[0]] = param[1]
    else:
        raise Exception(f'Section {section} not found in {filename}')
    return db