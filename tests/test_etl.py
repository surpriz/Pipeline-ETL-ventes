import unittest
import pandas as pd
import numpy as np
from unittest.mock import patch, MagicMock
from datetime import datetime
import sys
import os

# Configuration des chemins
project_root = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
sys.path.append(project_root)

# Import des classes à tester
from src.etl.extract import DataExtractor
from src.etl.transform import DataTransformer
from src.etl.load import DataLoader

# Schémas de test
test_schemas = {
    'customers': ['customer_id', 'customer_unique_id', 'customer_zip_code_prefix', 'customer_city', 'customer_state'],
    'products': ['product_id', 'product_category_name', 'product_weight_g', 'product_length_cm', 'product_height_cm', 
                'product_width_cm', 'product_photos_qty'],
    'orders': ['order_id', 'customer_id', 'order_status', 'order_purchase_timestamp', 'order_approved_at',
              'order_delivered_carrier_date', 'order_delivered_customer_date', 'order_estimated_delivery_date']
}

@patch('src.etl.extract.EXPECTED_SCHEMAS', test_schemas)
@patch('src.etl.transform.EXPECTED_SCHEMAS', test_schemas)
class TestDataExtractor(unittest.TestCase):
    def setUp(self):
        self.extractor = DataExtractor()
        self.sample_data = {
            'customer_id': ['1', '2'],
            'customer_unique_id': ['A1', 'A2'],
            'customer_zip_code_prefix': ['12345', '67890'],
            'customer_city': ['São Paulo', 'Rio'],
            'customer_state': ['SP', 'RJ']
        }
        
    @patch('pandas.read_csv')
    def test_extract_customers(self, mock_read_csv):
        mock_read_csv.return_value = pd.DataFrame(self.sample_data)
        result = self.extractor.extract_customers()
        self.assertIsInstance(result, pd.DataFrame)
        self.assertEqual(len(result), 2)
        self.assertTrue(all(col in result.columns for col in test_schemas['customers']))

    @patch('pandas.read_csv')
    def test_extract_products(self, mock_read_csv):
        products_data = {
            'product_id': ['1', '2'],
            'product_category_name': ['cat1', 'cat2'],
            'product_weight_g': [100, 200],
            'product_length_cm': [10, 20],
            'product_height_cm': [5, 10],
            'product_width_cm': [8, 15],
            'product_photos_qty': [2, 3]
        }
        translations = {
            'product_category_name': ['cat1', 'cat2'],
            'product_category_name_english': ['Cat1', 'Cat2']
        }
        mock_read_csv.side_effect = [pd.DataFrame(products_data), pd.DataFrame(translations)]
        result = self.extractor.extract_products()
        self.assertIn('product_category_name_english', result.columns)
        self.assertEqual(len(result), 2)

    @patch('pandas.read_csv')
    def test_extract_orders(self, mock_read_csv):
        orders_data = {
            'order_id': ['1', '2'],
            'customer_id': ['C1', 'C2'],
            'order_status': ['delivered', 'shipped'],
            'order_purchase_timestamp': ['2024-01-01', '2024-01-02'],
            'order_approved_at': ['2024-01-01', '2024-01-02'],
            'order_delivered_carrier_date': ['2024-01-02', '2024-01-03'],
            'order_delivered_customer_date': ['2024-01-03', '2024-01-04'],
            'order_estimated_delivery_date': ['2024-01-05', '2024-01-06']
        }
        mock_read_csv.return_value = pd.DataFrame(orders_data)
        result = self.extractor.extract_orders()
        self.assertTrue(all(col in result.columns for col in test_schemas['orders']))

class TestDataTransformer(unittest.TestCase):
    def setUp(self):
        self.transformer = DataTransformer()
        
    def test_transform_customers(self):
        input_data = pd.DataFrame({
            'customer_id': ['1', '2'],
            'customer_unique_id': ['A1', 'A2'],
            'customer_zip_code_prefix': ['12345', None],
            'customer_city': ['São Paulo', 'Rio'],
            'customer_state': ['SP', 'RJ']
        })
        result = self.transformer.transform_customers(input_data)
        self.assertFalse(result['customer_zip_code_prefix'].isnull().any())

    def test_transform_products(self):
        input_data = pd.DataFrame({
            'product_id': ['1', '2'],
            'product_category_name': ['cat1', 'cat2'],
            'product_weight_g': [100, np.nan],
            'product_length_cm': [10, 20],
            'product_height_cm': [5, 10],
            'product_width_cm': [8, 15],
            'product_photos_qty': [2, 3]
        })
        result = self.transformer.transform_products(input_data.copy())
        self.assertTrue(pd.notna(result['product_weight_g']).all())

    def test_transform_orders(self):
        input_data = pd.DataFrame({
            'order_id': ['1', '2'],
            'customer_id': ['C1', 'C2'],
            'order_status': ['delivered', 'shipped'],
            'order_purchase_timestamp': ['2024-01-01', '2024-01-02'],
            'order_approved_at': ['2024-01-01', '2024-01-02'],
            'order_delivered_carrier_date': ['2024-01-02', '2024-01-03'],
            'order_delivered_customer_date': ['2024-01-03', '2024-01-04'],
            'order_estimated_delivery_date': ['2024-01-05', '2024-01-06']
        })
        result = self.transformer.transform_orders(input_data)
        self.assertIn('delivery_delay', result.columns)

    def test_clean_dates(self):
        df = pd.DataFrame({
            'date1': ['2024-01-01', 'invalid_date'],
            'date2': ['2024-01-02', '2024-01-03']
        })
        result = self.transformer.clean_dates(df, ['date1', 'date2'])
        self.assertTrue(pd.api.types.is_datetime64_any_dtype(result['date2']))

class TestDataLoader(unittest.TestCase):
    def setUp(self):
        self.loader = DataLoader()
        
    @patch('psycopg2.connect')
    def test_load_customers(self, mock_connect):
        mock_cur = MagicMock()
        mock_conn = MagicMock()
        mock_connect.return_value = mock_conn
        mock_conn.cursor.return_value = mock_cur
        mock_cur.fetchall.return_value = [(1, '12345')]

        self.loader.connect()
        
        test_df = pd.DataFrame({
            'customer_id': ['1'],
            'customer_unique_id': ['A1'],
            'customer_zip_code_prefix': ['12345']
        })
        
        self.loader.load_customers(test_df)
        mock_conn.commit.assert_called_once()
        self.loader.disconnect()

    @patch('psycopg2.connect')
    def test_load_products(self, mock_connect):
        mock_cur = MagicMock()
        mock_conn = MagicMock()
        mock_connect.return_value = mock_conn
        mock_conn.cursor.return_value = mock_cur

        self.loader.connect()
        
        test_df = pd.DataFrame({
            'product_id': ['1'],
            'product_category_name': ['cat1'],
            'product_category_name_english': ['Cat1'],
            'product_weight_g': [100],
            'product_length_cm': [10],
            'product_height_cm': [5],
            'product_width_cm': [8],
            'product_photos_qty': [2]
        })
        
        self.loader.load_products(test_df)
        mock_conn.commit.assert_called_once()
        self.loader.disconnect()

    @patch('psycopg2.connect')
    def test_load_orders(self, mock_connect):
        mock_cur = MagicMock()
        mock_conn = MagicMock()
        mock_connect.return_value = mock_conn
        mock_conn.cursor.return_value = mock_cur

        self.loader.connect()
        
        test_df = pd.DataFrame({
            'order_id': ['1'],
            'customer_id': ['C1'],
            'order_status': ['delivered'],
            'order_purchase_timestamp': [datetime.now()],
            'order_approved_at': [datetime.now()],
            'order_delivered_carrier_date': [datetime.now()],
            'order_delivered_customer_date': [datetime.now()],
            'order_estimated_delivery_date': [datetime.now()],
            'delivery_delay': [2.5]
        })
        
        self.loader.load_orders(test_df)
        mock_conn.commit.assert_called_once()
        self.loader.disconnect()

if __name__ == '__main__':
    unittest.main()