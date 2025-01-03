import psycopg2
from configparser import ConfigParser
import os

def config():
    # Chemin absolu vers database.ini
    config_path = '/Users/jerome_laval/Desktop/Data Engineering/E-Commerce_pipeline/src/config/database.ini'
    section = 'postgresql'
    
    parser = ConfigParser()
    parser.read(config_path)
    
    db = {}
    if parser.has_section(section):
        params = parser.items(section)
        for param in params:
            db[param[0]] = param[1]
    else:
        raise Exception(f'Section {section} not found in the {config_path} file')
    
    return db

def test_connection():
    conn = None
    try:
        params = config()
        print('Connecting to the PostgreSQL database...')
        conn = psycopg2.connect(**params)
        
        cur = conn.cursor()
        print('PostgreSQL database version:')
        cur.execute('SELECT version()')
        
        db_version = cur.fetchone()
        print(db_version)
        
        cur.close()
    except (Exception, psycopg2.DatabaseError) as error:
        print(error)
    finally:
        if conn is not None:
            conn.close()
            print('Database connection closed.')

if __name__ == '__main__':
    test_connection()