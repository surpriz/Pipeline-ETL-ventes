# src/sql/create_views.py
import psycopg2
import logging
import os
import sys

# Ajout du chemin parent pour l'importation
sys.path.append(os.path.dirname(os.path.dirname(os.path.dirname(__file__))))
from src.etl.config import get_db_config

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

def create_analysis_views():
    """Crée ou met à jour les vues d'analyse"""
    conn = None
    cur = None
    try:
        # Connexion à la base
        conn = psycopg2.connect(**get_db_config())
        cur = conn.cursor()
        
        # Lecture du fichier SQL
        sql_path = os.path.join(os.path.dirname(__file__), 'analysis_views.sql')
        with open(sql_path, 'r') as file:
            sql_commands = file.read()
        
        # Exécution des commandes SQL
        cur.execute(sql_commands)
        
        # Validation
        conn.commit()
        logger.info("Vues d'analyse créées avec succès")
        
    except Exception as e:
        logger.error(f"Erreur lors de la création des vues: {str(e)}")
        if conn:
            conn.rollback()
    finally:
        if cur:
            cur.close()
        if conn:
            conn.close()

if __name__ == "__main__":
    create_analysis_views()