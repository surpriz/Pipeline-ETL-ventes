
# Système d'analyse des ventes d'e-commerce

Contexte :
Imaginez que vous êtes Data Engineer pour une entreprise d'e-commerce qui souhaite mettre en place un système d'analyse de ses ventes. Vous devez créer un pipeline de données complet.
Données sources (disponibles sur Kaggle) :

"Brazilian E-Commerce Public Dataset by Olist"
(Il contient plusieurs fichiers CSV avec des données sur les commandes, produits, clients, vendeurs, etc.)

Objectifs du projet :

Créer un pipeline ETL complet
Mettre en place un data warehouse
Préparer les données pour l'analyse

Étapes suggérées :

Ingestion des données

Créer des scripts Python pour lire les fichiers CSV
Mettre en place une validation basique des données
Simuler un système d'ingestion incrémentale


Stockage

Créer une base de données PostgreSQL locale
Définir le schéma du data warehouse (modèle en étoile)
Implémenter les tables dimensions et faits


Transformation

Nettoyer les données (valeurs manquantes, doublons)
Créer des transformations métier (ex: calcul du chiffre d'affaires par jour)
Enrichir les données (ex: ajout de catégories temporelles)


Pipeline et Orchestration

Utiliser Apache Airflow pour orchestrer le pipeline
Créer des DAGs pour automatiser les processus
Implémenter des contrôles de qualité


Accès aux données

Créer des vues SQL optimisées
Mettre en place des agrégations précalculées
Préparer des requêtes types pour l'analyse



Objectifs techniques à atteindre :

Modélisation :

Table de faits : orders_fact
Tables de dimensions : customers_dim, products_dim, sellers_dim, time_dim


Métriques à calculer :

Chiffre d'affaires quotidien/mensuel
Nombre de commandes par région
Performance des vendeurs
Délais de livraison moyens


Technologies suggérées :

Python (pandas, SQLAlchemy)
PostgreSQL
Apache Airflow
Git pour le versioning



Bonus (pour aller plus loin) :

Ajouter des tests unitaires
Implémenter un système de logging
Créer des tableaux de bord avec Metabase ou Preset
Conteneuriser l'application avec Docker

Livrables attendus :

Scripts Python pour l'ETL
Schéma de base de données
DAGs Airflow
Documentation technique
Tests unitaires
Procédures de déploiement

Ce projet vous permettra de :

Pratiquer la modélisation de données
Manipuler des données réelles
Mettre en place une architecture data complète
Utiliser des outils standards de l'industrie

