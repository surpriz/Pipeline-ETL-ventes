# Utiliser une image Python officielle comme image de base
FROM python:3.12-slim

# Définir le répertoire de travail dans le conteneur
WORKDIR /app

# Copier les fichiers nécessaires
COPY requirements.txt .
COPY src/ ./src/
COPY data/ ./data/
COPY conftest.py .

# Installer les dépendances
RUN pip install --no-cache-dir -r requirements.txt

# Définir les variables d'environnement pour la base de données
ENV POSTGRES_USER=jerome_laval
ENV POSTGRES_PASSWORD=Motdepasse13$
ENV POSTGRES_DB=olist_dw
ENV POSTGRES_HOST=postgres

# Définir le point d'entrée
CMD ["python", "-m", "src.etl.extract"]