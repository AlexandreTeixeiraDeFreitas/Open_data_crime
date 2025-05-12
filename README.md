# Open_data_crime

---

# 📊 Crime Forecasting Platform

## 🚀 Fonctionnalité

Cette application permet :

* D'afficher les données de crimes existantes sur une carte interactive.
* De demander une prédiction basée sur une ville et une date.
* De stocker automatiquement les prévisions de la journée pour chaque secteur.

## 🗺️ Configuration Mapbox

Pour afficher correctement la carte avec les styles Mapbox :

1. Crée un fichier `.env` à la racine de ton projet Frontend.
2. Ajoute ta clé Mapbox comme suit :

```
REACT_APP_MAPBOX_TOKEN=your_mapbox_api_key_here
```

Tu peux obtenir une clé gratuite sur [https://account.mapbox.com/access-tokens](https://account.mapbox.com/access-tokens).

---

## ⚠️ Dépendance au Remplissage de Données

Le **conteneur `forecast_job`** doit être lancé **uniquement après** que les données des crimes aient été **correctement insérées dans la base de données**.

Ce job :

* Analyse les données existantes.
* Calcule les prévisions pour tous les créneaux horaires de la journée en cours.
* Stocke les résultats dans la table `crime_forecasts`.

---

## ✅ Checklist pour remplir la base et reentrainé l'ia

1. Aller dans le conteneur `test_spark`
2. Lancer le scrypte send_nyc_data_to_kafka.py avec `python send_nyc_data_to_kafka.py`.

## ✅ Checklist de Démarrage prédiction sur superset

1. Avoir les données chargées dans la base (`Crimes` table).
2. Lancer le conteneur `forecast_job`.
3. Renseigner votre clé Mapbox dans le fichier `.venv` si vous voulez une carte dans superset.

---

Souhaites-tu que j’ajoute les commandes Docker ou l’arborescence du projet ?
