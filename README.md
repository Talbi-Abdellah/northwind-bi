# 📊 Projet BI Northwind - Solution Complète

## 🎯 Objectif du Projet

Ce projet présente une solution complète de Business Intelligence (BI) basée sur la célèbre base de données Northwind. Il démontre toutes les étapes d'un pipeline ETL moderne et la création d'un tableau de bord analytique interactif.

### Fonctionnalités principales :
- ✅ Extraction des données depuis une base SQL (SQLite/SQL Server/Access)
- ✅ Transformation et nettoyage des données avec Python/Pandas
- ✅ Création de métriques analytiques et KPIs
- ✅ Tableau de bord interactif avec visualisations dynamiques
- ✅ Génération de rapports et graphiques exportables

---

## 📁 Structure du Projet

```
northwind-bi-project/
│
├── data/
│   ├── raw/                    # Données extraites brutes
│   ├── processed/              # Données transformées
│   └── northwind.db            # Base de données SQLite
│
├── scripts/
│   ├── 01_extract.py          # Extraction (ETL - Extract)
│   ├── 02_transform.py        # Transformation (ETL - Transform)
│   ├── 03_load.py             # Chargement (ETL - Load)
│   └── 04_dashboard.py        # Dashboard interactif
│
├── figures/                    # Graphiques générés
├── reports/                    # Rapports PDF
├── notebooks/                  # Notebooks Jupyter d'analyse
│
├── README.md                   # Ce fichier
└── requirements.txt            # Dépendances Python
```

---

## 🚀 Installation et Configuration

### 1. Prérequis

- Python 3.8 ou supérieur
- pip (gestionnaire de paquets Python)
- Base de données Northwind (SQLite, SQL Server, ou Access)

### 2. Installation des dépendances

```bash
# Cloner ou télécharger le projet
cd northwind-bi-project

# Créer un environnement virtuel (recommandé)
python -m venv venv

# Activer l'environnement virtuel
# Windows:
venv\Scripts\activate
# Linux/Mac:
source venv/bin/activate

# Installer les dépendances
pip install -r requirements.txt
```

### 3. Configuration de la base de données

**Option A : Utiliser SQLite (recommandé pour débuter)**
- Téléchargez la base Northwind SQLite depuis : https://github.com/jpwhite3/northwind-SQLite3
- Placez le fichier `northwind.db` dans le dossier `data/`

**Option B : SQL Server**
- Modifiez la connexion dans `01_extract.py` pour utiliser pyodbc
- Exemple de chaîne de connexion :
```python
conn_str = 'DRIVER={SQL Server};SERVER=localhost;DATABASE=Northwind;Trusted_Connection=yes;'
```

**Option C : Microsoft Access**
- Installez le driver ODBC Access
- Modifiez la connexion pour pointer vers votre fichier .mdb/.accdb

---

## 🎬 Exécution du Projet

### Étape 1 : Extraction des données

```bash
python scripts/01_extract.py
```

**Ce que fait ce script :**
- Se connecte à la base Northwind
- Extrait toutes les tables principales (Customers, Orders, Products, etc.)
- Crée une vue consolidée pour l'analyse des ventes
- Sauvegarde les données en CSV dans `data/raw/`

**Résultat attendu :**
```
✓ Connexion établie à data/northwind.db
✓ Table Customers: 91 lignes extraites
✓ Table Orders: 830 lignes extraites
✓ Table Products: 77 lignes extraites
...
📁 Fichiers créés : customers.csv, orders.csv, sales_analysis.csv, etc.
```

---

### Étape 2 : Transformation des données

```bash
python scripts/02_transform.py
```

**Ce que fait ce script :**
- Charge les données brutes
- Nettoie les valeurs manquantes
- Enrichit avec des colonnes calculées (Year, Month, Quarter, DeliveryDays)
- Crée des agrégations (ventes mensuelles, par catégorie, par pays, etc.)
- Calcule les KPIs principaux
- Sauvegarde dans `data/processed/`

**Résultat attendu :**
```
🧹 Nettoyage des données de ventes...
  • Dates converties
  • Composantes temporelles ajoutées
  • Valeurs manquantes: 150 → 0

📊 Création des métriques agrégées...
  • monthly_sales: 24 lignes
  • category_sales: 8 lignes
  • top_products: 20 lignes
  
💰 KPIs principaux:
  • Revenu total: $1,354,458.59
  • Commandes: 830
  • Clients: 89
```

---

### Étape 3 : Lancement du Dashboard

```bash
python scripts/04_dashboard.py
```

**Ce que fait ce script :**
- Charge toutes les données transformées
- Crée des visualisations interactives
- Lance un serveur web local
- Affiche le dashboard dans votre navigateur

**Résultat attendu :**
```
🚀 Lancement du dashboard...
📡 Serveur démarré sur http://localhost:8050
💡 Ouvrez votre navigateur à cette adresse
```

Ouvrez votre navigateur et allez à : **http://localhost:8050**

---

## 📊 Indicateurs Clés (KPIs)

Le tableau de bord présente les KPIs suivants :

### KPIs Principaux
1. **💰 Revenu Total** : Somme de toutes les ventes
2. **📦 Nombre de Commandes** : Total des commandes passées
3. **👥 Nombre de Clients** : Clients uniques actifs
4. **📊 Panier Moyen** : Valeur moyenne par commande
5. **🚚 Délai de Livraison Moyen** : En jours

### Visualisations Disponibles

1. **📈 Évolution des ventes mensuelles**
   - Graphique en ligne montrant la tendance temporelle
   - Permet d'identifier la saisonnalité

2. **🎯 Répartition par catégorie**
   - Diagramme circulaire des ventes par catégorie de produits
   - Identifie les catégories les plus rentables

3. **🏆 Top 10 Produits**
   - Graphique à barres horizontales
   - Classement des produits les plus vendus

4. **🌍 Ventes par Pays**
   - Graphique à barres des ventes géographiques
   - Top 15 pays par revenus

5. **👔 Performance des Employés**
   - Comparaison des ventes par employé
   - Nombre de commandes traitées

---

## 🛠️ Choix Techniques

### Bibliothèques Python Utilisées

| Bibliothèque | Usage | Justification |
|-------------|-------|---------------|
| **Pandas** | Manipulation de données | Standard de l'industrie, performant, facile à utiliser |
| **SQLAlchemy** | Connexion BDD | Compatible avec tous types de bases de données |
| **Plotly** | Visualisations | Graphiques interactifs modernes et élégants |
| **Dash** | Dashboard web | Framework Python pour applications web analytiques |
| **NumPy** | Calculs numériques | Optimisé pour les opérations mathématiques |

### Architecture Choisie

**Pipeline ETL Modulaire**
- ✅ Séparation claire Extract / Transform / Load
- ✅ Chaque script peut être exécuté indépendamment
- ✅ Facilite le débogage et la maintenance
- ✅ Permet la réutilisation du code

**Stockage en CSV**
- ✅ Format universel et léger
- ✅ Facile à inspecter et déboguer
- ✅ Compatible avec tous les outils d'analyse
- ✅ Versionnable avec Git

---

## 📈 Analyses Possibles

Ce projet permet de répondre à des questions d'affaires telles que :

1. **Analyse des ventes**
   - Quelle est la tendance des ventes sur la période ?
   - Quels sont les mois les plus rentables ?
   - Y a-t-il une saisonnalité ?

2. **Analyse produits**
   - Quels produits génèrent le plus de revenus ?
   - Quelles catégories sont les plus populaires ?
   - Quel est le taux de réachat ?

3. **Analyse géographique**
   - Quels pays achètent le plus ?
   - Où concentrer les efforts commerciaux ?

4. **Analyse RH**
   - Quels employés sont les plus performants ?
   - Quelle est la charge de travail par employé ?

5. **Analyse logistique**
   - Quel est le délai moyen de livraison ?
   - Y a-t-il des retards significatifs ?

---

## 🎓 Pour Aller Plus Loin

### Améliorations Possibles

1. **Analyse prédictive**
   - Prévision des ventes futures (Machine Learning)
   - Détection d'anomalies

2. **Dashboard avancé**
   - Filtres interactifs par période/catégorie
   - Export de rapports PDF automatisés
   - Alertes en temps réel

3. **Optimisation**
   - Utilisation de bases de données NoSQL (MongoDB)
   - Cache des résultats pour améliorer les performances
   - Parallélisation du traitement

4. **Déploiement**
   - Hébergement sur cloud (AWS, Azure, Heroku)
   - Mise en place d'API REST
   - Automatisation avec Airflow

---

## 🐛 Dépannage

### Problème : Erreur de connexion à la base

**Solution :** Vérifiez que le fichier `northwind.db` existe dans `data/` et que le chemin est correct.

### Problème : Module introuvable

**Solution :** Assurez-vous que l'environnement virtuel est activé et que les dépendances sont installées :
```bash
pip install -r requirements.txt
```

### Problème : Le dashboard ne s'affiche pas

**Solution :** Vérifiez que le port 8050 n'est pas déjà utilisé. Changez le port si nécessaire :
```python
dashboard.run(debug=True, port=8051)
```

---

## 📚 Ressources Additionnelles

- [Documentation Pandas](https://pandas.pydata.org/docs/)
- [Documentation Plotly](https://plotly.com/python/)
- [Documentation Dash](https://dash.plotly.com/)
- [Base Northwind Officielle](https://github.com/microsoft/sql-server-samples/tree/master/samples/databases/northwind-pubs)

---

## 👤 Auteur

**Votre Nom**  
Projet réalisé dans le cadre d'une formation en Business Intelligence  
Date : Novembre 2025

---

## 📄 Licence

Ce projet est fourni à des fins éducatives. La base de données Northwind est une propriété de Microsoft mise à disposition publiquement.

---

## 🙏 Remerciements

- Microsoft pour la base de données Northwind
- La communauté Python pour les excellentes bibliothèques open-source
- Tous les contributeurs et formateurs

---

**✨ Bon apprentissage et bonne analyse ! ✨**