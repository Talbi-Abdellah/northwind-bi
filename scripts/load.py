"""
Script de chargement des données transformées
Charge les données dans une base de données ou génère des fichiers finaux
"""

import pandas as pd
import sqlite3
import os
from datetime import datetime

class NorthwindLoader:
    """Classe pour charger les données transformées"""
    
    def __init__(self, output_db='data/northwind_analytics.db'):
        self.processed_path = 'data/processed/'
        self.output_db = output_db
        self.conn = None
        
    def connect(self):
        """Crée ou se connecte à la base de données analytique"""
        try:
            self.conn = sqlite3.connect(self.output_db)
            print(f"✓ Connexion établie à {self.output_db}")
            return True
        except Exception as e:
            print(f"✗ Erreur de connexion: {e}")
            return False
    
    def load_to_database(self, df, table_name, if_exists='replace'):
        """
        Charge un DataFrame dans la base de données
        Args:
            df: DataFrame à charger
            table_name: Nom de la table de destination
            if_exists: 'replace', 'append', ou 'fail'
        """
        try:
            df.to_sql(table_name, self.conn, if_exists=if_exists, index=False)
            print(f"✓ Table {table_name}: {len(df)} lignes chargées")
            return True
        except Exception as e:
            print(f"✗ Erreur chargement {table_name}: {e}")
            return False
    
    def create_indexes(self):
        """Crée des index pour optimiser les requêtes"""
        print("\n🔧 Création des index...")
        
        indexes = [
            "CREATE INDEX IF NOT EXISTS idx_sales_date ON sales_clean(OrderDate)",
            "CREATE INDEX IF NOT EXISTS idx_sales_customer ON sales_clean(CustomerID)",
            "CREATE INDEX IF NOT EXISTS idx_sales_product ON sales_clean(ProductID)",
            "CREATE INDEX IF NOT EXISTS idx_sales_category ON sales_clean(CategoryName)",
        ]
        
        cursor = self.conn.cursor()
        for idx_query in indexes:
            try:
                cursor.execute(idx_query)
                print(f"  ✓ Index créé")
            except Exception as e:
                print(f"  ✗ Erreur: {e}")
        
        self.conn.commit()
    
    def create_views(self):
        """Crée des vues SQL pour faciliter l'analyse"""
        print("\n📊 Création des vues SQL...")
        
        views = {
            'v_sales_summary': """
                CREATE VIEW IF NOT EXISTS v_sales_summary AS
                SELECT 
                    Year,
                    Month,
                    COUNT(DISTINCT OrderID) as TotalOrders,
                    COUNT(DISTINCT CustomerID) as TotalCustomers,
                    SUM(LineTotal) as TotalRevenue,
                    AVG(LineTotal) as AvgLineValue
                FROM sales_clean
                GROUP BY Year, Month
                ORDER BY Year, Month
            """,
            
            'v_product_performance': """
                CREATE VIEW IF NOT EXISTS v_product_performance AS
                SELECT 
                    ProductName,
                    CategoryName,
                    COUNT(DISTINCT OrderID) as NumOrders,
                    SUM(Quantity) as TotalQuantity,
                    SUM(LineTotal) as TotalRevenue,
                    AVG(LineTotal) as AvgRevenue
                FROM sales_clean
                GROUP BY ProductName, CategoryName
                ORDER BY TotalRevenue DESC
            """,
            
            'v_customer_segmentation': """
                CREATE VIEW IF NOT EXISTS v_customer_segmentation AS
                SELECT 
                    CustomerID,
                    CustomerName,
                    CustomerCountry,
                    COUNT(DISTINCT OrderID) as NumOrders,
                    SUM(LineTotal) as TotalSpent,
                    AVG(LineTotal) as AvgOrderValue,
                    MAX(OrderDate) as LastOrderDate
                FROM sales_clean
                GROUP BY CustomerID, CustomerName, CustomerCountry
                ORDER BY TotalSpent DESC
            """
        }
        
        cursor = self.conn.cursor()
        for view_name, view_query in views.items():
            try:
                cursor.execute(view_query)
                print(f"  ✓ Vue {view_name} créée")
            except Exception as e:
                print(f"  ✗ Erreur {view_name}: {e}")
        
        self.conn.commit()
    
    def load_all_data(self):
        """Charge toutes les données transformées"""
        print("\n📥 Chargement des données transformées...\n")
        
        # Liste des fichiers à charger
        files_to_load = {
            'sales_clean': 'sales_clean.csv',
            'monthly_sales': 'monthly_sales.csv',
            'category_sales': 'category_sales.csv',
            'top_products': 'top_products.csv',
            'country_sales': 'country_sales.csv',
            'employee_sales': 'employee_sales.csv',
            'kpis': 'kpis.csv'
        }
        
        loaded_count = 0
        
        for table_name, filename in files_to_load.items():
            file_path = f"{self.processed_path}{filename}"
            
            if os.path.exists(file_path):
                try:
                    df = pd.read_csv(file_path)
                    self.load_to_database(df, table_name)
                    loaded_count += 1
                except Exception as e:
                    print(f"✗ Erreur chargement {filename}: {e}")
            else:
                print(f"⚠ Fichier non trouvé: {filename}")
        
        return loaded_count
    
    def generate_excel_report(self):
        """Génère un rapport Excel avec plusieurs onglets"""
        print("\n📊 Génération du rapport Excel...")
        
        output_file = 'reports/rapport_northwind.xlsx'
        os.makedirs('reports', exist_ok=True)
        
        try:
            with pd.ExcelWriter(output_file, engine='openpyxl') as writer:
                # Charger et écrire chaque dataset
                datasets = {
                    'KPIs': 'kpis.csv',
                    'Ventes Mensuelles': 'monthly_sales.csv',
                    'Par Catégorie': 'category_sales.csv',
                    'Top Produits': 'top_products.csv',
                    'Par Pays': 'country_sales.csv',
                    'Employés': 'employee_sales.csv'
                }
                
                for sheet_name, filename in datasets.items():
                    file_path = f"{self.processed_path}{filename}"
                    if os.path.exists(file_path):
                        df = pd.read_csv(file_path)
                        df.to_excel(writer, sheet_name=sheet_name, index=False)
                        print(f"  ✓ Onglet '{sheet_name}' ajouté")
            
            print(f"\n✓ Rapport Excel généré: {output_file}")
            return True
            
        except Exception as e:
            print(f"✗ Erreur génération Excel: {e}")
            return False
    
    def verify_data_quality(self):
        """Vérifie la qualité des données chargées"""
        print("\n🔍 Vérification de la qualité des données...\n")
        
        # Compter les enregistrements par table
        cursor = self.conn.cursor()
        cursor.execute("SELECT name FROM sqlite_master WHERE type='table'")
        tables = cursor.fetchall()
        
        print("Nombre d'enregistrements par table:")
        for table in tables:
            table_name = table[0]
            cursor.execute(f"SELECT COUNT(*) FROM {table_name}")
            count = cursor.fetchone()[0]
            print(f"  • {table_name}: {count:,} lignes")
        
        # Vérifier les valeurs nulles dans la table principale
        cursor.execute("SELECT * FROM sales_clean LIMIT 1")
        columns = [description[0] for description in cursor.description]
        
        print("\nValeurs manquantes dans sales_clean:")
        null_counts = []
        for col in columns:
            cursor.execute(f"SELECT COUNT(*) FROM sales_clean WHERE {col} IS NULL")
            null_count = cursor.fetchone()[0]
            if null_count > 0:
                null_counts.append((col, null_count))
        
        if null_counts:
            for col, count in null_counts:
                print(f"  ⚠ {col}: {count} valeurs nulles")
        else:
            print("  ✓ Aucune valeur manquante")
    
    def generate_summary_report(self):
        """Génère un rapport de synthèse"""
        print("\n" + "="*60)
        print("RAPPORT DE SYNTHÈSE DU CHARGEMENT")
        print("="*60)
        
        # Statistiques de base
        cursor = self.conn.cursor()
        
        # KPIs depuis la table
        cursor.execute("SELECT * FROM kpis")
        kpi = cursor.fetchone()
        col_names = [description[0] for description in cursor.description]
        kpi_dict = dict(zip(col_names, kpi))
        
        print(f"\n💼 Indicateurs Clés de Performance:")
        print(f"  • Revenu Total: ${kpi_dict['TotalRevenue']:,.2f}")
        print(f"  • Nombre de Commandes: {int(kpi_dict['TotalOrders']):,}")
        print(f"  • Nombre de Clients: {int(kpi_dict['TotalCustomers'])}")
        print(f"  • Panier Moyen: ${kpi_dict['AvgOrderValue']:,.2f}")
        print(f"  • Délai Livraison Moyen: {kpi_dict['AvgDeliveryDays']:.1f} jours")
        
        # Top catégorie
        cursor.execute("""
            SELECT Category, TotalSales 
            FROM category_sales 
            ORDER BY TotalSales DESC 
            LIMIT 1
        """)
        top_cat = cursor.fetchone()
        print(f"\n🏆 Catégorie #1: {top_cat[0]} (${top_cat[1]:,.2f})")
        
        # Top pays
        cursor.execute("""
            SELECT Country, TotalSales 
            FROM country_sales 
            ORDER BY TotalSales DESC 
            LIMIT 1
        """)
        top_country = cursor.fetchone()
        print(f"🌍 Pays #1: {top_country[0]} (${top_country[1]:,.2f})")
        
        # Meilleur employé
        cursor.execute("""
            SELECT Employee, TotalSales 
            FROM employee_sales 
            ORDER BY TotalSales DESC 
            LIMIT 1
        """)
        top_employee = cursor.fetchone()
        print(f"👔 Meilleur Vendeur: {top_employee[0]} (${top_employee[1]:,.2f})")
        
        print(f"\n📅 Date de chargement: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")
        print(f"💾 Base de données: {self.output_db}")
        print("="*60)
    
    def close(self):
        """Ferme la connexion"""
        if self.conn:
            self.conn.close()
            print("\n✓ Connexion fermée")
    
    def execute_full_load(self):
        """Exécute le processus complet de chargement"""
        print("\n🚀 DÉBUT DU CHARGEMENT\n")
        
        # 1. Connexion
        if not self.connect():
            return False
        
        # 2. Charger les données
        loaded = self.load_all_data()
        print(f"\n✓ {loaded} tables chargées")
        
        # 3. Créer les index
        self.create_indexes()
        
        # 4. Créer les vues
        self.create_views()
        
        # 5. Vérifier la qualité
        self.verify_data_quality()
        
        # 6. Générer le rapport Excel
        self.generate_excel_report()
        
        # 7. Rapport de synthèse
        self.generate_summary_report()
        
        # 8. Fermer
        self.close()
        
        print("\n✅ CHARGEMENT TERMINÉ AVEC SUCCÈS\n")
        return True


def main():
    """Fonction principale"""
    loader = NorthwindLoader()
    loader.execute_full_load()


if __name__ == "__main__":
    main()