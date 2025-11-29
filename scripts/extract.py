"""
Script d'extraction des données Northwind
Auteur: Votre Nom
Date: 2025
"""

import sqlite3
import pandas as pd
import os
from datetime import datetime

class NorthwindExtractor:
    """Classe pour extraire les données de la base Northwind"""
    
    def __init__(self, db_path='data/northwind.db'):
        """
        Initialise l'extracteur
        Args:
            db_path: Chemin vers la base de données
        """
        self.db_path = db_path
        self.conn = None
        self.raw_data_path = 'data/raw/'
        
        # Créer les dossiers nécessaires
        os.makedirs(self.raw_data_path, exist_ok=True)
        
    def connect(self):
        """Établit la connexion à la base de données"""
        try:
            self.conn = sqlite3.connect(self.db_path)
            print(f"✓ Connexion établie à {self.db_path}")
            return True
        except Exception as e:
            print(f"✗ Erreur de connexion: {e}")
            return False
    
    def extract_table(self, table_name):
        """
        Extrait une table complète
        Args:
            table_name: Nom de la table à extraire
        Returns:
            DataFrame contenant les données
        """
        try:
            query = f"SELECT * FROM {table_name}"
            df = pd.read_sql_query(query, self.conn)
            print(f"✓ Table {table_name}: {len(df)} lignes extraites")
            return df
        except Exception as e:
            print(f"✗ Erreur lors de l'extraction de {table_name}: {e}")
            return None
    
    def extract_all_tables(self):
        """Extrait toutes les tables principales de Northwind"""
        tables = {
            'customers': 'Customers',
            'orders': 'Orders',
            'order_details': 'Order Details',
            'products': 'Products',
            'categories': 'Categories',
            'employees': 'Employees',
            'suppliers': 'Suppliers',
            'shippers': 'Shippers'
        }
        
        extracted_data = {}
        
        for key, table_name in tables.items():
            df = self.extract_table(table_name)
            if df is not None:
                # Sauvegarder en CSV
                output_file = f"{self.raw_data_path}{key}.csv"
                df.to_csv(output_file, index=False, encoding='utf-8')
                extracted_data[key] = df
                print(f"  → Sauvegardé: {output_file}")
        
        return extracted_data
    
    def extract_sales_analysis(self):
        """Extrait une vue consolidée pour l'analyse des ventes"""
        query = """
        SELECT 
            o.OrderID,
            o.OrderDate,
            o.ShippedDate,
            o.CustomerID,
            c.CompanyName as CustomerName,
            c.Country as CustomerCountry,
            c.City as CustomerCity,
            e.EmployeeID,
            e.FirstName || ' ' || e.LastName as EmployeeName,
            od.ProductID,
            p.ProductName,
            p.CategoryID,
            cat.CategoryName,
            od.UnitPrice,
            od.Quantity,
            od.Discount,
            od.UnitPrice * od.Quantity * (1 - od.Discount) as LineTotal
        FROM Orders o
        JOIN Customers c ON o.CustomerID = c.CustomerID
        JOIN Employees e ON o.EmployeeID = e.EmployeeID
        JOIN [Order Details] od ON o.OrderID = od.OrderID
        JOIN Products p ON od.ProductID = p.ProductID
        JOIN Categories cat ON p.CategoryID = cat.CategoryID
        """
        
        try:
            df = pd.read_sql_query(query, self.conn)
            output_file = f"{self.raw_data_path}sales_analysis.csv"
            df.to_csv(output_file, index=False, encoding='utf-8')
            print(f"✓ Vue analytique: {len(df)} lignes extraites")
            print(f"  → Sauvegardé: {output_file}")
            return df
        except Exception as e:
            print(f"✗ Erreur extraction vue analytique: {e}")
            return None
    
    def close(self):
        """Ferme la connexion à la base de données"""
        if self.conn:
            self.conn.close()
            print("✓ Connexion fermée")
    
    def get_extraction_summary(self):
        """Affiche un résumé de l'extraction"""
        print("\n" + "="*60)
        print("RÉSUMÉ DE L'EXTRACTION")
        print("="*60)
        
        # Lister les fichiers extraits
        if os.path.exists(self.raw_data_path):
            files = os.listdir(self.raw_data_path)
            print(f"\nFichiers extraits ({len(files)}):")
            for f in files:
                file_path = os.path.join(self.raw_data_path, f)
                size = os.path.getsize(file_path) / 1024  # en KB
                print(f"  • {f} ({size:.2f} KB)")
        
        print(f"\nDate d'extraction: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")
        print("="*60)


def main():
    """Fonction principale d'extraction"""
    print("\n🚀 DÉBUT DE L'EXTRACTION DES DONNÉES NORTHWIND\n")
    
    # Créer l'extracteur
    extractor = NorthwindExtractor()
    
    # Se connecter
    if not extractor.connect():
        return
    
    # Extraire toutes les tables
    print("\n📊 Extraction des tables principales...")
    extracted_data = extractor.extract_all_tables()
    
    # Extraire la vue analytique
    print("\n📈 Extraction de la vue analytique...")
    sales_data = extractor.extract_sales_analysis()
    
    # Afficher le résumé
    extractor.get_extraction_summary()
    
    # Fermer la connexion
    extractor.close()
    
    print("\n✅ EXTRACTION TERMINÉE AVEC SUCCÈS\n")


if __name__ == "__main__":
    main()