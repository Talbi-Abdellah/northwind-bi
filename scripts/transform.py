"""
Script de transformation des données Northwind
Nettoyage, enrichissement et préparation pour l'analyse
"""

import pandas as pd
import numpy as np
import os
from datetime import datetime

class NorthwindTransformer:
    """Classe pour transformer les données extraites"""
    
    def __init__(self):
        self.raw_path = 'data/raw/'
        self.processed_path = 'data/processed/'
        
        # Créer le dossier de sortie
        os.makedirs(self.processed_path, exist_ok=True)
        
    def load_raw_data(self, filename):
        """Charge un fichier CSV depuis data/raw/"""
        try:
            df = pd.read_csv(f"{self.raw_path}{filename}")
            print(f"✓ Chargé: {filename} ({len(df)} lignes)")
            return df
        except Exception as e:
            print(f"✗ Erreur chargement {filename}: {e}")
            return None
    
    def clean_sales_data(self, df):
        """Nettoie et enrichit les données de ventes"""
        print("\n🧹 Nettoyage des données de ventes...")
        
        # Copie pour ne pas modifier l'original
        df_clean = df.copy()
        
        # 1. Convertir les dates
        df_clean['OrderDate'] = pd.to_datetime(df_clean['OrderDate'])
        df_clean['ShippedDate'] = pd.to_datetime(df_clean['ShippedDate'])
        
        # 2. Extraire des composantes temporelles
        df_clean['Year'] = df_clean['OrderDate'].dt.year
        df_clean['Month'] = df_clean['OrderDate'].dt.month
        df_clean['Quarter'] = df_clean['OrderDate'].dt.quarter
        df_clean['DayOfWeek'] = df_clean['OrderDate'].dt.dayofweek
        df_clean['MonthName'] = df_clean['OrderDate'].dt.strftime('%B')
        
        # 3. Calculer le délai de livraison
        df_clean['DeliveryDays'] = (df_clean['ShippedDate'] - df_clean['OrderDate']).dt.days
        
        # 4. Créer des catégories de montant
        df_clean['AmountCategory'] = pd.cut(
            df_clean['LineTotal'],
            bins=[0, 100, 500, 1000, float('inf')],
            labels=['Petit', 'Moyen', 'Grand', 'Très Grand']
        )
        
        # 5. Gérer les valeurs manquantes
        missing_before = df_clean.isnull().sum().sum()
        df_clean = df_clean.fillna({
            'DeliveryDays': df_clean['DeliveryDays'].median(),
            'Discount': 0
        })
        missing_after = df_clean.isnull().sum().sum()
        
        print(f"  • Dates converties")
        print(f"  • Composantes temporelles ajoutées")
        print(f"  • Délais de livraison calculés")
        print(f"  • Valeurs manquantes: {missing_before} → {missing_after}")
        
        return df_clean
    
    def create_aggregated_metrics(self, df):
        """Crée des métriques agrégées pour le dashboard"""
        print("\n📊 Création des métriques agrégées...")
        
        metrics = {}
        
        # 1. Ventes par mois
        monthly_sales = df.groupby(['Year', 'Month']).agg({
            'LineTotal': 'sum',
            'OrderID': 'nunique',
            'Quantity': 'sum'
        }).reset_index()
        monthly_sales.columns = ['Year', 'Month', 'TotalSales', 'NumOrders', 'TotalQuantity']
        metrics['monthly_sales'] = monthly_sales
        
        # 2. Ventes par catégorie
        category_sales = df.groupby('CategoryName').agg({
            'LineTotal': 'sum',
            'OrderID': 'nunique',
            'Quantity': 'sum'
        }).reset_index()
        category_sales.columns = ['Category', 'TotalSales', 'NumOrders', 'TotalQuantity']
        category_sales = category_sales.sort_values('TotalSales', ascending=False)
        metrics['category_sales'] = category_sales
        
        # 3. Top produits
        product_sales = df.groupby('ProductName').agg({
            'LineTotal': 'sum',
            'Quantity': 'sum',
            'OrderID': 'nunique'
        }).reset_index()
        product_sales.columns = ['Product', 'TotalSales', 'Quantity', 'NumOrders']
        product_sales = product_sales.sort_values('TotalSales', ascending=False).head(20)
        metrics['top_products'] = product_sales
        
        # 4. Ventes par pays
        country_sales = df.groupby('CustomerCountry').agg({
            'LineTotal': 'sum',
            'OrderID': 'nunique',
            'CustomerID': 'nunique'
        }).reset_index()
        country_sales.columns = ['Country', 'TotalSales', 'NumOrders', 'NumCustomers']
        country_sales = country_sales.sort_values('TotalSales', ascending=False)
        metrics['country_sales'] = country_sales
        
        # 5. Performance des employés
        employee_sales = df.groupby('EmployeeName').agg({
            'LineTotal': 'sum',
            'OrderID': 'nunique',
            'CustomerID': 'nunique'
        }).reset_index()
        employee_sales.columns = ['Employee', 'TotalSales', 'NumOrders', 'NumCustomers']
        employee_sales = employee_sales.sort_values('TotalSales', ascending=False)
        metrics['employee_sales'] = employee_sales
        
        # 6. KPIs globaux
        kpis = {
            'TotalRevenue': df['LineTotal'].sum(),
            'TotalOrders': df['OrderID'].nunique(),
            'TotalCustomers': df['CustomerID'].nunique(),
            'TotalProducts': df['ProductID'].nunique(),
            'AvgOrderValue': df.groupby('OrderID')['LineTotal'].sum().mean(),
            'AvgDeliveryDays': df['DeliveryDays'].mean()
        }
        metrics['kpis'] = pd.DataFrame([kpis])
        
        print(f"  • {len(metrics)} ensembles de métriques créés")
        for key, value in metrics.items():
            if isinstance(value, pd.DataFrame):
                print(f"    - {key}: {len(value)} lignes")
        
        return metrics
    
    def save_transformed_data(self, df, filename):
        """Sauvegarde les données transformées"""
        output_path = f"{self.processed_path}{filename}"
        df.to_csv(output_path, index=False, encoding='utf-8')
        print(f"✓ Sauvegardé: {output_path}")
    
    def transform_all(self):
        """Pipeline complet de transformation"""
        print("\n🚀 DÉBUT DE LA TRANSFORMATION\n")
        
        # 1. Charger les données brutes
        sales_df = self.load_raw_data('sales_analysis.csv')
        if sales_df is None:
            print("✗ Impossible de charger les données")
            return
        
        # 2. Nettoyer et enrichir
        sales_clean = self.clean_sales_data(sales_df)
        
        # 3. Créer les métriques agrégées
        metrics = self.create_aggregated_metrics(sales_clean)
        
        # 4. Sauvegarder tout
        print("\n💾 Sauvegarde des données transformées...")
        self.save_transformed_data(sales_clean, 'sales_clean.csv')
        
        for key, df in metrics.items():
            self.save_transformed_data(df, f'{key}.csv')
        
        # 5. Résumé
        self.print_summary(sales_clean, metrics)
        
        print("\n✅ TRANSFORMATION TERMINÉE\n")
        
        return sales_clean, metrics
    
    def print_summary(self, df, metrics):
        """Affiche un résumé de la transformation"""
        print("\n" + "="*60)
        print("RÉSUMÉ DE LA TRANSFORMATION")
        print("="*60)
        
        print(f"\n📊 Données nettoyées:")
        print(f"  • Lignes: {len(df):,}")
        print(f"  • Colonnes: {len(df.columns)}")
        print(f"  • Période: {df['OrderDate'].min()} à {df['OrderDate'].max()}")
        
        if 'kpis' in metrics:
            kpis = metrics['kpis'].iloc[0]
            print(f"\n💰 KPIs principaux:")
            print(f"  • Revenu total: ${kpis['TotalRevenue']:,.2f}")
            print(f"  • Commandes: {int(kpis['TotalOrders']):,}")
            print(f"  • Clients: {int(kpis['TotalCustomers'])}")
            print(f"  • Valeur moyenne commande: ${kpis['AvgOrderValue']:,.2f}")
            print(f"  • Délai livraison moyen: {kpis['AvgDeliveryDays']:.1f} jours")
        
        print(f"\n📁 Fichiers générés:")
        if os.path.exists(self.processed_path):
            files = os.listdir(self.processed_path)
            for f in files:
                print(f"  • {f}")
        
        print("="*60)


def main():
    """Fonction principale"""
    transformer = NorthwindTransformer()
    sales_clean, metrics = transformer.transform_all()


if __name__ == "__main__":
    main()