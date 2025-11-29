"""
Tableau de bord analytique Northwind
Dashboard interactif avec Plotly et Dash
"""

import pandas as pd
import plotly.graph_objects as go
import plotly.express as px
from dash import Dash, html, dcc, callback, Output, Input
import os

class NorthwindDashboard:
    """Classe pour créer le dashboard analytique"""
    
    def __init__(self):
        self.processed_path = 'data/processed/'
        self.figures_path = 'figures/'
        os.makedirs(self.figures_path, exist_ok=True)
        
        # Charger les données
        self.load_data()
        
    def load_data(self):
        """Charge toutes les données transformées"""
        print("📂 Chargement des données...")
        
        try:
            self.sales = pd.read_csv(f"{self.processed_path}sales_clean.csv")
            self.monthly = pd.read_csv(f"{self.processed_path}monthly_sales.csv")
            self.categories = pd.read_csv(f"{self.processed_path}category_sales.csv")
            self.products = pd.read_csv(f"{self.processed_path}top_products.csv")
            self.countries = pd.read_csv(f"{self.processed_path}country_sales.csv")
            self.employees = pd.read_csv(f"{self.processed_path}employee_sales.csv")
            self.kpis = pd.read_csv(f"{self.processed_path}kpis.csv")
            
            # Convertir les dates
            self.sales['OrderDate'] = pd.to_datetime(self.sales['OrderDate'])
            
            print("✓ Données chargées avec succès")
        except Exception as e:
            print(f"✗ Erreur: {e}")
    
    def create_kpi_cards(self):
        """Crée les cartes KPI"""
        kpi = self.kpis.iloc[0]
        
        cards = html.Div([
            html.Div([
                html.Div([
                    html.H3("💰 Revenu Total", style={'fontSize': '18px', 'color': '#666'}),
                    html.H2(f"${kpi['TotalRevenue']:,.0f}", style={'color': '#2ecc71', 'margin': '10px 0'}),
                ], style={'background': 'white', 'padding': '20px', 'borderRadius': '10px', 'boxShadow': '0 2px 4px rgba(0,0,0,0.1)'}),
                
                html.Div([
                    html.H3("📦 Commandes", style={'fontSize': '18px', 'color': '#666'}),
                    html.H2(f"{int(kpi['TotalOrders']):,}", style={'color': '#3498db', 'margin': '10px 0'}),
                ], style={'background': 'white', 'padding': '20px', 'borderRadius': '10px', 'boxShadow': '0 2px 4px rgba(0,0,0,0.1)'}),
                
                html.Div([
                    html.H3("👥 Clients", style={'fontSize': '18px', 'color': '#666'}),
                    html.H2(f"{int(kpi['TotalCustomers'])}", style={'color': '#e74c3c', 'margin': '10px 0'}),
                ], style={'background': 'white', 'padding': '20px', 'borderRadius': '10px', 'boxShadow': '0 2px 4px rgba(0,0,0,0.1)'}),
                
                html.Div([
                    html.H3("📊 Panier Moyen", style={'fontSize': '18px', 'color': '#666'}),
                    html.H2(f"${kpi['AvgOrderValue']:,.0f}", style={'color': '#9b59b6', 'margin': '10px 0'}),
                ], style={'background': 'white', 'padding': '20px', 'borderRadius': '10px', 'boxShadow': '0 2px 4px rgba(0,0,0,0.1)'}),
                
            ], style={'display': 'grid', 'gridTemplateColumns': 'repeat(4, 1fr)', 'gap': '20px', 'marginBottom': '30px'})
        ])
        
        return cards
    
    def plot_monthly_sales(self):
        """Graphique des ventes mensuelles"""
        fig = go.Figure()
        
        fig.add_trace(go.Scatter(
            x=self.monthly.index,
            y=self.monthly['TotalSales'],
            mode='lines+markers',
            name='Ventes',
            line=dict(color='#3498db', width=3),
            marker=dict(size=8),
            fill='tozeroy',
            fillcolor='rgba(52, 152, 219, 0.1)'
        ))
        
        fig.update_layout(
            title='📈 Évolution des ventes mensuelles',
            xaxis_title='Mois',
            yaxis_title='Ventes ($)',
            template='plotly_white',
            height=400,
            hovermode='x unified'
        )
        
        return fig
    
    def plot_category_distribution(self):
        """Graphique des ventes par catégorie"""
        fig = px.pie(
            self.categories,
            values='TotalSales',
            names='Category',
            title='🎯 Répartition des ventes par catégorie',
            color_discrete_sequence=px.colors.qualitative.Set3
        )
        
        fig.update_traces(textposition='inside', textinfo='percent+label')
        fig.update_layout(height=400)
        
        return fig
    
    def plot_top_products(self):
        """Top 10 produits"""
        top10 = self.products.head(10)
        
        fig = go.Figure(go.Bar(
            x=top10['TotalSales'],
            y=top10['Product'],
            orientation='h',
            marker=dict(
                color=top10['TotalSales'],
                colorscale='Viridis',
                showscale=True
            ),
            text=top10['TotalSales'].apply(lambda x: f'${x:,.0f}'),
            textposition='outside'
        ))
        
        fig.update_layout(
            title='🏆 Top 10 Produits',
            xaxis_title='Ventes ($)',
            yaxis_title='',
            template='plotly_white',
            height=400,
            yaxis={'categoryorder': 'total ascending'}
        )
        
        return fig
    
    def plot_country_sales(self):
        """Ventes par pays"""
        top_countries = self.countries.head(15)
        
        fig = px.bar(
            top_countries,
            x='Country',
            y='TotalSales',
            title='🌍 Ventes par pays (Top 15)',
            color='TotalSales',
            color_continuous_scale='Blues',
            text='TotalSales'
        )
        
        fig.update_traces(texttemplate='$%{text:,.0f}', textposition='outside')
        fig.update_layout(
            template='plotly_white',
            height=400,
            xaxis={'categoryorder': 'total descending'}
        )
        
        return fig
    
    def plot_employee_performance(self):
        """Performance des employés"""
        fig = go.Figure()
        
        fig.add_trace(go.Bar(
            name='Ventes',
            x=self.employees['Employee'],
            y=self.employees['TotalSales'],
            marker_color='#3498db'
        ))
        
        fig.add_trace(go.Bar(
            name='Nombre de commandes',
            x=self.employees['Employee'],
            y=self.employees['NumOrders'] * 100,  # Échelle pour visualisation
            marker_color='#e74c3c'
        ))
        
        fig.update_layout(
            title='👔 Performance des employés',
            xaxis_title='Employé',
            yaxis_title='Ventes ($)',
            template='plotly_white',
            height=400,
            barmode='group'
        )
        
        return fig
    
    def create_dash_app(self):
        """Crée l'application Dash interactive"""
        app = Dash(__name__)
        
        app.layout = html.Div([
            html.Div([
                html.H1('📊 Tableau de Bord Analytique Northwind', 
                       style={'textAlign': 'center', 'color': '#2c3e50', 'marginBottom': '30px'}),
                html.Hr(),
            ]),
            
            # KPIs
            self.create_kpi_cards(),
            
            # Graphiques principaux
            html.Div([
                html.Div([
                    dcc.Graph(figure=self.plot_monthly_sales())
                ], style={'width': '50%'}),
                
                html.Div([
                    dcc.Graph(figure=self.plot_category_distribution())
                ], style={'width': '50%'}),
            ], style={'display': 'flex', 'gap': '20px', 'marginBottom': '30px'}),
            
            html.Div([
                html.Div([
                    dcc.Graph(figure=self.plot_top_products())
                ], style={'width': '50%'}),
                
                html.Div([
                    dcc.Graph(figure=self.plot_country_sales())
                ], style={'width': '50%'}),
            ], style={'display': 'flex', 'gap': '20px', 'marginBottom': '30px'}),
            
            html.Div([
                dcc.Graph(figure=self.plot_employee_performance())
            ]),
            
            html.Footer([
                html.Hr(),
                html.P('Dashboard créé avec Python, Plotly et Dash | Projet BI Northwind 2025',
                      style={'textAlign': 'center', 'color': '#7f8c8d', 'marginTop': '30px'})
            ])
            
        ], style={'padding': '30px', 'fontFamily': 'Arial, sans-serif', 'background': '#ecf0f1'})
        
        return app
    
    def save_static_charts(self):
        """Sauvegarde les graphiques en PNG"""
        print("\n💾 Sauvegarde des graphiques...")
        
        charts = {
            'monthly_sales.png': self.plot_monthly_sales(),
            'category_distribution.png': self.plot_category_distribution(),
            'top_products.png': self.plot_top_products(),
            'country_sales.png': self.plot_country_sales(),
            'employee_performance.png': self.plot_employee_performance()
        }
        
        for filename, fig in charts.items():
            path = f"{self.figures_path}{filename}"
            fig.write_image(path, width=1200, height=600)
            print(f"  ✓ {filename}")
        
        print(f"\n📁 Graphiques sauvegardés dans {self.figures_path}")
    
    def run(self, debug=True, port=8050):
        """Lance le dashboard interactif"""
        print("\n🚀 Lancement du dashboard...")
        print(f"📡 Serveur démarré sur http://localhost:{port}")
        print("💡 Appuyez sur Ctrl+C pour arrêter\n")
        
        app = self.create_dash_app()
        app.run_server(debug=debug, port=port)


def main():
    """Fonction principale"""
    print("\n" + "="*60)
    print("CRÉATION DU TABLEAU DE BORD NORTHWIND")
    print("="*60 + "\n")
    
    # Créer le dashboard
    dashboard = NorthwindDashboard()
    
    # Sauvegarder les graphiques statiques
    # dashboard.save_static_charts()  # Décommenter si plotly-kaleido est installé
    
    # Lancer le dashboard interactif
    dashboard.run(debug=True, port=8050)


if __name__ == "__main__":
    main()