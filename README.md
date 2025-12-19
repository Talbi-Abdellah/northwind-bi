# Northwind ETL & Analytics — README

## Overview
This repository extracts data from two Northwind sources (files in the `Files` folder), cleans and merges them, loads a simple warehouse, and produces CSVs for Power BI analytics. Follow the steps below exactly to reproduce the same outputs.

+ Added: The repository includes Jupyter notebooks used during development; scripts are in the `Scripts/` folder and analysis/visualization steps are shown in `Notes` notebooks.

## Technologies used
- Python 3.8+
- Jupyter Notebooks
- pandas
- numpy
- pyodbc (for SQL Server / ODBC connections)
- SQLAlchemy (optional, used in some extraction/load steps)
- matplotlib and/or plotly (for plots shown in notebooks)
- Microsoft Access ODBC driver (for `.accdb` files)
- Power BI (for final dashboards/figures)

## Prerequisites
- Python 3.8+  
- Jupyter Notebook / JupyterLab (recommended for the `Notes` notebooks)
- Install required Python packages: run `pip install -r requirements.txt` from the repository root. Typical packages included: pandas, numpy, pyodbc, sqlalchemy, matplotlib, plotly, jupyter
- SQL Server (e.g. SQLEXPRESS or LocalDB) accessible on your machine
- Microsoft Access ODBC driver (for `.accdb` files) installed if you will read the Access DB
- The two database sources are in `Files/`:
  - `Northwind 2012.accdb` (Access DB)
  - `scriptNorthwind.sql` (SQL Server script to create/populate Northwind)

## Prepare the databases
1. SQL Server:
   - Create a database on your local SQL Server instance named `Northwind_DW` before running the load step. You can create it using SQL Server Management Studio or sqlcmd, for example:
     - In SSMS: Right-click Databases -> New Database -> enter `Northwind_DW`.
     - Or via sqlcmd:
       - sqlcmd -S localhost\SQLEXPRESS -Q "CREATE DATABASE [Northwind_DW]"
   - Ensure the Python SQL connection string in `Scripts/sql_db_extract.py` and `Scripts/Load.py` matches your server instance name (default in repo: `localhost\SQLEXPRESS`) and points to the `Northwind_DW` database if required.
2. Access:
   - Ensure the Access driver is installed and the file `Files/Northwind 2012.accdb` is present. The Access scripts use that relative path.

## Run order (exact)
1. Install requirements:
   - `pip install -r requirements.txt`
2. Extract, transform and clean (Access + SQL extraction):
   - From the `Scripts` folder run:
     - `python Extract&Transform.py`
   - This will extract data from the two sources and write cleaned CSVs to the Data folders (see Outputs below).
3. Load into warehouse:
   - Run `Scripts/Load.py` to merge cleaned CSVs and populate the warehouse (this script merges the datasets and writes warehouse CSVs). Make sure `Northwind_DW` exists and connection strings are correct.
4. Notes steps (post-load transforms)
   - Open the repository `Notes` and run the section labeled `Transform warehouse to csv` (execute the code or follow those steps to convert warehouse tables to CSV).
   - Then run the `Transform the warehouse csv to analytics` code in `Notes` to generate analytics-ready CSV files.
5. Power BI
   - Import the CSV files created by the analytics step from `Data/analytics` into Power BI and build your visuals. The final figures/dashboards come from these CSVs — load them into Power BI to reproduce the final charts.

## Output locations (where to find results)
- Cleaned Access CSVs: `Data/access/`
- Cleaned SQL extracts: `Data/sql_server/`
- Warehouse files: `Data/warehouse/`
- Analytics-ready CSVs: `Data/analytics/`




