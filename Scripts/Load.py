import pandas as pd
from sqlalchemy import create_engine
import urllib
import pyodbc

#  Create SQLAlchemy engine using pyodbc
conn_str = (
    r'DRIVER={ODBC Driver 17 for SQL Server};'
    r'SERVER=localhost\SQLEXPRESS;'
    r'DATABASE=Northwind_DW;'
    r'Trusted_Connection=yes;'
)
params = urllib.parse.quote_plus(conn_str)
engine = create_engine(f"mssql+pyodbc:///?odbc_connect={params}")


# Load CSVs

SQL_customers = pd.read_csv('../Data/sql_server/sqlcustomers.csv')
SQL_employees = pd.read_csv('../Data/sql_server/sqlemployees.csv')
SQL_orders = pd.read_csv('../Data/sql_server/sqlorders.csv')

access_customers = pd.read_csv('../Data/access/customer.csv')
access_employees = pd.read_csv('../Data/access/employees.csv')
access_orders = pd.read_csv('../Data/access/order.csv')

#  Merge SQL + Access data
customers = pd.concat([access_customers, SQL_customers], ignore_index=True)
employees = pd.concat([access_employees, SQL_employees], ignore_index=True)
orders = pd.concat([access_orders, SQL_orders], ignore_index=True)
orders_2 = pd.concat([access_orders, SQL_orders], ignore_index=True)


customers = customers.drop_duplicates(subset='Company', keep='first')

orders['OrderDate'] = pd.to_datetime(orders['OrderDate'])
orders['ShippedDate'] = pd.to_datetime(orders['ShippedDate'])
orders['ShippedDate'] = orders['ShippedDate'].fillna(pd.Timestamp('1900-01-01'))

orders['OrderDateKey'] = orders['OrderDate'].dt.strftime('%d%m%Y')
orders['ShippedDateKey'] = orders['ShippedDate'].dt.strftime('%d%m%Y')


orders_2['OrderDate'] = pd.to_datetime(orders_2['OrderDate'])
orders_2['ShippedDate'] = pd.to_datetime(orders_2['ShippedDate'])

# Combine all dates (order + shipped) into a single Series
all_dates = pd.concat([orders_2['OrderDate'], orders_2['ShippedDate']], ignore_index=True)


all_dates = all_dates.fillna(pd.Timestamp('1900-01-01'))

# Remove duplicates
all_dates = all_dates.drop_duplicates().reset_index(drop=True)

# Create the DimTime DataFrame
dim_time = pd.DataFrame()
dim_time['FullDate'] = all_dates
dim_time['DateKey'] = dim_time['FullDate'].dt.strftime('%d%m%Y')  # e.g., 01102025
dim_time['Month'] = dim_time['FullDate'].dt.month
dim_time['Year'] = dim_time['FullDate'].dt.year

# Optional: sort by date
dim_time = dim_time.sort_values('FullDate').reset_index(drop=True)
#  Load into Data Warehouse
customers.to_sql('DimCustomer', con=engine, if_exists='append', index=False)
employees.to_sql('DimEmployee', con=engine, if_exists='append', index=False)
dim_time.to_sql('DimTime', con=engine, if_exists='append', index=False)
orders[['OrderID', 'CustomerID', 'EmployeeID', 'OrderDateKey', 'ShippedDateKey']].to_sql(
    'Order_Fact', con=engine, if_exists='append', index=False
)


dim_customer = pd.read_sql("SELECT * FROM DimCustomer", engine)
dim_employee = pd.read_sql("SELECT * FROM DimEmployee", engine)
dim_time = pd.read_sql("SELECT * FROM DimTime", engine)
order_fact = pd.read_sql("SELECT * FROM Order_Fact", engine)

# Export to CSV
dim_customer.to_csv("DimCustomer.csv", index=False)
dim_employee.to_csv("DimEmployee.csv", index=False)
dim_time.to_csv("DimTime.csv", index=False)
order_fact.to_csv("Order_Fact.csv", index=False)
