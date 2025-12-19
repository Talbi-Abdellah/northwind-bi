import pyodbc
import pandas as pd
import os

#  SQL SERVER CONNECTION
conn_sql = pyodbc.connect(
    'DRIVER={ODBC Driver 18 for SQL Server};'
    'SERVER=localhost\\SQLEXPRESS;'
    'DATABASE=Northwind;'
    'Trusted_Connection=yes;'
    'TrustServerCertificate=yes;'
)

# Load SQL Server tables
df_customers = pd.read_sql("SELECT * FROM Customers", conn_sql)
df_employees = pd.read_sql("SELECT * FROM Employees", conn_sql)
df_shippers = pd.read_sql("SELECT * FROM Shippers", conn_sql)
df_suppliers = pd.read_sql("SELECT * FROM Suppliers", conn_sql)
df_order_details = pd.read_sql("SELECT * FROM [Order Details]", conn_sql)
df_orders = pd.read_sql("SELECT * FROM Orders", conn_sql)
df_products = pd.read_sql("SELECT * FROM Products", conn_sql)

# ACCESS CONNECTION
access_file = r"C:\Users\User\Desktop\northwind-bi3\Files\Northwind 2012.accdb"

conn_access = pyodbc.connect(
    r'DRIVER={Microsoft Access Driver (*.mdb, *.accdb)};'
    rf'DBQ={access_file};'
)

customers = pd.read_sql_query("SELECT * FROM [Customers]", conn_access)
order_details = pd.read_sql_query("SELECT * FROM [Order Details]", conn_access)
orders = pd.read_sql_query("SELECT * FROM [Orders]", conn_access)
products = pd.read_sql_query("SELECT * FROM [Products]", conn_access)
purchase_orders = pd.read_sql_query("SELECT * FROM [Purchase Orders]", conn_access)
shippers = pd.read_sql_query("SELECT * FROM [Shippers]", conn_access)
suppliers = pd.read_sql_query("SELECT * FROM [Suppliers]", conn_access)
employees = pd.read_sql_query("SELECT * FROM [Employees]", conn_access)

# CLEAN ACCESS
for i in [customers, order_details, orders, products, purchase_orders, shippers, suppliers, employees]:
    i.drop_duplicates(inplace=True)

customers['ContactName'] = (
    customers['First Name'].fillna('') + ' ' + customers['Last Name'].fillna('')
).str.strip().str.replace(r'\s+', ' ', regex=True)

customers.drop(columns=['First Name', 'Last Name'], inplace=True)
customers.rename(columns={'Country/Region':'Country','ID':'CustomerID'}, inplace=True)
customers['CustomerID'] = 'A_' + customers['CustomerID'].astype(str)

customers.drop(columns=[
    'E-mail Address','Job Title','Business Phone','Home Phone','Mobile Phone',
    'Fax Number','Address','City','State/Province','ZIP/Postal Code','Web Page',
    'Notes','Attachments'
], inplace=True)

os.makedirs("../Data/access", exist_ok=True)
customers.to_csv("../Data/access/customers.csv", index=False, encoding='utf-8')

# CLEAN ACCESS ORDERS
orders['Shipper ID'] = orders['Shipper ID'].fillna(0).astype(int)

orders['Order Date'] = pd.to_datetime(orders['Order Date']).dt.strftime('%Y-%m-%d')
orders['Shipped Date'] = pd.to_datetime(orders['Shipped Date']).dt.strftime('%Y-%m-%d')

orders.drop(columns=[
    'Ship Name','Ship Address','Ship City','Ship State/Province','Ship ZIP/Postal Code',
    'Ship Country/Region','Taxes','Payment Type','Paid Date','Notes','Tax Rate',
    'Tax Status','Status ID'
], inplace=True, errors='ignore')

orders.columns = orders.columns.str.replace(' ', '', regex=False)
orders['OrderID'] = 'A_' + orders['OrderID'].astype(str)
orders['CustomerID'] = 'A_' + orders['CustomerID'].astype(str)
orders['EmployeeID'] = 'A_' + orders['EmployeeID'].astype(str)

orders.to_csv('../Data/access/order.csv', index=False)

# CLEAN ACCESS EMPLOYEES
employees.rename(columns={'ID':'EmployeeID'}, inplace=True)
employees['EmployeeID'] = 'A_' + employees['EmployeeID'].astype(str)

if 'First Name' in employees.columns:
    employees['FullName'] = (employees['First Name'].fillna('') + ' ' + employees['Last Name'].fillna('')).str.strip()
    employees.drop(columns=['First Name', 'Last Name'], inplace=True)

employees.drop(columns=[
    'E-mail Address','Job Title','Business Phone','Home Phone','Mobile Phone',
    'Fax Number','Address','City','State/Province','ZIP/Postal Code','Country/Region',
    'Web Page','Notes','Attachments','Company'
], inplace=True, errors='ignore')

employees.to_csv("../Data/access/employees.csv", index=False)

# CLEAN SQL SERVER

for i in [df_customers, df_employees, df_shippers, df_suppliers, df_order_details, df_orders, df_products]:
    i.drop_duplicates(inplace=True)

# SQL Customers
df_customers.rename(columns={'CompanyName':'Company'}, inplace=True)
df_customers.drop(columns=[
    'ContactTitle','Address','City','Region','PostalCode','Phone','Fax'
], inplace=True, errors='ignore')

df_customers['CustomerIntID'] = df_customers['CustomerID'].astype('category').cat.codes
customer_id_map = dict(zip(df_customers['CustomerID'], df_customers['CustomerIntID']))

df_customers.drop(columns=['CustomerID'], inplace=True)
df_customers.rename(columns={'CustomerIntID':'CustomerID'}, inplace=True)
df_customers['CustomerID'] = 'S_' + df_customers['CustomerID'].astype(str)

df_customers.to_csv('../Data/sql_server/customers.csv', index=False)

# SQL Employees
if 'FirstName' in df_employees.columns:
    df_employees['FullName'] = (
        df_employees['FirstName'].fillna('') + ' ' + df_employees['LastName'].fillna('')
    ).str.strip() 
    df_employees.drop(columns=['FirstName', 'LastName'], inplace=True)

df_employees.drop(columns=[
    'Title','TitleOfCourtesy','BirthDate','HireDate','Address','City','Region',
    'PostalCode','Country','HomePhone','Extension','Photo','Notes','PhotoPath',
    'ReportsTo'
], inplace=True, errors='ignore')

df_employees['EmployeeID'] = 'S_' + df_employees['EmployeeID'].astype(str)
df_employees.to_csv('../Data/sql_server/sqlemployees.csv', index=False)

# SQL Orders
df_orders.rename(columns={'Freight':'ShippingFee', 'ShipVia':'ShipperID'}, inplace=True)

df_orders['ShipperID'] = df_orders['ShipperID'].fillna(0).astype(int)
df_orders['ShippedDate'] = pd.to_datetime(df_orders['ShippedDate'], errors='coerce')

df_orders['CustomerIntID'] = df_orders['CustomerID'].map(customer_id_map)
df_orders.drop(columns=['CustomerID'], inplace=True)
df_orders.rename(columns={'CustomerIntID':'CustomerID'}, inplace=True)

df_orders.drop(columns=[
    'RequiredDate','ShipName','ShipAddress','ShipCity','ShipRegion','ShipPostalCode','ShipCountry'
], inplace=True, errors='ignore')

df_orders.to_csv('../Data/sql_server/sqlorders.csv', index=False)

# Close SQL connection
conn_sql.close()
conn_access.close()
