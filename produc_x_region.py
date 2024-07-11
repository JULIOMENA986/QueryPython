import pandas as pd
from database import Connection as db_connections

# Configuración de la conexión a la base de datos
database = "northwind"
conn = db_connections(database)

query1 = "SELECT * FROM customers"
query2 = "SELECT * FROM orders"
query3 = "SELECT * FROM `order details`"
query4 = "SELECT * FROM products"
query5 = "SELECT * FROM employees"
query6 = "SELECT * FROM region"
query7 = "SELECT * FROM territories"
query8 = "SELECT * FROM employeeterritories"

df_customers = conn.fetch_dataframe(query1)
df_orders = conn.fetch_dataframe(query2)
df_order_details = conn.fetch_dataframe(query3)
df_products = conn.fetch_dataframe(query4)
df_employees = conn.fetch_dataframe(query5)
df_region = conn.fetch_dataframe(query6)
df_territories = conn.fetch_dataframe(query7)
df_employeeterritories = conn.fetch_dataframe(query8)

df_merged = pd.merge(df_customers, df_orders, on='CustomerID')
df_merged = pd.merge(df_merged, df_order_details, on='OrderID')
df_merged = pd.merge(df_merged, df_products, on='ProductID')
df_merged = pd.merge(df_merged, df_employees, on='EmployeeID')
df_merged = pd.merge(df_merged, df_employeeterritories, on='EmployeeID')
df_merged = pd.merge(df_merged, df_territories, on='TerritoryID')
df_merged = pd.merge(df_merged, df_region, on='RegionID')

df_merged['AñoVenta'] = pd.to_datetime(df_merged['OrderDate']).dt.year

df_merged['Ventas'] = df_merged['Quantity'] * df_merged['UnitPrice_y']

df_grouped = df_merged.groupby(['CustomerID', 'RegionDescription', 'ProductName', 'AñoVenta'], as_index=False).agg({
    'Ventas': 'sum'
})

df_grouped['Rank'] = df_grouped.groupby(['CustomerID', 'RegionDescription'])['Ventas'].rank(method='first', ascending=True)

df_filtered = df_grouped[df_grouped['Rank'] == 1]

df_filtered['ProductName_AñoVenta'] = df_filtered['ProductName'] + df_filtered['AñoVenta'].astype(str)

df_eastern = df_filtered[df_filtered['RegionDescription'].str.strip() == 'Eastern']
df_westerns = df_filtered[df_filtered['RegionDescription'].str.strip() == 'Westerns']
df_northern = df_filtered[df_filtered['RegionDescription'].str.strip() == 'Northern']
df_southern = df_filtered[df_filtered['RegionDescription'].str.strip() == 'Southern']

df_eastern_pivot = df_eastern.pivot_table(index='CustomerID', values='ProductName_AñoVenta', aggfunc=lambda x: ', '.join(map(str, x))).reset_index()
df_westerns_pivot = df_westerns.pivot_table(index='CustomerID', values='ProductName_AñoVenta', aggfunc=lambda x: ', '.join(map(str, x))).reset_index()
df_northern_pivot = df_northern.pivot_table(index='CustomerID', values='ProductName_AñoVenta', aggfunc=lambda x: ', '.join(map(str, x))).reset_index()
df_southern_pivot = df_southern.pivot_table(index='CustomerID', values='ProductName_AñoVenta', aggfunc=lambda x: ', '.join(map(str, x))).reset_index()

df_eastern_pivot.columns = ['CustomerID', 'Eastern']
df_westerns_pivot.columns = ['CustomerID', 'Westerns']
df_northern_pivot.columns = ['CustomerID', 'Northern']
df_southern_pivot.columns = ['CustomerID', 'Southern']

df_final = pd.merge(df_eastern_pivot, df_westerns_pivot, on='CustomerID', how='outer')
df_final = pd.merge(df_final, df_northern_pivot, on='CustomerID', how='outer')
df_final = pd.merge(df_final, df_southern_pivot, on='CustomerID', how='outer')

df_final = df_final.sort_values(by='CustomerID')

print("Resultado en la terminal:")
print(df_final)

# Guardar el resultado en un archivo Excel
excel_filename = 'productos_de_region.xlsx'
try:
    df_final.to_excel(excel_filename, index=False)
    print(f"\nSe ha guardado el resultado en '{excel_filename}' correctamente.")
except PermissionError as e:
    print(f"Error: Permiso denegado para guardar '{excel_filename}'. Verifica los permisos de escritura.")
except Exception as e:
    print(f"Error al guardar en '{excel_filename}': {e}")
finally:
    conn.close()
