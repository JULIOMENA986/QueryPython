import pandas as pd
from database import Connection as db_connections

# Configuración de la conexión a la base de datos
database = "northwind"
conn = db_connections(database)

query_orders = "SELECT * FROM orders"
query_order_details = "SELECT * FROM `order details`"
query_products = "SELECT * FROM products"
query_categories = "SELECT * FROM categories"

df_orders = conn.fetch_dataframe(query_orders)
df_order_details = conn.fetch_dataframe(query_order_details)
df_products = conn.fetch_dataframe(query_products)
df_categories = conn.fetch_dataframe(query_categories)

df_merged = pd.merge(df_orders, df_order_details, on='OrderID')
df_merged = pd.merge(df_merged, df_products, on='ProductID')
df_merged = pd.merge(df_merged, df_categories, on='CategoryID')

df_merged['Año'] = pd.to_datetime(df_merged['OrderDate']).dt.year

df_merged['Total'] = df_merged['UnitPrice_y'] * df_merged['Quantity']

df_max_purchased = df_merged.groupby(['CategoryID', 'CategoryName', 'Año', 'ProductID', 'ProductName'], as_index=False).agg({
    'Total': 'sum'
}).rename(columns={'Total': 'MaxSales'})

df_max_purchased = df_max_purchased.sort_values(by=['CategoryID', 'Año', 'MaxSales'], ascending=[True, True, False])
df_max_purchased = df_max_purchased.groupby(['CategoryID', 'Año']).first().reset_index()

df_customers = df_merged.merge(df_max_purchased[['ProductID', 'Año']], on=['ProductID', 'Año'])
df_customers = df_customers.groupby(['CategoryID', 'CategoryName', 'Año', 'ProductID', 'ProductName', 'CustomerID'], as_index=False).agg({
    'Total': 'sum'
}).sort_values(by=['CategoryID', 'ProductID', 'Año', 'Total'], ascending=[True, True, True, False])

df_min_customer = df_customers.groupby(['CategoryID', 'Año', 'ProductID'], as_index=False).agg({
    'Total': 'min'
}).rename(columns={'Total': 'Mini'})

df_max_customer = df_customers.groupby(['CategoryID', 'Año', 'ProductID'], as_index=False).agg({
    'Total': 'max'
}).rename(columns={'Total': 'Maxi'})

df_final_min = df_customers.merge(df_min_customer, left_on=['CategoryID', 'Año', 'ProductID', 'Total'], right_on=['CategoryID', 'Año', 'ProductID', 'Mini'], how='inner')
df_final_max = df_customers.merge(df_max_customer, left_on=['CategoryID', 'Año', 'ProductID', 'Total'], right_on=['CategoryID', 'Año', 'ProductID', 'Maxi'], how='inner')
df_final = pd.concat([df_final_min, df_final_max]).drop_duplicates().sort_values(by=['CategoryID', 'ProductID', 'Año'])

df_years = df_orders.groupby(df_orders['OrderDate'].dt.year).size().reset_index(name='Count').rename(columns={'OrderDate': 'Año'})
df_years['Rank'] = df_years['Año'].rank(method='dense', ascending=False).astype(int)
df_years = df_years.sort_values('Rank')

years = df_years['Año'].tolist()
columns = ['CategoryName', 'Ultimo', 'Penultimo', 'Antepenultimo']

df_final_result = pd.DataFrame(columns=columns)

for rank, col in enumerate(['Ultimo', 'Penultimo', 'Antepenultimo'], start=1):
    year = df_years[df_years['Rank'] == rank]['Año'].values[0]
    temp_df = df_final[df_final['Año'] == year].copy()
    temp_df['Info'] = temp_df['ProductName'] + ', ' + temp_df['CustomerID'] + '-' + temp_df['Total'].astype(str)
    temp_df = temp_df.groupby('CategoryName')['Info'].apply(lambda x: ', '.join(x)).reset_index()
    temp_df.columns = ['CategoryName', col]
    if df_final_result.empty:
        df_final_result = temp_df
    else:
        df_final_result = pd.merge(df_final_result, temp_df, on='CategoryName', how='outer')

df_final_result = df_final_result.sort_values(by='CategoryName').fillna('')

print("Resultado en la terminal:")
print(df_final_result)

# Guardar el resultado en un archivo Excel
excel_filename = 'examen.xlsx'
try:
    df_final_result.to_excel(excel_filename, index=False)
    print(f"\nSe ha guardado el resultado en '{excel_filename}' correctamente.")
except PermissionError as e:
    print(f"Error: Permiso denegado para guardar '{excel_filename}'. Verifica los permisos de escritura.")
except Exception as e:
    print(f"Error al guardar en '{excel_filename}': {e}")
finally:
    conn.close()  
