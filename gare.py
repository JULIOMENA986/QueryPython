import pandas as pd
from pymysql import connect
from database import Connection as db_conetions


# Conexión a la base de datos MySQL
database="northwind"
conn = db_conetions(database)
                

# Cursor para ejecutar las consultas

# Configuración de pandas para mostrar todas las columnas y filas
pd.set_option('display.max_colwidth', None)
pd.set_option('display.max_rows', None)
pd.set_option('display.max_columns', None)
pd.set_option('display.width', None)
pd.set_option('display.expand_frame_repr', False)

# Consultas SQL
query1 = "SELECT * FROM Orders"
query2 = "SELECT * FROM `order details`"
query3 = """ SELECT DISTINCT et.EmployeeID                                                                                                       , r.RegionID  FROM region r  JOIN territories t ON t.RegionID = r.RegionID JOIN employeeterritories et ON et.TerritoryID = t.TerritoryID ORDER BY et.EmployeeID, r.RegionID
"""
# Leer datos de MySQL en DataFrames de pandas
df_orders = conn.fetch_dataframe(query1)
df_order_details = conn.fetch_dataframe(query2)
df_employee_regions = conn.fetch_dataframe(query3)

# Unir los detalles de las órdenes con las órdenes
df_order_details_merged = pd.merge(df_order_details, df_orders, on='OrderID', how='inner')

# Calcular las ganancias por empleado
df_order_details_merged['Ganancia'] = df_order_details_merged['UnitPrice'] * df_order_details_merged['Quantity'] * (1 - df_order_details_merged['Discount'])
df_ganemp = df_order_details_merged.groupby("EmployeeID")['Ganancia'].sum().reset_index()

# Unir los empleados con las regiones y luego con las ganancias
df_reem = pd.merge(df_employee_regions, df_ganemp, on='EmployeeID', how='inner')

# Calcular la suma de las ganancias por región
df_final = df_reem.groupby("RegionID")['Ganancia'].sum().reset_index()

# Guardar el resultado final en un archivo Excel
df_final.to_excel("resultado_ganancias_por_region.xlsx", index=False)

# Mostrar el resultado final
print(df_final)
