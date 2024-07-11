import pandas as pd
from database import Connection as db_connections

# Configuración de la conexión a la base de datos
database = "pubs"
conn = db_connections(database)

# Consultas SQL
query1 = "SELECT * FROM sales"
query2 = """
 SELECT * FROM titleauthor                                                                                                                 UNION SELECT 'Anonimo' AS au_id, t.title_id, '' AS au_ord,        IFNULL((100 - SUM(royaltyper)), 100) AS royaltyper FROM titleauthor ta RIGHT JOIN titles t ON t.title_id = ta.title_id GROUP BY t.title_id HAVING royaltyper > 0 OR royaltyper IS NULL
"""
query3 = "SELECT * FROM titles"
query4 = "SELECT * FROM authors"

# Ejecutar las consultas utilizando la clase MySQLDatabase
df_sales = conn.fetch_dataframe(query1)
df_titleauthor = conn.fetch_dataframe(query2)
df_titles = conn.fetch_dataframe(query3)
df_authors = conn.fetch_dataframe(query4)

# Merge de las tablas
df_merged = df_sales.merge(df_titles, on="title_id", how="inner") \
                   .merge(df_titleauthor, on="title_id", how="left") \
                   .merge(df_authors, on="au_id", how="left")

# Calcular las ganancias
df_merged['Ganancias'] = df_merged.apply(
    lambda row: row['qty'] * row['price'] * (row['royaltyper'] if pd.notna(row['royaltyper']) else 0) / 100,
    axis=1
)

# Agrupar por nombre y apellido del autor
df_result = df_merged.groupby(
    [df_merged['au_fname'].fillna('Anónimo'), df_merged['au_lname'].fillna('')]
).agg({'Ganancias': 'sum'}).reset_index()

# Ordenar por ganancias en orden descendente
df_result = df_result.sort_values(by='Ganancias', ascending=False)

# Mostrar el resultado en la terminal
print("Resultado en la terminal:")
print(df_result)

# Guardar el resultado en un archivo Excel
excel_filename = 'ganancias_autores.xlsx'
try:
    df_result.to_excel(excel_filename, index=False)
    print(f"\nSe ha guardado el resultado en '{excel_filename}' correctamente.")
except PermissionError as e:
    print(f"Error: Permiso denegado para guardar '{excel_filename}'. Verifica los permisos de escritura.")
except Exception as e:
    print(f"Error al guardar en '{excel_filename}': {e}")
finally:
    conn.close()  # Cerrar la conexión a la base de datos
